import hashlib
from collections.abc import Iterator
from types import SimpleNamespace

import jwt
import pytest
from fastapi.testclient import TestClient

from app.config.settings import settings
from app.infrastructure.agent.intervention_handler import InterventionHandler
from app.infrastructure.security.auth import Role, create_access_token
from app.infrastructure.security.resource_scope import ResourceScopeClass
from app.main import app, limiter  # 必须先于路由模块导入，路由模块会反向 import app.main
from app.api.routers.chat import _get_chat_service  # noqa: I001
from app.observability.alert_manager import AlertManager
from app.observability.alert_types import AlertSeverity, AlertTypes
from app.observability.metrics import MetricsCollector


@pytest.fixture(autouse=True)
def _reset_rate_limiter() -> Iterator[None]:
    limiter.reset()  # 内存限流器进程内共享，避免用例间互相触发 429
    yield
    limiter.reset()


@pytest.fixture
def client(monkeypatch: pytest.MonkeyPatch) -> TestClient:
    monkeypatch.setattr(settings, "app_env", "production")  # 关闭 test 环境的免鉴权后门
    monkeypatch.setattr(
        settings,
        "admin_principals",
        {
            "admin-user": {
                "api_key_hash": hashlib.sha256(b"admin-key").hexdigest(),
                "role": "admin",
                "tenant_id": "tenant-a",
            },
            "auditor-user": {
                "api_key_hash": hashlib.sha256(b"auditor-key").hexdigest(),
                "role": "auditor",
                "tenant_id": "tenant-b",
            },
            "platform-user": {
                "api_key_hash": hashlib.sha256(b"platform-key").hexdigest(),
                "role": "platform_admin",
            },
        },
    )
    return TestClient(app, raise_server_exceptions=False)


def _bearer(user: str, role: Role, tenant: str | None) -> dict[str, str]:
    return {"Authorization": f"Bearer {create_access_token(user, role, tenant)}"}


def test_token_issued_with_server_side_role_and_tenant(client: TestClient) -> None:
    resp = client.post(
        "/admin/token",
        json={"user_id": "auditor-user", "api_key": "auditor-key", "role": "admin"},
    )

    assert resp.status_code == 200
    claims = jwt.decode(
        resp.json()["token"], settings.admin_jwt_secret, algorithms=[settings.admin_jwt_algorithm]
    )
    assert claims["role"] == "auditor"  # 客户端自报的 role 被忽略
    assert claims["tenant_id"] == "tenant-b"


@pytest.mark.parametrize(
    "payload",
    [
        {"user_id": "admin-user", "api_key": "wrong"},
        {"user_id": "ghost", "api_key": "admin-key"},
    ],
)
def test_invalid_credentials_return_401_error_contract(client: TestClient, payload: dict) -> None:
    resp = client.post("/admin/token", json=payload)

    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "AUTH_INVALID_CREDENTIALS"


@pytest.mark.parametrize("path", ["/admin/config", "/ingest/stats", "/admin/dlq/stats"])
def test_protected_endpoints_require_token(client: TestClient, path: str) -> None:
    resp = client.get(path)

    assert resp.status_code == 401
    assert resp.json()["error"]["code"] == "AUTH_INVALID_TOKEN"


def test_rbac_blocks_auditor_from_admin_only_endpoint(client: TestClient) -> None:
    resp = client.get("/admin/config", headers=_bearer("auditor-user", Role.AUDITOR, "tenant-b"))

    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "AUTH_FORBIDDEN"


@pytest.mark.parametrize(
    ("method", "path"),
    [
        ("get", "/admin/config"),
        ("get", "/admin/config/history"),
        ("post", "/admin/config/task_max_retries?value=5"),
        ("post", "/admin/config/task_max_retries/reset"),
        ("get", "/admin/dlq/stats"),
        ("get", "/admin/dlq/pending"),
        ("post", "/admin/dlq/dlq_x/retry"),
    ],
)
def test_tenant_admin_cannot_access_platform_endpoints(
    client: TestClient, method: str, path: str
) -> None:
    resp = client.request(method, path, headers=_bearer("admin-user", Role.ADMIN, "tenant-a"))

    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "AUTH_FORBIDDEN"


def test_platform_admin_can_read_config(client: TestClient) -> None:
    resp = client.get("/admin/config", headers=_bearer("platform-user", Role.PLATFORM_ADMIN, None))

    assert resp.status_code == 200


def test_platform_admin_token_has_no_tenant(client: TestClient) -> None:
    resp = client.post("/admin/token", json={"user_id": "platform-user", "api_key": "platform-key"})

    assert resp.status_code == 200
    claims = jwt.decode(
        resp.json()["token"], settings.admin_jwt_secret, algorithms=[settings.admin_jwt_algorithm]
    )
    assert claims["role"] == "platform_admin"
    assert claims["tenant_id"] is None


def test_platform_admin_cannot_use_tenant_endpoints(client: TestClient) -> None:
    resp = client.get("/chat/alerts", headers=_bearer("platform-user", Role.PLATFORM_ADMIN, None))

    assert resp.status_code == 403


def test_expired_token_is_rejected(client: TestClient) -> None:
    token = create_access_token("admin-user", Role.ADMIN, "tenant-a", expires_minutes=-1)

    resp = client.get("/admin/config", headers={"Authorization": f"Bearer {token}"})

    assert resp.status_code == 401


# ---- 租户/所有者/角色负向用例：request-1 属于 tenant-a 的 owner-user ----

OWNER = ("owner-user", Role.OPERATOR, "tenant-a")
PEER = ("peer-user", Role.OPERATOR, "tenant-a")
AUDITOR = ("auditor-user", Role.AUDITOR, "tenant-a")
OTHER_TENANT_ADMIN = ("other-admin", Role.ADMIN, "tenant-b")
PLATFORM_ADMIN = ("platform-user", Role.PLATFORM_ADMIN, None)

READ_PATHS = [
    "/chat/request-1/pending-intervention",
    "/chat/request-1/intervention-history",
    "/chat/metrics/request-1",
]


@pytest.fixture
def chat_service() -> Iterator[SimpleNamespace]:
    owner = ResourceScopeClass(scope="tenant", tenant_id="tenant-a", owner_user_id="owner-user")
    interventions = InterventionHandler()
    interventions.create_intervention_request(
        task_id="task-1", reason="r", user_id="owner-user", request_id="request-1", owner=owner
    )
    metrics = MetricsCollector()
    metrics.create_metrics("request-1", owner)
    alerts = AlertManager()
    alert = alerts.create_alert(
        owner=ResourceScopeClass(scope="tenant", tenant_id="tenant-a"),
        alert_type=AlertTypes.FAULT_ALERT,
        severity=AlertSeverity.INFO,
        title="t",
        message="m",
        affected_resource="task-1",
    )
    service = SimpleNamespace(
        _intervention_handler=interventions,
        metrics_collector=metrics,
        alert_manager=alerts,
        alert_id=alert.alert_id,
    )
    app.dependency_overrides[_get_chat_service] = lambda: service
    yield service
    app.dependency_overrides.pop(_get_chat_service, None)
    alerts.close()


@pytest.mark.usefixtures("chat_service")
@pytest.mark.parametrize("path", READ_PATHS)
def test_resource_owner_can_read(client: TestClient, path: str) -> None:
    assert client.get(path, headers=_bearer(*OWNER)).status_code == 200


@pytest.mark.usefixtures("chat_service")
@pytest.mark.parametrize("principal", [PEER, OTHER_TENANT_ADMIN])
@pytest.mark.parametrize("path", READ_PATHS)
def test_cross_tenant_or_non_owner_read_returns_404(
    client: TestClient, path: str, principal: tuple
) -> None:
    assert client.get(path, headers=_bearer(*principal)).status_code == 404


@pytest.mark.parametrize(
    ("principal", "method", "path", "body"),
    [
        (
            AUDITOR,
            "post",
            "/chat/request-1/intervention",
            {"task_id": "task-1", "intervention_type": "skip"},
        ),
        (AUDITOR, "post", "/chat/alerts/{alert_id}/acknowledge", None),
        (AUDITOR, "post", "/ingest/text", {"content": "x", "source_id": "s"}),
        (PLATFORM_ADMIN, "post", "/ingest/text", {"content": "x", "source_id": "s"}),
        (PLATFORM_ADMIN, "get", "/ingest/stats", None),
    ],
)
def test_write_and_tenant_endpoints_reject_wrong_role(
    client: TestClient,
    chat_service: SimpleNamespace,
    principal: tuple,
    method: str,
    path: str,
    body: dict | None,
) -> None:
    resp = client.request(
        method,
        path.format(alert_id=chat_service.alert_id),
        json=body,
        headers=_bearer(*principal),
    )

    assert resp.status_code == 403
    assert resp.json()["error"]["code"] == "AUTH_FORBIDDEN"


def test_alert_acknowledge_is_tenant_scoped_and_uses_jwt_subject(
    client: TestClient, chat_service: SimpleNamespace
) -> None:
    path = f"/chat/alerts/{chat_service.alert_id}/acknowledge?acknowledged_by=spoofed"

    assert client.post(path, headers=_bearer(*OTHER_TENANT_ADMIN)).status_code == 404
    assert client.post(path, headers=_bearer(*OWNER)).status_code == 200

    alert = chat_service.alert_manager.get_alerts(
        owner=ResourceScopeClass(scope="tenant", tenant_id="tenant-a")
    )[0]
    assert alert.acknowledged_by == "owner-user"  # 忽略客户端传入的确认人
