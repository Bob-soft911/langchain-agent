import pytest

from app.infrastructure.agent.intervention_handler import InterventionHandler
from app.infrastructure.security.auth import Role, TenantTokenPayload
from app.infrastructure.security.resource_scope import ResourceScopeClass


def _principal(tenant_id: str, user_id: str, role: Role) -> TenantTokenPayload:
    return TenantTokenPayload(sub=user_id, role=role, exp=4102444800, tenant_id=tenant_id)


@pytest.fixture
def handler() -> InterventionHandler:
    handler = InterventionHandler()
    handler.create_intervention_request(
        task_id="task-1",
        reason="needs review",
        user_id="user-a",
        request_id="request-1",
        owner=ResourceScopeClass(scope="tenant", tenant_id="tenant-a", owner_user_id="user-a"),
    )
    return handler


@pytest.mark.parametrize(
    "principal",
    [
        _principal("tenant-a", "user-a", Role.OPERATOR),
        _principal("tenant-a", "user-a", Role.AUDITOR),
        _principal("tenant-a", "other-admin", Role.ADMIN),
    ],
)
def test_owner_and_tenant_admin_can_read_interventions(
    handler: InterventionHandler, principal: TenantTokenPayload
) -> None:
    pending = handler.get_pending_intervention("request-1", principal)

    assert pending is not None and len(pending) == 1
    assert handler.get_intervention_history("request-1", principal) == []


@pytest.mark.parametrize(
    "principal",
    [
        _principal("tenant-b", "user-a", Role.ADMIN),  # 跨租户
        _principal("tenant-a", "user-b", Role.OPERATOR),  # 同租户非所有者
    ],
)
def test_other_principals_cannot_read_interventions(
    handler: InterventionHandler, principal: TenantTokenPayload
) -> None:
    assert handler.get_pending_intervention("request-1", principal) is None
    assert handler.get_intervention_history("request-1", principal) is None


def test_unknown_request_is_indistinguishable_from_forbidden(handler: InterventionHandler) -> None:
    owner = _principal("tenant-a", "user-a", Role.OPERATOR)

    assert handler.get_pending_intervention("missing", owner) is None
    assert handler.get_intervention_history("missing", owner) is None
