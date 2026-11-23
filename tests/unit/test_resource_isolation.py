"""Tenant/owner/platform access boundaries for alerts, DLQ entries and request metrics."""

import pytest

from app.infrastructure.queue.dlq_handler import DeadLetterQueue, DLQEntry
from app.infrastructure.security.auth import Role, TenantTokenPayload
from app.infrastructure.security.resource_scope import ResourceScopeClass
from app.observability.alert_manager import AlertManager
from app.observability.alert_types import AlertSeverity, AlertTypes
from app.observability.metrics import MetricsCollector

TENANT_A = ResourceScopeClass(scope="tenant", tenant_id="tenant-a")
TENANT_B = ResourceScopeClass(scope="tenant", tenant_id="tenant-b")
PLATFORM = ResourceScopeClass(scope="platform")
USER_A_OWNED = ResourceScopeClass(scope="tenant", tenant_id="tenant-a", owner_user_id="user-a")


def _principal(tenant_id: str, user_id: str, role: Role) -> TenantTokenPayload:
    return TenantTokenPayload(sub=user_id, role=role, exp=4102444800, tenant_id=tenant_id)


@pytest.mark.parametrize(
    ("resource", "tenant_id", "user_id", "role", "write", "expected"),
    [
        (USER_A_OWNED, "tenant-a", "user-a", Role.OPERATOR, False, True),
        (USER_A_OWNED, "tenant-a", "user-b", Role.OPERATOR, False, False),
        (USER_A_OWNED, "tenant-a", "user-b", Role.ADMIN, False, True),
        (USER_A_OWNED, "tenant-b", "user-a", Role.ADMIN, False, False),
        (USER_A_OWNED, "tenant-a", "user-a", Role.AUDITOR, True, False),
        (USER_A_OWNED, None, "p", Role.PLATFORM_ADMIN, False, False),
        (TENANT_A, "tenant-a", "anyone", Role.AUDITOR, False, True),
        (PLATFORM, None, "p", Role.PLATFORM_ADMIN, True, True),
        (PLATFORM, "tenant-a", "u", Role.ADMIN, False, False),
    ],
)
def test_can_access_enforces_tenant_owner_and_role(
    resource: ResourceScopeClass,
    tenant_id: str | None,
    user_id: str,
    role: Role,
    write: bool,
    expected: bool,
) -> None:
    assert (
        resource.can_access(tenant_id=tenant_id, user_id=user_id, role=role, write=write)
        is expected
    )


def test_platform_scope_rejects_owner_user() -> None:
    with pytest.raises(ValueError):
        ResourceScopeClass(scope="platform", owner_user_id="user-a")


def test_metrics_are_readable_only_by_authorized_principals() -> None:
    collector = MetricsCollector()
    stored = collector.create_metrics("request-1", USER_A_OWNED)

    owner_view = collector.get_metrics_for(
        "request-1", _principal("tenant-a", "user-a", Role.OPERATOR)
    )
    assert owner_view is not None and owner_view is not stored  # 返回副本
    assert collector.get_metrics_for("request-1", _principal("tenant-a", "x", Role.ADMIN))
    assert (
        collector.get_metrics_for("request-1", _principal("tenant-a", "user-b", Role.OPERATOR))
        is None
    )
    assert (
        collector.get_metrics_for("request-1", _principal("tenant-b", "user-a", Role.ADMIN)) is None
    )
    assert (
        collector.get_metrics_for("missing", _principal("tenant-a", "user-a", Role.ADMIN)) is None
    )


def test_evicted_metrics_drop_owner_record() -> None:
    collector = MetricsCollector(max_requests=1)
    collector.create_metrics("old", USER_A_OWNED)
    collector.create_metrics("new", USER_A_OWNED)

    assert "old" not in collector._owners


def test_alerts_are_isolated_by_owner() -> None:
    manager = AlertManager()
    try:
        alert = manager.create_alert(
            owner=TENANT_A,
            alert_type=AlertTypes.FAULT_ALERT,
            severity=AlertSeverity.INFO,
            title="t",
            message="m",
            affected_resource="task-1",
        )

        assert [a.alert_id for a in manager.get_alerts(owner=TENANT_A)] == [alert.alert_id]
        assert manager.get_alerts(owner=TENANT_B) == []
        assert not manager.acknowledge_alert(alert.alert_id, "user-b", TENANT_B)
        assert manager.acknowledge_alert(alert.alert_id, "user-a", TENANT_A)
    finally:
        manager.close()


@pytest.mark.parametrize("owner", [TENANT_A, PLATFORM])
def test_dlq_entry_owner_survives_round_trip(owner: ResourceScopeClass) -> None:
    entry = DLQEntry(
        dlq_id="dlq-1",
        task_id="task-1",
        payload={"content": "x"},
        failure_reason="boom",
        owner=owner,
        handler_key="ingest",
    )

    assert DLQEntry.from_dict(entry.to_dict()).owner == owner


@pytest.mark.asyncio
async def test_dlq_entries_are_isolated_by_owner() -> None:
    dlq = DeadLetterQueue(backoff_base_seconds=0)
    dlq.redis = None
    calls: list[dict] = []
    dlq.register_replay_handler("ingest", calls.append)

    entry = await dlq.add("task-1", {"content": "x"}, "boom", owner=TENANT_A, handler_key="ingest")

    assert [e.dlq_id for e in await dlq.list_pending(owner=TENANT_A)] == [entry.dlq_id]
    assert await dlq.list_pending(owner=TENANT_B) == []
    assert (await dlq.stats(owner=TENANT_B))["total"] == 0
    assert not await dlq.retry(entry.dlq_id, owner=TENANT_B)
    assert calls == []

    assert await dlq.retry(entry.dlq_id, owner=TENANT_A)
    assert calls == [{"content": "x"}]
