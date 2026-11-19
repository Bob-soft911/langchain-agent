"""Tenant/platform ownership boundaries for alerts and DLQ entries."""

import pytest

from app.infrastructure.queue.dlq_handler import DeadLetterQueue, DLQEntry
from app.infrastructure.security.resource_scope import ResourceScopeClass
from app.observability.alert_manager import AlertManager
from app.observability.alert_types import AlertSeverity, AlertTypes

TENANT_A = ResourceScopeClass(scope="tenant", tenant_id="tenant-a")
TENANT_B = ResourceScopeClass(scope="tenant", tenant_id="tenant-b")
PLATFORM = ResourceScopeClass(scope="platform")


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
