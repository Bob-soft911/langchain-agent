from app.infrastructure.agent.intervention_handler import InterventionHandler
from app.infrastructure.security.resource_scope import ResourceScopeClass


def test_intervention_requests_are_scoped_by_tenant() -> None:
    handler = InterventionHandler()
    tenant_a = ResourceScopeClass(scope="tenant", tenant_id="tenant-a")
    tenant_b = ResourceScopeClass(scope="tenant", tenant_id="tenant-b")

    handler.create_intervention_request(
        task_id="task-1",
        reason="needs review",
        request_id="request-1",
        owner=tenant_a,
    )

    assert len(handler.get_pending_intervention("request-1", tenant_a)) == 1
    assert handler.get_pending_intervention("request-1", tenant_b) == []
