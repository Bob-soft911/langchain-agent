from dataclasses import dataclass

from typing_extensions import Literal

ResourceScope = Literal["tenant", "platform"]


@dataclass(frozen=True)
class ResourceScopeClass:
    scope: ResourceScope
    tenant_id: str | None = None

    def __post_init__(self):
        if self.scope == "tenant" and self.tenant_id is None:
            raise ValueError("tenant_id must be provided for tenant scope")
        if self.scope == "platform" and self.tenant_id is not None:
            raise ValueError("tenant_id must be None for platform scope")

    def storage_key(self) -> tuple[str, str | None]:
        return self.scope, self.tenant_id
