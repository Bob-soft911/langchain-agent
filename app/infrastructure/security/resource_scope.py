from dataclasses import dataclass

from typing_extensions import Literal

from app.infrastructure.security.auth import Role

ResourceScope = Literal["tenant", "platform"]


@dataclass(frozen=True)
class ResourceScopeClass:
    scope: ResourceScope
    tenant_id: str | None = None
    owner_user_id: str | None = None  # None 表示租户级共享资源（如告警）

    def __post_init__(self):
        if self.scope == "tenant" and self.tenant_id is None:
            raise ValueError("tenant_id must be provided for tenant scope")
        if self.scope == "platform" and (self.tenant_id is not None or self.owner_user_id is not None):
            raise ValueError("tenant_id and owner_user_id must be None for platform scope")

    def storage_key(self) -> tuple[str, str | None]:
        return self.scope, self.tenant_id

    def can_access(
        self, *, tenant_id: str | None, user_id: str, role: Role, write: bool = False
    ) -> bool:
        if self.scope == "platform":
            return role == Role.PLATFORM_ADMIN
        if role == Role.PLATFORM_ADMIN or tenant_id != self.tenant_id:
            return False
        if write and role not in (Role.ADMIN, Role.OPERATOR):
            return False
        # 租户管理员可访问本租户所有资源；其他角色只能访问自己的
        return self.owner_user_id is None or role == Role.ADMIN or self.owner_user_id == user_id
