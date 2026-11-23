import hashlib
import hmac
import logging
import secrets
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Annotated, Any

import jwt
from fastapi import Depends
from fastapi.security import OAuth2PasswordBearer
from pydantic import BaseModel

from app.config.settings import settings
from app.schemas.errors import AppError, ErrorCode

logger = logging.getLogger(__name__)


class Role(str, Enum):
    ADMIN = "admin"  # 租户管理员
    OPERATOR = "operator"
    AUDITOR = "auditor"
    PLATFORM_ADMIN = "platform_admin"  # 平台系统管理员


class TokenPayload(BaseModel):
    sub: str
    role: Role
    exp: int
    tenant_id: str | None = None  # 签发时写入，业务侧强制使用


class TenantTokenPayload(BaseModel):
    sub: str
    role: Role
    exp: int
    tenant_id: str


oauth2_scheme = OAuth2PasswordBearer(tokenUrl="/admin/token", auto_error=False)


def create_access_token(
    user_id: str, role: Role, tenant_id: str | None, expires_minutes: int | None = None
) -> str:
    if role == Role.PLATFORM_ADMIN:
        if tenant_id is not None:
            raise ValueError("Platform admin should not have a tenant_id")
    elif not tenant_id:
        raise ValueError("Tenant ID is required for non-platform admins")
    exp_minutes = expires_minutes or 15
    expire_at = datetime.now(tz=UTC) + timedelta(minutes=exp_minutes)
    payload = {
        "sub": user_id,
        "role": role.value,
        "exp": int(expire_at.timestamp()),
        "tenant_id": tenant_id,
        "jti": secrets.token_urlsafe(8),
    }
    token = jwt.encode(payload, settings.admin_jwt_secret, algorithm=settings.admin_jwt_algorithm)
    return token


def get_principal_tenant(user_id: str) -> str | None:
    """根据用户 ID 获取其所属租户 ID。"""
    principal = settings.admin_principals.get(user_id)
    if principal:
        return principal.get("tenant_id")
    return None


def verify_principal(user_id: str, api_key: str) -> Role | None:
    """根据用户 ID 和租户 ID 校验其角色。"""
    principal = settings.admin_principals.get(user_id)
    if not principal:
        return None
    expected_hash = principal.get("api_key_hash", "")
    if not expected_hash:
        return None
    provided_hash = hashlib.sha256(api_key.encode()).hexdigest()
    if not hmac.compare_digest(provided_hash, expected_hash):
        return None
    return Role(principal.get("role")) if principal.get("role") else None


def get_current_user(token: str | None = Depends(oauth2_scheme)) -> TokenPayload:
    if token is None and settings.app_env == "test":
        return TokenPayload(
            sub="test-user",
            role=Role.ADMIN,
            exp=int(datetime.now(tz=UTC).timestamp()) + 3600,
            tenant_id="test-tenant",
        )
    credentials_exception = AppError(
        ErrorCode.AUTH_INVALID_TOKEN, headers={"WWW-Authenticate": "Bearer"}
    )
    try:
        if token is None:
            logger.warning("Authentication failed: bearer token is missing")
            raise credentials_exception
        payload = _decode_with_rotation(token)
        sub = payload.get("sub")
        role = payload.get("role")
        exp = payload.get("exp")
        tenant_id = payload.get("tenant_id")
        if not sub or not role or not exp:
            raise credentials_exception
        role_enum = Role(role)
        # 角色与租户声明必须一致：平台管理员无租户，其余角色必须有租户
        if role_enum == Role.PLATFORM_ADMIN and tenant_id is not None:
            raise credentials_exception
        if role_enum != Role.PLATFORM_ADMIN and not tenant_id:
            raise credentials_exception
        token_data = TokenPayload(sub=sub, role=role_enum, exp=exp, tenant_id=tenant_id)
        return token_data
    except (jwt.PyJWTError, ValueError) as exc:
        logger.warning("Authentication failed: invalid token (%s)", type(exc).__name__)
        raise credentials_exception


def _decode_with_rotation(token: str) -> dict[str, Any]:
    """依次尝试当前密钥与轮换期内的上一把密钥，全部失败才抛出异常。"""
    last_exc: jwt.PyJWTError = jwt.InvalidTokenError("no secret configured")
    for secret in [settings.admin_jwt_secret, getattr(settings, "admin_jwt_secret_previous", None)]:
        if not secret:
            continue
        try:
            return jwt.decode(token, secret, algorithms=[settings.admin_jwt_algorithm])
        except jwt.PyJWTError as exc:
            last_exc = exc
    raise last_exc


def require_auth(token_data: Annotated[TokenPayload, Depends(get_current_user)]) -> TokenPayload:
    """全局认证依赖：挂到 /chat、/ingest 等端点，强制校验 JWT。"""
    return token_data


def require_tenant_auth(
    token_data: Annotated[TokenPayload, Depends(get_current_user)],
) -> TenantTokenPayload:
    if not token_data.tenant_id or token_data.role == Role.PLATFORM_ADMIN:
        raise AppError(ErrorCode.AUTH_FORBIDDEN)
    return TenantTokenPayload.model_validate(token_data.model_dump())


def require_tenant_roles(*roles: Role):
    """租户内角色校验：先保证是租户主体，再校验角色。"""

    def checker(
        token_data: Annotated[TenantTokenPayload, Depends(require_tenant_auth)],
    ) -> TenantTokenPayload:
        if token_data.role not in roles:
            raise AppError(ErrorCode.AUTH_FORBIDDEN)
        return token_data

    return checker


TENANT_WRITE_ROLES = (Role.ADMIN, Role.OPERATOR)


def require_roles(*roles: Role):
    def role_checker(
        token_data: Annotated[TokenPayload, Depends(get_current_user)],
    ) -> TokenPayload:
        if token_data.role not in roles:
            raise AppError(ErrorCode.AUTH_FORBIDDEN)
        return token_data

    return role_checker


def get_rate_limit_identity(token: str) -> str | None:
    """Return a stable identity for rate limiting from a verified token.

    Args:
        token: Encoded JWT access token.

    Returns:
        A tenant-scoped user identity, or `None` when the token or claims are invalid.
    """
    try:
        payload = _decode_with_rotation(token)
    except jwt.PyJWTError:
        return None

    tenant_id = payload.get("tenant_id")
    user_id = payload.get("sub")
    if not isinstance(tenant_id, str) or not tenant_id:
        return None
    if not isinstance(user_id, str) or not user_id:
        return None
    return f"tenant:{tenant_id}:user:{user_id}"


def require_platform_admin(
    token_data: Annotated[TokenPayload, Depends(get_current_user)],
) -> TokenPayload:
    if token_data.role != Role.PLATFORM_ADMIN or token_data.tenant_id is not None:
        raise AppError(ErrorCode.AUTH_FORBIDDEN)
    return token_data
