"""死信队列 (Dead Letter Queue) 处理器

用于收集持续失败的任务，支持：
- 自动重投递 (可配置最大重投次数, 指数退避)
- 放弃标记与人工介入
- 失败原因归类统计
"""

import asyncio
import hashlib
import inspect
import json
import logging
import threading
import uuid
from collections.abc import Callable
from dataclasses import asdict, field, replace
from datetime import UTC, datetime
from enum import Enum
from typing import Any, Awaitable, cast

from pydantic.dataclasses import dataclass
from redis import RedisError
from redis import asyncio as redis_asyncio

from app.config.settings import settings
from app.infrastructure.security.resource_scope import ResourceScopeClass as ResourceOwner

logger = logging.getLogger(__name__)


class DLQStatus(str, Enum):
    """死信队列状态"""

    PENDING = "pending"
    RETRYING = "retrying"
    RESOLVED = "resolved"
    ABANDONED = "abandoned"


ReplayHandler = Callable[[dict[str, Any]], Awaitable[None] | None]
NON_REPLAYABLE_HANDLER = "manual"


@dataclass
class DLQEntry:
    """死信队列条目"""

    dlq_id: str
    task_id: str
    payload: dict[str, Any]
    failure_reason: str
    owner: ResourceOwner
    handler_key: str
    retry_count: int = 0
    max_retries: int = 3
    status: DLQStatus = DLQStatus.PENDING
    created_at: str = field(default_factory=lambda: datetime.now(UTC).isoformat())
    last_retry_at: str | None = None

    def to_dict(self) -> dict[str, Any]:
        data = asdict(self)
        data["status"] = self.status.value
        return data

    @staticmethod
    def from_dict(data: dict[str, Any]) -> "DLQEntry":
        return DLQEntry(
            dlq_id=data["dlq_id"],
            task_id=data["task_id"],
            payload=data["payload"],
            failure_reason=data["failure_reason"],
            owner=ResourceOwner(
                scope=data["owner"]["scope"], tenant_id=data["owner"].get("tenant_id")
            ),
            handler_key=data["handler_key"],
            retry_count=data.get("retry_count", 0),
            max_retries=data.get("max_retries", 3),
            status=DLQStatus(data["status"]),
            created_at=data["created_at"],
            last_retry_at=data.get("last_retry_at"),
        )


class DeadLetterQueue:
    "线程安全的死信队列处理器"

    def __init__(self, max_retries: int = 3, backoff_base_seconds: int = 1) -> None:
        self.max_retries = max_retries
        self.backoff_base_seconds = backoff_base_seconds
        self._lock = threading.Lock()
        self._entries: dict[str, DLQEntry] = {}
        self.redis = self._build_redis_client()
        self._worker_task: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event | None = None
        self._replay_handler: dict[str, ReplayHandler] = {}

    def register_replay_handler(self, handler_key: str, handler: ReplayHandler) -> None:
        self._replay_handler[handler_key] = handler

    async def _execute_entry(self, entry: DLQEntry) -> None:
        handler = self._replay_handler.get(entry.handler_key)
        if handler is None:
            raise RuntimeError(f"No replay handler registered for DLQ type: {entry.handler_key}")
        result = handler(entry.payload)
        if inspect.isawaitable(result):
            await result

    async def startup(self) -> None:
        """异步初始化：从Redis回捞历史DLQ数据，必须手动await调用"""
        await self._load_existing_entries()
        self._stop_event = asyncio.Event()
        self._worker_task = asyncio.create_task(self._worker_loop())

    async def close(self) -> None:
        """Stop the background worker and close the Redis client."""
        if self._stop_event is not None:
            self._stop_event.set()
        if self._worker_task is not None:
            await self._worker_task
            self._worker_task = None
        if self.redis is not None:
            await self.redis.aclose()

    @staticmethod
    def _build_redis_client() -> redis_asyncio.Redis | None:
        """创建Redis客户端"""
        try:
            return redis_asyncio.Redis(
                host=settings.redis_host,
                port=settings.redis_port,
                db=settings.redis_db,
                password=settings.redis_password,
                decode_responses=True,
                socket_timeout=2,
            )
        except ImportError:
            logger.warning("Redis module not found, please install it with `pip install redis`")
            return None

    def _entry_keys(self, dlq_id: str) -> str:
        """生成Redis键"""
        return f"dlq:entry:{dlq_id}"

    def _redis_index_key(self, owner: ResourceOwner) -> str:
        scope, tenant_id = owner.storage_key()
        owner_id = tenant_id if tenant_id is not None else "platform"
        return f"dlq:index:{scope}:{owner_id}"

    async def _persist_entry(self, entry: DLQEntry) -> None:
        """将条目持久化到Redis"""
        if not self.redis:
            return
        try:
            await self._write_entry(entry)
        except (ConnectionError, TimeoutError, ValueError, redis_asyncio.RedisError) as e:
            logger.error(f"Failed to persist DLQ entry: {e}")

    async def _write_entry(self, entry: DLQEntry, *, create_only: bool = False) -> DLQEntry:
        if self.redis is None:
            raise RedisError("DLQ Redis 未初始化")
        key = self._entry_keys(entry.dlq_id)
        index_key = self._redis_index_key(entry.owner)
        ttl = 7 * 24 * 3600
        async with self.redis.pipeline() as pipe:
            pipe.set(key, json.dumps(entry.to_dict(), ensure_ascii=False))
            pipe.sadd(index_key, entry.dlq_id)
            pipe.sadd("dlq:owners", index_key)
            pipe.expire(index_key, ttl)
            pipe.get(key)
            result = await pipe.execute()
        raw = result[-1]
        if raw is None:
            raise RedisError(f"DLQ 条目写入后无法读取：{entry.dlq_id}")
        return DLQEntry.from_dict(json.loads(raw))

    async def _load_existing_entries(self) -> None:
        """从Redis加载现有条目"""
        if not self.redis:
            return
        try:
            if self.redis:
                index_keys = await self.redis.smembers("dlq:owners")
                for raw_index_key in index_keys:
                    dlq_ids = await self.redis.smembers(raw_index_key)
                    for dlq_id in dlq_ids:
                        str_dlq_id = cast(str, dlq_id)
                        entry_data = await self.redis.get(self._entry_keys(str_dlq_id))
                        if not entry_data:
                            continue
                        try:
                            entry = DLQEntry.from_dict(json.loads(entry_data))
                        except (ValueError, KeyError) as e:
                            logger.error(f"Failed to load DLQ entry {str_dlq_id}: {e}")
                            continue
                        self._entries[str_dlq_id] = entry
        except (ConnectionError, TimeoutError, ValueError, redis_asyncio.RedisError) as e:
            logger.error(f"Failed to connect to Redis: {e}")

    async def add(
        self,
        task_id: str,
        payload: dict[str, Any],
        failure_reason: str,
        *,
        owner: ResourceOwner,
        handler_key: str,
        stream_source: tuple[str, str, str] | None = None,
    ) -> DLQEntry:
        """添加死信队列条目"""
        if stream_source is not None:
            dlq_id = f"dlq_{uuid.uuid4().hex[:12]}"
        else:
            identity = json.dumps(stream_source, separators=(",", ":"))
            dlq_id = f"dlq_stream_{hashlib.sha256(identity.encode('utf-8')).hexdigest()}"

        entry = DLQEntry(
            dlq_id=dlq_id,
            task_id=task_id,
            payload=payload,
            failure_reason=failure_reason,
            max_retries=self.max_retries,
            owner=owner,
            handler_key=handler_key,
        )
        if stream_source is not None:
            entry = await self._write_entry(entry, create_only=True)
            with self._lock:
                self._entries.setdefault(dlq_id, entry)
            logger.warning(f"Added DLQ entry: {entry}")
        else:
            with self._lock:
                self._entries[dlq_id] = entry
            await self._persist_entry(entry)
            logger.warning(f"Added DLQ entry: {entry}")
        return entry

    async def retry(
        self,
        dlq_id: str,
        owner: ResourceOwner | None,
    ) -> bool:
        """重试死信队列条目"""
        with self._lock:
            entry = self._entries.get(dlq_id)
            if entry is None or entry.status != DLQStatus.PENDING:
                return False
            if owner is not None and entry.owner != owner:
                logger.warning("DLQ 条目归属不匹配：dlq_id=%s", dlq_id)
                return False
            if entry.handler_key == NON_REPLAYABLE_HANDLER:
                logger.warning("DLQ 条目不支持自动重放：dlq_id=%s", dlq_id)
                return False
            exhausted = entry.retry_count >= entry.max_retries
            entry.status = DLQStatus.RETRYING
            if not exhausted:
                entry.retry_count += 1
                entry.last_retry_at = datetime.now(UTC).isoformat()
            final_status = DLQStatus.ABANDONED if exhausted else DLQStatus.PENDING

        try:
            if exhausted:
                logger.warning("DLQ 条目已达到最大重试次数：dlq_id=%s", dlq_id)
            else:
                delay = self.backoff_base_seconds * 2 ** (entry.retry_count - 1)
                await asyncio.sleep(delay)
                await self._execute_entry(entry)
                final_status = DLQStatus.RESOLVED
        except Exception as e:  # noqa: BLE001
            logger.exception(f"Retry failed for DLQ entry {dlq_id}: {e}")
        finally:
            try:
                await self._persist_entry(replace(entry, status=final_status))
            finally:
                with self._lock:
                    entry.status = final_status
        return final_status == DLQStatus.RESOLVED

    async def list_pending(self, *, owner: ResourceOwner | None) -> list[DLQEntry]:
        """列出所有待处理的死信队列条目"""
        with self._lock:
            return [
                entry
                for entry in self._entries.values()
                if entry.status == DLQStatus.PENDING and (owner is None or entry.owner == owner)
            ]

    async def stats(self, owner: ResourceOwner | None) -> dict[str, Any]:
        """统计死信队列条目状态"""
        with self._lock:
            entries = [e for e in self._entries.values() if owner is None or e.owner == owner]
            return {
                "total": len(entries),
                "pending": sum(1 for e in entries if e.status == DLQStatus.PENDING),
                "retrying": sum(1 for e in entries if e.status == DLQStatus.RETRYING),
                "resolved": sum(1 for e in entries if e.status == DLQStatus.RESOLVED),
                "abandoned": sum(1 for e in entries if e.status == DLQStatus.ABANDONED),
            }

    async def _worker_loop(self) -> None:
        """后台线程循环处理死信队列"""
        if self._stop_event is None:
            return
        while not self._stop_event.is_set():
            try:
                for entry in await self.list_pending(owner=None):
                    if entry.handler_key == NON_REPLAYABLE_HANDLER:
                        continue
                    logger.info(f"Retrying DLQ entry: {entry}")
                    await self.retry(
                        entry.dlq_id,
                        owner=None,
                    )
            except (ConnectionError, TimeoutError, ValueError) as e:
                logger.error(f"Error in DLQ worker loop: {e}")
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=5)
            except TimeoutError:
                continue


dead_letter_queue = DeadLetterQueue()
