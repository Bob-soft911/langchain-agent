import asyncio
import json
import logging
import socket
from collections.abc import Awaitable, Callable
from typing import Any, cast

from redis import asyncio as redis_asyncio
from redis.exceptions import RedisError

from app.infrastructure.queue.dlq_handler import NON_REPLAYABLE_HANDLER, dead_letter_queue
from app.infrastructure.security.resource_scope import ResourceScopeClass as ResourceOwner

logger = logging.getLogger(__name__)

TaskHandler = Callable[[dict[str, Any]], Awaitable[None]]
StreamMessage = tuple[str, dict[str, str]]
StreamResponse = list[tuple[str, list[StreamMessage]]]


class RedisStreamWorker:
    def __init__(
        self,
        redis_client: redis_asyncio.Redis,
        *,
        stream_key: str = "tasks:ingest",
        group_name: str = "ingest-workers",
        consumer_name: str | None = None,
        handler: TaskHandler,
    ) -> None:
        self._redis = redis_client
        self._stream_key = stream_key
        self._group_name = group_name
        self._consumer_name = consumer_name or socket.gethostname()
        self._handler = handler
        self._stop_event: asyncio.Event = asyncio.Event()
        self._worker_task: asyncio.Task[None] | None = None

    async def startup(self) -> None:
        try:
            await self._redis.xgroup_create(
                name=self._stream_key, groupname=self._group_name, id="0", mkstream=True
            )
        except RedisError as e:
            if "BUSYGROUP" not in str(e):
                raise
        self._worker_task = asyncio.create_task(self._consume())

    """理解文档:Redis Stream 消费系统完全指南.md"""

    async def _consume(self) -> None:
        cursor = "0-0"
        while not self._stop_event.is_set():
            try:
                cursor, pending, deleted = await self._redis.xautoclaim(
                    self._stream_key,
                    self._group_name,
                    self._consumer_name,
                    min_idle_time=1000,
                    start_id=cursor,
                    count=10,
                )
                if deleted:
                    logger.error("待确认消息已从 Stream 删除，无法恢复：ids=%s", deleted)
                for message_id, fields in pending:
                    logger.warning("Pending message: id=%s", message_id)
                    await self._handle_message(message_id, fields)
                messages = cast(
                    StreamResponse,
                    await self._redis.xreadgroup(
                        self._group_name,
                        self._consumer_name,
                        {self._stream_key: ">"},
                        count=10,
                        block=1000,
                    ),
                )
                for _, entries in messages:
                    for message_id, fields in entries:
                        await self._handle_message(message_id, fields)
            except RedisError as e:
                logger.error(f"Redis error while consuming stream: {e}")
                await asyncio.sleep(1)

    async def _handle_message(self, message_id: str, fields: dict[str, str]) -> None:
        payload: Any = None
        try:
            payload = json.loads(fields["payload"])
            await self._handler(payload)
            # await self._redis.xack(self._stream_key, self._group_name, message_id)
        except Exception as e:
            logger.exception("Stream task failed: message_id=%s", message_id)
            tenant_id = payload.get("tenant_id") if isinstance(payload, dict) else None
            owner = (
                ResourceOwner(scope="tenant", tenant_id=tenant_id)
                if isinstance(tenant_id, str) and tenant_id
                else ResourceOwner(scope="platform")
            )
            replayable = isinstance(payload, dict)
            await dead_letter_queue.add(
                task_id=message_id,
                payload=payload if replayable else {"fields": fields},
                failure_reason=str(e),
                owner=owner,
                handler_key="ingest" if replayable else NON_REPLAYABLE_HANDLER,
                stream_source=(self._stream_key, self._group_name, message_id),
            )
        await self._redis.xack(self._stream_key, self._group_name, message_id)
