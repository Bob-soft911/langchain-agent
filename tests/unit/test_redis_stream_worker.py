import json
from functools import partial
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from redis.exceptions import ConnectionError as RedisConnectionError

from app.infrastructure.queue import redis_stream
from app.infrastructure.queue.dlq_handler import (
    NON_REPLAYABLE_HANDLER,
    DeadLetterQueue,
    DLQEntry,
    DLQStatus,
)
from app.infrastructure.queue.redis_stream import RedisStreamWorker, TaskHandler
from app.infrastructure.security.resource_scope import ResourceScopeClass


class FakeRedis:
    def __init__(self) -> None:
        self.acked: list[tuple[str, str, str]] = []

    async def xack(self, stream: str, group: str, message_id: str) -> int:
        self.acked.append((stream, group, message_id))
        return 1


class FakeDLQ:
    def __init__(self) -> None:
        self.entries: list[dict[str, Any]] = []

    async def add(
        self,
        task_id: str,
        payload: dict[str, Any],
        failure_reason: str,
        *,
        owner: Any,
        handler_key: str,
        stream_source: tuple[str, str, str] | None = None,
    ) -> None:
        self.entries.append(
            {
                "task_id": task_id,
                "payload": payload,
                "reason": failure_reason,
                "owner": owner,
                "handler_key": handler_key,
                "stream_source": stream_source,
            }
        )


@pytest.fixture
def dlq(monkeypatch: pytest.MonkeyPatch) -> FakeDLQ:
    fake = FakeDLQ()
    monkeypatch.setattr(redis_stream, "dead_letter_queue", fake)
    return fake


def _worker(redis: FakeRedis, handler: TaskHandler) -> RedisStreamWorker:
    return RedisStreamWorker(redis, handler=handler, consumer_name="test")  # type: ignore[arg-type]


@pytest.mark.asyncio
async def test_success_acks_and_skips_dlq(dlq: FakeDLQ) -> None:
    redis, received = FakeRedis(), []

    async def handler(payload: dict[str, Any]) -> None:
        received.append(payload)

    await _worker(redis, handler)._handle_message("1-0", {"payload": json.dumps({"a": 1})})

    assert received == [{"a": 1}]
    assert redis.acked == [("tasks:ingest", "ingest-workers", "1-0")]
    assert dlq.entries == []


@pytest.mark.asyncio
async def test_handler_failure_goes_to_dlq_and_is_acked(dlq: FakeDLQ) -> None:
    redis = FakeRedis()

    async def handler(_payload: dict[str, Any]) -> None:
        raise RuntimeError("downstream down")

    await _worker(redis, handler)._handle_message(
        "2-0", {"payload": json.dumps({"tenant_id": "tenant-a"})}
    )

    assert [entry["task_id"] for entry in dlq.entries] == ["2-0"]
    assert "downstream down" in dlq.entries[0]["reason"]
    assert dlq.entries[0]["handler_key"] == "ingest"
    assert dlq.entries[0]["owner"] == ResourceScopeClass(scope="tenant", tenant_id="tenant-a")
    assert dlq.entries[0]["stream_source"] == ("tasks:ingest", "ingest-workers", "2-0")
    assert len(redis.acked) == 1  # 失败后 ACK，避免毒消息被反复投递


@pytest.mark.asyncio
@pytest.mark.parametrize("fields", [{}, {"payload": "not-json"}])
async def test_malformed_message_goes_to_dlq(dlq: FakeDLQ, fields: dict[str, str]) -> None:
    redis = FakeRedis()

    async def handler(_payload: dict[str, Any]) -> None:
        raise AssertionError("handler must not run for malformed messages")

    await _worker(redis, handler)._handle_message("3-0", fields)

    assert len(dlq.entries) == 1
    assert dlq.entries[0]["handler_key"] == NON_REPLAYABLE_HANDLER
    assert dlq.entries[0]["payload"] == {"fields": fields}
    assert dlq.entries[0]["owner"] == ResourceScopeClass(scope="platform")
    assert len(redis.acked) == 1


@pytest.mark.asyncio
async def test_failed_dlq_write_is_not_acked_until_claimed_message_is_saved(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = DeadLetterQueue()
    monkeypatch.setattr(redis_stream, "dead_letter_queue", queue)
    backend = MagicMock()
    monkeypatch.setattr(queue, "redis", backend)
    pipeline = backend.pipeline.return_value.__aenter__.return_value
    pipeline.execute.side_effect = RedisConnectionError("DLQ unavailable")
    redis = FakeRedis()
    worker = _worker(redis, AsyncMock(side_effect=RuntimeError("ingest failed")))
    fields = {"payload": '{"tenant_id":"tenant-a"}'}

    with pytest.raises(RedisConnectionError, match="DLQ unavailable"):
        await worker._handle_message("4-0", fields)

    assert redis.acked == []
    assert await queue.list_pending(owner=None) == []

    # Redis returns the stored JSON from the final GET in the transaction.
    async def execute() -> list[str]:
        assert redis.acked == []
        return [pipeline.set.call_args.args[1]]

    pipeline.execute.side_effect = execute
    monkeypatch.setattr(
        redis, "xautoclaim", AsyncMock(return_value=["0-0", [("4-0", fields)], []]), raising=False
    )

    async def finish_read(*_args: Any, **_kwargs: Any) -> list[object]:
        worker._stop_event.set()
        return []

    monkeypatch.setattr(redis, "xreadgroup", finish_read, raising=False)
    await worker._consume()

    assert redis.acked == [("tasks:ingest", "ingest-workers", "4-0")]
    assert [entry.task_id for entry in await queue.list_pending(owner=None)] == ["4-0"]


@pytest.mark.asyncio
async def test_ack_failure_after_success_does_not_create_dlq_entry(
    dlq: FakeDLQ, monkeypatch: pytest.MonkeyPatch
) -> None:
    redis = FakeRedis()
    monkeypatch.setattr(redis, "xack", AsyncMock(side_effect=RedisConnectionError("ACK failed")))
    handler = AsyncMock()

    with pytest.raises(RedisConnectionError, match="ACK failed"):
        await _worker(redis, handler)._handle_message(
            "5-0", {"payload": '{"tenant_id":"tenant-a"}'}
        )

    handler.assert_awaited_once_with({"tenant_id": "tenant-a"})
    assert dlq.entries == []


@pytest.mark.asyncio
async def test_redis_redelivery_preserves_existing_dlq_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    queue = DeadLetterQueue()
    backend = MagicMock()
    monkeypatch.setattr(queue, "redis", backend)
    pipeline = backend.pipeline.return_value.__aenter__.return_value
    entries: dict[str, str] = {}

    async def execute() -> list[str]:
        key, value = pipeline.set.call_args.args
        if not pipeline.set.call_args.kwargs.get("nx") or key not in entries:
            entries[key] = value
        return [entries[key]]

    pipeline.execute.side_effect = execute
    add = partial(
        queue.add,
        "6-0",
        {},
        owner=ResourceScopeClass(scope="tenant", tenant_id="tenant-a"),
        handler_key="ingest",
        stream_source=("tasks:ingest", "ingest-workers", "6-0"),
    )
    original = await add("ingest failed")
    (key,) = entries
    stored = DLQEntry.from_dict(json.loads(entries[key]))
    stored.status = DLQStatus.RESOLVED
    stored.retry_count = 1
    entries[key] = json.dumps(stored.to_dict())

    redelivery = await add("ingest failed again")

    assert redelivery.dlq_id == original.dlq_id
    assert len(entries) == 1
    assert redelivery.status == DLQStatus.RESOLVED
    assert redelivery.retry_count == 1
