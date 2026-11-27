# Redis Stream 消费系统完全指南（极简版）

> 生产可用写法 + 必改坑点，其余从略。

## 一、结构与原则

```
XADD → Stream
   ├── XREADGROUP ">"   新消息
   └── XAUTOCLAIM       idle 超时的 pending
        ▼  _consume → _handle_message
           ├── 成功 → XACK
           └── 失败 → DLQ.add(stream_source=(stream, group, msg_id)) → XACK
```

1. **DLQ 是权威存储，Stream 只是投递通道**。
2. 先写 DLQ 再 ACK；中间崩溃消息留在 PEL，会被重投，不会丢。
3. 重复投递靠「稳定 ID + SET NX」吸收：不重复建条目、不覆盖已 RESOLVED。

## 二、语义要点

- `>` 只读未投递过的新消息。XACK 后再读永远拿不到它，但消息本体仍在 Stream（靠 XTRIM/XDEL 删）。
- PEL = 已投递未 ACK，每个 consumer 各一份；`min_idle_time` 是空闲时长，不是重试次数。
- `XAUTOCLAIM` 转移 idle 超时消息的所有权，是崩溃自愈的唯一手段。
- 消费协程从 `startup()` 跑到 `stop_event` 置位，`block` 必须有限（1000ms）。

## 三、消费循环（唯一推荐）

```python
async def _consume(self) -> None:
    cursor = "0-0"
    while not self._stop_event.is_set():
        try:
            cursor, pending, deleted = await self._redis.xautoclaim(
                self._stream_key, self._group_name, self._consumer_name,
                min_idle_time=60_000, start_id=cursor, count=10)
            if deleted:
                logger.error("PEL 幽灵消息：%s", deleted)
            for message_id, fields in pending:
                await self._handle_message(message_id, fields)

            messages = await self._redis.xreadgroup(
                self._group_name, self._consumer_name,
                {self._stream_key: ">"}, count=10, block=1000)
            for _, entries in messages:
                for message_id, fields in entries:
                    await self._handle_message(message_id, fields)
        except RedisError:
            logger.exception("Stream Redis 失败，稍后重试")
            await asyncio.sleep(1)
```

- 只写 `">"` 的版本，单实例崩溃后 pending 永久僵死，不能上生产。
- `deleted` 是消息体已删、PEL 只剩记录的幽灵 ID，建议顺手 XACK，否则 PEL 膨胀。
- 60s idle 才允许抢；认领回来和新消息走同一个 `_handle_message`。
- 优化方向：先新消息后 pending；累计 `attempts` 超阈值直接 ACK 转人工。

## 四、处理与转存

```python
async def _handle_message(self, message_id: str, fields: dict[str, str]) -> None:
    payload: Any = None
    try:
        payload = json.loads(fields["payload"])
        await self._handler(payload)
        await self._redis.xack(self._stream_key, self._group_name, message_id)
    except Exception as e:
        logger.exception("Stream task failed: %s", message_id)
        tenant_id = payload.get("tenant_id") if isinstance(payload, dict) else None
        owner = (ResourceOwner(scope="tenant", tenant_id=tenant_id)
                 if isinstance(tenant_id, str) and tenant_id
                 else ResourceOwner(scope="platform"))
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
```

- `json.loads` 失败时 `payload` 仍是 `None`，取值必须 `isinstance` 保护。
- JSON 非法只能存原始 `fields`，绑 `NON_REPLAYABLE_HANDLER` 交人工，重放器按此 key 过滤。
- 顺序固定「DLQ 成功 → ACK」，反序一旦崩溃就是彻底丢消息。

## 五、幂等

```python
if stream_source is None:
    dlq_id = f"dlq_{uuid.uuid4().hex[:12]}"
else:
    identity = json.dumps(stream_source, separators=(",", ":"))
    dlq_id = f"dlq_stream_{hashlib.sha256(identity.encode()).hexdigest()}"
```

| | `stream_source != None` | 无 `stream_source` |
|---|---|---|
| Redis | `SET NX` 不覆盖 | 普通 SET 可覆盖 |
| 顺序 | Redis → 内存 | 内存 → Redis |
| 缓存 | `setdefault` | `[]=` 覆盖 |
| 风险 | 缓存可能缺数据 | **DLQ 可能丢失** |

**ACK 前崩溃为什么安全**：同 `stream_source` → 同 `dlq_id` → `SET NX` 失败 → 末尾 `GET` 拿到旧条目 → `setdefault` 不覆盖 → XACK。不新增条目，也不把 RESOLVED 打回 PENDING。

## 六、存储

```python
key, index_key = self._entry_keys(entry.dlq_id), self._redis_index_key(entry.owner)
ttl = 7 * 24 * 3600
async with self.redis.pipeline(transaction=True) as pipe:
    pipe.set(key, json.dumps(entry.to_dict(), ensure_ascii=False), ex=ttl, nx=create_only)
    pipe.sadd(index_key, entry.dlq_id)
    pipe.sadd("dlq:owners", index_key)
    pipe.expire(index_key, ttl)
    pipe.get(key)
    results = await pipe.execute()
raw = results[-1]                      # None → 抛 RedisError，否则 from_dict 返回
```

| Key | 类型 | TTL |
|---|---|---|
| `dlq:item:{id}` | STRING（JSON） | 7 天 |
| `dlq:index:{owner}` | SET（该 owner 的 id） | 7 天，每次写重置 |
| `dlq:owners` | SET（所有 index key） | **无** |

`MULTI/EXEC` 连续执行但**不回滚、不做条件分支**；末尾 `GET` 是写后回读校验。
`_persist_entry` 捕获 `RedisError`/`ValueError` 打日志不抛——DLQ 落盘失败不打断主链路，代价是该条死信丢失且无告警。

## 七、必改坑

**P0-1 成功分支漏 XACK**：handler 跑完不确认，60s 后被打回来，无限重复消费。

**P0-2 `SET NX` 失败后 SADD 仍执行**：SET 返回 nil 不写入，但 SADD/EXPIRE 照样跑；索引是 SET 时不会重复，换 LIST/ZSET 就堆积。改用 Lua 原子分支：

```lua
local ok
if ARGV[3] == "1" then
    ok = redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2], 'NX')
else
    ok = redis.call('SET', KEYS[1], ARGV[1], 'EX', ARGV[2])
end
if ok then
    redis.call('SADD', KEYS[2], ARGV[4])
    redis.call('SADD', KEYS[3], KEYS[2])
    redis.call('EXPIRE', KEYS[2], ARGV[2])
end
return redis.call('GET', KEYS[1])
```

其余：

- `dlq:owners` 无 TTL、index 过期后残留 → 定时扫 `EXISTS == 0` 的成员 SREM。
- item 过期但 index 仍留死 ID → 读取时 GET 为 nil 就 SREM。
- `EXPIRE index_key` 每次写都重置 TTL，持续出死信时索引永不过期。
- 两分支异常语义不一致（一条抛、一条吞）→ 统一走 `_persist_entry`。
- `self._entries` 与 Redis 生命周期脱节 → 容量上限 + FIFO 淘汰，或读时校验 EXISTS。
- async 里用 `threading.Lock` 阻塞事件循环 → 改 `asyncio.Lock`；`setdefault` 会掩盖 Redis 最新状态 → 以 Redis 为准回写。
- 启动必须 `decode_responses=True`，否则 payload 是 bytes，表现为**全部消息进 DLQ**；`id="0"` 会重投历史，已有数据用 `"$"`。

## 八、排查

| 现象 | 处置 |
|---|---|
| 不消费 | 查 Worker 启动、`XLEN`、`XINFO GROUPS`、组名与起始 ID |
| 重复处理 | 补成功分支 XACK；`XPENDING` 反复同 ID 即漏 ACK |
| 关不掉 | `block` 改 1000，`wait_for(30)` |
| PEL 膨胀 | 幽灵 ID 也 XACK，加 XAUTOCLAIM 重试上限 |
| DLQ 堆积 | 修业务错误 + 惰性清理索引 |

## 九、基础速查

- **落盘**：`write → flush（OS cache）→ os.fsync（磁盘）`；只 flush 掉电仍丢，fsync 阻塞要批量。
- **防篡改**：`pop("entry_hash")` 防循环，`sort_keys=True`，两端 `ensure_ascii` 一致。
- **滚动日志**：`rename` 原子，但旧句柄指向归档文件，必须关掉重开。
- **异步判定**：`iscoroutine` 漏 Future/Task，用 `inspect.isawaitable` 并 `result = await result`。
- **不可变更新**：`replace(entry, status=...)` 新对象不改原值，浅拷贝；Pydantic 用 `model_copy`。
- **分片 ID**：`sha256(tenant:source:digest:i)` 可复现，但改切分或局部更新即全失效。
- **空值**：`is None` 只匹配 None；`if not x` 连 `[]`/`""`/`0` 都算空。
- **鉴权**：`Annotated[Payload, Depends(require_tenant_roles(*ROLES))]` 是 OR 语义，业务层仍需校验资源租户一致。

## 十、上线检查

- [ ] 成功分支有 XACK，「先 DLQ 后 ACK」
- [ ] Stream 消息走稳定 ID + `SET NX`，索引用 Lua 原子维护
- [ ] owners 与死 ID 有清理；缓存有上限 + `asyncio.Lock`
- [ ] XAUTOCLAIM 有重试上限；`decode_responses=True`；能优雅退出

> （注：部分内容由豆包 AI 生成）