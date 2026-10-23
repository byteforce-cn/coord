# KV-T G6 (#172) Rust client Lease/Storage v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** `coord-client` 补齐 `coord.lease.v2` 与 `coord.storage.v2` 的 v2 方法，使 SDK 侧能用 `commit_ts`/`read_ts` 语义调用服务端已实现的租约与对象面，而不是退回 v1 `coord.lease` / `coord.storage`。

**Architecture:** 两个 v2 子客户端各占一个模块，由 `Client::lease_v2()` / `Client::storage_v2()` 构造；每次尝试都从 `Client` 读取当前 routing epoch 附到 metadata（0 = 未知不发，与服务端「缺失不校验」一致）。Read/流开启路径只在 `EPOCH_STALE` 上有界重试；写路径复用既有 `execute_write_with_retry`（已带 epoch 学习与 leader hint 处理）。`read_ts`/`commit_ts` 在本地先做 `u64 → i64` 的 wire 范围校验，越界直接 `InvalidArgument`，不发请求。

**Tech Stack:** Rust / tonic 0.14 / tokio。

**Spec:** issue #172（Rust client v2 GA 的 Lease/Storage 部分）；contracts `coord/lease/v2/lease.proto`、`coord/storage/v2/storage.proto`；服务端实现 `coord-server/src/kvt/lease.rs`、`coord-server/src/kvt/services.rs`（`coord.storage.v2`）；上一刀 `docs/superpowers/plans/2026-10-23-kv-t-g6-client-routing-epoch.md`。

## Global Constraints

- `[kv_t].enabled=false`（v1）行为不变：v2 子客户端只在显式调用时生效；v1 `lease()` / `storage()` 不读也不发 epoch。
- 不新增 wire 字段/错误码；v2 请求的 `commit_ts`/`read_ts` 由调用方显式传入，服务端分配口径不变（storage Put 的 `commit_ts` 仍由服务端返回）。
- 重试有界（`Config::max_retries`）；`EPOCH_STALE` 之外的错误不改变既有分类（`NOT_FOUND`/`FAILED_PRECONDITION` 等照常透传）。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings

1. **0 = 未知，不带 metadata。** 复用 `Client::attach_routing_epoch`；学到非 0 版本后每条重试都重新 attach（重试必须带新版本，否则被拒是确定的）。
2. **读/流开启只重试 `EPOCH_STALE`。** 与 `KvV2Client::range` 同口径：learn → 退避 → 重试；其他错误立即映射返回。`Get`/`Stat`/`TimeToLive` 无副作用，重放在入口守卫处即可。
3. **写/流上传复用 `execute_write_with_retry`。** `EPOCH_STALE` 在服务端入口守卫处返回，此时客户端流尚未被消费，整段重放不会留下半成品；`Put` 的「已存在」仍按 `ALREADY_EXISTS` 中止，不做幂等猜测。
4. **本地先校验时间戳。** `read_ts`/`commit_ts > i64::MAX` 无法上 wire，直接 `InvalidArgument`（与 `KvV2Client::ts_to_i64` 同一判据），不把「负值/回绕」留给服务端。
5. **不做后台 KeepAlive 循环。** 本切片只提供单次 `KeepAlive`（与 v1 `LeaseClient::keep_alive` 对齐）；绑定 key 的后台续约归 #170 的 Agent 迁移切片（需要生命周期/停止语义，另立计划）。
6. **不做 Watch 首帧 epoch、Java SDK、跨语言 e2e。** 它们仍是 #172 的 open 项，不在本切片。

## Wire 映射

| 客户端方法 | RPC | 关键字段 |
|:--|:--|:--|
| `grant(ttl, lease_id, request_id)` | `LeaseGrant` | `ttl_seconds`、`lease_id`（0=自动）、`request_id` |
| `revoke(lease_id, request_id)` | `LeaseRevoke` | `commit_ts = 0` = 无新决策（幂等重放） |
| `keep_alive(lease_id)` | `LeaseKeepAlive`（双向流） | 发送一帧，读一帧 `ttl_seconds` |
| `time_to_live(lease_id, keys)` | `LeaseTimeToLive` | `ttl_seconds <= 0` = 已过期；`keys` 受服务端上限 |
| `put`/`put_chunked` | `Storage.Put`（客户端流） | 首帧 `meta(total_size)`，后续 chunk；返回 `commit_ts` |
| `get(bucket, id, read_ts)` | `Storage.Get`（服务端流） | `read_ts=0` = safe_ts；首帧 stat + chunk |
| `stat(bucket, id, read_ts)` | `Storage.Stat` | `NOT_FOUND` → `Ok(None)`（与 v1 一致） |
| `delete(bucket, id, request_id)` | `Storage.Delete` | 返回 `(deleted, commit_ts)` |

## File Structure

- Create: `coord-client/src/lease_v2.rs` — `LeaseV2Client`（Grant/Revoke/KeepAlive/TimeToLive）+ 结果类型。
- Create: `coord-client/src/storage_v2.rs` — `StorageV2Client`（Put/Get/Stat/Delete）+ 结果类型，chunk 默认值复用 `DEFAULT_OBJECT_CHUNK_SIZE`。
- Modify: `coord-client/src/client.rs` — `Client::lease_v2()` / `Client::storage_v2()`；抽出 `ts_to_i64` 与「只重试 EPOCH_STALE」的 `execute_v2_read_with_epoch_retry`。
- Modify: `coord-client/src/lib.rs` — 模块与类型导出。
- Create: `coord-client/tests/lease_storage_v2_test.rs` — mock `coord.lease.v2` / `coord.storage.v2`。
- Modify: `docs/adr/0015-kv-t.md` — D12.5 补客户端 Lease/Storage 的 epoch 口径（一句话，不复制实现）。

## Tasks

### Task 1: Client 共享辅助

- [x] **Step 1: `ts_to_i64` 提为 crate 级**

`pub(crate) fn ts_to_i64(name: &str, ts: u64) -> Result<i64>`；`KvV2Client` 改用它，行为不变。

- [x] **Step 2: `execute_v2_read_with_epoch_retry`**

`FnMut(AuthedChannel) -> Fut`（closure 内自建 stub 并 attach epoch）；`EPOCH_STALE` → learn + `RetryState` 退避重试；其他错误 `from_status`。

- [x] **Step 3: 访问器与导出**

`Client::lease_v2()` / `Client::storage_v2()`；`lib.rs` 导出 `LeaseV2Client`、`LeaseV2Grant`、`LeaseV2State`、`StorageV2Client`、`PutObjectV2Result`、`ObjectDataV2`。

### Task 2: `coord.lease.v2`

- [x] **Step 1: 四个 RPC**

Grant/Revoke 走 `execute_write_with_retry`；KeepAlive 打开双向流发一帧读一帧；TimeToLive unary 走 read 重试。

- [x] **Step 2: 映射**

`LeaseGrantResponse.granted_ts`/`commit_ts` 用 `u64`（响应负值 → `Internal`）；`TimeToLive` 的 `NOT_FOUND` 透传为 `Error::NotFound`。

### Task 3: `coord.storage.v2`

- [x] **Step 1: Put**

`put`/`put_chunked`：chunk 上限与 v1 同判据；首帧 meta + 逐 chunk；返回 `PutObjectV2Result { commit_ts, size, chunks }`。

- [x] **Step 2: Get/Stat/Delete**

Get 首帧 stat + 拼 chunk；Stat `NOT_FOUND → Ok(None)`；Delete 带 `request_id` 返回 `(deleted, commit_ts)`。

### Task 4: 测试矩阵

- [x] **Step 1: Lease**

grant 的 wire 字段与返回值；首次 `EPOCH_STALE` 后重试带新 epoch、后续直接携带；`TimeToLive` 的 `NOT_FOUND` 映射；KeepAlive 只发一帧并读回 TTL。

- [x] **Step 2: Storage**

Put 的 meta/chunk 顺序与 `commit_ts`；首次 `EPOCH_STALE` 后整段重放；Get 的 `read_ts` 与数据拼装；Stat `NOT_FOUND → None`；Delete 的 `request_id` 与 `commit_ts`。

- [x] **Step 3: 本地校验**

`read_ts = u64::MAX` → `InvalidArgument`，mock 服务器零调用。

### Task 5: 文档与交付

- [x] **Step 1: ADR + 文档引用**

D12.5 补一句：Lease/Storage v2 客户端同样 attach/learn epoch；`bash scripts/check-doc-refs.sh` 为 0 悬空。

- [x] **Step 2: 门禁**

`cargo fmt --all -- --check`；`cargo clippy --workspace --lib --bins -- -D warnings`；`bash scripts/check-panics.sh`；`cargo audit`；`cargo deny check`；`cargo test -p coord-client`。

- [x] **Step 3: 交付**

PR（引用 #172）→ CI 全绿 → merge（`--merge`）→ 删分支；#172 回填进展并列出剩余 open 项。

## Review Focus

- 首次请求是否仍旧**不带** epoch（0 = 未知）？学到后每次重试是否带新版本？
- 读路径的重试是否严格限定在 `EPOCH_STALE`（不把 `NOT_FOUND`/`FAILED_PRECONDITION` 也重试）？
- Storage Put 的流是否在每次重试时**重建**（不能复用已被消费的 `messages`）？
- `commit_ts`/`read_ts` 的 `u64 → i64` 是否全部在本地校验，且错误类型是 `InvalidArgument`？
- `TimeToLive(keys=true)` 的 `keys` 顺序/上限是否原样透传（客户端不截断）？
