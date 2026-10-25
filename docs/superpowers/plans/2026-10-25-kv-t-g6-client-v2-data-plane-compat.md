# KV-T G6 (#170) — coord-client v2 数据面兼容层 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 Agent 内部的 v1 形状调用（`client.kv()` / `txn()` / `watch()` / `lease()`）在 v2 集群上落到 `coord.*.v2`，从而把 15 个依赖数据面的服务整体迁到 v2，而不改它们的业务逻辑；`DataPlane::V1`（默认）路径字节级不变。

**Architecture:** `coord-client` 新增 `Config::data_plane` 开关（默认 V1）。V2 模式下：

- **读**：`Kv::Range` → `coord.kv.v2.Range`（`read_ts=0` = 本地 safe_ts，显式 revision 原样）；
- **写**：`Put`/`Delete` → `coord.txn.v2`（`Begin(start_ts=0)` + `Commit` 单写集），不直接取 TSO：
  写时间戳由 Sequencer 分配，避免要求调用方持有 `data:tso:allocate`，也避开直写越过
  pending `CommitDecision` 的次序窗口（#244）；
- **CAS**：v1 `Txn(compare/success/failure)` → 在 v2 会话里 `Read` 比较键（服务端记录读集）→
  本地判定 → 选 success/failure 写集 → `Commit`；`TXN_CONFLICT` 按 `Config::max_retries` 重跑；
- **Watch**：v1 空前缀语义（空前缀 = 前缀订阅）→ v2 半开区间（`prefix_successor`）；v2
  原子事务批映射为一条 v1 `WatchEvent`（`revision = commit_ts`）；
- **read-your-writes**：本客户端已提交的最大 `commit_ts` 记为读下限，`revision=0`
  的 `Range` 用它作 `read_ts`（显式 revision 不被抬高），等待 apply 而不是读到旧快照；
- **Lease**：`Grant/Revoke/KeepAlive/TimeToLive` → `coord.lease.v2`；后台续期在 V2 下按单次
  `KeepAlive` 周期任务实现（v2 没有 v1 的长流续期）。

**Spec:** issue #170（Agent 数据面 v2 迁移）、ADR-0015 D7/D12/D13；v2 契约
`apis/contracts/proto/coord/{kv,txn,watch,lease}/v2/*.proto`；auth 前置 #253（scoped
`data:txn:execute` 可 Begin）。

## Global Constraints

- `DataPlane::V1`（默认）行为不变：所有分支先判模式，V1 走原代码路径。
- V2 模式只解决**内部调用**的形状兼容；Agent 对外 v1 数据面在 v2 模式下仍必须冻结
  （#173/#170 的下一切片负责代理层 fail-closed），本切片不改 `proxy.rs`。
- 写路径统一走 `coord.txn.v2`：不新增 TSO 授权要求；`Delete` 的 `deleted` 计数用同一
  会话的 range 读取得（读集冲突 → 重试），不返回伪造计数。
- v1 的 `Txn` 内 `RequestRange` 在 V2 下支持（读集 + 本地写覆盖），`revision != 0` 的
  历史读 fail-closed（v2 会话只有 `start_ts` 快照）。
- Watch 前缀语义必须与 v1 一致：空 `range_end` = 前缀；全部 `0xFF`/空前缀无后继时
  fail-closed（不静默退化成单键或无界扫描）。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`；临时文件只放
  `/tmp/coord-*`。合入前跑 AGENTS.md 门禁与 `scripts/check-doc-refs.sh`。

## Tasks

### Task 1: `DataPlane` 开关

**Files:** Modify `coord-client/src/config.rs`、`coord-client/src/lib.rs`

- [x] `pub enum DataPlane { V1, V2 }`（`Default = V1`，`serde` 不需要）；
- [x] `Config { pub data_plane: DataPlane, ... }` + `with_data_plane`；
- [x] `Client::data_plane()` 内部读取；
- [x] 单测：默认 V1、builder 覆盖。

### Task 2: `v2_compat` 模块

**Files:** Create `coord-client/src/v2_compat.rs`、Modify `coord-client/src/lib.rs`

**Interfaces:**

```rust
pub(crate) async fn kv_put(client: &Client, key: &[u8], value: &[u8], lease_id: i64,
                           request_id: &[u8]) -> Result<u64>;
pub(crate) async fn kv_delete(client: &Client, key: &[u8], range_end: &[u8],
                              request_id: &[u8]) -> Result<(i64, u64)>;
pub(crate) async fn txn(client: &Client, compares: Vec<Compare>, success: Vec<RequestOp>,
                        failure: Vec<RequestOp>, request_id: Vec<u8>) -> Result<TxnResponse>;
pub(crate) async fn watch(client: &Client, key: &[u8], range_end: &[u8],
                          start_revision: i64, prev_kv: bool)
    -> Result<mpsc::Receiver<Result<WatchEvent>>>;
```

- [ ] 读：`Client::kv_v2().range(...)` 映射 v1 `(kvs,count,revision)`；
- [ ] 写：`begin(0, 0)` → `commit(vec![Mutation::Put/Delete/DeleteRange], request_id)`；
  commit 冲突重试；返回 `commit_ts`；
- [ ] 删除计数：同一会话 `read(key, range_end)` 取命中数（`keys_only`）后再提交；
  范围读受 `max_range` 限制（超限把服务端错误原样上抛）；
- [ ] CAS：比较键去重 → `session.read` 逐 key（absent = `version=0`/`revision=0`/空值）→
  按 `Equal/Greater/Less/NotEqual` × `Version/Value/ModRev` 判定 → 选 success/failure →
  把 `RequestPut/RequestDelete` 转为 `Mutation`、`RequestRange` 在本地读+写覆盖上求值 →
  `Commit`；冲突重试到 `Config::max_retries`，耗尽返回 `Error::V2{TxnConflict}`；
- [ ] `TxnResponse { succeeded, responses, revision }`：Put/Delete 的 revision = commit_ts，
  `prev_kv` 仅在 v1 请求 `prev_kv=true` 时用会话读补齐；
- [ ] Watch：`range_end` 空 → `prefix_successor(key)`（空 key = 全 keyspace，与 v2
  `key="" & range_end=""` 同义）；`start_revision` 转 u64；v2 `WatchV2Batch.events`
  逐事件转 v1 `WatchEvent`（`type` = 有 PUT → Put，否则 Delete；`kvs` 带
  `mod_revision=commit_ts`）；`HistoryUnavailable` 转 v1 `EventType::HistoryUnavailable`
  后结束流；`prev_kv=true` 时逐 change 发一条 v1 事件（v1 事件只承载一个 prev_kv）；
- [x] Lease：grant/revoke/keep_alive/time_to_live 直接映射；后台续期 = 周期单次
  `keep_alive`（间隔 = TTL/3，下限 1s），失败只记日志并在下一拍重试，停止信号退出。

### Task 3: 接线四个客户端

**Files:** Modify `coord-client/src/client.rs`

- [x] `KvClient::{put_full,range_full,range_with_lease_full,delete_full}`：V2 分支；
- [x] `TxnClient::txn_full`：V2 分支；
- [x] `WatchClient::watch_full`：V2 分支；
- [x] `LeaseClient::{grant_with_id,revoke,keep_alive,keep_alive_background}`：V2 分支；
- [x] V1 分支保持逐字不变。

### Task 4: 判据

**Files:** Create `coord-client/tests/v2_data_plane_test.rs`

- [x] 假 v2 服务（TSO 不需要；KV Range、Txn Begin/Read/Commit、Watch 流、Lease）；
- [x] KV：V2 Put 走 Txn（不调用 TSO/KV.Put）；Range 映射 kv/lease/version；
  Delete 计数来自会话读；`read_ts` 透传；
- [x] CAS：`Version==0` 成功写入；比较失败走 failure 写集；注入一次 `TXN_CONFLICT`
  验证重跑并最终提交；`RequestRange` 见到同一事务先前的 Put；
- [x] Watch：空前缀 → v2 区间上界；原子批映射；Delete 类型；`HistoryUnavailable` 事件；
- [x] Lease：grant/revoke/keep_alive/后台续期（短间隔）调用次数；
- [x] V1 模式：现有 `integration_test` 不受影响（模式默认 V1）。

### Task 5: Agent 装配与 v1 冻结

**Files:** Modify `coord-agent/src/lib.rs`、`coord-agent/src/proxy.rs`、`scripts/ci-java-it-cluster.sh`、`coord-java-sdk/src/test/.../KvTV2ClusterIntegrationTest.java`；Create `coord-agent/src/frozen.rs`、`coord-agent/tests/agent_v2_mode_test.rs`

- [x] `AgentConfig::data_plane`（`v1` 默认 / `v2`），`AgentInner::new_with_data_plane`；
- [x] V2 模式下 v1 `Kv/Txn/Lease/Watch/Storage` 代理替换为本地冻结代理（与服务端
  同一 `FAILED_PRECONDITION` 文案/码），v2 代理与 Maintenance 不变；
- [x] readiness：V2 模式不再用 TCP 探针，改为 `coord.kv.v2.Range` 探测（v1-only
  集群 → NOT_READY，不把「TCP 连上」误报成就绪）；
- [x] 开发集群 v2 模式的 agent 配置切到 `data_plane = "v2"`、`idgen_mode = "segment"`；
- [x] Java 真实集群用例补 v1 形状服务（registry/config/lock/idgen + config watch）；
- [x] 修复号段模式 `allocate_segment` 未消费首个 ID 的重复 ID（Java 用例发现）。

## Verification Anchors

| 行为 | 证据 |
|:--|:--|
| 默认 V1 不变 | `config::tests::test_data_plane_defaults_to_v1` |
| 写不直连 KV/TSO | `v2_data_plane_test::v1_put_uses_v2_txn_not_direct_kv` |
| CAS 语义 | `v1_txn_compare_then_put_uses_v2_session` |
| 冲突重跑 | `txn_retries_on_conflict` |
| 前缀 Watch | `watch_empty_range_end_uses_prefix_successor` |
| Lease 后台续期 | `v2_keep_alive_background_renews` |
| Agent v1 冻结 | `agent_v2_mode_test::v1_data_plane_is_frozen_and_v2_stays_registered` |
| v1 形状服务在 v2 集群可用 | `KvTV2ClusterIntegrationTest#v1ShapedServicesSurviveOnTheV2DataPlane`（真实 server+agent） |
| 全量 | `cargo test -p coord-client`、`cargo test -p coord-agent`、workspace、CI |

## Residual（不在本切片）

- 其余 11 个服务（workflow/approval/scheduler/pki/transit/event/policy/featureflags/
  cache/scheduler/rate limiter）的逐服务 v2 冒烟；#170 仍 open 到 16 服务矩阵齐。
- `Txn` 内 `RequestRange` 的历史 revision 读：fail-closed。
- v1 `Put` 的全局 request_id 幂等在 v2 下退化为会话内幂等（会话丢失后可能重放一次写）；
  内部服务不依赖该弱保证，外部 SDK 直写请走 v2 KV/Txn。
