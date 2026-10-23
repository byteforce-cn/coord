# KV-T G5 (#167) 跨协调节点会话 drain Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** split/merge 搬迁前的区间 drain 不再只等本节点 `TxnSessions`：协调节点把 drain 扇出到 region 0 成员里的每个节点，等所有**可达**节点的相交会话 commit/abort/TTL 到期后才导出。

**Architecture:** 复用 `SplitDrain` 端口与节点本地 `TxnSessions` 的 drain 判据。新增一条**节点间 RPC**（raft 端口，与 `SubmitPdOp` 同一条认证/连接池路径），请求携带区间与超时，接收方用本节点 `TxnSessions` 等本地会话收尾并回执。执行器所在节点构造 `ClusterSplitDrain`：本地 drain + 对 region 0 成员逐个扇出，全部成功才算 drain 完成。会话不复制（D12.2），因此 drain 状态不需要持久化/复制；RPC 结束即释放本地 drain 标记。

**Tech Stack:** Rust / openraft / tonic / tokio / postcard（节点间 raft 载荷编码）。

**Spec:** issue #167「跨协调节点会话 drain」残余、issue #150 的 drain 契约、ADR-0015 D6/D12.2；上一刀 `docs/superpowers/plans/2026-10-22-kv-t-g5-lease-cleanup-fence.md`（裁定 4 明确本项为独立残余）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时不得改变 v1 行为（ADR-0001）：cluster drain 只在 `[kv_t].enabled` 的 split/merge 装配里构造；v1 不注册 drain handler，也不走 split executor。
- 用户级/迁移级失败不得让 raft apply 返回 `Err`（D11.5）；本切片不改任何状态机命令。
- drain 超时必须**有界**且 fail-closed：可达节点上仍有相交会话 → 整次迁移失败、intent 保留重试，不得在未 drain 的区间上导出。
- 会话只活在协调节点内存（D12.2）：不新增复制状态、不持久化 drain 标记；节点失联 = 其会话失效。
- 不新增 v2 数据面错误码、不改公开 proto（`DrainRange` 只在内部 `coord-proto/src/proto/raft.proto`）。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings

1. **用 RPC 扇出，不复制 drain 状态。** 会话是节点本地内存态，drain 的唯一作用是在一段时间内等待这些本地会话结束；把标记复制进 region 0 只能解决「广播」，仍需每个节点回报本地是否已排空（ack），代价与新的状态机命令、快照/重启语义相当。RPC 请求本身即可携带「等到什么时候」，接收方在请求生命周期内持有 drain guard，超时/断连自动释放。
2. **不可达节点按「无存活会话」跳过（记 warning）；可达节点的 drain 超时是失败。** 协调节点连不上一个节点时，其会话本来就无法在一个 split 的时间窗内可靠提交（region 0 决策需要多数派）；即使它仍能服务只读，读的是父 Region 快照，cutover 后的提交由决策重放落到子 Region（#167 已验证）。把不可达当失败会让任意节点宕机时 split 永久停摆。
3. **wire 语义：`end_key` 空 = +∞；超时单位毫秒。** 与 Region 区间/现有 drain 的 `[start, end)` 语义一致（`ReadSet::intersects_drain`）。
4. **drain handler 未装配（v1 节点）→ RPC 返回 `FAILED_PRECONDITION`。** 对该节点按传输失败跳过；集群配置不一致导致的 split 失败在日志里可定位，不静默降级成本地 drain。
5. **drain 只在请求生命周期内生效。** 与现有本地 `SplitDrain::drain` 一致：等待结束后新读恢复（导出期间的新事务读父 Region 快照，提交由决策重放续跑），不把区间冻结到 cutover。
6. **peer 集合 = region 0 成员（voter + learner），排除本节点。** 任何节点都可能协调 `coord.txn.v2` 会话；成员表是唯一全节点视图。

## File Structure

- Modify: `coord-proto/src/proto/raft.proto` — 新增内部 `DrainRange` RPC。
- Modify: `coord-server/src/raft/network.rs` — `DrainRangePayload`/`DrainRangeReply`、`RaftNetworkFactoryImpl::drain_range`、`RaftRpcService::set_split_drain` + handler。
- Modify: `coord-server/src/split_merge.rs` — `DrainTransport` 端口、`ClusterSplitDrain`（本地 + 扇出），单测。
- Modify: `coord/src/main.rs` — 装配 `ClusterSplitDrain`（region 0 成员 + raft 网络工厂），并给 raft RPC 服务注册 drain handler。
- Create: `coord/tests/kv_t_g5_cross_node_drain_test.rs` — 真实 tonic/raft 端口上的 `DrainRange` 往返（成功、阻塞、未装配、不可达）。
- Modify: `docs/adr/0015-kv-t.md`、`docs/production/ops/boundaries.md` — 去掉「跨协调节点 drain」残余，写清不可达节点口径。
- Modify: `coord-server/src/server/mod.rs` — 更新 `split_drain` 的注释（本地端口，生产由 cluster drain 包装）。

## Review Focus

- 一台可达节点上仍有相交会话时，从另一台节点发起的 drain 是否会超时失败（而不是只等本节点）？
- 可达节点的 drain 超时是否被当成失败；不可达节点是否只跳过而不会让整次 split 永久卡住？
- 扇出是否覆盖 region 0 的全部成员（含 learner），且不会把 RPC 发给自己/重复等？
- drain 区间语义（空 `end` = +∞、半开区间）是否与本地 `TxnSessions` 一致？
- `[kv_t].enabled=false`、单节点（无 peer）与未装配 handler 的节点是否都不改变既有行为？

---

### Task 1: 节点间 `DrainRange` RPC

**Files:**
- Modify: `coord-proto/src/proto/raft.proto`
- Modify: `coord-server/src/raft/network.rs`

**Interfaces:**
- Consumes: `RaftNetworkFactoryImpl`（`node_addrs` / `get_or_create_client_slot` / `raft_tls_config` / `shared_secret`，见 `submit_pd_op`）、`RaftRpcService::verify_incoming`、`encode/decode_message_payload`。
- Produces:
  - `pub enum DrainRpcOutcome { Drained, Blocked(String) }`
  - `pub async fn RaftNetworkFactoryImpl::drain_range(&self, target: u64, start: &[u8], end: &[u8], timeout: Duration) -> Result<DrainRpcOutcome, String>`（`Err` = 连接/协议失败；`Ok(Blocked)` = 对端可达但 drain 超时）
  - `pub fn RaftRpcService::set_split_drain(&self, drain: Option<Arc<dyn SplitDrain>>)`
  - proto：`rpc DrainRange(RaftMessage) returns (RaftMessage);`

- [x] **Step 1: 加 proto RPC 与请求/响应载荷**

`raft.proto` 末尾（`SubmitPdOp` 之后）追加 `DrainRange`，头注说明：请求 `region_id=0`、payload = postcard(`DrainRangePayload{start_key,end_key,timeout_ms}`)、响应 = postcard(`DrainRangeReply{drained,error}`)，认证与其他 raft RPC 同口径。

`network.rs` 增加：

```rust
#[derive(serde::Serialize, serde::Deserialize)]
pub struct DrainRangePayload { pub start_key: Vec<u8>, pub end_key: Vec<u8>, pub timeout_ms: u64 }

#[derive(serde::Serialize, serde::Deserialize)]
pub struct DrainRangeReply { pub drained: bool, pub error: String }
```

- [x] **Step 2: 写失败测试（RPC 往返）**

新建 `coord/tests/kv_t_g5_cross_node_drain_test.rs`，用 `RaftRpcService::new()` + `RaftRpcServer` 起一个真实 tonic listener，工厂 `RaftNetworkFactoryImpl::new(1)` 注册该地址：

```rust
struct FakeDrain { result: Result<(), String> }   // SplitDrain impl
#[tokio::test] async fn test_drain_range_rpc_roundtrip()            // Drained
#[tokio::test] async fn test_drain_range_rpc_reports_blocked()      // Err("...") → Blocked
#[tokio::test] async fn test_drain_range_rpc_requires_handler()     // 未 set_split_drain → Err
#[tokio::test] async fn test_drain_range_rpc_unreachable()          // 未监听地址 → Err
```

先跑：`cargo test -p coord --test kv_t_g5_cross_node_drain_test`，确认编译失败（接口尚不存在）。

- [x] **Step 3: 实现出站 `drain_range` 与入站 handler**

出站仿 `submit_pd_op`：取地址 → `get_or_create_client_slot` → `RaftNetworkImpl { region_id: 0, .. }` → `build_authed_message` → `tokio::time::timeout(timeout + 1s, client.drain_range(req))` → 解码 reply：

```rust
if reply.drained { Ok(DrainRpcOutcome::Drained) } else { Ok(DrainRpcOutcome::Blocked(reply.error)) }
```

入站：

```rust
let Some(drain) = self.split_drain.read().clone() else {
    return Err(tonic::Status::failed_precondition("split drain is not assembled on this node"));
};
let result = drain.drain(&p.start_key, &p.end_key, Duration::from_millis(p.timeout_ms)).await;
// reply { drained: result.is_ok(), error: result.err().unwrap_or_default() }
```

`RaftRpcService` 新增字段 `split_drain: Arc<RwLock<Option<Arc<dyn crate::split_merge::SplitDrain>>>>`，`Clone` 共享；`set_split_drain(None)` 用于单测/未装配路径。

- [x] **Step 4: 通过**

`cargo test -p coord --test kv_t_g5_cross_node_drain_test` 全绿；`cargo test -p coord-server --lib raft::network` 不回归。

### Task 2: `ClusterSplitDrain` 扇出与失败口径

**Files:**
- Modify: `coord-server/src/split_merge.rs`

**Interfaces:**
- Consumes: `SplitDrain`（本地 `TxnSessions`）、`DrainRpcOutcome` / `RaftNetworkFactoryImpl::drain_range`。
- Produces:
  - `pub trait DrainTransport: Send + Sync { async fn drain_peer(&self, node_id: NodeID, start: &[u8], end: &[u8], timeout: Duration) -> Result<DrainRpcOutcome, String>; }`
  - `impl DrainTransport for RaftNetworkFactoryImpl`
  - `pub struct ClusterSplitDrain { local: Arc<dyn SplitDrain>, node_id: NodeID, peers: Arc<dyn Fn() -> Vec<NodeID> + Send + Sync>, transport: Arc<dyn DrainTransport> }`
  - `ClusterSplitDrain::new(...)`；`impl SplitDrain for ClusterSplitDrain`

- [x] **Step 1: 写失败测试（扇出）**

`split_merge.rs` 测试模块增加 fake `SplitDrain`（成功/阻塞）与 fake `DrainTransport`（记录调用，按 node 返回 `Drained` / `Blocked` / `Err`）：

```rust
#[tokio::test] async fn test_cluster_drain_waits_on_every_peer_including_learners()
#[tokio::test] async fn test_cluster_drain_fails_when_a_reachable_peer_times_out()
#[tokio::test] async fn test_cluster_drain_skips_unreachable_peers()
#[tokio::test] async fn test_cluster_drain_fails_when_local_sessions_remain()
#[tokio::test] async fn test_cluster_drain_single_node_uses_local_only()
```

- [x] **Step 2: 实现**

`drain`：过滤 `node_id` 自身 → `futures::future::join_all` 并发本地 + 每个 peer（每个 peer 传同一 `timeout`）→ 分类：

- `Ok(Drained)`：通过；
- `Ok(Blocked(reason))`：`Err(format!("node {id} drain failed: {reason}"))`；
- `Err(reason)`：`tracing::warn!(node_id = id, "...unreachable, treating sessions as lost")` 后跳过；
- 本地 `Err`：`Err(format!("local drain failed: {reason}"))`。

- [x] **Step 3: 通过**

`cargo test -p coord-server --lib split_merge`（含既有 split/merge 用例）。

### Task 3: 生产装配

**Files:**
- Modify: `coord/src/main.rs`
- Modify: `coord-server/src/server/mod.rs`

- [x] **Step 1: 装配 cluster drain**

在 `[kv_t]` + Multi-Raft 的 split migrator 安装处（`manager` / `lifecycle` / `region0_raft` 可见的块）：

```rust
let drain = Arc::new(coord_server::split_merge::ClusterSplitDrain::new(
    node.split_drain(),
    node_id,
    Arc::new(move || {
        let metrics = region0_raft.metrics().borrow_watched().clone();
        metrics.membership_config.nodes().map(|(id, _)| *id).collect()
    }),
    Arc::new(region_shared_factory.clone()),
));
pd.install_split_migrator(range_import_migrator(manager, &lifecycle, drain));
```

`region0_raft` 在交给 `RegionLifecycleSpec` 前 clone 一份给闭包；`WatchReceiver` 已在 main 导入。

- [x] **Step 2: 给本节点 raft RPC 服务注册 handler**

在 `node` 构造完成、`RaftRpcServer` 使用 `raft_rpc_service` 之前：

```rust
raft_rpc_service.set_split_drain(node.kvt().is_enabled().then(|| node.split_drain()));
```

未启用 `[kv_t]` 时为 `None`（RPC 返回 `FAILED_PRECONDITION`）。

- [x] **Step 3: 更新注释**

`CoordNode::split_drain` 的「只覆盖本节点」改为「本节点会话端口；生产 split/merge 由 `ClusterSplitDrain` 包装扇出」；main 的安装日志改为 `drain=cluster (local TxnSessions + region-0 members)`。

- [x] **Step 4: 编译与相关套件**

`cargo build --workspace`；`cargo test -p coord --test kv_t_g5_cross_node_drain_test`；`cargo test -p coord-server --lib split_merge`；`cargo test -p coord-server --test kv_t_g5_split_merge_test`；`cargo test -p coord-server --test kv_t_g5_split_migrator_test`。

### Task 4: 文档与门禁

**Files:**
- Modify: `docs/adr/0015-kv-t.md`
- Modify: `docs/production/ops/boundaries.md`

- [x] **Step 1: ADR D6 与边界**

D6 两处 `**仍 open**：跨协调节点会话 drain` 改为已落地描述：split/merge 搬迁前由执行器把 drain 扇出到 region 0 全体成员（含 learner）；接收方在 RPC 生命周期内用本地 `TxnSessions` 等相交会话结束；可达节点超时 fail-closed，不可达节点按会话失效跳过（D12.2）。补判据测试路径。边界 B-KVT-10 去掉残余，写清同一口径。

- [x] **Step 2: 文档引用**

`bash scripts/check-doc-refs.sh`（悬空 0）。

- [x] **Step 3: 门禁**

`cargo fmt --all -- --check`；`cargo clippy --workspace --lib --bins -- -D warnings`；`bash scripts/check-panics.sh`；`cargo audit`；`cargo deny check`。

- [x] **Step 4: 交付**

PR #229 → CI 全绿 → merge → 删分支；#167 回填进展与残余（仅余多节点双写/双读故障注入）。`[kv_t].enabled=false` 的 v1 回归由 workspace tests 覆盖。
