# KV-T G4 Watch RPC、客户端与可观测 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 `coord.watch.v2.Watch` 从服务壳变成可用的双向流：Create/Cancel、cursor 续订、过滤、`HISTORY_UNAVAILABLE`、订阅指标，并提供带断线续订的 `coord-client` 包装。

**Architecture:** 服务端只做三件事：把首帧 `WatchCreateRequest` 校验成 `WatchStart`/`WatchFilter`（含保留命名空间与延后 scope）、用 `WatchCoordinator` 拉取式轮询组装 `WatchResponse`、用有界通道把批推给调用方（满则背压，不丢事件）。同一条流上的多个 create 各自一个拉取任务，cancel 用 abort 句柄；`HistoryUnavailable` 是流终止错误。客户端包装隐藏 watch_id、把每批 `cursor` 持久化到内存并在断线后用 `start_cursor` 重开流；`HISTORY_UNAVAILABLE` 不重试。

**Tech Stack:** Rust、tokio、tonic 0.14（双向流）、openraft（沿用 G4 既有 shard 组）、`coord_client::Client` 连接池。

**Spec:** `docs/adr/0015-kv-t.md` D5/D9/D12；issue #158（前置 #156、#157 已合入）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时 v1 行为不变；新增 handler 只在 v2 装配路径可达（ADR-0001），`Watch` 能力要显式加入 `KvTState::with_implemented`。
- `start_revision` 与 `start_cursor` 互斥：同时给出是请求歧义，返回 `INVALID_ARGUMENT`（不“后者优先”）。
- 事件语义由 `WatchCoordinator` 单点定义：按 `commit_ts` 全局有序、同一事务原子批、单批 ≤1000 change / 1MB；RPC 层**不得**重新分批或重排。
- 保留窗口不足 → `OUT_OF_RANGE` + `HISTORY_UNAVAILABLE`（稳定诊断锚点，逐字保留），消息里带最老可用 `(shard_id, commit_ts)`；该错误终止流且客户端不重试。
- 授权：能力门禁由鉴权层/`KvTState::gate` 负责；handler 只做首帧 scope 判定（`DeferredScopeGrants` 存在时 `check_deferred_scope` 失败即 `PERMISSION_DENIED`）与保留命名空间守卫。
- 订阅范围语义：`range_end` 空 = 单 key；`key` 与 `range_end` 都空 = 全 keyspace；`range_end == key` 按 `RangeSemantics::of` 视为单 key。全 keyspace 只在无 scope 约束（鉴权关闭/root）时可达，因为保留命名空间无法按订阅过滤（见 Ledger）。
- 每个订阅一个拉取任务；输出通道有界（容量 32），满时 `send().await` 等待（背压），**不得**丢弃事件或静默跳过 `commit_ts`。
- 注释只写当前“为什么”（不变量/陷阱/契约），不写日期、变更史或一次性任务号；不新增 TODO/FIXME。
- 临时文件只放 `/tmp`。
- 提交前本机必须跑通 `cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo audit`、`cargo deny check`；涉及文档跑 `bash scripts/check-doc-refs.sh`。

## Ledger of decisions

- **Ruling:** 首帧 create 在返回响应流之前同步解析起点（`WatchCoordinator::start`），因此 `HISTORY_UNAVAILABLE` / 参数错误以 RPC 错误返回；后续 create 的解析错误以流错误终止（同一流已被首个 create 建立，错误码语义一致）。
- **Ruling:** 服务端主动回一条空 `events` 的批（ack）：`cursor` = 起点（`start_revision=0` 时是服务端解析出的 `watermark` 位置），`watermark`/`sealed` 回显当前值。客户端据此在“还没收到事件就断线”时也能精确续订。
- **Ruling:** 同一流多个 create 共享输出通道；cancel 按 `watch_id` 中止对应拉取任务，未知 `watch_id` 视为幂等取消（订阅可能已因流终止/历史不足结束）。请求流 half-close 不终止订阅（客户端仍可读响应），输出通道关闭（客户端断开）才结束任务。
- **Ruling:** 保留命名空间守卫照 v1 口径：**有界**订阅触碰 `/_pki/`、`/_lease/`、`/_idgen/`、`/_pd/` → `PERMISSION_DENIED`；触碰 `/obj/` → `INVALID_ARGUMENT`。全 keyspace 订阅不做触碰判定（它必然覆盖全部命名空间；见 Global Constraints 的授权前提），但 scope 判定照常。
- **Ruling:** 不新增 proto 字段/枚举：批上限、cursor、watermark、sealed 都已在 `coord.watch.v2` 契约里；服务端只填现有字段。
- **Ruling:** 指标只做**无 label** 的聚合（metrics 框架没有 label 支持）：`coord_kv_t_watch_streams`（当前流数）、`coord_kv_t_watch_batches_total`、`coord_kv_t_watch_events_total`、`coord_kv_t_watch_lag_commit_ts`（最近一次下发批的 `watermark - 批末位置`）、`coord_kv_t_watch_history_unavailable_total`、`coord_kv_t_watch_backpressure_total`。每 shard 的 lag/sealed 由响应里的 `watermark`/`sealed` 与服务端既有 `coord_kv_t_commit_log_*` 共同表达；加 label 需先扩展指标框架（记入 docs）。
- **Ruling:** 客户端断线续订是**至少一次**：断线时已发出但未收到的批会在重连后重放（用 `cursor` 续订，按 `commit_ts` 去重）；不承诺“恰好一次”，也不在客户端静默去重（调用方需要审计时应按 `commit_ts` 幂等）。
- **Ruling:** 服务端不复用 `coord.watch.v2` 之外的 v1 dispatcher；v2 只读 Commit Log shard 组，未装配 shard（单机/未开 Multi-Raft）→ `FAILED_PRECONDITION`（配置/装配错误，不是 `UNIMPLEMENTED`）。

## Review Focus

- **首帧竞态**：客户端先发 create、再读响应；handler 必须等到首帧后才返回响应流，否则 `stub.watch()` 立即成功但订阅不存在。半关闭请求流后仍应继续收事件。
- **cursor 精确性**：ack 的 cursor 必须等于“已消费到的位置”，`start_revision=0` 时等于当前 watermark 位置；续订同一 cursor 不得重放上一事件，也不得跳过下一事件。`shard_id != 0 且 commit_ts = 0` 的起点跨 shard 后不能被误判为 `HISTORY_UNAVAILABLE`。
- **HISTORY_UNAVAILABLE 终止**：错误码必须是 `OUT_OF_RANGE` + trailer `HISTORY_UNAVAILABLE`（不是 `FAILED_PRECONDITION`/`INTERNAL`），且流不再产出后续事件；客户端不得无限重连。
- **背压不丢事件**：输出通道满时拉取任务等待而非丢弃；慢消费者只阻塞自己的订阅，不阻塞同流其它订阅，也不能把服务端内存变成无界队列。
- **取消/断开清理**：cancel 后不再产出该 watch 的事件；客户端断开（响应通道关闭）后拉取任务退出，`coord_kv_t_watch_streams` 回到 0。
- **授权/保留命名空间**：有 scope 约束时越界 prefix 在首帧就被拒（`PERMISSION_DENIED`）；`/_pki/` 等保留前缀永不投递给有界订阅；`/obj/` 订阅被显式拒绝。
- **v2 默认关闭**：`[kv_t].enabled=false` 时 `coord.watch.v2` 返回 `FAILED_PRECONDITION`，v1 `coord.watch.Watch` 行为逐字不变。

---

### Task 1: 首帧映射、守卫与错误码

**Files:**
- Create: `coord-server/src/kvt/watch_rpc.rs`
- Modify: `coord-server/src/kvt/mod.rs`（`mod watch_rpc;`）
- Test: `coord-server/src/kvt/watch_rpc.rs`（`#[cfg(test)] mod tests`）

**Interfaces:**
- Produces:
  - `pub(crate) struct WatchSpec { pub start: WatchStart, pub filter: WatchFilter }`
  - `pub(crate) fn spec_from_create(create: &watch_v2::WatchCreateRequest) -> Result<WatchSpec, Status>`
  - `pub(crate) fn guard_create(create: &watch_v2::WatchCreateRequest, grants: Option<&coord_core::grpc_auth::DeferredScopeGrants>) -> Result<(), Status>`
  - `pub(crate) fn watch_status(err: WatchError) -> Status`
  - `pub(crate) fn watch_scope_access(key: &[u8], range_end: &[u8]) -> coord_core::grpc_auth::ScopeAccess`
- 语义：
  - `spec_from_create`：`filter` 只接受 `0/1`（ALL）、`2`（PUTS_ONLY）、`3`（DELETES_ONLY）；`start_revision < 0`、`start_cursor.commit_ts < 0`、两者同时给出、`WatchFilter::validate`（反向区间）→ `INVALID_ARGUMENT` + trailer。
  - `guard_create`：`/obj/` 触碰（含 `range_end` 非空区间）→ `INVALID_ARGUMENT`；`/_pki/`、`/_lease/`、`/_idgen/`、`/_pd/` 触碰 → `PERMISSION_DENIED`；`grants` 为 `Some` 时用 `check_deferred_scope` 判 `watch_scope_access(key, range_end)`，失败 → `PERMISSION_DENIED`。全 keyspace（key 与 range_end 都空）跳过保留命名空间触碰判定。
  - `watch_scope_access`：全 keyspace → 无上界区间（`UNBOUNDED_RANGE_END`）；`range_end` 空或 `== key` → `ScopeAccess::point`；否则 `ScopeAccess::range`。
  - `watch_status`：`InvalidRequest`/`CursorUnavailable` → `INVALID_ARGUMENT`；`HistoryUnavailable` → `OUT_OF_RANGE` + `CoordErrorCode::HistoryUnavailable`，消息含 `requested` 与 `oldest_available`；`Unavailable` → `UNAVAILABLE`；`Storage(e)` → `crate::server::map_err(e)`。

- [ ] **Step 1: 写失败测试**

在 `watch_rpc.rs` 的 `#[cfg(test)] mod tests` 写（`base_create()` 返回 key=`/app/a`、`range_end` 空、`filter=0`）：

```rust
#[test]
fn spec_requires_exactly_one_start_form() {
    let mut create = base_create();
    create.start_revision = 10;
    create.start_cursor = Some(proto_cursor(1, 5));
    let err = spec_from_create(&create).unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert_eq!(error_code_of(&err).as_deref(), Some("INVALID_ARGUMENT"));
}

#[test]
fn spec_validates_filter_revision_and_range() {
    for filter in [4, -1, 99] {
        let mut create = base_create();
        create.filter = filter;
        assert_eq!(spec_from_create(&create).unwrap_err().code(), Code::InvalidArgument);
    }
    let mut negative = base_create();
    negative.start_revision = -1;
    assert_eq!(spec_from_create(&negative).unwrap_err().code(), Code::InvalidArgument);
    let mut negative_cursor = base_create();
    negative_cursor.start_cursor = Some(proto_cursor(1, -1));
    assert_eq!(spec_from_create(&negative_cursor).unwrap_err().code(), Code::InvalidArgument);
    let mut reversed = base_create();
    reversed.key = b"z".to_vec();
    reversed.range_end = b"a".to_vec();
    assert_eq!(spec_from_create(&reversed).unwrap_err().code(), Code::InvalidArgument);
}

#[test]
fn guard_denies_reserved_namespaces_and_object_space() {
    assert_eq!(
        guard_create(&create_for(b"/_pki/x", b""), None).unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        guard_create(&create_for(b"/_lease/x", b""), None).unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        guard_create(&create_for(b"/obj/a", b""), None).unwrap_err().code(),
        Code::InvalidArgument
    );
    // 反向区间在 spec 已拒绝，这里只验守卫独立可用
    assert!(guard_create(&create_for(b"/app/a", b""), None).is_ok());
    assert!(guard_create(&create_for(b"", b""), None).is_ok());
}

#[test]
fn guard_enforces_deferred_scope_prefixes() {
    let grants = DeferredScopeGrants {
        capability_id: "data:watch:subscribe".into(),
        grant_scopes: vec!["/app/a/".into()],
    };
    assert!(guard_create(&create_for(b"/app/a/x", b""), Some(&grants)).is_ok());
    assert_eq!(
        guard_create(&create_for(b"/app/b/x", b""), Some(&grants)).unwrap_err().code(),
        Code::PermissionDenied
    );
    assert_eq!(
        guard_create(&create_for(b"", b""), Some(&grants)).unwrap_err().code(),
        Code::PermissionDenied
    );
    let unrestricted = DeferredScopeGrants {
        capability_id: "data:watch:subscribe".into(),
        grant_scopes: vec![String::new()],
    };
    assert!(guard_create(&create_for(b"", b""), Some(&unrestricted)).is_ok());
}

#[test]
fn history_unavailable_keeps_the_diagnostic_anchor() {
    let status = watch_status(WatchError::HistoryUnavailable {
        requested: 1,
        oldest_available: WatchCursor { shard_id: 2, commit_ts: 0 },
    });
    assert_eq!(status.code(), Code::OutOfRange);
    assert_eq!(error_code_of(&status).as_deref(), Some("HISTORY_UNAVAILABLE"));
    assert!(status.message().contains("shard 2"), "{}", status.message());
    assert!(status.message().contains("commit_ts 0"), "{}", status.message());
}

#[test]
fn scope_access_uses_v2_watch_semantics() {
    assert_eq!(watch_scope_access(b"", b"").range_end, b"\0".to_vec());
    assert_eq!(watch_scope_access(b"/app/a", b"").range_end, Vec::<u8>::new());
    assert_eq!(watch_scope_access(b"/app/a", b"/app/c").range_end, b"/app/c".to_vec());
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-server --lib kvt::watch_rpc`
Expected: 编译失败（模块/函数不存在）。

- [ ] **Step 3: 实现映射、守卫与错误码**

在 `kvt/mod.rs` 加 `mod watch_rpc;`（`pub(crate)` 不必：`services.rs` 用 `super::watch_rpc`）。实现中复用：
`crate::auth::interceptor::reserved_internal_kv_namespace`（只传有界订阅）、`crate::storage::object_store::range_touches_object_space`、`coord_core::grpc_auth::{check_deferred_scope, ScopeAccess, UNBOUNDED_RANGE_END}`。
`/obj/` 与保留前缀的判定必须用**有效区间**：区间上界 `None`（`range_end == b"\0"`）时按无界处理；不能直接把 `"\0"` 当普通字节上界。

- [ ] **Step 4: 跑测试确认通过**

Run: `cargo test -p coord-server --lib kvt::watch_rpc`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/kvt/watch_rpc.rs coord-server/src/kvt/mod.rs
git commit -m "feat(kv-t): map and guard watch v2 create frames"
```

---

### Task 2: Watch RPC 流服务与指标

**Files:**
- Modify: `coord-server/src/kvt/watch_rpc.rs`（订阅任务、请求循环、`serve`）
- Modify: `coord-server/src/kvt/services.rs`（用真实 handler 替换 `shell`）
- Modify: `coord-server/src/metrics.rs`（订阅指标 + render）
- Modify: `coord/src/main.rs`（`KvTCapability::Watch` 加入 `implemented`）
- Create: `coord/tests/support/kv_t_watch_node.rs`（从 `coord/tests/kv_t_g4_watch_coordinator_test.rs` 抽出的共享夹具）
- Modify: `coord/tests/kv_t_g4_watch_coordinator_test.rs`（改用共享夹具）
- Create: `coord/tests/kv_t_g4_watch_rpc_test.rs`

**Interfaces:**
- Produces（metrics）：
  - `Metrics::inc_kv_t_watch_streams` / `dec_kv_t_watch_streams`
  - `Metrics::record_kv_t_watch_batch(events: u64, lag: u64)`
  - `Metrics::inc_kv_t_watch_history_unavailable`
  - `Metrics::inc_kv_t_watch_backpressure`
  - render 名称：`coord_kv_t_watch_streams`、`coord_kv_t_watch_batches_total`、`coord_kv_t_watch_events_total`、`coord_kv_t_watch_lag_commit_ts`、`coord_kv_t_watch_history_unavailable_total`、`coord_kv_t_watch_backpressure_total`
- Produces（fixture）：`WatchNode::start()`（实现能力含 `Watch`）、`WatchNode::serve()` → `{ addr, shutdown: oneshot::Sender<()>, handle: JoinHandle<()> }`，同时注册 `MaintenanceServer`（客户端 Leader 发现用）与 `coord.watch.v2` 的 `WatchServer`；`commit_pairs` / `settle` / `advance_clock_ms` / `wait_until_gc_before` / `wait_until_sealed` 原样保留。
- Handler 行为：
  - `watch`：`gate(Watch)` → `commit_log_shards()` 缺失 `FAILED_PRECONDITION` → 读首帧（非 `create`/空流 `INVALID_ARGUMENT`）→ `spec_from_create` + `guard_create` → `start()`（`HistoryUnavailable` 在这里返回 RPC 错误）→ 输出通道（32）+ 首订阅任务 → 请求循环任务 → 返回 `ReceiverStream`。
  - 订阅循环：`poll` → `record_kv_t_watch_batch(events.len(), watermark - 批末位置)` → 有界发送 → `has_more` 则立刻再 poll，否则 `sleep(100ms)`；`WatchError::Unavailable` 只退避重试；其它错误 `watch_status` 发 `Err` 终止流。
  - `metrics` 流计数用 `Drop` 守卫（最后一个持有者释放时 `dec`）。
- 测试（`kv_t_g4_watch_rpc_test.rs`，原生 tonic stub）：
  1. `test_watch_rpc_streams_global_order`：commit ts1、ts2；create `start_revision=ts1`；收到的 `events` 串联后 `commit_ts == [ts1, ts2]`；最后一批 `cursor.commit_ts == ts2` 且 `watermark >= ts2`。
  2. `test_watch_rpc_ack_and_cancel`：`start_revision=0` 的首条响应 `events` 为空且 `cursor` 等于服务端解析位置；拿到 `watch_id` 后发 cancel；再 commit 一个事务，500ms 内不再收到事件。
  3. `test_watch_rpc_resumes_from_cursor`：收 ts1 后按响应 `cursor` 重开流（`start_cursor`），只收到 ts2（不重放 ts1）。
  4. `test_watch_rpc_history_unavailable`：照 #157 夹具把 shard 回收后，create `start_revision=1` → `stub.watch()` 返回 `OUT_OF_RANGE` + `HISTORY_UNAVAILABLE`。
  5. `test_watch_rpc_filter_and_prev_kv`：一个事务写 `aa`/`zz`；create `key="aa"`、`prev_kv=true` → 一条事件、一个 change、`prev_kv.value == b"seed"`。
  6. `test_watch_rpc_gate_and_guards`：`[kv_t]` 关闭的裸节点 → `FAILED_PRECONDITION`；开启但未装配 shard → `FAILED_PRECONDITION`；`/_pki/x` → `PERMISSION_DENIED`；`/obj/a` → `INVALID_ARGUMENT`。

- [ ] **Step 1: 抽共享夹具并确认既有测试不回归**

把 `WatchNode` 结构与 `start`、`put_v2`、`begin`、`commit_puts`、`commit_pairs`、`settle`、`open_first_shard`、`advance_clock_ms`、`wait_until_gc_before`、`wait_until_sealed`、`poll_until`（连同 `find_port`、`raft_config`、`FakeTso`、`wait_for_leaders`）移到 `coord/tests/support/kv_t_watch_node.rs`，字段与所需方法 `pub`，模块级 `#![allow(dead_code)]`；`kv_t_g4_watch_coordinator_test.rs` 用 `#[path = "support/kv_t_watch_node.rs"] mod watch_node; use watch_node::*;`。在 `with_implemented` 里加入 `KvTCapability::Watch`。

Run: `cargo test -p coord --test kv_t_g4_watch_coordinator_test`
Expected: PASS（纯搬移）。

提交：`test(kv-t): share the watch node fixture across e2e tests`

- [ ] **Step 2: 写失败的服务端 e2e 测试**

新文件按上面 6 个用例写；辅助函数：

```rust
async fn open_watch(
    addr: &str,
    create: watch_v2::WatchCreateRequest,
) -> Result<tonic::Streaming<watch_v2::WatchResponse>, tonic::Status> {
    let channel = tonic::transport::Channel::from_shared(format!("http://{addr}"))
        .unwrap().connect().await.unwrap();
    let mut stub = watch_v2::watch_client::WatchClient::new(channel);
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tx.send(watch_v2::WatchRequest { request: Some(Request::Create(create)) }).await.unwrap();
    let resp = stub.watch(tonic::Request::new(ReceiverStream::new(rx))).await?;
    Ok(resp.into_inner())
}
```

Run: `cargo test -p coord --test kv_t_g4_watch_rpc_test`
Expected: FAIL（`UNIMPLEMENTED`/`FAILED_PRECONDITION`：handler 仍是壳）。

- [ ] **Step 3: 实现 handler、订阅任务与指标**

- `services.rs`：`watch` 的 `Self::WatchStream` 改为 `ReceiverStream<Result<WatchResponse, Status>>`，逻辑委托 `super::watch_rpc::serve(self, request)`。
- `watch_rpc::serve` 签名：`pub(crate) async fn serve(node: &CoordNode, request: Request<Streaming<watch_v2::WatchRequest>>) -> Result<Response<ReceiverStream<...>>, Status>`。
- `main.rs` 的 `with_implemented` 增加 `KvTCapability::Watch`。
- 指标方法与 render 块按 `kv_t_commit_log_*` 的既有写法追加。

Run: `cargo test -p coord --test kv_t_g4_watch_rpc_test`
Expected: PASS。

- [ ] **Step 4: 跑 server 单测与 fmt**

Run: `cargo test -p coord-server --lib kvt:: && cargo fmt --all -- --check`
Expected: PASS。

- [ ] **Step 5: 提交**

```bash
git add coord-server/src/kvt/watch_rpc.rs coord-server/src/kvt/services.rs \
        coord-server/src/metrics.rs coord/src/main.rs \
        coord/tests/support/kv_t_watch_node.rs \
        coord/tests/kv_t_g4_watch_coordinator_test.rs \
        coord/tests/kv_t_g4_watch_rpc_test.rs
git commit -m "feat(kv-t): serve coord.watch.v2 streams with subscription metrics"
```

---

### Task 3: `coord-client` v2 Watch 包装（cursor + 重连）

**Files:**
- Create: `coord-client/src/watch_v2.rs`
- Modify: `coord-client/src/lib.rs`（`pub mod watch_v2;` + 重导出）
- Modify: `coord-client/src/client.rs`（`Client::watch_v2()`）
- Test: `coord-client/src/watch_v2.rs`（纯映射/重试分类单测）
- Test: `coord/tests/kv_t_g4_watch_rpc_test.rs`（续订 + 服务重启重连）

**Interfaces:**
- Produces:
  - `pub struct WatchV2Client`（`Client::watch_v2()` 构造）
  - `WatchV2Client::watch(&self, options: WatchV2Options) -> Result<WatchV2Subscription>`
  - `WatchV2Options { key, range_end, prev_kv, filter: WatchV2Filter, start: WatchV2Start }`，构造器 `new(key)`, `with_range`, `with_prev_kv`, `with_filter`, `from_revision(u64)`, `from_cursor(WatchV2Cursor)`
  - `WatchV2Start { revision: u64, cursor: Option<WatchV2Cursor> }`
  - `WatchV2Cursor { shard_id: u64, commit_ts: u64 }`（`From`/`Into` proto）
  - `WatchV2Batch { events: Vec<WatchV2Event>, cursor: Option<WatchV2Cursor>, watermark: u64, sealed: bool }`
  - `WatchV2Subscription::recv() -> Option<Result<WatchV2Batch>>`、`cursor() -> Option<WatchV2Cursor>`、`cancel(self).await`
- 语义：
  - 每次响应先更新内部 cursor；`events` 为空的 ack 不向上投递（只更新 cursor/watermark）。
  - 断线（`Ok(None)`、`UNAVAILABLE`、`DEADLINE_EXCEEDED`、`RESOURCE_EXHAUSTED`）→ 退避后用 `start_cursor = 最后一批 cursor` 重开流；若还没收到过任何响应，用原 `start_revision`。
  - `OUT_OF_RANGE` + `HISTORY_UNAVAILABLE` → 终止并返回 `Error::HistoryUnavailable`，不重连。
  - `cancel`/`Drop` 关闭请求通道并等待任务退出（`Drop` 只发停止信号）。
  - 重试次数用 `RetryState::new(&client.config)`；收到任何成功批后 `reset()`，耗尽 → 返回最后一次错误。

- [ ] **Step 1: 写失败单测（纯函数）**

在 `watch_v2.rs`：
```rust
#[test]
fn cursor_and_filter_round_trip() {
    let c = WatchV2Cursor { shard_id: 3, commit_ts: 42 };
    let p = c.to_proto();
    assert_eq!(WatchV2Cursor::from(p), c);
    assert_eq!(WatchV2Filter::PutsOnly.to_proto(), 2);
}

#[test]
fn retryable_statuses_exclude_history_unavailable() {
    assert!(retryable(Status::unavailable("x")));
    assert!(retryable(Status::deadline_exceeded("x")));
    assert!(retryable(Status::resource_exhausted("x")));
    assert!(!retryable(attach(Status::invalid_argument("x"), CoordErrorCode::InvalidArgument)));
    assert!(!retryable(attach(Status::out_of_range("x"), CoordErrorCode::HistoryUnavailable)));
}
```
Run: `cargo test -p coord-client --lib watch_v2` → FAIL（模块不存在）。

- [ ] **Step 2: 实现客户端包装**

关键循环（伪码，注释里写清“为什么”）：

```rust
loop {
    let create = proto::WatchCreateRequest {
        key: options.key.clone(),
        range_end: options.range_end.clone(),
        start_revision: if resume.is_some() { 0 } else { options.start.revision as i64 },
        start_cursor: resume.map(WatchV2Cursor::to_proto),
        prev_kv: options.prev_kv,
        filter: options.filter.to_proto(),
    };
    match open_stream(&client, create).await {
        Ok(mut out) => {
            retry.reset();
            loop {
                tokio::select! {
                    _ = &mut stop => return,
                    msg = out.message() => match msg {
                        Ok(Some(resp)) => { /* update cursor; skip empty ack; tx.send(batch) */ }
                        Ok(None) => break, // 服务端结束：按游标重连
                        Err(status) if retryable(&status) => break,
                        Err(status) => { let _ = tx.send(Err(watch_error(status))).await; return; }
                    }
                }
            }
        }
        Err(status) if retryable(&status) => {}
        Err(status) => { let _ = tx.send(Err(watch_error(status))).await; return; }
    }
    match retry.next_attempt() {
        Some(delay) => tokio::time::sleep(delay).await,
        None => { let _ = tx.send(Err(Error::ClusterUnavailable("watch reconnect attempts exhausted".into()))).await; return; }
    }
}
```

Run: `cargo test -p coord-client --lib watch_v2`
Expected: PASS。

- [ ] **Step 3: 写失败的 e2e（续订 + 重连）**

加到 `coord/tests/kv_t_g4_watch_rpc_test.rs`：
1. `test_watch_client_resumes_from_cursor`：`Client::new(Config::new(vec![addr]))` → `watch_v2().watch(opts.from_revision(ts1))` 收 ts1 → 记录 `cursor()` → `cancel()` → 新订阅 `from_cursor(cursor)` → 只收 ts2。
2. `test_watch_client_reconnects_after_server_restart`：订阅后收 ts1；`server.shutdown().await`；commit ts2 并 `settle`；用同一地址重新 `fixture.serve().await`；500ms 轮询内收到 ts2（事件序列 `[ts1, ts2]`，无 ts1 重放）。
3. `test_watch_client_history_unavailable_is_terminal`：GC 后用 `from_revision(1)` 订阅；`recv()` 返回 `Error::HistoryUnavailable`，且 1s 内不再有响应（不重连）。

Run: `cargo test -p coord --test kv_t_g4_watch_rpc_test watch_client`
Expected: FAIL（`Client::watch_v2` 不存在）。

- [ ] **Step 4: 跑通并提交**

Run: `cargo test -p coord-client --lib watch_v2 && cargo test -p coord --test kv_t_g4_watch_rpc_test`
Expected: PASS。

```bash
git add coord-client/src/watch_v2.rs coord-client/src/lib.rs coord-client/src/client.rs \
        coord/tests/kv_t_g4_watch_rpc_test.rs
git commit -m "feat(kv-t): add resumable coord.watch.v2 client wrapper"
```

---

### Task 4: 文档、门禁与 PR

**Files:**
- Modify: `docs/production/consumer-contract.md`（把“尚未提供 v2 Watch RPC”改为实际语义：Create/Cancel、ack、cursor、重连至少一次、HISTORY_UNAVAILABLE、限制）
- Modify: `docs/production/ops/boundaries.md`（新增 B-KVT-7：v2 Watch 订阅范围/背压/全 keyspace 的授权前提）
- Modify: `docs/production/ops/runbook.md`（“KV-T v2 Watch 排障”小节：订阅指标与 HISTORY_UNAVAILABLE 处置）
- Modify: `apis/contracts/STATUS.md`（若 Watch v2 行需要更新实现状态；只写事实）
- Modify: `docs/adr/0015-kv-t.md`（仅在实现与 ADR 口径有偏差时补 D5 落地注记；不改语义）

- [ ] **Step 1: 更新文档**

`consumer-contract.md` 的 Watch 段补：
- 服务端在建立订阅后先回一条空 `events` 批（cursor/watermark/sealed）；
- 同流多 create 共享流、cancel 按 `watch_id`；
- 断线用最后一批 `cursor` 续订；客户端包装自动重连，语义为**至少一次**；
- 全 keyspace 订阅只在无 scope 约束时可用（保留命名空间无法按订阅过滤）。

`boundaries.md` 新增一行 B-KVT-7，写明：单流输出通道有界、慢消费者只承受背压（不丢事件）、订阅数/批/事件/lag/历史不足/背压六个指标名、以及“每 shard 指标不做 label（框架限制）”。

`runbook.md` 新增小节：`coord_kv_t_watch_history_unavailable_total` 上升 → 客户端应全量重同步；`..._backpressure_total` 上升 → 消费端慢；`..._lag_commit_ts` 持续高 → 检查本地 watermark 与 shard 组推进。

- [ ] **Step 2: 文档引用检查**

Run: `bash scripts/check-doc-refs.sh`
Expected: 悬空引用 0。

- [ ] **Step 3: 全量门禁**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo audit
cargo deny check
cargo test -p coord-server --lib kvt::
cargo test -p coord-client --lib
cargo test -p coord --test kv_t_g4_watch_rpc_test
cargo test -p coord --test kv_t_g4_watch_coordinator_test
```
Expected: 全绿。

- [ ] **Step 4: 提交文档**

```bash
git add docs/ apis/contracts/STATUS.md
git commit -m "docs(kv-t): document coord.watch.v2 rpc, resume and metrics"
```

- [ ] **Step 5: 开 PR（按 AGENTS.md：分支 → PR → CI 全绿 → merge → 删分支）**

```bash
git push -u origin feat/kv-t-g4-watch-rpc
gh pr create --base main --fill
gh pr checks --watch
gh pr merge --merge --delete-branch
```

PR 描述里逐条勾选 #158 的验收项，并把证据（测试名/命令）写进去；未交付的分项（如有）在 PR 里显式列为 follow-up，不在 issue 里假装完成。
