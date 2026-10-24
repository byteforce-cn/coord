# KV-T G6 — Agent `coord.watch.v2` / `coord.storage.v2` 代理 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Agent 注册并转发 `coord.watch.v2` 与 `coord.storage.v2`，补齐 #170 数据面代理的最后两个服务，同时保持 v1 行为与 v2 语义（局部 watch_id、cursor/watermark、commit_ts、`read_ts`）不变。

**Architecture:** 两个代理都写在 `coord-agent/src/proxy_v2.rs`（与 TSO/KV/Lease/Txn 同一模块，复用 `map_v2_error` / `no_backend` / `ts_from_i64` / `ts_to_i64`），底层一律走 `coord-client` 的 v2 包装：Watch 用 `WatchV2Client`（客户端内部按 cursor 重连），Storage 用 `StorageV2Client`（服务端 Commit 分配 `commit_ts`）。Watch 的 `watch_id` 由**代理本地**分配（客户端包装隐藏了服务端 id；本地 id 在重连后仍稳定，取消/映射不会因为上游重开流而漂移），每个 create 的 scope 用 `check_deferred_scope` + `watch_create_access` 在首帧判定（与 v1、server 同一实现）。

**Tech Stack:** Rust / tokio / tonic / tonic streaming / `coord-client`。

**Spec:** issue #170（Agent 数据面 v2 迁移）、ADR-0015 D5/D6/D7/D9/D12；v2 契约：`apis/contracts/proto/coord/watch/v2/watch.proto`、`apis/contracts/proto/coord/storage/v2/storage.proto`。

## Global Constraints

- `[kv_t].enabled=false`（v1）行为字节级不变；本次只新增 v2 服务，不改 v1 代理/路由。
- 无 Server 连接时 v2 RPC fail-closed：`UNAVAILABLE` + `x-coord-error-code: UNAVAILABLE`（与 #239/#241 同口径），不得占位成功。
- wire 错误码原样透传：`HISTORY_UNAVAILABLE`（`OUT_OF_RANGE`）、`EPOCH_STALE`（`ABORTED`）、`COMMIT_REJECTED`、`INVALID_ARGUMENT` 不得压成 `INTERNAL`。
- Watch 语义不在代理重写：不重分批、不重排、不合并事件；空批只用于上游 cursor 推进，代理不向上丢事件批。
- Watch scope：每个 `create` 都判定（首个 create 在 handler 内同步判定，后续 create 在读取任务里判定）；越界 → `PERMISSION_DENIED`；保留命名空间/`/obj/` 由上游 server 继续拒绝。
- Storage 代理沿用 v1 的整对象缓冲上界（256 MiB）与 4 MiB 回放分片；但 v2 必须尊重声明的 `total_size`（`>= 0` 时与实际字节数不符 → `INVALID_ARGUMENT`，不发上游）。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`；临时文件只放 `/tmp/coord-*`。
- 合入前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny）与文档引用检查。

## Review Focus

- 客户端 half-close 请求流后仍要能继续收事件（server 的语义）：代理读帧任务结束不得顺带 abort 订阅。
- 取消是幂等的：未知/已结束 `watch_id` 的 `cancel` 不得报错、不得把整条流打断。
- 上游不可恢复错误（尤其 `HISTORY_UNAVAILABLE`）必须作为一条错误帧下发并结束该订阅，不能静默停流。
- 多 create 共享一条流：每个订阅的事件只能带自己的本地 `watch_id`，不能串号。
- Storage 声明长度与实际长度不符、越界 `read_ts`、空对象必须在代理层就拒绝（不发上游、不消耗 `commit_ts`）。

---

### Task 1: `coord.storage.v2` 代理

**Files:**
- Modify: `coord-agent/src/proxy_v2.rs`（新增 `StorageV2Proxy` + 模块头注释）
- Modify: `coord-agent/src/lib.rs`（注册 `StorageServer` v2 + health）
- Test: `coord-agent/tests/agent_v2_storage_proxy_test.rs`（新建）
- Test: `coord-agent/tests/agent_grpc_test.rs`（骨架 fail-closed 覆盖）

**Interfaces:**
- Produces:
  ```rust
  pub struct StorageV2Proxy { inner: Option<Arc<AgentInner>> }
  impl StorageV2Proxy { pub fn new(inner: Option<Arc<AgentInner>>) -> Self }
  impl coord_proto::storage::v2::storage_server::Storage for StorageV2Proxy {
      type GetStream = ReceiverStream<Result<storage_v2::GetResponse, tonic::Status>>;
  }
  ```
- Consumes: `AgentInner.client.storage_v2()` → `put_chunked(&str, &[u8], &[u8], usize)`, `get(&str, &[u8], u64)`, `stat(&str, &[u8], u64)`, `delete_full(&str, &[u8], &[u8])`；`map_v2_error`、`no_backend`、`ts_from_i64`。

- [x] **Step 1: Write the failing proxy test**

`coord-agent/tests/agent_v2_storage_proxy_test.rs`：假 `coord.storage.v2` 服务（`put` 收集 meta/chunks 并回 `commit_ts=77`；`get` 先 stat（`commit_ts=77`）再两段 chunk；`stat` 对未知 key 回 `exists=false` 零 stat；`delete` 回 `(true, 88)`），断言：

```rust
// put: 声明的 total_size 与实际字节原样到上游；响应 commit_ts 不回退
assert_eq!(resp.commit_ts, 77);
assert_eq!(fake.puts(), vec![(b"b".to_vec(), b"o".to_vec(), b"hello".to_vec(), 5)]);

// get: read_ts 原样透传，首条 stat + 全量数据
assert_eq!(fake.get_read_ts(), vec![9]);
assert_eq!(first_part_stat.commit_ts, 77);
assert_eq!(data, b"hello");

// stat: v2 的「不存在」是 exists=false 的零 stat，不是 NOT_FOUND
assert_eq!(resp.stat.unwrap().exists, false);

// delete: deleted 与 tombstone commit_ts 原样
assert_eq!((resp.deleted, resp.commit_ts), (true, 88));
```

再写 `proxy_rejects_declared_size_mismatch_before_put`（meta 声明 10、实际 3 字节 → `INVALID_ARGUMENT`，fake `put` 调用数为 0）与 `skeleton_storage_v2_fails_closed`（`inner=None` 时 Put/Get/Stat/Delete 均 `UNAVAILABLE` + `UNAVAILABLE` 码）。

- [x] **Step 2: Run tests to verify they fail**

Run: `cargo test -p coord-agent --test agent_v2_storage_proxy_test`
Expected: 编译失败（`StorageV2Proxy` 不存在）。

- [x] **Step 3: Implement `StorageV2Proxy`**

- `put`：首帧必须是 meta（否则 `INVALID_ARGUMENT`）；按 v1 的 256 MiB 缓冲上界收集 chunk；`meta.total_size >= 0 && data.len() as i64 != total_size` → `INVALID_ARGUMENT`；空数据 → `INVALID_ARGUMENT`；调用 `put_chunked`，返回 `PutResponse { commit_ts, size, chunks }`（u64→i64 用 `ts_to_i64`）。
- `get`：`ts_from_i64("read_ts", req.read_ts)`；`get()` 后回放 stat + 4 MiB chunk。
- `stat`：`ts_from_i64`；`None` → `NOT_FOUND`（v2 服务端正常返回零 stat，`None` 只防御上游旧口径）。
- `delete`：`delete_full` 返回 `deleted`/`commit_ts`。
- 在 `lib.rs` 注册 `StorageV2Server::new(StorageV2Proxy::new(svcs.inner.clone()))` 并 `set_serving`。

- [x] **Step 4: Run the proxy tests**

Run: `cargo test -p coord-agent --test agent_v2_storage_proxy_test`
Expected: PASS。

- [x] **Step 5: Extend the skeleton registration test**

`agent_grpc_test::test_agent_registers_kvt_v2_services_fail_closed` 增加 storage v2：`storage_v2::StorageClient::put`（单条 meta）首条响应错误为 `UNAVAILABLE`。

- [x] **Step 6: Commit**

```bash
git add coord-agent/src/proxy_v2.rs coord-agent/src/lib.rs coord-agent/tests/agent_v2_storage_proxy_test.rs coord-agent/tests/agent_grpc_test.rs
git commit -m "feat(kv-t): proxy storage v2 through the agent (#170)"
```

---

### Task 2: `coord.watch.v2` 代理

**Files:**
- Modify: `coord-agent/src/proxy_v2.rs`
- Modify: `coord-agent/src/lib.rs`（注册 `WatchServer` v2 + health）
- Test: `coord-agent/tests/agent_v2_watch_proxy_test.rs`（新建）
- Test: `coord-agent/tests/agent_grpc_test.rs`

**Interfaces:**
- Produces:
  ```rust
  pub struct WatchV2Proxy { inner: Option<Arc<AgentInner>> }
  impl WatchV2Proxy { pub fn new(inner: Option<Arc<AgentInner>>) -> Self }
  impl coord_proto::watch::v2::watch_server::Watch for WatchV2Proxy {
      type WatchStream = ReceiverStream<Result<watch_v2::WatchResponse, tonic::Status>>;
  }
  fn watch_options_from_create(create: &watch_v2::WatchCreateRequest) -> Result<WatchV2Options, tonic::Status>
  fn watch_response(watch_id: i64, batch: WatchV2Batch) -> Result<watch_v2::WatchResponse, tonic::Status>
  ```
- Consumes: `AgentInner.client.watch_v2().watch(WatchV2Options)` → `WatchV2Subscription::{recv, cancel}`；`coord_core::grpc_auth::{check_deferred_scope, DeferredScopeGrants, watch_create_access}`。

- [x] **Step 1: Write the failing proxy test**

`coord-agent/tests/agent_v2_watch_proxy_test.rs`：假 `coord.watch.v2` 服务，每条流解析首帧 create 后在独立任务里每 10ms 下发一批（`watch_id=1`、一个 PUT event、cursor `(shard=3, commit_ts=42)`、`watermark=50`、`sealed=true`），发送失败即记为 closed；后续 create 记入 `creates`，`cancel` 记入 `cancels`。

用例：
1. `proxy_forwards_watch_v2_with_local_ids_cursor_and_cancel`：一条客户端流上先 create `/a` 再 create `/b`；收到的批分别带 `watch_id=1`/`2`、cursor/watermark/sealed 原样；取消 `1` 后 2s 内 fake 观察到 1 条流关闭，且后续批只出现 id `2`；取消未知 `999` 不报错、不打断 id `2`。
2. `proxy_denies_out_of_scope_create_before_subscribing`：请求扩展注入 `DeferredScopeGrants { capability_id: "data:watch:subscribe", grant_scopes: vec!["/app/"] }`，create `/other/x` → `PERMISSION_DENIED`，fake `creates` 为空。
3. `skeleton_watch_v2_fails_closed`：`inner=None`，首帧 create → `UNAVAILABLE` + `UNAVAILABLE` 码。

- [x] **Step 2: Run tests to verify they fail**

Run: `cargo test -p coord-agent --test agent_v2_watch_proxy_test`
Expected: 编译失败（`WatchV2Proxy` 不存在）。

- [x] **Step 3: Implement `WatchV2Proxy`**

- 取 `DeferredScopeGrants` 必须在 `into_inner()` 之前；首帧不是 create（含空流、首帧 cancel）→ `INVALID_ARGUMENT`（与 server 同文案）。
- `watch_options_from_create`：`EventFilter` 只接受 0/1/2/3，`start_revision < 0` 拒绝，`start_cursor.commit_ts < 0` 拒绝；`start_revision` 与 `start_cursor` 互斥（客户端已校验，代理先给出 `INVALID_ARGUMENT`）。
- 首帧 scope 判定在**要求后端连接之前**（拒绝不应泄露后端状态）。
- 首个订阅同步建立（参数错误以 RPC 错误返回），本地 id 从 `1` 起；读取任务用 `AtomicI64` 从 `2` 起分配，`HashMap<i64, JoinHandle<()>>` 持有转发任务；`cancel` 只 `abort()` 对应句柄（Drop 触发 `WatchV2Subscription` 停止），未知 id 幂等忽略。
- 请求流正常 half-close 后 `std::mem::forget` 订阅句柄（server 同语义：half-close 不等于取消）；后续 create/取消错误时下发一条错误帧并 drop 句柄。
- 转发任务：`recv()` → `watch_response`；`Err` → `map_v2_error` 后下发错误帧并结束。
- 在 `lib.rs` 注册 `WatchV2Server` 并 `set_serving`。

- [x] **Step 4: Run the proxy tests**

Run: `cargo test -p coord-agent --test agent_v2_watch_proxy_test`
Expected: PASS。

- [x] **Step 5: Extend the skeleton registration test**

`agent_grpc_test`：`watch_v2::WatchClient::new(channel).watch(stream([create]))` 后首条消息错误为 `UNAVAILABLE`。

- [x] **Step 6: Commit**

```bash
git add coord-agent/src/proxy_v2.rs coord-agent/src/lib.rs coord-agent/tests/agent_v2_watch_proxy_test.rs coord-agent/tests/agent_grpc_test.rs
git commit -m "feat(kv-t): proxy watch v2 through the agent (#170)"
```

---

### Task 3: Docs, gates, PR

**Files:**
- Modify: `docs/production/consumer-contract.md`（「Agent 代理面」段落）
- Modify: `docs/superpowers/plans/2026-10-23-kv-t-g6-agent-watch-storage-v2-proxy.md`（勾选）

- [x] **Step 1: Update the contract paragraph**

把「`coord.watch.v2` / `coord.storage.v2` 仍需直连 Server」改为：两者已由 Agent 转发；Watch 的 `watch_id` 是代理本地作用域（跨上游重连稳定），cursor/watermark/sealed 原样透传，`HISTORY_UNAVAILABLE` 仍是终点；Storage 保持整对象缓冲（≤256 MiB）并尊重 `total_size`，`read_ts` 透传。写清 cancel 幂等、half-close 仍收事件。

- [x] **Step 2: Run doc reference check**

Run: `bash scripts/check-doc-refs.sh`
Expected: 0 dangling refs。

- [x] **Step 3: Run the local gates**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
cargo test -p coord-agent --test agent_v2_storage_proxy_test --test agent_v2_watch_proxy_test --test agent_grpc_test
cargo audit
cargo deny check advisories licenses bans sources
```

Expected: 全绿；若 clippy/tests 因新代码失败，修到绿再继续。

- [ ] **Step 4: Branch → PR → CI → merge**

```bash
git switch -c feat/kv-t-g6-agent-watch-storage-v2-proxy origin/main
gh pr create --base main --fill
gh pr checks --watch
gh pr merge --merge --delete-branch
```

PR 描述写清：Watch 本地 id 的理由、scope 判定位置、half-close/cancel 语义、Storage `total_size` 校验、证据测试名。

- [ ] **Step 5: Update issue #170**

在 #170 评论记录本次切片（服务、语义、证据、仍 open 的 16 服务迁移），不关闭 issue。

## Self-Review

- Spec coverage：#170 的「Watch/Storage v2 代理」由 Task 1/2 覆盖；16 服务迁移与 v1 关闭仍留在 issue 的 open 列表（非本切片）。
- Step scan：每步都有可执行命令/断言；没有 TBD。
- Type consistency：`WatchV2Options`/`WatchV2Batch`/`WatchV2Cursor`/`ObjectDataV2`/`PutObjectV2Result` 均来自 `coord-client` 现有导出；proto 类型路径与 `proxy_v2.rs` 现有 import 风格一致。
- Review Focus 每一条都有对应用例（half-close、幂等 cancel、错误帧、多 create 串号、Storage 校验）。
- Proportion：两个服务各一个任务 + 文档/门禁，符合前两个代理 PR 的体量。
