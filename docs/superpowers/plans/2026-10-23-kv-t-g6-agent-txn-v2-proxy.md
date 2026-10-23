# KV-T G6 — Agent `coord.txn.v2` 代理（会话亲和）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Agent 注册并转发 `coord.txn.v2`（Begin/Read/Commit/Abort），把同一
`txn_id` 的后续调用送回 **Begin 选中的协调节点**，并原样保留 `TXN_CONFLICT` /
`TXN_NOT_FOUND` / `TXN_TOO_LARGE` 的 wire 码。

**Architecture:** 服务端事务会话只活在创建它的协调节点（ADR-0015 D12.2）。
`coord-client` 的 `TxnV2Session` 在 `begin` 时钉死一条 channel；代理按 `txn_id`
持有这个 session（`HashMap<txn_id, Arc<tokio::sync::Mutex<Option<TxnV2Session>>>>`），
`Read` 复用、`Commit`/`Abort` 取走并释放，绝不按「当前 leader」重新解析——否则
leader 切换后必然 `TXN_NOT_FOUND`。代理侧对未知 `txn_id` 直接回
`ABORTED + TXN_NOT_FOUND`（与协调节点同一口径），`Abort` 保持幂等成功。
被放弃的会话（客户端既不 Commit 也不 Abort）在超过服务端 TTL 上界
（`[kv_t].txn_ttl_secs` 校验为 `1..=3600`）+ 余量后才从代理表里清掉，避免
「代理先过期、服务端还活着」的语义破坏。

**Tech Stack:** Rust / tonic（agent 代理）、coord-client v2 包装、coord-proto
生成的 `coord.txn.v2` stub、tokio。

**本切片不包含：** `coord.watch.v2` / `coord.storage.v2` 代理（后续切片）；Java
SDK 的 Txn v2；Agent 内部 16 个服务切换 v2（#171）。

---

## File Structure

| 文件 | 责任 |
|:--|:--|
| `coord-agent/src/proxy_v2.rs` | `TxnV2Proxy` + `TxnSessions` 登记表（钉住会话、惰性回收）；模块头注释更新为 TSO/KV/Lease/Txn |
| `coord-agent/src/lib.rs` | 注册 `TxnV2Server`、health `set_serving`、导出 `TxnV2Proxy` |
| `coord-agent/tests/agent_v2_txn_proxy_test.rs` | 假 `coord.txn.v2` 服务：转发、冲突码、未知会话、幂等 abort、跨 leader 切换的会话亲和 |
| `coord-agent/tests/agent_grpc_test.rs` | 注册面断言补 `coord.txn.v2.Txn` |
| `coord-client/src/txn_v2.rs` | `TxnV2Session::commit_in_place`（保留句柄的提交，供代理在传输失败后用同一会话 + `request_id` 重试） |
| `docs/production/consumer-contract.md` | 「Agent 代理面」行：Txn 已代理，Watch/Storage 仍直连 |

## Task 1: 代理与登记表骨架（fail-closed）

**Files:**
- Modify: `coord-agent/src/proxy_v2.rs`
- Test: `coord-agent/src/proxy_v2.rs`（`#[cfg(test)] mod tests`）

- [x] 写失败测试：`skeleton_txn_fails_closed_with_unavailable`——`Begin`/`Read`/
      `Commit`/`Abort` 四个方法在 `TxnV2Proxy::new(None)` 下都返回
      `UNAVAILABLE` + `x-coord-error-code: UNAVAILABLE`，且 `Read` 不是
      `TXN_NOT_FOUND`（没有后端时不能假装「会话不存在」）。
- [x] 跑 `cargo test -p coord-agent --lib proxy_v2::`，确认因缺少 `TxnV2Proxy` 失败。
- [x] 实现 `TxnV2Proxy { inner: Option<Arc<AgentInner>>, sessions: TxnSessions }`，
      `TxnSessions` 用 `parking_lot::Mutex<HashMap<Vec<u8>, Arc<TxnSlot>>>`；
      `TxnSlot { session: tokio::sync::Mutex<Option<TxnV2Session>>, last_used_ms: AtomicU64 }`。
- [x] 重跑测试通过。

## Task 2: 转发 Begin/Read/Commit/Abort + 错误码

**Files:**
- Modify: `coord-agent/src/proxy_v2.rs`
- Test: `coord-agent/tests/agent_v2_txn_proxy_test.rs`（新建）

- [x] 写失败测试（假 `Txn` 服务，记录 `txn_id` 与调用序列）：
  - `Begin` 经代理返回原 `txn_id`/`start_ts`；
  - `Read` 转发同一 `txn_id` 并返回 kv；
  - `Commit` 成功返回 `commit_ts`；
  - 冲突事务：`Commit` 返回 `ABORTED` + trailer `TXN_CONFLICT`（不能压成
    `INTERNAL`/裸 `ABORTED`）；
  - `Commit`/`Read` 已结束或不存在的 `txn_id`：`ABORTED` + `TXN_NOT_FOUND`；
  - `Abort` 不存在 `txn_id`：`Ok`（幂等）。
- [x] 跑测试确认失败。
- [x] 实现四个 handler：
  - `begin`：`client.txn_v2().begin(start_ts, ttl_ms)`，登记 `txn_id`；
  - `read`：取登记（未知 → `TXN_NOT_FOUND`），`session.read(...)`；服务端回
    `TXN_NOT_FOUND` 时先删登记再回错；
  - `commit`：先摘登记；无登记 → `TXN_NOT_FOUND`；`TxnCommit::Conflict` →
    `Status::aborted` + `TXN_CONFLICT`；成功回 `commit_ts`；
  - `abort`：摘登记并 `abort`；无登记直接 `Ok`。
- [x] 重跑测试通过。

## Task 3: 会话亲和（Begin 后 leader 切换）

**Files:**
- Test: `coord-agent/tests/agent_v2_txn_proxy_test.rs`

- [x] 写失败测试：两个假服务 A/B；`Client::connect_direct` 以 `[A, B]` 发现
      leader=A；`Begin` 经代理落到 A；再用一次 v2 KV Put 让 A 返回
      `UNAVAILABLE` + `coord-leader-hint: B`（客户端把 leader 缓存切到 B 并重试
      Put 成功）；随后代理 `Read` 必须仍落到 A（B 的 Txn 调用数为 0）。
      去掉「按 txn_id 复用 session」而改为每次 `get_leader_channel` 时该测试必红。
- [ ] 跑测试确认失败。
- [x] 实现已由 Task 1/2 的登记表满足；若失败则修登记/取用路径（不得改为无状态）。
- [ ] 重跑测试通过。

## Task 4: 登记表回收（有界内存）

**Files:**
- Modify: `coord-agent/src/proxy_v2.rs`（单元测试）

- [x] 写失败测试：`sweep_at` 在 `now - last_used >= max_idle` 时删除条目，
      未到期条目保留；常量 `TXN_PROXY_MAX_IDLE = 61 min`，注释写明来源
      （服务端 TTL 上界 3600s + 60s 余量）。
- [x] 在 `begin` 入口先 `sweep()`；重跑测试通过。

## Task 5: 装配、文档与门禁

**Files:**
- Modify: `coord-agent/src/lib.rs`、`coord-agent/tests/agent_grpc_test.rs`、
  `docs/production/consumer-contract.md`

- [x] 注册 `TxnV2Server::new(TxnV2Proxy::new(svcs.inner.clone()))`，health
      `set_serving::<TxnV2Server<TxnV2Proxy>>()`，`pub use` 导出。
- [x] `agent_grpc_test` 的服务清单断言加入 `coord.txn.v2.Txn`。
- [x] consumer-contract「Agent 代理面」改为：TSO/KV/Lease/Txn 已代理；
      Watch/Storage 仍直连（下一片）。
- [x] 跑门禁：
      `cargo fmt --all -- --check`；
      `cargo clippy --workspace --lib --bins -- -D warnings`；
      `bash scripts/check-panics.sh`；
      `cargo test -p coord-agent --test agent_v2_txn_proxy_test`；
      `cargo test -p coord-agent --lib proxy_v2::`；
      `cargo test -p coord-agent --test agent_grpc_test`；
      `bash scripts/check-doc-refs.sh`；`cargo audit`；`cargo deny check`。
- [ ] 分支 → PR → CI 全绿 → squash merge → 删分支。

## 验收与证据

- 转发真实发生（假服务记录到同一 `txn_id` 的 Read/Commit）；
- `TXN_CONFLICT` / `TXN_NOT_FOUND` 以 trailer 原码到调用方；
- leader 切换后 Read 仍回到 Begin 的协调节点；
- 无后端时 `UNAVAILABLE`（不假装会话不存在/写成功）；
- `[kv_t].enabled=false` 的 v1 路径字节级不变（本切片只加 v2 服务）。

## 执行记录（2026-10-23）

- `cargo test -p coord-agent --test agent_v2_txn_proxy_test` — 3/3（转发、冲突/未知码、
  提交传输失败后的幂等重试、跨 leader 切换的会话亲和）。
- `cargo test -p coord-agent --lib proxy_v2::` — 7/7（含骨架 fail-closed 与 sweep）。
- `cargo test -p coord-agent --test agent_grpc_test` — 8/8；`agent_v2_proxy_test` 1/1。
- `cargo test -p coord-client --lib` — 60/60（`commit_in_place` 为增量 API）。
- 变异验证：把 `read` 改成重新按当前 leader 开会话 → 亲和用例红（读到 `from-B`）；
  去掉提交失败的登记回填 → 幂等重试用例红（`TXN_NOT_FOUND`）。
- `cargo fmt --check`、`clippy -D warnings`、`check-panics`、`audit`、
  `deny check` 全绿。`check-doc-refs` 在干净检出下无悬空（本机被 .gitignore 的
  pki-assessment 草稿与 .superpowers 历史文件会误报，CI 不可见）。
