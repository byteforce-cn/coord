# KV-T G6 (#172) Rust client routing epoch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** `coord-client` 的 v2 KV / Txn 调用跟随服务端 Region 表版本：从 `EPOCH_STALE` 的 trailing metadata 学到 `coord-routing-epoch`，后续请求带上它；被拒的请求用新版本有界重试，而不是把「表刚变过」暴露给调用方。

**Architecture:** epoch 状态挂在 `Client` 的共享 `ClientInner`（`AtomicU64`，0 = 未知 → 请求不带 metadata，保持 D12.5 的「缺失不校验」）。所有 v2 请求在**每次尝试**时读取当前 epoch 并附到 metadata；只有 `EPOCH_STALE` 会触发「学新版本 + 重试」，重试次数复用现有 `Config::max_retries`。KV 写走既有 `execute_write_with_retry`（补 epoch 学习），Range 与 Txn 会话 RPC 各自走同一口径的有界重试。

**Tech Stack:** Rust / tonic / tokio。

**Spec:** issue #172（Rust client v2 GA 的失败恢复部分）、issue #161 客户端残余、ADR-0015 D12.5；上一刀 `docs/superpowers/plans/2026-10-23-kv-t-g5-epoch-convergence.md`（服务端多节点收敛）。

## Global Constraints

- `[kv_t].enabled=false`（v1）行为不变：epoch 只在 v2 客户端请求上附加，v1 路径不读也不发。
- 不新增 wire 字段/错误码；epoch 仍是 Region 表 `version`，metadata 名不变。
- 重试有界（`Config::max_retries`），不在迁移窗口里无限等待；`EPOCH_STALE` 之外的错误不改变既有分类。
- epoch 只能单调前进（`fetch_max`）：不因落后节点的回传而降级，否则会把旧表放行。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings

1. **0 = 未知，不发 metadata。** 单 Region / 启动期客户端本来就不带；把 0 发出去会被服务端当成「客户端表版本 0」而拒绝。学到非 0 版本后才附加。
2. **只从 `EPOCH_STALE` 的 trailer 学习。** 成功响应不带 trailer（服务端只在错误路径回传），因此不假设成功响应可学；缺失 trailer（例如迁移 fence 的 `EPOCH_STALE`）不改变本地版本。
3. **`fetch_max` 而不是覆盖。** 客户端已经学到 v2 时，落后节点回传 v1 说明该节点还没 apply；降级会让请求在旧路由上通过守卫（正是要禁止的）。宁可重试到节点收敛或重试耗尽。
4. **Txn 只重试 `EPOCH_STALE`。** `begin`/`read`/`commit`/`abort` 在入口守卫处被拒时没有副作用；换节点重试仍不做（会话只活在协调节点，D12.2）。
5. **Watch / Lease / Storage 不在本切片。** Watch 目前不发 epoch（缺失 = 不校验），其断线重连已覆盖传输错误；Lease/Storage 的 v2 客户端方法归 #172 后续切片。

## File Structure

- Modify: `coord-client/src/client.rs` — `ClientInner` 的 epoch、attach/learn 辅助、KV v2 三条路径。
- Modify: `coord-client/src/txn_v2.rs` — `begin`/`read`/`commit`/`abort` 的 epoch 附加与有界重试。
- Create: `coord-client/tests/routing_epoch_test.rs` — mock `coord.kv.v2` / `coord.txn.v2`：首次 `EPOCH_STALE` + trailer，断言重试带上新 epoch、后续调用直接携带。
- Modify: `docs/adr/0015-kv-t.md` — D12.5 补客户端学习/重试口径（一句话，不复制实现）。

## Tasks

### Task 1: Client epoch 状态与 KV v2

- [x] **Step 1: 状态与辅助**

`ClientInner.routing_epoch: AtomicU64`（两个构造器初始化 0）；`routing_epoch()` / `attach_routing_epoch(&mut Request)` / `learn_routing_epoch(&Status)`（仅 `EPOCH_STALE` + trailer，`fetch_max`）。

- [x] **Step 2: KV 路径**

`put`/`delete` 在闭包内每次尝试 attach；`execute_write_with_retry` 的错误分支先 learn。`range` 改为「拿通道 → attach → 调用；仅 `EPOCH_STALE` 按 `RetryState` 重试」。

- [x] **Step 3: 测试**

`cargo test -p coord-client --test routing_epoch_test`：KV 首次拒/二次成功，metadata 序列 `[None, Some(2)]`，第二条请求直接带 `2`。

### Task 2: Txn v2

- [x] **Step 1: 有界重试辅助**

`retry_epoch_stale(client, attempt)`：`attempt` 每次重建请求（attach 当前 epoch）；`EPOCH_STALE` → learn + 退避重试，其他错误原样返回 `Status` 由调用方映射（保留 `TXN_CONFLICT` 的业务结果语义）。

- [x] **Step 2: 四个会话 RPC**

`begin`（失败归还 channel）、`read`、`commit`（先判冲突）、`abort` 接入；测试覆盖 begin 与 read 的「首次拒/二次成功」。

### Task 3: 文档与门禁

- [x] **Step 1: ADR**

D12.5 补：客户端从 `EPOCH_STALE` trailer 学版本、后续请求携带、被拒请求有界重试。

- [x] **Step 2: 文档引用与门禁**

`bash scripts/check-doc-refs.sh`；`cargo fmt --all -- --check`；`cargo clippy --workspace --lib --bins -- -D warnings`；`bash scripts/check-panics.sh`；`cargo audit --no-fetch`；`cargo deny --offline check`。

- [x] **Step 3: 交付**

PR → CI 全绿 → merge → 删分支；#172 回填进展（Rust client 的 KV/Txn epoch 部分；Watch/Lease/Storage 与 Java SDK 仍 open）。

## Review Focus

- 首次请求是否**不带** epoch（0 = 未知），学到后是否每条重试都重新 attach？
- 落后节点回传更小版本时是否保持本地较大版本（不降级）？
- `EPOCH_STALE` 的重试是否有界，且只在入口拒绝路径上发生（不重放已执行的写）？
- `TXN_CONFLICT` 是否仍是 `TxnCommit::Conflict` 而不是被重试/映射成错误？
- v1 客户端路径是否完全不受影响？
