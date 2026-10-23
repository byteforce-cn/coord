# KV-T G5 (#161) 多节点 routing epoch 收敛验收 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 补上 #161 最后一条验收缺口——`coord-routing-epoch` 的取值在**每个节点**上跟随权威 Region 表收敛：3 节点 region 0 raft 把表从 v1 改到 v2 后，任一节点的 v2 数据面入口都用本地已 apply 的 v2 拒绝携带 v1 的请求，并在 trailing metadata 回传 v2。

**Architecture:** 复用 `coord-server/tests/kv_t_g5_region_table_test.rs` 的 3 节点装配（raft 端口 + 本地 `MvccStorage` + `RegionManager`）。表变更经 leader 提交、各节点 apply；每个节点用共享的 `mvcc` / `manager` 起一个 `coord.kv.v2` gRPC 服务。入口守卫在比较前按本地已 apply 表刷新（`guard_routing_epoch`），因此**未主动 refresh 的节点**也会在首个请求上收敛到新版本，而不是把自己的落后状态误报成客户端过期。

**Tech Stack:** Rust / openraft / tonic / tokio。

**Spec:** issue #161 验收「单测/多节点；旧 epoch 全入口拒绝；错误可重试」、ADR-0015 D12.5；上一刀 `docs/superpowers/plans/2026-10-23-kv-t-g5-cross-node-drain.md`（#167）。

## Global Constraints

- `[kv_t].enabled=false`（默认）不改变 v1 行为：测试只在显式 `KvTState::new(true, ..)` 的节点上挂 v2 服务。
- 不新增 wire 字段/错误码；epoch 仍是 Region 表 `version`（D12.5）。
- 入口判据保持「缺失不校验、一致放行、不一致 `ABORTED` + `EPOCH_STALE` + trailer、非法 `INVALID_ARGUMENT`」。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings

1. **测试挂在现有 3 节点 region 表套件里，不新建集群装配。** 本切片要验的是「表版本 → 每节点入口判据」，与 #159 的表收敛共用同一套真实 raft；再抄一份 boot 只会多一处漂移。
2. **用 `commit_ts=0` 作为「通过守卫」的探针。** 守卫在参数校验之前，`commit_ts=0` 的 Put 若返回 `INVALID_ARGUMENT` 就证明 epoch 已被放行；这比依赖未装配的 Region 运行时更稳定（不会因为路由/raft 状态变成别的错误）。
3. **不在本切片做客户端刷新重试。** `coord-client` 学习 epoch、收到 `EPOCH_STALE` 后刷新重试归 #172（#161 评论已如此归属）。

## File Structure

- Modify: `coord-server/tests/kv_t_g5_region_table_test.rs` — 新增多节点 epoch 收敛用例与 v2 KV 服务装配。
- Modify: `docs/adr/0015-kv-t.md`、`docs/production/consumer-contract.md` — 仅在措辞需要时补「多节点收敛」判据（若现状已准确则不动）。

## Tasks

### Task 1: 多节点 epoch 收敛用例

- [ ] **Step 1: 装配每节点 v2 KV 服务**

在 `ClusterNode` 上增加 `v2_node()`（共享 `mvcc`/`manager`，`node_id`、`KvTState::new(true, ..).with_implemented([Kv])`）与起服务的辅助；测试里为每个节点绑定独立 listener。

- [ ] **Step 2: 断言入口判据**

表 v1 收敛后每个节点 refresh 到 1；leader 提交 split 批到 v2 并等三副本收敛。**不主动 refresh**，对每个节点：
- `epoch=1` 的 Put（`commit_ts=0`）→ `ABORTED` + `EPOCH_STALE`，trailer `2`；
- `epoch=2` 的同请求 → `INVALID_ARGUMENT`（守卫放行，参数校验拒绝）；
- 不带 epoch → `INVALID_ARGUMENT`；
- 服务后 `manager.table_version() == 2`。

- [ ] **Step 3: 相关套件**

`cargo test -p coord-server --test kv_t_g5_region_table_test`（3 用例）；`cargo test -p coord --test kv_t_g5_routing_epoch_test`。

### Task 2: 文档与门禁

- [ ] **Step 1: ADR / consumer-contract**

若 D12.5 未写「每个节点用本地已 apply 版本判定；比较前按表刷新」，补一句，不复制实现细节。

- [ ] **Step 2: 文档引用**

`bash scripts/check-doc-refs.sh`（悬空 0）。

- [ ] **Step 3: 门禁**

`cargo fmt --all -- --check`；`cargo clippy --workspace --lib --bins -- -D warnings`；`bash scripts/check-panics.sh`；`cargo audit`；`cargo deny check`。

- [ ] **Step 4: 交付**

PR → CI 全绿 → merge → 删分支；#161 回填证据并关闭（客户端刷新重试指向 #172）；#167 回填「仅余多节点 live 故障注入，归 #175」并关闭。

## Review Focus

- 表变更后，**没主动 refresh 的节点**是否在首个请求上收敛，而不是把 stale 判成通过？
- 三个节点是否都以自己的 `table_version()`（而不是共享常量/leader 版本）判定？
- 通过守卫的探针是否真的越过了守卫（`INVALID_ARGUMENT` 而非 `EPOCH_STALE`/`FAILED_PRECONDITION`）？
- v1（`[kv_t]` 关闭）路径是否完全没被该测试改动？
