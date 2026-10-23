# KV-T G6 (#172) Rust client Watch v2 first-frame epoch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** `WatchV2Client` 打开流时携带已学到的 routing epoch，并在服务端守卫返回 `EPOCH_STALE` 时学习新版本后重开流；重连用的 create 帧必须带**当前**版本，而不是永远不提或停在旧版本。

**Architecture:** 复用 `Client::attach_routing_epoch` / `learn_routing_epoch`。`open_stream` 在每次尝试时把当前版本附到 bidi 请求 metadata（0 = 未知不发）；`run` 在打开失败与流中断两处先 learn，再交给既有 `retryable()` 决定重连/结束。订阅语义（至少一次投递、cursor 续订、`HISTORY_UNAVAILABLE` 终止）不变。

**Tech Stack:** Rust / tonic 0.14 / tokio。

**Spec:** issue #172（Rust client v2 GA 的 Watch 残余）、ADR-0015 D12.5；上一刀 `docs/superpowers/plans/2026-10-23-kv-t-g6-client-lease-storage-v2.md`。

## Global Constraints

- `[kv_t].enabled=false`（v1）行为不变：epoch 只出现在 `coord.watch.v2` 的请求上。
- 不新增 wire 字段/错误码；metadata 名不变。
- 只从 `EPOCH_STALE` 的 trailer 学习；`HISTORY_UNAVAILABLE` 仍是终点，不因 learn 改变。
- 重连有界（`Config::max_retries`），耗尽仍返回 `ClusterUnavailable`（现状）。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings

1. **首帧带 epoch，而不是等被拒后才带。** 学到过版本的客户端继续不带 metadata 就等于绕过守卫；watch 与 KV/Txn/Lease/Storage 必须同一口径。
2. **mid-stream 不假设会有 `EPOCH_STALE`。** 服务端只在 handler 入口守卫；流中断处仍调用 learn，是因为同一错误码也可能由节点切换路径返回，且 learn 对非该码是 no-op。
3. **不改订阅语义。** `EPOCH_STALE` 是 `UNAVAILABLE`，已在 `retryable()` 内；本切片只补「学到新版本」，不新增重试类别、不改 cursor 续订。
4. **不做 Watch 的 per-batch epoch。** 版本是流开启时的路由快照；会话中 Region 表变化由服务端 watch coordinator 处理，不需要客户端中途换版本。

## File Structure

- Modify: `coord-client/src/watch_v2.rs` — 模块注释、`open_stream` attach、`run` 的 learn。
- Modify: `coord-client/tests/routing_epoch_test.rs` — mock `coord.watch.v2`：首开 `EPOCH_STALE` + trailer，断言重开带新版本、后续直接携带。
- Modify: `docs/adr/0015-kv-t.md` — D12.5 的客户端清单补 Watch（一句话）。

## Tasks

### Task 1: 客户端

- [x] **Step 1: `open_stream` attach**

每次尝试读当前 epoch 附到请求 metadata；0 = 未知时不发。

- [x] **Step 2: `run` learn**

打开失败与流中断两处先 `learn_routing_epoch(&status)`，再走既有 `retryable()` 分支。

### Task 2: 测试

- [x] **Step 1: mock watch 服务**

记录每次 open 的 epoch；首开返回 `EPOCH_STALE` + `coord-routing-epoch: 3`；成功后下发一条带 cursor 的事件并保持流开启。

- [x] **Step 2: 断言**

订阅收到事件与 cursor；`watch_open` 序列为 `[None, Some(3)]`；取消后订阅结束。

### Task 3: 文档与交付

- [x] **Step 1: ADR**

D12.5 的 Rust 客户端清单加入 Watch。

- [x] **Step 2: 门禁**

fmt / clippy / check-panics / audit / deny / `cargo test -p coord-client` / `check-doc-refs`（干净工作树）。

- [x] **Step 3: 交付**

PR（引用 #172）→ CI 全绿 → merge（`--merge`）→ 删分支；#172 回填进展。

## Review Focus

- 首次 open 是否不带 metadata，学到后**重开**的 create 帧是否带新版本？
- `HISTORY_UNAVAILABLE` 是否仍然直接终止（不被 learn/重试吞掉）？
- watch 之外的 v2 客户端是否未被本切片改动？
- 订阅的 cursor/至少一次语义是否保持不变？
