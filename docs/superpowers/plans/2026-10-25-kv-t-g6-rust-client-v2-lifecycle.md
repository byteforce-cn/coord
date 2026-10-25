# KV-T G6 (#172) — Rust v2 client 生命周期（后台 KeepAlive / 流式 open_put）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 补齐 #172 的 Rust client v2 生命周期：`coord.lease.v2` 的后台续期句柄与 `coord.storage.v2` 的流式上传会话（已知/未知长度），二者都要有明确的停止/放弃语义，不留下无主的 tokio 任务。

**Architecture:** `LeaseV2Keeper` 持有 `(stop sender, JoinHandle)`：周期单次 `KeepAlive`（TTL/3，下限 1s），`release()` 先等在飞续期退出再 `Revoke`（避免 revoke 之后又被续活），Drop 只 abort。`ObjectV2Writer` 与 v1 `ObjectWriter` 同形：首帧 `PutMeta` + 后续 chunk 推进 mpsc→tonic 请求流，`finish()` 关闭流并等响应，`abort()`/Drop 关闭流并 abort 任务；已知长度模式在本地拒绝越过声明的 chunk。

**Spec:** issue #172（Rust client v2 的 KeepAlive/open_put 残余）、`apis/contracts/proto/coord/{lease,storage}/v2/*.proto`。

## Tasks

- [x] `LeaseV2Client::keep_alive_background(lease_id, ttl_seconds) -> LeaseV2Keeper`；
- [x] `LeaseV2Keeper::release`（停止 → 等在飞退出 → revoke）、Drop（停止 + abort）；
- [x] `StorageV2Client::open_put(bucket, object_id, total_size)` / `open_put_unknown`；
- [x] `ObjectV2Writer::{write_chunk, finish, abort, bytes_written, total_size}`；
- [x] v1 形状 `LeaseClient::keep_alive_background` 的 V2 分支复用同一后台任务；
- [x] 测试：后台续期次数/停止后不再续期/revoke 一次；未知长度流式上传的 meta `-1` 与 chunk 顺序；已知长度越界本地拒绝。

## Verification Anchors

| 行为 | 证据 |
|:--|:--|
| 后台续期 + 停止 | `lease_storage_v2_test::lease_v2_background_keep_alive_renews_and_stops` |
| 未知长度流式上传 | `lease_storage_v2_test::storage_v2_open_put_streams_unknown_size` |
| 声明长度越界 | `lease_storage_v2_test::storage_v2_open_put_rejects_chunk_past_declared_size` |
| 全量 | `cargo test -p coord-client`、workspace、CI |

## Residual（#172 仍 open）

- `java-example` 的 v2 集群跑通示例；
- Java ↔ Rust ↔ server 的同一场景互操作脚本（当前两端各自对同一 wire 做 e2e）。
