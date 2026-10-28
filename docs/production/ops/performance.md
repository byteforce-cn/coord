# KV-T 性能与容量报告

> Owner: maintainers ｜ Last verified: 2026-10-25

- **状态**：**部分实测**。本文只登记**已跑过**的数字与命令；未覆盖项在 §6 明确列出，
  不把「没测」写成「达标」。
- 口径与默认值以 ADR-0015 D9 为准；调参只改数值，不改语义（改语义必须新增 ADR）。

---

## 1. 环境与命令

| 项 | 值 |
|:--|:--|
| 主机 | 8 vCPU / loopback 单机（无跨机网络） |
| 构建 | `cargo test --release -p coord --test perf_bench`（`PERF_GATE=1`） |
| 测量方法 | 全量基准串行执行（`--test-threads=1`）：并行跑多个重基准会抢同一条 fsync 路径，噪声带宽大于 20% 判据 |
| 入口 | `bash scripts/bench-ci.sh`（release；CI 每周跑，报告作为工件上传） |
| 结论 | 9 个基准全绿；多 Region 硬闸 `>= 单 Region 基线 80%` **PASSED** |

## 2. 写路径（MvccStorage，含加密 + Changelog）

| Value | 迭代 | 耗时 | 吞吐 |
|:--|--:|--:|--:|
| 64 B | 2000 | 1.716 s | 1165 ops/s |
| 256 B | 2000 | 1.886 s | 1060 ops/s |
| 1 KiB | 1000 | 2.171 s | 461 ops/s |
| 4 KiB | 1000 | 1.490 s | 671 ops/s |

裸 Redb 写（无 MVCC 开销）同机 1420–1687 ops/s ⇒ **写路径是 fsync 受限**，MVCC 的
加密/Changelog 只吃掉约 0–40%（加密是随机化开销，不看字节数）。

写延迟（256 B）：

| 平均 | P50 | P95 | P99 |
|--:|--:|--:|--:|
| 915 µs | 751 µs | 1468 µs | 1664 µs |

## 3. 读路径

| 场景 | 迭代 | 耗时 | 吞吐 |
|:--|--:|--:|--:|
| Point read（单 key） | 20000 | 15.9 ms | 1.26 M ops/s |
| Prefix scan（100 keys） | 2000 | 299 ms | 6697 ops/s |
| Prefix scan（1000 keys） | 500 | 255 ms | 1963 ops/s |

读吞吐远高于写：读走内存/redb 读事务，不落盘；**读不是瓶颈**。

## 4. 多 Region 写吞吐（共享存储引擎）

| Region 数 | 256 B × 5000 keys | 三次采样中位数 |
|--:|--:|--:|
| 1 | 479 / 486 / 715 ops/s | 486 ops/s |
| 5 | 729 / 511 / 523 ops/s | 523 ops/s |
| 10 | 782 / 556 / … ops/s | 采样见 CI 报告 |

Region 数增加**不劣化**（每个 Region 独立 raft/mvcc，写放大只在共享的 fsync 路径）；
硬闸 `>= 单 Region 中位数 × 0.80` 通过。

## 5. Watch 扇出

| 订阅者 | 事件数 | 耗时 | 吞吐（events/s） | 总投递（events/s） |
|--:|--:|--:|--:|--:|
| 10 | 200 | 464 µs | 430 632 | 4 306 317 |
| 50 | 200 | 2.85 ms | 70 293 | 3 514 642 |
| 100 | 200 | 7.42 ms | 26 967 | 2 696 667 |
| 200 | 100 | 5.48 ms | 18 246 | 3 649 261 |

扇出成本近似线性；单条事件 ≈ 2–3 µs × 订阅者。**这是进程内 fanout 成本**，不含
客户端网络与 Commit Log 读盘。

## 6. 事务面实测（#174 收口）

环境同 §1（8 vCPU、loopback、无跨机网络）。测量时 72h 浸泡同时在跑（约 0.3
txn/s），单次数字偏保守；下表是 release 构建的实测值，命令：

```bash
PERF_GATE=1 cargo test -p coord --release --test kv_t_g6_perf_v2_test \
  -- --ignored --nocapture --test-threads=1
```

### 6.1 Commit Sequencer 吞吐 / 批边界

| 客户端 | 事务 | 墙钟 | 吞吐 | 平均批大小 | 冲突 | p50 | p95 |
|--:|--:|--:|--:|--:|--:|--:|--:|
| 32 | 800 | 5.2 s | 152 txn/s | 1.0 | 0 | 171 ms | 219 ms |
| 32 | 800 | 7.0 s | 115 txn/s | 1.0 | 0 | 177 ms | 1103 ms |

**上限是串行提案率**：RPC 路径每次 `Commit` 只提交 1 个候选
（`validate_and_commit(vec![candidate])`），D9 的 `commit_sequencer_batch=256`
在这条路径上不生效（`sequencer_txns / sequencer_batches` 恒为 1.0）。把批真正
用起来需要「多事务合并提案」的入口（新增契约/延迟权衡），G6 只记录现状。

### 6.2 OCC 冲突率与重试放大

| 客户端 | 尝试 | 冲突 | 冲突率 | 成功 | 耗时 |
|--:|--:|--:|--:|--:|--:|
| 16 | 192 | 180 | 93.8% | 12 | 12.0 s |

热 key 写冲突按设计不写决策（客户端换 `start_ts` 重跑）：一次成功的重试放大
≈ 8 倍尝试；冲突率由 key 争用决定，不是服务端瓶颈。

### 6.3 Watch 端到端延迟

25 个事务，从 `Commit` 返回到客户端收到同 `commit_ts` 事件：

| p50 | p95 | max |
|--:|--:|--:|
| 540 ms | 578 ms | 579 ms |

延迟由 CommitWatermark/批下发节拍主导（实测分布很窄），不是网络 RTT。
注意：`coord.kv.v2` 直写不产生 CommitRecord，**不进入** v2 watch 流（B-KVT-13）。

### 6.4 写放大（稳态，3 副本合计）

300 事务 × 2 key × 256 B = 153,600 B 用户字节 → **11.1 MB** 磁盘增量：

| 用户字节 | 磁盘字节 | 放大 | 每事务磁盘 |
|--:|--:|--:|--:|
| 153,600 B | 11,124,736 B | **72.4×** | 37 KB |

小 value 下固定开销主导（Region raft 日志 + region 0 决策日志 + CommitRecord +
redb 页）；24h 容量按「事务数 × 37 KB ÷ 3（单副本）」估算，不要用用户字节直接估。

### 6.5 ValidationIndex / 决策日志容量

没有 GC（`/_sys/seq/{ridx,iidx,decision}/` 只增不减）：容量随「不同 key 数 /
不同区间数 / 决策数」单调增长；`catch_up_sequencer_index` 能从决策日志重建索引，
但没有回收路径。上界、排除项与后续条件见 `boundaries.md` B-KVT-13。

### 6.6 Split/Merge 停顿

**仍无数字**。迁移路径本身的两个缺陷已修（PR #273 导出水位判据、PR #275 目标组
leader 转发，回归见 `kv_t_g5_split_migrator_test` / `kv_t_g5_region_command_forward_test`），
但 PD 侧没有 Region 统计来源：`PlacementDriver::handle_region_heartbeat` 没有生产
调用方、`region_sample_keys` 只有测试注入 ⇒ `approximate_keys` / `approximate_size`
恒为 0，split-checker 永不产生 operator（缺口与修法见 #278）。

复现器保留：

```bash
PERF_GATE=1 cargo test -p coord --test kv_t_g6_perf_v2_test perf_split_write_pause \
  -- --ignored --nocapture --test-threads=1     # 需 debug 构建：调度解冻钩子只在 debug 编译
```

当前 fail-closed 在 `region_split_total = 0`（「the PD-driven split never ran」），
不是「停顿很小」的证据。统计上报落地后按同一命令取值并回填本节。

## 7. Soak

| 口径 | 状态 |
|:--|:--|
| CI 冒烟（120s，真实 3 进程，v1） | 每个 PR 的 `chaos (kill9 + soak, real processes)` job 执行 `chaos_soak_distributed`；`scripts/collect-evidence.sh soak-smoke` 可产出证据目录 |
| v2 冒烟（45s + 2 次 region 0 leader kill，真实 3 进程） | `KVT_SOAK_SECS=45 KVT_SOAK_FAULT_SECS=15 cargo test -p coord --release --test kv_t_g6_soak_v2_test soak_v2_mixed_workload_stays_consistent -- --ignored --nocapture --test-threads=1`（每次 PR 的混沌 job 同时跑 v2 决策面故障套件；本行是长窗口冒烟命令） |
| **72h 生产浸泡（v2）** | ✅ **通过**：2026-10-25T15:05:11Z → 2026-10-28T15:18:46Z（259 200 s，main `c53bbfb`），`soak-report.md` 结论「通过（无一致性违规）」；103 630 轮、TSO 2.555 亿、863 次 kill -9 region 0 leader 注入后恢复、watchdog 0 次、节点日志 0 ERROR、无 `passed unapplied decision` / `decision apply stopped` / `behind the applied revision` 锚点。看门狗（卡住 → 告警 → abort）见 PR #266；证据目录 `/tmp/coord-soak72/`（报告全文贴在 #174） |

```bash
# KV-T GA 的证据口径是 v2 数据面的 72h 浸泡：
#   TSO 单调 / 直写 safe_ts 可见 / 跨区事务 commit_ts 可读 / 每 300s kill -9 region 0 leader。
# 验收标准：全程无一致性违规、无 leader 空窗超时、无数据丢失；
# 报告落 ${COORD_EVIDENCE_DIR}/soak-report.md，节点日志落 ${COORD_EVIDENCE_DIR}/v2-nodes。
COORD_EVIDENCE_DIR=/path/to/artifact-store SOAK_SUITE=v2 SOAK_DURATION_SECS=259200 \
  ./scripts/soak-72h.sh
# v1 数据面浸泡（默认 SOAK_SUITE=v1）仍可单独跑；它不是 KV-T GA 的替代证据。
```

在 72h 证据入库（或挂到发布工件）之前，任何「长稳已达标」的表述都不成立。
本次实例的证据是 `/tmp/coord-soak72/`（报告 + driver.log + 三节点日志），报告全文
贴在 #174；最终 GA 提交上的长稳证据由 Jepsen v2（#272）承担（维护者 2026-10-28
裁定，见 `kv-t-ga-gate.md` §3.1）。

## 8. 调参口径（只改数值）

来源：`coord/src/config.rs` 的 `[kv_t]` 段（默认值 = ADR-0015 D9）。

| 参数 | 默认 | 允许范围/说明 |
|:--|--:|:--|
| `tso_batch_size` | 1024 | 1–65536；批只保证唯一/单调，允许空洞 |
| `commit_sequencer_batch` | 256 | ≥1；越大吞吐越高、尾延迟越差 |
| `commit_log_shard_records` | 1_000_000 | 先到先 seal；影响单 shard 回放时间 |
| `commit_log_shard_secs` | 3600 | 与条数阈值取先到者 |
| `shard_seal_timeout_secs` | 300 | 超时告警；不阻塞已 seal 数据 |
| `watch_retention` | 86400 | 1h–7d；超出 `HISTORY_UNAVAILABLE` |
| `max_txn_bytes` | 4 MiB | 硬上限，超限 `TXN_TOO_LARGE` |
| `max_read_set` | 4096 | 读集条目上限 |
| `max_range` | 10000 | 单次 range 扫描上限 |
| `read_ts_wait_ms` | 2000 | `read_ts > safe_ts` 的等待上限（超时 `SAFE_TS_LAG`） |
| `txn_ttl_secs` | 60 | 会话 TTL；长事务会阻塞 split drain |

调参后的回归判据：`PERF_GATE=1 bash scripts/bench-ci.sh` 全绿 + 本报告 §2–§5 的
数字不劣化超过 20%（跨运行比较只在存在基线文件时执行）。

## 9. 瓶颈与上限（当前认知）

- **单 Commit Sequencer 是串行化点**：serializable 需要全局校验；当前证据只覆盖
  存储层吞吐，sequencer 的真实上限未测（§6.1）。
- **写路径 fsync 受限**：单机约 1–1.7k ops/s（256 B），跨机延迟会进一步压低；
  这不是 KV-T 特有，但决定了单节点写上限。
- **Commit Log 写放大**：完整 value 双写（MVCC + Commit Log），24h 保留窗口的容量
  必须按「写入字节 × 2 × 保留时长」估算；尚未实测（§6.3）。
- **Watch fanout 是 CPU 线性成本**：单事件 × 订阅者数，10 万级订阅需要分片/限流。
