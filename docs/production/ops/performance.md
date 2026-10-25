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

## 6. 未覆盖项（#174 仍未完成的部分）

以下项**没有**本机实测数字，验收前必须补：

1. **Commit Sequencer 吞吐/批边界**（D9 默认 256/批）——需要真实并发事务负载，不是存储层基准。
2. **OCC 冲突率与重试放大**（write skew / range 幻读负载）。
3. **Commit Log 写放大**（完整 value 在 MVCC 与 Commit Log 各存一份）与 24h 保留窗口的磁盘估算。
4. **Watch lag**（watermark/sealed 推进在持续写下的端到端延迟）。
5. **Split/Merge 停顿**（迁移期间的写停顿/请求失败率）。
6. **ValidationIndex 内存占用**（key 哈希 + range 区间索引，`max_range` 生命周期累计）。
7. **72h soak** —— 见 §7，当前只跑过 CI 的 120s 冒烟。

## 7. Soak

| 口径 | 状态 |
|:--|:--|
| CI 冒烟（120s，真实 3 进程） | 每个 PR 的 `chaos (kill9 + soak, real processes)` job 执行 `chaos_soak_distributed`；`scripts/collect-evidence.sh soak-smoke` 可产出证据目录 |
| **72h 生产浸泡** | **未执行**（需隔离生产环境；命令如下） |

```bash
# 验收标准：全程无线性一致性违规、无 leader 空窗超时、无数据丢失；
# 报告落 ${COORD_EVIDENCE_DIR}/soak-report.md（运行产物不写仓库）。
COORD_EVIDENCE_DIR=/path/to/artifact-store SOAK_DURATION_SECS=259200 \
  ./scripts/soak-72h.sh
```

在 72h 证据入库（或挂到发布工件）之前，任何「长稳已达标」的表述都不成立。

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
