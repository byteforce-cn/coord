# MQ v2 性能与容量报告

> Owner: maintainers ｜ Last verified: 2026-11-14

- **状态**：**部分实测**。只登记已跑过的数字与命令；未覆盖项在 §7 明确列出，
  不把「没测」写成「达标」。
- 语义与默认值以 ADR-0016 为准；阈值是**本机回归下限**（基线 ×0.6 左右），
  不是 SLO 校准、也不是跨机容量承诺。
- 与 KV-T 的报告分开：`docs/production/ops/performance.md` 是 KV-T 面；本文只覆盖
  `coord.mq.v2`。

---

## 1. 环境与命令

| 项 | 值 |
|:--|:--|
| 主机 | 8 vCPU / 31 GiB / loopback 单机（无跨机网络） |
| 夹具 | `coord/tests/support/mq_v2_node.rs`：单进程内 multi-Region（2 个日志分片 + 1 个组 shard），真实 Raft 提交与 redb 段日志 |
| 构建 | release |
| 写路径基准 | `PERF_GATE=1 cargo test --release -p coord --test mq_v2_bench -- --ignored --nocapture --test-threads=1` |
| 浸泡 | `PERF_GATE=1 MQ_V2_SOAK_SECS=600 cargo test --release -p coord --test mq_v2_soak -- --ignored --nocapture --test-threads=1` |
| 测量方法 | 串行（`--test-threads=1`）：并发跑多个重基准会抢同一条 fsync 路径，噪声大于阈值带宽 |
| 阈值 | `PERF_GATE=1` 时由测试断言；没有该变量直接失败（未跑 ≠ 通过） |

## 2. 写路径（Publish，128 B payload）

| 场景 | 条数 | 吞吐 | p50 | p99 |
|:--|--:|--:|--:|--:|
| 顺序（1 client） | 1000 | 283 ops/s | 2024 µs | 36182 µs |
| 并发（64 clients × 50） | 3200 | 2133 ops/s | 28833 µs | 54363 µs |

- 并发段 `MqAppendBatch` 平均批深度 **5.90 req/entry**（3200 requests / 542 raft entries）：
  同分片多请求被 batcher 合并，是并发吞吐高于顺序段的主因。
- 顺序段的 p99 由 fsync/批等待尾部决定；同一台机器上两次运行的吞吐差异约
  10–15%（283–320 ops/s），报告取 `PERF_GATE=1` 的那次。
- **瓶颈是提交路径**（Raft 多数派 + apply），不是客户端 gRPC：并发只把等待摊薄。

## 3. 读路径与组（Poll / Ack / Rebalance）

| 场景 | 条数 | 吞吐 | 说明 |
|:--|--:|--:|:--|
| Poll（200 条/批，10 批） | 2000 | 4783 msgs/s | Leader 只读，不发 Raft 提案 |
| AckBatch（≤256 条/批） | 2000 | 5563 acks/s | 组 shard 单条 Raft 命令/批 |
| 再平衡（4 members / 4 partitions） | — | **0.14 s** | 每次 Join 后等组回到 STABLE 再进下一个成员 |

- Poll 吞吐远高于写：读不落盘；响应按消息整条截断（不切消息）。
- AckBatch 的批上限 256 与组 shard 数（默认 4）决定 Ack 侧上限；当前夹具 1 个
  组 shard，数字不代表多组 shard 聚合。
- 再平衡耗时包含 Join 后等待分配可见；0.14 s 是**本机空载**数字，不含客户端
  重启/网络抖动。

## 4. 浸泡（publish → poll → ack，600 s）

| 时长 | 发布 | 消费（唯一 offset） | Ack | 吞吐 | RSS（夹具进程） |
|--:|--:|--:|--:|--:|--:|
| 600 s | 261206 | 261206 | 261206 | 435 msgs/s | 38 MiB → 547 MiB |

不变量（任一不成立即失败）：

1. 每个 partition 的 offset 恰为 `0..published-1`（不丢、不重、不复用）；
2. 消费到的 `(partition, offset)` 唯一集合 == 发布集合；
3. 结束时 `DescribeGroup` 的 lag 全 0、committed == published；
4. 心跳全程成功（单成员不触发再平衡）。

**RSS 不作通过判据**：夹具在测试进程内跑 Raft/MQ 状态机，默认保留 7 天且
idempotency 索引按保留窗口保存；实测 38 MiB → 547 MiB 的线性增长是**容量行为**
（约 2 KiB/条，含 Raft 日志与段索引），不是泄漏证据。泄漏检测需要真实进程 +
稳态窗口对比（§7）。

- 4 个生产者合计 435 msgs/s（fsync 受限），消费与 Ack 全程跟上，结束时无积压。
- 15 s 冒烟（9276 条 / 618 msgs/s）与 600 s 正式跑的吞吐差异来自生产者数量与
  编译后热路径；报告取 600 s 的数字。
- 重跑命令见 §1；数字随机器/负载变化，阈值仍是本机回归下限。

## 5. 容量口径（默认值）

| 项 | 默认 | 含义 |
|:--|:--|:--|
| `log_shards` | 8 | 分区按最少分区数分到各日志分片；同分片共享 Leader/fsync |
| `group_shards` | 4 | 组名 FNV-1a 64 取模；Ack/Join/Heartbeat 落组 shard Leader |
| `max_message_bytes` | 1 MiB（上限 8 MiB） | 单条上限；超出整批不追加 |
| `segment_bytes` | 64 MiB | 段封口大小 |
| `retention_secs` | 7 d（1 h–30 d） | 按**整段**回收，至少保留最新段 |
| `max_topic_bytes` | 1 GiB | 记账 = 段 + DLQ + 幂等索引；DLQ 在主段保留期内双份计费 |
| `max_partitions_per_shard` | 1024 | 建 topic 时的分区上限 |

容量结论：同分片的分区共享同一条 fsync 路径，8 个日志分片在单机上是**写吞吐的
上限来源**；并发 publish 的 2133 ops/s 是 2 分片夹具的数字，分片数增加不会线性
放大（共享磁盘）。需要更高写吞吐时先扩 `log_shards`（新集群）或做 MovePartition
（post-GA，ADR-0016 非目标）。

## 6. 开放问题（不在本报告内声称已解决）

1. **热分片**：同一分片多分区共享 Leader/fsync；默认 8 分片是否够取决于分区
   分布与 payload。当前没有跨分片写吞吐的数字。
2. **组 shard 吞吐**：4 个组 shard 的聚合 Ack 上限未测；当前只测了单组 shard。
3. **快照大小/恢复时间**：段数据随保留期增长；M1-6 的备份/恢复预算未做容量演练。
4. **DLQ 双份存储**：主段保留期内双份计费；1 GiB 默认配额在生产负载下是否合
   理需要真实容量数据。
5. **跨机网络**：所有数字是 loopback；跨机 Raft/gRPC 的延迟与带宽未测。

## 7. 边界（必须与数字一起读）

- **不是 SLO 校准**：阈值是回归下限（基线 ×0.6），不是 14 天真实曲线的校准值
  （对齐 `observability.md` §2.4 的同一口径）。
- **夹具不是生产拓扑**：单进程内多 Region，无跨机网络、无 3 副本复制、无对象
  存储/manifest 路径。
- **浸泡不是泄漏测试**：见 §4；RSS 只记录，不作判据。
- **未跑**：跨机集群、多组 shard 聚合、72h 长稳、容量打满（quota full）、快照
  安装期间的吞吐。
