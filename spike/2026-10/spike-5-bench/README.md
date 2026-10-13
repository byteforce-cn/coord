# Spike 5 · 基准测试（原始数据 + 容量规划）

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 状态：✅ 初版完成（2026-10-13，5/5 测试通过，原始数据逐条落盘，可复现）
> 手册对应：§4 Spike 5｜证据标准：§5
> ⚠️ **非目标硬件**：数据采于 8C/31G 共享测试机（`loadavg` 5–8.4），
> 阈值（§3.3）与目标硬件清单未定稿 ⇒ **不做达标/不达标判定**。

## 产出

| 产出 | 路径 |
|:--|:--|
| 基准数据报告（结果矩阵 / 瓶颈 / 容量规划） | `REPORT.md` |
| 基准集成测试（5 个 `#[ignore]`） | `coord/tests/spike5_bench.rs` |
| 复现脚本（一条命令跑全部 + 落原始数据） | `scripts/repro.sh` |
| 原始数据（逐条 jsonl + 运行日志） | `evidence/raw/` |
| 机器规格 | `evidence/machine.txt` |

## 复现

```bash
# 一条命令：跑全部基准（约 5–8 分钟），原始数据落到 evidence/raw/
bash spike/2026-10/spike-5-bench/scripts/repro.sh

# 或直接跑测试（等价，--test-threads=1 是测量方法的一部分，勿并行）
CARGO_BUILD_JOBS=3 cargo test -p coord --test spike5_bench -- \
  --ignored --nocapture --test-threads=1
```

缩范围（环境变量，全部有默认值）：`SPIKE5_KV_LEVELS`、`SPIKE5_KV_WARMUP_MS`、
`SPIKE5_KV_MEASURE_MS`、`SPIKE5_WATCH_WATCHERS`、`SPIKE5_WATCH_ROUNDS`、
`SPIKE5_STORAGE_WRITES`、`SPIKE5_STORAGE_SAMPLE_EVERY`、`SPIKE5_EVIDENCE_DIR`。

单测粒度：`cargo test -p coord --test spike5_bench -- --ignored --nocapture --test-threads=1 <测试名>`，
测试名 ∈ {`bench_kv_write_throughput`、`bench_txn_size_limit`、`bench_value_size_limit`、
`bench_watch_fanout`、`bench_storage_growth`}。

## 证据索引

| 数据集 | 文件 | 规模 |
|:--|:--|:--|
| KV 写延迟/吞吐（canonical） | `evidence/raw/kv-write-latency.jsonl` + `kv-write-meta.json` | 3860 样本 |
| KV 写方差（3 次辅助运行） | `evidence/raw/aux-kv-variance/run{1,2,3}/` | 各 4.8–6.2k 样本 |
| Txn 操作数阶梯 | `evidence/raw/txn-size-ladder.jsonl` | 14 行 |
| 单值大小阶梯 | `evidence/raw/value-size-ladder.jsonl` | 9 行 |
| Watch 扇出（canonical） | `evidence/raw/watch-fanout.jsonl` + `watch-meta.json` | 20020 行 |
| Watch 未错峰建连（辅助） | `evidence/raw/aux-watch-burst/` | — |
| 存储增长 | `evidence/raw/storage-growth.jsonl` + `storage-meta.json` | 21 行 |
| 运行日志 | `evidence/raw/run-20261013-114453.log` | 本次 canonical 全量输出 |
| 机器规格 | `evidence/machine.txt` | — |

## 关键结论（详见 `REPORT.md`）

- **Txn 操作数上限 = 128**（compare+success+failure 合计）；超出返回
  `InvalidArgument: txn ops N exceeds max 128`。契约注释（`txn.proto` 称「未强制上限」）
  与运行时不一致，需阈值定稿时对齐。
- **单值大小上限 ≈ gRPC 消息 4 MiB**（非存储层限制）；4,194,304 B 写入被拒，
  4,194,276 B 可写但**回读超客户端解码上限**。
- **Watch 1000 连接：1000/1000 成功、20000 次投递 0 丢失**，端到端
  P50 14.1 ms / P95 53.1 ms / P99 107.9 ms。
- **存储增长 2819.5 B/写**（256 B 值）≈ 2.82 GB/百万写。
- **KV 写在共享负载下被 5s 超时淹没**（并发 ≥500 全部 `DeadlineExceeded`），
  成功吞吐运行间方差 0–176 ops/s ⇒ 须在安静机器复跑后方可用于容量规划。

## 边界与红线

- 产出为**决策证据**，不合并 main；工装仅在本 spike 分支登记。
- 原始数据逐条落盘、非汇总；报告中每个统计量均可由 `evidence/raw/` 独立复算。
- 不做达标判定（阈值/目标硬件未定稿）；绝对数字不作承诺。
