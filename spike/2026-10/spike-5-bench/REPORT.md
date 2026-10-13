# Spike 5 · 基准数据报告（KV 写 / Txn 上限 / 单值上限 / Watch 扇出 / 存储增长）

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 手册对应：§4 Spike 5（原始数据 + 容量规划）｜ 证据标准：§5
> 工装：`coord/tests/spike5_bench.rs`（5 个 `#[ignore]` 集成测试，真实单节点 Raft）
> 复现：`bash spike/2026-10/spike-5-bench/scripts/repro.sh`
> 原始数据：`evidence/raw/`（逐条 jsonl/tsv + 运行日志 + `evidence/machine.txt`）

## 0. 免责声明与判定前置条件（先读）

**本报告只采原始数据与统计，不做「达标 / 不达标」判定。**

1. **非目标硬件判定**：数据在 8C/31G 共享测试机（Intel i7-1165G7，见
   `evidence/machine.txt`）上采集。手册 §3.3 的**目标硬件清单尚未定稿**，因此
   **本报告的任何绝对吞吐 / 延迟数字都不能作为目标硬件下的判定依据**。
2. **canonical 为安静机复跑**：canonical 数据采集于 2026-10-13 12:36（集成分支
   `spike/2026-10-workflow-v2`，机器空闲，`/proc/loadavg` 1.5–2.5），日志
   `evidence/raw/run-20261013-123644.log`。合并 Spike 2/3 期间的共享负载首跑
   （loadavg 5.0–8.4，KV 并发 ≥500 时 100% 撞服务端 5s 写超时）保留为
   `evidence/raw/aux-shared-load/`，仅作「超容量行为 / 负载敏感性」证据，
   **不作为容量结论**。每个数据集自带 `loadavg_start/end` 字段以携带上下文。
3. **Profile**：运行命令为 brief 指定的 `cargo test`（`[profile.dev] opt-level=1`），
   非 `--release`。仓库自带的性能闸（`scripts/bench-ci.sh`）用 release 口径，二者
   不可直接比较。
4. **判定前置条件**：10-18 阈值定稿（含「依据栏」与「不达标后果」）+ 目标硬件清单
   + 安静机器复跑。在此之前 §6 的容量规划是**带假设的推算**，不是承诺。

## 1. 摘要

| 维度 | 关键原始结果（见 §4） |
|:--|:--|
| KV 写 | 安静机并发 100/500/1000：成功吞吐 **635.9 / 723.6 / 691.8 ops/s**，P50 **134 / 649 / 1331 ms**，P99 **512 / 970 / 2131 ms**，三档 0 错误 0 超时（dev profile，见 §4.1） |
| Txn 操作数上限 | 默认 `max_txn_ops = 128`（compare+success+failure 合计）；129 起 `InvalidArgument: txn ops N exceeds max 128`。**compare 也计数** |
| 单值大小上限 | 写入上限 ≈ gRPC 消息 4 MiB（`4,194,276 B` 写成功、`4,194,304 B` 被拒）；~4 MiB 值**回读会超客户端解码上限**。**非存储层限制** |
| Watch 扇出 | 1000/1000 连接成功、20 轮 20000 次投递 **0 丢失**；端到端 P50 **6.1 ms** / P95 **13.6 ms** / P99 **16.6 ms** |
| 存储增长 | 256 B 值：**2819.5 B / 写**（≈ 2.82 GB / 百万写）；单写者吞吐 **380.3 写/s**（安静机） |
| 瓶颈 | 单节点 Raft **串行提交 + 每写持久化**；并发升高只增加排队与延迟，成功吞吐在 **~640–725 ops/s** 附近饱和 |

## 2. 测量口径（场景与取样）

- **被检对象**：`coord/tests/common/mod.rs::start_test_server()` 装配的真实单节点
  openraft + KV/Txn/Lease/Watch（与生产同口径挂 `WatchDispatcher` 到状态机）。
  每档并发、每个测试各起独立 server；server 与压测客户端**同机同进程**。
- **闭环取样**：每个 worker 串行发请求并逐请求计时（`lat_us` 含排队与提交），
  不做 coordinated-omission 修正。每条样本带相对时钟 `t_us`（相对本测试首个
  `Instant`），吞吐可由原始数据独立复算。
- **KV**：并发 100/500/1000，每 worker 一条独立连接（避免 HTTP/2
  `max_concurrent_streams` 串行化）；统一绝对窗口「预热 2 s + 测量 10 s」，
  吞吐 = 窗口内**成功** ops / 10 s。value 256 B。
- **Watch**：1000 个订阅端各一条独立连接、订阅**同一 key**；每轮一次 Put 后等待
  各订阅端事件，延迟 = 发起 Put → 订阅端收到。建连以 2 ms/条错峰（见 §4.4 说明）。
- **存储**：单个客户端顺序写 20000 个唯一 key × 256 B，每 1000 写采一次 data-dir
  递归字节数。
- **串行执行**：`--test-threads=1` 是测量方法的一部分（并行基准互抢 fsync 路径）。

## 3. 原始数据索引

| 数据集 | 路径 |
|:--|:--|
| KV 写延迟/吞吐（canonical，安静机） | `evidence/raw/kv-write-latency.jsonl` + `kv-write-meta.json` |
| KV 写（共享负载首跑，超容量行为） | `evidence/raw/aux-shared-load/` |
| KV 写（3 次辅助运行，共享负载下证方差） | `evidence/raw/aux-kv-variance/run{1,2,3}/` |
| Txn 操作数阶梯 | `evidence/raw/txn-size-ladder.jsonl` |
| 单值大小阶梯 | `evidence/raw/value-size-ladder.jsonl` |
| Watch 扇出（canonical） | `evidence/raw/watch-fanout.jsonl` + `watch-meta.json` |
| Watch 未错峰建连（辅助） | `evidence/raw/aux-watch-burst/` |
| 存储增长 | `evidence/raw/storage-growth.jsonl` + `storage-meta.json` |
| 运行日志（带时间戳） | `evidence/raw/run-20261013-123644.log`（canonical）；`run-20261013-114453.log` 存 `aux-shared-load/` |
| 机器规格 | `evidence/machine.txt` |

所有 jsonl 为**逐条**样本（非汇总）；表 §4 的每个统计量都可由 §3 原始数据独立复算
（`python3` 排序取百分位即可）。

## 4. 结果矩阵

### 4.1 KV 写延迟 / 吞吐（canonical，安静机，`loadavg` 1.5–2.5）

| 并发 | 提交 ops | 成功 | 失败 | 成功吞吐(ops/s) | P50 | P95 | P99 | max |
|--:|--:|--:|--:|--:|--:|--:|--:|--:|
| 100 | 6359 | 6359 | 0 | 636 | 134 ms | 159 ms | 512 ms | 1412 ms |
| 500 | 7236 | 7236 | 0 | 724 | 649 ms | 946 ms | 970 ms | 976 ms |
| 1000 | 6918 | 6918 | 0 | 692 | 1331 ms | 2122 ms | 2131 ms | 2135 ms |

- 安静机下三档均无错误、无 5s 写超时；吞吐在并发 500 后饱和（~640–725 ops/s），
  延迟随并发线性放大 —— 典型「固定串行服务率 + 排队」。单写者等价服务率约
  **380 写/s**（=1/(52.6s/20000)，§4.6），客户端并发只是把请求堆在同一提交
  队列后。
- 对比共享负载首跑（`aux-shared-load/`，loadavg 5.0–8.4）：并发 ≥500 时 100%
  撞 5s `DeadlineExceeded`。这说明**超容量/高负载下会在 5s 后被拒绝而非无限
  排队**，但那组数字是负载噪声，不是容量结论（仍非目标硬件、dev profile）。

### 4.2 KV 共享负载下的方差（历史辅助，`loadavg` 5.1–7.4）

| 运行 | conc100 成功/提交 | conc100 吞吐 | conc500 成功吞吐 | conc1000 成功吞吐 |
|:--|--:|--:|--:|--:|
| run1 | 1744/1768 | 174 ops/s | 0 | 0 |
| run2 | 1758/1857 | 176 ops/s | 133 ops/s | 148 ops/s |
| run3 | 1386/1485 | 139 ops/s | 42 ops/s | 0 |

同一命令、同一机器，共享负载下成功吞吐在 0–176 ops/s 间抖动。安静机 canonical
单跑为 636–724 ops/s 且 0 错误；**安静机下的运行间方差尚未测**（缺口 §8）。

### 4.3 Txn 操作数上限（默认 `max_txn_ops`）

| 场景 | 总操作数 | compare | puts | RPC ok | succeeded | 延迟 | 错误 |
|:--|--:|--:|--:|:--|:--|--:|:--|
| puts_only | 1 | 0 | 1 | yes | yes | 4.5 ms | — |
| puts_only | 128 | 0 | 128 | yes | yes | 52 ms | — |
| puts_only | 129 | 0 | 129 | no | no | 0.51 ms | `InvalidArgument: txn ops 129 exceeds max 128` |
| cas_compare_plus_puts | 128 | 1 | 127 | yes | yes | 66 ms | — |
| cas_compare_plus_puts | 129 | 1 | 128 | no | no | 0.49 ms | `InvalidArgument: txn ops 129 exceeds max 128` |

- 上限口径 = `compare + success + failure` 总操作数（`coord-server/src/server/mod.rs:2218-2225`）。
- **契约与实现口径不一致**：`apis/contracts/proto/coord/txn/txn.proto` 自述「当前实现
  未强制操作数量上限」，但运行时默认强制 128。属事实源漂移，需在阈值定稿时对齐。
- 手动阈值「单事务操作数上限 > 100」：128 > 100，但**本报告不判定达标**。

### 4.4 单值大小上限

| 值字节 | 写 RPC | 回读 | 错误 |
|--:|:--|:--|:--|
| 1 MiB | ok | ok | — |
| 2 MiB | ok | ok | — |
| 4,194,276 | ok | **失败** | 回读 `OutOfRange: decoded message length too large: found 4194318 > limit 4194304` |
| 4,194,304 | **失败** | — | `OutOfRange: decoded message length too large: found 4194331 > limit 4194304` |
| 8 / 16 / 32 MiB | 失败 | — | 同上（found = 值 + ~27 B 信封） |

- 边界是 **gRPC 消息解码上限 4 MiB**（`coord/src/main.rs:2794-2803` 显式
  `max_decoding_message_size(coord_server::auth::MAX_GRPC_DECODING_BYTES)`，
  常量 = `4 * 1024 * 1024`），**不是 redb / MVCC 存储层限制**。
- 含 key/元数据的回读响应同样受 4 MiB 解码上限约束 ⇒ 值逼近 4 MiB 时**能写不能读**。
- 手动阈值「单值 > 1 MB」：1–2 MiB 可写可读；但真实硬边界是 ~4 MiB 消息上限。

### 4.5 Watch 扇出（1000 连接，同 key，20 轮）

| 指标 | 值 |
|:--|:--|
| 连接成功 | **1000 / 1000**（0 连接错误） |
| 投递 | 20000 / 20000（20 轮 × 1000），**0 丢失** |
| 端到端延迟 P50 / P95 / P99 / max | **6.1 ms / 13.6 ms / 16.6 ms / 17.6 ms** |
| 单轮 P50 区间 | 4.6 – 15.1 ms（第 0 轮含连接后首次投递，偏高） |
| 进程 RSS 增量（1000 watcher + server 同进程） | +60.6 MB（116.6 → 177.2 MB） |
| 进程 CPU 累计 | 1.34 s |

建连口径说明：客户端与服务端同进程，1000 条连接**瞬时并发**建连会互相挤占
runtime 的 accept/握手路径 ⇒ 实测仅 504–856/1000 成功（错误
`Unknown: transport error` / `Cancelled: operation was canceled`，见
`evidence/raw/aux-watch-burst/`）。改为 **2 ms/条错峰**后 1000/1000 成功。这是
**同进程压测的工装伪影**，不是 coord 服务端连接上限——KV 档 1000 条连接并发
建连 1000/1000 成功可佐证。错峰只影响建连速率，不影响「已建连后的投递延迟」口径。

### 4.6 存储增长（20000 写 × 256 B，单写者）

| 指标 | 值 |
|:--|:--|
| data-dir | 618,496 B → 57,008,342 B |
| 增量 | **56,389,846 B** |
| **字节 / 写** | **2819.5 B**（≈ 2.82 KB，其中值 256 B、其余 ≈ 2.56 KB 为 raft log + changelog + redb 页开销） |
| 单写者吞吐 | 380.3 写/s（20000 写 / 52.6 s，含客户端往返） |
| 外推 | **2.82 GB / 百万写** |

曲线非严格线性（redb 文件按页增长，如 5000→6000 写处跳增约 6.5 MB），故
`bytes/写` 是**区间均值**，非恒定速率。

## 5. 瓶颈分析

1. **写路径是单点串行**：单节点 Raft 每条写经「leader 追加日志 → 持久化 →
   状态机 apply」。安静机实测单写者一次 Put 约 **2.6 ms**（=1/380.3），并发升高后
   成功吞吐在 ~640–725 ops/s 饱和、延迟线性放大——典型「固定串行服务率 + 排队」
   形状。并发只是把请求堆在同一个提交队列后面。
2. **容量上限由「每写持久化成本」决定**：本机（dev profile、安静机）观察到的
   成功吞吐上限约 **636–724 写/s**（并发 500/1000）。多节点下写入仍由 leader 串行
   提交，除非引入批量提交 / 流水线（当前未见），吞吐不会随节点数线性扩展。
3. **超容量行为明确**（共享负载首跑观测）：写入超过服务端 5 s `write_timeout`
   即返回 `DeadlineExceeded`。容量规划必须预留 headroom，且上游重试需把该错误当
   可重试而非数据丢失（`kv.proto` 亦声明 UNAVAILABLE 应重试）。
4. **Watch 不是瓶颈**：1000 路扇出投递本身 P99 ≈ 17 ms，且无丢失；端到端延迟
   主要由「写提交」贡献（写完成时间与投递延迟同量级）。
5. **大对象受 gRPC 4 MiB 限制**：值 > 4 MiB 无法经 KV 写入；这是协议层封顶，
   与存储引擎无关。工作流实例/事件载荷若可能超 4 MiB，需分片或走对象存储。
6. **Txn 原子性规模**：单事务 ≤128 个操作（含 compare）。「复杂分支一次原子提交」
   的编排必须落在此预算内，否则需拆分（会牺牲原子性）。

## 6. 容量规划（带假设的推算，待目标硬件复测）

**写入存储量**
```
B_write ≈ V + 2563   （V = 值字节；线性外推自 V=256 的单点实测，未验证更大 V）
GB/day = writes_per_day × B_write / 1e9
```
| 写/天 | 估算 GB/天（256 B 值） |
|--:|--:|
| 10^4 | 0.03 |
| 10^5 | 0.28 |
| 10^6 | 2.82 |
| 10^7 | 28.2 |
| 10^8 | 282 |

假设：每写一次完整实例状态 Put；含 raft log + changelog + redb 开销；未计压缩 /
回收（本仓无 TTL/归档，`boundaries.md` B-WF-1）。**手动阈值 <1 GB/天 对应约
4.4 写/s 持续写入**——请阈值 owner 核对口径（是稳态写速率还是峰值）。

**写入吞吐 / 实例数**
- 目标硬件下的实例容量 = `目标硬件成功写吞吐 × 单实例平均写频率的倒数`。
  安静机 dev profile 实测成功写吞吐 636–724/s（非目标硬件、非 release，仅供参考）；
  **目标硬件复测前无法给出实例数上限**。
- 规划原则：按峰值写速率 × 安全系数（建议 ≥2）× 预留
  （`1/5s 超时`）倒推实例密度；不要让稳态写入逼近吞吐上限，否则延迟与超时同时恶化。

**Watch 订阅规模**
- 每订阅端 ≈ 一条长连接 + 服务端有界缓冲；实测 1000 watcher 使同进程 RSS +50 MB
  （含客户端），即 **≤ ~50 KB/watcher 上界**。目标部署里客户端在独立进程/机器，
  服务端占用更低。多订阅端 fan-out 无丢失、P99 < 17 ms（本机）。

**协议上限（与硬件无关）**
- 单值 / 单消息 ≤ 4 MiB；单事务 ≤ 128 操作。这两个是**常量预算**，容量模型里
  按硬约束处理。

## 7. 非目标硬件判定 & 未达标判定前置条件

- **非目标硬件判定**：目标硬件定义缺失（`CHARTER-INPUTS.md` §3.3），本报告**拒绝**
  给出「达标 / 不达标」结论；所有绝对数字标注为测试机 + dev profile（canonical
  为安静机；负载敏感性见 `aux-shared-load/`）。
- 待 10-18 阈值定稿后，判定需要：① 目标硬件清单；② 干净机器 `scripts/repro.sh`
  复跑（✅ canonical 2026-10-13 12:36 已完成）；③ release profile 口径复测
  （仓库性能闸 `scripts/bench-ci.sh` 用 release，本报告为 dev）；④ 安静机运行间
  方差；⑤ 对 §4 每个指标套用手册 §3.3 阈值。
- 已知与阈值相关的**事实**（非判定，数字见 §4）：单值 4 MiB 边界 > 手动 1 MB 线；
  Txn 128 > 手动 100 线；存储外推 2.82 GB/百万写；KV 写安静机 P50 134–1331 ms /
  P99 512–2131 ms、成功吞吐 636–724 ops/s（对照手册初版：P50 <10 ms、P99 <50 ms、
  并发 100 >500、500 >2000、1000 >3000 ops/s —— 待 release + 目标硬件复核）。

## 8. 缺口与后续

- 安静机 canonical 复跑已完成（`run-20261013-123644.log`）；仍缺：安静机运行间
  方差、release profile 口径、目标硬件。
- KV 绝对吞吐/延迟仅单次安静机采样，尚不足以支撑目标硬件容量数字。
- `bytes/写` 仅测 V=256 B；更大值（如 64 KB–1 MB 实例）的摊销关系未测。
- 未测多节点集群写路径、未测批量提交/流水线优化后的吞吐上限。
- 契约漂移：`txn.proto` 注释称「未强制上限」与运行时 `max_txn_ops=128` 不一致。
