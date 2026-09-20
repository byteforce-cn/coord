# coord Jepsen 计划进度（配套 `dev.md` v3）

> 更新规则：**每个任务收尾当天更新本文件的"状态"与"证据"两列**；状态只有
> `未开始 / 进行中 / 已验证 / 阻塞`。`已验证` 必须给出可核对的证据（脚本名 +
> 产物路径 / run 的 store 路径），口头"跑过了"不算。
> 本轮更新：2026-09-18（**第六轮 · 第二轮：coord-agent 做实 —— 四个面全绿（除 F-28）+ 差分基线可用**）：
> 把 §14 的三件「未闭环」做完，并在过程中用矩阵交出**十条测试自身缺陷**（F-35…F-44）
> 与**一条 coord-agent 侧观察**（F-46）。最终结论：
> * **F-34 闭环**：lock 的「互斥重叠 99/162」是**测试自身的三层区间度量缺陷**
>   （锚点噪声 / `exists=false` 闭合判据 / 闭合时刻记在观测循环之后）；修后
>   `:mutual-exclusion {:hard 0 :near-boundary 0}`，服务端地面真值探针
>   （`:f :lock-probe`，绕开 agent 直读 `/_lock/{name}`）**0 矛盾 / 0 边界**。
> * **四个本地面全部可判决**：`election:none|kill-agent`、`idgen:none|kill-agent`、
>   `registry:none|kill-agent` 全绿；`lock` 的三个 cell **只剩 F-28**（fencing
>   110/110，coord 侧真缺陷）。
> * **差分基线第一次可用**：`map` 的 direct / `--via-agent` 双绿（路由证明
>   `:total 161`），并已用它抓到一条真形态（F-45：能力清单漏 `data:kv:delete`
>   ⇒ 经 agent 的 Delete 全被拒，而 direct 因为 root 旁路一直绿）。
> * **soakfull 起得来**：`--soak-mix` 默认含四个 agent 面，soak 日志里能直接看到
>   `:lock-contend` / `:elect-campaign` 的 invoke/completion。
> * 新增工具：`scripts/lock-diag.clj`（同一历史四种区间口径）、
>   `scripts/agent-port-open.sh`（gRPC 就绪探针）；fixture 从 21 套 120 个涨到
>   **26 套 133 个**（`make checkers` 退出码 0，0 个 FAIL）；矩阵新增
>   `KEEP_GOING=1`（分面归因档，退出码仍 1）。
> * 详见 `coord-findings.md` §15（F-35…F-46）与 `dev.md` §5.5 第 13–22 条。
>
> 本轮更新：2026-09-18（**第六轮：M5a 落地 —— coord-agent 首次被 Jepsen 真跑**）：
> 把 `coord-agent-coverage-plan.md` 的建议落成代码：**agent 部署**（多实例、SSH
> 隧道、路由证明、Ed25519 验签、能力引导）、**wire 层**（13 个 `coord.agent.*`
> 方法 + 与 `agent_api.proto` 的一致性自检）、**4 个 agent 本地面 workload 与
> checker**（lock / election / idgen / registry，共 20 个 fixture）、**agent 侧
> nemesis**（kill / kill-all / pause / partition-agent-server / agent-all）、
> **门禁**（`make checkers` 扩到 21 套 120 个 fixture；新增 `matrix-m5` /
> `matrix-m5-diff`）。首次真跑即产出 **2 条 P0**（F-28 锁 fencing 缺失 162/162、
> F-32 agent 无 root 能力旁路 ⇒ 经 agent 的 root 调用全被拒）、1 条未闭环 P0 候选
> （F-34 互斥重叠 99/162）、3 条测试自身缺陷（F-29/F-30/F-31，均已修 + 补守门
> 员 fixture）与 1 条契约字段缺陷（F-33 `new_ttl` 恒 0）。详见 `coord-findings.md` §14。
>
> 本轮更新：2026-09-17（第五轮）：**T2.2 lease 落地**（`--workload lease` 三场景 +
> `jepsen.coord.leaseck` 6 条判据 + 12 个 fixture）、**T6.1 组合浸泡入口落地**
> （`--workload soakfull` + `--soak-mix`，未实现的面构造期硬失败）、**soak 常态化**
> （`make nightly` / `make soakfull` / `make soak-wait` + `nightly-soak-gates.sh`）、
> **soak 结项报告**（`soak-closure-report.md`）。fixture 从 14 套 86 个涨到
> **17 套 99 个**；`matrix-m2` 从 4 组合扩到 8 组合（加入 lease 四档）。
> 本轮离线编译抓到 3 条我方缺陷（F-23/F-24/F-25，全在「改完先加载一次」这条线上）。
> **复跑更新（2026-09-17，docker lab）**：`make checkers`（17 套/99 个）与
> `make matrix-m1`（9/9）复跑**全绿**；`make matrix-m2` 复跑 **7/8** ——
> `lease × partition-halves` 红，已登记为新缺陷 **F-27（coord 侧 P1，未闭环）**，
> 详见 `coord-findings.md` §13。
> 本轮更新：2026-09-17（第四轮）：**M1 收口**——**T1.5 `mixture` workload 落地**
> （map+txn+scan 混合 + 组合 checker `jepsen.coord.mixck` + 11 个新 fixture），
> **CAS 命中率质量改进**（read-then-CAS），**F3 值大小扫描**（4KB/64KB），
> checker fixture 扩到 **14 套 86 个**；`matrix-m1` 扩到 9 个组合、新增 `matrix-m2`。
> 第四轮的中间过程也暴露一条**我方流程缺陷**：在两个 combo 之间改了 bind-mount
> 的源码，导致 `txn/kill` 以「Syntax error compiling」失败并看着像被测系统的问题
> ——已记入 `memory` 与本文 §3。

## 0. 总览

| 里程碑 | 计划人日 | 状态 | 完成度 |
|:--|:--|:--|:--|
| M0 基座（T0.1–T0.6） | 4 | **已验证** | 6/6（T0.4/T0.6 的 lab 实跑已完成，见 §1；实跑暴露的 F-09…F-12 已修并补 fixture） |
| M1 数据面（T1.1–T1.5） | 6.5 | **已验证** | 5/5（T1.1/T1.2/T1.3/T1.4/T1.5）；checker fixture 从 4 套 29 个涨到 **10 套 75 个** |
| M2 Watch+Lease（T2.0–T2.3） | 5 | 进行中 | **T2.0 + T2.1 已 lab 验证绿；T2.2 复跑只剩一档红**（BIDI 流式接线 + `--workload watch` + `--workload lease`（T2.2：TTL 到期 / KeepAlive 续期 / Revoke 级联，6 条判据 + 12 个 fixture）；`make checkers` 扩到 **17 套 99 个**；`matrix-m2` = watch/lease × none\|kill\|pause\|partition-halves 共 8 组合，**2026-09-17 复跑 7/8：`lease:partition-halves` 红 → F-27（coord 侧 P1，未闭环）**）。T2.3 收口（2h 混合浸泡）待做 |
| M3 运维面（T3.1–T3.6） | 5 | 未开始 | 0（T3.5 行为已由 F-07 判定、T3.4 设计已由 F-08 修正） |
| M4 Multi-Raft（T4.1–T4.3） | 2.5 | 未开始 | 0（`multi-register` 静态多 region 已可跑） |
| M5 Agent 层（T5.1–T5.7） | 6.5 | 未开始 | 0（T5.2 硬前置已兜现；**T5.3/T5.4/T5.5 是 T6.1 组合浸泡里 lock/election/registry 三个面的前提**） |
| M6 收口（T6.0–T6.3） | 2 | 进行中 | **T6.1 的入口已落地**（`--workload soakfull` + `--soak-mix` + 组合 checker + 路由门槛 + 夜间门禁脚本）；长跑（12h/24h/72h）未执行 |

**本轮机器时间**：docker lab 上约 25 次 run（45–150s 短跑）+ 1 次 2h mixture
浸泡；vagrant 的 8h/24h/72h 额度仍未被占用。

**门禁结果（本轮收尾）**：

| 门禁 | 命令 | 结果 |
|:--|:--|:--|
| checker fixture | `make checkers` | **17 套 99 个全绿**（本轮新增 `lease-fixtures` 9 + `lease-fixtures-sample` 2 + `soakfull-fixtures` 2；离线与 docker lab 两边都跑过；**2026-09-17 lab 复跑 99 PASS / 0 FAIL**） |
| M2 矩阵 | `make matrix-m2` | **2026-09-17 复跑 7/8**：watch 四档 + `lease:none/kill/pause` 绿；**`lease:partition-halves` 红 = F-27**（矩阵共 8 组合；lease 档带 45s 口径的 §5.1 样本门槛 5/2）。台账：`store/coord` 全量只有 6 个 lease run、全部在 2026-09-17 ⇒ 此前「8 组合全绿」无 run 支撑 |
| T6.1 组合浸泡 | `make soakfull SOAK_TIME_LIMIT=...` | 入口已就绪（`--soak-mix` 默认 = T6.1 比例里已实现的部分）；长跑待做 |
| 夜间门禁 | `make nightly` / `SKIP_LONG=1 jepsen/scripts/nightly-soak-gates.sh` | 本轮新增（checkers → matrix-m1 → matrix-m2 → soakfull + wait + results） |
| 本轮离线回归 | 17 套 fixture（离线 harness） | **全绿**（99/99） |
| M1 数据面矩阵 | `make matrix-m1`（map/txn/scan/mixture × none\|kill 45s + idempotency:none） | **9 组合全绿**，**2026-09-17 复跑 9/9（`ALL M1 MATRIX PASSED`）**（见 §1.4；首轮的 `txn/kill` 失败系我方在 run 中间改源码所致，与 coord 无关，已重跑） |
| T1.5 2h 浸泡 | `make soak WORKLOAD=mixture`（rate 2、seed 固定、`--checker soak`） | 见 §1.4 |
| F3 值大小扫描 | `map` + `--value-size 4096` / `65536`，各 60s kill | **两档都绿**（4KB：156 ops / 20 delete / 值长 4096；64KB：169 ops / 28 delete / 值长 65536） |
| T2.0/T2.1 watch | `make checkers`（+4 套）+ watch/none 45s + watch/kill 120s | 见 §1.5：**全绿**（kill 档 1070 事件 / 78 次流重开） |

> 两份矩阵的逐组合 `results.edn` 都在 `jepsen/store/coord/<时间戳>/`；需要
> 入库时可对这 19 个目录逐个跑 `scripts/collect-evidence.sh`（本轮只归档了
> 有结论价值的那几个，见 `docs/production/evidence/README.md`）。

## 1.0 本轮（第三轮）落地清单

| 项 | 产物 | 状态 |
|:--|:--|:--|
| T1.1 map/delete | `--workload map`（`--map-keys` / `--value-size` / `--map-min-deletes`）、`jepsen.coord.mapck`（`:linear` knossos + `:index` O(n)）、`scripts/map-fixtures/`（12）+ `scripts/map-fixtures-min-deletes/`（2） | **已验证**：60s `--nemesis kill` 绿（170 ops / 27 delete = 15.9%） |
| T1.2 txn 全形态 | `--workload txn`（5 形态）、`jepsen.coord.txnck`、`scripts/txn-fixtures/`（8） | **已验证**：60s kill 绿（176 txn：成功 141 / 失败 31；`response-counts {1 125, 3 31, 0 16}` 与分支语义一致） |
| T1.3 scan/revision | `--workload scan`、`jepsen.coord.scanck`、`scripts/scan-fixtures/`（10） | **已验证**：60s kill 绿（93 scan / 44 read-at / 404 值观察） |
| G6 空转门槛 | `jepsen.coord.gates` 的 `:liveness`（`--min-op-ok-ratio` / `--min-op-sample`）+ 2 个新 fixture | **已验证**（F-13 的两次形态都是它抓到的） |
| T1.4 收尾分支 | `--idem-replay-node-offset N`（确定性「换节点重放」）+ 契约限缩 | **已验证**：offset 单独用**无效**（写只能由 leader 服务，见 F-03 负面结果）；有效配方 `--nemesis kill --idem-replay-delay-ms 4000` 跑出 13 组 `:revision-advanced`（证据 `20260916T150552Z-t1.4-cross-node-f03`） |
| coord 修复回归 | T1.4 四条幂等承诺 | **`closed`**：154 组 / 352 次重放 **0 违反**（证据 `20260916T150557Z-t1.4-regression-after-f01-f02-fix`） |
| 全回归矩阵 | `make matrix`（register × 6 + cas-register × 6，45s/组合） | **ALL MATRIX PASSED**（12/12）。其中 **cas-register × 6 是首次真正跑 Txn**（此前 `txn-req` 构造报错，全 `:info` 假绿，见 F-13）；新增 `make matrix-m1` 门禁新 workload |

## 1. M0 基座（第二轮收口，证据保留）

| 任务 | 状态 | 证据 |
|:--|:--|:--|
| T0.1 证据入库管道 | 已验证 | `jepsen/scripts/collect-evidence.sh` + `summarize-results.clj`；实跑归档 `docs/production/evidence/20260916T122844Z-baseline-partition-ring-60s/`（MANIFEST.md / summary.txt / sha256sums.txt 齐全） |
| T0.2 checker 门槛（quiet 可用率 / RTO / 值唯一性前提） | 已验证 | `jepsen/src/jepsen/coord/gates.clj`；4 个 fixture 全绿（`scripts/gates-fixtures/`）；**真实 store 回归**：`store/coord/latest` → `:valid? true`，RTO 样本 `[0.38 0.87 1.80 1.82 1.03]s` |
| T0.3 统一 fixture 运行器 | 已验证 | `jepsen/scripts/run-checker-tests.clj`；`soak` 14 + `gates` 7（含 G6）+ `gates-defaults` 1 + `idem` 10 + `map` 12 + `map-min-deletes` 2 + `txn` 8 + `scan` 10 = **8 套 64 个 fixture 全绿**；已接入 `make checkers`，并作为 `quick` / `test` 的前置（`SKIP_CHECKERS=1` 可跳过，仅供调 fixture 用） |
| T0.4 环境清理与 preflight | **已验证** | `scripts/env-reset.sh` + `env-reset-all.sh`；lab 实跑 `make env-reset STRICT_CLOCK=1`（docker provider）→ **control + n1..n5 全部 clean**。三类判定在真实节点上生效：`iptables clean` / `tc/netem clean` / `clock offset`（control 节点自身 `self`；n1..n5 用 `control-bracket`）。证据：`docs/production/evidence/20260916T132137Z-m0-lab-verification/` |
| T0.5 随机种子与回放 | **已验证** | `--seed` 落到 test map；`make quick SEED=42` 的 `results.edn` 里 `:seed 42` 可读，MANIFEST 也记录了；`scripts/replay.clj` 由种子重建 jittered 排期 |
| T0.6 nemesis 抖动 | **已验证**（并修正判据） | 修掉 F-11 后实测节拍 `n=15 distinct=13 min=1.16 max=7.52`。证据：`docs/production/evidence/20260916T132141Z-m0-jitter-verification/`（对照：`...20260916T132139Z-m0-jitter-before-f11-fix/`，那份是**恒定节拍**，正好留作 F-11 的复现凭证） |

### 1.1 M0 的验证口径（本地无 lab 时怎么算"已验证"）

第一轮（2026-09-16 上午）在没有 lab 的环境里只做了**离线可复现**的验证：
用 Clojure CLI + `jepsen 0.3.13` + gRPC/protobuf 依赖编译 `CoordRpc.java`，
把整个 `jepsen.coord.*` 装载编译，并用真实 store 产物回归 checker。

**第二轮已补上 lab 实跑**（docker provider，3 节点真实 coords + 真实 nemesis）：
`make up` → `make upload` → `make checkers` → `make env-reset STRICT_CLOCK=1`
→ `make quick SEED=42` → `make test NEMESIS=kill SEED=42`。
这一步的价值立刻体现出来：**四条只在真跑时才暴露的测试自身缺陷（F-09…F-12）
全部是"离线看起来是绿的"那一类** —— 门槛被 nil 吃掉、节拍恒定、摘要器挂掉。
离线验证只能证明"代码能装载、fixture 能过"，不能替代实跑。

### 1.2 本轮发现的两类问题

**A. 计划级修正（改设计/改判据）**

1. **F-11（改判据）**：T0.6 的验收判据"相邻 nemesis op 间隔 ∈ [3,8]s"
   **不充分** —— 恒定 6.42s 节拍完全满足它。判据已改为看**取值离散度**
   （周期 ≥6 时不同取值必须 ≥3），并写进 `scripts/nemesis-timeline.clj` 的自检。
2. **F-09（改实现）**：门槛参数不能用 destructuring `:or` 解析（键存在但为 nil
   时不接管），必须用 `or`；并新增只能用默认门槛判出 invalid 的 fixture。
3. **F-10（改环境假设）**：lab 一律按**离线**跑（`lein -o`），
   时钟校验改用控制机夹逼采样（不再要求节点装 chrony/ntpq）。
4. **F-06（改计划，上一轮）**：`--watch-semantics` 需要第三态 `overflow-marker`。

**B. coord 侧缺陷（倒逼产出，见 `coord-findings.md`）**

F-01（Delete 无幂等，含范围删重放删掉新写入）、F-02（Put 命中丢 `prev_kv`）
已由 T1.4 的 run **零故障注入 100% 复现**；F-03（去重不跨节点/不持久）
的边界已由同一 run 的对照组确认为"同 leader 内确实有效"，
跨节点分支留待 T1.4 收尾跑（`--nemesis kill-all --idem-replay-delay-ms 1500`）。

### 1.3 T1.4 幂等专项（本轮新增任务，已收口）

| 项 | 内容 |
|:--|:--|
| workload | `--workload idempotency`：`:idem-put` / `:idem-delete` / `:idem-range-delete` 三种分组轮换，每组一个新 key/rid，重放 2–3 次，收尾点读 |
| checker | `jepsen.coord.idem`（O(n)）：四条断言 —— 不重复生效（`revision`/`deleted` 一致 + `version` 不超预期）、返回首次结果（`prev_kv`/`prev_kvs` 逐字段一致）、不放大破坏面（重放后区间内新写入的 key 必须存活）、删除不复活 |
| 负控制 | `scripts/idem-fixtures/` **10 个 fixture**（7 个 `expect-invalid-*` 各自对应一条断言，3 个 `expect-valid-*` 覆盖基线/`:info` 首尝试/小样本），已接入 `make checkers`（现在是 4 套、29 个 fixture 全绿） |
| 样本门槛 | `--idem-min-replay-attempts`（默认 50，§5.1）；不足则 `:valid? false` + `:reason :insufficient-sample`，不判绿 |
| 证据 | `docs/production/evidence/20260916T132103Z-t1.4-idempotency/`（seed 1727727433，`overall-valid false / gates-valid true`）与 `...-seed42/`（seed 42） |
| 判决 | 136 / 138 个分组，`:prev-kv-mismatch` 26/26（= 带 setup 的 put 分组全部）、`:delete-count-mismatch` 47/47、`:delete-prev-kvs-mismatch` 47/47、`:replay-deleted-new-write` 39/39 → **红是结论，不是抖动** |
| 未做 | 无。跨节点重放分支已于第三轮完成（`--nemesis kill --idem-replay-delay-ms 4000` → 13 组 `:revision-advanced`，见 `coord-findings.md` F-03 判决与 `20260916T150552Z-t1.4-cross-node-f03`） |

### 1.4 T1.5 M1 收口（第四轮；`mixture` workload + 2h 浸泡）

| 项 | 内容 |
|:--|:--|
| workload | `--workload mixture`：map / txn / scan 三个面混在同一条历史里跑；每份 op 带 `:sub` 标记（`:map`/`:txn`/`:scan`），`--mixture-ratio W,W,W`（默认 `4,2,2` = 目标 op 份额 50/25/25） |
| checker | `jepsen.coord.mixck`：**组合**而非新造判据 —— 按 `:sub` 切片后分别跑 `mapck`（`:linear` 短矩阵 / `:index` 长跑）/ `txnck` / `scanck`，外加 `routing-checker` |
| 组合层活性门槛 | `routing-checker` 断言 ①每个面的完成数 ≥ `--min-op-sample`（默认 10）②不存在**没被路由**的客户端 op。理由是 F-13 的组合形态：生成器漏打 `:sub` 时三个子 checker 谁都看不到那条 op，组合结果却是绿的 |
| 实测份额 | 每个 run 的 `:routing {:share ...}` 给出实测 op 份额（目标 vs 实际的偏差可见）：120s / 1198 op 的一轮实测 **0.517 / 0.255 / 0.228**（目标 0.50/0.25/0.25，±0.015） |
| 口径修正（写进源码注释） | `gen/mix` = 「均匀抽一个子生成器、从它取 **1 个** op」⇒ 面份额 = **实例数占比**，与子生成器内部槽位数无关。中间实现曾按槽位 8/5/5 折算（预测 .615/.192/.192），被上面那轮实测否掉并回退 |
| 负控制 | `scripts/mixture-fixtures/` 8 个（任一面的违反都必须**穿透**组合）+ `scripts/mixture-fixtures-routing/` 3 个（`min-sample` 门槛与「未路由 op」分开钉住）。`make checkers` 因此从 8 套 64 个涨到 **10 套 75 个** |
| workload 质量改进 | `cas-register` 的 `old` 从「200 元集合均匀抽样」改为 `client/last-seen`（最近一次观察值）= 教科书 read-then-CAS；原来 45s 只有 13 个 `:ok` / 108 个 cas（其余全 `cas-miss`，G6 的 `:ok` 率 0.12 贴着 0.1 下界） |
| F3 值大小扫描 | `--value-size 4096` / `65536` 各 60s `--nemesis kill`（map） | **两档都绿**：4KB（156 ops / 20 delete / 值长度实测 4096）、64KB（169 ops / 28 delete / 值长度实测 65536）；值唯一性前提 0 重复 |
| 收尾读 | soak 模式下 `mixture` 也做 §5.2 的最终收敛读（读 map 面的 8 个 key，带 `:sub :map`，由 map 面写索引判 fabricated/future/stale） | 已实现（实测在 2h 浸泡里跑） |
| 实跑 | `make matrix-m1` 9 组合全绿（含 mixture:none / mixture:kill 45s）；T1.5 2h mixture 浸泡（rate 2、seed 42、`--checker soak`、`--map-min-deletes 200`） | 矩阵全绿；浸泡结果见下 |
| 证据 | `docs/production/evidence/` 下本轮的 `…-t1.5-mixture-120s-share/`、`…-t1.5-mixture-45s-kill/`、`…-f3-value-size-4k/`、`…-f3-value-size-64k/`、`…-t1.5-mixture-soak-2h/` |

### 1.5 M2（T2.0 流式接线 + T2.1 watch workload；本轮落地）

| 项 | 内容 |
|:--|:--|
| T2.0 流式接线 | `CoordRpc`：watch.proto 的 FileDescriptor（`WatchCreateRequest` / `WatchEvent`（含嵌套 `EventType`）/ `WatchRequest`（oneof `create`）/ `WatchResponse`）+ `Watch` 的 **BIDI_STREAMING** MethodDescriptor + `Watcher`（后台读线程 → 有界队列 → `.poll`/`.tryPoll`/`.close`；`close()` = 取消 = 契约的「关闭流」；读线程每收一条主动 `call.request(1)`）；`proto.clj`：`watch-create-req` / `watch-req` / `watch-id` / `event-type`（enum 标量无 presence，走 `EnumValueDescriptor`）/ `event->edn` / `response-events` / `open-watch` |
| T2.1 workload | `--workload watch`：一个 watch key + 写产生事件；**一次会话 = 一个 op**（开流 → 窗口内收事件 → 关流；流中断时在同一个 op 内按契约重开到 `last-revision + 1`，`--watch-resumes` 默认 2）。两种会话：`start-revision 0`（从最新）与 `:last`（从本客户端上次观察到的最大 revision） |
| T2.1 checker | `jepsen.coord.watchck`：①流内 revision 严格递增（+类型/kvs 结构）②`start_revision = R > 0` 的会话不得收到 revision < R 的事件 ③事件值必须真的写过（fabricated）④**静默丢事件**（P0：确认写的 revision 落在会话有效区间 `(lower, upper]` 内却没出现，且缺口之后没有 `BUFFER_OVERFLOW`/`HISTORY_UNAVAILABLE` 标记）⑤事件数 < `--watch-min-events`（默认 200，§5.1）⇒ invalid |
| 可判定性设计 | `start_revision = 0` 时服务器不告诉客户端当时的 revision ⇒ 取**首个事件的 revision** 当保守下界（只可能漏报、不会误报）；`>0` 时下界就是它，判据②因此可严格判 |
| 语义档 | `--watch-semantics overflow-marker`（默认，F-06 的源码口径）/ `lossless` / `coalescing`；lossless 与 coalescing 用**同一条有缺口的历史**做期望值相反的 fixture ⇒ 参数真的接上了 |
| 负控制 | `scripts/watch-fixtures/`（7，含 F-21 的守门员 `expect-valid-empty-session`）+ `watch-fixtures-sample/`（2）+ `watch-fixtures-lossless/`（1）+ `watch-fixtures-coalescing/`（1）= **11 个**；`make checkers` 从 10 套 75 个涨到 **14 套 86 个** |
| 实跑 1（T2.0 冒烟） | `--workload watch --nemesis none --time-limit 45 --rate 5`：**绿**（`events 380` / `sessions 91` / `resumes 0`；G6 逐类 `:ok` 率 1.0；值唯一性前提 94 写 0 重复） |
| 实跑 2（T2.1 续传） | `--workload watch --nemesis kill --time-limit 120 --rate 5`：**绿**（`events 1070` / `resumes 78` —— 「流中断 → 按契约 resume」的真实证据） |
| 本轮新增的三条**测试自身**缺陷 | **F-19**：`open-watch` 的调用方传普通 map 进 oneof 字段 ⇒ 69 个会话全 `:info`、0 事件；**被 G6 op 级活性门槛 + watch 样本门槛 + 顶层 combine 三张网同时抓到，没有产出假绿**。**F-20**：会话内重开时把「最后一次尝试的起点」当成 op 的 `:start-revision` 汇报 ⇒ 6 条假红。**F-21**：`:end-revision` 用了同一种兜底写法（空会话被当成覆盖了 `(start, start+2]`）⇒ 7 条 `:watch-event-loss` 假红。三条都已修 + 进 §5.5 判据补充 9/10 与 fixture |
| 与 M1 的关系 | T2.0/T2.1 不改 T1.x 的任何判据；`--workload watch` 独立成面（watch 的判定不需要线性化搜索），因此不受 `windex` 的「值唯一」前提约束（watch 的写值仍带 run tag，可归因） |

### 1.6 T2.2 lease + T6.1 组合浸泡（第五轮；lab 已实跑）

| 项 | 内容 |
|:--|:--|
| T2.2 workload | `--workload lease`：`:lease-ttl`（到期）/ `:lease-keepalive`（续期）/ `:lease-revoke`（撤销级联）三场景按 2:1:1 混跑；key 带 run tag 且每 op 唯一 |
| T2.2 判定基准 | 全是**客户端单调量**（`:at-ms` = 相对 op 起点的毫秒），**不跨机比较时钟** ⇒ 与 F-08（服务端单调时钟）口径一致，不受节点时钟差/墙钟跳变影响；TTL 一律用**响应里实际授予的**值 |
| T2.2 checker | `jepsen.coord.leaseck` 6 条判据：①存活期内提前消失（P0）②`Put{lease_id}` 后不可见 ③到期后未在 ttl+grace 内消失 ④Revoke 未在 grace 内级联删除（P0）⑤Revoke 后续期回了 ttl>0 ⑥续期场景从未回 ttl>0 + §5.1 样本门槛；锚点缺失时**不判**并把数量记入 `:liveness-unjudged`（F-26） |
| T2.2 接线 | `CoordRpc`：`lease.proto` FileDescriptor + `LeaseGrant`/`LeaseRevoke`（unary）+ `LeaseKeepAlive`（**BIDI_STREAMING**）+ `KeepAliver`（`.send`/`.poll`/`.close`）；`proto.clj`：`put-req*`（`lease_id` 绑定字段）/`lease-grant-req`/`lease-revoke-req`/`open-keepalive`/`keepalive->edn` |
| T2.2 负控制 | `scripts/lease-fixtures/` **9 个**（6 负控制 + 3 守门员：读失败不算消失、缺锚点不崩、`cas` 型基线）+ `lease-fixtures-sample/` **2 个**（门槛接线） |
| **T2.2 实跑** | `--workload lease --nemesis none --time-limit 60 --concurrency 2n --lease-min-grants 20 --lease-min-expiries 8`：**绿（退出码 0，`:valid? true`）**——`grants 207 / expiries 95 / keepalive 58 / revoke 54`、`keepalive-responses 348`、**六类违反全 0**、`liveness-unjudged 1`（守门员可见）；示例门槛 20/8 也满足 §5.1 的 100/30 验收档。证据 `docs/production/evidence/20260917T144810Z-t2.2-lease-60s/`（seed 42，`overall-valid true / gates-valid true`） |
| T2.2 首跑即抓到一条我方缺陷 | **F-26**：第一版 `leaseck` 在活性锚点缺失时 `(long nil)` 抛 NPE ⇒ jepsen 把**整个 checker 判成 `:valid? :unknown`（退出码 2）**——「既不是红也不是绿」的隐蔽形态。已修 + 守门员 fixture |
| T6.1 workload | `--workload soakfull` + `--soak-mix`（面集合与权重；默认 = T6.1 比例里**已实现**的部分 `map=40,txn=20,scan=5,watch=15,lease=10`）；权重按 gcd 约简成 `gen/mix` 的实例数 |
| T6.1 checker | 复用 `mixck`（泛化出 `:surfaces` 入口）：**每个面仍由它自己的 checker 判**（不新造判据）+ 路由门槛（每面最小样本 + 禁止未路由 op） |
| **T6.1 拒绝静默丢弃** | `--soak-mix` 里出现未实现的面（lock/election/registry 需 M5）⇒ **构造期抛异常**（`soakfull-mix`）；已跑出的 op 若不在声明面内 ⇒ 路由门槛判红 |
| T6.1 负控制 | `scripts/soakfull-fixtures/` **2 个**：五面组合必须 valid；未声明面（`:sub :lock`）必须 invalid |
| **T6.1 实跑** | `--workload soakfull --nemesis kill --time-limit 90 --concurrency 2n`（每面门槛 10 / watch 50 / lease 10,3）：**绿（退出码 0，`:valid? true`）**——`:routing {:valid? true, :by-sub {:map 134 :txn 57 :watch 54 :lease 30 :scan 12}, :share {0.467 0.199 0.188 0.105 0.042}, :unrouted 0, :insufficient []}`。证据 `docs/production/evidence/20260917T144816Z-t6.1-soakfull-90s-kill/` |
| soak 常态化 | `make nightly` / `jepsen/scripts/nightly-soak-gates.sh`（`checkers` → `matrix-m1` → `matrix-m2` → `soakfull` + `soak-wait` + `soak-results`；`SKIP_LONG=1` 只跑前三步）；`coord-soak.sh` 新增 `--extra` 通用透传与 `wait` 子命令；`make soakfull SOAK_MIX=...` |
| soak 结项 | [`soak-closure-report.md`](soak-closure-report.md)：量化「逼出了什么」（coord 侧 8 条 / 测试自身 15 条）+ 诚实列出未兑现部分 |

### 1.7 第七轮（2026-09-19）：AG-06 + election 服务端探针 —— 并挖出**凭据面**的 P0

| 项 | 内容 |
|:--|:--|
| 新增工具 | `src/jepsen/coord/faultwin.clj`：把 nemesis 历史还原成「**每个 agent 什么时候不可能再续期**」的时间窗（递归展开 `:value`，兼容 `:kill-agent-all` 与 compose 的嵌套 op map）。弃锁/遗弃类判据的期望值在故障前后**相反**，没有这个窗口就只能猜（猜错方向的代价是假红或假绿各一半） |
| AG-06 workload | `--workload lock` 新增**弃锁** op（`:f :lock-abandon`，1/6 槽位，**独立锁名池** `<name>-abandon`，避免污染争抢样本）：拿到锁之后**故意不释放**，模拟「插件进程里的句柄没被 drop」。同步把地面真值探针提到 1/3 槽位 |
| AG-06 checker | `lockck` 判据 6 `:lock-orphan-not-reclaimed`（持有 agent 被 kill/pause/分区之后，服务端必须在 `ttl+grace` 内回收；锚点 = 故障事件**完成**时刻，保守）+ 判据 7 `:lock-phantom-loss`（持有 agent **活着**时 key 不得消失）+ 门槛 `:lock-abandon-unjudged`（判过 ≠ 违反，见 §5.5-23） |
| election 探针 | `:f :election-probe`（绕开 agent 读 `/_election/{group}`）+ `electck` 判据 4 `:election-server-truth-contradiction`（边界 100ms 只记录）+ 门槛 `:election-probe-missing` —— 补上 F-35 明示的「残留」 |
| 组合面接线 | `mixck` 的 lock/election 分支同步开 `:probe?/:abandon?/:agent-nodes`：否则 soak 里的 lock 面会**静默少判**（生成器产出弃锁 op，而没有判据看它） |
| **首跑即抓到 F-50（coord-agent，P0 候选）** | 零故障的 `lock:none` cell 里，服务端探针看到「持有者还活着，key 却不见了」。根因不是测量：agent 节点日志四路同因 —— `failed to register node_id / failed to load initial catalog / failed to subscribe Watch / auto-renew of lock … : unauthenticated: missing CCT token`。**agent 自发**的流量（锁续期、registry 目录加载与订阅、idgen 节点注册）**没有凭据通道**（共享 `inner.client` 的 token 只装「调用方转发进来的 CCT」）⇒ 服务端一开鉴权，这四件事全死 |
| F-50 的实测数字 | `LOCK_TTL_SECONDS=30 / --nemesis none / 120s`：`:abandon {:ops 13 :judged 12 :phantom-loss 9}`（9/13 个「还活着的持有者」丢了自己的锁）；`lock:partition` 150s：`:orphans 0`（**回收这一半是好的**）+ `:phantom-loss 42`。⇒ AG-06 的两半一正一反，把 F-50 钉在「假丢锁」这一侧 |
| 本轮新增的测试自身缺陷 | **F-47**（Makefile EDN 的双引号被两层 shell 吃掉 → 字符串静默变 symbol）、**F-48**（多一个 `)` 让函数返回 `vec` 本身；`lein check` 全绿）、**F-49**（把「判过」实现成「在违反列表里」→ 正例 fixture 第一次跑就红）、**F-53**（run 边界用 jepsen `:time`、样本用 `:t0-ns` ⇒ 两个时间轴混用，F-34 同型复发，被 `:lock-abandon-unjudged` 当场抓到）、**F-54**（把「TTL ≤ 节拍」做成豁免的第一版连带赦免了「归因失败」）、**F-55**（partition 清残留不验证 ⇒ 正常结束的 cell 也留下 6 条 DROP，后续 3 个 cell 全以「agent 就绪超时」失败） |
| 判据纪律新增 | `dev.md` §5.5 第 23–27 条（判过≠违反 / 故障窗口必须还原 / Makefile EDN 引号 / `lein check` 不保证真跑 / 括弧写错会「返回一个函数」） |
| fixture | 新增 `lock-agent-fixtures/`（6）+ `elect-probe-fixtures/`（4），`make checkers` 从 26 套 133 个涨到 **28 套 143 个** |
| 结论 | **`lock` 的红现在有两条**：F-28（fencing，旧）+ **F-50**（假丢锁，新）。其余三个本地面在修复后仍绿。差分矩阵的归因提示已按「本格 direct 结果」分叉（F-43 的遗留提醒） |


## 2. 下一步（按依赖顺序，可直接认领）

1. ~~**T1.4 回归跑（验证 F-01/F-02 修复，0.5d，最高优先）**~~ —— **已完成**：
   修复后 154 组 / 352 次重放 0 违反（F-01/F-02 `closed`）；F-03 用
   `--nemesis kill --idem-replay-delay-ms 4000` 跑出 13 组 `:revision-advanced`
   （`confirmed-by-run`），并把契约措辞限缩到实际作用域。
2. **T1.5 M1 收口（0.5d + 2h）** —— `mixture` workload（map+txn+scan 混合比例）
   + 2h soak；产物入库。顺带一项 workload 质量改进：`cas-register` 的命中率
   偏低（实测 45s：108 个 cas 里只有 13 个 `:ok`、95 个 `cas-miss`，因为
   `old` 取自全局观测池而不是「刚读到的值」）—— 改成「读-再-CAS 刚读到的值」
   可以把线性一致性的有效样本提高一个量级。
3. **值大小档（F3）** —— `--value-size 4096 / 65536` 各跑一轮 map，校验
   大 value 与 MVCC/压缩/对象存储的交互（当前默认 16B）。
4. **M2 起跑**（T2.0 流式接线 → T2.1 watch），前置已就绪（F-06）。

## 3. 风险与阻塞

| 风险 | 影响 | 缓解 |
|:--|:--|:--|
| **「这个 workload 根本没真的跑」类假绿**（F-13） | 覆盖面为零但报告全绿，是最危险的一类 | 已加 **G6 op 级活性门槛**（单类 op `:ok` 率下界）+ 两个守门员 fixture；第四轮又把同一形态抬到**组合层**：`mixck` 的 `routing-checker` 断言「每个面都达到最小样本 + 没有未路由的客户端 op」，负控制见 `scripts/mixture-fixtures*/` |
| **lab run 进行中修改仓库源码**（第四轮新发现，我方流程缺陷） | docker lab 把仓库 bind-mount 进控制机、每个 combo 现场编译；跑到一半改 `.clj`（哪怕只是引入一个未定义符号）会让**后续组合以「Syntax error compiling」失败**，而输出看上去像被测系统出问题 —— 实测把 `make matrix-m1` 的 `txn/kill` 打红，浪费一轮矩阵 | 已写入 `memory`（jepsen-lab-ops）：要改代码先确认 lab 空闲（`make soak-status`）；长跑一律把输出写文件而不是管进 grep（否则编译错误会被过滤器吞掉，只剩空结果） |
| **「环境假设」类修正没铺全**（F-22） | `coord-soak.sh` 的 `lein` 漏了 `-o`（F-10 早就定了离线约定），导致 soak 起跑后日志长时间空白、看着像卡死 | 已修（`LEIN_OFFLINE` 变量 + `ONLINE` 覆盖）；教训：修环境假设类缺陷要 `grep -rn "lein " scripts/ lab/` 把同类调用点一次找全 |
| **本轮 3 条「新面首跑」缺陷（F-19/F-20/F-21）全是我方** | 都是「派生字段/构造期」一类，其中 F-19 与 F-13 同形（整类 op 变 `:info`），F-20/F-21 造出**像被测系统违约的假红** | F-19 被 G6 + 样本门槛抓到（**没有假绿**）；F-20/F-21 被 checker 自身的判据抓到；三条都已修 + 各补 fixture + 写进 §5.5 判据补充 9/10 |
| lab 节点数据**不保证每 run 前清空**（`make test` 不 wipe） | 上一个 run 残留的值会让本 run 的写索引认为「没人写过」→ 假红 `:fabricated` | 新 workload（map/txn/scan/idem）的 key 与写值都带 **run tag（种子）**，与历史数据完全隔离；`make quick` 仍带 `--wipe-data` |
| **G6 门槛对「高合法失败率」的 workload 偏敏感** | `cas-register` 实测 108 个 cas 里 95 个 `cas-miss`（13/108 = 0.12，贴着 0.1 下界） | **已缓解（第四轮）**：G6 分母已扣除 §5.1 白名单里的合法失败；同时按 T1.5 的 workload 质量项把 `cas` 的 `old` 改成「最近一次观察值」（read-then-CAS），命中率由真实并发冲突决定而非由 200 元窗口决定 |
| `--idem-replay-node-offset` 单独用**不能**复现跨节点重放 | 会让人误以为已覆盖 F-03 | 已写入 F-03 的负面结果与有效配方（`--nemesis kill --idem-replay-delay-ms 4000`），并在 CLI help 里写明 offset 只影响**首次尝试**的起点 |
|:--|:--|:--|
| T0.2 的 quiet 可用率门槛在短跑里**恒不判定**（样本 < 100） | 短矩阵拿不到可用率结论 | 已在 summary 里显式输出 `quiet-judged`/`quiet-skipped-small-sample`，短跑只作回归不作可用性证据 |
| RTO 只对"写阶段仍活跃"的 disruption 有定义 | soak 尾部扰动恒为 `not-measured` | 已在 `gates/rto` 实现并在真实 store 上验证（`unrecovered 0 / not-measured 1`）；集群最终恢复由 soak checker 的收敛门禁判 |
| F-05 的鉴权失败会让短矩阵出现非白名单 `:fail` | 假红 / 可用性噪声 | 需 §5.4-③ 与 E4 一起定；短期在 run 记录里单独统计 `:no-client` |
| M5 依赖 `T1.4 ⇒ T5.2` 闸口 | 若 T1.4 延误，M5 冒烟顺延 | T1.4 主体已落地并跑出结论；剩余"跨节点重放"分支不影响 M5 的设计前提（去重不可依赖），只是补齐证据 |
| **docker lab 的时钟测量精度不足（RTT ≈ 270ms）** | `clock offset` 只能判到 ±135ms，无法审计 100ms 上界 | 已在报告里显式打印不确定度并按"可证才 FAIL"判定；真需要 100ms 级结论时用 vagrant lab |
| **T1.4 的红会让 `make matrix` 一类回归套件长期红** | 回归套件失去"红=新缺陷"的判别力 | 已按 §6 立项（F-01/F-02 是待修缺陷）；在修好之前 T1.4 **不进**回归矩阵，单独作为"证据 run"跑并在 §6 缺陷账里跟踪 |
| 里程碑依赖链上的 T1.1/T1.2/T1.3 已完成 | —（已解除） | 三项均已在 lab 上实跑绿（见 §1.0）；M1 收口（T1.5）可直接起跑 |

## 4. 变更记录

| 日期 | 变更 |
|:--|:--|
| 2026-09-17（第四轮） | **M1 收口**：T1.5 `--workload mixture`（map+txn+scan 混合，`--mixture-ratio` 目标份额）+ 组合 checker `jepsen.coord.mixck`（含**组合层活性门槛**：每个面最小样本 + 禁止未路由 op）+ 11 个新 fixture（10 套 75 个）；**CAS 命中率改进**（read-then-CAS，`client/last-seen`）；**F3 值大小扫描**（4KB/64KB）；`matrix-m1` 扩到 9 组合；`coord-soak.sh`/`make soak` 支持 `--seed`/`--concurrency`/`--mixture-ratio`/`--map-min-deletes` 透传；实测口径修正一条（`gen/mix` 份额 = 实例数占比）；发现并记录我方流程缺陷一条（run 中间改 bind-mount 源码）。**M2 T2.0+T2.1 落地并 lab 验证绿**：BIDI 流式接线（`CoordRpc` + `Watcher`）+ `--workload watch` + `jepsen.coord.watchck`（4 条判据 + 样本门槛 + 3 个语义档）+ 11 个 fixture（14 套 86 个）；`matrix-m2` 目标就绪。新面首跑暴露 **F-19/F-20/F-21**（全为测试自身，已修 + 补 fixture + 写进 §5.5 判据补充 9/10）与 **F-22**（`coord-soak.sh` 漏 `-o`） |
| 2026-09-16（第三轮） | **M1 主体落地**：T1.1 `--workload map`、T1.2 `--workload txn`、T1.3 `--workload scan` 三个 workload + 三个专用 checker + 30 个新 fixture，均在 docker lab 上 `--nemesis kill` 60s 实跑绿；新增 **G6 op 级活性门槛**（F-13 类假绿）；**coord 侧首轮倒逼落地**：F-01（Delete 幂等）+ F-02（Put 命中回放 `prev_kv`）已修；真跑暴露 **F-13…F-17** 五条测试自身缺陷并全部修完；§9-⑨ 已答（F-18） |
| 2026-09-16（第二轮） | **M0 收口**（lab 实跑：checkers 29 fixture 全绿 / env-reset STRICT_CLOCK 全节点 clean / quick SEED=42 绿 / jitter 实测 13 个不同取值）；**T1.4 落地并跑出红证据**（F-01/F-02 `confirmed-by-run`）；修掉 4 条测试自身缺陷 **F-09…F-12**；`proto.clj` 补齐 T1.1/T1.2/T1.3 所需的全部 builder/reader；新增 5 份 evidence 归档 |
| 2026-09-16（第一轮） | M0 六个任务的代码全部落地（其中 T0.1/0.2/0.3/0.5 本地已验证，T0.4/0.6 待 lab）；静态审计产出 F-01…F-08；计划修正 3 处（§5.4-⑥ / T3.4 / T2.1 默认语义） |

### 1.8 第八轮（2026-09-19）：M5b cache / mq 首次 lab 真跑

全部 docker lab、`--agents 1`、45s、`CONCURRENCY=2n`、checker 门槛按 `matrix-m5b` 的取值。
（首跑的三轮失败全部是**测试自身**缺陷，逐步排除过程见 `coord-findings.md` §17 的 F-63…F-66。）

| cell | 判定 | 关键数字 | 备注 |
|:--|:--|:--|:--|
| `cache / none` | **绿**（判据 0 违反） | `:sets 14 :gets 20 :list-ops 38`；`:violations-by-class {}` | 修掉时间轴混用后，同一 cell 由 37 条违反 → 0 |
| `cache / kill-agent` | **未执行**（样本门槛） | `:sets 2 :gets 1`；`:violations-by-class {}`；`:restarts 4` | agent 被杀期间吞吐塌陷 ⇒ §5.1 口径判「未执行」，需更长的 run；判据本身 0 违反（含 `:cache-restart-loss`） |
| `cache / partition-agent-server` | **绿** | `:sets 28 :gets 25 :list-ops 50`；`:violations-by-class {}` | 与 `none` 同判据：与 server 断连不影响本地 cache（符合实现：Cache 全在 agent 本地 redb，`grpc_handlers.rs:381+` 走 `run_blocking`） |
| `mq / none` | **绿**（`Everything looks good`） | `:publishes 81 :published-offsets 81 :delivered-offsets 64`；59 个 Poll 全部非空；0 违反 | **`idem-dups 23`** = F-57 的 lab 证据（同一 `idempotency_key` 连发两次拿到两个 offset）；**`poll-ack-failures 59 / acked-offsets 0`** ⇒ 见 F-67 |
| `mq / kill-agent` | 待复跑 | 首跑：`:publishes 0`、77 个 Poll 全空 | 原因是主题名接线缺陷（生成器拼 run 标签、客户端拿 `nil` ⇒ 主题从未创建），已修（F-66 同族）；修后 `mq/none` 绿 |
| `mq / partition-agent-server` | 待复跑 | 同上 | 同上 |

> 本轮机器时间：docker lab 约 14 次短跑（45–150s）≈ 0.5h，仍在 §7.2 的「短矩阵不计入主表」额度内。
