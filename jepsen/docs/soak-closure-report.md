# soak 结项报告 —— Jepsen 作为 coord 质量倒逼工具的价值核算

> 口径：本文件回答一个问题 —— **soak（Jepsen 测试体系）到底给 coord 逼出了
> 什么**。数据全部来自同目录的 [`coord-findings.md`](coord-findings.md)（缺陷单）
> 与 [`PROGRESS.md`](PROGRESS.md)（逐步证据），每条都可点回源文件核对。
>
> 生成日期：2026-09-17（第四轮收尾）。范围：M0/M1 完成、M2 主体完成（T2.0/T2.1
> 已 lab 验证，T2.2 本轮落地）；M3–M6 未执行（见 §5「未兑现部分」）。

---

## 1. 结论摘要

| 指标 | 数值 |
|:--|:--|
| 缺陷单条目总数（F 编号） | **26**（F-01…F-26） |
| 其中 **coord 侧**（被测系统） | **8** |
| 其中 **测试自身**（我方流程/代码） | **18** |
| coord 侧已修复并有回归证据 | **2**（F-01、F-02） |
| coord 侧已确认行为并**限缩契约文字** | **2**（F-03、F-06） |
| coord 侧行为已判定、可闭环（§9 问答） | **3**（F-07、F-08、F-18） |
| 仍未闭环的 coord 侧条目 | **1**（F-05 登录限流，P1，待 E4 参数确认） |
| checker 负控制 fixture | **17 套 / 99 个**（全部进 `make checkers` 前置门） |
| lab 实跑次数（docker，短跑为主 + 1 次 2h 浸泡） | **81** 次 run（`jepsen/store/coord/` 台账逐目录可数） |
| 其中「离线看着是绿的、真跑才暴露」的测试缺陷 | **11**（F-09…F-13、F-17、F-19…F-21、F-24、F-26 中的假绿/无结论类） |

**一句话**：soak 目前逼出的**真实 coord 缺陷是 2 条（F-01 Delete 无幂等、F-02
幂等命中丢 `prev_kv`），外加 2 条契约措辞与实现不符（F-03 作用域、F-06 Watch 语义）
和 3 条「行为未定义 → 已定义」的闭环（F-07/F-08/F-18）。**
但 soak 的**最大产出其实是我方测试体系的假绿自查**：18 条测试自身缺陷里，
有 11 条属于「报告全绿/无结论而覆盖面为零或判据失效」。这条清单本身是引入决策时最
需要的东西 —— 它决定了「绿」能不能被相信。

---

## 2. coord 侧缺陷账（倒逼产出）

| # | 一句话 | 分级 | 状态 | 证据 |
|:--|:--|:--|:--|:--|
| F-01 | `Delete` 不参与 `request_id` 幂等去重，契约却明确承诺；**范围删重放会删掉首次执行之后新写入的数据** | P1 | **已修 + 回归 0 违反** | `evidence/20260916T132103Z-t1.4-idempotency/`（47/47 复现）→ `...-t1.4-regression-after-f01-f02-fix/`（154 组 / 352 次重放 0 违反） |
| F-02 | `Put` 幂等**命中**时 `prev_kv` 恒为 `None`（去重生效了，但返回的不是首次执行的结果） | P2 | **已修 + 同一回归** | 同上（26/26 → 0；不带 setup 的对照组 0 违反，证明去重确实生效） |
| F-03 | 幂等缓存是单节点进程内（60s / 4096 FIFO），不随 raft 复制、不持久化 ⇒ **换节点/换 leader/重启后重放会重复生效** | P1 | **已确认 + 契约文字限缩** | `...-t1.4-cross-node-f03/`（13 组 `:revision-advanced`）；`kv.proto` / `txn.proto` 的 `request_id` 注释已写明四个边界 |
| F-05 | 60s 短跑即可见 `:fail :write [:no-client Failed to authenticate]`（登录限流 × 重启窗口） | P1 | **confirmed-by-run，未闭环** | `store/coord/latest/history.txt:31`；需 §5.4-③ / E4 参数确认 |
| F-06 | Watch 语义既不是 coalescing 也不是 lossless：**丢最旧 + 合成 `BufferOverflow` + 按 revision 去重** | — | **已判 + 改计划** | `coord-server/src/watch/mod.rs:228/267/90`；`--watch-semantics` 三态 + 同一条历史两个期望值相反的 fixture |
| F-07 | 磁盘写满行为**已定义**：<5% → 写 `RESOURCE_EXHAUSTED`、读仍可用、恢复空间自动恢复 | — | **已判（§9-② 闭环）** | `disk_watermark.rs:6` + `server/mod.rs:329` 的 6 个写入口 |
| F-08 | Lease 到期判定用**单调时钟**（`tokio::time::Instant`）⇒ 墙钟跳变对租约无效 | — | **已判（§9-⑤ 闭环）** | `lease/mod.rs:43`；据此把 T3.4 从「墙钟跳变」改成 `pause` / `kill` 两类**能真正证伪**的故障 |
| F-18 | key 的 `version` 起始值与「不存在」的表示：新建 1、删除也 +1、软删除被全读路径过滤 ⇒ 「不存在」= version 0 ⇒ `Compare{VERSION,EQUAL,0}` 是精确存在性判定 | — | **已判（§9-⑨ 闭环）** | `storage/mvcc.rs` 的 `new_key` / `update` / `mark_deleted` / `evaluate_compares_in_tx` |

### 2.1 这 8 条里，「只有真跑才能拿到」的是哪些

| 类型 | 条目 | 为什么静态/单测拿不到 |
|:--|:--|:--|
| 数据破坏 | F-01 第 3 条（范围删重放删掉新写入） | 需要「首次执行 → 区间内新写 → 同 rid 重放」的**时序**，单测里没人会这么写 |
| 静默错误结论 | F-02（命中丢 `prev_kv`） | 只有「去重命中」这条路径才暴露，而命中的前提是**真实重放** |
| 契约与实现不符 | F-03（作用域四个边界） | 边界是「换节点/重启」这类**集群级**事件，单进程测试构造不出来 |
| 行为未定义 | F-05（限流 × 重启窗口） | 需要「节点刚被 kill 重启」的窗口叠加 |
| 语义口径 | F-06（Watch 溢出语义） | 需要**慢消费者 + 缓冲区打满**才可见 |

---

## 3. 我方测试体系缺陷账（18 条，含「假绿」自查）

这一节是 soak 体系**自我证伪**的产出，价值不低于第 2 节：它回答了「你凭什么
相信这份绿」。

| 类 | 条目 | 后果 | 若没有 soak 会怎样 |
|:--|:--|:--|:--|
| **整类 op 从未发出**（假绿） | F-13（`txn-req` 用不存在的 `addAllField`）、F-19（watch oneof 塞普通 map） | 整个 Txn 面 / watch 面 completion 全 `:info`，knossos 对空历史判 valid ⇒ **报告全绿、覆盖面为零** | 引入评审会看到「Txn 矩阵全绿」，而 Txn 从未被测 |
| **整类 op 变成 `:info`** | F-14（读值当整数解析）、F-15（无缓存 revision 直接 `:info`） | 整类 `:read`/`read-at` 从未成功 | 同上 |
| **判据方向错**（假红） | F-17（写索引只喂 `:ok`）、F-20/F-21（派生字段取了循环变量） | 合法历史被判违约（F-21 造出 7 条**看着像被测系统违约**的 `:watch-event-loss`） | 会把测试噪声当成 coord 缺陷立项，浪费双方时间并损害信任 |
| **判据漏一类 op**（假红） | F-16（map 的 knossos 路径丢掉 invoke op） | 这一类 op 的合法历史被判违约 | 同「判据方向错」 |
| **门槛静默失效** | F-09（`:or` 不接管显式 nil）、F-12（摘要器挂掉 → 门槛结论恒 `unknown`） | 硬门槛名存实亡 / 证据链断在摘要一步 | 「可用率 ≥0.95」这类承诺实际上从未被检查 |
| **默认值被遮蔽** | F-24（`:keys [.. surfaces ..]` 遮蔽本 ns 的同名默认值） | 默认三面组合直接报「no surfaces」——只在走默认路径时出现 | 默认档要么恒绿要么恒红，取决于遮蔽方向 |
| **节拍退化** | F-11（`clojure.core/cycle` 预求值 ⇒ 抖动恒定） | 故障排期与集群周期共振，覆盖不到真实抖动 | 会得到「抖动已覆盖」的假结论 |
| **环境假设错** | F-10（离线 lab 的 `lein` 挂死）、F-22（soak 启动器漏 `-o`） | 门槛/证据链不可用、soak 起跑即假死 | 长跑结果不可信 |
| **单位/时间基准错** | F-04（纳秒当 epoch 毫秒） | 所有按时间取值的门槛失效 | 同上 |
| **读取期错误让整文件失效** | F-23（docstring 里的 ASCII 引号让 ns 编译不过，报错指向别处）、F-25（少一个 `[`，报错落在文件末尾） | 诊断成本远高于缺陷本身；在 lab 里极易被误读成「被测系统的缺陷」 | 每改一次 checker 都要靠猜 |
| **checker 崩溃 ⇒ 无结论** | F-26（`leaseck` 缺活性锚点时 NPE，jepsen 把整个 checker 降成 `:valid? :unknown`） | 比假绿更隐蔽：报告里写的是「没有结论」，而没人会去看 | 「绿」与「没判」分不清 |

**闭环动作**：每一条都落成了**机制**，而不是「下次注意」：

* 假绿类 → **G6 op 级活性门槛**（`--min-op-ok-ratio` / `--min-op-sample`）
  + **组合层路由门槛**（`mixck` 的 `routing-checker`：每个面必须达到最小样本，
  且不允许存在未被路由的客户端 op）+ 每个新 workload 必须自带「这个面真的跑了吗」
  的判据且进 `:valid?`（dev.md §5.5-5/9）。
* 假红类 → 判据补充写进 dev.md §5.5-6/7/8/10（`:info` 写算可能生效、nil 判据的探针、
  历史读不适用陈旧读、汇报字段粒度 = 被汇报事实的粒度）。
* 门槛类 → F-09 的默认值只用 `or` 解析 + `gates-fixtures-defaults/` 固定住
  「只能用生产默认门槛判出 invalid」这一档；F-12 用 `:default` reader 修摘要器。
* 无结论类 → F-26 缺锚点不再抛异常，改记 `:liveness-unjudged` 计数并由门槛
  显式暴露（**`unknown` 不得当作通过**）；F-24 改用不遮蔽的 `surfaces*` 局部名，
  由 `mixture-fixtures/` 的默认三面档固定住。
* 节拍类 → 判据从「间隔 ∈ [3,8]s」改成**看取值离散度**（周期 ≥6 时不同取值 ≥3）。

---

## 4. 门禁与证据链（「绿」凭什么可信）

| 门禁 | 内容 | 现状 |
|:--|:--|:--|
| `make checkers` | **17 套 / 99 个** checker fixture（每个 checker ≥1 个负控制 + 门槛档） | lab 全绿（含 lease 9+2、soakfull 2） |
| `make matrix-m1` | map/txn/scan/mixture × none\|kill + idempotency:none（45s/组合） | 9 组合全绿 |
| `make matrix-m2` | watch/lease × none\|kill\|pause\|partition-halves（45s/组合） | 8 组合（lease 四档本轮新增） |
| `make nightly` | checkers → matrix-m1 → matrix-m2 → soakfull（默认 2h）+ 等待 + 结果 | 本轮新增（`scripts/nightly-soak-gates.sh`） |
| 功能面实跑（本轮） | `--workload lease` 60s（`grants 207 / expiries 95 / 六类违反 0`）；`--workload soakfull` 90s kill（五面全跑到、`:unrouted 0`） | 两份证据已入库（见下） |
| 证据入库 | `collect-evidence.sh` → `docs/production/evidence/<ts>-<label>/`（MANIFEST 带真实门槛值 / 种子 / commit / 镜像 ID） | 已有 21 份归档 |
| 可回放 | `--seed` 落 `results.edn`；`scripts/replay.clj` 由种子重建故障排期 | 已用真实 store 验证 |

三条「不能让 soak 白跑」的硬约束（已落地）：

1. **样本不足 = 未执行**（既不算绿也不算红）：`--map-min-deletes` /
   `--watch-min-events` / `--lease-min-grants` / `--lease-min-expiries` /
   `mixck` 的 `:min-sample`。任何「一条 op 都没跑完」的 run 不会产出绿。
2. **门槛参数必须真到达 checker**：`checkers` 有专门的 fixture 档证明门槛接上了
   （`gates-fixtures-defaults`、`watch-fixtures-sample`、`lease-fixtures-sample`、
   `mixture-fixtures-routing`、`soakfull-fixtures`）。
3. **组合不得静默丢面**：T6.1 的 `--soak-mix` 里出现未实现的面（lock/election/
   registry）时**构造期硬失败**；已跑出的 op 若不在声明面内则路由门槛判红。

---

## 5. 未兑现部分（诚实清单）

soak 的价值 = 覆盖面 × 可信度。**当前覆盖面仍是局部**：

| 未做 | 影响 | 登记位置 |
|:--|:--|:--|
| M2 收口 T2.3（2h watch+lease 混合浸泡） | 尚无 M2 的长跑结论（短跑已绿） | dev.md §4 M2 |
| M3（membership / compact / 网络增强 / 时钟域 / 磁盘满） | 运维面（成员变更、快照压缩、故障域）无端到端证据 | dev.md §4 M3 |
| M4（PD split / 跨 region / region compact） | 多 raft 动态调度无证据（`multi-register` 静态多 region 已可跑） | dev.md §4 M4 |
| M5（agent 层：lock / election / registry / RBAC + 24h 冒烟） | **T6.1 的 3 个面（lock 10% / election 3% / registry 2%）无法跑**，`--soak-mix` 里一出现就构造期失败 | dev.md §4 M5、本报告 §4-3 |
| M6（12h 高压 / **72h soak-full**） | 尚无长跑结论；2h 浸泡是当前最长 | dev.md §4 M6 |
| 延期项 B3（Seal/Unseal）、B7（滚动升级）、E2（TLS 矩阵） | 明确延后到生产动作触发前 | dev.md §8 |
| F-05（登录限流） | 唯一未闭环的 P1 coord 条目 | coord-findings.md F-05 |

**因此本报告不声明「通过」**：它声明的是「已执行的范围内，缺陷账与证据链可核对，
且假绿机制已被系统性堵住」。

---

## 6. 复现方式（评审可自行核对）

```bash
# 1) 全部 checker 负控制（离线 lab，~3min）
cd jepsen/lab && make checkers JEPSEN_PROVIDER=docker
# 2) 数据面矩阵（~12min）
make matrix-m1 JEPSEN_PROVIDER=docker
# 3) Watch + Lease 矩阵（~8min）
make matrix-m2 JEPSEN_PROVIDER=docker
# 4) 组合浸泡（T6.1 入口；本地验证用 10min 档）
make soakfull SOAK_TIME_LIMIT=600 SEED=42 JEPSEN_PROVIDER=docker
make soak-wait && make soak-results
# 5) 夜间门禁（= 上面 1–4 + 归档，供定时任务调用）
jepsen/scripts/nightly-soak-gates.sh          # SKIP_LONG=1 只跑 1–3
```

本轮已入库的两份功能面证据（均可直接点开核对）：

| 证据 | 内容 | 判决 |
|:--|:--|:--|
| `docs/production/evidence/20260917T144810Z-t2.2-lease-60s/` | `--workload lease --nemesis none --time-limit 60 --concurrency 2n`（seed 42） | `overall-valid true / gates-valid true`；`grants 207 / expiries 95 / keepalive 58 / revoke 54`、`keepalive-responses 348`、六类违反 0 |
| `docs/production/evidence/20260917T144816Z-t6.1-soakfull-90s-kill/` | `--workload soakfull --nemesis kill --time-limit 90 --concurrency 2n`（seed 42） | `overall-valid true / gates-valid true`；`:routing {:by-sub {:map 134 :txn 57 :watch 54 :lease 30 :scan 12}, :unrouted 0, :insufficient []}` |

每条证据的定位：`jepsen/docs/coord-findings.md` 的 `F-xx` 小节里都有
`store/coord/<时间戳>/` 或 `docs/production/evidence/<时间戳>-<label>/`。
