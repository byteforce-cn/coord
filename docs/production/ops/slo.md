# coord SLO 定义（W5-2）

- **日期**：2026-09-21
- **锚点**：`docs/production/production-readiness-plan-2026-09-21.md` §5 W5-2（P-Gate 7）
- **状态**：**草案，待与 §5.4-③（quiet 可用率 0.95）参数确认方对齐后生效**。
  在参数确认 ③ 签回之前，本文的数值**不得**用于对外 SLA 沟通
  （§4.3 承诺分级：SLA/规模承诺属 **L3**，不在本计划范围内）。

---

## §0 为什么要有这份文件

P-Gate 7 要求「SLO 定义 + 每条告警绑定 runbook」。没有 SLO 时，
「告警该不该响」只能靠人拍脑袋；有 SLO 时，告警阈值与错误预算就是**可对话的数字**。

本文只定义**平台级** SLO（Server + Agent 的协调面）。**业务级** SLO（具体应用的
可用率/时延）由接入方在自己的监控里定义，本文件只约束**我们提供的这一层**。

---

## §1 指标口径（先对齐"算什么"）

| 术语 | 口径 | 来源 |
|:--|:--|:--|
| **请求** | 一次**客户端可见的 RPC**（含经 agent 代理的） | `metrics.method_metrics` / `coord_*` |
| **成功** | gRPC 返回 `OK`；**不含** `RESOURCE_EXHAUSTED`（限流是「拒绝服务」不是「错误」，必须分开看） | gRPC status code |
| **可用率** | 成功请求数 / 总请求数（按**分钟**窗口聚合，再按 SLO 窗口平均） | 见 §2 |
| **时延** | 服务端处理时延（不含客户端排队）；用 p50 / p95 / p99 | `metrics.method_metrics` 直方图 |
| **错误率** | 非 `OK` 且非 `RESOURCE_EXHAUSTED` 的占比 | 同上 |

> ⚠️ **与 §5.4-③ 对齐的红线**：本仓库的 jepsen 参数台账里 quiet 档的
> **可用率门槛是 0.95**。SLO 的可用率目标**不得高于**该口径，否则会出现
> 「生产 SLO 说 99.9%，而验收门槛只证到 95%」这种自相矛盾。
> 因此 §2 的目标值一旦要上调到 99.9%，**必须**同时上调 §5.4-③ 并重新取证。

---

## §2 平台级 SLO（单集群，Region 0）

| # | SLI | 目标（30 天滚动） | 测量点 | 备注 |
|:--|:--|:--|:--|:--|
| **SLO-1** | 协调面**可用率** | ≥ **99.5%**（quiet 档验收门槛 0.95 之上留裕量） | 客户端侧 + 服务端侧**双测**，取**较差**者 | 双测是本仓库对「自述式 no-op」的教训：只测服务端会漏掉「服务端说成功、客户端收不到」 |
| **SLO-2** | 读时延 p99 | ≤ **50 ms**（同机房） | 服务端直方图 | 线性一致性读（`range`）单独统计 |
| **SLO-3** | 写时延 p99 | ≤ **150 ms**（同机房，含 raft 提交） | 服务端直方图 | 失去 quorum 时**快速失败**（`write_timeout_ms`），不计入 p99 分母但计入错误率 |
| **SLO-4** | 错误率（非 OK、非限流） | ≤ **0.5%** | 服务端 + 客户端 | 含 `UNAVAILABLE` / `DEADLINE_EXCEEDED` |
| **SLO-5** | leader 稳定性 | 30 天内非计划 leader 切换 ≤ **3 次** | `changes(raft_leader_id[30d])` | 与 `CoordLeaderChurn` 告警配套 |

### 2.1 明确**不**纳入 SLO 的项（防止"什么都算进去"）

- 客户端主动取消的请求、`RESOURCE_EXHAUSTED`（磁盘只读闸 / 登录限流）。
- 计划内的成员变更、滚动升级、compaction 维护窗口（需在变更记录里标注）。
- 长跑/断网演练期间（`jepsen` 注入窗口）—— 演练数据单独统计，不与生产 SLO 混算。

---

## §3 错误预算（Error Budget）

- 30 天窗口，SLO-1 = 99.5% ⇒ 允许不可用 **≈ 3 小时 36 分钟**。
- 预算消耗 ≥ 50% ⇒ 冻结非必要变更（只上修缺陷）。
- 预算耗尽 ⇒ 停止新特性上线，优先恢复稳定性；已上线的回退。

---

## §4 SLO 与告警的映射

| 告警（`monitoring/prometheus-rules.yml`） | 影响的 SLI | 处置入口 |
|:--|:--|:--|
| `CoordNoLeader` | SLO-1/SLO-4（直接不可用） | `runbook.md#coordnoleader--集群-60s-无-leader` |
| `CoordLeaderChurn` | SLO-5/SLO-2/SLO-3（切换期间时延尖峰） | `runbook.md#coordleaderchurn--3-分钟内-leader-变化--2-次` |
| `CoordFollowerApplyLag` | SLO-2/SLO-3（慢 follower 拖读） | `runbook.md#coordfollowerapplylag--follower-apply-落后-commit--1000-且持续-5m` |
| `CoordDiskWatermarkHigh` | SLO-1（临近只读闸） | `runbook.md#coorddiskwatermarkhigh--coorddiskwatermarkcritical` |
| `CoordDiskWatermarkCritical` | SLO-1（写已开始被拒） | 同上 |
| `CoordWatchBackpressure` | 语义可用率（订阅者丢事件） | `runbook.md#coordwatchbackpressure--watch-丢弃率--10` |
| `CoordAuthDeniedStorm` | 安全面（非 SLO） | `runbook.md#coordauthdeniedstorm--鉴权拒绝率异常` |
| `CoordSealedNodeServing` | SLO-1（配置错误） | `runbook.md#coordsealednodeserving--sealed-仍在写` |
| `CoordBackgroundTaskDead` | **能力面**（第四轮 §6.3.6：能力死亡必须可观测） | `runbook.md#coordbackgroundtaskdead--受监督后台任务死亡` |
| `CoordPlugin*` | 插件面（不影响 SLO-1，影响业务能力） | `runbook.md#coordplugintrap--coordpluginloadfailure--coordplugininvocationerrorrate` |

---

## §5 取证要求（P-Gate 7 的判据）

1. 每条 SLI 都要有**可重跑的采集脚本**（或明确指向 Prometheus 查询），不能只有文字。
2. SLO 数值调整必须记录**裁定人 + 日期 + 理由**（与 §5.4-③ 的联动见 §1 红线）。
3. 7×24 曲线（内存/磁盘/`keep_alive`/重启次数）与 SLO 的关系见 W5-5 / W3-7。
