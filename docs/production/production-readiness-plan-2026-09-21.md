# coord 生产化开发计划（Production Readiness Plan）

- **日期**：2026-09-21
- **基线 commit**：`8b65290`（`origin/main`，工作树 clean）
- **目标窗口**：2026-12-31 前把「生产可用」判据全部转绿（**全量 17 项 + 对象存储**）
- **最近一次定义级变更**：**U-14**（2026-09-25）—— 移除第三方审计门槛（无经费；
  改为「未经独立审计」边界声明），见 §8.7
- **可投入人力**：**1 人**（本计划按此编写；§1.3 给出该配置下的可行性核算与三个备选）
- **体例**：沿用 `docs/production/remaining-known-gaps.md:10-11` 与
  `docs/production/agent-ga-remediation-baseline.md:6-8` —— 每条必须是**可核验事实**
  （`file:line` / 命令 / 测试 / CI run）。**估计值一律标注「估算」**，不得当作承诺。
- **本文不采信**：`WHITEPAPER.md` 的自述、既有计划书的自述。凡与实测冲突处，以实测为准。
- **与在先文档的关系**：见附录 B。契约迁移（v1.2.0）与 agent GA 已闭合的部分**不复述**，
  本文的唯一输入是 §3 的差距清单。

---

## §0 本文定位：为什么需要一份新的生产化计划

三件事同时成立，所以需要本文：

1. **对外白皮书引用的生产就绪判据已经丢失**。`apis/contracts/WHITEPAPER.md:25-26` 写明
   「生产就绪验收以 `docs/production-readiness-remediation-2026-08-27.md` §6 的 **9 道验收门**为准」，
   而该文件**在磁盘与 git 历史中均不存在**（实测：`find / -name 'production-readiness-remediation*'`
   无结果；`git log --all -- docs/production-readiness-remediation-2026-08-27.md` 无输出）。
   且它即使存在，放在 `docs/` 顶层也**不可入库**（`.gitignore:41` ⇒ 见 §3 D-06）。
2. **项目自身已明确「尚不可发布」**。`docs/production/agent-ga-remediation-baseline.md:216`
   的小节标题即「未落地（**v0.2.0 尚不可发布**）」；`jepsen/docs/soak-closure-report.md:20`
   的交付状态分栏把「引入验收结论（soak 通过）」标为 **❌ 未达成 —— 本报告不声明**。
3. **对外文本与台账互相矛盾**（§3 D-05）：同一个仓库里，`README.md:78-81` 说 4 个能力是
   EXPERIMENTAL、`STATUS.md` 说它们已 COMMITTED、`apis/contracts/README.md:10` 还停在 v1.1.0。
   在这三份文本对齐之前，**任何"对外承诺"的动作本身都会传播错误口径**。

因此本文要做的第一件事不是写代码，而是**把「生产可用」重新定义成一组可机械判定的门**（§4），
再把差距（§3）映射成工作流（§5）与排期（§6）。白皮书的悬空引用由本文 §4 接替。

---

## §1 目标、范围与可行性核算

### 1.1 目标

**12-31 前让 §4 的 9 道生产门（P-Gate 1–9）全部转绿**，从而对外可声明
**L2 生产可用**（承诺分级的定义见 §4.3），业务落地随之解锁（§10）。

### 1.2 范围

全量 = `apis/contracts/STATUS.md:14-30` 的 **17 项 COMMITTED + `coord.storage`**，加上
`STATUS.md:36-41` 的 6 项 STABLE 底座（KV/Txn/Lease/Watch/Maintenance/Health）。

按既有批次（`docs/coord-agent-ga-v0.2.0-plan-2026-09-19.md:578-590`）与已公示硬期限：

| 批次 | 能力 | 契约期限 |
|:--|:--|:--|
| 1 | registry / idgen | **2026-10-31** |
| 2 | lock / election | **2026-11-30** |
| 3–6 | event / config / pki / policy / circuitbreaker / ratelimiter / transit / featureflags / storage | 2026-12-31 |
| 7 | cache / mq / workflow / scheduler | 2026-12-31（workflow / scheduler 契约上为 **2027-03-31**） |

> **注意一个口径事实**：本项目**已公示的**最晚期限里，workflow 与 scheduler 是 2027-03-31。
> 因此"12-31 前全部转绿"是**比项目自身契约更严**的要求。这既是本计划的优点（对外承诺唯一），
> 也是 §1.3 缺口的来源。

### 1.3 可行性核算（**1 人 × 14 周**）

日历：2026-09-21 → 2026-12-31 = **101 天 ≈ 14.4 周 ≈ 72 个工作日**（扣节假日取约 **70 人日**）。

§5 的十条工作流按「一项任务一人做完」估算合计 **93 人日**（逐项估算见 §5 各表）：

| 工作流 | 人日（估算） |
|:--|--:|
| W0 文本与台账对齐 | 2 |
| W1 代码缺陷闭环 | 13 |
| W2 门禁可信化 | 10 |
| W3 长跑与故障注入证据 | 20 |
| W4 安全与合规 | 11 → **6**（U-14 移除外审整改 5 人日） |
| W5 可观测性与 SLO | 12 |
| W6 运维与升级 | 11 |
| W7 交付工程 | 7 |
| W8 业务落地准备 | 5 |
| W9 治理 | 2 |
| **合计** | **93 → 88**（U-14 后） |

**结论（必须写清楚）：1 人在 12-31 前做不到「全量 + 9 门全绿」。缺口约 18 人日（26%）**
（U-14 后口径：合计 93 → 88，扣减 §5 W4-3 的 5 人日整改；其 3–6 周**外部交付期**本就不在 93 内、
但也已随 U-14 移出关键路径。若旧口径未把审计整改计入 93，则合计不变、缺口仍为 23 人日 ——
**两种口径下结论相同：1 人不足。**）

三个备选（请人工裁定，本计划不替你选）：

| 案 | 做法 | 结果 |
|:--|:--|:--|
| **A（推荐）** | **加 1 人**（第二人专责 W3 长跑值守 + W5/W6 运维面） | 12-31 可达；长跑与码改可并行 |
| **B** | 维持 1 人，**把 W3 的 14 天运行判定日挪到 2027-01 上旬**，12-31 交付 P-Gate 1–4、7–9 | 12-31 可对外声明「L2 除长跑外已达标」，**不得**声明 L2 完整达成 |
| **C** | 维持 1 人，整体延期到 **2027-01-31** 判定 | 最诚实；对外承诺时间后移一个月 |

**不建议 D（降低判据）**：那等于把"生产可用"重新定义成"当前状态"，本项目自己已多次拒绝这种写法
（见 `remaining-known-gaps.md:249`「红色的门禁不是严格的门禁，是教人忽略的门禁」）。

### 1.4 不可压缩的日历约束（比人日更硬）

| 约束 | 硬性来源 | 倒排 |
|:--|:--|:--|
| ≥14 天连续运行 | `docs/coord-review-verification-and-remediation-2026-09-12.md:295`（Gate 3 出口） | 必须在 **12-17 前**起跑 ⇒ RC 冻结 ≤ 12-16 |
| 72h soak-full | `jepsen/docs/dev.md:323`（T6.1） | 需 3 天墙钟，且须在 RC 冻结后 ⇒ 12-10 → 12-13 |
| ~~第三方安全审计~~ | ~~`WHITEPAPER.md:509`（§12.2 缺失项） + 第四轮 §6.3~~ ⇒ **U-14（2026-09-25）移出门槛**：不采买（无经费），改为「未经独立审计」边界声明（§8.7） | —— |
| 14 天运行与 72h 不能抢同一套 lab | 本仓 lab 为单套（`jepsen/lab/`，docker 或 vagrant 二选一） | 两套环境（docker + vagrant）或串行 ⇒ 串行必然跨年 |

> **推论（2026-09-25 更新，U-14）**：审计采买原是本计划最长的外部依赖，现已裁定**不采买**
> ⇒ 关键路径上**不再有外部交付期**；剩余硬约束全部是**工程进度**（W1 清账 → RC 冻结）与
> **墙钟**（72h + 14 天，lab 单套）⇒ **判定日不再被任何外部依赖拖住**；若 W1–W3 提前完成，
> 冻结与长跑可提前起跑，判定日可早于 12-31（**不晚于** 12-31）。

---

## §2 现状基线（本次独立核验，2026-09-21）

### 2.1 绿（可作为承诺依据）

| 项 | 核验方式（可复现） | 结果 |
|:--|:--|:--|
| 五道本地卡口 | `bash apis/contracts/scripts/check-wire-sync.sh` 等 5 个脚本 | 全部 `exit 0`（wire-sync / wire-descriptor / sdk-sync / panics 0 violations / error-code 一致） |
| 证据归档完整性 | `cd docs/production/evidence && for d in */; do (cd $d && sha256sum -c --quiet sha256sums.txt); done` | **28 份全过，0 失败** |
| HEAD 的 push CI | `8b65290` push run（2026-09-20T16:31Z） | 绿：fmt+clippy / workspace tests / proto contract / java sdk / java-example-it / plugin matrix / frontend lint |
| 契约面 | `CHANGELOG.md` `[contracts/v1.2.0]` + 三道卡口 | Minor、无 Breaking、17 包 + storage 已挂契约包 |

### 2.2 红（当前阻断项）

| 项 | 证据 | 性质 |
|:--|:--|:--|
| **F-27 lease 过期 revoke 可静默丢失** | `coord-server/src/server/mod.rs:766-790`（`client_write` 失败只 `warn!`，**不重试不回插**）+ `coord-server/src/lease/mod.rs:115`（`check_expired()` 返回前已把过期 lease 移出本地管理器）⇒ 该次 revoke **永久丢失**；`jepsen/docs/coord-findings.md:838` 记 `confirmed-by-run`、**未闭环**；`git log --since=2026-09-17 -- coord-server/src/lease/` **无输出** | **合同级缺陷：契约承诺「Lease 过期 ⇒ 绑定 Key 级联删除」，实现留了永久泄漏口** |
| F-05 登录限流 × 重启窗口 | `jepsen/docs/coord-findings.md:29`、`:216`（`confirmed-by-run`，未闭环） | 60s 短跑即可见 `:no-client Failed to authenticate` |
| F-68 jepsen checker 未纳入 Poll `start_offset` | `docs/production/evidence/README.md:111-116` | 被测系统侧无缺陷，但 **多消费者拓扑会永久红** ⇒ 门禁不可信 |
| 定时门禁不可信（近 31 次 CI run 统计，见下） | GitHub Actions API 聚合 | 见下表 |
| 对外文本三处矛盾 | `README.md:17`、`README.md:78-81`、`apis/contracts/README.md:10`/`:31` vs `STATUS.md:14-30` | 消费者按任一份都会读错承诺面 |

**CI 门禁可信度（我按 run 逐 job 聚合，非文档自述）**：

| job | 红/运行 | 说明 |
|:--|:--|:--|
| `weekly perf baseline` | **13/13** | 该 job 仅 `if: schedule`（`ci.yml:448`）⇒ **从未通过过一次** |
| `cargo audit + deny` | 22/31 | HEAD 上 09-21 的 3 次定时跑全红，而 09-20 同 SHA 的 push 跑为绿 ⇒ 环境/通告类，**根因未定位** |
| `real-process chaos` | 14/31 | 间歇红；含 09-21 06:16 最近一次定时跑 |

> **限制与诚实声明**：`gh` CLI 不可用，job 日志端点需鉴权（实测 403），
> **因此上述三处红只报了现象，没有根因**。W2 的第一件事就是取日志定位。
> 另外本计划**没有**在本机重跑 workspace 全量测试（无 `target/`，从零编译代价过高），
> "workspace tests 绿"引用的是 CI 在 `8b65290` 上的实跑结果。
>
> **2026-09-25 更新（详见 §11 第八轮）**：表内三处红已全部有结论 ——
> `weekly perf baseline` 与 `cargo audit + deny` 已在 schedule 事件转绿
> （前者于 run `36086768094` 首次通过）；chaos 近 9 次 7 绿 / 2 红且两次红均已归因。
> 本表作为 2026-09-21 的基线原样保留。

### 2.3 长跑与覆盖现状

| 维度 | 现状 | 锚点 |
|:--|:--|:--|
| 已归档最长浸泡 | **2 小时** | `docs/production/evidence/20260917T134013Z-t1.5-mixture-soak-2h/` |
| 72h soak-full | **未跑** | `soak-closure-report.md:180-190`（§5 未兑现部分） |
| ≥14 天连续运行 | **未做** | `coord-review-...-2026-09-12.md:295` |
| jepsen 里程碑 | M0/M1 完成、M2 主体完成；**M2 收口（T2.3）/ M3 / M4 / M5 / M6 未执行** | `soak-closure-report.md:186-190` |
| 覆盖缺口（最重要） | **idgen、registry 是 10-31 硬期限，却没有 lab 产物**（checker 已写：`jepsen/src/jepsen/coord/idgenck.clj`、`regck.clj`） | `jepsen/docs/dev.md:488`（M5 覆盖方案自述「缺 idgen 与 event」） |
| 拓扑缺口 | 本 lab 的 agent 只绑 loopback + SSH 隧道（彼此不可达）⇒ **ISR 复制面与跨 agent 投递结构性不可测** | `jepsen/docs/dev.md:195`（F-58） |
| 证据效力 | 全部归档的 worktree 字段为 **DIRTY**（run 都发生在提交之前）；§5.4 参数确认 **③ 未签**（需引入方团队签） ⇒ 依赖 ③ 的门禁结论**「不得用于引入评审」** | `soak-closure-report.md:20-40`、`docs/production/evidence/README.md:75-84` |

---

## §3 差距清单（本文的唯一输入）

严重度两轴：`阻断哪些生产门` / `是否阻断业务落地`。「阻断」= 不修则 §4 的门不可能绿。

| # | 差距 | 证据锚点 | 阻断门 | 本文归属 |
|:--|:--|:--|:--|:--|
| **D-01** | F-27：lease 过期 revoke 丢失 ⇒ 绑定 Key 永久泄漏（违反级联删除契约） | `coord-server/src/server/mod.rs:766-790`、`lease/mod.rs:115`、`coord-findings.md:838` | 1, 3, 4 | W1-1 |
| **D-02** | F-05：登录限流 × 重启窗口的鉴权失败 | `coord-findings.md:29`、`:216` | 1, 3 | W1-2 |
| **D-03** | F-68：jepsen checker 未纳入 Poll `start_offset` ⇒ 多消费者拓扑永久红 | `evidence/README.md:111-116` | 4 | W1-3 |
| **D-04** | 门禁可信度：perf 13/13 常驻红、audit 定时红、chaos 间歇红；分支保护未设置 | CI 聚合（§2.2）、`remaining-known-gaps.md:74-78`（B5⑧） | 1, 8 | W2 |
| **D-05** | 对外文本三处矛盾（免责声明 vs Stability labels vs 台账 vs contracts README） | `README.md:17`/`:78-81`、`apis/contracts/README.md:10`/`:31`、`STATUS.md:14-30` | 9 | W0 |
| **D-06** | 白皮书 §12 引用的「9 道验收门」文档不存在且不可入库 | `WHITEPAPER.md:25-26`、`.gitignore:41` | 9 | W0-4（本文 §4 接替） |
| **D-07** | 长跑缺失：72h 未跑、14 天未做、最长归档 2h | §2.3 | 5 | W3-6/7 |
| **D-08** | 覆盖缺失：M2 收口/M3/M4/M5/M6；**idgen、registry 零 lab 产物（10-31 硬期限）** | §2.3 | 3, 4 | W3-1…4 |
| **D-09** | 多 agent 拓扑不可测 ⇒ cache/mq 复制语义无法验收 | `dev.md:183-188`（F-58） | 3 | W3-5 |
| **D-10** | 证据效力：28 份 DIRTY；§5.4 ③ 未签 | §2.3 | 3, 5 | W3-8/9 |
| **D-11** | 安全：TLS 非 fail-closed；KEK 由配置串确定性派生；~~无第三方审计~~（已由 U-14 裁定移出门槛 ⇒ 化为「未经独立审计」边界声明义务，见 §8.7）；`SECURITY.md` 支持版本表只列 0.1.x | `README.md:61`、`WHITEPAPER.md:518`（§12.7）、`SECURITY.md` | 6 | W4 |
| **D-12** | 运维面空白：**无 runbook**（全仓 `grep -rn runbook` 仅命中白皮书的"缺口"表述）；`deploy/k8s/statefulset.yaml` 无验证证据；告警无 runbook 绑定 | `monitoring/prometheus-rules.yml`、`deploy/k8s/` | 7 | W5/W6 |
| **D-13** | 无界增长四项 + 未接线机制（`MemoryWorkflowStore`+5s 全扫、动态 region 清理、`snapshot_logs_since_last=0`、连接数上限；81 处 `tokio::spawn` vs 5 处监督） | `第四轮.md:6.2` 第 17–21 项 | 1, 5 | W1-4/5 |
| **D-14** | 默认开关反直觉且未裁定：`cache`/`workflow` 默认 **true**（曾是未整改面）、承诺面 `leader_election`/`event_notification` 默认 **false** | `docs/production/agent-ga-remediation-baseline.md:55-71`（E5b / R8 / G6） | 3, 9 | W0-5 + W1-6 |
| **D-15** | 交付面：v0.2.0 未 tag；SDK 发布未裁定（U3/R7）；release.yml 历史失败点 | `git tag`（仅 `v0.1.0`）、`Cargo.toml:15` = 0.2.0、计划 §8 R7 | 8 | W7 |
| **D-16** | 未决项未裁定：U1 / U3 / U4 / U5 / U6 / U8 / U9 | 计划 §8「未决项」表 | 9 | W0-6 |
| **D-17** | enum 演进无机械保护（`buf.yaml` 豁免 `ENUM_VALUE_PREFIX`/`ENUM_ZERO_VALUE_SUFFIX`；改名对 grpc-java 是**静默破坏**） | `remaining-known-gaps.md:154-165`（C22）、`coord-proto/buf.yaml` | 2 | W1-7 |
| **D-18** | 免责声明与任何生产承诺互斥（`README.md:17`） | 同 D-05 | 9 | W0-3 |
| **D-19** | **「默认开关」两条路径并未拉平**（U-03 的原话过度声称）：`registry` / `config_center` / `lock` / `idgen` / `policy` / `pki` 六项**代码默认 `true`**，但字段是普通 `#[serde(default)]` ⇒ **配置文件路径**下缺省 `false`。⇒ 同一个 agent「走不走 `--agent-config`」得到**不同的服务集合**（**表在但键缺**时全关；**整张表缺失**时（字段级 `#[serde(default)]` 回落到 `ServiceConfig::default()`）走的是**代码默认**——两层语义不同，更正见 §11 第七轮） | `coord-agent/src/service.rs`（六处 `#[serde(default)]` vs `impl Default`）；判据：`test_known_divergence_code_default_vs_toml_is_pinned`（**故意钉住不一致**，改任一侧即红）；**发现于 2026-09-22**（因为把 `pki` 加进 `test_service_config_toml_missing_fields_match_code_defaults` 的比对而暴露） | 9 | **✅ 2026-09-24 裁定（U-12，见 §8.6）取②：六项代码默认改 `false`**（严守 G9「默认关」）并已落地；判据改为 `test_service_config_toml_missing_fields_match_code_defaults` 的**全字段逐字段等价**——旧的 `test_known_divergence_code_default_vs_toml_is_pinned` 已删（不一致已消） |

---

## §4 「生产可用」的定义（可机械判定）

### 4.1 平台级：9 道生产门（P-Gate 1–9）

每道门 = 一组**可执行判据** + 一份**留档产物**。任一门红 ⇒ 不得声明 L2。

| 门 | 名称 | 判据（可执行） | 产物 |
|:--|:--|:--|:--|
| **P1** | 代码面 | `cargo fmt --all -- --check` / `cargo clippy --workspace --all-targets -- -D warnings`（非测试目标）/ `bash scripts/check-panics.sh` / `cargo test --workspace --all-targets` 全绿；**且 `remaining-known-gaps.md` 与 `coord-findings.md` 中无未闭环 P0/P1**（D-01/D-02 已闭环） | CI run 链接 + 本地 `run.log` |
| **P2** | 契约面 | `check-wire-sync.sh` + `check-wire-descriptor.sh` + `check-sdk-sync.sh` + `buf breaking` 全 `exit 0`；**无悬空文档引用**（`grep -rn 'docs/[a-z-]*\.md' --include=*.md` 逐条可达） | 三个脚本输出 + 引用可达性清单 |
| **P3** | 语义验收 | 每个 COMMITTED 能力有**端到端可复现证据**，且覆盖**两个正交维度**：**经/不经 agent** × **开/不开鉴权**（第四轮 §7 指出的"两个维度从未交叉"必须被打破） | `docs/production/evidence/<ts>-<cap>/` |
| **P4** | 故障注入 | kill / pause / partition × 各能力矩阵绿，**无 `invalid` 档**（D-03 修好后 `matrix-m2`/`matrix-m5`/`matrix-m5b` 全绿） | `jepsen/lab` `make matrix-*` 输出 |
| **P5** | 长跑 | **72h soak-full 绿** + **≥14 天连续运行**（无重启 / 内存曲线有界 / `keep_alive` 无挂起） | 两份归档 + 曲线图 |
| **P6** | 安全 | TLS **fail-closed** 落地并验证（D-11）；KEK 供给裁定落地（U-04）；`cargo deny` 零未闭合豁免（当前 bincode 豁免须有替代路径）；**「未经独立第三方安全审计」边界声明落地**（U-14，2026-09-25：无经费 ⇒ **第三方审计不再作为门槛**；**反向判据** = 任何文本出现"经审计/独立审计"暗示即置红，见 §9-4） | `cargo deny` 输出 + 边界声明 diff |
| **P7** | 可观测与运维 | SLO 定义 + 每条告警绑定 runbook + 运维演练记录（升级 / 回滚 / 备份恢复 / 密钥轮换 / Seal-Unseal）；**"能力死亡必须可观测"**（第四轮 §6.3.6） | `docs/production/ops/` + 演练归档 |
| **P8** | 交付工程 | 版本 tag + 制品与校验和 + SBOM + 分支保护已设置（direct push 被拒）+ **门禁负控制演练通过**（注入违规 ⇒ 必须置红） | release 页 + 演练记录 |
| **P9** | 文档与承诺一致 | `README.md`(EN/zh-CN) / `apis/contracts/README.md` / `STATUS.md` / `WHITEPAPER.md` **四处口径逐条一致**；免责声明已裁定（D-18）；消费者告知已按 §11.2.1 走完 | 四份 diff + 裁定记录 |

### 4.2 能力级：G1–G9（在既有 G1–G6 上补三条）

既有 G1–G6 见 `docs/coord-agent-ga-v0.2.0-plan-2026-09-19.md:565-575`（契约包 / 语义验收 /
版本头 / 文档 / 边界声明 / **默认开关口径**）。本计划补三条：

| 条 | 内容 | 判据 |
|:--|:--|:--|
| **G7** | 该能力在故障注入下有 e2e 证据（不是只跑通 happy path） | 对应 `matrix-*` 档绿，且**负控制**存在 |
| **G8** | 该能力进入 soak-mix 并在 72h 中达标 | `--soak-mix` 含该面 + soak 报告分面统计 |
| **G9** | 默认开关口径已裁定**且与整改状态一致**（不得"默认开启未整改面"） | `coord-agent/src/service.rs:160-182` 与裁定表逐条对齐 |

**逐能力覆盖表**（`✅` 已有可复现证据 / `🟡` 部分 / `❌` 无）：

| 能力 | 期限 | lab e2e | 故障注入 | soak 覆盖 | 默认开关 | 主要待办 |
|:--|:--|:--:|:--:|:--:|:--:|:--|
| 底座 KV/Txn/Watch/Lease | STABLE | ✅ | ✅（M1/M2 矩阵） | 🟡（2h） | n/a | **W1-1**（F-27）、W3-1 |
| registry | **10-31** | ❌ | ❌ | ❌ | true | W3-4（checker 已在，零产物） |
| idgen | **10-31** | ❌ | ❌ | ❌ | true | W3-4 |
| lock | 11-30 | 🟡（`matrix-m5`） | 🟡 | ❌ | true | W1-6（scope 过度限制）、W3-5 |
| election | 11-30 | 🟡 | 🟡 | ❌ | **false** | W0-5（默认开关裁定） |
| event | 12-31 | ❌ | ❌ | ❌ | **false** | W3-4 |
| config / pki | 12-31 | 🟡（进程级） | ❌ | ❌ | true | W3 |
| policy / cb / rl | 12-31 | 🟡（进程级） | ❌ | ❌ | true/false | W1-6（边界已声明，需 e2e） |
| transit | 12-31 | 🟡 | ❌ | ❌ | **false**（U-11） | W4-2a（**已落地**，2026-09-22） |
| featureflags | 12-31 | 🟡 | ❌ | ❌ | false | W3 |
| cache | 12-31 | 🟡（不可测 ISR） | 🟡 | ❌ | **false**（U-03） | **W3-5**（多 agent 拓扑）、W0-5 |
| mq | 12-31 | ✅（2026-09-21 `1n`/`1` 两份归档） | 🟡 | ❌ | false | W1-3（F-68，**已闭环**）、W3-5 |
| workflow | 2027-03-31 | 🟡（进程级） | ❌ | ❌ | **false**（U-03） | W1-4（无界增长）、W0-5 |
| scheduler | 2027-03-31 | 🟡 | ❌ | ❌ | false | W3 |
| storage | 12-31 | 🟡（进程级+混沌） | 🟡 | ❌ | n/a | W6-4（快照 rebuild 边界） |

### 4.3 承诺分级（对外沟通用；**不得越级声明**）

| 级 | 名称 | 门槛 | 可对外说的话 |
|:--|:--|:--|:--|
| **L0** | **接口承诺** | P2 + P9 绿 | 「wire 已冻结、兼容规则与不承诺清单已成文；自本契约文件生成客户端，不依赖服务端反射」 |
| **L1** | **受控试验接入** | L0 + 该能力的 G1–G7 绿 + 接入方书面接受边界（§10） | 「可在非关键路径试点，我们与接入方共同承担风险，随时可回退」 |
| **L2** | **生产可用** | **P-Gate 1–9 全绿**（U-14 后：P6 **不含**独立外审） | 「生产可用」——**这是本计划的目标级** |
| **L3** | **SLA / 规模承诺** | L2 + 容量规划 + 多集群运维 + 值守 + 演练常态化 | 需要单独的 SLO 文本与赔付口径；**不在本计划范围** |

> **纪律**：`WHITEPAPER.md:509`（§12.1）自己写着「GA 期限前请勿将 COMMITTED 契约当作生产可用面验收」。
> 本计划把 L2 的门槛抬到**高于**契约期限——契约期限到点只说明"接口已冻结并挂载"，
> **不说明"生产可用"**。两者混谈是当前对外口径混乱的根源。
>
> **U-14（2026-09-25）**：P6 不再包含独立第三方安全审计（无经费采买/执行）。因此
> **L0–L3 任何声明都不得出现「经第三方审计 / 独立审计」字样**；涉及安全的表述必须带
> 「未经独立第三方审计」边界（与接入手册 §3/§6 同口径）。

---

## §5 工作流详细设计

> 每条任务给出：**做法 / 判据（命令或测试）/ 产物 / 依赖 / 人日（估算）**。
> 「判据」必须是别人能重跑的东西；写不出判据的任务不允许进入本表。

### W0 文本与台账对齐（2 人日；**第 1 周内完成，先于一切**）

| # | 任务 | 判据 | 产物 |
|:--|:--|:--|:--|
| W0-1 | `README.md:78-81` 的 Stability labels 与 `STATUS.md` 对齐：17 项 + storage 均为 COMMITTED；**删除"Workflow … in-memory placeholder"这一已过期论断**（`KvWorkflowStore` 在 `coord-agent/src/services/workflow_store.rs:39`） | 人工逐行对照 + `grep -n 'EXPERIMENTAL' README.md` 只剩正确的分级说明 | 两份 README diff |
| W0-2 | `apis/contracts/README.md` 更新到 v1.2.0（`:10` 版本号、承诺表 5 → 17+1、EXPERIMENTAL 区清空的说明）；该文件 mtime 停在 **2026-08-29** | `bash apis/contracts/scripts/check-wire-sync.sh` + 人工比对 `STATUS.md` 行数一致 | diff |
| W0-3 | **免责声明裁定**（`README.md:17`「not intended for production use」）：三选一 —— ①撤除并改为 §4.3 的分级承诺；②改为「接口承诺可用 / 生产面见 §12」；③保留但明写"何时撤销、由谁决定" | 裁定记录进本文 §8（U-01） | 裁定记录 + 文本 diff |
| W0-4 | 白皮书的悬空引用改指本文 §4（`WHITEPAPER.md:26`、`:309` 两处） | `grep -rn 'production-readiness-remediation-2026-08-27' .` 归零 | diff |
| W0-5 | 默认开关裁定（**E5b / R8 / G6**）：`cache`/`workflow` 默认 `true` 且曾属未整改面；承诺面 `leader_election`/`event_notification` 反而默认 `false`。裁定"启用即可用" vs "默认对外"，并落地到 `coord-agent/src/service.rs:160-182` | `cargo test -p coord-agent --lib service` + 逐行对齐表 | 裁定表 + 代码 diff |
| W0-6 | 未决项裁定：U1 / U3 / U4 / U5 / U6 / U8 / U9（U5「是否存在未知外部 `coord.agent.*` 消费者」直接决定 R1 是否翻转） | 每条给出裁定人与日期 | 本文 §8 U 表回填 |

### W1 代码缺陷闭环（13 人日）

| # | 任务 | 做法 | 判据 / 产物 | 人日 |
|:--|:--|:--|:--|--:|
| W1-1 | **F-27**：lease 过期 revoke 失败可重试 | 把 `start_lease_expiry_worker` 改为「**提交成功才从本地管理器移除**」，失败项进 `pending` 集合按 tick 重试（`start_region_lease_revoker` 已有 `BTreeSet` 待办模式可参照，`coord-server/src/server/mod.rs:812+`） | 新增单测：注入 `client_write` 失败 ⇒ 下一 tick 必须重试；lab 复跑 `make -C jepsen/lab test WORKLOAD=lease NEMESIS=partition-halves TIME_LIMIT=45 CONCURRENCY=1n` **由红转绿**（当前 `matrix-m2` 7/8） | 2 |
| W1-2 | **F-05**：登录限流 × 重启窗口 | 定位 `coord-findings.md:216` 的形态 | ~~60s 短跑 0 条 `:no-client Failed to authenticate`~~ → **判据已修正（2026-09-21）**：「**quorum 丢失**期间的登录失败是**固有**的（`Authenticate` 要两次 raft 提案），不能靠改断言消除」⇒ 可执行判据改为两条：①**客户端可见的两类失败必须可区分**（密码错=`UNAUTHENTICATED`；无 quorum=`UNAVAILABLE`/`DEADLINE_EXCEEDED`）②lab 复跑统计出现率并**归因**（不得计入一致性违反） | 3 |
| W1-3 | **F-68**：checker 纳入 `start_offset` | 判据必须读 Poll 请求的 `start_offset` | `CONCURRENCY=1n` 复跑转绿，且单客户端仍绿（**两份证据一起引用**，`evidence/README.md:113-116`） | 1 |
| W1-4 | 无界增长四项（第四轮 §6.2 第 17–20 项） | `MemoryWorkflowStore` + 5s 全量扫描（agent 侧最重增长）、动态 region 清理（或**显式声明不支持**）、`snapshot_logs_since_last = 0` 语义、连接数上限与并发保护 | 每项二选一：修 + 测试，或写进契约/文档的"不承诺"清单（§10 规则 6 口径） | 4 |
| W1-5 | 未接线机制逐条接上或删除（第四轮 §6.2 第 21 项） | `dead_tasks()` 已有消费者与告警（`remaining-known-gaps.md:92`），复核其余；监督覆盖率 81 处 `tokio::spawn` vs 5 处 `spawn_supervised` | 逐条交代表 + `grep -c tokio::spawn` 对比 | 2 |
| W1-6 | watch scope 过度限制（功能限制，非安全问题） | `remaining-known-gaps.md:30-58`：scope 受限角色**完全无法订阅**。修法：在 handler 侧评估首个解码的 `WatchCreateRequest`（`extract_scope_access` 已建模但未接线） | 正/反双测：受限角色订阅**范围内**前缀 ⇒ Allow；范围外 ⇒ Deny。注意该改动**只能放宽**，必须在 RC 冻结前完成 | 3 |
| W1-7 | enum 演进机械保护（C22/D-17） | 给枚举加 `reserved`、取消 `buf.yaml` 枚举 lint 豁免（**源码破坏性**，必须在有外部消费者之前做） | `buf breaking` 绿 + Java SDK 重新生成并 `mvn verify` | 2 |

> **W1-6 是唯一"只能在冻结前做"的放宽类改动**：它触碰刚出过 P0 的同一条 auth 路径
> （`remaining-known-gaps.md:55-58` 的教训），因此**必须**放在 RC 冻结 ≥2 周之前，
> 且必须带正反双测。

### W2 门禁可信化（10 人日）——**本工作流不完成，后续所有「绿」不计入判据**

| # | 任务 | 判据 | 人日 |
|:--|:--|:--|--:|
| W2-1 | `weekly perf baseline` **13/13 常驻红**定位（`ci.yml:448` 仅 schedule 触发） | 要么修到绿，要么按 `coord-ui` 覆盖率先例改成 **ratchet** 并写明"这不是目标值"（`remaining-known-gaps.md:241-257` 的先例） | 3 |
| W2-2 | `cargo audit + deny` 定时红定位（HEAD 同 SHA：push 绿 / 定时红） | 取一次 job 日志，判定属于：advisory-db 拉取失败 / 权限 / 新通告三类之一，并给出对应处置 | 2 |
| W2-3 | `real-process chaos` 间歇红（14/31）稳定化 | 产出"红的判据是产品缺陷还是假红"的**判定流程**（孤儿进程清理已做，但 14/31 的残余分布未分析） | 3 |
| W2-4 | 分支保护（B5⑧，仓库设置项，提交无法保证） | 设置后验证：向 `main` 直接 push 被拒；无该设置则"45 次全红 CI"可重演 **（✅ 2026-09-26 已执行，证据见 §11 第十四轮与 `ops/ci-gate-forensics-2026-09-22.md` §4）** | 0.5 |
| W2-5 | 九道门的**负控制演练** | 对每道门各注入一次违规并确认置红（沿用 `ci.yml` 的 gate self-check 模式），留档 | 1.5 |

### W3 长跑与故障注入证据（20 人日 + 墙钟）

| # | 任务 | 判据 / 产物 | 人日 |
|:--|:--|:--|--:|
| W3-1 | M2 收口 T2.3（2h watch+lease 混合浸泡） | `make soak*` 归档 | 2 |
| W3-2 | M3 运维面：membership / compact / 网络增强 / 时钟域 / 磁盘满 | 8h soak + 各 cell 绿（`dev.md:458`） | 5 |
| W3-3 | M4 Multi-Raft：PD split / 跨 region / region compact | 对应 cell 绿（`dev.md:320`） | 2.5 |
| W3-4 | **M5 agent 面：idgen、registry、event、RBAC 安全面**（idgen/registry 是 **10-31 硬期限，当前零产物**） | 每个面一份归档；安全面三条：断连降级 / CCT 失效回退 / 网关拒绝**是否真阻断** | 5 |
| W3-5 | **多 agent 拓扑**（F-58） | 给出 agent 间可达的拓扑，使 **cache ISR 复制面与 mq 跨 agent 投递可测**；否则 cache/mq 的复制语义**永远无法验收** | 3 |
| W3-6 | T6.1 72h soak-full | 72h 归档 + 分面统计（每面样本门槛达标） | 1.5 + 3d 墙钟 |
| W3-7 | ≥14 天连续运行 | 曲线：内存 / 磁盘 / `keep_alive` / 重启次数（第四轮 §6.3.4 的四条曲线） | 1 + 14d 墙钟 |
| W3-8 | §5.4 参数确认 ③ 由**引入方团队**签回 | `bash jepsen/scripts/backfill-param-confirmation.sh --check` 台账 `待签 0` | 0.5（协调） |
| W3-9 | 所有验收 run 在 **clean tree** 上跑 | MANIFEST 的 worktree 字段为 `CLEAN`（消掉 28 份 DIRTY 的复现折扣） | 0.5 |

### W4 安全与合规（6 人日；U-14 后 = 原 11 − 审计整改 5；外部交付期已移出）

| # | 任务 | 判据 | 人日 |
|:--|:--|:--|--:|
| W4-1 | **TLS fail-closed** | `README.md:61` 自述的三种放行形态逐条关闭：`dev` 模式 / 鉴权开启 + `raft_shared_secret` / 非 loopback 且无 mTLS。判据：负控制测试（缺 CA ⇒ 拒绝启动，不静默降级明文）**（✅ 2026-09-26 已落地：`coord/tests/plaintext_remote_failclosed_test.rs` 4 条正反双向；见 §11 第十四轮）** | 3 |
| W4-2 | **U9 KEK 供给** | 三选一：外部 KMS / 启动注入密钥材料 / 显式接受并写入边界（`WHITEPAPER.md:520` 已披露"拿到配置即可推导 KEK"）。**L2 前必须裁定**，否则"加密"承诺有名无实。**✅ 2026-09-21 已裁定：取「启动注入 + 显式边界」** | 3 |
| **W4-2a** | **U-04 的实施**（裁定 ≠ 落地）：①KEK 不再由配置串**确定性派生**，改为启动时从环境/文件注入；②**缺材料即拒绝启动**（fail-closed，不许静默降级）；③负控制测试（无材料 ⇒ 拒绝启动；材料为空 ⇒ 拒绝启动）；④`WHITEPAPER.md` §12.7 / `security.md` 口径同步为"启动注入（非外部 KMS）" | 判据：`cargo test` 的三条负控制 + 启动日志里不再出现派生 KEK 的路径 | 3 |
| ~~W4-3~~ | **已移除（U-14，2026-09-25：无经费）**：不采买第三方审计 ⇒ 不再是 P6 判据、不再是 §9 No-Go 项。替代物 = ①「未经独立审计」边界声明（P6 判据）②内部安全评估**仅作整改输入**（明确非独立、不当作门禁证据；该本地文件未入库，如需引用先迁入版本化路径） | 边界声明 diff + 整改输入清单 | 0（原 5 人日整改 + 3–6 周交付期一并移出） |
| W4-4 | 依赖治理 | `deny.toml` 的 bincode 豁免是**唯一**例外，需给出替代路径（bincode 是持久化格式：快照/Raft 日志/`/_sys/auth/`/PD 元数据/对象存储 manifest） | 2 |
| W4-5 | Gate 0 四条回归 + RSS 峰值实测口径 | 越权 / DoS（含 RSS 断言）/ 空密钥启动失败 / 非 loopback raft 无密钥启动失败 | 2 |
| W4-6 | `SECURITY.md` 支持版本矩阵更新（当前只列 `0.1.x`，代码已是 0.2.0） | 版本表与 `Cargo.toml:15` 一致 | 0.5 |

### W5 可观测性与 SLO（12 人日）

| # | 任务 | 判据 | 人日 |
|:--|:--|:--|--:|
| W5-1 | 指标面复核 | `/health?verbose=true`、`coord_dead_background_tasks`、`metrics.method_metrics`（`MAX_METHOD_METRICS` 有界）逐项可用且有消费者 | 2 |
| W5-2 | **SLO 定义** | 可用率 / 时延 / 错误率三项，且与 §5.4-③ 的 quiet 可用率 0.95 口径**对齐**（否则引入评审无法对照） | 3 |
| W5-3 | 告警 → runbook 绑定 | `monitoring/prometheus-rules.yml` 每条告警有 `runbook_url` | 2 |
| W5-4 | "能力死亡必须可观测"（第四轮 §6.3.6） | §3.13 列的监督/清理机制**至少接上出口并配告警**——否则任何能力静默死亡时没有任何路径会告诉你 | 3 |
| W5-5 | 7×24 内存/磁盘曲线 | 四条曲线可解释且有界（与 W3-7 共用数据） | 2 |

### W6 运维与升级（11 人日）

| # | 任务 | 判据 / 产物 | 人日 |
|:--|:--|:--|--:|
| W6-1 | **runbook**（当前全仓不存在） | 启动 / 停止 / 缩扩容 / 成员变更 / 备份恢复 / 密钥轮换 / Seal-Unseal / compaction，每条含命令与预期输出 | 4 |
| W6-2 | k8s 部署验证 | `deploy/k8s/statefulset.yaml` 端到端验证（当前无任何验证证据）：就绪探针 / 持久卷 / 反亲和 / 优雅下线 | 2 |
| W6-3 | 滚动升级 + 版本兼容矩阵（N-1） | 明确当前延后的 B7（滚动升级）要么做、要么写进"不支持"清单 | 3 |
| W6-4 | 备份恢复演练 | 含对象存储的**已知边界**：快照恢复落后节点本地 chunk 清空 rebuild（Get 返回 UNAVAILABLE）、全集群配置须一致 | 2 |

### W7 交付工程（7 人日）

| # | 任务 | 判据 | 人日 |
|:--|:--|:--|--:|
| W7-1 | 版本与 tag（v0.2.0 未 tag；`Cargo.toml:15`=0.2.0、SDK 0.2.0） | tag 后 **clean tree 重跑并归档**（`remaining-known-gaps.md:144-152` 的正解） | 2 |
| W7-2 | 制品 + 校验和 + SBOM | 制品可复现构建；校验和与证据归档同规范 | 2 |
| W7-3 | `release.yml` 复核 | 历史失败点（缺 checkout，`ad22337`）已修，需一次真发布验证 | 1 |
| W7-4 | SDK 发布裁定（U3/R7） | 不发布 ⇒ 不阻塞；发布 ⇒ 走 Maven 产物与版本流程 | 1 |
| W7-5 | 消费者告知（§11.2.1） | v1.2.0 已走过一次，后续按流程 | 1 |

### W8 业务落地准备（5 人日）

| # | 任务 | 判据 | 人日 |
|:--|:--|:--|--:|
| W8-1 | 接入文档 + SDK 示例 | SDK 是**唯一支持面**；Spring 接入提供 `@Bean(destroyMethod = "close")` recipe（**不恢复 starter**，`remaining-known-gaps.md:99`/`:104-114` 是产品决策） | 2 |
| W8-2 | 试点场景与回退预案 | 回退路径可执行 + 数据可逆性说明 | 2 |
| W8-3 | 接入方验收清单 | 把第四轮 §6.3 六条改写为当前 17 项现实（其中"config 不是契约能力"已被 `coord.config.v1` 取代） | 1 |

### W9 治理（2 人日）

缺陷 SLA（`jepsen/docs/dev.md:841`）+ 门禁维护责任人 + 承诺变更流程（`WHITEPAPER.md:451` §11）演练一次。

---

## §6 里程碑与排期（1 人基线 + 案 A 并行版）

### 6.1 里程碑

| 里程碑 | 日期 | 出口判据 | 证据 |
|:--|:--|:--|:--|
| **M0 文本与门禁先立** | 09-30 | W0 全部完成（对外口径一致）+ W2 完成（门禁可信）——**W2-1…W2-4 ✅（第十三/十四轮收尾）；W2-5 余 P3–P5（lab 组）与 P8 真发布面（与 W3/W7 同期回填）** | 四份 diff + 门禁负控制演练记录（`gate-drills-2026-09-21.md` §4） |
| **M1 代码面清账** | 10-24 | W1 全部完成；`matrix-m2` 由红转绿；F-27/F-05/F-68 闭环 | CI run + lab 归档 |
| **M2 安全裁定** | 10-31 | W4-1/W4-2/W4-4 完成；**U-14 边界声明已落地**；批次 1（registry/idgen）**契约期限到点** | 裁定记录 + 边界声明 diff |
| **M3 覆盖补齐** | 11-21 | W3-1…W3-5 完成（含 idgen/registry/多 agent 拓扑）；批次 2（lock/election）期限到点 | 各面归档 |
| **M4 RC 冻结** | **12-10** | P-Gate 1–4、7、8、9 绿；代码冻结 | 冻结记录 |
| **M5 长跑** | 12-10 → 12-25 | 72h soak（12-10→12-13）+ 14 天运行（12-11→12-25，**需第二套 lab**） | 两份归档 + 曲线 |
| **M6 判定** | 12-26 → 12-31 | P-Gate 5、6 收口；对外**L2 声明**或按 §1.3 案 B 降级声明 | 本文 §9 的 Go/No-Go 记录（含 U-14 边界） |

### 6.2 硬约束倒排（**比人日更硬**）

```
09-25  U-14：第三方审计移出门槛 ⇒ 关键路径不再有外部交付期（§8.7）
10-31  批次 1 契约期限（registry/idgen）—— 需已在 M3 前有 lab 产物
11-30  批次 2 契约期限（lock/election）
12-10  RC 冻结 ──┬─ 72h soak  12-10 → 12-13   （docker lab）
                 └─ 14 天运行 12-11 → 12-25   （**必须另一套环境**，否则与 72h 抢 lab）
12-25  长跑结束 ⇒ 12-26 起收口；若期间有任何 P0 修复 ⇒ **判据作废重跑** ⇒ 判定顺延至 2027-01
12-31  对外 L2 声明（或按案 B 声明"除长跑外达标"）
```

> **1 人配置下的最大风险**：14 天运行与 72h soak 必须在**两套 lab**上并行，
> 而 1 个人同时值守两套长跑 + 收口文档，任何一次机器故障都会直接吃掉判定日。
> 这就是 §1.3 推荐"案 A（加 1 人）"的核心原因——**不是工作量问题，是关键路径没有冗余**。

---

## §7 验收与证据规范（沿用既有规范，不得放松）

1. **一次真实运行 = 一份产物目录**：`docs/production/evidence/<UTC 时间戳>-<场景>/`，
   含 `MANIFEST.md`（谁跑的 / commit / 命令 / 环境 / 结论）、完整 `run.log`、`sha256sums.txt`
   （`docs/production/evidence/README.md:9-16`）。
2. **run 必须在 clean tree 与已 tag 的 commit 上跑**：当前 28 份归档的 worktree 字段
   一律 `DIRTY`，"run 跑在哪个 commit 上"不可追溯（`soak-closure-report.md:34-38`）。
3. **每个判据必须有负控制**：坏历史 fixture / 注入违规，且**正反两向都验**
   （既有先例：`check-agent-wire.clj`、`watch_scope_is_fail_closed_not_bypassed`、
   `test_agent_uses_config_data_dir_unless_flag_is_explicit`）。
4. **不得弱化断言**：跑出产品缺陷时测试保持红 + 立项，禁止改断言迁就实现
   （`jepsen/docs/dev.md:214`）。
5. **自述式缺陷专项检查**：模块头/文档宣称某机制存在而实现未接线，属**独立缺陷类**
   （`agent-ga-remediation-baseline.md:134` §9.4），GA 验收须含「自述式 no-op grep」。

---

## §8 风险与未决项

### 8.1 风险

| # | 风险 | 影响 | 缓解 |
|:--|:--|:--|:--|
| **R-01** | **1 人 + 12-31 + 全量 ⇒ 关键路径无冗余**（§1.3 缺口 23 人日） | 判定日必然顺延 | 案 A 加 1 人；否则先按案 B 声明并公示 |
| **R-02** | ~~第三方审计采买 + 整改 3–6 周~~ **已由 U-14 处置（2026-09-25：不采买）** | 残余风险 = 无独立安全保证；任何"已审计"暗示 = No-Go（§9-4） | 边界声明落地（P6 判据）；内部评估（非独立、未入库）仅作整改输入 |
| **R-03** | 14 天运行期间出现 P0 修复 ⇒ 判据作废重跑 | 判定跨年 | RC 冻结前把 W1 全部清完；冻结后只接受"阻断级"修复并**公开记录重跑** |
| **R-04** | F-58 多 agent 拓扑若在 lab 上做不出来 | cache ISR / mq 跨 agent 投递**永远无法验收** ⇒ 只能降级为"边界声明" | M3 前 spike 一次拓扑可行性；做不出则按 §10 规则 6 声明并**移出 L2 承诺面** |
| **R-05** | 一次性包名改名（v1.2.0）打断未知外部消费者 | 存量中断 | **U5 必须在 W0-6 裁定**；未知消费者 ⇒ 回退双服务期（计划 §8 R1 的唯一翻转点） |
| **R-06** | enum 演进保护是**源码破坏性**改动 | Java SDK 需同步 | 必须在有外部消费者之前做（W1-7），拖到 L2 之后成本翻倍 |
| **R-07** | 门禁"常驻红"仍在（perf 13/13）**→ 2026-09-25 已解除**（run `36086768094` schedule 首次通过） | 门禁不可信 ⇒ 所有"绿"打折 | 已按 W2 先于一切处置；残余观察面 = chaos 间歇红（W2-3，n=9 仍不宣布稳定） |
| **R-08** | 证据"参数未满签"（③） | 依赖 ③ 的门禁结论**不得用于引入评审** | W3-8 引入方签回，或把 ③ 相关判据从 L2 判据中移除 |

### 8.2 未决项（须人工裁定；本计划不替你决定）

| # | 未决项 | 需谁定 | 截止 | 本计划中的位置 |
|:--|:--|:--|:--|:--|
| ~~**U-01**~~ | ~~`README.md:17` 免责声明的处置（撤 / 限定 / 保留并定期限）~~ **✅ 已裁定 2026-09-21：取②「限定」** | 决策方 | ✅ M0 前 | W0-3（**已完成**） |
| ~~**U-02**~~ | ~~人力案 A/B/C（§1.3）~~ **✅ 已裁定 2026-09-21：案 A（加 1 人）** | 决策方 | ✅ 本周 | §1.3 |
| ~~**U-03**~~ | ~~默认开关裁定：`cache`/`workflow` 默认开 vs 承诺面默认关（E5b/R8/G6）~~ **✅ 已裁定 2026-09-21：默认关，显式启用即可用** | 架构 | ✅ M0 前 | W0-5（**已完成**） |
| ~~**U-04**~~ | ~~KEK 供给：外部 KMS / 启动注入 / 显式接受（U9）~~ **✅ 已裁定 2026-09-21：取②启动注入 + ③显式边界** | 安全 + 架构 | ✅ M2 前 | W4-2（**裁定完成，实施见 W4-2a**） |
| ~~**U-05**~~ | ~~是否存在未知外部 `coord.agent.*` 消费者~~ **✅ 已裁定 2026-09-21：无已知外部消费者** | 交付 | ✅ M0 | W0-6（**已完成**） |
| ~~**U-06**~~ | ~~Java SDK 是否发布到仓库（U3/R7）~~ **✅ 已裁定 2026-09-21：不发布** | 发布 | ✅ M2 前 | W7-4（**已完成**） |
| ~~**U-07**~~ | ~~`replication` 是否需要对外契约（U8，涉及红线 R3）~~ **✅ 已裁定 2026-09-21：不建对外契约** | 架构 | ✅ M0 | W0-6（**已完成**） |
| ~~**U-08**~~ | ~~`policy` RBAC 是否持久化（U4）~~ **✅ 已裁定 2026-09-21：不持久化（Agent 本地）** | 架构 | ✅ M3 前 | W1-6（**已完成**） |
| ~~**U-09**~~ | ~~U1（cb/rl 本地边界的 §10 规则 4 正式确认）/ U6（白皮书与台账行数口径）~~ **✅ 已裁定 2026-09-21** | 架构 / 契约 | ✅ M0 | W0-6（**已完成**） |
| ~~**U-10**~~ | ~~承诺对象：外部客户 or 内部业务团队~~ **✅ 已裁定 2026-09-21：内部业务团队** | 决策方 | ✅ M0 | §4.3 + `docs/production/adopter-playbook.md` |

### 8.3 裁定记录（2026-09-21 回填）

| # | 裁定内容 | 裁定人 | 日期 | 落地证据（可重跑） |
|:--|:--|:--|:--|:--|
| **U-01** | **取②「限定」**：两处 README 的「not intended for production use」改为「**接口承诺（L0）可用**；生产面见 `WHITEPAPER.md` §12 与本文 §4」，并写明裁定日期与「本条取代原措辞」 | 决策方（本次会话） | 2026-09-21 | `README.md:17`、`README.zh-CN.md:17` 的 diff；`grep -n 'not intended for production use' README*.md` 归零 |
| **U-02** | **案 A**：加 1 人（第二人专责 W3 长跑值守 + W5/W6 运维面），12-31 冲 §4 全量 9 门 | 决策方 | 2026-09-21 | 本文 §1.3 表（A 行）；⚠️ 人力属**组织事实**，无法由仓内产物证明 ⇒ 记入 §9 的 Go/No-Go 前检查项 |
| **U-03** | **默认关，显式启用即可用**（承诺面与未整改面一致）：`cache` / `workflow` 由默认 `true` → `false`；`transit` 保持 `true`（其整改已闭合）| 架构 | 2026-09-21 | `coord-agent/src/service.rs` 的 `impl Default` + 新增测试；`cargo test -p coord-agent --lib service::tests` ⇒ 5 ✓；`cargo test -p coord-agent --lib` ⇒ 468 ✓ <br>⚠️ **`transit` 那一句已被 U-11（2026-09-22）超越** ⇒ 现为 `false`，见 §8.5 |

> **U-01 的一个附带事实**：原措辞是「本文件**不可入库**」以外的又一个口径冲突源 —— 它与
> `WHITEPAPER.md` §10/§12 的契约承诺、与 §4.3 的 L0 分级**互斥**。裁定②保留了风险提示、
> 又把「接口承诺可用」这一可操作事实写清楚，因此不阻塞 M0。
>
> **U-03 的一个附带发现**：各字段是逐字段 `#[serde(default)]`（缺省 = `false`），
> 所以**配置文件路径**上未列出的服务本来就是关的；旧口径的不一致只存在于
> `ServiceConfig::default()`（代码默认，如 `dev` 模式/不带 `--agent-config`）——
> 本次改动把两条路径拉平，并新增测试 `test_service_config_toml_missing_fields_match_code_defaults`
> 把这个一致性变成机器判据。

### 8.4 第二轮裁定（2026-09-21，U-04…U-10 一次性回填）

> **体例说明**：与 U-01…U-03 相同，裁定人记为「本次会话」= **AI 助理代拟 + 需人工复核**。
> 每条都给**理由**与**可核验的落地物**；凡「裁定完成、实施未完成」的，一律在 §11 台账里
> 标 🟡 并单列实施项，**不得**读作"已落地"。

| # | 裁定 | 理由（可核验） | 落地物 / 后续 |
|:--|:--|:--|:--|
| **U-04** | **KEK 供给取②「启动时注入密钥材料」+ ③「显式接受并写入边界」**（不引入外部 KMS） | ①外部 KMS 是 P1 工作量且引入外部依赖（`deny.toml` 依赖面 + 可用性依赖），在 1 人配置下会挤掉 W3 长跑；②③组合能把"加密"承诺变成**可验证**的事实：进程启动时从环境/文件拿密钥材料，缺失即**拒绝启动**（fail-closed），并保留"非外部 KMS"的边界声明（`WHITEPAPER.md:520` 已披露"拿到配置即可推导 KEK"）。三者里只有②能满足"L2 前必须裁定"且不伪装成 KMS | 裁定记录（本表）；**实施 = W4-2a**（见 §11）：①KEK 不再由配置串确定性派生 ②缺材料拒绝启动 ③负控制测试（无材料 ⇒ 拒绝启动）④文档口径同步 |
| **U-05** | **无已知外部 `coord.agent.*` 消费者** ⇒ 一次性改名（v1.2.0）**可执行**，R1 的"回退双服务期"**不触发** | SDK 未发布（U-06）、承诺对象是内部业务团队（U-10）、仓库内无第三方依赖声明（`grep -rn 'coord.agent' --include=*.toml` 无外部消费方）；且 `Handshake.Negotiate` 现在会**明确**告知"协议版本不支持"而不是静默失败（`coord-agent/src/services/handshake.rs:29`） | 写入 `docs/production/adopter-playbook.md`；**约束**：一旦发布 SDK 或出现外部消费方，本条裁定作废，后续改按 Major 流程 |
| **U-06** | **Java SDK 不发布到制品仓库**（源码随仓库 tag 提供，消费方自行构建） | 无消费方需要它（U-05）；发布要额外承担 Maven 产物、版本流程与制品维护（W7-4 = 1 人日但不是一次性成本）；SDK 的可用性已由 `java-example-it` CI job 每次真实集群验证（`-Pit` 跑集成套件） | `adopter-playbook.md` §1 写明构建方式；CI job 继续作为 SDK 可用性证据 |
| **U-07** | **`replication`（`/coord.agent.Replica/*`）不建对外契约** | 它是 agent↔server 的**内部面**（数据面复制与 ISR 心跳），不是业务能力；红线 R3 明确"不承诺内部面"。当前它已在能力表里登记（`coord-core/src/grpc_auth.rs` 的 `coord:replica:*`），保持"可鉴权但无契约包"的状态是刻意的 | `docs/production/ops/boundaries.md` 已有对应声明；`STATUS.md` 不新增行 |
| **U-08** | **`policy` 的 RBAC 判定不持久化**（Agent 本地、重启丢失）；OPA **bundle** 的持久化是另一件事（已持久化） | RBAC 判定是本地执行面（`coord-agent/src/services/policy.rs`），把它持久化会引入"本地状态与 server 授权模型漂移"的风险；与 §10 规则 4 口径一致 | `boundaries.md` 的 B-PL 条已声明；接入方口径见 `adopter-playbook.md` §3 |
| **U-09** | **cb/rl 的"本地不共享"按 §10 规则 4 正式确认为不承诺**；**白皮书与台账的行数口径以 `STATUS.md` 为单一事实来源** | 前者已写在 `boundaries.md`（B-CX / B-RL 类）；后者是 D-05 的第二个根因（`apis/contracts/README.md` 停在 v1.1.0 的 5 行 vs `STATUS.md` 的 17 行）⇒ W0-2 已把 README 对齐到 17+1 行并写明"以 STATUS.md 为单一事实来源" | `boundaries.md` + `apis/contracts/README.md`（W0-2 的 diff） |
| **U-10** | **承诺对象 = 内部业务团队**（不是外部客户） | SDK 不发布（U-06）+ 无外部消费者（U-05）+ 无第三方审计（U-14：不采买）⇒ 对外部客户做任何 L1/L2 措辞都会隐含 SLA 义务，而 L3 明确不在本计划范围；内部接入的措辞可用"共同承担风险、随时回退"（§4.3 的 L1 定义） | §4.3 文本 + `docs/production/adopter-playbook.md` §0/§6（含"不可以说"清单） |

> **U-04 的一个诚实标注**：本次**只做裁定，没做实施**。因此 P6 的"KEK 供给"判据
> **仍为红**：`WHITEPAPER.md:520` 披露的"拿到配置即可推导 KEK"这一事实在实施前不变。
> 把裁定当落地是本仓库反复出现的失败形态（"自述式 no-op"），故单列 W4-2a。

### 8.5 第三轮裁定与 CI 定位（2026-09-22）

> 本轮做两件事：**把 U-04 从裁定推进到落地**（W4-2a），以及**把 §2.2 的三处「常驻红/
> 间歇红」从现象推进到根因**（W2）。后者的方法见
> `docs/production/ops/ci-gate-forensics-2026-09-22.md`。

| # | 裁定 / 结论 | 理由（可核验） | 落地物 |
|:--|:--|:--|:--|
| **U-11** | **`transit` 默认开关由 `true` 改为 `false`**（**超越 U-03 中"transit 保持 true"那一句**，其余不变） | U-03 给 `transit` 保持 `true` 的唯一理由是"其整改（DEK 持久化）已闭合 ⇒ 启用即可用"。U-04 落地（W4-2a）后**该前提不再成立**：启用 `transit` 必须先注入 32 字节 KEK 材料，缺失即拒绝启动 ⇒ 它已属"未整改面"。按 G9「不得默认开启未整改面」，默认值必须为 `false` | `coord-agent/src/service.rs` 的 `impl Default` + `test_service_config_defaults`（新增负断言）+ `test_service_config_toml_missing_fields_match_code_defaults`（**新增 `transit`/`pki` 两列**——此前这两列根本没被比过） |
| **W2-2 根因（已定位，非猜测）** | `cargo audit + deny` 的定时红**不是依赖问题**，是 `rustsec/audit-check@v2.0.0` 的**事件名分叉**：`schedule` → `reportIssues()`（调 `issues.create`）／其它事件 → `reportCheck()`（调 `checks.create`）。job 只授了 `checks: write` ⇒ 定时跑在 `issues.create` 上 403 `Resource not accessible by integration` ⇒ `setFailed` | 四面证据：①action 源码 `src/main.ts` 末段的 `eventName == 'schedule'` 分支；②check-run 注释里四条定时跑均有 `Resource not accessible by integration - …/rest/issues#create-an-issue`，而同一 SHA 的 push 跑**没有**这条；③四条跑与 push 跑的 `cargo audit` 结果**完全相同**（都是 `1 warnings found!` = 唯一一条 bincode unmaintained，非漏洞）；④本地复跑 `cargo deny check bans licenses sources` ⇒ `bans ok, licenses ok, sources ok` | `ci.yml` 的 `security-audit` job：补 `issues: write` + 把根因写进注释；顺带修正 step 8 名字（它才是**阻断**闸） |
| **W2-2 的第二个收益** | 此前 **step 7 一红，step 8 就 `skipped`** ⇒ `licenses` / `bans` / `sources` 三道**从未有过执行记录** | 四条定时跑的 step 列表里 step 8 恒为 `skipped` | 同上（修好后 step 8 会真的跑） |

### 8.6 第四轮裁定（2026-09-24：U-12 —— D-19 收口）

> 体例同 §8.4/§8.5：裁定人记为「本次会话」= **AI 助理代拟 + 需人工复核**；
> 每条给理由与可核验落地物；「裁定完成、实施未完成」的条目一律单列实施项。

| # | 裁定 | 理由（可核验） | 落地物 |
|:--|:--|:--|:--|
| **U-12** | **D-19 取②**：`registry` / `config_center` / `lock` / `idgen` / `policy` / `pki` 六项的**代码默认 `true` → `false`**；两条路径拉平为「全部默认关」（TOML 缺省本来就是 `false`，不动）。 | ①U-03 已立原则「默认关，显式启用即可用」，六项是该原则的遗留例外；②G9 的判据是「与整改状态一致」：六个面的 **G8（soak 覆盖）都没有产物** ⇒ 同属未整改面，默认开违反 G9；③`impl Default` 的注释**自己**写着"不再有『只写代码就默认开着』的口子"，而代码里仍有六个 `true`（自述式不一致，与 §7-5 同族）；④若取①（TOML 侧改 `default_true`）等于把六个未验证面默认对外，与 D-14/W0-5 的方向相反。 | `coord-agent/src/service.rs`：六项 `false` + 注释引 U-12；`test_service_config_defaults` 改为反向断言；`test_service_config_toml_missing_fields_match_code_defaults` **扩到全字段逐字段等价**（D-19 的验收判据）；旧「钉住不一致」测试删除；`coord/tests/dev_mode_test.rs` 的 4 条依赖旧默认的用例按「显式启用即可用」显式启用 `services.idgen = true`。判据：`cargo test -p coord-agent --lib` ⇒ **487 ✓**；`cargo test -p coord-agent --tests` ⇒ 全 ok；`cargo test -p coord --test dev_mode_test` ⇒ **7 ✓** |

---

### 8.7 定义级裁定（2026-09-25：U-14 —— 移除第三方审计门禁）

> 体例同前（§8.4–§8.6）：裁定人记为「决策方（本次会话）+ 需人工复核」。
> 本条是**定义级变更**（修订 §4 的 P6 与 §9 一条 No-Go），按文末约定记录裁定人与日期。

| # | 裁定 | 理由（可核验） | 落地物 |
|:--|:--|:--|:--|
| **U-14** | **P-Gate 6 不再要求「第三方安全审计报告」**（无经费采买/执行独立审计 ⇒ 不做）。替代物 = 三件可执行的事：①P6 其余判据照旧必须全绿（TLS fail-closed / KEK / `cargo deny`）；②「**未经独立第三方安全审计**」边界声明落地到 §4.3、接入手册与白皮书 §12 口径（**反向判据**：任何文本出现"经审计/独立审计"暗示即置红，见 §9-4）；③内部安全评估（`docs/audit-2026-09-25.md`，其自述明确**非独立、未入库**）**仅作整改输入**，不得当作门禁证据 | ①决策方 2026-09-25 明确"无经费进行独立（审计）"；②承诺对象是内部业务团队（U-10）⇒ 不需要对外审计背书；③W4-3 是原计划**最长的外部依赖**（采买+执行+整改 3–6 周；10 月中不起步必然跨年）⇒ 移出后关键路径只剩工程进度与墙钟 | §4.1 P6、§4.3 纪律、§9 第 4 条、§1.3/§1.4、§5 W4-3、§6 里程碑、§8.1 R-02、§10、附录 A；`adopter-playbook.md` §4/§6、`ops/security.md` §2.3、`ops/governance.md` §5、`WHITEPAPER.md` §12 第 2 条、`gate-drills-2026-09-21.md` §4.4 注记 |

> **本裁定的性质（如实记录）**：它**降低**了 2026-09-21 版定义中 P6 的门槛 —— 这是决策方在
> "无经费"约束下的显式取舍，不是"把现状重新命名为生产可用"。§1.3 的"不建议 D（降低判据）"
> 对**其他任何门**仍然成立：本裁定**不允许**被当作移除 P1–P5、P7–P9 任一条判据的先例。
>
> **对"尽快生产可用"的口径**：外审移出后，**判定不再等任何外部交付期**；12-31 的可达性
> 只取决于 W1–W3 的工程进度（§1.3 的缺口 18–23 人日仍存在，案 A/B 取舍不变）。
>
> **残余风险（必须随声明一起出现）**：无独立第三方保证 ⇒ 所有声明使用「未经独立审计」
> 边界；内部安全评估不是审计报告，不得对外出示。

---

## §9 生产上线 Go / No-Go（一票否决）

以下任一条成立 ⇒ **No-Go**，不得对外声明 L2：

1. **P-Gate 1–9 任一红**。
2. **F-27 未闭环**——契约承诺了 lease 级联删除而实现留了永久泄漏口，且 lock/lease 在承诺面内。
3. **存在"常驻红"门禁**（判据保留为常设规则）—— 2026-09-21 时的实例是 `weekly perf baseline`
   13/13，已于 2026-09-25 定时跑转绿（run `36086768094`）；**任何时候出现新的常驻红门禁即触发本条**。
4. **任何对外/对内部表述暗示「经第三方审计 / 独立审计」**（U-14 的反向判据：审计已按 2026-09-25
   裁定移出门槛 ⇒ 不得因此暗示"已审"；替代义务 = 明示「未经独立第三方安全审计」）。
5. **免责声明与承诺文本并存**（`README.md:17` 未处置）——两句话同时公开等于自相矛盾。
6. **U-05（未知外部消费者）未裁定却已执行一次性改名**（计划 §8 R1 的翻转点）。
7. **长跑期间发生 P0 修复但未重跑**，或 14 天曲线出现不可解释的增长。
8. **证据不满签**（§5.4-③）却把依赖它的门禁结论用于引入评审。

---

## §10 业务落地路径（L2 之后）

| 阶段 | 准入 | 范围 | 观测与回退 |
|:--|:--|:--|:--|
| **试点** | L1：L0 + 该能力 G1–G7 绿 + 接入方书面接受边界 | 1 个业务、**非关键路径**、单机房 | 观测：错误率 / 时延 / 锁与租约的可观测异常；回退：接入方在 1 小时内可切回原方案 |
| **灰度** | L2 达成（§9 全绿） | 3–5 个业务、含 1 条准关键路径 | 观测：SLO 达成情况 + 告警量；回退：按业务逐个回退，数据可逆性已文档化 |
| **全量** | 灰度 ≥4 周无 P0/P1 + SLO 达标 + runbook 演练完毕 | 全部目标业务 | 进入 L3（SLA）另行立文 |

**接入方必须知道的三件事**（写进入接文档，避免第四轮指出的"高置信度的错误安心感"）：
1. 客户端只依赖 gRPC Status Code（`WHITEPAPER.md:304` §6.3），不解析内部错误串。
2. 能力边界（cache 不承诺跨节点原子、分区 Leader 无自动故障转移、MQ `Subscribe` 是 best-effort、
   cb/rl 本地不共享、policy RBAC 为 Agent 本地、transit 的 KEK 非外部 KMS、
   **平台未经独立第三方安全审计**（U-14））——**逐条列出**。
3. 每个能力的**不承诺**与**承诺**同等重要（`WHITEPAPER.md:42` §10 规则 4）。

---

## §11 执行台账（进度）

> 体例：每行必须有**可重跑的判据**与**产物落点**。**「完成」不等于「已验收」** ——
> 凡依赖 lab / 外部（引入方签字）的判据一律标注**待验收**，不得当作已绿；
> **U-14 后**「审计」不再是外部依赖（见 §8.7）。
> 最后更新：**2026-09-26（第十四轮：W2-4 分支保护落地 + W4-1 TLS fail-closed 落地）**（基线 `8b65290` + 前十三轮改动 + 本轮改动）。
> 第八轮主题见下方「第八轮追加」；CI 侧取证方法见 `docs/production/ops/ci-gate-forensics-2026-09-22.md` §6。

| # | 任务 | 状态 | 判据（已跑的命令） | 结果 / 产物 |
|:--|:--|:--|:--|:--|
| **W0-1** | 两份 README 的 Stability labels 与台账对齐 | ✅ 完成 | 人工逐行对照 `STATUS.md`；`grep -n 'EXPERIMENTAL' README.md` | EN 由「3 项 COMMITTED + 4 项 EXPERIMENTAL（Workflow 为内存占位）」改为「16 个 `coord.<domain>.v1` + `coord.storage` 全 `COMMITTED`；EXPERIMENTAL 区已于 v1.2.0 清空」；zh-CN 补齐同一段。**顺带修掉两份 README 的过期论断**："artifacts are not yet committed" / "尚无产物入仓"（实为 28 份已入仓）与 26 → **28** 计数 |
| **W0-2** | `apis/contracts/README.md` 升到 v1.2.0 | ✅ 完成 | `bash apis/contracts/scripts/check-wire-sync.sh` ⇒ exit 0 | 版本号 v1.1.0→v1.2.0；承诺表 5 行 → **17 行**（期限与 `STATUS.md` 逐行一致）；EXPERIMENTAL 区改写为「已清空」；目录树按 `find` 实测补齐 17 个 domain + storage + 三支卡口脚本 |
| **W0-3** | 免责声明裁定（U-01） | ✅ 完成（取②） | 两份 README diff；`grep -n 'not intended for production use' README*.md` 归零 | 改为「**接口承诺（L0）可用**；生产面见 `WHITEPAPER.md` §12 与本文 §4」，并写明裁定日期与「本条取代原措辞」 |
| **W0-4** | 白皮书悬空引用改指本文 §4 | ✅ 完成 | `grep -rn 'production-readiness-remediation-2026-08-27' .` | 两处**引用性**提及已改指本文 §4；全仓剩余 4 处为**取证/裁定记录**（本文 §0、D-06、W0-4 行 + 白皮书里「该文件不存在」的说明），非悬空引用 |
| **W0-5** | 默认开关裁定（U-03）落地 | ✅ 完成 | `cargo test -p coord-agent --lib service::tests` ⇒ **5 ✓**；`cargo test -p coord-agent --lib` ⇒ **468 ✓** | `service.rs` 的 `impl Default`：`cache`/`workflow` 由 `true` → `false`（`transit` 保持 `true`）；新增 `test_service_config_toml_missing_fields_match_code_defaults` 把「缺省字段 ≡ 代码默认」变成机器判据 |
| **W0-6** | 未决项回填 | 🟡 部分 | 见 §8.3 | U-01 / U-02 / U-03 已裁定并回填；U-04…U-10 仍待裁定 |
| **W1-1** | **F-27**：lease 过期 revoke 不得静默丢失 | ✅ 代码完成 / 🔴 **lab 复跑仍红（形态已变）** | `cargo test -p coord-server --lib f27` ⇒ **5 ✓**；`cargo test -p coord-server --test lease_raft_test --test region_lease_test` ⇒ 3 ✓ / 3 ✓；lab 复跑（见右） | `LeaseManager`：过期记录**保留至 revoke 确认提交**（`finish_expired`），重试轮次继续上报且指标**只结算一次**；`keep_alive`/`attach_key`/`detach_key`/`get_lease`/计数把「待提交」记录视为**不存在**（fail-closed）。worker：`pending` 集合 + 可注入的 `advance_pending_revokes` 重试轮 + 告警节流（5s）。<br>**lab 复跑**（`store/coord/2026-09-21T14:53:38.329176044Z/`，binary `7c4b3222`）：`:grants 94 / :expiries 40 / :violations-by-class {:lease-not-expired 4}` ⇒ **仍 invalid**，但①静默丢失通道已闭环（n1 失败后重试成功、pending 不再增长可观测）；②剩余 4 条经日志归因为**分区时长 > 活性窗口（ttl+grace=6s，实测分区 5.2–7.6s）**⇒ 属 §5.4-⑤ `lease grace` 的**参数裁定项**，详见 `jepsen/docs/coord-findings.md` §F-27「修复与复跑」 |
| **W1-3** | **F-68**：checker 纳入消费者身份 + `start_offset` | ✅ **完成（fixture 级 + lab 复跑两份均绿）** | 控制机内 `run-checker-tests.clj jepsen.coord.mqck scripts/mq-fixtures`；`make -C jepsen/lab test WORKLOAD=mq NEMESIS=none TIME_LIMIT=60 AGENTS=1 CONCURRENCY=1n`（与 `CONCURRENCY=1` 成对） | 判据 5 改为「同一 process 已 Ack **且** `start_offset > offset`」；新增两份守卫 fixture。**修前 10/12（恰好两份新 fixture 红）→ 修后 12/12**，负控制两轮都红。**lab**：`CONCURRENCY=1n` 由修前 `:mq-redelivered-after-ack 461` → **`:violations-by-class {}`（绿）**，`CONCURRENCY=1` 仍绿。归档：`docs/production/evidence/20260921T150133Z-m5b-mq-poll-ack-multi-client-f68-fixed/`、`…150135Z-…-single-client-f68-fixed/`（两份 MANIFEST 均为 **DIRTY**，clean 复跑归 W3-9） |
| **P1 卡口复核** | 本次改动后的门禁自查 | ✅ 完成 | `cargo fmt --all -- --check`；`cargo clippy --workspace -- -D warnings`；五道契约/卡口脚本 | fmt ✓；clippy（**非测试目标**）✓；wire-sync / wire-descriptor / sdk-sync / panics / error-code 五道全 `exit 0` |
| **W2（观察）** | 门禁可信度的两条新证据 | 🟡 记录 | `cargo test -p coord-server --lib`；`cargo clippy --workspace --all-targets -- -D warnings` | ① `storage::object_store::tests::test_chunk_auto_rotate_by_age` 全量跑时红一次、单独跑绿 ⇒ 疑时间边界抖动（与 lease 改动无关）；② `--all-targets` 的 clippy 在**基线**上就有 **23 处**错误（全在 test 目标）⇒ 与 §4 P1「非测试目标」的措辞一致，但这是 W2-1/W2-3 「假红 vs 真缺陷」流水账的第一条 |

**本次会话未触碰**（仍按 §5/§6 排期执行）：W1-2（F-05 的 lab 复跑）、W1-6、W1-7、W2（除本轮新增项）、W3、W4-1/W4-2/W4-3/W4-5、W5-1/W5-4/W5-5、W6-3/W6-4、W7、W8。

### 第二轮追加（2026-09-21，同一会话续）

| # | 任务 | 状态 | 判据（已跑的命令） | 结果 / 产物 |
|:--|:--|:--|:--|:--|
| **W1-4(a)** | 无界增长：`MemoryWorkflowStore` + **5s 全量扫描** | ✅ 代码完成 | `cargo test -p coord-core --lib workflow::runtime` ⇒ **26 ✓**（含新增负控制）；`cargo clippy --workspace -- -D warnings` ✓ | 子流程恢复由「每 5s `list_instances(usize::MAX)`」改为**事件登记表**（`pending_subflows`：父因 `RunSubflow` 挂起时登记）；稳态 tick 由 O(实例数) 降到 O(待恢复对)，登记表为空即 O(1)；**启动期保留一次**全量对账（恢复上个进程遗留的挂起实例）。新增负控制测试 `test_subflow_recovery_uses_pending_registry_not_full_scan`（双向：空表不得恢复、登记后必须恢复**且注销**） |
| **W1-4(b)** | `snapshot_logs_since_last = 0` 语义 | ✅ 完成（显式声明） | `git diff coord/src/main.rs config.example.toml` | 启动时对该配置打 **WARN**（说明「无持久快照 ⇒ raft 日志永不回收」）；`config.example.toml` 注释补同一后果。不拒绝启动（单节点/短命部署可能需要） |
| **W1-4(c)(d)** | 动态 region 清理 / 连接数上限 | ✅ 完成（走"显式声明"分支） | `docs/production/ops/boundaries.md` §3/§4 | 按 §5 允许的「或写进不承诺清单」分支：**不支持动态增删 region**（含 `CompactionManager` 不随 region 增删）、**无连接数上限**（`max_concurrent_streams` 限的是每连接流数）。每条附「若要变成承诺需要做什么」 |
| **W1-5** | 未接线机制逐条复核 | ✅ 完成（1 项修复 + 全表交代表） | `bash scripts/check-panics.sh`（新代码无 panic 违规）；`cargo test -p coord-client --lib pool::` ⇒ **4 ✓** | **修复**：`ConnectionPool::cleanup_idle()` 此前**零生产调用** ⇒ 改为访问时机会式清理（`maybe_sweep`，挂在 `get_from`，窗口 = `idle_timeout`、最小节流 60s；不新增后台任务因为在无 runtime 上下文构造会 panic）。新增 2 条节流负控制测试。全表见 `docs/production/ops/unwired-mechanisms.md`（含**监督覆盖率真数**：`spawn_supervised` 生产调用点 **6** vs 生产代码 `tokio::spawn` **110**，并给出长活任务清单） |
| **W2-5** | 门禁负控制演练（新增运维面卡口） | ✅ 完成（含 CI 接线 + 自带 self-check） | `bash scripts/check-gate-drills.sh` ⇒ exit 0；两组注入 ⇒ exit 1；还原 ⇒ exit 0 | 新增 `scripts/check-gate-drills.sh`（3 条判据：告警必有 `runbook_url` / 锚点必须真实存在 / 告警处置段无孤儿小节）。**已接 CI**（`ci.yml` 的 fmt+clippy job）并在 `gate-self-check` job 加了它的 self-check。演练记录：`docs/production/ops/gate-drills-2026-09-21.md` |
| **W5-2** | SLO 定义 | ✅ 完成（草案） | `docs/production/ops/slo.md` | 可用率/时延/错误率三项 + 错误预算 + SLO↔告警映射；**明确写出与 §5.4-③ quiet 可用率 0.95 的对齐红线**（SLO 不得高于验收门槛）。标注"参数确认 ③ 签回前不得对外" |
| **W5-3** | 告警 → runbook 绑定 | ✅ 完成 | `bash scripts/check-gate-drills.sh` ⇒ 12 条告警全部有可达锚点 | `monitoring/prometheus-rules.yml` 12 条告警全部补 `runbook_url`；`docs/production/ops/runbook.md` §6 逐告警处置（触发/命令/预期/失败时怎么办） |
| **W6-1** | runbook（此前全仓不存在） | ✅ 完成（**未演练**） | `docs/production/ops/runbook.md` | 启动/停止/缩扩容/成员变更/备份恢复/密钥轮换/Seal-Unseal/compaction + 逐告警处置。**每条标"⏳ 待演练"**，不写"已运维" |
| **W6-2** | k8s 部署验证 | 🟡 静态核验完成 + **修了 2 个真缺陷** | `docs/production/ops/k8s-verification.md`（11 项静态表） | 🔴 **就绪探针用错端点**：`/healthz` 是存活语义（永远 200），未选主的 pod 会被标 Ready ⇒ 改 `/ready`（未就绪返 503）；🔴 **镜像 tag 停在 `0.1.0`** ⇒ 改 `0.2.0`；🟡 补**软反亲和**。实机演练仍 ⏳ |
| **W9** | 治理（SLA / 门禁责任人 / 承诺变更流程） | ✅ 完成（流程演练一次） | `docs/production/ops/governance.md` | 沿用 `jepsen/docs/dev.md` §6 的 P0/P1/P2 定义 + 补「谁盯 / 留档在哪 / 升级条件」；门禁责任人表（含本轮新增门禁）；承诺变更流程用 **U-01 的真实全流程**做演练 |
| **W4-4** | bincode 替代路径 | 🟡 立项（豁免未关） | `docs/production/ops/dependencies.md` | 把「换序列化格式」从一句技术债升级为 **P0→P3 分阶段计划**（每阶段带判据与回滚）；P3 完成才删 `deny.toml` 豁免 |
| **W4-6** | `SECURITY.md` 支持版本矩阵 | ✅ 完成 | 人工比对 `SECURITY.md` 与 `Cargo.toml:15` | 表由「只列 0.1.x」改为「0.2.x 当前 / 0.1.x 已结束支持」，并写明本表必须与 `workspace.package.version` 一致 |
| **W4-1/W4-2** | TLS fail-closed / KEK 供给 | 🔴 **未完成（有意为之）** | `docs/production/ops/security.md` §1/§2 | **不做**的理由写成可核验事实：jepsen lab 用 `auth_enabled=true` + `raft_shared_secret` + **无 TLS** + **非 loopback**（`jepsen/src/jepsen/coord/db.clj:132/:141`）⇒ 直接加严会让 lab 每个节点起不来 ⇒ W3 全部证据链断裂。给出 **4 步联立变更计划**（lab 先启 mTLS → 加拒绝规则 → 加显式逃生阀 → 改对外口径） |
| **gitignore 陷阱** | 新交付的 ops 文档**不可入库** | ✅ 修复 | `git check-ignore -v docs/production/ops/runbook.md` ⇒ **exit 1（不再被忽略）** | `docs/production/*` 会把 `docs/production/ops/` 整个目录排除 ⇒ 加 `!docs/production/ops/`。**这与 §0/D-06 是同一形态**（「看起来交付了，实际对任何 clone 都是 404」），属本轮独立发现 |
| **P1 卡口复核（二轮）** | 本次改动后的门禁自查 | ✅ 完成 | `cargo fmt --all -- --check`；`cargo clippy --workspace -- -D warnings`；六道脚本（wire-sync / wire-descriptor / sdk-sync / panics / error-code / gate-drills） | fmt ✓；clippy ✓（非测试目标）；**六道全 `exit 0`**；`cargo test -p coord-core --lib` ⇒ **296 ✓**、`-p coord-client --lib` ⇒ **41 ✓** |

**第二轮未触碰**（仍按 §5/§6 排期执行）：W1-2 的 **lab 复跑**（形态已定位，见下）、W1-6、W1-7、W2-1/W2-2/W2-3/W2-4、W3、W4-1/W4-2/W4-3/W4-5、W5-1/W5-4/W5-5、W6-3/W6-4、W7、W8。

### 第三轮追加（2026-09-21，同一会话续）

> 本轮的主题是 **「把约定变成机器判据」**：W1-6/W1-7/W5-4 都是同一个形态 ——
> 仓库里**已经有**注释/文档说明"这件事必须怎样"，但没有任何东西会在它被违反时变红。

| # | 任务 | 状态 | 判据（已跑的命令） | 结果 / 产物 |
|:--|:--|:--|:--|:--|
| **W1-6** | watch scope 过度限制（**只能放宽**类改动，须在 RC 冻结前完成） | ✅ 完成（**判定改位置，但没消失**） | `cargo test -p coord-core --lib grpc_auth` ⇒ **13 ✓**；`cargo test -p coord-agent --lib auth::interceptor` ⇒ **26 ✓**；`cargo test -p coord-agent --lib watch_scope` ⇒ **9 ✓** | 判定从「鉴权层因**看不到** prefix 而整块拒绝」改为「鉴权层把**授权快照**（`DeferredScopeGrants`）放进请求扩展，`WatchProxy::watch` 解码首帧后用**同一个** `grpc_auth::scope_allows` 判定」。<br>**单一实现**：`scope_allows` / `watch_create_access` 从 agent 移入 `coord-core::grpc_auth`（区间由 `watch_match_interval` 给出 ⇒ 与投递侧 `key_matches` 同源）。<br>**四类判据**：①`deferred_scope_rpcs_are_streaming_scope_bearing_and_disjoint`（延后集合 ⊆ 流式 ∩ 有能力的，且与 body 缓存集合**互斥** —— 相交就是第四轮 P0）②`watch_create_access_is_identical_to_body_extraction`（6 组 key/range 对照鉴权层 body 提取）③handler 级正反双测 9 条（越界前缀/兄弟前缀/区间投递集合/无快照/无约束快照）④**tower 级接线测试**：`Capturing` inner 断言"放行时请求扩展里真的带着快照" |
| **W1-7** | enum 演进机械保护（D-17/C22） | ✅ 完成（**换了可执行的落实方式**，见右） | `cargo test -p coord-proto --test enum_wire_freeze` ⇒ **2 ✓**；负控制两组：新增值 ⇒ `新增 1 条` 红、改号 ⇒ `改号 1 条` 红；还原 ⇒ 绿 | **计划原文的"取消 `buf.yaml` 枚举 lint 豁免"在本仓库**不可执行**：`apis/contracts/buf.yaml:31-34` 写明这两个豁免是"与线端既有实现保持 wire 兼容的刻意决定，禁止为通过 lint 而修改"（重命名枚举值本身就是 Breaking）。改为做**真正有意义的机械保护**：`coord-proto/wire-freeze/enums.txt`（32 值快照）+ `tests/enum_wire_freeze.rs`（逐条比对：新增/删除/改名/改号分别报出）+ `zero-value-allowlist.txt`（零值口径白名单，**反向判据**：白名单条目不再是零值 ⇒ 红）。标签：新增=Minor、改名/改号=**静默** Breaking（grpc-java 编译不报错） |
| **W5-1** | 指标面复核 | ✅ 完成 | `docs/production/ops/observability.md`（6 行逐项「谁产生/谁消费/怎么验证」） | 3 项已闭环、1 项本轮新增（W5-4）；**如实写出两处残余**：采样任务自身无监督（§2.2）、SLO 阈值未经真实数据校准（§2.3，属 W5-5/W3-7） |
| **W5-4** | 能力死亡必须可观测（第四轮 §6.3.6） | ✅ 完成（**这曾是 agent 面最后一块空白**） | `cargo test -p coord-core --lib workflow::runtime` ⇒ **30 ✓**（含 4 条新判据）；`cargo test -p coord-agent --lib metrics` ⇒ **11 ✓**；`bash scripts/check-gate-drills.sh` ⇒ exit 0（13 条告警全部有可达 runbook 锚点） | `coord-core` 新增 `WorkerLiveness` + `WorkerRegistry`：**循环型** worker 用 `is_finished()`、**一次性** worker 用"结束但未置 completed 标志"（不用 `catch_unwind`：`futures` 不在 coord-core 依赖面，为一个可观测性需求引入新依赖不划算）。4 处 `drive` spawn 统一走 `spawn_drive`。agent 侧：3 个指标 + 15s 采样任务（首次观察到故障打 ERROR）+ 告警 `CoordAgentWorkflowWorkerFault` + runbook 小节。**内存有界**：正常收尾的登记项立即结算丢弃、故障标签保留 ≤8 条（有判据） |
| **W6-3** | 滚动升级 / N-1 兼容 | ✅ 完成（**裁定为"不支持"+ 给出可执行的停机升级路径**） | `docs/production/ops/upgrade.md` | 结论：**不支持滚动升级、不声明 N-1 兼容、不声明跨版本快照恢复**，升级 = 停机且整集群同版本。三条依据全部可核验：①agent↔server **无**协议协商（`grep -rn protocol_version coord-server/src` 无输出）②**诚实的坏消息**：`sim_chaos_test.rs:530` 那个名为"快照格式兼容"的测试其实是在测试里**重新定义**了一个影子 `SnapshotHeader` 做 bincode roundtrip，**不覆盖真实快照路径** ③agent↔SDK 协商是**真的**（`handshake.rs:29` + 对表测试）。含 5 条"不支持"清单（U-1…U-5）与 3 条待办 |
| **W8-1/W8-2/W8-3** | 业务落地准备 | ✅ 完成（文档面） | `docs/production/adopter-playbook.md` | 接入面=SDK（**不恢复 starter**，附 `@Bean(destroyMethod="close")` 配方与"为什么必须"）；SDK **不发布**（U-06）的构建方式；§3 边界清单 11 行（逐条对应 `boundaries.md`）；§4 **如实列出 7 道门的红灯**；§5 试点准入 6 条 + 回退/数据可逆性 + 8 条接入方验收清单（其中 G8/72h **明确标未做**）；§6 「可以说 / 不可以说」口径 |
| **U-04…U-10** | 未决项裁定 | ✅ 完成（**U-04 只裁定、未实施，已单列 W4-2a**） | 本文 §8.4（含理由与落地物） | U-04 KEK=启动注入+边界声明、U-05 无外部消费者、U-06 SDK 不发布、U-07 replication 不建契约、U-08 policy RBAC 不持久化、U-09 cb/rl 边界确认+`STATUS.md` 单一事实来源、U-10 承诺对象=内部业务团队 |
| **P1 卡口复核（三轮）** | 本次改动后的门禁自查 | ✅ 完成 | `cargo fmt --all -- --check`；`cargo clippy --workspace -- -D warnings`；**六道脚本**；`cargo test --lib -p coord-core -p coord-agent` | fmt ✓；clippy ✓（非测试目标）；六道全 `exit 0`；**coord-core 300 ✓ / coord-agent 479 ✓**（含本轮新增 15 条判据） |
| **P1 卡口复核（三轮）** | 本次改动后的门禁自查 | ✅ 完成 | `cargo fmt --all -- --check`；`cargo clippy --workspace -- -D warnings`；**六道脚本**；`cargo test --lib -p coord-core -p coord-agent` | fmt ✓；clippy ✓（非测试目标）；六道全 `exit 0`；**coord-core 300 ✓ / coord-agent 479 ✓**（含本轮新增 15 条判据） |
| **顺手清理** | `coord-agent/src/auth/sync.rs` 的未使用 `use std::thread;` | ✅ 完成 | 全仓 `grep -n 'thread::' coord-agent/src/auth/sync.rs` ⇒ 无输出 | 每条 `cargo test` 都留一条 warning 会让"新引入的 warning"这个信号被淹没 |

### 第四轮追加（2026-09-21，同一会话续）

| # | 任务 | 状态 | 判据（已跑的命令） | 结果 / 产物 |
|:--|:--|:--|:--|:--|
| **W1-2** | **F-05**：登录路径 × 无 quorum | ✅ 完成（判据已修正 + 关键性质已钉住 + lab 复跑已归档） | `cargo test -p coord-server --lib auth::service` ⇒ **146 ✓**（含 2 条新判据）；lab：`make -C jepsen/lab test WORKLOAD=register NEMESIS=kill-all TIME_LIMIT=60 CONCURRENCY=1n SKIP_CHECKERS=1 JEPSEN_PROVIDER=docker` ⇒ 退出码 **0** | **①判据修正**：原判据"60s 短跑 0 条 `:no-client`"在 **quorum 全丢**时**不可能成立**（`Authenticate` 要提交两次 `persist_session`）⇒ 改为两条**可执行**判据：无 quorum 的登录失败必须是**可重试**码、密码错必须是 `UNAUTHENTICATED`（含负控制：改成 `unauthenticated` ⇒ 必红）。<br>**②lab 复跑**：`:fail 0` / `:no-client 0`（5 轮 kill-all），RTO p95 2.31s，**本 run 未复现** F-05 —— 但只有 1 次 run（原路径要求 ×3）且形态不完全同源（原出错的多是经 agent 的 M5b 与 partition-ring）⇒ **不得**读成"F-05 已消失"。归档 `docs/production/evidence/20260921T163732Z-w1-2-f05-kill-all-60s/`（**工作树 clean**）。 |
| **W6-4** | 备份恢复演练 | ✅ 进程内完成（10/10，**首份 CLEAN 树归档**） | `snapshot_rpc_test` 2 ✓ / `snapshot_transfer_test` 1 ✓ / `restart_recovery_test` 4 ✓ / `m0_recovery_suite` 3 ✓ | `docs/production/evidence/20260921T161653Z-w6-4-backup-restore-drill/`（MANIFEST + run.log + sha256sums）。覆盖在线快照→恢复、导出→清空→导入 roundtrip、落盘重启加载、**purge 守卫**、applied 水位持久化、重放幂等、**kill -9 后 revision 不回退**。<br>**边界与证据同引**：不含对象存储 / 不含多节点 / 未人工复核 ⇒ 不得当作"备份恢复已验收"。 |
| **提交纪律** | 本轮起证据跑在**已提交的 CLEAN 树**上 | ✅ 完成 | `git log --oneline`：`19cb350`（代码+文档）、`d1b0273`（证据+台账）；`git status --porcelain` 在证据 run 时只剩证据目录自身 | 消掉 D-10 的一半（"全部归档的 worktree 字段为 DIRTY"）：**新归档从此可以是 CLEAN**；旧的 28 份仍为 DIRTY（重跑才能覆盖，归 W3-9） |

### 第五轮追加（2026-09-22）—— 主题：把「裁定」与「现象」都推进到「落地」与「根因」

> 本轮两条线：①**U-04 从裁定到落地**（W4-2a）；②**§2.2 的三处门禁红从现象到根因**（W2）。
> 方法见 `docs/production/ops/ci-gate-forensics-2026-09-22.md`。

| # | 任务 | 状态 | 判据（已跑的命令） | 结果 / 产物 |
|:--|:--|:--|:--|:--|
| **W4-2a** | **U-04 落地**：transit KEK 改为启动注入 + fail-closed | ✅ 代码完成 | `cargo test -p coord-agent --lib services::transit` ⇒ **35 ✓**（含 6 条新增负控制）；`cargo test -p coord-agent --test agent_transit_test` ⇒ **9 ✓**（含 1 条集成层负控制） | 修前 `SHA-256("coord-transit-kek:" \|\| kek_id)`（拿到配置即可推导 KEK）；修后 `HKDF-SHA256(材料, info="coord-transit-kek-v1:" \|\| kek_id)`，材料由 `COORD_TRANSIT_KEK`（hex64）或 `<data_dir>/transit-kek.bin`（32B）注入。**缺材料 ⇒ agent `serve()` 返回 Err ⇒ 进程非 0 退出**（修前只是 `tracing::error!` 后少注册一个服务 = 静默降级）。`TransitKekMaterial` 的 `Debug` 刻意 redact。<br>负控制：长度 0/1/16/31/33/64 一律拒绝；空 hex/仅空白/非 hex 一律拒绝；**env 非法时不静默回落到文件**；**同 `kek_id`、不同材料 ⇒ 必须解不开**（这条同时证明 KEK 来自材料而非配置串）；HMAC 密钥随材料变化 |
| **U-11** | `transit` 默认开关 `true` → `false`（**超越 U-03 中那一句**） | ✅ 完成 | `cargo test -p coord-agent --lib service::tests` ⇒ **6 ✓** | 理由：U-03 给 transit 保持 `true` 的前提是"启用即可用"，而 U-04 落地后启用它必须先注入 KEK ⇒ 已属未整改面 ⇒ 按 G9 必为默认关。见 §8.5 |
| **W2-2** | `cargo audit + deny` 定时红：**根因定位 + 修复** | ✅ 完成（待 CI 实证） | `curl …/check-runs/{job_id}/annotations`；action 源码 `src/main.ts`；RustSec advisory-db 提交列表 + 本地 `Cargo.lock` 交叉比对；本地 `cargo deny check bans licenses sources` ⇒ `bans ok, licenses ok, sources ok` | 根因：`rustsec/audit-check@v2.0.0` 在 **schedule** 事件走 `reportIssues()`（`issues.create`），其余事件走 `reportCheck()`（`checks.create`）；job 只授了 `checks: write` ⇒ 定时跑 403 `Resource not accessible by integration`。**四条定时跑与 push 跑的 audit 结果逐字相同**（`1 warnings found!`，唯一一条是 bincode unmaintained 非漏洞）⇒ 不是依赖问题。修：`ci.yml` 的 `security-audit` 补 `issues: write` + 根因写进注释。**附带**：此前 step 7 一红 step 8 就 skipped ⇒ `licenses`/`bans`/`sources` **从未有执行记录** |
| **W2-1** | `weekly perf baseline` 13/13 常驻红：**根因已复现 + 修复已验证** | ✅ 已完成（本地验证） | 修前：本地 `PERF_GATE=1 cargo test --release -p coord --test perf_bench -- --ignored --nocapture` ⇒ **`FAILED. 8 passed; 1 failed` / EXIT=101**（与 CI 同形）。修后加 `--test-threads=1` ⇒ **`ok. 9 passed; 0 failed` / EXIT=0` / 324.22s**，**两组实例均过**（25/1 Region ratio 0.944 与 1.044） | 红因是**测量方法**：多个重型基准默认并发（`test-threads = nproc`）争 fsync ⇒ 同一次跑里两组同代码测量结论相反：单独实例 **1.360（过）**，`bench_all` 实例 **0.541（失败）**；1-Region 基线自身漂移 **35 → 219 ops/s（6 倍）**，且裸 Redb 写入在并发下从 227 掉到 57 ops/s ⇒ 0.80 判据落在噪声带内。修：`scripts/bench-ci.sh` 加 `--test-threads=1`（**不动阈值、不删断言**）。**残留（已记账）**：基线只采 200 迭代 vs 25-Region 5000 ⇒ 余量最小仅 0.859，未根治 |
| **W2-3** | `real-process chaos` 间歇红：**范围收窄**（未定位根因） | 🟡 部分 | 5 次跑逐 step 聚合（`/actions/runs/{id}/jobs`） | 新事实：失败**永远在 step 7**（`chaos_real` kill9 套件），且其后 5 个 step 全部 **skipped** ⇒ `14/31` 不是"6 个套件随机各红一次"。**不声称**是真缺陷或假红（job 日志 403）⇒ 给出可执行判定流程（见 forensics §3.3） |
| **W2-4** | 分支保护 | ⛔ 阻塞 | `GET /branches/main/protection` ⇒ **401 Requires authentication** | 本环境无 `gh`、无可用于 REST 的 token（推送走 `GIT_ASKPASS`，不应取出当 API token）⇒ 属**组织/仓库设置事实**，必须由 admin 执行；已给出 `gh api` 命令与验证判据（向 `main` 直推被拒） |
| **D-19（本轮新发现）** | 「默认开关」两条路径**并未拉平** | ✅ 钉住（未修） | `cargo test -p coord-agent --lib service::tests::test_known_divergence_code_default_vs_toml_is_pinned` ⇒ **1 ✓** | 把 `pki` 加进 `test_service_config_toml_missing_fields_match_code_defaults` 的比对时暴露：`registry`/`config_center`/`lock`/`idgen`/`policy`/`pki` 六项**代码默认 `true`** 而字段是普通 `#[serde(default)]`（配置文件缺省 `false`）⇒「走不走 `--agent-config`」得到**不同服务集合**。按纪律不删断言、不放宽：新增**钉住测试**（改任一侧即红），并立 D-19 待裁定修法 |
| **CI 首验第三/四轮** | 推送 `ff2dfb39` 让 CI **第一次真正验证**第三、四轮的改动 ⇒ **抓到 U-03 的两处回归** | ✅ 已修（本地两条均已验证转绿） | CI run `35739871833` 的 `workspace tests` = **failure**（`exit code 101`）。本地 `cargo test --workspace --no-fail-fast` **复现出完全相同的两条**：<br>① `coord/tests/cli_agent_test.rs::test_agent_uses_config_data_dir_unless_flag_is_explicit`<br>② `coord-agent/tests/agent_grpc_test.rs::test_agent_cache_rpop_llen` | **根因（同一个）**：`AgentConfig.services` 是 `#[serde(default)]`，而这两个用例的 TOML **不写 `[services]`** ⇒ 走 `ServiceConfig::default()`。U-03 把 `cache`/`workflow` 由 `true` 改 `false` 后：① **agent 从不主动创建 `data_dir`**（此前只是被 cache/workflow 在自己的路径里顺手建出来）⇒「agent 必须在配置的 `data_dir` 下建目录」这条产品口径失去可观测结果；② cache 服务不注册 ⇒ 客户端 `Unimplemented`。<br>**为什么以前没抓到**：第三、四轮**从未推送**（`origin/main` 停在 `8b65290`），且第三轮的「P1 卡口复核」只跑 `--lib` 目标 ⇒ **集成测试那半边没有任何执行记录**（`ci.yml` 自己记过同型事故：「CI 曾连续 45 次全红无人发现」）。<br>**修法**：①产品侧 —— `serve_inner` 启动即创建 `data_dir`（**best-effort + WARN**：默认 `/var/lib/coord-agent` 在非 root 的 CI 不可写，而 `agent_non_loopback_guard_test::test_loopback_without_auth_allowed` 正是用默认配置起服务并要求**成功**）；②测试侧 —— cache 用例显式 `config.services.cache = true`（U-03 的「显式启用即可用」）。**没有改弱任何断言**；验证：这两个 target 分别 **1 ✓** / **7 ✓**，且 `agent_non_loopback_guard_test` **3 ✓**（证明不影响默认 data_dir 的用例） |
| **P1 卡口复核（五轮）** | 本轮改动后的门禁自查 | ✅ 完成 | `cargo fmt --all -- --check`；`cargo clippy --workspace -- -D warnings`；**六道脚本**；`cargo test -p coord-agent --lib` | fmt ✓；clippy ✓（非测试目标）；六道全 `exit 0`（panics / gate-drills / wire-sync / wire-descriptor / sdk-sync / error-code）；**coord-agent lib 488 ✓**（前值 479 + 本轮 9 条新判据） |

**第五轮未触碰**：W2-1 的 **CI 实证**（需 CI 跑一次 schedule）、W2-3 根因（需 job 日志或本地长跑）、
W2-4（仓库设置）、W2-5 的九道门逐门负控制（1.5 人日）、W3 全部（要长跑）、
W4-1（TLS fail-closed，仍按「lab 联立变更」4 步计划）、~~W4-3（审计）~~（**第九轮已按 U-14 移除**）、W4-5（RSS 口径）、
W5-5、W6-2 实机演练、W7 全部。

> **第五轮的 CI 实证（push run `35739871833`，`ff2dfb39`）**：`fmt + clippy -D warnings`、`gate self-check`、
> `proto contract`、`java sdk`、`java example integration`、`frontend lint`、
> `plugin engine feature matrix`、**`cargo audit + deny`** 均 **success**；
> `workspace tests` **failure**（见上表「CI 首验第三/四轮」：**不是本轮的代码问题**，
> 是第三轮 U-03 的回归首次被 CI 抓到，已在下一提交修复）。
> ⚠️ 另两点必须写清楚：① `weekly perf baseline` 在该 run 是 **`skipped`**（job 条件 `if: schedule`）
> ⇒ **W2-1 的 CI 实证要等下一次定时跑**，本轮只有**本地同参**实证（`EXIT=0`）；
> ② W2-2 的修复在 **schedule** 事件下才走 `reportIssues()` 分支 ⇒ 同样**要等定时跑**才算闭环。
> 因此本节所有 ✅ 仍受 §5 W2 的裁决约束（**W2 未完成 ⇒ 不计入对外门禁证据**）。
>
> **流程教训（本轮最重要的非代码收获）**：**未推送的提交 = 从未被验证的提交**。
> 第三、四轮共 4 个提交在本地停留两天，其间 CI 一直在验证 `8b65290`（= 第二轮）。
> 而本地"卡口复核"习惯只跑 `--lib` 目标 ⇒ **集成测试层长期无执行记录**。
> 后续每轮收尾必须：**推送 + 等 CI 出结论 + 把 run id 与结论写回本表**（不接受"本地跑过了"）。

**第四轮未触碰**：同第三轮 —— W2-1/W2-2/W2-3（CI 日志不可得）、W2-4（仓库设置项）、
W3 全部（要长跑）、W4-1/W4-2a/W4-3/W4-5、W5-5、W6-2 实机演练、W7。

**第三轮未触碰**：W1-2 的 lab 复跑、W2-1/W2-2/W2-3（**需要 CI job 日志，本环境无 `gh` 且仓库私有 ⇒ 无 token 时无法取**）、W2-4（仓库设置项）、W3 全部、W4-1/W4-2**a**/W4-3/W4-5、W5-5、W6-2 实机演练、W7-1/W7-2/W7-3。

> **纪律提醒**：W2 未完成（门禁仍有常驻红/间歇红）⇒ 上表所有 ✅ 都只是「本地可重跑」，
> 按 §5 W2 的裁决，**尚不得计入对外门禁证据**；⏳ 项一律不得当作已绿。

### 第六轮追加（2026-09-23）—— 主题：**把「取不到的日志」变成「失败的注解」**

> 触发：两次新实跑带来两个必须回答的问题（同 SHA 的 push 红 / 定时绿 ⇒ 间歇红的
> **测试名**是什么？perf 首次真跑即红 ⇒ **红在哪条断言、什么数字**？）。两者都卡在同一
> 通道问题上（日志 403 / 工件 401 / 无 `gh`）⇒ 先修通道。方法学见
> `docs/production/ops/ci-gate-forensics-2026-09-22.md` §6。

| # | 任务 | 状态 | 判据（已跑的命令） | 结果 / 产物 |
|:--|:--|:--|:--|:--|
| **W2-2 闭环实证** | audit 定时红修复在 **schedule 事件**下生效 | ✅ **完成（含 CI 实证）** | schedule run `35810892940`（SHA `1a8c827`）逐 job：`cargo audit + deny` = **success**；`Security audit` = **success** | 第五轮要求的「等下一次定时跑」已兑现；W2-2 从"待实证"转为**已闭环** |
| **W2-1 首次真跑** | perf job（`weekly perf baseline`）**首次真正执行**即红 | ✅ 取证（根因待数字） | `GET /actions/jobs/107021983636` 的 step 时间戳 02:34:53→02:42:42（**7m49s**） | 7m49s 排除"编译失败/超时"；退出码 **101** 排除 python 门禁（其退出码为 1/2）⇒ 红在 `perf_bench` 内部断言。**具体格数与数字**要等下一次 schedule 跑（注解通道本轮已就位） |
| **W2-1 附带** | perf 的「静默通过」路径（自述式 no-op）修掉 | ✅ 完成 | `git diff scripts/bench-ci.sh`；`bash -n` 语法检查 | 无基线时不再静默写 baseline + `exit 0`（读起来像通过），改发 `::warning::` **明说"跨运行比较未执行"**；报告工件改 `if: always()`；cargo 输出由 `>"$REPORT"` 改为 `tee`（修前失败时 step 日志几乎为空） |
| **W2 通道**（新增，服务后续所有轮次） | **失败自描述**：注解器 + 包装器 + 10 处接线 | ✅ 完成 | 合成日志测试：编译错误（带 `--> path:line`，**位置在错误行下方**）、`test ... FAILED` 用例名、`panicked at path:line:col` + 消息、`PERF GATE`/`REGRESSION` 行、兜底尾 5 行；包装器 `ls /nonexistent` ⇒ **exit 2 透传** | `scripts/ci-annotate-test-failures.sh`（≤10 条 `::error::` + step summary，恒 exit 0）、`scripts/ci-run-with-annotations.sh`（tee + 透传退出码）；`ci.yml`：`test` job ×2、`chaos-nightly` ×7 + 新增 DoS step、`perf-bench` 工件 |
| **W2-3 观察** | chaos **连续两次 success** | 🟡 记录（n=2） | push `35745314425` + schedule `35810892940` 逐 job | 与 14/31 的历史相比是连续两绿；**不得**据此宣称稳定化 |
| **W2-3 结构修补** | chaos job 的后继套件不再被 skip | ✅ 完成 | `python3 -c "import yaml; …"` 读回 `ci.yml`：chaos job 8 个套件 step 全部 `if: always()` | 此前失败恒在 step 7 而其后 5–6 step 全 skipped（计划书 §2.2 已记）⇒ 「后面的套件没有任何执行记录」。现 job 红/绿语义不变，但一次 run 能给出**全部套件**的结论；每步开头的孤儿进程清理仍保留 |
| **W2-5（部分）** | 九道门的**负控制演练**：自检从 2 道门扩到 **6 道** | ✅ 完成（6/9） | 逐条本地注入验证：descriptor（字段号 3→30 ⇒ **exit 1**，报 `签名不一致: field:RangeRequest.limit`）、wire-sync（rpc 改名 ⇒ **exit 1**）、sdk-sync（给非 allowlist impl 加内部面 import ⇒ **exit 1**）、panic 路径（注入非测试 `panic!` ⇒ **exit 1**，报 `error_code.rs:281: clippy::panic`）；fmt / 告警↔runbook 为既有两道 | `ci.yml` 的 `gate-self-check` job 新增 4 步（每步“先确认绿 → 注入 → 断言变红 → 还原”）+ rust-cache + `timeout-minutes: 45`。未覆盖：P3–P6（需 lab；其中"审计"一项已按 U-14 移除）、P8（需仓库设置 + 真发布）、P9（尚无机械门禁） |
| **W4-5(2)** | Gate 0 DoS 的 **RSS 峰值实测口径** | ✅ **完成（本机可重跑）** | `cargo test -p coord --test dos_rss_peak_test -- --ignored --nocapture` ⇒ **1 ✓** | 新增 `coord/tests/dos_rss_peak_test.rs`（进程级、量**服务端** `VmHWM`）：100 × 8 MiB（累计 800 MiB，在飞并发 25）**100/100 被拒**，总增长 **116 MiB**（阈值 256 MiB），逐波 +69.5/+29/+14/+6.5 MiB **递减**。两条自我防护：读数为 0 必须报错（防解析 bug 使断言恒真）；另设"首波后漂移"断言专抓"随累计字节增长"。已接 `chaos-nightly`；`security.md` §3 表同步 |
| **workspace tests 间歇红** | push 红（2m12s）/ 同 SHA schedule 绿（2m13s） | 🟡 通道就位（未复现） | 本地 detached 复跑 `cargo test --workspace --no-fail-fast`；CI run `35869740196` 的 `workspace tests` | 红跑耗时与绿跑一致 ⇒ 整套件跑完才失败（非早期崩溃）。**本地复跑**：跑完 77 个测试二进制、**0 失败**后卡在 `test_pd_executor_real_raft_add_transfer_remove_peer` **>45 分钟**（已终止；同命令 CI 2m13s）⇒ 本地全量只能当烟测。**CI 复跑**：本 run 绿 ⇒ 间歇红未复现；测试名由新注解通道在下次复现时带出 |
| **P1 卡口复核（六轮）** | fmt / clippy / **六道脚本** | ✅ 完成 | `cargo fmt --all -- --check`；`cargo clippy --workspace -- -D warnings`；wire-sync / wire-descriptor / sdk-sync / panics / error-code / gate-drills | fmt ✓；clippy ✓（非测试目标）；**六道全 `exit 0`** |
| **CI 实证（`a8e6fb3`）** | 第六轮改动的首次 CI 验证 | ✅ 取证完成 | run **`35869740196`** 逐 job（`/actions/runs/{id}/jobs`） | `workspace tests` **success**（间歇红未复现）；`gate self-check` **failure** ⇒ 定位为“该 job 唯一不装 protoc” ⇒ 已修（新自检立刻抓到“门禁在某 job 里跑不起来”，与 fmt 卡口当初同型）；`real-process chaos` **failure** ⇒ 注解带出 `chaos_real.rs:562` 的 `soak put failed at iteration 784`，且 `if: always()` 使**其余 7 套件全部执行且全绿**（含新 DoS drill ⇒ **W4-5 第 2 项在 CI 首次执行即绿**）；perf skipped（push 事件不跑） |
| **chaos 失败点（收窄，未定位根因）** | `chaos_soak_distributed` 的**每迭代写只有一次机会** | 🟡 已收窄 | 同 run：step 7（默认 300s）第 784 次迭代写失败；step 8（120s 单独跑）**通过** | 已排除：kill9 用例的节点泄漏（该用例结尾显式 `kill9()` 全部节点）、“重试后仍失败”。**未弱化断言**：改为把逐节点错误 + 集群快照（可达性/revision）带进 panic 消息 ⇒ 下次红经注解直接归因（`coord/tests/chaos_real.rs` 的 `put_any`/`cluster_snapshot`） |
| **CI 复跑（`e197e2e`）** | 上述两处修复后的复跑：**全绿** | ✅ 完成 | run **`35876239831`** 逐 job（`/actions/runs/{id}/jobs`） | `gate self-check` **success**（6 步自检**全过**：fmt / 告警↔runbook / wire-descriptor / wire-sync / sdk-sync / panic 路径）；`real-process chaos` **success**；`workspace tests` / `cargo audit + deny` / `fmt + clippy` / `proto contract` / `java sdk` / `java example integration` / `frontend lint` / `plugin matrix` / `Security audit` 全 success；`weekly perf baseline` skipped（push 不跑）。⚠️ **不宣布 chaos 已稳定**：最近 4 次为 绿/绿/红/绿 |

> **本轮的一条方法论收获（写给后续轮次）**：perf 失败时"报告被重定向进文件、工件又
> 被 skip"⇒ 一个数字都带不出来；`chaos` 失败时"其后 step 全 skipped"⇒ 后面的套件
> **没有任何执行记录**。**门禁的价值不在它会不会红，而在它红的时候你能不能知道为什么。**
> 本轮的注解通道把"能知道"变成默认行为，而不是靠人去想办法。

### 第七轮追加（2026-09-24）—— 主题：**把"取不到原因的失败"变成可达的自描述，并给 10-31 硬期限补首批 lab 产物**

> 触发：两个新 CI 红都**取不到原因**——push run `35881648825` 的 `gate self-check` step 11
> （18s、exit 101、零注解）与 schedule run `35947874357` 的 perf 第 2 次红（同形态）。
> 本轮先修"为什么拿不到原因"（W2），再用 docker lab 补 W3-4/W1-2 证据（含 10-31 硬期限
> 且此前**零产物**的 idgen / registry），最后按 G9 收口 D-19（U-12）。

| # | 任务 | 状态 | 判据（已跑的命令） | 结果 / 产物 |
|:--|:--|:--|:--|:--|
| **W2-7a** | perf 注解器**不可达死代码**（两次 perf 红"零注解"的根因） | ✅ 完成 | ①最小复现 `bash -c 'set -euo pipefail; false \| tee /dev/null; echo REACHED'` ⇒ **不打印 REACHED**（管道失败即终止脚本）；②`PATH` 注入 stub cargo 模拟 perf 红 ⇒ `GITHUB_ACTIONS=true` 下发出 **10 条 `::error::`**（含 4 行 Region 比值表）、退出码 **101 透传** | 根因：`set -e` + `pipefail` 下 `cmd \| tee` **直接终止脚本** ⇒ `PERF_STATUS=${PIPESTATUS[0]}` 与失败分支（注解调用）**永远不可达**。修法：两条管道显式 `set +e` / `set -e` 包裹（`collect-evidence.sh` 早有同型先例）。**这是"自述式 no-op"的第三种形态：错误处理代码不可达。** |
| **W2-7b** | perf 门禁的**第二个真根因**（测量分母）+ 本地复现 | ✅ 完成（本地双向印证） | 修前 `PERF_GATE=1 taskset -c 0-3 cargo test --release -p coord --test perf_bench -- --ignored --nocapture --test-threads=1` ⇒ **FAILED. 8 passed; 1 failed**（474s、退出码 101，与 CI 同形） | `bench_all` 实例：1 Region = **222 ops/s**（200 次 ≈0.9s 的短窗口）⇒ 25 Region = 148 ⇒ **ratio 0.667 红**；**同一次跑**的 standalone 实例：1 Region = 128 ops/s、ratio 1.233 绿。⇒ **不稳定的是分母**（短窗口测到"未进稳态"的乐观基线；旧口径各档窗口相差 25 倍），不是被测系统 |
| **W2-7c** | 采样对称化：所有档位**同迭代数（5000）+ 同预热（100）** | ✅ 完成（本地验证；**CI 实证待下一次 schedule**） | 修后同参复跑 ⇒ **`9 passed; 0 failed`**（539.61s） | bench_all：1=157 / 5=147（0.940）/ 10=214（1.363）/ 25=207（1.324）；standalone：**209 / 215 / 223 / 215 ops/s**，ratio **1.026 / 1.066 / 1.025**（四档完全贴合，窗口各 ~23s）。**阈值仍 0.80、断言未删**；代价是 perf 测试期 +~65s |
| **W2-7d** | panic 门自检失败的**自描述** | ✅ 完成 | stub clippy（exit 101 + stderr）⇒ `::error::`（含 `file:line` 编译错误）+ step 日志 stderr tail + `df -h`；退出码 1 置红 | `scripts/check-panics.sh`；run 35881648825 的"18s/101/零注解"形态下次复现即可归因 |
| **W2-7e** | `.gitignore` 补 `/benchmark-results/` | ✅ 完成 | `git check-ignore -v benchmark-results` ⇒ 命中 | 本地跑 perf 后残留的未跟踪目录（与 §7-2 clean tree 判据冲突的假 DIRTY 来源） |
| **W3-4（首批产物）** | **idgen / registry 零产物补齐**（10-31 硬期限） | ✅ 4/4 pass、**已归档** | `make -C jepsen/lab test WORKLOAD=idgen\|registry NEMESIS=none\|kill-agent TIME_LIMIT=60 CONCURRENCY=2n SKIP_CHECKERS=1 AGENTS=2 IDGEN_MIN_IDS=10 REGISTRY_MIN_CYCLES=1 JEPSEN_PROVIDER=docker`（binary `764773c1`、commit `f01635a`、F-55 预检 0 DROP） | 四份归档（下附）；`:violations-by-class {}` ×4、`:linear :valid? true`；registry 判定 98 cycles（含 76 次 `dup?` 幂等性重复注册）；MANIFEST 工作树 **clean** |
| **W1-2（补齐 ×3）** | F-05 复跑第 2、3 次 | ✅ 2/2 pass、**已归档** | 同形式 `WORKLOAD=register NEMESIS=kill-all TIME_LIMIT=60 CONCURRENCY=1n` | run2：写 93 次中 **1 次** `:no-client Failed to authenticate`（`gates-valid: true`、`:linear :valid? true`）；run3：`:fail 0`、valid。**出现率 ≈1%（3 次 run 合计 1 次）且不计入一致性违反**——与第四轮修正后的判据一致 |
| **U-12（D-19 收口）** | 六服务默认开关取②（代码默认改 `false`） | ✅ 裁定 + 落地 | `cargo test -p coord-agent --lib` ⇒ **487 ✓**；`cargo test -p coord-agent --tests` ⇒ 全 ok；`cargo test -p coord --test dev_mode_test` ⇒ **7 ✓** | 见 §8.6；六项 `true→false`；判据改为**全字段逐字段等价**；`dev_mode_test.rs` 4 条旧默认用例改为显式启用 |
| **CI 实证（`f01635a`）** | 第七轮脚本改动的首次 CI 验证 | ✅ **全绿** | run **`36000602894`** 逐 job（`/actions/runs/{id}/jobs`） | 全部 success：`fmt+clippy` / `workspace tests` / **`gate self-check`（含新诊断的 6 步）** / `cargo audit + deny` / `Security audit` / `proto contract` / `java sdk` / `java example integration` / `plugin matrix` / `frontend lint` / `real-process chaos`；`weekly perf baseline` skipped（push 不跑）⇒ **perf 的 CI 实证要等 09-25 02:17 schedule（注解通道已修好，数字会直接带出）**。⚠️ 不宣布 chaos 已稳定：最近 5 次 绿/绿/红/绿/绿 |
| **P1 卡口复核（七轮）** | fmt / clippy / 六道脚本 | ✅ 完成 | `cargo fmt --all -- --check`；`cargo clippy --workspace -- -D warnings`；六道脚本 | fmt ✓；clippy ✓（非测试目标）；**六道全 `exit 0`**（含改写后的 `check-panics.sh` 其自身绿路径） |
| **CI 实证（`cab765a`）** | U-12 + perf 采样的 CI 验证 | 🟡 `workspace tests` **success**（U-12 影响面在 CI 上过）；`chaos` 抓到一处 U-12 边界效应（已修，见下行） | run **`36007047810`** 逐 job | `workspace tests` / `gate self-check` / `fmt+clippy` / `cargo audit + deny` / `Security audit` / `proto contract` / `java sdk` / `java example integration` / `plugin matrix` / `frontend lint` 全 success；`real-process chaos` **failure** ⇒ **注解通道第一次真正把根因带出来**：`coord/tests/agent_auth_process_test.rs:516` 的 F-50 判据（`idgen nodeid registration … must land in /_idgen/nodes/`） |
| **F-50 用例修复（U-12 边界）** | `agent_auth_process_test` 的 agent.toml 显式启用 idgen | ✅ 完成（本地复现→修复均实证） | `AGENT_AUTH_REAL=1 cargo test -p coord --test agent_auth_process_test -- --ignored --nocapture --test-threads=1` ⇒ 修前 **FAILED. 0 passed（63.63s，恰好是 60s 注册死线）**，修后 **1 ✓（3.09s）** | 根因：用例的 `agent.toml` **整张 `[services]` 表都没写**，而 `AgentConfig.services` 是**字段级** `#[serde(default)]` ⇒ 缺表时 serde 用的是 **`ServiceConfig::default()`（代码默认）**——U-12 之前 idgen 因此是开的。修法：显式 `[services] idgen = true`（"显式启用即可用"） |
| **CI 复验（`647171b`）** | F-50 修复后的全量复验 | ✅ **全 job success（含 chaos）** | run **`36013532266`** 逐 job | `real-process chaos` **success**（F-50 修复在 CI 生效；⚠️ 本次红**不是**间歇抖动，而是 U-12 的**确定性**回退面，已按注解通道定位并修复）；`workspace tests` / `gate self-check` / `fmt+clippy` / `cargo audit + deny` / `Security audit` / `proto contract` / `java sdk` / `java example integration` / `plugin matrix` / `frontend lint` 全 success；`weekly perf baseline` skipped（push 不跑）⇒ **perf 的 schedule 实证仍待 09-25 02:17** |

> **被 CI 抓到的 U-12 边界效应（同日修复）**：F-50 用例的 `agent.toml` **整张 `[services]`
> 表都没写**，而 `AgentConfig.services` 是**字段级** `#[serde(default)]` ⇒ 缺表时 serde 用的是
> **`ServiceConfig::default()`（代码默认）**，U-12 之前 idgen 因此是开的。**顺带更正 D-19 的
> 一句话**：原文"传个没写 `[services]` 的 TOML 就会把这些服务全部关掉"**不准确** —— 真正
> 缺省 `false` 的是「**表在但键缺**」；**整表缺失**走的是代码默认。两层语义本轮两处均已
> 按实测修正（`service.rs` 注释 + §3 D-19 行）。**这也是注解通道的第一次真实收益**：红 job
> 在无日志、无 `gh` 的环境下直接给出了文件名:行号与断言文本。

证据归档（6 份；MANIFEST 工作树均 clean、commit `f01635a`、binary `764773c1`）：

- `docs/production/evidence/20260924T130833Z-w3-4-idgen-none-agents2-60s/`
- `docs/production/evidence/20260924T130834Z-w3-4-idgen-kill-agent-agents2-60s/`
- `docs/production/evidence/20260924T130836Z-w3-4-registry-none-agents2-60s/`
- `docs/production/evidence/20260924T130837Z-w3-4-registry-kill-agent-agents2-60s/`
- `docs/production/evidence/20260924T130839Z-w1-2-f05-kill-all-60s-run2/`
- `docs/production/evidence/20260924T130841Z-w1-2-f05-kill-all-60s-run3/`

> **本轮的方法论收获（第三种形态）**：第六轮发现"失败带不出原因"，第七轮发现原因之一是
> **错误处理代码本身不可达**（`set -e` + `pipefail` + 管道）——"写了错误处理"≠"错误处理会跑"。
> 两种形态同属一类：**任何"应该发生的动作"都必须有一条可核验的执行路径**（注解 / 日志 / 退出码）。

**第七轮未触碰**：W2-3 根因（chaos 又连续 2 绿、n=5 仍不宣布稳定）、W2-4（仓库设置）、
W2-5 剩余 3 道门（P3–P6 / P8 / P9 负控制）、W3-1…W3-3 / W3-5…W3-9（长跑 / 多 agent 拓扑）、
W4-1（TLS fail-closed，仍按"lab 联立变更"计划）、~~W4-3（审计采买）~~（**第九轮已按 U-14 移除**）、W5-5、W6-2 实机演练、W7。

---

### 第八轮追加（2026-09-25）—— 主题：**门禁“常驻红”清零的 CI 实证 + W3-1 浸泡首跑（F-69 立项）**

> 触发：第七轮收尾点名了两处"待定时跑"窗口（perf 修复后的首次 schedule 跑、chaos 收窄后的观测序列）。
> 两者都已在 2026-09-25T02:33Z 的定时跑到期 ⇒ 本轮先取结论回写，再启 W3 长跑。

| # | 任务 | 状态 | 判据（已跑的命令） | 结果 / 产物 |
|:--|:--|:--|:--|:--|
| **W2-1 闭环实证** | `weekly perf baseline` 在 **schedule 事件**下通过 | ✅ **完成（含 CI 实证）** | schedule run `36086768094`（SHA `b6c0a41`）逐 job 拉 `/actions/runs/{id}/jobs`；对照 push run `36017211618`（同 SHA） | 该 run **全部 job success**，其中 `weekly perf baseline` = **success**（02:33:51→02:43:55 ≈ 10m04s）—— **该 job 历史上第一次通过**（此前 13/13 常驻红）；`real-process chaos` / `cargo audit + deny` / `workspace tests` / `gate self-check` / `plugin engine feature matrix` 同 run 全 success。push run 同 SHA：**10 success + perf skipped**（符合 `if: schedule` 设计；另有仓库外部的 `Security audit` check，见下）⇒ W2-1 的修复（采样对称化 + 注解器可达性）在两种事件下均获印证；§8.1 R-07 随之解除 |
| **W2-3 观察更新** | chaos 残余分布逐条归因（n=9） | 🟡 记录（**仍不宣布稳定**） | 逐 run 拉 `/actions/runs/{id}/jobs`（9 次，2026-09-23→09-25） | 近 9 次 **7 绿 / 2 红**：`35869740196` **红**（迭代 784，第六轮已收窄、此后未复发）→ 绿×4（`35876239831` / `35881648825` / `35947874357` / `36000602894`）→ `36007047810` **红**（F-50 确定性边界，U-12 引入，当日归因并修复）→ 绿×3（`36013532266` / `36017211618` / `36086768094`）。**两次红都带得出原因**（注解通道的持续收益）；历史 14/31 的残余分布仍无法逐条归因（job 日志 403）。结论按纪律封顶为"观察中" |
| **W2-5（扩项）** | P-Gate 9 首次拥有机械门禁 + 负控制演练（**6/9 → 7/9**） | ✅ 完成（本轮） | `bash scripts/check-promise-consistency.sh` ⇒ exit 0；四组注入逐一验证：裸免责声明 ⇒ 1、契约版本漂移 ⇒ 1、删承诺行 ⇒ 1、悬空链接 ⇒ 1；**还原后全部回 0**；`python3 -c "import yaml; yaml.safe_load(open('.github/workflows/ci.yml'))"` ⇒ OK | 新增 `scripts/check-promise-consistency.sh`（4 条判据：U-01 裸声明归零 / 五份文本悬空引用 / 契约版本三方一致 / 台账↔proto 双向一致），已接 CI `lint` job + `gate-self-check` job 的 self-check。**首跑即抓到 4 处真实悬空链接**（W0-4 改引用时把相对路径写错：`apis/contracts/README.md`×2、`apis/contracts/WHITEPAPER.md`×2，已修复为 `../../docs/production/…`）。演练记录：`docs/production/ops/gate-drills-2026-09-21.md` §4 |
| **CI 实证（bba0e57）** | 本轮 P9 改动的 CI 验证 | ✅ 全 job success | run `36132342953` 逐 job | `fmt+clippy` / **`gate self-check`（含 P9 演练步骤）** / `workspace tests` / **`real-process chaos`** / `cargo audit + deny` / `proto contract` / `java sdk` / `java example integration` / `plugin matrix` / `frontend lint` 全 success；`weekly perf baseline` skipped（push 不跑）。⚠️ 尾随提交 `e8c9687`（`.gitignore` 补 `.ua/`）的推送本轮因 GitHub 网络中断未完成——待下次重试 |
| **W3-1（T2.3 收口浸泡）** | 2h `soakfull`（map+watch+lease）首跑 | 🔴 **invalid（新发现 F-69，P0 候选）** | `make soakfull JEPSEN_PROVIDER=docker WORKLOAD=soakfull SOAK_TIME_LIMIT=7200 SOAK_QUIET=1800 SOAK_DISRUPT=600 SOAK_RATE=2 CONCURRENCY=2n SEED=42 SOAK_MIX='map=20,watch=40,lease=40' SOAK_EXTRA='--watch-min-events 200 --lease-min-grants 100 --lease-min-expiries 30'`；判定见 `results.edn` / `summary.txt` | run 11:54:36Z→13:59Z（3 节点 n1/n2/n3）：**kill n1（leader）后 7.5 分钟，n2（新 leader）的 raft 判 fatal**（`when Read Snapshot(None): snapshot not found`）⇒ 其后 **85 分钟全集群写失败（9626 处）**、两个 quiet 窗口 `0/955` 与 `0/1054`、`rto-unrecovered=2`；**F-69 已立项**（`jepsen/docs/coord-findings.md`，含机制/候选/复现要点/修法方向）；另有 checker 次生：256 条 `:fail` 被 mapck 记为 delete 违反（与 F-68 同族）。证据归档：`docs/production/evidence/20260925T141209Z-t2.3-m2-watch-lease-2h-kill-snapshot-fatal/`（MANIFEST 工作树 **clean**）；诊断对照（未复现）：`…/20260925T142520Z-diag-kill-3min-openraft-logs-no-repro/` |
| **证据归因备注（本轮）** | MANIFEST 的 commit 链路与二进制 | ✅ 记录 | `git log` / `git describe` / `git reflog` | run 的二进制 `747995a0` 构建于 `96f9406`；收集时本地 HEAD = `1c411bb`（= `bba0e57` + `.gitignore` 一行 `.ua/`，系本地 amend 产物）⇒ 已打 tag **`evidence-t2.3-soak-2026-09-25`** 固定该 commit（保证 MANIFEST 引用可达），并以 `e8c9687` 在主线落等价变更；两次提交间**无运行时代码差异** |

> **W3-8 备注（参数台账）**：`backfill-param-confirmation.sh --check` ⇒ 38 份归档、待填 11。
> 本轮**不批量回填**（观察：已回填的 24 份都是 ①–⑧ 参数时代的面；待填 11 份含 mq/cache/idgen/registry
> 等使用 §5.4 ⑨–⑫ **未确认参数**的 run ⇒ 回填口径应随确认单更新一起裁定）。本轮新增的两份归档
> （使用面 map/watch/lease，属 ①–⑧）同样留待统一回填。
>
> **2026-09-26 更新（第十三轮按上述裁定执行）**：46 份归档，复核 **待填 4 / 已回填 39 / 无该字段 3**。
> 回填 15 份（①–⑧ 覆盖面：F-05 ×4、idgen/registry ×4、T2.3 浸泡/诊断 ×5、lease pause/partition ×2）；
> **mq ×4 保持待填**（⑨–⑫ 未确认 ⇒ 不得用 ①–⑧ 链接超范围背书，本轮曾误全量回填、已回退并留痕）。
> W3-8 余项收敛为两条"人签"：**③**（引入方团队）+ **⑨–⑫ 处置裁定**。详见
> `docs/production/evidence/PARAM-CONFIRMATION.md` 的 2026-09-26 更新段。
>
> **U-13（lease 活性窗口）备注**：F-27 的矩阵残余（`partition-halves` 下 4 条）仍是**待裁定的
> 活性窗口口径**问题；但本轮 soak 的 3 条 `:lease-not-expired` 与之**不同源**（发生在 F-69 的
> 全集群失能窗口内）。U-13 的实施不因本轮结果降级，也不得用来解释本轮 run。
>
> **副观察（一条容易再次踩的坑）**：push 跑的 job 列表里有一条 `Security audit`
> （`/actions/runs/{id}/jobs` 可见，但其 `html_url` 指向独立路径），而它**不在本仓
> `.github/workflows/`**（只有 ci.yml / contract-check.yml / release.yml 三份）；
> 2026-09-25 的 schedule 跑里也没有出现 ⇒ 判定为**仓库外部的 check**（组织级/平台侧配置），
> 不属本仓工作流。记在这里，避免下次把它当成"未知 job"重新分诊。

---

### 第九轮追加（2026-09-25）—— 主题：**定义级变更：移除第三方审计门槛（U-14，无经费）**

> 触发：决策方 2026-09-25 明确「我们没有经费进行独立（审计）」⇒ 按本文文末约定
> （修订 §4 生产门定义 = 修订"生产可用"的定义，须记录裁定人与日期），本轮把 **U-14**
> 一次性落地：计划书定义面 + 接入手册 + ops 文档 + 白皮书 §12 口径，并把**反向判据**
> （不得暗示"已审计"）升级为 P9 卡口的第 5 条机械判据。

| # | 任务 | 状态 | 判据（已跑的命令） | 结果 / 产物 |
|:--|:--|:--|:--|:--|
| **U-14（§8.7）** | 移除第三方审计门槛（P6 / §9-4 / W4-3 / R-02 / M2·M6 / §1.3·§1.4 / §10 / 附录 A） | ✅ 裁定 + 落地 | `grep -rn "第三方审计\|第三方安全审计\|独立审计" docs/ apis/ README*.md --include='*.md'` 逐条复核语境 | 全部保留命中均为**裁定记录 / 「未经独立审计」边界 / 历史注记**（带"已按 U-14 移出"字样），**无一处**再把第三方审计当**当前门槛**；裁定原文见 §8.7（写明"这是降低门槛的显式取舍，不允许作为移除其他门的先例"） |
| **口径同步（五份文本）** | `adopter-playbook.md` §4/§6、`ops/security.md` §2.3、`ops/governance.md` §5、`gate-drills-2026-09-21.md` §4、`WHITEPAPER.md` §12-2 | ✅ 完成 | 同上的反向判据扫描 | 接入手册新增「可以说：未经独立第三方审计」「不可以说：经第三方审计」；白皮书 §12-2 由"审计缺失"改为"不再作为验收门槛 + 任何声明不得暗示已审计"；`ops/security.md` 的 P6 红因更新为"仅余 TLS fail-closed" |
| **反向判据机械化（判据 5）** | `check-promise-consistency.sh` 增至**五条判据**：含「第三方(安全)审计 / 独立审计」的行必须带边界语境词（未经/不再/不采买/移出/已按/不得） | ✅ 完成（含负控制） | 基线 ⇒ **exit 0**；向 `WHITEPAPER.md` 注入裸声明「本平台已通过第三方安全审计。」⇒ **exit 1**（`[P9/U-14] WHITEPAPER.md:578 …`，且只报这一条）；还原 ⇒ **exit 0** | 脚本注释同步为五条；`gate-drills-2026-09-21.md` §4 登记"第九轮增至五条" |
| **卡口复核（九轮）** | 本轮只有 `.md` + 一个 `.sh` 改动（**无 Rust 代码**） | ✅ 完成 | wire-sync / wire-descriptor / sdk-sync / error-code 四道非 cargo 卡口 + `check-promise-consistency.sh` + `check-gate-drills.sh` | 六道全 `exit 0`；`check-panics.sh`（需 clippy）因无代码改动未重跑 |
| **CI 实证（`73b86f7`）** | 本轮改动的 CI 验证 | ✅ **全 job success** | run **`36154751619`** 逐 job（push 事件，共 11 job）；另有 `contract-check` run **`36154751497`** = success | **11/11 全绿**：`fmt+clippy` / `workspace tests` / **`gate self-check`（含新增第 5 条判据的脚本）** / **`real-process chaos` success（15:31:35→15:57:10 ≈ 25m35s）** / `cargo audit + deny` / `proto contract` / `java sdk` / `java example integration` / `plugin matrix` / `frontend lint`；`weekly perf baseline` skipped（push 不跑）。⚠️ chaos 本次偏慢（≈25 分钟）但**非卡死**——后续看到"最后一个 job 久不完"先拉 `/jobs` 看 started_at，勿过早判卡 |
| **推送备注（网络）** | `73b86f7` 的推送前两次失败（`curl 28` 连接 github.com:443 超时 133s/135s），第三次成功 | ✅ 记录 | `git push` ×3；成功时 `21dcca2..73b86f7` | 与第八轮 `e8c9687` 同类（github.com 直连间歇中断）⇒ 推送失败先 `getent hosts github.com` + TCP/curl 探测，网络恢复立即重试即可，**不需要改 remote** |

> **本轮的口径要点（写给后续轮次）**：U-14 之后，安全的对外表述只有两种合法形态 ——
> ①「未经独立第三方审计」（边界）；②「已按 U-14 不采买 / 移出门槛」类历史或裁定记录。
> 任何第三种（"已审计 / 已通过审计 / 独立验证"）都会被判据 5 置红。**"不做审计"≠"没有风险"**：
> 残余风险的承载物是边界文本 + §9-4 的反向 No-Go，不是沉默。

**第九轮未触碰**：所有代码面（W1 剩余项、W2-3/W2-4、W3 长跑与 F-69 修复）、§1.3 的案 A/B 人力裁定。

---

### 第十轮追加（2026-09-25）—— 主题：**F-69 根因修复（复制路径 fatal 形态闭环）+ 2h soak 现场复跑**

> 触发：第八轮 2h soak 判 invalid（F-69：kill 后 leader 判 fatal ⇒ 全集群写失败 85 分钟）。
> 本轮把 F-69 从「根因候选」推进到「代码闭环 + 进程内复现 + 现场验证」，并按 W3-1 同参数复跑。

| # | 任务 | 状态 | 判据（已跑的命令） | 结果 / 产物 |
|:--|:--|:--|:--|:--|
| **F-69 根因（代码级）** | openraft 在 `get_snapshot_builder()` 的**克隆**上执行 `build_snapshot()`（`core/sm/worker.rs` 的 `try_create_snapshot_builder` + spawn），而复制路径 `GetSnapshot` 走主实例；coord 此前把 `current_snapshot` **深拷贝**给克隆 ⇒ 构建结果对主实例永不可见 ⇒ 落后 follower 需要快照时 `get_current_snapshot()` 恒 `None` ⇒ `StorageError::read_snapshot(None)` ⇒ RaftCore **fatal** | ✅ 定位 | openraft 源码构造点 `snapshot_transmitter.rs:203`（唯一构造点）+ purge 守卫共享（tracker 是 Arc）而槽位不共享的对照 | 现场 9626 条 `INTERNAL: raft auth write failed: when Read Snapshot(None)` 全部可归因 |
| **F-69 修复** | `coord-server/src/raft/state_machine.rs`：①`current_snapshot` 改 **Arc 共享槽位**；②`build_snapshot` 经 `publish_snapshot()` **单调发布**（防迟到旧构建覆盖新装快照）；③`get_current_snapshot()` **磁盘兜底**（内存空时从 `META_SNAPSHOT`+SHA256 加载） | ✅ 代码完成 | ①单测 3：`cargo test -p coord-server --lib raft::state_machine`（修复前 2 红 → 修复后 3 ✓）；②集成 1：`cargo test -p coord-server --test snapshot_visibility_test`（修复前 60s 超时红 → 修后 0.14s 绿）；③**三节点复现**：`cargo test -p coord-server --test replication_snapshot_f69_test`（隔离 follower ⇒ purge 越位 ⇒ 断言不失能+恢复后经 snapshot 追上；修复前 **2.35s 复现 fatal**，修后 ~10s 绿、连跑 3 次稳定） | commit `eb91d58`；全量复核：coord-server lib 594 ✓ + 全部集成目标 ✓、m0 3 ✓ |
| **mapck 次生（F-68 同族）** | delete 形状断言只对 `:ok`（`:fail` 无响应字段；现场 256 条 `:fail` 曾被误判 `:delete-response-inconsistent`） | ✅ 完成（双向负控制） | control 内 `run-checker-tests.clj jepsen.coord.mapck scripts/map-fixtures '{:mode :index}'` | 新 13/13；**旧 mapck 12/13（新 fixture 必红）**——修分类不放宽 |
| **lab 工具陷阱** | `make binary` 只在二进制**缺失**时构建（`test -x \|\| cargo build`）⇒ `make upload` 静默沿用旧二进制（实测修 F-69 后 upload 仍报旧 hash `747995a0`） | ✅ 已发现（修复待提交） | `ls -la target/release/coord` + sha 对比 | 规则：lab 前显式 `cargo build --release -p coord` 并核对 hash；Makefile 修复随下个提交 |
| **CI 实证（`eb91d58`）** | run `36161178888` 逐 job | ✅ **全 job success** | `/actions/runs/36161178888/jobs` | `fmt+clippy` / `workspace tests` / `gate self-check` / `cargo audit + deny` / `proto contract` / `java sdk` / `java example integration` / `plugin matrix` / `frontend lint` / `real-process chaos`（16:30→16:59 ≈ 29m）全 success；perf skipped（push 不跑）。⚠️ 本轮 CI 构建偏慢（cold cache，Build tests ≈ 13–15 min），非门禁异常 |
| **W3-1 现场复跑（F-69 验证）** | 2h `soakfull` 同参数（SEED=42；16:40:35→18:44:29 UTC） | ✅ **F-69 场景闭环** | `make soakfull … SOAK_MIX='map=20,watch=40,lease=40'`；二进制 `8dd7f766` @ `eb91d58`；归档 `20260925T185131Z-t2.3-m2-watch-lease-2h-f69-fixed`（MANIFEST 工作树 **clean**） | kill n1（leader，17:13:32）→ 旧 run 的 fatal 窗口（kill 后 ~7.5min、第二次快照 9999、purge≈9000）本轮 **三节点 `snapshot not found` 全 0、客户端全时段 `:fail` 0**；n1 于 17:24 重启后 `Installed snapshot persisted to …/snapshot-9999-2.snap`（17:24:02）——快照成功传输并安装。门槛：gates **valid**、rto-p95 0.295s、**rto-unrecovered 0**、quiet worst-ratio **1.0**（旧 run：0.0/失能 2） |
| **W3-1 verdict** | 🔴 **仍 invalid：`:violations-by-class {:lease-not-expired 4}`** | 🟡 已归因（两个独立面） | 逐条时间轴 + 日志对照（见 `coord-findings.md` §18） | ①**F-70 [P1 候选]**：failover 窗口 `ka/7636`（grant+put 均 ok，**首次 keepalive 即 NOT_FOUND `lease 1 not found`**，其后 41 次读全成功 Key 始终在 ⇒ 真实 Key 泄漏）；佐证：n3/n2 上任时 `LeaseManager rebuilt … 0/3 leases`、lease id 轨迹 failover 后由 1536 跌回 1。②**F-71 [P2]**：17:45:36-44 两条违规为**观测缺口**（CCT 1h TTL 集中过期波：`auth/service.rs:593` `exp = now+3600`；9 条 `CCT rejected` 重认证；leaseck 轮询把 26/27 次失败读只计 `:failed` 不触发重认证）。**结论不变**：红灯面已从「全集群失能」缩小到「failover 窗口的 lease 语义 + 认证观测口径」 |
| **卡口复核（十轮）** | fmt / clippy（非测试目标）/ 六道脚本 | ✅ 完成 | `cargo fmt --all -- --check`；`cargo clippy --workspace -- -D warnings`；六道脚本 | 全绿 |

**第十轮未触碰**：F-70 修复（须先复现）、F-71 checker 口径、W2-3/W2-4、W3 其余项（M3/M4）、W4-1 TLS fail-closed、§1.3 案 B 裁定。

> **第十轮的口径要点**：F-69 从「P0 候选」转为「修复 + 现场验证 + 进程内复现判据（2.35s 红/10s 绿）」
> —— 长跑暴露的失能面**已消失**；W3-1 仍未收口，但剩余红灯全部集中在 **failover 瞬间的
> lease 语义**（F-70）与 **认证过期波导致的观测缺口**（F-71）。**下一轮第一优先 = F-70 复现
> 与修复**（含 failover 后租约列表/keepalive 自洽/到期级联删除三判据），F-71 同期修 checker
> 口径；两者闭环后按同参数重跑 2h soak（判据作废重跑纪律，见 §6.2）。

---

### 第十一轮追加（2026-09-26）—— 主题：**F-70 根因修复（failover 窗口 lease 语义闭环）+ F-71 checker 口径；T2.3 2h soak 复跑首次 `valid`**

> 触发：第十轮把 W3-1 的红灯面收敛到「failover 瞬间的 lease 语义（F-70）」与
> 「认证过期波的观测缺口（F-71）」并定为本轮第一优先。本轮把 F-70 从
> 「三个可疑面」推进到**唯一根因 + 三处修复 + 进程内判据**，再修 F-71 的
> checker 口径，然后按「判据作废重跑」纪律同参数复跑 2h soak。

| # | 任务 | 状态 | 判据（已跑的命令） | 结果 / 产物 |
|:--|:--|:--|:--|:--|
| **F-70 根因** | `rebuild` 的「先清空再装载」抹掉在飞 Grant | ✅ 定位 | 竞态推演：grant 先插本地记录/后入 raft ⇒ reconciler 快照（apply 前读）缺该租约 ⇒ `clear()` 抹掉本地 TTL 调度依据 | ① KeepAlive 本地查不到 ⇒ 立即 `NOT_FOUND`；② 过期任务不再调度 ⇒ 绑定 Key 直到下一次 failover 前**永不删除**（现场 `ka/7636` 的 41 次读均见 Key） |
| **F-70 修复（三处）** | ①`rebuild` 合并语义（只增不删，两视图都有取更晚 deadline）；②reconciler 装载前加 `ensure_linearizable_barrier`（失败下 tick 重试）；③KeepAlive 本地缺失时 `resolve_keepalive_ttl` 以状态机补水（`rehydrate`；已过期⇒泄漏自愈；屏障/存储失败⇒UNAVAILABLE 不伪装 NOT_FOUND） | ✅ 代码完成 | `cargo check` / `cargo fmt` / `cargo clippy --workspace -- -D warnings` | commit `109b462` |
| **F-70 判据（进程内）** | 三单测 + 两节点级判据（含负控制） | ✅ 全绿 | `cargo test -p coord-server --lib lease::`（15 ✓）；`cargo test -p coord-server --test lease_raft_test`（5 ✓） | 单测：在飞 Grant 不被抹+到期仍上报 / 合并取更晚 deadline / rehydrate 自愈；节点级：重建竞态后 keepalive 自洽+级联删 Key、状态机有记录/本地缺失时补水+泄漏自愈；负控制=还原旧 `rebuild`/去补水路径 |
| **F-71 修复** | client.clj 轮询 `:unauthenticated` 先刷新会话重试（记 `:ok-reads/:reauths`）；leaseck.clj `poll-unjudged?`（ok-reads=0 ⇒ 不判活性，计入 `:liveness-unjudged`） | ✅ 完成 | `make -C jepsen/lab checkers JEPSEN_PROVIDER=docker`（全部套件） | lease **12/12** + sample **2/2**；新增 3 fixture（其中 `expect-valid-unjudged-all-poll-reads-failed` 在旧 checker 下必红）；全 lab checker 套件 0 FAIL |
| **本地卡口（十一轮）** | fmt / clippy / 六道脚本 / 相关套件 | ✅ 完成 | `cargo fmt --all -- --check`；`cargo clippy --workspace -- -D warnings`；6 脚本；`-p coord-server --lib` 596 ✓、`--test region_lease_test` 3 ✓、`--test restart_recovery_test` 4 ✓ | 全绿 |
| **CI 实证（`109b462`）** | push run **`36206091021`** 逐 job | ✅ **全 job success** | `/actions/runs/36206091021`（00:46:49Z→01:16:03Z） | 11 success + `weekly perf baseline` skipped（push 不跑，符合 `if: schedule` 设计）；含 `workspace tests`（→01:05:35Z）与 `real-process chaos`（→01:16:02Z） |
| **W3-1 / T2.3 复跑** | 同参数 2h `soakfull`（SEED=42；00:55:38→03:00:xx UTC） | ✅ **valid（首次）** | `make soakfull …`（参数与第十轮逐项相同）；二进制 `ff25fdd6` @ `109b462` | 证据 `docs/production/evidence/20260926T030216Z-t2.3-m2-watch-lease-2h-f70-fixed/`（MANIFEST 工作树 **clean**、`sha256sum -c` OK）：grants **5699** / expiries **2845**（by-scenario：ttl 2845 + revoke 1476 + keepalive 1378）；**`violations-by-class {}`**、`liveness-unjudged 0`、`keepalive-stream-errors 0`、keepalive-responses 8268；gates **valid**、rto-p95 **2.57s** / **unrecovered 0**、quiet worst-ratio **1.0**；nemesis：kill n1 01:28:28→重启 01:39:12、pause n3 02:06:55→02:16:01、partition 02:48:00 |
| **现场对账（F-70/F-69）** | store 归档的 n1–n3 `coord.log` | ✅ | `grep -ac` 逐节点 | **`keep-alive failed` 三节点全 0**（第十轮 `ka/7636` 形态消失）；`when Read Snapshot(None)` / `snapshot not found` **0**；failover 后 1s（01:28:29）n2 走 `rebuilt from state machine (merge, F-70) tracked=2`；无 `lease rebuild deferred`（屏障一次通过） |

> **本轮口径要点**：T2.3（W3-1）从第十轮的 invalid（`:lease-not-expired 4`）
> 转为 **valid（violations 空集）** ⇒ F-70/F-71 双双 closed；W3-1 的「2h 归档 +
> 门槛达标」判据达成。**72h（W3-6）与 ≥14 天（W3-7）仍未执行**，W3 其余分面
> （M3/M4/M5）不受本轮影响；§5.4 ③ 参数确认仍未签（W3-8）。
>
> **本轮自身效力**：归档 MANIFEST 工作树为 **clean**、commit 与二进制哈希可溯源；
> 但按 §2.3/§7 的既有边界，依赖 ③ 的门禁结论仍「不得用于引入评审」——
> 本轮不改变该边界。

**第十一轮未触碰**：F-71 修法方向 2（客户端主动提前重认证）、W2-3/W2-4、
W3-2…W3-9、W4-1 TLS fail-closed、§1.3 案 A/B 人力裁定。

---

### 第十一轮（续）追加（2026-09-26）—— 主题：**matrix-m2 全绿（M1 出口判据之一）+ F-72 口径修复（pause 窗口）**

> 触发：拟闭 M1 的「`matrix-m2` 由红转绿」判据，先复跑 matrix-m2 ⇒ 在 F-70
> 二进制上 `lease / pause` 格判红（3 条 `:lease-not-expired`）。定位结论：
> **服务端行为正确**（新 leader 接管后 sub-second 提交 revoke），红灯来自
> **判定口径**三类缺陷（与 F-71 同族），立 **F-72** 并修复。

| # | 任务 | 状态 | 判据（已跑的命令） | 结果 / 产物 |
|:--|:--|:--|:--|:--|
| **F-72a 观测断档** | lease 观测读默认 5000ms 超时，被 SIGSTOP 节点钉死 5s ⇒ 窗口内零观测 | ✅ 修复 | `p/call` 单节点超时 500ms（`lease-point-read`，仅 lease 观测路径）；checker 新增 `observation-truncated?` | 客户端：`lease-read-timeout-ms=500`；checker：断档 ⇒ 计入 `:liveness-unjudged`（不判≠通过） |
| **F-72b 锚点** | ttl 场景窗口锚在 op 起点；pause 下 grant 自身花 2–4.6s ⇒ 窗口末端落在真实截止之前 | ✅ 修复 | workload 记录 `:grant-at-ms` 并以它起算轮询 deadline；checker 窗口同锚（旧数据回退 op 起点） | 首版有 `dl` 单位错误（相对毫秒当绝对 ns），由 run 的 `expiries=0` 当场暴露并修正 |
| **F-72c 迟发不可归因** | 读断档恢复后的「观测到消失时刻」≠「删除时刻」（ka/109：删除在 grant+2.1s，读数 13.7s 才恢复） | ✅ 修复 | 轮询有 `:failed` 读时迟到性不判，计 `:liveness-unjudged` | 豁免只针对迟到性；`absent=nil`（走 F-71/断档分支）与提前消失（安全侧）不受影响 |
| **服务端可观测性（P7/W5-4）** | pause 窗口「到底删没删」此前无法离线判定 | ✅ 落地 | 过期 worker：`leadership view changed` / `revokes committed`（含 lease_ids）；reconciler：`snapshot` 数 + `lease_ids` 样本；客户端：每次读仅归类一次 + `ok-reads/failed/reauths/last-ok-at-ms/last-ok-present?` | 本次三类口径缺陷全部靠这些字段在离线数据里定位（现场首见 `revokes committed lease_ids=[44,45]`） |
| **F-72 判据（fixture）** | 4 新增 + 1 更新（含双向负控制） | ✅ 16/16 | `make -C jepsen/lab checkers`（lease 档）；另 `jepsen.coord.client` 命名空间加载 | 新增：断档⇒valid（旧必红）、慢启动锚点⇒valid（旧必红）、读断档迟发⇒valid（旧必红）、慢启动晚删⇒invalid（防免罪）；更新 present-at-deadline 负控制（采样覆盖窗口末端仍必红） |
| **lease/pause 单元复跑** | 修复后稳定性 | ✅ **3/3 绿** | `make -C jepsen/lab test WORKLOAD=lease NEMESIS=pause TIME_LIMIT=45 …`（每轮 env-reset） | 3 轮 `violations-by-class {}`，`:liveness-unjudged 9/15/13`（如实计数） |
| **matrix-m2（M1 出口）** | 全 8 格 | ✅ **ALL M2 MATRIX PASSED** | `make -C jepsen/lab matrix-m2 JEPSEN_PROVIDER=docker`（exit 0；8 格逐格绿） | watch×{none,kill,pause,partition-halves} + lease×{none,kill,pause,partition-halves} 全绿；**第十轮的 U-13 残余（lease/partition-halves 4 条）与本次 lease/pause 均转绿** |
| **证据归档** | 两格归档 | ✅ | `collect-evidence.sh m2-lease-pause-fixed` / `m2-lease-partition-halves` | `docs/production/evidence/20260926T035534Z-m2-lease-pause-fixed/`、`…T035535Z-m2-lease-partition-halves/`（均 clean、`overall-valid: true`、`violations-by-class {}`）；旧口径红格诊断归档 `…T032009Z-m2-lease-pause-leader-freeze-diag/` |
| **CI 实证** | `12bf740`（F-72 代码）+ `44d4156`（台账） | ✅ **全 job success**（门禁 run `36216573898`，HEAD） | `/actions/runs/36216573898`（push；11 success + `weekly perf baseline` skipped，符合设计） | 含 `workspace tests` / `fmt+clippy` / `gate self-check` / `cargo audit + deny` / **`real-process chaos`（约 29 分钟，success）** 等全绿。⚠️ `12bf740` 自己的 run `36215852431` 被随后推送触发的并发组**取消**（superseded）——有效门禁以 HEAD 的 run 为准；另注：GitHub badge/shields 会拾取"最近一次完成的 run"而把被取消的旧 run 显示为 `failing`，**判读 CI 一律以 `/actions/runs/<HEAD 的 run>` 逐 job 为准** |

> **判据达成说明**：「matrix-m2 由红转绿」（§6.1 M1 出口之一）**达成**：
> 第十轮记 `7/8`（lease/partition-halves 红，U-13 残余）；本轮 8/8 全绿，
> 且原来那格的 4 条残余在 F-70/F-72 口径修复后**自然消失**（服务端行为一直正确）。
> 注意：F-72 的三类豁免必须与 `:liveness-unjudged` 一并阅读 —— **不判 ≠ 通过**。

**第十一轮（续）未触碰**：W2-3/W2-4、W3-2…W3-6/W3-7、W3-8/W3-9、W4、§1.3 裁定。

---

### 第十二轮追加（2026-09-26）—— 主题：**W1-2 闭环（F-05 登录路径 × quorum 窗口）**

> 触发：M1 出口的「F-27/F-05/F-68 闭环」还差 F-05。按 2026-09-21 修订后的判据执行
> （不再要求「0 条 `:no-client`」——quorum 整体丢失超过登录超时时该失败**固有**）。

| # | 任务 | 状态 | 判据（已跑的命令） | 结果 / 产物 |
|:--|:--|:--|:--|:--|
| **W1-2① 分类判据** | 两类失败可区分：密码错=UNAUTHENTICATED；无 quorum=UNAVAILABLE/DEADLINE_EXCEEDED | ✅ 已由测试钉住（复核） | `cargo test -p coord-server --lib -- wrong_password_yields_unauthenticated session_persist_failure_propagates` | 两测全过（`auth::service::cct_tests`，含负控制）；服务端 quorum 窗口路径映射为 `UNAVAILABLE`+leader hint / `DEADLINE_EXCEEDED`，不落 `INTERNAL` |
| **W1-2② 复跑统计与归因** | `kill-all ×3`（60s）统计 `:no-client` 出现率并归因 | ✅ **0/3 复现** | `make -C jepsen/lab test WORKLOAD=register NEMESIS=kill-all TIME_LIMIT=60 CONCURRENCY=1n`（每轮 env-reset；bin 工作树 = `109b462` + F-70/F-72） | REP1/2/3：全部 valid、**`:fail` 0 / `:no-client` 0 / 服务端 INTERNAL-auth 0**；登录成功 34/31/36。累计口径（含 09-21 的 1 run≈5 周期）＝ **4 runs≈20 kill-all 周期，0 出现**；代表 run 归档 `docs/production/evidence/20260926T045524Z-w1-2-f05-kill-all-60s/` |
| **边界（与结论同引）** | —— | —— | —— | 残余是**固有可用性属性**（quorum 丢失 > 登录超时 ⇒ 登录必失败），属参数裁定项（与 U-13 同族），非缺陷；出现率上界 <1/4 run（本 config）；原经 agent 的 M5b MQ 形态不在覆盖内，若再现按「分类是否被破坏」分诊。详见 `coord-findings.md` F-05 收口记录 |

> **M1 出口进度（2026-09-26 复核）**：`matrix-m2` 由红转绿 ✅（第十一轮（续））；
> F-27 ✅ / F-68 ✅ / F-05 ✅；**W1-1…W1-7 已全部完成**（W1-4/W1-5 见第四轮台账
> §11:598-601，W1-6/W1-7 见同轮 :623-624）⇒ **M1 出口三条全部达成，M1 达成**
> （早于 10-24 里程碑日）。**M0 亦达成**：W2-3 第十三轮完成（16 红分解 + 判定流程）；
> W2-4 于 2026-09-26 由 admin 启用（读数与直推被拒实证见 forensics §4/§7 与 §11 第十四轮）
> ；后续按 §6 推进 W3 覆盖（含 10-31 硬期限的
> idgen/registry lab 产物）与 W4。

**第十二轮未触碰**：W2-3/W2-4、W3、W4、§1.3 裁定。

---

### 第十三轮追加（2026-09-26）—— 主题：**W2-3 chaos 红全量取证 + 浸泡写判据修正**

> API token（`$GHT`）可用后，历史 job 日志（保留期内）可下载 ⇒ §3 的"日志 403
> ⇒ 无法判定"解除。本轮把 W2-3 从"范围收窄"推进到"**16 红逐条分解 + 判据修正 +
> 本地验证**"；全部细节与可复现命令见 `ops/ci-gate-forensics-2026-09-22.md` **§7**
> 。

| 项 | 状态 | 判据 / 证据 | 说明 |
|:--|:--|:--|:--|
| **W2-3 取证：分布分解** | ✅ 完成 | forensics §7.2：最近 60 次 CI run（09-12…09-25，chaos job 口径）= success 34 / **red 16** / cancelled 8（并发取代，非红）；16 红原文日志分解：**基建 7**（09-12 半天内的 `dtolnay/rust-toolchain` 红，日志含该 action 自带的 spurious-network 包装步骤）＋**plugin e2e 4**（09-13 当日已修的 `missing CCT token` 形态）＋**agent+auth 2**（F-05 形态，已由 W1-2 收口）＋**soak 3**（下条） | 此前"14/31 残余分布未分析"项**就此关闭**；每条归属都有下载到的日志原文 |
| **W2-3 取证：3 次 soak 红** | ✅ 形态判定 + **口径修正** | 三次均为 `chaos_soak_distributed` @ `chaos_real.rs:562`（`soak put failed at iteration 793/958/784`）；**无注水**、**单 pass 零重试**（三节点各一次机会）、失败于 196–240s；同 run kill9 套件绿、50 次收敛检查从未触发。修正：新增 `soak_put()`（3 pass × 250ms，瞬时失败**可见不判红**，超窗口 panic 带全部错误串＋快照＋瞬时报数）；本地验证 45s ⇒ `writes=194 transient_retried_ok=0` ✓ | 三次红早于"逐节点错误串"诊断落地 ⇒ **不声称**根因（产品瞬断 vs 环境抖动）；新口径下判红 = "**重试窗口内仍写不进**" |
| **判定流程（W2-3 产物）** | ✅ 完成（取代旧 §3.3） | forensics §7.5：失败 step × 签名 → 归因表；**无法归类必须显式登记**（禁止"偶发"结案）；绿色 run 的 `transient_retried_ok > 0` 也要登记 | 门禁可诊断性 + 观察义务 |
| **W3-8 台账（参数确认）** | ✅ 15 份回填 / 4 份保持待填 | `backfill-param-confirmation.sh --check` ⇒ **待填 4 / 已回填 39 / 无该字段 3**；`PARAM-CONFIRMATION.md` 2026-09-26 更新段 | 按**第十轮裁定**拆分覆盖面：①–⑧ 覆盖的 15 份回填；**mq ×4 因 ⑨–⑫ 未确认保持待填**。本轮曾误全量回填（含 mq），同一会话内**回退 4 份 + 重算校验和**，两处文档均留痕 |
| **CI 实证（`f20501c`）** | ✅ **全 job success** | run **`36219956592`**：`fmt+clippy` / **`real-process chaos`** / `workspace tests` / `gate self-check` / `cargo audit + deny` / `proto contract` / `java sdk` / `java example integration` / `plugin matrix` / `frontend lint` / `Security audit` 全 success（`weekly perf baseline` skipped：push 不跑）。该 run 同时覆盖 `120474c` 的 soak 判据改动 | **观察义务（§7.5）首次执行**：chaos 日志两处 `soak summary` ⇒ `writes=1305 transient_retried_ok=0`（300s 档）与 `writes=492 transient_retried_ok=0`（120s 档）⇒ **零瞬时失败**，登记表无新增 |
| chaos 稳定性宣布 | 🟡 **仍不宣布**（n=12） | 最后一次 chaos 红 = `cab765a`（09-24，F-05 形态）；其后**连续绿 n=12**（9 次窗口内 + `109b462` + `44d4156` + `f20501c`；cancelled/取代不计） | n 仍不足门槛；其中 9 次早于 W1-2 修复；`f20501c` 是**soak 判据修正后**的首个 green（两档 soak 零瞬时失败） |

| **W2-4（分支保护）** | ✅ **关闭（2026-09-26 实测）** | `GET /branches/main/protection` 读数：`enforce_admins=true`、`strict=true`、必需检查 3 条（`fmt + clippy -D warnings` / `workspace tests` / `proto contract`）、无必审 review；**负控制**：直推 `main` ⇒ `GH006 Protected branch update failed`（正是计划书要求的验证判据） | 自本轮起所有变更走 **PR + 必需检查**；`docs/production/ops/ci-gate-forensics-2026-09-22.md` §4 已更新 |

**第十三轮未触碰**：W3、W4、§1.3 裁定。

---

### 第十四轮追加（2026-09-26）—— 主题：**W2-4 分支保护落地（M0 最后一项）+ W4-1 TLS fail-closed 落地（P6 余项）**

> 触发：拿到 **repo admin token**（`$GHT`，不落盘/不打印）⇒ M0 唯一未完成项（W2-4）
> 可执行；同时按 §5 W4 推进 P6 的最后一块（TLS fail-closed）。**本轮起 `main` 有分支
> 保护**：交付流程从"直推"改为"PR + 必需检查 + merge"（见下表与 forensics §4）。

| # | 任务 | 状态 | 判据（已跑的命令） | 结果 / 产物 |
|:--|:--|:--|:--|:--|
| **W2-4** | 分支保护设置 + 负控制实证 | ✅ **完成** | `PUT /branches/main/protection`（`strict=true` + 3 必需检查 + `enforce_admins=true`）；真实直推（从未被 CI 验证过的空提交）⇒ **`GH006: Protected branch update failed` / `3 of 3 required status checks are expected` / `protected branch hook declined`，exit=1** | 3 个 context 与 check-run 名逐字一致（`fmt + clippy -D warnings` / `workspace tests` / `proto contract (buf lint + breaking)`，app=actions，`app_id=15368`）；**与原拟命令的差异**：`enforce_admins=false → true`（token 属主 `admin=true`，`false` 下可按 GitHub 语义绕过直推 ⇒ 与"直推被拒"判据互斥；取更强且无锁死）。⚠️ **`--dry-run` 不构成证据**（dry-run 不触发服务端评估，实测 exit 0 假绿）。证据原文见 forensics **§4** |
| **W4-1** | **TLS fail-closed 落地**（P6 最后一块） | ✅ **代码完成（正反双向全绿）** | `cargo test -p coord --test plaintext_remote_failclosed_test -- --test-threads=1` ⇒ **4 ✓**（拒绝含 `R-SEC-04`+逃生阀名 / 逃生阀正例 / TLS 正例 / loopback 豁免）；回归 `raft_sec03_failclosed_test`+`dev_insecure_bind_test`+`auth_enforcement_test`+`cli_tls_test` ⇒ **9 ✓** | 新规则：`auth_enabled=true` + gRPC 非 loopback + 无 `tls_cert/tls_key` ⇒ 拒绝启动；逃生阀 `security.allow_plaintext_remote`（默认 false，启用时 WARN）；三处 fail-closed 判定统一 `is_non_loopback_bind()`。**lab 以显式逃生阀保持证据链**（`db.clj` + 注释）；**lab mTLS 登记为后续强化项**（不影响 P6 判据）。口径同步：README×2 / `config.example.toml` / `boundaries.md` B-SE-1 / `runbook.md` / `security.md` §1.4 / `adopter-playbook.md` §4 |
| **W2-5（扩项）** | 负控制演练再补两道：**P6**（TLS fail-closed 拒绝启动）+ **P8 仓库设置面**（直推 ⇒ GH006） | ✅ 完成 | 同 W2-4 / W4-1 两行；演练记录 `gate-drills-2026-09-21.md` §4.5 | 剩余：P3–P5（lab 组）与 P8 的**真发布**面（随 W7-3 做） |
| **卡口复核（十四轮）** | fmt / clippy（非测试目标）/ 六道脚本 | ✅ 完成 | `cargo fmt --all -- --check`；`cargo clippy --workspace -- -D warnings`；wire-sync / wire-descriptor / sdk-sync / panics / error-code / gate-drills | 全绿；panics 0 violations |
| **交付流程变更** | `main` 启用保护后的纪律 | ✅ 记录 | 本轮回写起改走 **PR 流程**合入（push 分支 → PR → 必需检查 → merge） | PR 号与检查结论随下轮回写 |

> **M0 状态更新**：W0 ✅、W2-1…W2-4 ✅（W2-3 见第十三轮、W2-4 见本轮）⇒ **M0 除 W2-5 的
> lab 组（P3–P5）与 P8 真发布面外全部达成**；两项均绑定 W3 长跑 / W7 发布窗口。§6.1 的
> M0 出口行已同步更新。

### 第十五轮追加（2026-09-26）—— 主题：**W3 预飞（F-73/F-74：M5a lock checker 判定修复）+ 四矩阵与 9 面冒烟全绿；T6.1 72h 起跑**

> 触发：W1 出口（M1）已达成、功能判「完备」，按「尽快启动 soak」进入 T6.1 预飞。
> 预飞矩阵首跑即暴露 `matrix-m5` 红 —— 根因不在产品面，而在 **M5a lock checker 的
> 两处判定缺陷**（F-73/F-74，均为 checker 侧修复，产品二进制不变）。修复后 m5 首度
> 全绿，全部预飞项通过；72h 起跑参数在表内**预注册**。

| # | 任务 | 状态 | 判据（已跑的命令） | 结果 / 产物 |
|:--|:--|:--|:--|:--|
| 预飞-1 | 二进制与布置 + 门禁 | ✅ | `make binary`（`81955cc2` @ `ed15ea7`）；`make upload`；`make checkers`；`make quick` | 全绿（W4-1 TLS 逃生阀在 lab 生效） |
| 预飞-2 | `make matrix-m2` | ✅ 全绿 | 8 格（watch/lease × none/kill/pause/partition-halves） | `ALL M2 MATRIX PASSED`；lease `violations {}`；liveness-unjudged 13/15/14（kill/pause/partition 格） |
| 预飞-3 | **F-73 / F-74 修复** | ✅ 修复 + 全量验证 | lock-agent fixtures **9/9**（新增 3 个：F-73 正例 / F-74 正例 / 「顺序读读到丢失不得假绿」负控制）；两次现场红 run 离线重放 ⇒ 均 `valid? true`、`phantom-loss 0`（`clock-offset-ms 85840376` 实证两轴偏移） | `lockck.clj`：`rel->abs-offset-ms` + `abs-windows`（F-73：faultwin 相对轴 vs `:t0-ns` 绝对轴混用）；H2 双条件证据窗（F-74：并发读不得当顺序证据）；summary 增 `:clock-offset-ms`。文档：coord-findings **§19**、dev.md 教训 **#31/#32** |
| 预飞-4 | `make matrix-m5` | ✅ **全绿（首度）** | 9 格（lock/election/idgen/registry × none/kill-agent + lock/partition-agent-server；`LOCK_TTL_SECONDS=30`） | `ALL M5 MATRIX PASSED` |
| 预飞-5 | `make matrix-m5-diff` | ✅ 全绿 | 8 格（map:none、txn:none、watch:kill-agent、lease:none × direct/via-agent） | `ALL M5 DIFF MATRIX PASSED` |
| 预飞-6 | `make matrix-m1` | ✅ 全绿 | 9 格（map/txn/scan/mixture × none/kill + idempotency:none） | `ALL M1 MATRIX PASSED` |
| 预飞-7 | soakfull 冒烟（**9 面**） | ✅ 全绿 | `make soakfull`（900s、seed 42、`--agents 2 --via-agent --lock-ttl-seconds 30` + 保守门槛；显式 9 面 mix） | `Everything looks good!`；全部 14 项 `valid? true`（含 **idgen**）；routing 全 9 面见数（map 753 / txn 341 / watch 250 / lease 182 / lock 144 / scan 82 / idgen 73 / registry 41 / election 31）；样本：lease 182 grants / 74 expiries（unjudged 0）、watch events 536、idgen ids 376（distinct 376、regressions 0）、lock acquires 76（phantom-loss 0）、election campaigns 17、registry cycles 27 |
| **T6.1-72h** | **起跑（预注册参数）** | ⏳ 本轮合并后启动 | `make soakfull`：`SOAK_TIME_LIMIT=259200 SOAK_QUIET=1800 SOAK_DISRUPT=600 SOAK_RATE=2 CONCURRENCY=2n SEED=42`；`SOAK_MIX='map=40,txn=20,scan=5,watch=15,lease=10,lock=10,election=3,registry=2,idgen=5'`；`SOAK_EXTRA='--agents 2 --via-agent --lock-ttl-seconds 30 --watch-min-events 200 --lease-min-grants 100 --lease-min-expiries 30 --lock-min-acquires 200 --election-min-campaigns 50 --registry-min-cycles 30 --idgen-min-ids 100'` | 起跑于 tag `t6.1-72h-2026-09-26`；结果 + 证据归档随结项回写（第十六轮）。PR 号与 CI 结论随下轮回写 |

> **一个踩坑（记录）**：`soakfull` 默认 mix **故意**只含 8 面（不含 idgen；源码 docstring
> 明确「验收级长跑请显式传完整比例」）。首次冒烟用默认 mix 跑完后，从 `:routing` 面份额
> 发现 idgen 缺席 ⇒ 以显式 9 面 mix 重跑（上表预飞-7 为**显式 mix** 结果）。若直接以
> 默认 mix 起 72h，会得到一份「少一个面但全绿」的报告 —— 正是 `soakfull-mix` 注释中
> 「拒绝静默丢弃」要防的情形（这次由预飞兜住了）。

> **口径边界**：F-73/F-74 均为 checker 侧修复，不改变产品二进制与 lock 验收参数；
> §5.4-⑨（默认 ttl=5s < 续期节拍 10s）**仍未裁定**，lock 验收沿用 `LOCK_TTL_SECONDS=30`
> （即 soak 的 `--lock-ttl-seconds 30`，与 2026-09-19 M5a 验收参数一致）。watch 的
> `unjudged-items 45`（冒烟）按「不判 ≠ 通过」口径如实记录，72h 结项时同读。

---

## 附录 A：差距 → 门 → 判据 → 证据（追溯表）

| 差距 | 门 | 判据（可执行） | 证据落点 |
|:--|:--|:--|:--|
| D-01 | P1/P3/P4 | 新增 F-27 单测 + `make matrix-m2` 全绿 | `docs/production/evidence/<ts>-f27-*/` |
| D-02 | P1/P3 | 60s 短跑 0 条 `:no-client Failed to authenticate` | 同上 |
| D-03 | P4 | `CONCURRENCY=1n` 转绿 + 单客户端仍绿 | `<ts>-m5b-mq-*`（与既有两份并列引用） |
| D-04 | P1/P8 | 三 job 转绿 + 分支保护验证 + 负控制演练 | CI run + `docs/production/ops/gate-drills-<date>.md` |
| D-05/D-06/D-18 | P9 | 四份文本逐条一致 + 无悬空引用 | 四份 diff |
| D-07 | P5 | 72h + 14 天归档 | `<ts>-soak-72h/`、`<ts>-soak-14d/` |
| D-08 | P3/P4 | 各面 `matrix-*` 绿 + 逐面归档 | `<ts>-m5-<cap>/` |
| D-09 | P3 | 多 agent 拓扑下的 ISR/投递验收 | `<ts>-m5b-topology/` |
| D-10 | P3/P5 | MANIFEST worktree=`CLEAN`；参数台账 `待签 0` | 全部新归档 |
| D-11 | P6 | TLS 负控制 + KEK 裁定 + `cargo deny` + 「未经独立审计」边界声明（U-14） | `docs/production/ops/security.md` + 边界声明 diff |
| D-12/D-13 | P1/P5/P7 | runbook 演练 + 有界曲线 + 无界增长逐条处置 | `docs/production/ops/` |
| D-14 | P3/P9 | 默认开关逐行对齐表 + 代码 diff | 裁定表 |
| D-15/D-16/D-17 | P2/P8/P9 | tag + 制品 + 未决项回填 + `buf breaking` 绿 | release 页 + 本文 §8 回填 |

## 附录 B：与在先文档的关系

| 文档 | 关系 |
|:--|:--|
| `apis/contracts/WHITEPAPER.md` | **协议承诺**（接口层）。本文 §4.3 的 L0 = 白皮书口径；白皮书 §12.2 的悬空引用由本文 §4 接替 |
| `apis/contracts/STATUS.md` | 承诺台账（单一事实来源，脚本解析）。本文不新增台账状态位；W0-2 只对齐文本 |
| `docs/production/agent-ga-remediation-baseline.md` | agent 全量 GA 的 blocker 基线。其 B-01…B-16 已基本闭合，**剩余项**（V11 故障注入、72h、M3/M4、tag、U 项）已并入本文 §3 |
| `docs/coord-agent-ga-v0.2.0-plan-2026-09-19.md` | **不可入库**（`.gitignore:41`，实测 `git check-ignore` 命中）。本文不回引其内存；其 G1–G6 验收条在本文 §4.2 被引用并扩展 |
| `jepsen/docs/soak-closure-report.md` | soak 结项报告，明确不声明"通过"。本文 W3 是其 §5「未兑现部分」的执行计划 |
| `docs/production/remaining-known-gaps.md` | 第四轮整改的诚实清单。本文 §3 已并入其未闭合项（C22/§12/§13 等） |
| `docs/第三轮.md` / `docs/第四轮.md` | **不可入库**（本地工作文档，实测 `git check-ignore` 命中）。第四轮 §6.3「准入门槛」已被本文 §4/§10 吸收并适配到 17 项现状 |

---

*本文随 `docs/production/` 版本化（`.gitignore:49` 允许 `docs/production/*.md`）。
修订本文 §4 的生产门定义 = 修订"生产可用"的定义，须记录裁定人与日期。*
