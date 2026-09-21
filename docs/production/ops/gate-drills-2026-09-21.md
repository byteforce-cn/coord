# 门禁负控制演练记录（W2-5）— 2026-09-21

- **锚点**：`docs/production/production-readiness-plan-2026-09-21.md` §5 W2-5、
  §7 证据规范第 3 条（每个判据必须有负控制，正反两向都验）、第四轮 §6.2 B5⑥
- **体例**：每条演练 = **注入什么违规 / 期望 / 实测 / 结论**。
  演练对象是**门禁本身**，不是产品功能。

> **为什么必须先做这个**：W2 的定义是「本工作流不完成，后续所有『绿』不计入判据」。
> 一个从没红过的门禁，和没有门禁是同一回事（`remaining-known-gaps.md:249`
> 「红色的门禁不是严格的门禁，是教人忽略的门禁」）。下面每条都**真的红过**。

---

## §1 本轮新增门禁：`check-gate-drills.sh`（W5-3 告警↔runbook 绑定）

判据（三条）：
1. 每条 Prometheus 告警有非空 `runbook_url`；
2. 每个 `runbook_url` 的锚点在被指向的文档里**真实存在**（GitHub 锚点 slug 规则）；
3. `runbook.md` 的告警处置段里**没有孤儿小节**（每个小节都被至少一条告警引用）。

### 1.1 基线（未注入）

```
$ bash scripts/check-gate-drills.sh
check-gate-drills: 12 条告警，1 个 runbook 文档
OK: 每条告警都有可达的 runbook 处置入口，且无孤儿小节
exit=0
```

### 1.2 负控制 A：删掉一条告警的 `runbook_url`

| 项 | 内容 |
|:--|:--|
| 注入 | 删除 `CoordNoLeader` 的 `runbook_url` 行 |
| 期望 | 判据 1 置红 |
| **实测** | `exit=1`；报 `[W5-3] 告警 CoordNoLeader 缺少 runbook_url` + 孤儿小节 `CoordNoLeader — 集群 60s 无 leader` |

### 1.3 负控制 B：改掉 runbook 的小节标题（锚点失配）

| 项 | 内容 |
|:--|:--|
| 注入 | 把 `### \`CoordNoLeader\` — 集群 60s 无 leader` 改成 `### \`CoordNoLeader\` 集群无主` |
| 期望 | 判据 2 置红 |
| **实测** | `exit=1`；报 `[W5-3] 告警 CoordNoLeader 的锚点 #coordnoleader--集群-60s-无-leader 在 …runbook.md 里不存在` |

### 1.4 还原复核

```
$ bash scripts/check-gate-drills.sh
OK: 每条告警都有可达的 runbook 处置入口，且无孤儿小节
exit=0
```

**结论**：该门禁**双向可证伪**（缺绑定会红、锚点漂移会红、还原后绿）。

---

## §2 既有门禁的负控制（复核，非本轮新增）

| 门禁 | 负控制（既有） | 状态 |
|:--|:--|:--|
| **`cargo fmt --check`（CI `gate-self-check` job）** | 向 `coord-core/src/kv_range.rs` 追一行不合格式的 `fn`，断言 fmt gate 必红 | ✅ **既有已落地**（`.github/workflows/ci.yml` 的 `gate-self-check` job；第四轮 §6.2 B5⑥ 的落地） |
| `check-wire-sync.sh` | 契约↔实现路径级缺失 | 既有（CI `proto contract` job） |
| `check-wire-descriptor.sh` | descriptor 级漂移（service/method/field/enum） | 既有 |
| `check-sdk-sync.sh` | SDK↔契约第 4 道反向判据（G4） | 既有 |
| `check-panics.sh` | 生产代码里的 `unwrap/expect/panic/todo` | 既有（实测：新加 `.expect` 会让 `TOTAL: 2` 报错） |
| `check-error-code-contract.sh` | 错误码一致性 | 既有 |
| `check-gate-drills.sh`（**本轮新增**） | 见 §1 | ✅ **本轮新增，且已接 CI + 自带 self-check** |
| jepsen checker fixture | `expect-valid-*` 必须 valid / `expect-invalid-*` 必须 invalid | 既有（F-68 修复即用此负控制） |

### 2.1 本轮补上的：新门禁**自己也有 self-check**

`ci.yml` 的 `gate-self-check` job 新增第二步：「把 runbook 的小节标题改名 ⇒ 断言
`check-gate-drills.sh` 变红」。本地已按同一步骤实操验证：

```
$ bash scripts/check-gate-drills.sh
OK: 每条告警都有可达的 runbook 处置入口，且无孤儿小节
# 注入锚点失配后：
OK: alert-runbook gate correctly turned red on the injected violation
```

> 为什么这步重要：新门禁如果只加进 CI 而不带 self-check，就有可能重演 fmt 门禁当初
> 「写了但从未跑起来」的形态。见 §1 开头的说明。

---

## §3 剩余项（如实列出，不掩盖）

| # | 项 | 阻塞 |
|:--|:--|:--|
| 1 | 其余 5 道本地卡口（wire-sync / descriptor / sdk-sync / panics / error-code）**没有** CI self-check | 可在本机复现场景，但需 CI job 才有护栏价值；本轮只给 fmt 与新增的告警卡口加了 self-check |
| 2 | `weekly perf baseline` 13/13 常驻红的定位（W2-1） | 需 CI job 日志（本机 `gh` 不可用，job 日志端点 403） |
| 3 | `cargo audit + deny` 定时红根因（W2-2） | 同上 |
| 4 | `real-process chaos` 14/31 间歇红（W2-3） | 部分可在本机复跑（需 docker lab） |
| 5 | 分支保护（W2-4） | 仓库设置项，非代码可保证 |
