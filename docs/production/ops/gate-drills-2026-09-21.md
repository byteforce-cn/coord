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

---

## §4 第八轮追加（2026-09-25）：P-Gate 9 首次拥有机械门禁（`check-promise-consistency.sh`）

> 背景：W2-5 的九道门里，P3–P6 需要 lab/审计、P8 需要仓库设置与真发布，**只有 P9
> 此前没有任何机械门禁**（「四处口径一致」全靠人工比对）。本轮把 P9 的可机械化
> 子集变成脚本 + CI 卡口 + 自带 self-check；它首跑即抓到 4 处**真实**缺陷。

判据（四条，纯文本 + python3，不需要 Rust/protoc）：

1. **免责口径（U-01）不得回归**：`README.md` / `README.zh-CN.md` 里**裸**声明
   （行内不含「取代 / 原先 / replaces / former」语境词的旧措辞）为零；
   且两份都必须链接生产化计划（承诺分级可接续）。
2. **五份对外文本无悬空引用**：所有仓库内相对 markdown 链接必须可达。
3. **契约版本三方一致**：`CHANGELOG.md` 最新条目 == `WHITEPAPER.md` 头部 ==
   `apis/contracts/README.md`。
4. **承诺面双向一致**：`STATUS.md` 的 COMMITTED（包名+期限）== contracts README
   承诺表；且 COMMITTED ∪ STABLE 的包名集合 ↔ `apis/contracts/proto/coord/`
   实际目录集合（承诺了没有 proto、或有 proto 没进台账，都置红）。

### 4.1 基线

```
$ bash scripts/check-promise-consistency.sh
promise-consistency OK：免责口径（U-01）未回归；五份文本无悬空引用；
契约版本 1.2.0 三方一致；COMMITTED 17 行 + STABLE 5 行 ↔ proto 目录双向一致。
exit=0
```

### 4.2 四组负控制（本地实测，逐一还原后复核）

| # | 注入 | 期望 | 实测 |
|:--|:--|:--|:--|
| A | 向 `README.md` 追加**裸**的 `not intended for production use` | 判据 1 置红 | `exit=1`（仅"裸声明"触发；裁定记录里的引用不触发） |
| B | `contracts/README.md` 的「契约 v1.2.0」改成 `v9.9.9` | 判据 3 置红 | `exit=1`：`[P9/版本] 契约版本不一致` |
| C | 删除承诺表里的 `coord.registry.v1` 行 | 判据 4 置红 | `exit=1`：`[P9/台账] COMMITTED 包名集合不一致` |
| D | 向 `STATUS.md` 追加 `[坏链接](docs/nope-not-exist.md)` | 判据 2 置红 | `exit=1`：`[P9/D-06] … 链接悬空` |

CI 侧：卡口接 `lint` job（"Promise ↔ docs consistency gate"）；self-check 接
`gate-self-check` job（注入 A 的形态 ⇒ 断言必红）。还原后四组均回到 `exit=0`。

### 4.3 门禁的第一次真实收益（抓到 4 处悬空引用）

基线首跑即红：**W0-4 的两处改动引入了 4 条坏链接**（从 `apis/contracts/` 出发写成
`../docs/production/...` / `../production/...`，正确解析应为
`../../docs/production/production-readiness-plan-2026-09-21.md`）。逐处修复：

| 文件 | 处数 | 形态 |
|:--|--:|:--|
| `apis/contracts/README.md` | 2 | 展示文本 + 目标路径均为 `../docs/…`（两行各一处） |
| `apis/contracts/WHITEPAPER.md` | 2 | 展示文本正确、**目标路径** `../production/…` 错误 |

> 这正是判据 2 存在的意义：链接**文本**看起来完全合理，只有**解析一遍**才知道是 404。

### 4.4 覆盖率更新（W2-5）

**6/9 → 7/9**：P1（fmt、panic 路径）/ P2（wire-sync、wire-descriptor、sdk-sync）/
P7（告警↔runbook）/ **P9（本轮新增）**。剩余 3 道：P3–P6（需 lab / 第三方审计）、
P8（需仓库设置 + 真发布）。
