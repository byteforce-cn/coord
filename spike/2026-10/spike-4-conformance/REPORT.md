# Spike 4 · 规范符合性量化报告（CNCF Serverless Workflow DSL 1.0.3）

> Owner: Spike 执行（AI 协作者）｜ Last verified: 2026-10-13
> 判据：官方 1.0.3 JSON Schema（`schema/workflow-1.0.3.yaml`，sha256 `df3326da…`）
> 被检对象：coord `looks_like_cncf_sw` / `parse_cncf_sw_value` /
> `parse_and_validate_yaml`（与 coord-agent 部署分发一致，`workflow.rs:2137`）
> 原始数据：`evidence/parser-results.json`、`evidence/schema-results.json`、
> `evidence/matrix.tsv`、`evidence/summary.txt`

## 1. 摘要

24 份语料（**22 份 schema-合法 + 2 份 schema-非法**：`19-legacy-states-0.8` 为 0.8 遗留形状、`20-invalid-no-document` 为故意非法）的扫描结果：

| 判定 | 数量 | 含义 |
|:--|--:|:--|
| PASS（schema 合法 & 解析通过） | 19 | 其中 **6 份解析成功但语义被静默误读**（见 §4） |
| GAP（schema 合法 & 解析拒绝） | 3 | `switch.then`、函数引用、`call: openapi` |
| LAX（解析接受 & schema 非法） | 1 | 0.8 `states/start` 遗留格式（有意兼容） |
| REJECTED（双方拒绝） | 1 | 缺 `document` 的非法文档 |

**最重要发现不是「拒绝」而是「静默误读」**：`run: script|container|shell|workflow`
被无一例外编译成 `run_subflow`，且 `workflow_ref` 三段全为空字符串，**无任何报错**；
`use.extensions` 被接受后静默丢弃（`before`/`after` 任务不进入执行列表）。

## 2. 方法与复现

```bash
# 1) 解析扫描（真实分发路径）
cargo run -p spike-conformance -- spike/2026-10/spike-4-conformance/corpus \
  > spike/2026-10/spike-4-conformance/evidence/parser-results.json

# 2) 官方 Schema 校验（Draft 2020-12，python jsonschema）
python3 spike/2026-10/spike-4-conformance/scripts/schema_check.py \
  spike/2026-10/spike-4-conformance/schema/workflow-1.0.3.yaml \
  spike/2026-10/spike-4-conformance/corpus \
  > spike/2026-10/spike-4-conformance/evidence/schema-results.json

# 3) 合并矩阵
python3 spike/2026-10/spike-4-conformance/scripts/matrix.py \
  .../schema-results.json .../parser-results.json .../matrix.tsv
```

- Schema：CNCF Serverless Workflow DSL 1.0.3，自包含 `$defs`（139 个内部 `$ref`），
  来源与完整性见 `schema/PROVENANCE.md`。
- 语料：`corpus/01..24`，覆盖 document/do、set、call(http/grpc/function/openapi)、
  do、switch、fork、for、wait、listen、emit、raise、try、run(script/container/
  shell/workflow/await+return)、use.extensions、use.functions、taskBase `if`、
  0.8 遗留 states/start、非法缺 document。
- 解析结果同时导出顶层任务摘要（含 `run` 解析出的 workflow_ref、TaskMeta 的
  `if`/`timeout`），用于区分“接受且正确”与“接受但误读”。

## 3. 覆盖清单（现状解析器）

### 3.1 支持且语义对齐

| 构造 | 证据 | 备注 |
|:--|:--|:--|
| `document` + `do` 顶层结构 | 01 | 与官方同形 |
| `set` | 01 | |
| `call: http` | 02 | `with.method/endpoint/headers` 解析 |
| `call: grpc` | 21 | CallType::Grpc |
| `call: <function>` + `use.functions` | 24 | 定义后可解析（严格引用校验：未定义即拒绝） |
| `do`（顺序子任务） | 03 | |
| `fork`（branches + compete） | 05 | |
| `for`（each/in + do） | 06 | 解析接受；迭代语义未在本轮执行期验证 |
| `wait` | 07 | |
| `listen`（`to.one.with`） | 08 | 仅测 one 消费策略 |
| `emit`（event.with source/type/data） | 09 | |
| `raise`（error） | 10 | |
| `try`/`catch.do` | 11 | |
| taskBase `if` | 22 | 解析进 TaskMeta.if_condition（`$defs/taskBase`） |
| taskBase `input`/`output`/`export`/`timeout`/`retry` | 代码 | TaskMeta 字段齐（`model.rs:319`），本轮未逐项构造语料 |
| `use.functions` / `use.retries` / `use.timeouts` | 24 / 代码 | UseComponents |

### 3.2 不支持（硬拒绝）

| 构造 | 语料 | 现象 |
|:--|:--|:--|
| 官方 `switch` 形状（case 内 `when` + `then`） | 04 | 报错 `switch condition missing transition target`：coord 要求 `transition`，官方是 `then`（flowDirective） |
| `call: openapi`（asyncapi/a2a/mcp 同理推断） | 23 | 按函数名解析 → `references unknown function 'openapi'`；协议语义不覆盖 |
| 函数未在 `use.functions` 定义 | 16 | 引用校验拒绝（严格于 schema；属可接受的 fail-closed 行为） |

### 3.3 行为不一致（接受但语义错误 / 静默丢弃）

| 构造 | 语料 | 解析产物（证据） | 风险 |
|:--|:--|:--|:--|
| `run: script` | 12 | `run_subflow`, ref `{"","",""}` | 高：脚本永不执行，且无报错 |
| `run: container` | 13 | 同上 | 高：容器永不启动，无报错 |
| `run: shell` | 14 | 同上 | 高：命令永不执行，无报错 |
| `run: workflow`（带 namespace/name/version） | 15 | 同上（嵌套一层读不到） | 高：子流程调用空引用 |
| `run.await` / `run.return` | 18 | 被忽略 | 中：时序/输出语义丢失 |
| `use.extensions`（含 `before` 任务） | 17 | 解析通过，`before` 不在任务列表 | 高：扩展静默失效，审计/拦截类扩展会被误信为已生效 |
| 非 switch 任务的 `then` | 02/03/06/… | Task 模型无 `then` 字段，被忽略 | 中：`then: <taskName>` 的跳转在列表顺序执行下失真 |
| 0.8 `states/start` | 19 | 走 `cncf_sw` 路线接受（schema 判非法） | 低（有意兼容），但与「宣称 1.0.3 权威」口径冲突 |

## 4. 量化结论

- 解析层通过率：19/22 schema-合法语料（86%）；合法语料中被硬拒 3 份。
- **语义正确率**：19 份 PASS 中 6 份语义被误读/丢弃 → **13/22（59%）可用**。
- 最关键的单点：`run:*` 家族 100% 静默误读（5/5 语料），且不产生任何错误，
  消费方无法通过错误码感知。
- `extensions` 与官方扩展点的 9 个 `extend` target 全部无实现，解析静默通过。

## 5. 新解析器 vs 适配层评估

| 方案 | 范围 | 工作量（人天，粗估） | 风险 |
|:--|:--|:--|:--|
| A. 适配层（官方 → 现有 native 方言） | `then`/flowDirective 转 `end`/transition 接线、switch case 形状、fork/for/listen/emit/wait 形状归一、`run.workflow` 拆包 | 解析+校验 **6–10**，测试 3–5 | 语义保真有损（无 round-trip）；`run:script|container|shell`、extensions、协议调用 **无法靠适配覆盖** |
| B. 完整 1.0.3 解析器（不动执行引擎） | 原生解析全部 12 类任务 + taskBase + use.*；未知构造 fail-closed | **15–25**（含 300+ 语料与 schema 一致性测试） | 解析对了但执行语义仍缺（run/扩展点），会产生「解析通过但跑不了」的新一类静默问题，除非同步加能力白名单 |
| C. 引擎语义扩展（真正支持） | run:script/container/shell/workflow 执行面、extensions before/after 注入、flowDirective 完整语义、asyncapi/openapi/a2a/mcp 协议、listen 全消费策略 | **25–45**（不含容器/沙箱运行时的安全评审与运维） | 与 G-WF-1（2027-03-31）排期竞争；run 执行面引入沙箱/凭据/资源限制等新边界 |

**建议**：

1. **立即（不等 Spike 6）**：把未实现构造从「静默误读」改为 **fail-closed 拒绝**
   （至少覆盖 `run.*` 非 workflow、`use.extensions`、非 switch 的 `then`）。
   这是一行级风险/收益比最高的改动，且符合仓库既有 fail-closed 口径。
2. 短期采用 **方案 A + 能力白名单**：满足文档流动场景，不伪装支持脚本/容器。
3. 中期与 ADR-0010 的 G-WF-1 合同期对齐，按消费方真实需求决定方案 C 的子集
   （优先 `run:workflow` 的正确子流程调用与 `then` 流程控制）。
4. 「完整 1.0.3」若作为对外承诺，必须连带执行语义与验收项，否则会制造
   解析层符合、运行期失败的新口径分裂。

## 6. 对消费方与治理的影响

- 现有消费方（EIS/IMS，ADR-0010 G-WF-1）使用 native `document/do` 方言，
  本轮改动不破坏其契约；但 fail-closed 白名单若引入，需要发布说明。
- 若承诺 1.0.3，新增字段/行为需走 `apis/contracts/WHITEPAPER.md` 兼容性流程 +
  `STATUS.md` 验收项 + `grpc_auth.rs` 能力登记（若暴露新 RPC）。
- ICPS 的审批场景若依赖 `run:script` 做规则判断，则必须等方案 C；否则应使用
  `call` + 外部服务。

## 7. 限制（未覆盖）

- 只做解析层验证，未执行工作流（执行期语义另测）。
- `asyncapi`/`a2a`/`mcp` 未逐条构造（`openapi` 已证明「按函数名解析」路径，
  推断同类）。
- `listen` 仅测 `one` 消费策略；`fork`/`for` 仅测形状。
- Schema 固定 1.0.3；未验证 1.0.0–1.0.2 差异。
- 人天估计未含安全评审、容器/脚本执行面的运维成本。
