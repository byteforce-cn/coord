# 1.0.3 形状适配层 Implementation Plan（M2-A）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让官方 1.0.3 形状的**标准子集**（`switch.when/then`、`fork.branches` map、`for.each/in`、`wait.seconds|milliseconds|until`、`listen.to.one.with`、`emit.event.with`、`run.workflow.input`）解析后**语义正确**，不再有「解析通过但字段丢失」的静默误读（Spike 4 §3.3 的 05/06/07/08/09 与 GAP 04/15）。

**Architecture:** 方案 A —— 在现有解析/校验层内置形状归一（不做独立 crate、不改执行引擎 IR）：`coord-core/src/workflow/validate.rs` 的 `resolve_task`/`parse_*` 识别官方形状并映射到现有 `Task` 模型；`coord-core/src/workflow/sw.rs` 的 states 路线保持现状。所有归一化后仍走 `Task`/`TaskMeta` 既有执行路径，未实现构造继续 fail-closed（M1 Task 8 的拒绝清单不放松）。

**Tech Stack:** Rust（coord-core）、serde_json、现有 `spike4_conformance` 测试 harness 与 corpus。

**Spec:** `spike/2026-10/spike-4-conformance/REPORT.md` §3.3/§5（方案 A）与 `corpus/04..09,15`；判定矩阵 `evidence/matrix.tsv`。ADR-0013 D6（fail-closed + 适配层方向）。

**Base:** 分支 `feat/workflow-103-adapter`，基线 `feat/workflow-v2-foundation@6658ece`（M1 收口；PR #71 stack 之上）。

---

## Global Constraints

- **不动对外契约**：不改 `apis/contracts/proto/**`、不加/改 RPC 与 wire 字段。
- **不改执行引擎语义**：适配只做「官方形状 → 现有 Task/TaskMeta」映射；`Task` 模型新增字段仅限映射必需（如 wait 的时长、listen 过滤、emit 载荷）。
- **fail-closed 不回退**：M1 Task 8 的拒绝清单（`run.script|container|shell|await|return`、`use.extensions`、非 switch `then` 的非法值）保持拒绝；适配层不得把「无法映射」变成静默忽略。
- **内存路径/既有语料不回退**：01–03、10–11、21–24 的行为与 M1 后一致；`cargo test -p coord-core`、`coord --test spike4_conformance --test spike3_durable_kv` 全绿。
- 注释只写当前「为什么」；不新增 TODO/FIXME；临时文件只在 `/tmp`；`bash scripts/check-doc-refs.sh` 保持 OK。

## Review Focus

1. **官方形状变体**：`wait` 字符串("PT5S")与对象(`seconds`/`milliseconds`/`until`)并存时，二者的 `WaitTask` 表示必须语义等价；`for` 有/无 `do`（body 与 `for` 同级）都必须吃到循环体；`fork` 的 map-列表与 `{name,do}` 两种形状都必须产出命名分支与非空 tasks。
2. **`then` 指令语义**：switch case 的 `then: <task>/end/exit/continue` 必须映射为该 case 的转场；任务级 `then` 仍按 M1 规则（顶层末尾 exit/end 接受、其余拒绝）。
3. **表达式上下文**：`for.each`/`in` 与 `switch.when` 的 `${...}` 在映射后仍由现有表达式求值器处理，不得在解析期求值或丢失。
4. **未知/不支持变体**：官方形状中未覆盖的键（如 `fork.branches` 项非法、`wait` 未知键）必须显式报错并指名构造，不得静默产出空任务。
5. **矩阵一致性**：`spike4_conformance` 的语义断言必须能区分「解析 OK 且字段正确」与「解析 OK 但字段空」（即逐字段 `assert_eq!`，不是只断言 `is_ok()`）。

---

### Task 1: `wait` 官方时长形状

**Files:**
- Modify: `coord-core/src/workflow/validate.rs`（`parse_wait`）
- Test: `coord-core/src/workflow/validate.rs` 单测（`test_wait_*`）；`coord/tests/spike4_conformance.rs`

**Interfaces:**
- Produces: `WaitTask.wait` 统一为 ISO8601 时长字符串（现有执行器消费形态）。
- 映射：`wait: "PT5S"`（字符串）原样；`wait: {seconds: N}` → `PT{N}S`；`{milliseconds: N}` → 毫秒时长；`{until: "<ISO8601 绝对时刻>"}` → 现有 `WaitTask` 若只支持时长，则该变体在解析期换算为「相对现在的时长」不可行（解析期无时钟）⇒ **显式拒绝**并指名 `wait.until`（fail-closed，不静默）。
- 未知键（如 `{minutes: …}` 非规范键）：拒绝并指名 `wait.<key>`。

- [ ] **Step 1: 写失败测试**

  `validate.rs`：`test_wait_official_seconds_maps_to_duration`（`{seconds: 5}` ⇒ `WaitTask.wait == "PT5S"`）；`test_wait_until_is_rejected_with_construct_name`；`test_wait_unknown_key_is_rejected`。`spike4_conformance`：corpus 07 的语义断言（见 Task 8，本任务先加 unit）。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib workflow::validate::tests::test_wait`。Expected: FAIL（当前 `wait` 为空串）。

- [ ] **Step 2: 实现映射与拒绝分支**

- [ ] **Step 3: 跑通并回归**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib workflow`。Expected: 全绿。Commit: `feat(workflow): adapt official 1.0.3 wait duration shape`

---

### Task 2: `emit` 官方载荷形状

**Files:**
- Modify: `coord-core/src/workflow/validate.rs`（`parse_emit`）
- Test: `validate.rs` 单测 + `coord/tests/spike4_conformance.rs`

**Interfaces:**
- 映射：`emit: {event: {with: {type, source, data}}}` → 现有 `Task::Emit`（`event_type`/`source`/`data`）；兼容既有平铺形状（若现有测试依赖）。
- `type` 缺失 ⇒ 拒绝并指名 `emit.event.with.type`；`source`/`data` 可空。

- [ ] **Step 1: 写失败测试**：`test_emit_official_event_with_shape`（断言三个字段逐字相等）；`test_emit_missing_type_rejected`。
- [ ] **Step 2: 实现**
- [ ] **Step 3: 回归 + 提交**（同上命令）。Commit: `feat(workflow): adapt official 1.0.3 emit event shape`

---

### Task 3: `listen` 官方过滤形状

**Files:**
- Modify: `coord-core/src/workflow/validate.rs`（`parse_listen`）
- Test: `validate.rs` 单测 + `spike4_conformance.rs`

**Interfaces:**
- 映射：`listen: {to: {one: {with: {type, source?, subject?}}}}` → 事件过滤（复用 `SuspensionMeta.event_filter` 的字段形态）；`one` 之外的消费策略（`all`/`any`）本期不支持 ⇒ 显式拒绝并指名 `listen.to.<strategy>`。
- `type` 缺失 ⇒ 拒绝并指名 `listen.to.one.with.type`。

- [ ] **Step 1: 写失败测试**：`test_listen_official_to_one_with_shape`（type/source/subject 逐字断言）；`test_listen_unsupported_strategy_rejected`。
- [ ] **Step 2: 实现**
- [ ] **Step 3: 回归 + 提交**。Commit: `feat(workflow): adapt official 1.0.3 listen filter shape`

---

### Task 4: `fork` 官方分支 map 形状

**Files:**
- Modify: `coord-core/src/workflow/validate.rs`（`parse_fork`/`parse_fork_branch`）
- Test: `validate.rs` 单测 + `spike4_conformance.rs`

**Interfaces:**
- 官方形状：`fork: {compete?: bool, branches: [ {<branch_name>: <taskBody>}, ... ]}`（corpus 05）⇒ `ForkBranch { name: <key>, tasks: <从 body 解析> }`；保留既有 `{name, do|tasks}` 兼容。
- 分支项既无单键对象也无 `name` ⇒ 拒绝并指名 `fork.branches[i]`；空 `branches` ⇒ 拒绝（不得产出 0 分支的 Fork）。

- [ ] **Step 1: 写失败测试**：`test_fork_official_branch_map_shape`（`branch_a`/`branch_b` 名字与各自 tasks 逐字断言）；`test_fork_empty_branches_rejected`。
- [ ] **Step 2: 实现**
- [ ] **Step 3: 回归 + 提交**。Commit: `feat(workflow): adapt official 1.0.3 fork branch map`

---

### Task 5: `for` 官方 each/in 形状与解析优先级

**Files:**
- Modify: `coord-core/src/workflow/validate.rs`（`resolve_task` 优先级 + `parse_for_each`）
- Test: `validate.rs` 单测 + `spike4_conformance.rs`

**Interfaces:**
- 官方形状：`for: {each: "<var>", in: "<expr>"}` + **与 `for` 同级的 `do: [...]` 为循环体**（corpus 06）⇒ `ForEachTask { iteration: each, input: in, tasks: do[] }`；保留既有 `input/iteration` 键兼容。
- `resolve_task` 必须在 `do` 之前识别「同时含 `for` 与 `do`」的任务为 ForEach（当前 `do` 先命中 ⇒ 循环体当顺序任务、迭代丢失）。
- `each`/`in` 任一缺失 ⇒ 拒绝并指名 `for.each`/`for.in`；`do` 缺失 ⇒ 拒绝并指名 `for.do`。

- [ ] **Step 1: 写失败测试**：`test_for_official_each_in_with_sibling_do`（断言 iteration/in/tasks 非空且任务名逐字）；`test_for_missing_each_rejected`。
- [ ] **Step 2: 实现**
- [ ] **Step 3: 回归 + 提交**。Commit: `feat(workflow): adapt official 1.0.3 for-each shape`

---

### Task 6: `switch` 官方 when/then 形状

**Files:**
- Modify: `coord-core/src/workflow/validate.rs`（`parse_switch`）
- Test: `validate.rs` 单测 + `spike4_conformance.rs`

**Interfaces:**
- 官方形状：`switch: [ {<case_name>: {when: "<expr>", then: "<target>|end|exit|continue"}}, ... ]` ⇒ 现有 `Task::Switch`：`when` → `condition`，`then` → `transition`（`end`/`exit` → `"__end"` 并在需要时合成 `Task::End`；其他字符串 → 目标任务名）。保留既有 `{condition, transition}` 形状兼容。
- 无 `when` 的 case ⇒ 映射到 `default_condition`（多个默认 case ⇒ 拒绝并指名 `switch.<case>.when`）。
- `then: continue` ⇒ 拒绝并指名 `switch.<case>.then`（fall-through 后处理为后续项，不在本任务）。
- `then` 缺失 ⇒ 拒绝并指名 `switch.<case>.then`；`default_condition`/条件目标必须通过引用校验；用户定义的 `__end` 若非 `Task::End` ⇒ 拒绝（保留终名）。

- [ ] **Step 1: 写失败测试**：`test_switch_official_when_then_shape`（两个 case 的 condition/transition 逐字断言）；`test_switch_default_case_maps_to_default_condition`；`test_switch_multiple_default_cases_rejected`。
- [ ] **Step 2: 实现**
- [ ] **Step 3: 回归 + 提交**。Commit: `feat(workflow): adapt official 1.0.3 switch when/then`

---

### Task 7: `run.workflow.input` 提取

**Files:**
- Modify: `coord-core/src/workflow/validate.rs`（`parse_run`）
- Test: `validate.rs` 单测 + `spike4_conformance.rs`

**Interfaces:**
- `run: {workflow: {namespace, name, version, input?}}` ⇒ ref 提取（M1 已做）+ `input` 不再从任务级读取而丢失：映射到 `RunTask` 的输入字段（若模型无该字段，则落到 `TaskMeta.input` 或显式拒绝——实现者先读 `RunTask`/`TaskMeta` 结构后二选一，并在报告中说明；**不得静默丢弃**）。

- [ ] **Step 1: 写失败测试**：`test_run_workflow_nested_input_preserved`（或按选定映射断言）。
- [ ] **Step 2: 实现**
- [ ] **Step 3: 回归 + 提交**。Commit: `feat(workflow): preserve nested run.workflow input`

---

### Task 8: 语料语义断言升级 + 报告落地标注

**Files:**
- Modify: `coord/tests/spike4_conformance.rs`（把 04/05/06/07/08/09/15 的断言从 `is_ok()` 升级为逐字段）
- Modify: `spike/2026-10/spike-4-conformance/REPORT.md`（§5「立即/短期」两项标注已落地 + 指向本测试；不改历史结论）

**Interfaces:**
- 每条语料的断言：04 → 两个 case 的 `when`/`then` 映射；05 → `branch_a`/`branch_b` 名与 tasks 非空；06 → `iteration=="item"`、`in=="${ .items }"`、循环体任务名 `process`；07 → `wait=="PT5S"`；08 → filter `type=="com.example.approved"`；09 → `type/source/data` 逐字；15 → ref + （Task 7 选定映射）。
- 负控制：任一 `assert_eq!` 去掉后测试必须失败（报告写明验证方式）。

- [ ] **Step 1: 升级断言并先看失败**（对当前 HEAD 应有多条红——逐字段缺失）。
- [ ] **Step 2: 与前面任务实现对齐，跑绿**
- [ ] **Step 3: 报告标注 + 提交**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord --test spike4_conformance -- --test-threads=1` 与 `cargo test -p coord-core`。Commit: `test(workflow): assert 1.0.3 corpus semantics field-by-field`

---

## 里程碑判据（M2-A）

| 判据 | 测试/证据 |
|:--|:--|
| 官方标准子集逐字段语义正确（04–09/15） | `spike4_conformance` 升级后的逐字段断言 |
| 既有形状不回退（01–03/10–11/21–24） | 同套件 + `coord-core` 全量 |
| 无法映射的变体 fail-closed 且指名构造 | 各任务负例（until/未知键/空 branches/缺 each 等） |
| 拒绝清单不放松 | M1 Task 8 既有断言保持绿 |
| 对外契约零改动 | `git diff --stat feat/workflow-v2-foundation..HEAD -- apis/` 为空 |

---

### Task 9: `set` 官方 map 形状（执行顺序：紧随 Task 4）

> 该项由 Task 4 实现者发现：官方 `set: {counter: 0}`（corpus 01/05/06）当前解析为**空 SetTask** —— 静默丢字段，必须先于 Task 8 的语义断言修复。

**Files:**
- Modify: `coord-core/src/workflow/validate.rs`（`parse_set`）
- Test: `validate.rs` 单测 + `spike4_conformance.rs`（Task 8 会追加逐字段断言）

**Interfaces:**
- 官方形状：`set: {<var>: <literal-or-runtime-expression>}`（单键）。
- 映射：`variable = <key>`；`value` 必须让现有 `tasks/set.rs` 的表达式求值得到正确结果：
  - `${ <expr> }` 运行时表达式 → 取 `<expr>` 本体（与引擎既有表达式语法对齐，实现者先读 `expression.rs`/`set.rs` 确认 `${}` 是否已由求值器处理，并报告选择）；
  - 其余 JSON 字面量 → 以 JSON 序列化形式作为表达式（如 `0` → `"0"`、`"hello"` → `"\"hello\""`），保证求值结果等于字面量本身。
- 多键 `set`（官方合法但现有 `SetTask` 单变量模型表达不了）⇒ **显式拒绝**并指名 `set.<key>`（fail-closed；多键支持需 `SetTask`/`StepResult` 扩展，属后续任务，不在本里程碑承诺）。
- 既有 native `{variable, value}` 形状保持兼容。

- [ ] **Step 1: 写失败测试**：`test_set_official_map_numeric_literal`（`{counter: 0}` ⇒ variable=="counter" 且求值得到 0）；`test_set_official_map_string_literal`（`{greeting: "hi"}` ⇒ 求值得到字符串 `hi`，不是字段访问）；`test_set_official_multi_key_rejected_naming_key`。
- [ ] **Step 2: 实现映射与拒绝**
- [ ] **Step 3: 回归 + 提交**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib workflow` 与 `cargo test -p coord --test spike4_conformance -- --test-threads=1`。Commit: `feat(workflow): adapt official 1.0.3 set map shape`
