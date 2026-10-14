# v2 审批模型与存储 Implementation Plan（M2-B）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 Spike 1 验证过的审批 v2 五层状态模型（Instance / Branch / Task / Round / 三级变量作用域）与 v2 存储（独立键空间 + CAS 唯一写路径 + 状态与事件同 Txn）落成 `coord-core` / `coord-agent` 的生产代码，作为 M2-C（proto/授权/approve 方言）可直接调用的模型与存储层。

**Architecture:** 新增 `coord-core/src/workflow/approval/`（ADR-0013 D1：扩展现有 `coord-core/workflow`，不另建 crate）。语义**逐字移植** `spike/2026-10/spike-1-state-model/crate/`（16 个场景用例是判据），只做四类适配：① `Engine` 可持久化（serde + 注入实例 id + 持久化 seq）；② 语义字段补齐（`engine_version`/`definition_hash`）；③ 错误类型改名 `ApprovalError` 并消除命令路径 panic；④ 存储层新增（端口 + 内存实现 + KV 实现 + 键布局）。v2 实例走独立键空间 `/_workflow/v4/instances/{id}`，与 v1 的 `/_workflow/v3/` 扫描面互不可见；outbox 复用 v3 传输设施（`/_workflow/v3/outbox/**`），状态与事件在同一 KV Txn 提交。

**Tech Stack:** Rust（coord-core / coord-agent）、serde/serde_json、async-trait、coord-server KV Txn（mod_revision CAS）。

**Spec:** `docs/adr/0013-workflow-approval-v2.md` D1/D2/D3/D8（本计划固化 D8 的具体键布局）；语义与字段清单 `spike/2026-10/spike-1-state-model/docs/MODEL.md`、`spike/2026-10/spike-1-state-model/docs/TRANSITIONS.md`；缺口口径 `spike/2026-10/spike-1-state-model/docs/GAPS.md`。证据原型 `spike/2026-10/spike-1-state-model/crate/`。

**Base:** 分支 `feat/workflow-v2-model`，基线 `feat/workflow-103-adapter@a12c8ea`（M2-A 已收口，stack 在 PR #71 之上）。

---

## Global Constraints

- **对外契约零改动**：不改 `apis/**`（proto/RPC 零增量）、不改 `coord-proto`。事件类型在 M2-B 仅为**草案常量**，正式契约（`.v1` 后缀与 JSON Schema）由 M2-C 经 EIS/ICPS 确认后固化——本里程碑不接线，不会有生产事件流出。
- **冻结证据只读**：`spike/2026-10/**` 一律不改（含 Spike 1 crate 与 Spike 3 schema）；移植是读源 + 新写。
- **v1 行为逐字不变**：`coord-core`、`coord-agent`、`coord` 全量测试绿；v1 实例键空间、`list_instances` 扫描面、watch 前缀、outbox relay 行为不变。
- **默认关闭不接线**：M2-B 不新增任何生产装配点（`coord-agent/src/services/mod.rs` 只登记模块；`workflow.rs`/`main.rs` 不构造 `KvApprovalStore`），新代码只被测试驱动。
- **无旁路写路径（D3）**：v2 提交接口没有"无 CAS"形态；CAS 冲突必须零写入并返回 `Ok(false)`，调用方重读后重试，禁止盲写覆盖。
- **fail-closed**：CAS 失败、序列化失败、事务失败、未知 `engine_version` 一律拒绝并留痕；禁止先写状态后补事件的"半提交"降级。
- 依赖只用现有 workspace 依赖；不新增 crate；不新增 `TODO`/`FIXME`；注释只写当前「为什么」（不写变更史/日期）；临时文件只放 `/tmp`；`bash scripts/check-doc-refs.sh` 保持 OK。

## 移植规则（Task 1–3 共用，评审逐条核对）

1. **语义逐字**：状态转换、判定顺序、错误分类、审计字段与 `spike/2026-10/spike-1-state-model/docs/TRANSITIONS.md` §1–§3 完全一致；不得"顺手简化"或调整判定优先级。
2. **文件映射**：`crate/src/model.rs` → `approval/model.rs`；`crate/src/engine.rs` → `approval/engine.rs`；`crate/tests/scenarios.rs` → `coord-core/tests/approval_scenarios.rs`（公开 API 集成测试）。
3. **命名**：`Error` → `ApprovalError`（避免与 `coord_core::workflow::errors` 混淆）；其余类型/字段/方法名逐字保留（含 `Effect` 的 serde tag 形状）。
4. **可持久化**：`Engine` 增 `Serialize/Deserialize`；`seq` 与 `node_attempts` 必须随聚合持久化（重载后 ID 不重复、attempt 不倒退）。
5. **ID 注入**：`Engine::start` 由调用方给实例 id；子实体 ID = `format!("{}:{}", instance_id, gen(prefix))`，`gen` 用持久化的 `seq` 递增（无随机、无时钟 ⇒ 同输入同输出可重放）。
6. **无 panic**：命令路径不得出现 `unwrap()/expect()/panic!`；spike 中的 4 处非测试 unwrap（`engine.rs:161,264,749,796-799`）改为 fail-closed 错误路径或返回 `Result`。
7. **rustdoc**：每个 `pub` 类型/方法带文档注释；不变量从 `MODEL.md`/`TRANSITIONS.md` 摘写（当前口径），不新编语义。

## Review Focus

1. **判定顺序**：`recount` 必须按 TRANSITIONS §3 的 1→7 顺序判定（一票否决最先、`total=0`/全通过次之、再 any/quorum/ratio、最后耗尽）；顺序错会让"否决+同时满足通过条件"的轮次误判为 passed（Task 2 `p0_one_vote_veto`、`ratio_pass_then_exhausted_reject`）。
2. **CAS 零写入**：`ExpectedRev` 不匹配时，状态、事件、修订号三者都必须原样不变（Task 4 `memory_store_cas_failure_leaves_no_state_and_no_events`；Task 6 `exact_rev_conflict_is_zero_write`）。
3. **键空间隔离**：v4 键绝不出现在 v3 的实例列表/全量扫描/watch 前缀内；v3 键不得被 v4 实例扫描吞掉（Task 4 键布局单测；Task 6 `v1_list_does_not_see_v2_instances`）。
4. **serde 无损**：聚合往返（含 `seq`/`node_attempts`/全部状态枚举）后语义一致，重载后继续执行不产生重复 ID（Task 2 ID 唯一性用例；Task 6 `load_after_reload_preserves_semantics`）。
5. **事件同 Txn**：一批事件要么与状态同一次提交生效、要么一条都不生效；不得出现 `MemoryAtomicCommit` 那种"先状态后事件"的非原子实现（Task 4 原子性用例；Task 6 `commit_persists_state_and_events_in_one_txn`）。

---

### Task 1: `approval` 模块骨架与实体模型

**Files:**
- Create: `coord-core/src/workflow/approval/mod.rs`
- Create: `coord-core/src/workflow/approval/model.rs`
- Modify: `coord-core/src/workflow/mod.rs`（`pub mod approval;` 并补一行模块说明）
- Test: `coord-core/src/workflow/approval/model.rs`（`mod tests`）

**Interfaces:**
- Consumes: 无（本任务只依赖 `serde`/`serde_json`/`BTreeMap`）。
- Produces（后续任务按此名使用，字段清单 = `MODEL.md` §1 + 本任务的两处补字段）：
  - `pub type Id = String; pub type Principal = String; pub type Scope = BTreeMap<String, Value>;`
  - `pub struct Instance { id, definition_ns, definition_name, definition_version, definition_hash, engine_version, initiator, status, context, root_branch_id, revision, jump_count, audit, returns, created_at, updated_at }`（相对 spike 新增 `definition_hash: String`、`engine_version: String` 两字段）。
  - `pub const APPROVAL_ENGINE_VERSION: &str = "v2";`
  - 其余实体与枚举逐字移植：`InstanceStatus`、`AuditEntry`、`ReturnRecord`、`BranchStatus`、`JoinPolicy`、`RejectPolicy`、`Branch`、`TaskKind`、`TaskStatus`、`Delegation`、`Decision`、`VoteRecord`、`Task`、`SeatSpec`（含 `approver/reader/pool` 构造）、`RoundStatus`、`Round`、`AssignKind`、`Effect`、`ApprovalError`（由 spike `Error` 改名，变体与显示文本不变）。
  - 全部类型 derive `Debug, Clone, PartialEq, Serialize, Deserialize`（spike 有 `Copy/Eq` 的保留）。

- [ ] **Step 1: 写失败测试（`model.rs` 的 `mod tests`）**

  `test_model_serde_roundtrip_full_instance`：构造含全部状态枚举、`AuditEntry`/`ReturnRecord`、`Delegation`、`VoteRecord` 的 `Instance`/`Branch`/`Task`/`Round`，`serde_json::to_value` → `from_value` 断言相等（`assert_eq!(back, original)`）。
  `test_enum_serde_names_are_snake_case`：`InstanceStatus::Rejected` → `"rejected"`；`JoinPolicy::Quorum(2)` → `{"quorum":2}`；`JoinPolicy::Ratio{num:2,den:3}` → `{"ratio":{"num":2,"den":3}}`；`RejectPolicy::AnyReject` → `"any_reject"`；`TaskKind::ReadAck` → `"read_ack"`；`Effect::TaskAssigned{..}` 的 JSON 含 `"type":"task_assigned"`。
  `test_effect_event_serde_shape`：`Effect::Event{name:"com.china-workflow.approval.round.passed".into(), data: json!({"a":1})}` → `{"type":"event","name":...,"data":{"a":1}}`。
  `test_approval_engine_version_pinned`：`assert_eq!(APPROVAL_ENGINE_VERSION, "v2")`。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib workflow::approval`。Expected: FAIL（模块不存在）。

- [ ] **Step 2: 建模块 + 移植**

  移植 `spike/2026-10/spike-1-state-model/crate/src/model.rs`（文件顶部注释保留"字段清单见 MODEL.md / 转换见 TRANSITIONS.md"的指向，但改为指向本仓当前文档路径并用 rustdoc 形式）。两处补字段放在 `Instance` 定义处并加 rustdoc：`definition_hash`（实例启动时钉死的定义内容哈希；漂移校验在加载定义的驱动层，M2-C）与 `engine_version`（`APPROVAL_ENGINE_VERSION`；v2 实例不允许被其他版本驱动）。
  `mod.rs`：模块级 rustdoc（职责 + 与 ADR-0013 D2/D8 的对应 + 语义来源指向 `docs/production/workflow-semantics.md`），本任务只声明 `pub mod model;`；`engine`/`keys`/`store`/`events` 由各自任务在同一文件追加声明。

- [ ] **Step 3: 跑通并提交**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib workflow::approval`。Expected: PASS。`cargo fmt --all`。
  Commit: `feat(workflow): port v2 approval entities into coord-core approval module`

---

### Task 2: 引擎核心（生命周期 / 席位 / 计票判定）

**Files:**
- Create: `coord-core/src/workflow/approval/engine.rs`
- Modify: `coord-core/src/workflow/approval/mod.rs`（`pub mod engine;`）
- Create: `coord-core/tests/approval_scenarios.rs`（本任务放核心场景；Task 3 追加管理场景）

**Interfaces:**
- Consumes: Task 1 的 `model::*`、`ApprovalError`、`APPROVAL_ENGINE_VERSION`。
- Produces:
  - `pub struct Engine { pub instance: Instance, pub branches: BTreeMap<Id, Branch>, pub tasks: BTreeMap<Id, Task>, pub rounds: BTreeMap<Id, Round>, /* private */ node_attempts: BTreeMap<String,u32>, seq: u64 }`，derive `Serialize, Deserialize`。
  - `pub struct StartSpec { pub id: String, pub definition_ns: String, pub definition_name: String, pub definition_version: String, pub definition_hash: String, pub initiator: Principal, pub context: Scope }`
  - `pub const MAX_JUMPS: u32 = 20;`
  - `pub fn Engine::start(spec: StartSpec, now: i64) -> Result<Engine, ApprovalError>`：空 `id`/`definition_ns`/`definition_name`/`definition_version`/`definition_hash`/`initiator`、`id.contains('/')` 或 `id.len() > 128` ⇒ `ApprovalError::InvalidArgument("<字段名>")`；成功时 `revision=0`、`status=Pending`、`engine_version=APPROVAL_ENGINE_VERSION`。
  - 逐字移植：`enter_branch`、`open_approval`、`add_sign`、`remove_sign`、`claim`、`approve`、`reject`、`acknowledge`、`expire`、`finish`、`resolve_var`、`open_tasks_for`、`snapshot_json(&self) -> Result<Value, ApprovalError>`（spike 的 `snapshot_json` 由无返回失败的 `Value` 改为 `Result`，序列化失败映射 `ApprovalError::SerializationError`）。
  - `ApprovalError` 加性变体（裁定，Task 2 落地）：`SerializationError(String)`（Display `serialization error: {msg}`）。计划原稿引用了该失败但冻结枚举无此变体；裁定**不**引 `StoreError`（领域层引存储端口倒置分层）、也**不**映射到 `InvalidArgument`（新造语义）。既有 5 变体与 Display 文案不变；`ApprovalError` 不入任何持久文档，此行不影响契约。
  - 子实体 ID 规则（移植规则 5）：`format!("{}:{prefix}-{seq:04}", self.instance.id)`。

- [ ] **Step 1: 写失败测试（`coord-core/tests/approval_scenarios.rs`）**

  从 `spike/2026-10/spike-1-state-model/crate/tests/scenarios.rs` 移植（保留用例名与断言，只把 `Engine::start(...)` 调用改成 `Engine::start(StartSpec{ id:"inst-t2".into(), ..., definition_hash:"sha256:t2".into(), ...}, now)`）：
  `p0_huiqian_with_add_sign`、`p0_one_vote_veto`、`p0_any_sign_cancels_rest`、`ratio_pass_then_exhausted_reject`、`read_ack_does_not_gate_and_reader_only_branch_passes`、`stale_revision_is_rejected`、`pool_claim_rules`、`deadline_expire_excludes_seat_from_quorum`。
  新增（不照搬 spike）：
  `start_rejects_empty_required_fields`：空 `id`、空 `definition_hash`、含 `/` 的 `id` 各返回 `InvalidArgument`，错误消息含对应字段名。
  `generated_ids_are_derived_from_instance_and_survive_reload`：连续两次 `enter_branch` 后把 `Engine` `to_value`→`from_value` 还原再 `enter_branch`，断言三次产生互不相同的 branch id、且都已 `instance.id` 为前缀；`node_attempts` 复原（第三次 entering 同名节点 attempt==3）。
  `terminal_instance_rejects_commands`：走 `enter_branch → open_approval(1 席) → approve → finish` 得到 `Completed` 终态后，`enter_branch` 返回 `InvalidTransition`（不依赖 Task 3 的 `withdraw`）。
  `snapshot_json_failure_is_error_not_panic`：断言 `snapshot_json()` 为 `Ok`，且 `to_value` 输出的键集合 = `instance/branches/tasks/rounds`。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --test approval_scenarios`。Expected: FAIL（编译失败：模块不存在）。

- [ ] **Step 2: 移植引擎核心**

  逐字移植 `crate/src/engine.rs` 的构造、`enter_branch`、`open_approval`、`add_sign`、`remove_sign`、`claim`、`approve`、`reject`、`acknowledge`、`expire`、`finish`、内部 `create_branch/add_seats/recount/cancel_pending_tasks/close_branch/set_branch_status/guard/touch/emit/gen` 与判定逻辑（TRANSITIONS §3 顺序不得改动）。本任务不移植 `transfer/delegate/return_to_node/withdraw/terminate`（Task 3 做，先以 `unimplemented` 会违反"无 panic"，故这些方法**本任务不出现**）。
  消除 unwrap：`self.tasks.get_mut(tid).unwrap()`（spike `engine.rs:264,749`）改为 `ok_or_else(|| ApprovalError::NotFound(tid.into()))?`；`self.rounds.get(...).unwrap()`（`engine.rs:161`）同理。

- [ ] **Step 3: 跑通并提交**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --test approval_scenarios`（本任务 12 个用例全绿）与 `cargo test -p coord-core --lib workflow`（回归）。
  Commit: `feat(workflow): port v2 approval engine core (lifecycle, seats, tally)`

---

### Task 3: 引擎管理命令与作用域

**Files:**
- Modify: `coord-core/src/workflow/approval/engine.rs`
- Modify: `coord-core/tests/approval_scenarios.rs`

**Interfaces:**
- Consumes: Task 2 的 `Engine`/`StartSpec`/`ApprovalError`。
- Produces（逐字移植，签名与 spike 一致）：
  - `pub fn transfer(&mut self, expected: u64, task_id: &str, to: &str, by: &str, now: i64) -> Result<Vec<Effect>, ApprovalError>`
  - `pub fn delegate(&mut self, expected: u64, task_id: &str, to: &str, by: &str, now: i64) -> Result<Vec<Effect>, ApprovalError>`
  - `pub fn return_to_node(&mut self, expected: u64, branch_id: &str, target_node: &str, by: &str, comment: Option<&str>, now: i64) -> Result<(Id, Vec<Effect>), ApprovalError>`
  - `pub fn withdraw(&mut self, expected: u64, by: &str, now: i64) -> Result<Vec<Effect>, ApprovalError>`
  - `pub fn terminate(&mut self, expected: u64, by: &str, reason: &str, now: i64) -> Result<Vec<Effect>, ApprovalError>`
  - 内部助手（本任务移植，仅被上述命令调用）：`close_rounds_of_branch`、`cancel_open_branches`。

- [ ] **Step 1: 写失败测试**

  移植 `p0_return_to_node_reopens_history`、`p0_transfer_moves_authority`、`delegate_allows_proxy_and_keeps_principal`、`withdraw_only_by_initiator`、`terminate_cascades_and_seals_instance`、`return_target_whitelist_and_jump_limit`、`variable_scope_precedence`、`evidence_snapshot_serializable`（写入断言：snapshot 可 `serde_json::to_value` 且含四键；不写证据文件）。
  新增：`return_to_node_rejects_non_whitelisted_target`（错误消息含目标节点名）；`finish_after_returned_branch_is_invalid_transition`（无 completed 根分支时 `finish` 拒绝）。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --test approval_scenarios`。Expected: FAIL（方法缺失）。

- [ ] **Step 2: 移植管理命令**

  逐字移植 `crate/src/engine.rs` 的 `transfer/delegate/return_to_node/withdraw/terminate` 及其使用的 `cancel_open_branches`。`jump_count` 上限沿用 `MAX_JUMPS`；白名单 = `node_attempts` 已进入节点；驳回记录写入 `instance.returns`。

- [ ] **Step 3: 跑通并提交**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --test approval_scenarios`（16 个移植用例 + 6 个新增全绿）。
  Commit: `feat(workflow): port v2 approval management commands and scopes`

---

### Task 4: 存储端口 + 内存实现 + v2 键布局

**Files:**
- Create: `coord-core/src/workflow/approval/keys.rs`
- Create: `coord-core/src/workflow/approval/events.rs`
- Create: `coord-core/src/workflow/approval/store.rs`
- Modify: `coord-core/src/workflow/approval/mod.rs`（`pub mod events; pub mod keys; pub mod store;`）
- Test: `keys.rs`/`events.rs`/`store.rs` 内 `mod tests`

**Interfaces:**
- Consumes: Task 2/3 的 `Engine`；`coord_core::workflow::durable::OutboxRecord`；`coord_core::workflow::ports::StoreError`。
- Produces:
  - `keys.rs`：`pub const APPROVAL_INSTANCE_PREFIX: &[u8] = b"/_workflow/v4/instances/";`、`pub fn instance_key(instance_id: &str) -> Vec<u8>`（=`APPROVAL_INSTANCE_PREFIX + id`）、`pub fn instance_prefix() -> &'static [u8]`、`pub fn prefix_range_end(prefix: &[u8]) -> Vec<u8>`（字典序上界，语义同 `coord-agent` 的 `prefix_end`；全 0xFF 返回空 = 扫到无穷）。
  - `events.rs`：`pub const APPROVAL_EVENT_TYPES: &[&str]`（7 个草案类型，逐字取自 Spike 1 engine 的 emit：`com.china-workflow.instance.returned|cancelled|terminated|completed|rejected`、`com.china-workflow.approval.round.passed|rejected`）；`pub fn event_record(aggregate_id: &str, name: &str, data: &Value, now_ms: i64, id: String) -> Result<OutboxRecord, ApprovalError>`——`name` 不在目录内 ⇒ `InvalidArgument(name)`（fail-closed，防错拼类型泄入契约）；目录内 ⇒ `OutboxRecord::lifecycle(..)`。
  - `store.rs`：
    - `#[derive(Debug, Clone, Copy, PartialEq, Eq)] pub enum ExpectedRev { CreateOnly, Exact(i64) }`
    - `#[async_trait] pub trait ApprovalStore: Send + Sync { async fn load(&self, instance_id: &str) -> Result<Option<(Engine, i64)>, StoreError>; async fn commit(&self, engine: &Engine, events: &[OutboxRecord], expected: ExpectedRev) -> Result<bool, StoreError>; }`
    - `pub struct MemoryApprovalStore`（`Mutex<BTreeMap<String, Entry>>`，`Entry { engine, mod_rev, events: Vec<OutboxRecord> }`）；`pub fn new() -> Self`；`pub async fn events(&self, instance_id: &str) -> Vec<OutboxRecord>`（对账/测试）；`pub async fn mod_rev(&self, instance_id: &str) -> Option<i64>`。
    - commit 语义：`CreateOnly` 要求键不存在，`Exact(n)` 要求当前 `mod_rev == n`，不匹配 ⇒ `Ok(false)` 且不改任何东西；匹配 ⇒ 存储状态、追加该批事件（按批次顺序分配 per-instance 单调 `sequence`，首条 = 已存 max+1）、`mod_rev += 1`。状态与事件在同一把锁的临界区内完成 ⇒ 原子。
- 键布局（D8 固化）：状态单文档 `/_workflow/v4/instances/{instance_id}`；事件仍写 `/_workflow/v3/outbox/**`（共享传输设施，relay 已认该前缀）。

- [ ] **Step 1: 写失败测试**

  `keys.rs`：`approval_instance_key_layout`（`instance_key("inst-1") == b"/_workflow/v4/instances/inst-1"`）；`approval_keys_are_outside_v1_instance_prefix`（`!instance_key(..).starts_with(b"/_workflow/v3/instances/")`，且 `/_workflow/v3/instances/x` 不在 `[instance_prefix(), prefix_range_end(..))` 区间内）；`prefix_range_end_handles_ff_suffix`（全 0xFF → 空 Vec）。
  `events.rs`：`event_record_rejects_unknown_type`（未知名 `InvalidArgument`）；`all_emitted_event_names_are_in_catalog`（跑一个含驳回/撤回/收口的场景，收集全部命令输出里的 `Effect::Event.name`，每个都 ∈ `APPROVAL_EVENT_TYPES`）。
  `store.rs`：`memory_store_create_only_is_exactly_once`（首次 `CreateOnly` `true`；同 id 再次 `CreateOnly` `false` 且状态仍是首次的）；`memory_store_exact_cas_rejects_stale`（`Exact(1)` 成功、`Exact(1)` 再来一次 `false`）；`memory_store_cas_failure_leaves_no_state_and_no_events`（失败后 `load` 的状态修订、`events()` 条数、`mod_rev` 均与失败前逐字相等）；`memory_store_load_roundtrip_preserves_engine`（`Engine` 全量往返后 `snapshot_json()` 相等）；`memory_store_assigns_monotonic_sequences`（两批事件 `sequence` 连续且跨批递增，入参记录的 `sequence==0` 被覆盖）。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib workflow::approval`。Expected: FAIL。

- [ ] **Step 2: 实现 keys.rs 与 store.rs**

  `MemoryApprovalStore` 的 rustdoc 必须写明：**本实现的状态与事件同锁临界区提交是原子的，可用于正确性测试**（与 `MemoryAtomicCommit` 的"非原子、禁止生产"注释形成对照）；生产路径必须用 Task 5 的 KV Txn 实现。

- [ ] **Step 3: 跑通并提交**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core`（全绿，v1 回归在内）。
  Commit: `feat(workflow): add v2 approval store port, memory store and keyspace`

---

### Task 5: KV 实现（coord-agent）

**Files:**
- Create: `coord-agent/src/services/approval_store.rs`
- Modify: `coord-agent/src/services/mod.rs`（登记 `pub mod approval_store;`）
- Test: `approval_store.rs` 内 `mod tests`

**Interfaces:**
- Consumes: `ApprovalStore`/`ExpectedRev`/`keys`（Task 4）；`KvOutboxStore` 的 `pub(crate)` 助手 `outbox_ops`/`outbox_compares`/`read_sequence`（`coord-agent/src/services/outbox_kv.rs`，已存在）；`AgentInner`。
- Produces: `pub struct KvApprovalStore { inner: Arc<AgentInner>, outbox: Arc<KvOutboxStore> }`，`pub fn new(inner: Arc<AgentInner>, outbox: Arc<KvOutboxStore>) -> Self`，`impl ApprovalStore for KvApprovalStore`。
  - `load`：`inner.client.kv().range_with_rev(instance_key(id), instance_key(id), 1, 0)` → `serde_json::from_slice::<Engine>`；必须用 `range_with_rev`（线性一致）而非缓存。
  - `commit`：有界重试 ≤8 次：读当前实例（存在性 + mod_rev，用于区分失败原因）、读 outbox seq → 构造 `Txn`：`put(keys::instance_key, engine_json)` + `KvOutboxStore::outbox_ops(events, cur+1)`；谓词 = `outbox_compares(aggregate, cur_bytes, events)` + 实例谓词（`CreateOnly` ⇒ `Compare(VERSION==0)`，`Exact(n)` ⇒ `Compare(MOD_REV==n)`）。
    - 成功 ⇒ `Ok(true)`；失败 ⇒ 判因：若实例谓词已不成立（`CreateOnly` 且存在 / `Exact(n)` 且现 rev≠n）⇒ `Ok(false)`（零写入，调用方重读）；否则（seq 竞争）重试；重试中若本批全部事件 id 已存在（重放）⇒ `Ok(true)`；耗尽 ⇒ `Ok(false)`。
- 生产装配点（按 Global Constraints）：**不新增** `new` 调用（除测试）。

- [ ] **Step 1: 写失败测试**

  `approval_kv_key_layout_isolated_from_v1`：`keys::instance_key("i")` 前缀等于 `/_workflow/v4/instances/`，且不以 `KvWorkflowStore::instance_key("i")` 为前缀（后者需在 `workflow_store.rs` 已 `pub(crate)`，同 crate 可见；若不可见则改断言字面量 `/_workflow/v3/instances/`）。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-agent --lib services::approval_store`。Expected: FAIL（模块不存在）。

- [ ] **Step 2: 实现**

  `Txn` 构造复用 `workflow_store.rs`/`outbox_kv.rs` 的既有模式（`CompareResult::Equal`、`Target::Version|ModRev|Value`、`RequestOp{Op::RequestPut}`）；错误统一 `StoreError::IoError/SerializationError`。

- [ ] **Step 3: 跑通、确认无装配、提交**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-agent --lib services::approval_store` 与 `cargo test -p coord-agent`（回归）。
  检查：`git grep -n "KvApprovalStore::new" -- 'coord-agent/src' 'coord/src' 'coord-core/src'` 只命中 `approval_store.rs` 测试与定义，不得命中 `main.rs`/`workflow.rs`/`services/mod.rs` 之外的生产装配。
  Commit: `feat(workflow): add KV-backed v2 approval store with same-txn events`

---

### Task 6: 集成验收（真实 Raft KV）

**Files:**
- Create: `coord/tests/approval_store_test.rs`
- Test: 同文件

**Interfaces:**
- Consumes: `common::start_test_server()`/`wait_tcp_ready`（`coord/tests/common/mod.rs`）、`AgentInner` 构造（照 `coord/tests/spike3_durable_kv.rs:setup`）、`KvApprovalStore`、`KvOutboxStore`、`KvWorkflowStore`、`OutboxStore::list_all`、approval `Engine`/`StartSpec`/`Effect`、`store::{ApprovalStore, ExpectedRev}`。
- 环境：单节点 Raft + gRPC（`start_test_server`）；每个用例独立 TempDir。

- [ ] **Step 1: 写失败测试（先红）**

  `create_only_exactly_one_winner_under_concurrency`：构造两个内容不同的 `Engine`（同 id、不同 `initiator`/`definition_hash`），`tokio::join!` 两次 `commit(.., CreateOnly)`；断言恰一个 `true`、一个 `false`；`load` 得到的内容与赢家逐字相等；`OutboxStore::list_all` 中赢家的每个事件都在、输家的事件一个都没有。并发重复 10 轮（服务端 CAS 是权威，测试用于抓抖动）。
  `exact_rev_conflict_is_zero_write`：`load` 取 rev → `commit(Exact(rev))` 成功 → 用过期的 `Exact(rev)` 再提交不同状态 ⇒ `false`；`load` 的状态、`list_all` 的事件数、rev 都不变。
  `commit_persists_state_and_events_in_one_txn`：一次提交携带 2 条 `events::event_record(..)`；提交成功后 `list_all` 两条都在且 `sequence` 连续、`status=Pending`、`aggregate_id == instance_id`；随后用同 id 状态 + `CreateOnly` + 另 2 条新事件提交（必失败）⇒ 新事件一条都不在、状态与 rev 不变。
  `load_after_reload_preserves_semantics`：提交 → 重新 `load` 构造新 `KvApprovalStore`（模拟重启读取）→ `snapshot_json()` 与提交前相等，rev 单调递增；继续 `enter_branch` 不产生与已存子实体重复的 id。
  `v1_list_does_not_see_v2_instances`：`KvWorkflowStore` 的 `list_instances`（含 `page_size=usize::MAX`）不含 v2 实例 id；v4 前缀 `range` 只返回 v2 键。
  `v4_scan_does_not_see_v1_instances`：写入一个 v1 实例（`KvWorkflowStore::save_instance`）后，v4 `instance_prefix()` 扫描结果不含它。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord --test approval_store_test -- --test-threads=1`。Expected: FAIL（编译失败）。

- [ ] **Step 2: 实现测试（不改生产代码）**

  若测试暴露生产缺陷，回到 Task 5 修复（同分支同 worktree，按 SDD fix round 走），本任务不"顺手改生产代码"。

- [ ] **Step 3: 跑通并提交**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord --test approval_store_test -- --test-threads=1`（全绿）+ `cargo test -p coord --test spike3_durable_kv --test spike2_concurrency`（v1 恢复/并发不回归）。
  Commit: `test(workflow): v2 approval storage acceptance on real Raft KV`

---

### Task 7: 文档落地

**Files:**
- Modify: `docs/adr/0013-workflow-approval-v2.md`（D8 补具体键布局与 CAS 期望；不改其它决定）
- Modify: `docs/production/workflow-semantics.md`（新增「v2 审批模型与存储（未接线）」小节）
- Modify: `coord-core/src/workflow/approval/mod.rs`（rustdoc 收口：模块图 + 与 ADR/语义页的指向）

**Interfaces:**
- Consumes: Task 1–6 的实现事实（键布局、`ExpectedRev`、CAS/事件原子性、`APPROVAL_ENGINE_VERSION`、`events::APPROVAL_EVENT_TYPES` 草案目录）。
- Produces: 文档事实 = 代码事实（单一归属，不复制 Spike 1 文档正文；只链接）。

- [ ] **Step 1: ADR-0013 D8 固化**

  在 D8 内追加（不改 D8 既有条目语义）：键布局 `/_workflow/v4/instances/{instance_id}`（状态单文档，独立于 v3 扫描面）；事件仍走 `/_workflow/v3/outbox/**`；写路径 `ExpectedRev::{CreateOnly, Exact(mod_rev)}`，无无守卫写入；内存实现仅测试用。

- [ ] **Step 2: 语义页小节**

  `docs/production/workflow-semantics.md` 新增小节（标注"未接线，默认关闭；契约与授权 M2-C"）：v2 五层实体与判定规则指向 `spike/2026-10/spike-1-state-model/docs/TRANSITIONS.md`（证据）与本模块 rustdoc（当前口径）；键空间与 CAS 一句话；事件类型列表标注"草案，M2-C 固化"。

- [ ] **Step 3: 检查引用并提交**

  Run: `bash scripts/check-doc-refs.sh`（悬空引用 = 0）、`cargo fmt --all -- --check`。
  Commit: `docs(workflow): record v2 approval keyspace, CAS and storage semantics`

---

## 里程碑判据（M2-B）

| 判据 | 测试/证据 |
|:--|:--|
| 五层模型语义与 Spike 1 逐字一致（P0 全场景） | `coord-core/tests/approval_scenarios.rs`（16 移植 + 6 新增） |
| CAS 是唯一写路径，冲突零写入 | Task 4 单测；Task 6 `exact_rev_conflict_is_zero_write` |
| 状态与事件同 Txn（无半提交） | Task 4 单测；Task 6 `commit_persists_state_and_events_in_one_txn` |
| v2 键空间与 v1 扫描面互不可见 | Task 4 键布局单测；Task 6 双向隔离用例 |
| 对外契约零改动 | `git diff --stat <base>..HEAD -- apis/ coord-proto/` 为空 |
| 冻结证据零改动 | `git diff --stat <base>..HEAD -- spike/2026-10/` 为空 |
| v1 行为不回退 | `cargo test -p coord-core`、`-p coord-agent`、`-p coord` 全绿（含 spike2/spike3/foundation） |
| 无生产接线（默认关闭） | Task 5 Step 3 的 `git grep` 检查；终审复核 |
