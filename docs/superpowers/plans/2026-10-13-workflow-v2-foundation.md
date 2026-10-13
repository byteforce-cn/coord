# Workflow v2 地基收口 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把「一实例一驱动、状态与事件同事务、外部副作用可幂等去重、崩溃后可对账恢复、定义钉死且未实现构造 fail-closed」收口为现有引擎的唯一写路径（不动对外 proto/RPC）。

**Architecture:** 复用 Spike 2/3 已落在 `spike/2026-10-workflow-v2` 的机制（`coord-core/src/workflow/durable.rs`、`coord-agent/src/services/{outbox_kv,outbox_relay,workflow_lease}.rs`、`WorkflowRuntime::{with_durable,with_instance_lease}`、持久化定时器与恢复扫描），把剩余「未接线项」折叠进单一 `commit_instance(...)` 写路径：CAS/fencing 是唯一写权、outbox 记录与状态同 KV Txn、StepToken 先登记后派发。

**Tech Stack:** Rust（coord-core / coord-agent）、openraft KV `Txn`（mod_revision / version CAS）、tokio、async_trait、serde_json、sha2（工作区已有）。

**Spec:** `docs/adr/0013-workflow-approval-v2.md`（D3/D4/D5/D6 及「落地要求」）；证据与缺口：`spike/2026-10/spike-2-concurrency/{DESIGN,STEPTOKEN}.md`、`spike/2026-10/spike-3-recovery/{OUTBOX,RECOVERY,EVENT-CONTRACT}.md`、`spike/2026-10/spike-4-conformance/REPORT.md`。原始架构文档的判据以 ADR-0013 为收敛口径（例如：不实现官方 `use.extensions`，改原生方言 + fail-closed）。

**Base:** 分支 `feat/workflow-v2-foundation`，基线 `spike/2026-10-workflow-v2@850989a`（worktree `/tmp/coord-v2/wt`）。PR #70 合并进 main 后，把本分支 rebase 到 main。

---

## Global Constraints

- **不动对外契约**：不改 `apis/contracts/proto/**`、不加/改 RPC 与 wire 字段；完成时 `git diff spike/2026-10-workflow-v2..HEAD -- apis/` 必须为空。
- **唯一权威写路径**：实例状态写回一律走 KV `Txn` 的 `mod_revision` CAS（有租约时叠加 fencing）；KV `request_id` 是单节点/进程内/60s 语义，**不得**用作幂等或 StepToken 基础。
- **状态与事件同 Txn**：生产写路径不得出现「状态已落、伴生事件未落」的可观测窗口；降级路径（无 durable）必须在代码注释中显式标注为 legacy。
- **默认关闭**：新增生产行为由配置开关控制且默认 `false`（ADR-0001/0009）；`WorkflowRuntime::new` 的内存路径行为逐字不变。
- **幂等键**：外部副作用幂等键 = StepToken，由 `(instance_id, step_id)` 确定性派生，跨重试不变。
- **未实现构造 fail-closed**：`run:script|container|shell|await|return`（非 `run:workflow`）、`use.extensions`、非 switch 的 `then` 必须显式拒绝，禁止静默误读（Spike 4）。
- **注释纪律**（AGENTS.md）：只写当前「为什么」（不变量/陷阱/契约）；不写变更史；诊断文案中的稳定锚点逐字保留；不新增 TODO/FIXME。
- **临时文件**：一律 `/tmp/coord-*`；不在仓库工作区建 scratch 目录。
- 验证命令（每个任务按需取子集）：`cargo fmt --all -- --check`、`cargo clippy --all-targets --all-features`、`cargo test -p coord-core`、`cargo test -p coord-agent`、`CARGO_BUILD_JOBS=3 cargo test -p coord --test spike2_concurrency --test spike3_durable_kv -- --test-threads=1`、`bash scripts/check-doc-refs.sh`。

## Review Focus

以下输入/条件没有被任何单个任务的测试完整覆盖，评审时优先看：

1. **两个驱动者/控制面操作并发同一实例**：恰一个状态转换生效；败者零写入、零事件；`cas_conflict_total`/`lease_skipped_total` 可观测，不静默。
2. **外部副作用在重试/崩溃恢复下重复**：同一逻辑步骤必须复用同一幂等键（先登记后派发）；派发成功但 `mark_done` 前崩溃 ⇒ 同 token 重发，由下游按 token 去重。
3. **「状态已落、事件未落」窗口**：任何生产装配路径都不得产生；发现降级路径时必须 fail-closed 或显式标注。
4. **同版本定义内容变化后恢复旧实例**：恢复必须 fail-closed（实例 pin 的 hash ≠ 加载的 hash），不得静默按新语义继续。
5. **未实现 DSL 构造**：部署即拒绝（含 `.yml` 经 `DeployDefinition` 的路径），错误信息可定位到构造名；不得接受后误执行。

---

### Task 1: StepToken 存储 + 幂等键经 dispatcher 透传

**Files:**
- Modify: `coord-core/src/workflow/durable.rs`（`StepState`/`StepRecord`/`StepTokenStore`/`MemoryStepStore`；`DurableSupport` 增 `steps`）
- Create: `coord-agent/src/services/step_kv.rs`（`KvStepStore`）
- Modify: `coord-core/src/workflow/ports.rs`（`TaskDispatcher::dispatch` 签名 + `Arc<T>` 委托）
- Modify: `coord-core/src/workflow/runtime.rs`（两处 `ExternalCall` 派发点：先登记、携带 token、成功后 `mark_done`）
- Modify: `coord-agent/src/services/workflow.rs`（`HttpTaskDispatcher::dispatch` 实现：`Idempotency-Key` 头）
- Modify: `coord-agent/src/services/mod.rs`（`pub mod step_kv;`）
- Test: `coord/tests/workflow_steptoken.rs`（新建）；`coord-agent/src/services/step_kv.rs` 内单测

**Interfaces:**
- Consumes: `coord_proto::txn::{Compare, RequestOp}`（KV CAS 形状参考 `workflow_lease.rs` 的 create-if-absent）。
- Produces:
  ```rust
  pub enum StepState { Pending, Done }
  pub struct StepRecord { pub token: String, pub state: StepState, pub at_ms: i64 }

  #[async_trait]
  pub trait StepTokenStore: Send + Sync {
      /// create-if-absent；created=false 时 token 是既有记录的值。
      async fn register(&self, instance_id: &str, step_id: &str, token: &str, now_ms: i64)
          -> Result<(StepRecord, bool), StoreError>;
      async fn mark_done(&self, instance_id: &str, step_id: &str) -> Result<(), StoreError>;
      async fn get(&self, instance_id: &str, step_id: &str) -> Result<Option<StepRecord>, StoreError>;
  }
  // TaskDispatcher（破坏性内部签名变更；所有实现与测试替身一并更新）：
  async fn dispatch(&self, service: &str, with: Option<&Value>, input: &Value,
                    idempotency_key: Option<&str>) -> DispatchResult;
  ```
- KV key：`/_workflow/v3/steps/{instance_id}/{step_id}`；value = `StepRecord` JSON；`register` = `Txn(Compare(Version==0) → Put)`，失败则读回既有记录。
- runtime 派生：`step_id = format!("{}:{}", inst.current_task_index, frame.task_name)`（v1 临时口径；v2 由分支/轮次/席位替换，见 ADR D2）；`token = format!("wf:{instance_id}:{step_id}")`（确定性 ⇒ 跨重试不变）。
- `DurableSupport::new(timers, outbox)` 签名不变（`steps: None`），新增 `with_steps(Arc<dyn StepTokenStore>)`；未注入时 dispatch 传 `None`，行为与现状一致。

- [ ] **Step 1: 写失败测试（KV 存储 CREATE-IF-ABSENT 与两态）**

  `coord/tests/workflow_steptoken.rs`（复用 `mod common` 的单节点装配，模式照抄 `spike3_durable_kv.rs` 的 `setup()`）：
  ```rust
  #[tokio::test(flavor = "multi_thread")]
  async fn register_is_create_if_absent_and_mark_done_is_visible() { /* register 两次：第二次 created=false 且 token 不变；get 见 Done */ }
  ```
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord --test workflow_steptoken -- --test-threads=1`
  Expected: FAIL（`step_kv`/trait 尚不存在，编译错）

- [ ] **Step 2: 实现 `durable.rs` 契约与 `KvStepStore`**

  `register` 的失败分支必须回读既有记录：`Txn` 失败且 `get` 命中 ⇒ 返回 `(record, false)`；`get` 未命中 ⇒ `Err(StoreError::IoError("step key vanished after CAS failure"))`（fail-closed，不返回伪造 token）。

- [ ] **Step 3: 扩展 dispatch 签名并在 HTTP 头注入幂等键**

  `HttpTaskDispatcher::dispatch_http(...)` 在既有自定义 headers 之后追加：`req = req.header("Idempotency-Key", key)`（仅 `Some` 时）。更新全部实现/替身（`NoopTaskDispatcher`、测试内匿名实现、`Arc<T>` 委托）。

- [ ] **Step 4: runtime 先登记后派发**

  两个派发点（普通 call 与 try 体内 call）：派发前 `let (rec, _) = steps.register(...).await`；`DispatchResult::Success` 后 `steps.mark_done(...)`；`Failure` 保持 `Pending`（重试复用同 token）。未注入 steps 时保持现状。

- [ ] **Step 5: 集成测试：重试复用同一幂等键**

  `coord/tests/workflow_steptoken.rs`：注入捕获型 dispatcher（记录每次 `idempotency_key`），定义 `call` 首次失败 + retry 一次后成功；断言两次派发的 key 非空且相等。Run: 同 Step 1。Expected: PASS。

- [ ] **Step 6: 回归 + 提交**

  Run: `cargo fmt --all -- --check && CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib workflow && CARGO_BUILD_JOBS=3 cargo test -p coord --test workflow_steptoken -- --test-threads=1`
  Expected: 全绿。Commit: `feat(workflow): step token store and idempotency key plumbing`

---

### Task 2: 同事务批量提交原语（状态 + N 条事件）

**Files:**
- Modify: `coord-core/src/workflow/durable.rs`（`AtomicOutboxCommit` 增批量方法；新增 `MemoryAtomicCommit` 供测试）
- Modify: `coord-agent/src/services/outbox_kv.rs`（批量实现；单条方法改为默认委托）
- Test: `coord/tests/spike3_durable_kv.rs`（保留既有断言，新增批量用例）

**Interfaces:**
- Produces:
  ```rust
  #[async_trait]
  pub trait AtomicOutboxCommit: Send + Sync {
      async fn commit_state_and_outbox(&self, inst: &WorkflowInstance, rec: &OutboxRecord,
                                       expected_mod_rev: Option<i64>) -> Result<bool, StoreError> {
          self.commit_state_and_outbox_batch(inst, std::slice::from_ref(rec), expected_mod_rev).await
      }
      /// 同一 Txn 提交状态 + recs（顺序即 sequence 顺序）；false = 什么都没写。
      async fn commit_state_and_outbox_batch(&self, inst: &WorkflowInstance, recs: &[OutboxRecord],
                                             expected_mod_rev: Option<i64>) -> Result<bool, StoreError>;
  }
  ```
- KV 语义：一次读 `outbox/seq/{agg}` 取 `seq`，记录按数组顺序分配 `seq+1..seq+n`；compares = `seq VALUE == cur` + 每条 `outbox/ids/{id} VERSION == 0` +（可选）实例 `MOD_REV == expected`；success 写实例 + seq+n + n 组 `ids/records/pending`。CAS 失败后：若**全部** id 已存在 ⇒ `Ok(true)`（幂等重提交）；否则重读 seq 重试（≤8 次）。
- `MemoryAtomicCommit::new(store: Arc<MemoryWorkflowStore>, outbox: Arc<MemoryOutboxStore>)`（仅测试/无 KV 装配；文档注明非原子跨存储、不得用于生产）。

- [ ] **Step 1: 写失败测试（两条记录同事务 + 幂等重提交）**

  `state_and_multiple_events_commit_atomically`：同一批 2 条事件 + 状态；断言两条 `sequence` 连续、`list_all` 各一条、state 可见。`batch_commit_is_idempotent_on_replay`：同批重放不新增记录、返回 true。Run: `CARGO_BUILD_JOBS=3 cargo test -p coord --test spike3_durable_kv -- --test-threads=1`。Expected: FAIL（方法不存在）。

- [ ] **Step 2: 实现批量 Txn 与 `MemoryAtomicCommit`**

  `KvOutboxStore` 提取私有 `fn outbox_ops(recs: &[OutboxRecord], seq_start: i64) -> Vec<RequestOp>` 与 compare 构造，供本任务与 Task 3 的 lease fenced 路径共用。

- [ ] **Step 3: 跑通并回归既有同事务用例**

  Run: 同 Step 1（含既有 `state_and_outbox_commit_is_all_or_nothing`）。Expected: 全绿。Commit: `feat(workflow): batch atomic state+outbox commit`

---

### Task 3: 运行时唯一写路径 `commit_instance`（drive 接线 + fenced 批量）

**Files:**
- Modify: `coord-core/src/workflow/ports.rs`（`LeaseGuard::fenced_commit`）
- Modify: `coord-agent/src/services/workflow_lease.rs`（`KvLeaseGuard` 持 `Arc<KvOutboxStore>` 并实现）
- Modify: `coord-agent/src/services/workflow.rs`（`KvInstanceLease::new` 增 outbox 参数；装配点同步）
- Modify: `coord-core/src/workflow/runtime.rs`（`persist_instance` → `commit_instance`；drive 内 `emit_lifecycle` 调用点收进提交）
- Test: `coord/tests/spike2_concurrency.rs`（新增 fenced+事件原子性用例）；`coord-core/src/workflow/runtime.rs` 测试模块

**Interfaces:**
- Produces:
  ```rust
  // ports.rs
  #[async_trait]
  pub trait LeaseGuard: Send + Sync {
      // …既有 renew/release/epoch/renew_interval/fenced_save 不变…
      /// fencing 保护下原子提交状态 + 伴生事件；events 为空等价 fenced_save。
      /// 默认 fail-closed（Err(Unsupported)）——不支持批量原子提交的后端不得静默分两次写。
      async fn fenced_commit(&self, inst: &WorkflowInstance, expected_mod_rev: i64,
                             events: &[OutboxRecord]) -> Result<bool, StoreError> {
          if events.is_empty() { return self.fenced_save(inst, expected_mod_rev).await; }
          Err(StoreError::Unsupported("fenced_commit with events is not supported".into()))
      }
  }

  // runtime.rs（唯一实例写路径；调用方拿 false ⇒ 不得静默吞掉）
  async fn commit_instance(&self, store: &Arc<S>, inst: &WorkflowInstance, rev: &mut Option<i64>,
                           guard: Option<&dyn LeaseGuard>, events: &[&str]) -> bool;
  ```
- `commit_instance` 选择顺序：① `guard` 存在 ⇒ `fenced_commit(inst, rev.unwrap_or(-1), &records)`；② `durable.atomic_commit` 存在 ⇒ `commit_state_and_outbox_batch(inst, &records, *rev)`；③ 无 durable ⇒ legacy：`save_instance_atomic`/`save_instance` + 逐条 `event_provider.emit`（注释标注 legacy，仅内存/测试路径）。
- 事件记录构造从 `emit_lifecycle` 中提取为纯函数：`fn lifecycle_data(inst) -> Value`、`fn lifecycle_record(inst, event_type, now_ms, id) -> OutboxRecord`；`emit_lifecycle` 删除或仅供 legacy 路径使用。
- `KvLeaseGuard::fenced_commit`：三重 compare（实例 `MOD_REV`、driver key `MOD_REV==epoch`、driver key `VALUE==token`）不变，success ops = 实例 put + 复用 Task 2 的 `outbox_ops`。

- [ ] **Step 1: 写失败测试（fenced 提交事件 + 陈旧驱动零写入）**

  `fenced_commit_writes_state_and_events_in_one_txn`；`stale_driver_fenced_commit_rejects_state_and_events`（撤销/重授租约后旧 guard 提交 ⇒ 返回 false，实例与 outbox 均无变化）。Run: `CARGO_BUILD_JOBS=3 cargo test -p coord --test spike2_concurrency -- --test-threads=1`。Expected: FAIL。

- [ ] **Step 2: 实现 `fenced_commit` 与共享 `outbox_ops`**

- [ ] **Step 3: 运行时改用 `commit_instance`，drive 内事件入同一提交**

  drive 内每个「改状态 + 发事件」点改为一次调用：`self.commit_instance(&store, &inst, &mut rev, guard, &[lifecycle::TASK_COMPLETED]).await`；两事件处传 `&[TASK_COMPLETED, WORKFLOW_RESUMED]`。提交返回 `false` 的语义：首写（Pending→Running）⇒ 退出 drive；其余 ⇒ 计 `cas_conflict_total` 并由下一轮重读重写。

- [ ] **Step 4: 内存路径回归**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib workflow && CARGO_BUILD_JOBS=3 cargo test -p coord --test spike3_durable_kv -- --test-threads=1`
  Expected: 全绿（含 emit append-only 回归）。Commit: `feat(workflow): single fenced commit path with same-txn events`

---

### Task 4: 非 drive 保存点全部收口（resume / timeout / fail / 父子恢复）

**Files:**
- Modify: `coord-core/src/workflow/runtime.rs`（`auto_resume`、`resume_by_event`、`apply_timeout`、`fail_instance`、`resume`、`resume_parent_after_subflow`；模块头注释列出仍允许的裸写点）
- Modify: `coord-core/src/workflow/errors.rs`（如缺：新增 `RuntimeError::StaleRevision(String)`）
- Test: `coord/tests/workflow_foundation.rs`（新建：并发 resume / 迟到写拒绝）

**Interfaces:**
- Consumes: Task 3 的 `commit_instance`。
- 控制面（无租约）用「线性一致读修订号 + CAS」：`load_instance_with_rev` 取 `rev`，`commit_instance(..., None, ...)`；CAS 冲突有界重试（3 次）后返回 `StaleRevision`（**不得**盲写覆盖）。
- 各路径事件：`auto_resume(wait)`/`resume_by_event`/`resume`/`resume_parent_after_subflow` → `[TASK_COMPLETED, WORKFLOW_RESUMED]`；`apply_timeout`/`fail_instance` → `[WORKFLOW_FAULTED]`。
- 仅允许保留的裸写（新键创建，无并发写同一键的问题）：`WorkflowRuntime::start` 的首写与输入校验失败写、`run` 子流程内联建子实例写。模块头注释逐条列出这 3 处；其余出现 `save_instance(` 即视为回归。

- [ ] **Step 1: 写失败测试（两个并发 resume 恰好一个生效）**

  `coord/tests/workflow_foundation.rs`：Suspend 一个实例后 `tokio::join!(resume(...), resume(...))`（不同 idempotency_key）；断言恰一个 `Ok`、另一个 `StaleRevision`/`AlreadyCompleted`，且 outbox 中 `workflow.resumed` 恰好一条。Run: `CARGO_BUILD_JOBS=3 cargo test -p coord --test workflow_foundation -- --test-threads=1`。Expected: FAIL（当前裸写 ⇒ 两条事件/双双 Ok）。

- [ ] **Step 2: 逐个保存点改为 `commit_instance`**

- [ ] **Step 3: 裸写审计**

  Run: `grep -n "save_instance(" coord-core/src/workflow/runtime.rs`
  Expected: 仅 helper 内部 + 模块头注释列出的 3 处新键创建；其余为 0。

- [ ] **Step 4: 回归 + 提交**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core && CARGO_BUILD_JOBS=3 cargo test -p coord --test workflow_foundation --test spike3_durable_kv -- --test-threads=1`
  Expected: 全绿。Commit: `feat(workflow): route all save points through cas commit`

---

### Task 5: 启动对账全覆盖（Pending/Running 重驱动）+ 恢复先于投递

**Files:**
- Modify: `coord-core/src/workflow/runtime.rs`（`reconcile_resumable_instances`）
- Modify: `coord-agent/src/services/workflow.rs`（仅加注释锁定 `start()` 既有顺序：`recover_durable_state()` → `start_durable_workers()` → relay；顺序已正确，本任务不得回退）
- Test: `coord/tests/workflow_foundation.rs`

**Interfaces:**
- 对账规则（按状态分派，全部经 `spawn_drive` ⇒ 租约互斥）：
  - `Pending | Running` ⇒ 加载定义（hash 校验见 Task 7）后 `spawn_drive`；
  - `Waiting` reason `listen` ⇒ `spawn_event_wait`；reason `wait|retry` ⇒ 补写定时器（现状保留）；
  - `Suspended`（signal）⇒ 不自动驱动（人工语义），仅 `tracing::debug!` 记录（ADR 的「所有非终态」在此显式排除并写明原因）；
  - 终态 ⇒ 跳过。
- relay 就绪顺序（R4）：`recover_durable_state()` 完成后才启动 relay —— 现状已满足（`services/workflow.rs` 的 `start()`），以注释固化，改动回归即视为判据破坏。

- [ ] **Step 1: 写失败测试（Running 实例重启后被重驱动）**

  `running_instance_is_redriven_after_restart`：KV 装配下建实例，直接写回一个 `Running` 且无 `drivers/` 键的实例快照；重建 `WorkflowEngineService` 并 `start()`；断言实例在超时内到达终态且事件不丢。Run: `CARGO_BUILD_JOBS=3 cargo test -p coord --test workflow_foundation -- --test-threads=1`。Expected: FAIL（当前只对账 Waiting）。

- [ ] **Step 2: 实现对账分派与启动顺序**

- [ ] **Step 3: 进程级恢复证据复跑**

  Run: `SPIKE3_SKIP_BUILD=1 bash spike/2026-10/spike-3-recovery/scripts/repro.sh`
  Expected: wait/listen/retry 三场景 PASS，`missing_event_ids=0`、`duplicate_event_ids=0`。Commit: `feat(workflow): reconcile all non-terminal instances and order recovery before relay`

---

### Task 6: listen 投递原子化（envelope + ack-after-state）

**Files:**
- Modify: `coord-core/src/workflow/ports.rs`（`EventDelivery`；`EventProvider::wait_for_event` 返回 delivery；新增 `ack_event`）
- Modify: `coord-core/src/workflow/ports.rs`（`MemoryEventProvider` 实现；如独立文件则同步）
- Modify: `coord-agent/src/services/mq_event_provider.rs`（命中即返回 delivery、**不**提交游标；`ack_event` 提交 `offset+1`）
- Modify: `coord-core/src/workflow/runtime.rs`（`spawn_event_wait`：`resume_by_event` 成功后 `ack_event`）
- Test: `coord-agent/src/services/mq_event_provider.rs` 测试模块；`coord/tests/workflow_foundation.rs`
- Modify: `coord/tests/spike3_durable_kv.rs`（凡用 `wait_for_event` 的断言同步签名）

**Interfaces:**
- Produces:
  ```rust
  pub struct EventDelivery { pub event_type: String, pub subscription: String, pub offset: u64 }
  // EventProvider：wait_for_event(...) -> Option<EventDelivery>；ack_event(&self, d: &EventDelivery)
  ```
- 语义：**消费位点提交延后到实例状态写成功之后**。崩溃窗口从「事件丢失」变为「重复投递」；`resume_by_event` 的状态守卫（非 `Waiting`/reason 不匹配即返回）吸收重复，`ack_event` 随后提交位点。不匹配事件只推进本地扫描、不提交位点（下次重扫无害）。

- [ ] **Step 1: 写失败测试（重复投递不重复推进；位点不提前提交）**

  在 `mq_event_provider.rs`：`delivery_is_acked_only_after_explicit_ack`（wait 命中后重启 provider，事件仍可再次取到；ack 后不再取到）。Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-agent --lib services::mq_event_provider`。Expected: FAIL。

- [ ] **Step 2: 改 trait 与两个实现，同步 runtime**

- [ ] **Step 3: 集成回归（listen 重订阅 + kill9）**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord --test spike3_durable_kv -- --test-threads=1` 与 `SPIKE3_SKIP_BUILD=1 bash spike/2026-10/spike-3-recovery/scripts/repro.sh`
  Expected: 全绿，kill9 listen 场景 PASS。Commit: `feat(workflow): ack listen events after state commit`

---

### Task 7: 定义钉死（definition_hash / engine_version + 漂移 fail-closed）

**Files:**
- Modify: `coord-core/src/workflow/model.rs`（`WorkflowInstance` 增两个 `Option` 字段；`WorkflowInstance::new` 填 `Some`；`ENGINE_VERSION` 常量 = `"v1"`；`definition_hash(def)` 规范 JSON+sha256）
- Modify: `coord-core/src/workflow/runtime.rs`（`assert_definition_pin(inst, def)`；`drive` 开头与 `resume`/`auto_resume`/`resume_by_event` 加载定义后调用；不匹配 ⇒ 实例置 `Faulted` + `DefinitionDrift` 诊断、控制面返回 `RuntimeError`）
- Modify: `coord-core/src/workflow/errors.rs`（新增 `RuntimeError::DefinitionDrift { instance_id, expected, actual }`）
- Test: `coord-core/src/workflow/model.rs` 单测；`coord/tests/workflow_foundation.rs`

**Interfaces:**
- 字段（向后兼容：旧快照 `None` = legacy 未钉死，首次成功驱动时回填）：
  ```rust
  #[serde(default, skip_serializing_if = "Option::is_none")] pub definition_hash: Option<String>,
  #[serde(default, skip_serializing_if = "Option::is_none")] pub engine_version: Option<String>,
  pub const ENGINE_VERSION: &str = "v1"; // M2 引入 v2 后按分支值分派
  pub fn definition_hash(def: &WorkflowDefinition) -> String;   // sha256(hex) of canonical JSON
  ```
- `definition_hash` 必须对 map 插入顺序不敏感：序列化为 `serde_json::Value` 后按 key 递归排序再 `to_vec`，`sha2::Sha256` 取 hex。
- 不匹配的稳定诊断锚点：`definition drift: instance pinned {expected}, loaded {actual}`（测试逐字断言）。
- **非目标（M2）**：同 `(ns,name,version)` 重部署内容覆盖的拒绝（改 `save_definition_atomic` 语义，属行为变更，需契约治理）。

- [ ] **Step 1: 写失败测试（hash 稳定 + 漂移 fail-closed）**

  `definition_hash_is_stable_across_map_order`（同一文档两种 map 插入顺序 ⇒ 同 hash）；`resume_after_redeploy_same_version_fails_closed`（KV：部署→start→同版本改内容重部署→resume ⇒ `RuntimeError::DefinitionDrift`，实例 `Faulted`，事件含 `workflow.faulted`）。Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib workflow::model && CARGO_BUILD_JOBS=3 cargo test -p coord --test workflow_foundation -- --test-threads=1`。Expected: FAIL。

- [ ] **Step 2: 实现字段、hash 与 guard；串联所有加载定义入口**

- [ ] **Step 3: 回归 + 提交**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core && CARGO_BUILD_JOBS=3 cargo test -p coord --test workflow_foundation -- --test-threads=1`
  Expected: 全绿。Commit: `feat(workflow): pin definition hash and fail closed on drift`

---

### Task 8: 未实现构造 fail-closed + Spike 4 语料回归

**Files:**
- Modify: `coord-core/src/workflow/sw.rs`（解析期拒绝）与/或 `coord-core/src/workflow/validate.rs`
- Create: `coord/tests/spike4_conformance.rs`
- Modify: `spike/2026-10/spike-4-conformance/REPORT.md`（如实现状态变化，只在「立即」条目标注落地锚点；不改历史结论）

**Interfaces:**
- 拒绝清单（部署即 `ParseError`/校验错误，错误信息含构造名，禁止静默丢弃）：
  - `run: script | container | shell | await | return`（`run.workflow` 保持现行子流程语义）；
  - `use.extensions`（含 `before`/`after`）；
  - 非 switch 上下文的 `then`（官方 switch 条件缺 target 的 GAP 也走显式拒绝，不猜测）。
- CI 判据：语料 12/13/14/17 必须被拒绝；15/16/21/23/24 行为与 `matrix.tsv` 的「PASS/拒绝」一致；任何「接受但语义不同」= 测试失败。

- [ ] **Step 1: 写失败测试（语料 12/13/14/17 当前被接受）**

  `coord/tests/spike4_conformance.rs` 直接读 `../../spike/2026-10/spike-4-conformance/corpus/12-run-script.yaml` 等文件（路径用 `env!("CARGO_MANIFEST_DIR")` 拼），调用现有解析入口断言 `is_err()` 且错误串包含 `run.script`/`run.container`/`run.shell`/`use.extensions`。Run: `CARGO_BUILD_JOBS=3 cargo test -p coord --test spike4_conformance -- --test-threads=1`。Expected: FAIL。

- [ ] **Step 2: 实现显式拒绝并逐条对齐 corpus**

- [ ] **Step 3: 提交**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord --test spike4_conformance --test spike3_durable_kv -- --test-threads=1`
  Expected: 全绿。Commit: `feat(workflow): fail closed on unimplemented sw constructs`

---

### Task 9: outbox 保留清理（守卫式 prune）

**Files:**
- Modify: `coord-core/src/workflow/durable.rs`（`OutboxStore::prune` 默认实现 `Ok(0)`；`RelayPolicy.retention_ms` 默认 24h；`RelayReport.pruned`；relay tick 调用）
- Modify: `coord-agent/src/services/outbox_kv.rs`（KV 实现）
- Test: `coord/tests/spike3_durable_kv.rs`

**Interfaces:**
- 删除条件（缺一不可）：`status ∈ {Delivered, Dead}` 且 `delivered_at_ms(or created_at_ms) + retention_ms < now_ms`；只删 `records/`、`ids/`、`pending/`（pending 不应存在），**不删** `outbox/seq/{agg}`（序号单调必须跨清理保持）；单批上限 + 同日多次 tick 幂等。
- 审计：prune 不产生事件（内部清理），但 `RelayReport.pruned` 必须可被指标出口读取。

- [ ] **Step 1: 写失败测试（pending 永不删 / delivered 过期删 / 序号不回退）**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord --test spike3_durable_kv -- --test-threads=1`。Expected: FAIL。

- [ ] **Step 2: 实现 prune 并接入 relay tick**

- [ ] **Step 3: 提交**

  Run: 同 Step 1。Expected: 全绿。Commit: `feat(workflow): guarded outbox retention pruning`

---

### Task 10: durable 装配可配置（默认关闭）

**Files:**
- Modify: `coord-agent/src/service.rs`（`ServicesConfig`：`workflow_durable: bool` 默认 `false`；`workflow_events_topic: String` 默认 `com.china-workflow.events`，均 `#[serde(default)]`）
- Modify: `coord-agent/src/lib.rs`（`config.services.workflow && workflow_durable && inner.is_some()` ⇒ `new_with_kv_store_durable`，同时注入 `KvInstanceLease` 与 `KvStepStore`；否则保持现状）
- Modify: `config.example.toml`（追加注释块，说明默认关闭与前置条件）
- Test: 配置默认值单测；`coord-agent` 既有装配测试回归

**Interfaces:**
- 装配矩阵：`workflow=false` ⇒ 不注册；`workflow=true, durable=false` ⇒ 现有 `new_with_kv_store_and_dispatch`（不注入租约/outbox）；`workflow=true, durable=true, 有 server` ⇒ durable 路径（KvOutboxStore + MqEventProvider + KvCursorStore + KvInstanceLease + KvStepStore + relay）；无 server ⇒ 内存路径 + `warn!`。
- 不改变任何默认行为；durable 路径关闭时不得要求 MQ 存在。

- [ ] **Step 1: 写失败测试（默认值 false + durable 关闭不触 MQ）**

- [ ] **Step 2: 接线并更新 `config.example.toml`**

- [ ] **Step 3: 提交**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-agent --lib && bash scripts/check-doc-refs.sh`
  Expected: 全绿。Commit: `feat(agent): config-gated durable workflow assembly`

---

### Task 11: M1 验收（全量测试 + 进程级证据 + 契约零改动核实）

**Files:**
- Test: 全仓；证据：`spike/2026-10/spike-3-recovery/scripts/repro.sh` 输出

- [ ] **Step 1: 全量测试与静态检查**

  Run:
  ```bash
  cargo fmt --all -- --check
  cargo clippy --all-targets --all-features
  CARGO_BUILD_JOBS=3 cargo test -p coord-core
  CARGO_BUILD_JOBS=3 cargo test -p coord-agent
  CARGO_BUILD_JOBS=3 cargo test -p coord --test spike2_concurrency --test spike3_durable_kv --test workflow_foundation --test workflow_steptoken --test spike4_conformance -- --test-threads=1
  bash scripts/check-doc-refs.sh
  ```
  Expected: 全绿；无 clippy warning-as-error 新增。

- [ ] **Step 2: 进程级 kill -9 三场景**

  Run: `SPIKE3_SKIP_BUILD=1 bash spike/2026-10/spike-3-recovery/scripts/repro.sh`
  Expected: wait/listen/retry 三场景 PASS，`missing_event_ids=0`、`duplicate_event_ids=0`、`outbox_pending=0`。

- [ ] **Step 3: 契约零改动核实**

  Run: `git diff --stat spike/2026-10-workflow-v2..HEAD -- apis/`
  Expected: 空输出。

- [ ] **Step 4: 里程碑判定记录**

  在 `spike/2026-10/README.md` 追加「阶段二 M1 落地状态」小节（判据 → 测试名 → 结论），**不**改历史证据；提交。Commit: `docs(workflow): record m1 foundation closure verdicts`

---

## 里程碑判据（M1）

| 判据 | 测试/证据 |
|:--|:--|
| 并发驱动/控制面：恰一胜、败者零写入 | `workflow_foundation`（并发 resume）、`spike2_concurrency`（fenced 拒绝迟到写） |
| 状态 + 伴生事件同 Txn | `spike3_durable_kv::state_and_multiple_events_commit_atomically`、fenced 用例 |
| 外部副作用幂等键先登记后派发、跨重试不变 | `workflow_steptoken` |
| 崩溃恢复：wait/listen/retry + Running 重驱动 | `workflow_foundation::running_instance_is_redriven_after_restart`、kill9 repro 三场景 |
| 定义漂移 fail-closed | `workflow_foundation::resume_after_redeploy_same_version_fails_closed` |
| 未实现构造 fail-closed | `spike4_conformance` |
| 对外契约零改动 | Task 11 Step 3 |
