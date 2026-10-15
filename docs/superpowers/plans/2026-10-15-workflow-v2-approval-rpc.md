# Workflow v2 审批接线（M2-C）Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把 M2-B 已落成的 v2 审批模型/存储接到对外可用：proto 增量 RPC、原生 `approve` DSL 节点、runtime 挂起/恢复桥、引擎级授权、事件契约固化（`.v1` + JSON Schema）、查询面与 Java/Rust 客户端。

**Architecture:** 一个 workflow 实例最多一个 v2 审批 `Engine`（实例 id = workflow 实例 id，懒创建）。workflow 走到 `approve` 节点时由 runtime 桥（`coord-core/src/workflow/approval/bridge.rs`）调 `Engine::{start,enter_branch,open_approval}` 并以 `ApprovalStore::commit`（v4 键空间 + v3 outbox 同 Txn）持久化，workflow 置 `Suspended`（人工挂起，`expected_signal = "approval.completed"`）；外部 RPC 命令由 `coord-agent` 的 `WorkflowEngineService` 读 CCT 身份、经桥加载/授权/执行/提交，终态（completed/rejected/cancelled/terminated）由桥回写 `Runtime::resume_approval` 恢复 workflow（rejected 走 `on_reject` 目标，未配置则 fault）。deadline 由持久化定时器驱动 `Engine::expire`（仅置过期、绝不自动通过）。

**Tech Stack:** Rust（coord-core / coord-agent / coord-proto / coord-client）、tonic/prost、buf、serde、JS/JSON Schema（契约 schema）、Java 21 + protobuf-maven-plugin（coord-java-sdk）、Vitest/TanStack（coord-ui，仅评估）。

**Spec:** `docs/adr/0013-workflow-approval-v2.md`（D2/D4/D7/D8/D10 + 落地清单 1–8）；语义模型证据 `spike/2026-10/spike-1-state-model/`（只读）；M2-B 计划 `docs/superpowers/plans/2026-10-14-workflow-v2-model-storage.md`。

## 本计划锁定的决定（实现者逐字遵守；偏差须走评审裁定）

1. **审批实例 id = workflow 实例 id**（确定、可查询、可对账）；一个 workflow 实例懒创建一个 v2 `Engine`，跨多个 `approve` 节点复用（`node_attempts` 白名单支撑 `return_to_node`）。
2. **挂起协议**：新 `SuspendReason::WaitingForApproval { approval_instance_id, node }`；`SuspensionMeta` 增 `approval_instance_id: Option<String>`（`#[serde(default, skip_serializing_if = "Option::is_none")]`）；`expected_signal = "approval.completed"`；状态 `Suspended`（人工；`reconcile_resumable_instances` 不得重驱动）。
3. **恢复协议**：新 `WorkflowRuntime::resume_approval(instance_id, outcome: Value, next_target: Option<&str>)`。`Completed` → `next_target=None`（推进到下一任务）；`Rejected/Cancelled/Terminated` → 有 `on_reject` 则 `next_target=Some(target)`，否则 fault（`WorkflowFault`，错误码 `approval_rejected`）。
4. **DSL 节点**（原生、非 SW extensions）：
   ```yaml
   - finance:
       approve:
         approvers: [alice, bob]          # 静态名单（可选，与 pool/readers 至少一项非空）
         pool: [carol]                    # 候选池（可选）
         readers: [dave]                  # 阅签（可选）
         roles: [finance]                 # 角色来源（可选；经 ApproverResolver 解析，未配置 ⇒ fail-closed）
         mode: all                        # all|any|quorum:<n>|ratio:<num>/<den>，默认 all
         veto: true                       # 默认 true = AnyReject 一票否决；false = OnExhausted
         deadline: {hours: 24}            # 可选，官方 duration 形状/ISO8601 字符串（复用 parse_wait 口径）
         on_pass: notify                  # 可选；默认 = 顺序下一任务
         on_reject: rework                # 可选；缺省 = workflow fault
         sign_admins: [admin]             # 可选；加签/减签/驳回重开/终止的授权名单
         allow_self_approve: false        # 默认 false（发起人不可自批）
   ```
   未知键一律 `UnsupportedConstruct`（fail-closed）；`on_pass`/`on_reject` 目标必须存在于顶层任务名（`check_task_refs` 覆盖）。
5. **授权矩阵**（引擎已有 `actor_can_act` 保留；桥补职责分离）：
   - `approve/reject/acknowledge/claim/transfer/delegate`：引擎 `actor_can_act` + `actor != initiator`（`allow_self_approve=false` 时；`initiator == "anonymous"` 跳过该检查）。
   - `add_sign/remove_sign`：`actor ∈ branch.policy.sign_admins` 或 `actor == initiator`。
   - `return_to_node`：当前分支任一 open 席位持有者（assignee/candidate/delegate）或 `sign_admins`/initiator。
   - `withdraw`：initiator（引擎自身强制）。
   - `terminate`：`actor ∈ sign_admins`。
   - `expire`：仅桥（定时器）调用，无 RPC。
   - `Branch` 增 `policy: ApprovalPolicy { allow_self_approve: bool, sign_admins: Vec<Principal> }`（serde 默认值），`enter_branch` 增 `policy` 参数。
6. **事件契约**：7 个类型追加 `.v1`（`com.china-workflow.instance.returned|cancelled|terminated|completed|rejected.v1`、`com.china-workflow.approval.round.passed|rejected.v1`），catalog == engine emit 字面量（含 `finish` 的兜底 arm）；schema 落 `apis/contracts/schemas/events/<type>.json`，`$id` == `https://schemas.coord.dev/events/<type>.json`（`dataschema_for` 产物），由 `scripts/check-event-schemas.sh` 门禁。
7. **超时语义**：`Engine::expire` 只置 `Expired` 并从 `total_count` 剔除；**全部席位过期 ⇒ 轮次保持 Open、绝不自动通过/驳回**（修 `recount`：只有"从未创建过可计票席位"的空轮次才按通过处理）。处置由人（`add_sign`/`terminate`）决定——对应 Q4「无自动判定」。
8. **幂等**：命令请求带 `idempotency_key`；桥先查 `load_idem`，命中直接返回缓存响应（与状态+事件**同一 Txn** 写入 `/_workflow/v4/idempotency/{instance}/{key}`）；未命中则 CAS 提交时携带 `IdemRecord{key, response}`。
9. **身份**：actor 一律取 `request.extensions().get::<GatewayIdentity>()` 的 `subject`（空 ⇒ `"anonymous"`），**不读请求体 actor 字段**；v1 `WorkflowInstance` 增 `initiator: Option<String>`（serde default），Start RPC 注入；审批 `StartSpec.initiator` = 该字段（None ⇒ `"anonymous"`）。
10. **默认关闭**：审批命令面沿用 `workflow_durable` 开关语义；未启用 KV/durable 装配时审批 RPC 一律 `FAILED_PRECONDITION("workflow durable engine disabled")`，内存路径仅供测试。approve 节点在非 durable 装配下执行 ⇒ 节点进入即 fault（fail-closed，不静默降级为无审批继续）。
11. **proto 契约**：`apis/contracts/proto/coord/workflow/v1/workflow.proto` 与 `coord-proto/src/proto/workflow.proto` 逐字一致；12 个 RPC（Approve/Reject/Acknowledge/AddSign/RemoveSign/ReturnToNode/Transfer/Delegate/Withdraw/Terminate/ListPendingTasks/GetInstanceDetail）；命令 → `coord:workflow:execute`，查询 → `coord:workflow:read`；`StaleRevision → ABORTED`、`NotAuthorized → PERMISSION_DENIED`、`InvalidTransition → FAILED_PRECONDITION`。
12. **SDK 范围**：coord-java-sdk 与 coord-client 实现审批包装；coord-ui 只做影响评估（UI 走 REST facade，本里程碑不建 facade，记入 carry）。

## Global Constraints

- **v1 行为不回退**：不用 `approve` 节点的定义/实例行为逐字不变；`cargo test -p coord-core`、`-p coord-agent`、`-p coord` 全绿。
- **唯一写路径**：approval 状态只经 `ApprovalStore::commit(_with_idem)`；workflow 状态只经 `commit_instance`/`commit_control_plane`；禁止裸写。
- **fail-closed**：未知 DSL 键、未解析角色、无审批存储、CAS 失败、陈旧 revision、终态变更一律拒绝且留痕；不得"先写状态后补事件"。
- **冻结证据只读**：`spike/2026-10/**` 零改动；`apis/` 只按本计划增 proto/schema/docs。
- **契约同步**：改 proto 必须两份同改并跑 `check-wire-sync.sh`、`check-wire-descriptor.sh`、`buf lint/breaking`。
- **注释规则**（AGENTS.md）：只写当前「为什么」；无日期/无章节号/无变更史；不新增 `TODO/FIXME`；临时文件只放 `/tmp`。
- **门禁**：每个任务收尾跑 `cargo fmt --all -- --check` 与 `bash scripts/check-doc-refs.sh`；整分支收尾跑 AGENTS.md 全套（fmt/clippy/panic/audit/deny）。
- 依赖只用现有 workspace 依赖；不新增 crate。

## Review Focus

1. **超时绝不自动放行**：全员过期/减签至 0 席时轮次必须保持 Open（`recount` 的 `total==0` 分支只允许"从未有可计票席位"的阅签轮通过）——Task 2 测试钉死。
2. **幂等单写**：同 `idempotency_key` 重放返回逐字节相同响应、事件/状态只写一次（跨进程重启后仍成立）——Task 4 单测 + Task 12 e2e 钉死。
3. **重复进入不双开**：approve 节点在"持久化审批状态后、提交 workflow 挂起前"崩溃后重驱动，`ensure_node_open` 必须复用既有 open 分支而不是再开一轮——Task 8 测试钉死。
4. **身份不可伪造**：命令 actor 只来自 CCT；请求体无 actor 字段；非责任人的投票/加签/终止被拒——Task 7/11/12 测试钉死。
5. **失败不静默**：非 durable 装配下 approve 节点必须 fault；未解析角色必须拒绝；未知 DSL 键必须拒绝——Task 5/6/11 测试钉死。
6. **契约与事件一致性**：catalog 与 engine emit 字面量逐字一致（改名后测试仍钉）；JSON Schema `$id` == `dataschema_for` 输出——Task 3 门禁钉死。
7. **v1 回归**：不使用 approve 的既有定义/drive/resume/cancel/对账路径行为逐字不变——每任务回归 + 终审。

---

### Task 1: 契约层——proto 增量 + 能力表

**Files:**
- Modify: `apis/contracts/proto/coord/workflow/v1/workflow.proto`
- Modify: `coord-proto/src/proto/workflow.proto`（逐字镜像）
- Modify: `coord-core/src/grpc_auth.rs`（`rpc_capability` 表 + 负控制测试）
- Test: `coord-core/src/grpc_auth.rs`（`mod tests`）

**Interfaces:**
- Consumes: 无。
- Produces: 12 个生成的 RPC 方法/消息类型（`coord_proto::workflow::v1::*`，兼容重导出 `coord_proto::agent::*`），供 Task 11/12/14/15 消费。新增消息（字段号从 1 连续，命令请求都含 `string idempotency_key` 末字段）：
  - `WorkflowApproveRequest{workflow_id=1,task_id=2,comment=3,idempotency_key=4}`；`WorkflowRejectRequest` 同形；`WorkflowAcknowledgeRequest{workflow_id=1,task_id=2,idempotency_key=3}`。
  - `WorkflowAddSignRequest{workflow_id=1,branch_id=2,repeated string approvers=3,repeated string pool=4,repeated string readers=5,comment=6,idempotency_key=7}`。
  - `WorkflowRemoveSignRequest{workflow_id=1,branch_id=2,repeated string task_ids=3,comment=4,idempotency_key=5}`。
  - `WorkflowReturnToNodeRequest{workflow_id=1,branch_id=2,target_node=3,comment=4,idempotency_key=5}`。
  - `WorkflowTransferRequest{workflow_id=1,task_id=2,to=3,comment=4,idempotency_key=5}`；`WorkflowDelegateRequest` 同形。
  - `WorkflowWithdrawRequest{workflow_id=1,comment=2,idempotency_key=3}`；`WorkflowTerminateRequest{workflow_id=1,reason=2,idempotency_key=3}`。
  - 每命令独立响应消息（buf STANDARD `RPC_REQUEST_RESPONSE_UNIQUE`；`apis/contracts/buf.yaml` 的豁免栏只给既有 RPC，不为新面放宽）：`WorkflowApproveResponse`/`WorkflowRejectResponse`/`WorkflowAcknowledgeResponse`/`WorkflowAddSignResponse`/`WorkflowRemoveSignResponse`/`WorkflowReturnToNodeResponse`/`WorkflowTransferResponse`/`WorkflowDelegateResponse`/`WorkflowWithdrawResponse`/`WorkflowTerminateResponse`，各 `{instance_id=1,status=2}`（status 大写：`RUNNING/COMPLETED/REJECTED/CANCELLED/TERMINATED`）。
  - `WorkflowListPendingTasksRequest{page_size=1,page_token=2}`；`WorkflowListPendingTasksResponse{repeated PendingApprovalTask tasks=1,next_page_token=2}`；`PendingApprovalTask{instance_id=1,task_id=2,node=3,branch_id=4,round_id=5,kind=6,status=7,assignee=8,deadline_ms=9,created_at_ms=10}`。
  - `WorkflowGetInstanceDetailRequest{workflow_id=1}`；`WorkflowGetInstanceDetailResponse{instance_id=1,status=2,initiator=3,revision=4,definition_name=5,repeated ApprovalBranch branches=6,repeated ApprovalTask tasks=7,repeated ApprovalRound rounds=8}`；`ApprovalBranch{id=1,node=2,attempt=3,status=4,join=5,reject=6,repeated string round_ids=7}`；`ApprovalTask{id=1,branch_id=2,round_id=3,kind=4,status=5,assignee=6,repeated string candidates=7,deadline_ms=8}`；`ApprovalRound{id=1,branch_id=2,number=3,status=4,total_count=5,completed_count=6,rejected_count=7}`。
- 能力表新增（`grpc_auth.rs:202-215` 后）：
  - 命令 10 个 → `coord:workflow:execute`；`ListPendingTasks`/`GetInstanceDetail` → `coord:workflow:read`。

- [ ] **Step 1: 写能力表负控制测试（先红）**

  在 `grpc_auth.rs` `mod tests` 增 `workflow_v2_approval_rpcs_are_registered`：逐一断言 12 个路径映射到预期能力；并断言未登记路径（如 `/coord.workflow.v1.Workflow/Approve` 拼错为其它 case）返回 `None`（fail-closed 语义）。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib grpc_auth::tests::workflow_v2_approval_rpcs_are_registered`。Expected: FAIL。

- [ ] **Step 2: 两份 proto 同改 + 能力表登记**

  先改 `apis/contracts/proto/...`，再机械复制到 `coord-proto/src/proto/workflow.proto`（`cmp` 逐字节一致）。RPC 追加在 `DeleteInstance` 之后。同时在 `coord-agent/src/services/grpc_handlers.rs` 的两个 `impl Workflow` 块（legacy `WorkflowService` 与 `WorkflowEngineService`）各补 12 个 `Status::unimplemented` 桩（禁止 panic）：否则新 trait 方法使中间提交无法编译；Task 11 替换 engine service 的桩体。
  Run: `cargo build -p coord-proto`（生成通过）；`cargo check -p coord-agent`（exit 0）；`cmp apis/contracts/proto/coord/workflow/v1/workflow.proto coord-proto/src/proto/workflow.proto`（无输出）。
  契约脚本：`bash apis/contracts/scripts/check-wire-sync.sh`、`bash apis/contracts/scripts/check-wire-descriptor.sh`（protoc 已装）。
  `buf lint`/`buf breaking`：本机 sandbox 下 buf 二进制 segfault（exit 139），本地不可执行——由 CI `contract-check` 在 PR 上覆盖；proto 手写必须守 STANDARD（字段 snake_case、命名不冲突），**不得**声称本地跑过 buf。

- [ ] **Step 3: 能力表测试转绿 + 全量回归**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib grpc_auth`；`cargo check -p coord-agent`；`cargo fmt --all -- --check`。
  Commit: `feat(workflow): add approval v2 RPC contract and capabilities`

---

### Task 2: 引擎硬化 carry（超时语义 / 减签重复 / 阅签收口）

**Files:**
- Modify: `coord-core/src/workflow/approval/engine.rs`
- Test: `coord-core/tests/approval_scenarios.rs`（追加）、`coord-core/src/workflow/approval/engine.rs`（`mod tests` 若有）

**Interfaces:**
- Consumes: M2-B `Engine`（不变签名，除本任务明确无签名变化）。
- Produces: 修正后的 `recount` 判定（无 API 变化）；`remove_sign` 去重/终态跳过（无 API 变化）。

- [ ] **Step 1: 写失败测试**

  1) `all_seats_expired_keeps_round_open`：1 席 + deadline；`open_approval` 后 `expire(now=deadline)`；断言 round 仍 `Open`、instance 仍 `Running`、`total_count==0`、无 `round.passed` 事件。
  2) `remove_all_unvoted_seats_keeps_round_open`：2 席未投票，`remove_sign` 两席后 round 仍 `Open`（不得因 total==0 通过）。
  3) `reader_only_round_still_passes`：仅 ReadAck 席 ⇒ 立即 `Passed`（既有语义不回退）。
  4) `remove_sign_duplicate_targets_rejected`：`remove_sign([X, X])` 返回 `InvalidArgument`（在 `guard` 之后、任何变更之前）。
  5) `remove_sign_skips_terminal_targets`：目标已 `Completed` ⇒ 该目标零效果（不产生 `Cancelled→Cancelled` 冗余 Effect），其余 open 目标正常取消；返回 `Ok`。
  6) `round_close_cancels_open_read_ack_seats`：驳回重开/轮次关闭时，ReadAck 席与 Approval 席一并 `Cancelled`（不再残留 open）。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --test approval_scenarios`。Expected: FAIL（1/2/4/5/6 红）。

- [ ] **Step 2: 最小实现**

  `recount`：把 `total == 0` 通过条件改为「`approvals.is_empty()`（该轮从未创建可计票席位）」；`total > 0 && completed >= total`；耗尽驳回分支加 `total > 0` 守卫。`remove_sign`：入口 `task_ids` 排序去重，重复 ⇒ `InvalidArgument("task_ids")`；跳过非 open 目标。`cancel_pending_tasks`：不再按 `TaskKind::Approval` 过滤，取消该轮全部 open 席位。
  Run: 同 Step 1 + `cargo test -p coord-core --lib workflow::approval`（M2-B 23 场景不回退）。

- [ ] **Step 3: 回归并提交**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core`；`cargo fmt --all -- --check`。
  Commit: `fix(workflow): expired seats never auto-pass and round close cancels all open seats`

---

### Task 3: 事件契约固化（`.v1` + JSON Schema + 门禁）

**Files:**
- Modify: `coord-core/src/workflow/approval/events.rs`（catalog）
- Modify: `coord-core/src/workflow/approval/engine.rs`（7 处 emit 字面量 + `finish` 兜底 arm）
- Modify: `coord-core/src/workflow/approval/model.rs`（serde 夹具中的事件名）
- Create: `apis/contracts/schemas/events/com.china-workflow.instance.returned.v1.json`（及另外 6 个，共 7）
- Create: `apis/contracts/schemas/events/README.md`
- Create: `scripts/check-event-schemas.sh`
- Modify: `.github/workflows/ci.yml`（lint job 中 doc-refs 门禁后加一步）

**Interfaces:**
- Consumes: `coord_core::workflow::durable::{EVENT_TYPE_NAMESPACE, DATASCHEMA_PREFIX, dataschema_for}`。
- Produces: 固化事件名（下述 7 个，Task 7 构造 outbox 记录时使用）：
  `com.china-workflow.instance.returned.v1`、`...cancelled.v1`、`...terminated.v1`、`...completed.v1`、`...rejected.v1`、`com.china-workflow.approval.round.passed.v1`、`...round.rejected.v1`。

- [ ] **Step 1: 写失败测试**

  `events.rs` 现有 `all_emitted_event_names_are_in_catalog` 保持；追加 `catalog_matches_v1_suffix_convention`：每个目录项以 `.v1` 结尾且以 `com.china-workflow.` 开头。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib workflow::approval::events`。Expected: FAIL。

- [ ] **Step 2: 改名 + schema + 门禁**

  全量改名 catalog 与 engine emit 字面量（含 `finish` 的 `status_changed` 兜底 arm，保证 catalog==emit 断言可判别），并同步所有测试中引用旧名的断言（Task 2 新增测试的事件名断言一并改名）。7 个 JSON Schema（draft 2020-12）核心字段：`id/aggregate_id/sequence/event_type/specversion/source/subject/dataschema/time/data`（记录序列化键为 `event_type`，非 CloudEvents 概念属性 `type`）；`data` 含 `instance_id`（`instance.*`）或 `instance_id/branch_id/round_id/total/completed/rejected`（`round.*`）；`additionalProperties: true`（消费者前向兼容），`required` 只钉上列键。
  `scripts/check-event-schemas.sh`：① catalog（`events.rs`）中每个类型有对应文件；② 每个 schema `$id` == `dataschema_for(type)`；③ `python3 -m json.tool` 解析通过；类型清单用 `grep` 从 `events.rs` 提取（与文件比对，缺一即 exit 1）。CI 步骤：`bash scripts/check-event-schemas.sh`。
  Run: `bash scripts/check-event-schemas.sh`；`cargo test -p coord-core --lib workflow::approval`。

- [ ] **Step 3: 回归并提交**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core`；`bash scripts/check-doc-refs.sh`。
  Commit: `feat(workflow): finalize approval event types with v1 suffix and schemas`

---

### Task 4: 存储扩展（幂等记录 + 前缀扫描）

**Files:**
- Modify: `coord-core/src/workflow/approval/keys.rs`（幂等键布局）
- Modify: `coord-core/src/workflow/approval/store.rs`（`IdemRecord`、trait 默认方法、Memory 实现、测试）
- Modify: `coord-agent/src/services/approval_store.rs`（KV 实现：同 Txn 幂等写 + `scan`）
- Test: `coord/tests/approval_store_test.rs`（追加）

**Interfaces:**
- Consumes: Task 1 无；M2-B `ApprovalStore`/`ExpectedRev`/`KvApprovalStore`。
- Produces:
  - `keys.rs`：`pub fn idem_key(instance_id: &str, key: &str) -> Vec<u8>`（`/_workflow/v4/idempotency/{instance}/{key}`）；`pub fn idem_prefix(instance_id: &str) -> Vec<u8>`。
  - `store.rs`：
    ```rust
    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
    pub struct IdemRecord { pub key: String, pub response: Value }

    #[derive(Debug, Clone, PartialEq)]
    pub struct ApprovalInstanceEntry { pub id: String, pub engine: Engine, pub mod_rev: i64 }

    #[async_trait]
    pub trait ApprovalStore: Send + Sync {
        async fn load(&self, instance_id: &str) -> Result<Option<(Engine, i64)>, StoreError>;
        async fn load_idem(&self, instance_id: &str, key: &str) -> Result<Option<Value>, StoreError>;
        async fn commit(&self, engine: &Engine, events: &[OutboxRecord], expected: ExpectedRev) -> Result<bool, StoreError> {
            self.commit_with_idem(engine, events, expected, None).await
        }
        async fn commit_with_idem(&self, engine: &Engine, events: &[OutboxRecord], expected: ExpectedRev, idem: Option<&IdemRecord>) -> Result<bool, StoreError>;
        async fn scan(&self, after: Option<&str>, limit: usize) -> Result<Vec<ApprovalInstanceEntry>, StoreError>;
    }
    ```
  - 语义：`commit_with_idem` 成功时状态、事件、幂等记录同一临界区/Txn 生效；`load_idem` 命中即缓存响应；`scan` 按实例 id 字典序、`after` 独占、`limit` 上限（0 ⇒ 默认 50，硬上限 200）。

- [ ] **Step 1: 写失败测试**

  coord-core（Memory）：`idem_record_roundtrips_and_is_isolated_per_instance`；`commit_with_idem_writes_record_on_success`；`commit_with_idem_failure_leaves_no_idem_record`；`scan_paginates_in_key_order`；`scan_limit_bounds`；`empty_event_batch_does_not_advance_seq_counter`（零事件提交后 `read_sequence` 不前进、下一批 seq 从 1 起）。
  coord（KV，真 Raft）：`kv_idem_and_state_are_same_txn`（提交成功后 `load_idem` 命中；CAS 失败后 `load_idem` 为空）；`kv_scan_sees_v4_only_paginates`.
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib workflow::approval` 与 `cargo test -p coord --test approval_store_test -- --test-threads=1`。Expected: FAIL（方法缺失）。

- [ ] **Step 2: 实现（Memory + KV）**

  Memory：`Entry` 增 `idem: BTreeMap<String, Value>`；`scan` 用 `BTreeMap::range`。KV：`commit_with_idem` 在既有 Txn 中追加幂等键 `put`（谓词加 `VERSION==0`）；`load_idem` 用 `range_with_rev` 线性一致读；`scan` 用 v4 前缀 `range_with_rev` + `prefix_range_end`，值反序列化为 `Engine`。
  Run: Step 1 两命令全绿。

- [ ] **Step 3: 回归并提交**

  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core`、`cargo test -p coord-agent`（回归）。
  Commit: `feat(workflow): add approval idempotency records and prefix scan`

---

### Task 5: DSL `approve` 节点（模型 + 解析 + 校验）

**Files:**
- Modify: `coord-core/src/workflow/model.rs`（`Task::Approve(ApproveTask)` + `WorkflowInstance.initiator`）
- Modify: `coord-core/src/workflow/validate.rs`（`resolve_task` 分支 + `parse_approve` + `check_task_refs`/`check_cycles` 覆盖）
- Modify: `coord-core/src/workflow/engine.rs`（v1 执行器：`match` 分支 + `task_type_name` ⇒ `"approve"`；本任务只登记，不实现挂起——执行体在 Task 6）
- Test: `coord-core/src/workflow/validate.rs`（`mod tests`）、`coord-core/src/workflow/model.rs`（serde 往返）

**Interfaces:**
- Consumes: `coord_core::workflow::approval::model::{JoinPolicy, RejectPolicy, SeatSpec}`。
- Produces:
  ```rust
  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
  #[serde(rename_all = "camelCase")]
  pub struct ApproveTask {
      pub approvers: Vec<String>,          // #[serde(default)]
      pub pool: Vec<String>,               // #[serde(default)]
      pub readers: Vec<String>,            // #[serde(default)]
      pub roles: Vec<String>,              // #[serde(default)]
      pub mode: JoinPolicy,                // #[serde(default = "default_join_all")]，复用 approval::model::JoinPolicy
      pub veto: bool,                      // #[serde(default = "default_true")]
      pub deadline: Option<String>,        // 原样保存的 duration 串（ISO8601/官方对象序列化）
      pub on_pass: Option<String>,
      pub on_reject: Option<String>,
      pub sign_admins: Vec<String>,        // #[serde(default)]
      pub allow_self_approve: bool,        // #[serde(default)]
  }
  ```
  `ApproveTask::to_seats(&self) -> Vec<SeatSpec>`（approvers→`SeatSpec::approver`、pool→`SeatSpec::pool`、readers→`SeatSpec::reader`；roles 先经 Task 11 的 resolver，本任务不解析）；`ApproveTask::to_reject(&self) -> RejectPolicy`（`veto ? AnyReject : OnExhausted`）；`ApproveTask::deadline_ms(&self, now_ms: i64) -> Option<i64>`（校验期已保证可解析；运行期解析失败 ⇒ `None` 由调用方 fail-closed）。
  `WorkflowInstance` 增 `#[serde(default, skip_serializing_if = "Option::is_none")] pub initiator: Option<String>`（旧快照兼容；Task 11 的 Start 注入）。
  `parse_approve` 接受键集合恰为上述字段名（camelCase：`allowSelfApprove`/`signAdmins`/`onPass`/`onReject`），未知键 ⇒ `UnsupportedConstruct("approve.<key>")`；`mode` 形状 `all|any|quorum:<n>|ratio:<num>/<den>`（字符串），非法 ⇒ `UnsupportedConstruct("approve.mode")`；`deadline` 复用 `parse_wait` 的 duration 解析（把结果转回规范化字符串存 `deadline: Option<String>`），`until`/`${}`/未知键拒绝；`approvers`/`pool`/`readers`/`roles` 全空 ⇒ `MissingRequiredField("approve seats")`；`onPass`/`onReject` 目标纳入 `check_task_refs`；`approve` 节点进入 `check_cycles` 的边集合。
  `definition_hash` 自动包含新字段（无需改）。

- [ ] **Step 1: 写失败测试**

  `validate.rs`：`parse_approve_minimal`（仅 `approvers` ⇒ mode=All/veto=true/allow_self_approve=false）；`parse_approve_full`（全字段逐字段断言）；`parse_approve_rejects_unknown_key`；`parse_approve_rejects_unknown_mode`；`parse_approve_rejects_absolute_deadline`；`parse_approve_requires_a_seat_source`；`parse_approve_on_pass_target_must_exist`（`UnknownTaskReference`）；`parse_approve_on_reject_target_must_exist`。`model.rs`：`approve_task_serde_roundtrip`；`instance_initiator_serde_default_compat`（旧 JSON 反序列化 `initiator==None`）。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib workflow::validate`。Expected: FAIL。

- [ ] **Step 2: 实现模型/解析/校验 + 执行器桩**

  执行器桩：`engine.rs` 的 `execute_named_task` 增 `Task::Approve(_) =>` 返回 `StepResult::Failed` 的显式未接线错误（Task 6 替换为 `Suspend`），保证本任务可编译且不静默执行。
  Run: Step 1 全绿 + `cargo test -p coord-core --lib workflow` 回归。

- [ ] **Step 3: 提交**

  Run: `cargo fmt --all -- --check`。
  Commit: `feat(workflow): parse and validate native approve nodes`

---

### Task 6: `approve` 执行器与挂起元信息

**Files:**
- Create: `coord-core/src/workflow/tasks/approve.rs`
- Modify: `coord-core/src/workflow/tasks/mod.rs`（登记模块）
- Modify: `coord-core/src/workflow/model.rs`（`SuspendReason::WaitingForApproval`、`SuspensionMeta.approval_instance_id`、`to_meta` 映射）
- Modify: `coord-core/src/workflow/engine.rs`（`execute_named_task` 换成 `tasks::approve::execute`）
- Test: `coord-core/src/workflow/tasks/approve.rs`（`mod tests`）、`coord-core/src/workflow/model.rs`（serde 兼容测试）

**Interfaces:**
- Consumes: Task 5 `ApproveTask`。
- Produces:
  - `pub fn execute(named: &NamedTask, task: &ApproveTask, instance: &WorkflowInstance, clock: &dyn Clock) -> StepResult`：返回 `StepResult::Suspend { reason: SuspendReason::WaitingForApproval { approval_instance_id: instance.id.clone(), node: named.name.clone() }, frame }`；frame `task_type = "approve"`、`status = Running`；若 `instance.definition_hash.is_none()` ⇒ 返回 `StepResult::Failed`（fail-closed，pin 必须在 drive 内已回填）。
  - `SuspendReason::WaitingForApproval { approval_instance_id: String, node: String }`；`to_meta()` ⇒ `SuspensionMeta { reason: "approval", expected_signal: Some("approval.completed"), approval_instance_id: Some(..), ..Default }`。
  - `SuspensionMeta` 新字段 `pub approval_instance_id: Option<String>`（`#[serde(default, skip_serializing_if = "Option::is_none")]`）。
- Consumes 约定：`ApproveTask` 的 deadline 解析结果（毫秒偏移）由 Task 8 的 runtime 桥计算，本任务不做 I/O。

- [ ] **Step 1: 写失败测试**

  `approve.rs`：`execute_returns_waiting_for_approval_suspend`（断言 reason 字段、frame.task_type=="approve"、expected_signal=="approval.completed"）；`execute_fails_without_definition_pin`。
  `model.rs`：`suspension_meta_legacy_json_deserializes_without_approval_field`（旧 JSON 反序列化 `approval_instance_id==None`）；`waiting_for_approval_to_meta_maps_reason_and_signal`。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib workflow`。Expected: FAIL。

- [ ] **Step 2: 实现**

  按 `tasks/listen.rs` 的纯函数模板实现；`SuspendReason`/`to_meta`/serde 按上方接口。
  Run: Step 1 全绿 + `cargo test -p coord-core --lib workflow`。

- [ ] **Step 3: 提交**

  Commit: `feat(workflow): add approve task executor and approval suspension meta`

---

### Task 7: 审批桥（授权 / 命令 / 效果→事件）

**Files:**
- Create: `coord-core/src/workflow/approval/bridge.rs`
- Modify: `coord-core/src/workflow/approval/mod.rs`（`pub mod bridge;`）
- Modify: `coord-core/src/workflow/approval/model.rs`（`ApprovalPolicy` + `Branch.policy`）
- Modify: `coord-core/src/workflow/approval/engine.rs`（`enter_branch` 增 `policy: ApprovalPolicy` 参数；M2-B 测试调用点补 `ApprovalPolicy::default()`）
- Modify: `coord-core/src/workflow/approval/store.rs`（如 `ApprovalInstanceEntry` 需要 `ApprovalPolicy` 派生）
- Test: `coord-core/src/workflow/approval/bridge.rs`（`mod tests`，用 `MemoryApprovalStore` + 固定 `Clock`）

**Interfaces:**
- Consumes: Task 2/3/4 的 engine/events/store；Task 5 `ApproveTask`。
- Produces:
  ```rust
  #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, Default)]
  pub struct ApprovalPolicy {
      pub allow_self_approve: bool,
      pub sign_admins: Vec<Principal>,
  }
  // Branch 增字段：#[serde(default)] pub policy: ApprovalPolicy,

  pub struct ApprovalBridge { /* store + clock + resolver */ }
  pub trait ApproverResolver: Send + Sync {
      fn resolve(&self, role: &str) -> Option<Vec<Principal>>;
  }
  pub struct MapApproverResolver { /* HashMap<String, Vec<Principal>> */ }
  impl MapApproverResolver { pub fn new(map: std::collections::HashMap<String, Vec<Principal>>) -> Self; }
  impl ApprovalBridge {
      pub fn new(store: Arc<dyn ApprovalStore>, clock: Arc<dyn Clock>, resolver: Option<Arc<dyn ApproverResolver>>) -> Self;
      pub async fn ensure_node_open(&self, wf: &WorkflowInstance, node: &str, task: &ApproveTask)
          -> Result<(), BridgeError>;              // 幂等：已存在同 node 的 open 分支则复用不重开；task.roles 逐角色经 resolver 展开，任一无映射 ⇒ InvalidArgument("approve.role.<name>")
      pub async fn command(&self, approval_id: &str, actor: &str, cmd: ApprovalCommand<'_>, idem_key: Option<&str>)
          -> Result<CommandOutcome, BridgeError>;
      pub async fn expire_due(&self, approval_id: &str, now_ms: i64) -> Result<ExpireOutcome, BridgeError>;
      pub async fn finish_if_complete(&self, approval_id: &str) -> Result<Option<ApprovalOutcome>, BridgeError>;
      pub async fn terminate(&self, approval_id: &str, actor: &str, reason: &str) -> Result<(), BridgeError>;
  }
  pub enum ApprovalCommand<'a> { Approve{task_id:&'a str, comment:Option<&'a str>}, Reject{..}, Acknowledge{task_id:&'a str},
      AddSign{branch_id:&'a str, seats:Vec<SeatSpec>, comment:Option<&'a str>}, RemoveSign{task_ids:Vec<String>},
      ReturnToNode{branch_id:&'a str, target_node:&'a str, comment:Option<&'a str>}, Transfer{task_id:&'a str, to:&'a str},
      Delegate{task_id:&'a str, to:&'a str}, Withdraw, Terminate{reason:&'a str} }
  pub enum NodeOutcome { Passed, Rejected }   // 节点/分支级结果（Task 11A）
  pub enum CommandOutcome { Applied{response:Value, node_outcome:Option<NodeOutcome>, terminal:Option<ApprovalOutcome>}, Duplicate(Value) }
  pub struct ExpireOutcome { pub expired: u32, pub node_outcome: Option<NodeOutcome> }
  pub enum ApprovalOutcome { Completed, Rejected, Cancelled, Terminated }
  pub enum BridgeError { NotFound(String), NotAuthorized(String), InvalidTransition(String), InvalidArgument(String), Stale(String), Store(String), Serialization(String) }
  ```
  行为：
  - `ensure_node_open`：`load(id)` → 无 ⇒ `Engine::start(StartSpec{ id=wf.id, definition_*=wf.*, definition_hash=wf.definition_hash.clone().unwrap_or_default(), initiator=wf.initiator.clone().unwrap_or("anonymous"), context=Json::Null→Scope::new() }, now)`；已存在 ⇒ 复用当前 open 分支（`node` 相同）不重开；随后 `enter_branch(.., policy=ApprovalPolicy{..})`（复用时不调）、`open_approval(seats, deadline_ms)`；effects→`event_record` + `commit_with_idem(CreateOnly 或 Exact(rev), None)`。`seats` = `task.to_seats()` + `task.roles` 逐个经 `ApproverResolver` 展开的 `SeatSpec::approver`；resolver 为 `None` 或角色无映射 ⇒ `InvalidArgument("approve.role.<name>")`（fail-closed，不许静默少派席位）。
  - `command`：`load_idem` 命中 ⇒ `Duplicate`；否则 `load` + 授权矩阵（决定 5）→ 引擎命令 → 事件 id 确定性生成 `format!("{approval_id}:{rev_after}:{index}")` → `commit_with_idem(Exact(rev), Some(IdemRecord{key, response}))`；失败重载重试 ≤3；终态检测：`engine.instance.status` ∈ {Completed, Rejected, Cancelled, Terminated} ⇒ `Applied.terminal=Some(..)`。
  - `expire_due`：对全部 open 且 `deadline <= now` 的席位逐个 `expire`（同一 engine 命令序列），提交；不产生终态（除非外部策略），返回计数。
  - `finish_if_complete`：无 open 分支且根分支 Completed ⇒ `finish` ⇒ `Completed`。
  - `terminate`：未终态时 `terminate(actor=..)` + 提交。
- `Branch`/`enter_branch` 是有意的加性偏离（Q6 授权落地），在 ADR 语义页记明（Task 13）。

- [ ] **Step 1: 写失败测试**

  `bridge.rs`：`ensure_node_open_starts_engine_and_persists`；`ensure_node_open_reuses_existing_open_branch`（同 id 调两次，分支数不增、事件不重复）；`ensure_node_open_expands_roles_via_resolver`；`ensure_node_open_unresolved_role_fails_closed`；`self_approve_blocked_by_default`；`self_approve_allowed_when_policy_permits`；`non_assignee_vote_rejected`（桥不放松引擎判定）；`add_sign_requires_sign_admin_or_initiator`；`terminate_requires_sign_admin`；`command_idem_replay_returns_cached_response_and_writes_once`；`expire_due_marks_expired_without_pass`；`finish_if_complete_emits_completed_once`；`policy_serde_default_compat`（旧 JSON 无 `policy` 可反序列化）。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --lib workflow::approval::bridge`。Expected: FAIL。

- [ ] **Step 2: 实现**

  严格按接口块；所有事件经 `events::event_record`（fail-closed）；错误不 panic（AGENTS panic gate）。
  Run: Step 1 + `cargo test -p coord-core --lib workflow::approval`（M2-B 场景 + 新断言全绿）。

- [ ] **Step 3: 提交**

  Commit: `feat(workflow): add approval bridge with authz, idempotency and event mapping`

---

### Task 8: Runtime 挂起接线（approve 节点 → Suspended）

**Files:**
- Modify: `coord-core/src/workflow/runtime.rs`（`WorkflowRuntime` 增 `approval: Option<Arc<ApprovalBridge>>` + `with_approval_bridge`；drive 的 `Suspend` 分支增 `WaitingForApproval` arm；`reconcile_resumable_instances` 保持跳过 `Suspended`）
- Test: `coord/tests/workflow_foundation.rs` 或新 `coord-core/tests/approval_bridge_runtime.rs`（`MemoryApprovalStore` + 内存 runtime）

**Interfaces:**
- Consumes: Task 7 `ApprovalBridge`；Task 6 suspend reason；Task 5 `WorkflowInstance.initiator`。
- Produces:
  - `WorkflowRuntime::with_approval_bridge(self, bridge: Arc<ApprovalBridge>) -> Self`。
  - drive `WaitingForApproval` 语义：调 `bridge.ensure_node_open(inst, node, task)`（`task` 由 `definition` 按 `inst.current_task_index` 定位）；成功 ⇒ frame 入栈、`status=Suspended`、`suspension_meta=to_meta()`、`commit_instance`（事件 `WORKFLOW_SUSPENDED`）；失败（无 bridge/存储错/角色/seats 空）⇒ `fail_closed` 把实例置 `Failed`（事件 `WORKFLOW_FAULTED`），**不得**继续执行后续节点。

- [ ] **Step 1: 写失败测试**

  `approval_bridge_runtime.rs`：`approve_node_suspends_with_approval_meta_and_engine_persisted`；`re_drive_after_suspend_does_not_double_open`（挂起后强制再 drive 一次，分支/轮次计数不变）；`approve_node_without_bridge_faults_closed`。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --test approval_bridge_runtime`。Expected: FAIL。

- [ ] **Step 2: 实现**

  drive 分支放在 `WaitingForSignal` 附近；所有写经 `commit_instance`；`initiator` 不影响既有序列化（`skip_serializing_if`）。
  Run: Step 1 + `cargo test -p coord-core --lib workflow` + `cargo test -p coord --test workflow_foundation` 回归。

- [ ] **Step 3: 提交**

  Commit: `feat(workflow): suspend workflow instances at approve nodes`

---

### Task 9: Runtime 恢复接线（outcome → resume/fault/finish/cancel）

**Files:**
- Modify: `coord-core/src/workflow/runtime.rs`（`resume_approval`；`cancel_instance`/fault 路径收口审批实例；`Completed` 分支调 `finish_if_complete`）
- Test: `coord-core/tests/approval_bridge_runtime.rs`（追加）

**Interfaces:**
- Consumes: Task 7 `ApprovalBridge::{command,finish_if_complete,terminate}`；既有 `resume` 的 CAS 机制。
- Produces:
  ```rust
  pub async fn resume_approval(&self, instance_id: &str, outcome: Value, next_target: Option<&str>)
      -> Result<WorkflowInstance, RuntimeError>
  ```
  语义：CAS 重放（≤3）；前置 `suspension_meta.reason == "approval"`；完成最后一个 `Running` frame（`output = outcome`）；`context["_approval"] = outcome`；`next_target` 有值 ⇒ `current_task_index = find_task_index(target)`（缺失 ⇒ `RuntimeError`，fail-closed），否则 `+1`；`status=Running`、`suspension_meta=None`；提交 `TASK_COMPLETED, WORKFLOW_RESUMED`；`spawn_drive`。
  终态分工：`Completed` ⇒ `next_target=None`；`Rejected/Cancelled/Terminated` ⇒ 由调用方（service）按 `on_reject` 传 target；无 target 时 service 调既有 `fail_instance`/等价路径置 fault（错误码 `approval_rejected`）。
  `cancel_instance`：若 `suspension_meta.reason=="approval"` ⇒ 先 `bridge.terminate(id,"system","workflow_cancelled")`（未终态时才调），再按既有取消路径提交。
  workflow `Completed` 分支：提交前调 `bridge.finish_if_complete(id)`（幂等；不存在审批实例时为 no-op）。

- [ ] **Step 1: 写失败测试**

  `approve_completed_resumes_to_next_task`；`approve_rejected_resumes_to_on_reject_target`；`approve_cancelled_resumes_to_on_reject_target`；`resume_approval_without_approval_suspension_rejected`；`resume_approval_unknown_target_rejected_and_state_unchanged`；`workflow_completion_finishes_approval_instance`；`workflow_cancel_terminates_open_approval`；`resume_approval_is_cas_safe_under_replay`.
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --test approval_bridge_runtime`。Expected: FAIL。

- [ ] **Step 2: 实现**

  复用 `commit_control_plane`；不得新开无 CAS 写路径；`find_task_index` 目标必须与 Task 5 的 `check_task_refs` 校验一致（顶部任务名）。
  Run: Step 1 + `cargo test -p coord-core --lib workflow` + `cargo test -p coord --test workflow_foundation`。

- [ ] **Step 3: 提交**

  Commit: `feat(workflow): resume workflows from approval outcomes`

---

### Task 10: 审批 deadline 持久化定时器

**Files:**
- Modify: `coord-core/src/workflow/durable.rs`（`TimerKind::ApprovalDeadline { approval_instance_id: String }`）
- Modify: `coord-core/src/workflow/runtime.rs`（`schedule_approval_deadline`；`fire_timer` 新 arm；stale 守卫）
- Modify: `coord-agent/src/services/workflow.rs`（phase4 `BaseService::start` 的 durable workers 无需改；若 `fire_timer` 在 agent 侧有分派则同步）
- Test: `coord/tests/approval_store_test.rs` 或新 `coord-core/tests/approval_bridge_runtime.rs`（追加）；`coord/tests/spike3_durable_kv.rs` 回归

**Interfaces:**
- Consumes: Task 7 `expire_due`；M1 `DurableTimerStore`/`TimerRecord`。
- Produces: `TimerKind::ApprovalDeadline { approval_instance_id: String }`（`#[serde(tag="kind", rename_all="snake_case")]` 加性变体）；drive 挂起 arm 在 `deadline` 存在时 `schedule_timer(fire_at=deadline_ms, kind=..)`。
  fire 语义：读 workflow 实例；`suspension_meta.reason != "approval"` 或 `approval_instance_id` 不匹配 ⇒ 丢弃（stale）；否则 `bridge.expire_due(id, now)`；返回有终态 ⇒ 按 Task 9 恢复；仍有 deadline 更晚的 open 席位 ⇒ 重新调度；全部过期且轮次 Open ⇒ 不恢复、不重调度。

- [ ] **Step 1: 写失败测试**

  `deadline_timer_expires_seat_and_keeps_workflow_suspended`；`stale_deadline_timer_after_resume_is_dropped`；`later_deadline_reschedules_timer`；`all_expired_never_auto_passes_and_never_resumes`；`legacy_timer_json_with_known_kinds_still_deserializes`.
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-core --test approval_bridge_runtime`。Expected: FAIL。

- [ ] **Step 2: 实现**

  照既有 `schedule_timeout`/`fire_timer` 模式；不引入进程内 sleep 作为正确性基础。
  Run: Step 1 + `cargo test -p coord --test spike3_durable_kv -- --test-threads=1` 回归。

- [ ] **Step 3: 提交**

  Commit: `feat(workflow): drive approval deadlines from durable timers`

---

### Task 11: 服务层与 gRPC 面（命令 + 查询 + 身份 + 幂等）

**Files:**
- Modify: `coord-agent/src/services/workflow.rs`（phase4 `WorkflowEngineService`：`approval: Option<Arc<ApprovalBridge>>` + constructors/assemble/Clone；命令/查询 inherent 方法）
- Modify: `coord-agent/src/services/grpc_handlers.rs`（`map_approval_error` + 12 个 trait 方法）
- Modify: `coord-agent/src/lib.rs`（durable 装配点把 `KvApprovalStore`+`ApprovalBridge` 注入 service/runtime；`workflow_durable` 关闭时审批命令 fail-closed）
- Modify: `coord-agent/src/services/mod.rs`（如需要）
- Test: `coord-agent/src/services/workflow.rs`（`phase4_tests`，内存路径）+ `coord-agent/src/services/grpc_handlers.rs` 单元测试

**Interfaces:**
- Consumes: Task 1 RPC 类型；Task 4 store；Task 7 bridge；Task 8/9 runtime 方法；`GatewayIdentity`（`coord-agent/src/plugin/gateway.rs`）。
- Produces:
  - Service 方法（供 handler 调用，全部返回 `(instance_id, status)` 语义或既有错误族）：
    `start_instance_as(def_id, input, initiator: Option<&str>)`（原 `start_instance` 保留并转调 `None`）；
    `approval_command(workflow_id, actor, cmd, idem) -> (instance_id, status)`：无 `approval` ⇒ `FailedPrecondition`；命令 `Applied{node_outcome,terminal}` ⇒：`node_outcome==Some(Passed)` 调 `runtime.resume_approval(id, json!({"status":"passed"}), node.on_pass)`（无 `on_pass` 则 `None`）；`terminal ∈ {Rejected,Cancelled,Terminated}` 调 `runtime.apply_approval_node_outcome`（有 `on_reject` 走目标，无则 fault `approval_rejected`）；`Duplicate(resp)` 直接返回；
    `list_pending_tasks(actor, page_size, page_token)`；`get_instance_detail(workflow_id)`。
  - **Task 11A 接口消费**：`bridge::NodeOutcome`/`apply_approval_node_outcome`（公开 helper）；审批 `command` 的 CAS 重试环内**每轮先重查 `load_idem`**（Task 7 review P2：并发同 key 防双重 Apply）；`RemoveSign` 空 `task_ids` 与 `AddSign` 无席位在 RPC 边界 ⇒ `InvalidArgument`；deadline 路径（Task 10/11A）已用 helper，Task 11 须把命令路径的 `on_pass` 与之一致（若 11A 未传 `on_pass`，在 helper 中补上）。
  - 角色解析：`ApprovalBridge` 构造时注入 `MapApproverResolver`（service 配置 `workflow_approval_roles: HashMap<String, Vec<String>>`，`ServicesConfig` 新增字段，`#[serde(default)]`）；无映射角色在节点开启时 fail-closed（Task 7 负责）。
  - `map_approval_error`：决定 11 的映射。
  - handler：先 `let identity = request.extensions().get::<GatewayIdentity>().cloned().unwrap_or_default();` 再 `into_inner()`；actor = `identity.subject`（空 ⇒ `"anonymous"`）；`start` 用 `identity.subject` 注入 initiator。
  - `ListPendingTasks`：`page_size<=0 ⇒ DEFAULT_LIST_PAGE_SIZE`；`page_token` = 上一页最后 instance_id；只返回 `open_tasks_for(actor)` 非空中的该 actor 任务。

- [ ] **Step 1: 写失败测试**

  phase4 内存测试：`start_instance_as_records_initiator`；`approval_command_without_bridge_fails_closed`；`approve_with_unmapped_role_faults_closed`（经 runtime drive）；`list_pending_tasks_paginates_and_filters_actor`；`get_instance_detail_returns_snapshot_fields`；`idem_key_replay_returns_same_status`。
  grpc_handlers：`map_approval_error_matrix`（5 变体逐项断言 gRPC code）。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-agent --lib services`。Expected: FAIL。

- [ ] **Step 2: 实现（含 lib.rs 装配）**

  durable 装配：`new_with_kv_store_durable` 构造 `KvApprovalStore(inner, outbox)` → `ApprovalBridge::new(store, clock, Some(MapApproverResolver::new(config.workflow_approval_roles)))` → 注入 service 与 runtime（`with_approval_bridge`）；非 durable 构造 `approval=None`。`Clone`/`Debug` 同步。
  Run: Step 1 + `cargo test -p coord-agent`（回归）。

- [ ] **Step 3: 自检装配点**

  `git grep -n "KvApprovalStore::new"` 只在 `approval_store.rs`（定义/测试）与 `workflow.rs`（durable 装配）命中；`git grep -n "with_approval_bridge"` 只在装配与测试命中。
  Commit: `feat(workflow): expose approval v2 commands and queries over gRPC`

---

### Task 12: 端到端验收（真 server + agent）

**Files:**
- Create: `coord/tests/approval_rpc_test.rs`
- Test: 同文件

**Interfaces:**
- Consumes: `coord/tests/common` 的 `start_test_server`/`wait_tcp_ready`；Task 11 的 RPC 面（经 tonic client）；Task 3 schema。
- 环境：单节点 Raft + durable（照 `coord/tests/approval_store_test.rs` setup 的 durable 开启方式；若该 setup 未起 gRPC service，则用 `common::start_test_server` + 配置 durable）。

- [ ] **Step 1: 写失败测试（先红）**

  `approve_rpc_flow_completes_workflow`：deploy 含 `approve` 节点定义（approvers=[u1,u2]，mode=all）→ start → 实例 `SUSPENDED`；`GetInstanceDetail` 含 open 任务；`ListPendingTasks(actor=u1)` 见 1 条；u1、u2 各 `Approve` → 工作流 `COMPLETED`，审批实例 `COMPLETED`（finish 已被调），outbox 见 `round.passed.v1`+`instance.completed.v1`。
  `reject_routes_to_on_reject_branch`：`veto=true`，u1 `Reject` → 工作流走 `on_reject` 目标并最终 `COMPLETED`（或按定义 fail）；审批实例 `REJECTED`。
  `add_sign_and_return_to_node_paths`：`AddSign` 后新席位可批；`ReturnToNode` 重开前节点（attempt 递增）。
  `withdraw_and_terminate_authorization`：非发起人 `Withdraw` ⇒ `PERMISSION_DENIED`；发起人成功且工作流恢复到 `on_reject`/fault；`sign_admins` 之外 `Terminate` ⇒ `PERMISSION_DENIED`。
  `idempotency_key_replay_single_write`：同 key 连发两次 `Approve` ⇒ 两次响应一致，outbox 中对应事件只一份，轮次计数只加一。
  `stale_revision_maps_aborted`：并发两票（同 task、不同 actor 不适用；改用同 actor 与预期 rev 并发）⇒ 恰一成功、另一 `ABORTED` 或按重试语义成功且计数正确。
  `expired_seat_never_auto_passes`：deadline 秒级 + 测试时钟不可控时改为短 deadline + `sleep` 到定时器触发；断言 round 仍 open、工作流仍 `SUSPENDED`、无人为动作不完成。
  Run: `CARGO_BUILD_JOBS=3 cargo test -p coord --test approval_rpc_test -- --test-threads=1`。Expected: FAIL（编译/接口缺失）。

- [ ] **Step 2: 实现测试（不改生产代码；缺陷回对应任务修）**

  沿用 `approval_store_test.rs` 的 server/agent 启动与 `KvApprovalStore` 直连做证据核对；RPC 用 `coord_proto` tonic client。
  Run: Step 1 + `cargo test -p coord --test approval_store_test --test spike2_concurrency --test spike3_durable_kv -- --test-threads=1`（v1/v2 不回退）。

- [ ] **Step 3: 提交**

  Commit: `test(workflow): end-to-end approval RPC acceptance on real server`

---

### Task 13: 治理文档与契约记录

**Files:**
- Modify: `docs/adr/0013-workflow-approval-v2.md`（状态 `proposed → accepted`（2026-10-15，用户方裁定：EIS/ICPS 无疑问、集成后反馈）；D2 增 DSL 语法小节；D7 增授权矩阵；D8 增幂等键/扫描口径；D9 边界补 deadline 无自动处置、命令路径不产生 `Faulted`、UI/外部角色解析 carry）
- Modify: `docs/production/workflow-semantics.md`（§7 改为「已接线」终态口径：RPC、事件名+`.v1`、schema、授权矩阵、deadline 无自动判定、幂等）
- Modify: `docs/production/ops/boundaries.md`（新增/更新 `B-WF-*`：deadline 无自动处置、ListPendingTasks 全前缀扫描、Memory 审批存储非生产、UI 无审批面）
- Modify: `docs/production/consumer-contract.md`（审批 RPC 能力映射 + 事件类型/去重键）
- Modify: `apis/contracts/CHANGELOG.md`（`## [contracts/v1.6.0] — 2026-10-15（Minor：workflow 审批 v2 RPC，ADR-0013 / G-WF-1）` + 影响范围/回滚/灰度 + 本地校验）
- Modify: `apis/contracts/STATUS.md`（工作流行：目标日期 2027-03-31；实现落点补审批 RPC 与语义页）
- Modify: `apis/contracts/WHITEPAPER.md`（§11.2 已按清单填；§13 镜像如受影响）
- Create: `docs/superpowers/plans/2026-10-15-workflow-v2-approval-rpc.md`（本计划，随首个提交入库）

**Interfaces:**
- Consumes: Task 1–12 的实现事实。
- Produces: 文档事实 = 代码事实；无悬空引用。

- [ ] **Step 1: 逐文档落文**

  按上列位置更新；事件名、schema URL、能力字符串、错误码映射逐字与代码一致（单一口径，不复制 ADR 正文）。
- [ ] **Step 2: 门禁**

  Run: `bash scripts/check-doc-refs.sh`（0 悬空）；`bash scripts/check-event-schemas.sh`；`buf lint`（apis/contracts）。
- [ ] **Step 3: 提交**

  Commit: `docs(workflow): record approval v2 RPC, authz and event contract`

---

### Task 14: coord-java-sdk 审批包装 + 示例

**Files:**
- Modify: `coord-java-sdk/src/main/java/cn/byteforce/coord/sdk/workflow/WorkflowClient.java`（新增方法）
- Modify: `coord-java-sdk/src/main/java/cn/byteforce/coord/sdk/internal/rpc/WorkflowClientImpl.java`
- Create: `coord-java-sdk/src/main/java/cn/byteforce/coord/sdk/workflow/ApprovalDecision.java`、`PendingApprovalTask.java`、`ApprovalInstanceDetail.java`（+ 需要时 `ApprovalBranch/Task/Round.java`）
- Modify: `coord-java-sdk/src/test/java/cn/byteforce/coord/sdk/internal/rpc/WorkflowClientContractTest.java`（`FakeWorkflowService` 覆写 + 请求断言 + proto→DTO 映射）
- Modify: `coord-java-sdk/src/test/java/cn/byteforce/coord/sdk/WorkflowClientVersioningTest.java`（新 DTO）
- Create: `java-example/src/main/java/cn/byteforce/coord/example/WorkflowApprovals.java`
- Test: `cd coord-java-sdk && mvn -q verify`（离线依赖就绪时）

**Interfaces:**
- Consumes: Task 1 生成的 `cn.byteforce.coord.contracts.workflow.v1.*` stub（`protobuf-maven-plugin` 从 `coord-proto/src/proto` 生成）。
- Produces（接口方法，`WorkflowClient`）：
  `String approve(String workflowId, String taskId, String comment, String idempotencyKey)` 返回命令后 `status`；
  `reject(...)` 同形；`acknowledge(workflowId, taskId, idempotencyKey)`；
  `addSign(workflowId, branchId, List<String> approvers, List<String> pool, List<String> readers, String comment, String idempotencyKey)`；
  `removeSign(workflowId, branchId, List<String> taskIds, String comment, String idempotencyKey)`；
  `returnToNode(workflowId, branchId, String targetNode, String comment, String idempotencyKey)`；
  `transfer(workflowId, taskId, String to, String comment, String idempotencyKey)`；`delegate(...)` 同形；
  `withdraw(workflowId, String comment, String idempotencyKey)`；`terminate(workflowId, String reason, String idempotencyKey)`；
  `List<PendingApprovalTask> listPendingTasks(int pageSize, String pageToken)`；`ApprovalInstanceDetail getInstanceDetail(String workflowId)`。
  所有方法经 `callWithRetry` + `config.getRequestTimeout()`（照 `rollbackDefinition` 模板）。

- [ ] **Step 1: 写失败测试**

  契约测试新增：`approveSendsIdempotencyKeyAndTaskId`；`commandStatusMapped`；`listPendingTasksMapsPageToken`；`getInstanceDetailMapsBranchesTasksRounds`；`withdrawCarriesCommentAndKey`。版本测试：新 DTO 构造/getter/equals。
  Run: `cd coord-java-sdk && mvn -q -Dtest=WorkflowClientContractTest,WorkflowClientVersioningTest test`。Expected: FAIL（方法缺失）。

- [ ] **Step 2: 实现 DTO + client + 示例**

  示例类照 `ConfigClient.java` 的 connect/blocking stub/close 模板；只演示 `listPendingTasks` + `approve` 两条路径。
  Run: `cd coord-java-sdk && mvn -q verify`（若离线缺依赖，记录并转 `-o`）；`cd java-example && mvn -q -o compile`（如独立可编译）。

- [ ] **Step 3: 提交**

  Commit: `feat(java-sdk): wrap approval v2 RPCs and add example`

---

### Task 15: coord-client 包装与 UI 影响评估

**Files:**
- Modify: `coord-client/src/client.rs`（`WorkflowClient`/`ApprovalClient` + `Client::workflow()`）
- Create: `docs/production/approval-ui-impact.md`（UI 影响评估：dangling `/workflow` 路由、REST facade 缺失、所需 `api.workflow` 面与后续里程碑口径；含 Owner/Last verified）
- Test: `coord-client` 编译 + 既有测试；`coord-ui` 不改代码

**Interfaces:**
- Consumes: Task 1 生成 stub（`coord_proto::workflow::v1`）。
- Produces: `Client::approval()` 返回 `ApprovalClient`，方法 `approve/reject/...` 与 Task 14 同名，异步 `Result<_, ClientError>`；`list_pending_tasks(page_size, page_token)`、`get_instance_detail(id)`。

- [ ] **Step 1: 写失败测试/编译证据**

  `coord-client` 增最小单测（请求构造/错误映射），Run: `CARGO_BUILD_JOBS=3 cargo test -p coord-client`。Expected: FAIL（方法缺失）。
- [ ] **Step 2: 实现 + UI 评估文档**

  Run: Step 1 + `bash scripts/check-doc-refs.sh`。
- [ ] **Step 3: 提交**

  Commit: `feat(client): add approval v2 wrappers and UI impact note`

---

## 里程碑判据（M2-C）

| 判据 | 测试/证据 |
|:--|:--|
| 12 个 RPC 契约与实现两侧逐字一致、buf/wire 门禁全绿 | Task 1 脚本输出；Task 12 e2e |
| approve 节点可挂起、可恢复（pass/reject/return/add_sign/withdraw/terminate） | Task 8/9/12 |
| 超时绝不自动放行；全部过期保持 Open | Task 2 单测；Task 10/12 |
| 幂等键单写、重放返回同响应 | Task 4 单测；Task 12 |
| 授权：非责任人/自批/非 sign_admin 全部拒绝；身份只来自 CCT | Task 7/11/12 |
| 事件契约固化（`.v1` + schema 门禁） | Task 3；Task 13 门禁 |
| durable 关闭时审批面 fail-closed | Task 8/11 测试 |
| 命令路径不产生 `Faulted`（驱动层终态；记录在语义页/边界） | Task 13 文档口径 |
| v1 行为零回退 | 各任务回归；终审 |
| 文档单一归属、无悬空引用 | Task 13 `check-doc-refs.sh` |
| SDK 消费面（Java/Rust）与 UI 影响记录 | Task 14/15 |

## 合并后的发布路径（不在本计划任务内，由 control 在终审后执行）

1. 逐级合并栈：#70 → #71 → #72 → #73 → M2-C PR（每个 PR 先同步最新 main 并重跑门禁）。
2. 版本号与发布：由用户裁定 `0.3.1`（补丁位）或 `0.4.0`（pre-1.0 加法 minor，按 v0.3.0 先例）；执行 `Cargo.toml`/`Cargo.lock`/`pom.xml`/README/SECURITY/ops 文档同步 + `tag v*`（`.github/workflows/release.yml`）。
3. Java SDK Maven Central 发布（`-Prelease`）与 README 版本行更新。
