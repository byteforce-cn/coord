# ADR-0011: 消费方审计第二批（#44–#53）：配置位点、缓存键空间、调度回读、开关管理面与 SDK 依赖治理

- 状态：accepted
- 日期：2026-10-11
- 决策者：维护团队

## 背景

seedj 接入层（coord 消费方）对 v0.2.2 完成第二轮审计，提出 10 项问题
（仓库 issue #44–#53）：4 项文档口径缺陷（#44/#45/#47/#48）、6 项能力/契约
缺口或治理项（#46、#49、#50、#51、#52、#53）。维护团队逐条复核后**全量承接**，
本 ADR 固定各项的落地口径（含两处原本「需产品决策」的定谳）。

单一归属：问题陈述与消费方实证归 seedj 侧；本仓只维护「coord 承诺什么」
（本 ADR、`apis/contracts/STATUS.md`、proto 与 `docs/production/` 的相应页面）。

## 决定

### 1. 配置中心：快照 + 位点续订（#46/#47）

- `ConfigListResponse` 新增 `revision`（快照位点 W）；`ConfigWatchEvent.revision`
  与 `ConfigPutResponse.revision` **统一为全局位点**（Server KV 全局 Revision
  口径，三者同一坐标轴）。⚠️ `Put` 的 revision 坐标轴变更已在
  `contracts/v1.4.0` CHANGELOG 显式告知（此前无文档承诺）。
- Agent 的 `Config/Watch` 改为**逐订阅者直连 Server KV Watch**：honor
  `start_revision`（0 = 实时；>0 = 回放后转实时）、`new_value` presence 缺省 =
  **删除**、事件位点 = KV 全局 revision。修掉旧实现两处语义缺口：只广播本进程
  `set()`（他 agent/直连 KV 的变更与删除不下发）、忽略 `start_revision`（续订
  静默丢窗口）。
- 快照位点安全方向：取数**先于**数据读取采集 C0，位点推进（缓存换入后 /
  批次应用后）恒满足「缓存包含全部 ≤ W 的变更」，故「W 起订」的回放必然覆盖
  快照外变更（可重复、不可丢）。对账后订阅从新 C0 续订（不复位为 0，避免
  对账→订阅缝隙静默丢事件）。
- 位点不可达（历史压缩）⇒ 流以 `FAILED_PRECONDITION` 终止（消费者重读 List
  重订）；订阅缓冲溢出 ⇒ `UNAVAILABLE` 终止（按位点重连重放即可）。
- Java SDK：`listWithRevision` / `watchFrom`（首连 = `startRevision + 1`，与
  断线续订同一语义）/ `putReturningRevision`。
- 判据：`coord/tests/config_resync_test.rs`（快照后直删存储行 → 位点事件收敛
  删除 → 本地缓存收敛 → 实时值事件）。

### 2. 调度：结果回读（#49）

- **定谳：补读取路径**（而非改文档收回「可回读」声明）。新增 `Scheduler/GetJob`
  （能力 `coord:scheduler:read`）：只读幂等；未注册 ⇒ `found=false`；返回
  `state` / `result`（首个完成为准）/ `completed_at_ms`。复用既有
  `get_task_detail`。
- 判据：`agent_scheduler_test.rs`（回读首个结果、重复完成不覆盖、共享 store
  的「重启等价物」读取）。

### 3. 缓存：原子条件写与键空间（#50/#51）

- **SetIfAbsent（SETNX）**：单 agent 下判定与写入同一写事务（并发恰一成功，
  判据 `test_string_put_if_absent_concurrent_single_winner`）；ISR 下判定在
  Leader 本地事务完成、成功经复制通道下发；TTL 仅在写入成功时生效；错误语义与
  `Set` 相同（本地可能已生效），重试 = 再次尝试获取（Redis SETNX 同口径）。
- **Keys / Clear**：`Keys` 有界、游标分页、四类逻辑键去重升序（本节点视角
  best-effort）；`Clear` 按逻辑键前缀清理全部数据类型，批式执行（单批 ≤1024
  行/写事务）、记账同步扣减，ISR 下新增单条目复制（`ReplicaCacheClear`，循
  `MqDeleteTopic` 先例）经复制通道全域下发。边界登记为 `boundaries.md` B-PL-5。
- 键/前缀不得含 NUL（存储编码以 NUL 作分隔）；违规返回 `INVALID_ARGUMENT`。
- **顺带修复（审计外，落地时发现）**：缓存 ISR 幂等键 `{op}:{key}:{秒}` 会让
  同一秒内同 key 重写被判「重复应用」静默跳过（Leader 与 Follower 一起停在
  旧值）。幂等键改为含本地序列号 `{op}:{key}:{seq}`；回归判据
  `test_cache_replicated_same_second_rewrite_is_not_deduped`（负控制实跑：
  还原旧键格式 ⇒ 必红）。

### 4. 特性开关：管理面（#52）

- **定谳：补管理 RPC**（而非仅文档化 KV 写入面）。新增 `PutFlag`（全量 /
  `percentage` 灰度，越界 `INVALID_ARGUMENT`）/ `DeleteFlag`（幂等）/
  `ListFlags`，能力 `coord:flags:manage`（读面 `coord:flags:read` 不变）。
- 读路径维持「直读存储、无本地缓存」⇒ 写入**立即生效**、跨 agent 一致；带外
  KV 路径（键布局 + JSON schema）在消费方契约页 §11 保留说明。

### 5. 文档口径（#44/#45/#47/#48）

- `STATUS.md` 为状态标签唯一事实源：消费方契约页与 SDK README 的过期
  `EXPERIMENTAL` 标注全部清除，并有 `scripts/check-consumer-doc-labels.sh`
  机器守卫（两文件不得再出现该标注；CI 接入 + 注入式自检）。
- 消费方契约页 §13 B-WF-1 边界行修正为 G-WF-1 实况（显式保留 API 存在；
  不承诺自动 TTL/归档/删除审计），并向 `boundaries.md` 对齐核对其余条目。
- Watch 删除语义（presence）在 proto 头注 / 契约页 §2 / SDK javadoc 三处声明；
  `ConfigGetResponse` 陈旧注释同步；`SchedulerClient.completeJob` javadoc 修正为
  「随任务持久化（首个为准），经 `getJob` 回读」。

### 6. Java SDK 依赖治理（#53）

- pom 引入 `grpc-bom` / `protobuf-bom` 管理传递版本（消除散钉）；
  README 声明测试区间（当前：gRPC 1.68.0–1.83.1、protobuf-java 4.28.0–4.35.1，
  下界 = 仓库钉住组合，上界 = 消费方平台实测组合）；CI 对两组组合各跑一次
  `mvn verify`。
- shading/relocation **评估后不采纳**：公开 API 不暴露 gRPC/protobuf 类型，
  消费者可经 BOM 自由对齐（README 已写明复评触发条件）。

### 7. 版本与发布

- 本批为**加法特性**：Rust workspace `0.2.2 → 0.3.0`、Java SDK `0.2.1 → 0.3.0`、
  契约 `contracts/v1.4.0`（7 新 RPC + 1 新字段；语义级变更见 CHANGELOG 告知）。
  本批不附 tag/发布动作（发布另行触发）。

## 后果

- 消费方获得：配置的可靠 resync 闭环（含删除）、cache 的 putIfAbsent /
  keyspace 运维面、scheduler 结果回读、flag 全生命周期管理、明确的文档标签与
  依赖区间。
- 新增守卫：消费方文档标签门、SDK 依赖矩阵 CI；缓存幂等键回归用例。
- 已知取舍：`Keys` 为本地视角（不做跨节点一致枚举）；`Clear` 与并发写按到达
  顺序（窗口内暂停写入是最佳实践）；`GetJob` 不暴露 worker 身份。
- 上述「定谳」可经后续评审翻转（例如 #49 若产品改取「收回可回读声明」路径，
  需新 ADR 超越本条目）。

## 参考

- Issues #44–#53（本仓）；`contracts/v1.4.0` CHANGELOG 条目。
- `docs/production/consumer-contract.md` §2/§6/§7/§11/§13；
  `docs/production/ops/boundaries.md` B-PL-5。
- 判据用例：`coord/tests/config_resync_test.rs`、
  `coord-agent/tests/agent_scheduler_test.rs`、`agent_cache_test.rs`（lib 单测）、
  `agent_isr_test.rs`、`agent_feature_flags_test.rs`；
  Java：`ConfigClientResyncTest`、`EventSchedulerContractTest`、
  `CacheClientTest`、`NewClientFacesTest`。
