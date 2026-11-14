# MQ v2 GA Gate（M0–M7 证据与签字）

> Owner: maintainers ｜ Last verified: 2026-11-14（M0–M5 落地，M6-1/M6-2/M6-3/M6-4 落地）
>
> **状态：M0–M5 完成（GO）；M6 进行中（M6-1/M6-2/M6-3/M6-4 ✅；M6-5 未开始）**。
> M0-1（#178 链接 #332–#339）、M0-2（`coord.mq.v2`
> 冻结为 PROPOSED）、M0-3（`[mq_v2]` 配置/启动校验）、M0-4（服务壳/能力门禁）
> 均已合入；M1-1–M1-6（分片区间、段日志、组状态机、topic 元数据、保留/配额、
> 耐久性）的存储层证据见 §1；M2-1–M2-5（写路径、offset/幂等、上限/原子性、
> Rust 客户端 + Agent 代理、指标/基准）见 §1 与 §2b；M3-1–M3-6（Poll/长轮询、
> Ack/代际 fencing、组协调、Rust consumer helper、best-effort Subscribe、lag/组 TTL）
> 见 §1 与 §2c。保留回收与组回收的周期入口（`mq_v2_reaper_pass`，仅分片 Leader
> 提案）随 M2-1/M3-3 接线。
> M7 GA 不早于 KV-T GA（`docs/adr/0015-kv-t.md` D8），该前置已于
> 2026-11-11 签字。本文是 MQ v2 gate 状态的**单一事实源**；语义以
> `docs/adr/0016-mq-v2.md` 为准，工作项与依赖以
> `docs/superpowers/specs/2026-10-29-mq-v2-design.md` 为准。
>
> 划线规则：**没有可复跑证据的项不写“通过”**；“未跑”必须留在未达标列。

## 1. Gate 总览

| Gate | 出口口径 | 状态 | 证据（可复跑） |
|:--|:--|:--|:--|
| M0 契约冻结 | ADR/契约/配置/服务壳；默认拒绝 | ✅ | 见 §2 |
| M1 日志/元数据/容量 | 分片/段日志/组状态机/topic 元数据/保留 | ✅ | M1-1–M1-6 存储层全部落地：`cargo test -p coord-server --lib storage::mq_log`（14 例：保留截止/配额释放/truncate 重放/重启/副本一致）、`storage::mq_group`（13 例：代际分配/整批 Ack/过期回收/重启/快照）、`storage::mq_topic`（5 例）、`carve_mq`、`cargo test -p coord --bins derive_`、`cargo test -p coord-server --lib test_apply_mq_group_commands`；`check-panics` 0。周期 reaper/指标接线随 M2-1 分片 Leader 循环；整节点备份演练属 M6-5 |
| M2 Publish | 路由/批量/offset/幂等/配额 | ✅ | 见 §2b |
| M3 Poll/Ack/组 | 至少一次/长轮询/代际 fencing/再平衡 | ✅ | 见 §2c |
| M4 DLQ/管理面 | 死信/删除回收/更新/能力 | ✅ | 见 §2d |
| M5 工作流 | v2 outbox/durable/审批/trigger | ✅ | 见 §2e |
| M6 SDK/Ops/证据 | Rust/Java/Ops/性能/混沌/Jepsen | 🚧 M6-1/M6-2/M6-3/M6-4 ✅；M6-5 未开始 | 见 §2f |
| M7 GA | 全绿 + KV-T GA + 签字 | ⛔ 未达标 | 见 §3 |

## 2. M0 分项（启动 gate）

| 项 | 状态 | 证据 / 未达标原因 |
|:--|:--|:--|
| M0-1 ADR/设计/任务树 | ✅ | ADR-0016 accepted；设计文档 + 本文件入库；#178 评论链接 8 个 phase issue（#332–#339） |
| M0-2 `coord.mq.v2` 契约冻结 | ✅ | 两侧 proto 副本 + build 接线；`check-wire-sync.sh` / `check-wire-descriptor.sh`（strict）/ `buf lint` / `check-error-code-contract.sh` / `check-sdk-sync.sh`（PROPOSED 豁免）绿；11 个码 Rust↔Java 逐字一致；19 个 RPC 能力映射进 `rpc_capability`（capability registry 门禁强制） |
| M0-3 `[mq_v2]` 配置/启动校验 | ✅ | `cargo test -p coord config::tests::test_mq_v2`（5 例）；默认关闭；非法值点名 `mq_v2.*`；`enabled=true` 缺 `[kv_t]` 启动拒绝并点名两项 |
| M0-4 服务壳/能力门禁 | ✅ | `cargo test -p coord --test mq_v2_g0_test`（4 例：关闭/未交付/已交付三态 + reflection 可见）；`coord_mq_v2_enabled` gauge；v1 数据面不受影响。Agent `MqV2Proxy` 已随 M2-4 接线（见 §2b） |

### M2 分项（Publish 写路径）

| 项 | 状态 | 证据 / 未达标原因 |
|:--|:--|:--|
| M2-1 Publish/PublishBatch + 路由/批量 | ✅ | `cargo test -p coord-server --lib mq_v2::publish`（8 例：topic/代际/分区校验、多 Raft 装配 fail-closed、FIFO/64 条上限、拒绝码、int64 溢出）；`cargo test -p coord --test mq_v2_m2_publish_test`（11 例含提交后可见、并发 offset=`0..64`、批深度 < 请求数、保留回收）；非 Leader → `NOT_LEADER` + `coord-leader-hint` 由 `mq_v2_m2_cluster_test` 钉住 |
| M2-2 offset/顺序/幂等 | ✅ | 同一集成文件的 `publish_offsets_are_unique_and_monotonic_under_concurrency`、`duplicate_idempotency_key_returns_the_first_offset`、`publish_after_retention_truncation_reuses_the_idempotency_key`；apply 层（M1-2）的恢复/换届去重由 `cargo test -p coord-server --lib storage::mq_log` 覆盖 |
| M2-3 上限/错误/原子性 | ✅ | `publish_batch_is_atomic_when_one_entry_is_oversized`、`publish_batch_over_the_quota_writes_nothing`、`publish_batch_rejects_empty_and_over_64_entries`、`topic_epoch_is_enforced_on_the_write_path`；错误码/trailer 只按码断言（`error_code_of`） |
| M2-4 Agent 代理 + Rust 客户端 | ✅ | `cargo test -p coord-client --lib mq_v2`（5 例：hint 解析、分区缓存、批上限、配置往返）；`cargo test -p coord-agent --test agent_v2_mode_test`（`services.mq=true` + v2 ⇒ 代理注册、无后端时 `UNAVAILABLE`）；`cargo test -p coord --test mq_v2_m2_cluster_test -- --test-threads=1`（真实 3 节点：follower hint、客户端自动重路由、Agent 端到端发布） |
| M2-5 指标/基准 | ✅ | 指标写入点：`coord_mq_v2_publish_{requests,entries,bytes,duplicates,latency_us,not_leader,rejected}`、`coord_mq_v2_append_{batches,requests}`、`coord_mq_v2_retention_{truncate,segments}`（`cargo test -p coord-server --lib metrics::tests::test_metrics_mq_v2`）；基准入口 `PERF_GATE=1 cargo test --release -p coord --test mq_v2_bench -- --ignored --nocapture --test-threads=1`（M6-3 报告：`docs/production/ops/mq-v2-performance.md`） |

周期保留回收（M1-5 的「周期」项）由 `mq_v2_reaper_pass` 接线：只在分片 Leader 上按
topic 保留期提案 `MqTruncate`，无到期段不提案；关闭态零副作用
（`cargo test -p coord-server --lib mq_v2::reaper`、`retention_reaper_reclaims_expired_segments_on_the_leader`）。

### M3 分项（读路径与消费组）

| 项 | 状态 | 证据 / 未达标原因 |
|:--|:--|:--|
| M3-1 Poll/长轮询 | ✅ | `cargo test -p coord --test mq_v2_m3_consume_test`（8 例：已提交读、tombstone 跳过、`OFFSET_OUT_OF_RANGE` + earliest/latest trailer、未设置起点从最早开始、长轮询被 append 唤醒/超时空批、条数/字节整条截断、Subscribe）；`cargo test -p coord-server --lib mq_v2::consume`（3 例：上限截断、越界 trailer、背压丢弃） |
| M3-2 Ack/AckBatch | ✅ | `cargo test -p coord --test mq_v2_m3_group_test`（11 例，含位点单调、过期代际、整批原子、topic/partition 错误码）；`cargo test -p coord-server --lib mq_v2::groups`（3 例：组 shard 哈希稳定、ack entry 映射、拒绝码） |
| M3-3 组协调 | ✅ | `coord/tests/mq_v2_m3_group_test.rs`：Join 分配 + 已提交位点、同订阅重连不推进代际、未知 topic fail-closed、Heartbeat/Leave、会话过期回收并转移分区、DescribeGroup lag、ListGroups 跨分片分页；reaper 见 `mq_v2_reaper_pass` |
| M3-4 Rust consumer helper | ✅ | `cargo test -p coord-client --lib mq_`（7 例）；`cargo test -p coord --test mq_v2_m3_cluster_test -- --test-threads=1`（真实 3 节点：两成员各占一个分区、成员变化自动重新 Join、离组后接管并从已提交位点续读，无重复投递已 Ack offset） |
| M3-5 Subscribe（best-effort） | ✅ | `subscribe_pushes_new_messages_and_never_replays_history`、`subscribe_rejects_an_explicit_offset_below_earliest`、`consume::tests::subscribe_drops_when_the_queue_is_full_and_stops_when_closed`（满即丢弃、不推进位点、`start_offset=0` = 最新） |
| M3-6 lag/组 TTL | ✅ | DescribeGroup 的 `GroupPartitionLag`（committed/next/lag）在 M3-3 用例中断言；空组 + 位点 TTL 回收：`storage::mq_group::tests::gc_removes_empty_groups_only_after_the_offset_ttl`、`group_ttl_reaper_removes_empty_groups_and_offsets`；指标 `coord_mq_v2_group_{joins,rebalances,expired_members,gc_removed}_total`、`coord_mq_v2_poll_*`、`coord_mq_v2_ack_*`、`coord_mq_v2_subscribe_*` |

组 shard 哈希固定为 FNV-1a 64 取模（跨版本稳定；`DefaultHasher` 会让组的位点在
Rust 升级后漂到别的分片）。

### M4 分项（DLQ 与管理面）

| 项 | 状态 | 证据 / 未达标原因 |
|:--|:--|:--|
| M4-1 MoveToDlq/PollDlq | ✅ | `cargo test -p coord --test mq_v2_m4_dlq_admin_test`（`move_to_dlq_hides_the_message_from_poll_and_is_idempotent`、`move_to_dlq_rejects_an_unknown_offset_and_poll_dlq_paginates`）：复制 + tombstone、`Poll` 跳过、`PollDlq` 原 offset/reason/detail、重复调用 `already_moved`、DLQ 位点分页 |
| M4-3 UpdateTopic/Describe/List | ✅ | 同一测试文件的 `describe_topic_reports_config_partitions_and_bounds`、`list_topics_paginates_and_filters_by_prefix`、`update_topic_merges_fields_and_enforces_the_epoch`；`coord-server/src/mq_v2/admin.rs` 的 2 个合并/校验单测。`UpdateTopic` 的 `0 = 保持当前值`（含 `max_bytes`，清空配额需删除重建）已在代码注释固定 |
| M4-2 DeleteTopic/GetTopicDeletion | ✅ | `cargo test -p coord --test mq_v2_m4_delete_test`（3 例：删除后立即 `TOPIC_NOT_FOUND`、reaper 回收 2 个分区 + 组位点清空 + `GetTopicDeletion` 转 Done、同名重建不复活、重复回收幂等、代际/未知 id 映射）；存储层 `storage::mq_log::tests::drop_partition_removes_records_dlq_and_idempotency_and_replays_idempotently`、`storage::mq_group::tests::drop_topic_removes_only_that_topics_offsets` |
| M4-4 能力/审计 | ✅ | `coord-core/src/grpc_auth.rs::mq_v2_rpcs_are_mapped_with_the_same_capability_split_as_v1`（21 个 RPC 的能力映射）；`dlq_is_gated_by_the_dlq_capability`（未交付 `FAILED_PRECONDITION`）；`mq_v2_g0_test`（关闭/未交付/已交付三态 + reflection）。审计沿用全仓口径（拒绝路径带 RPC 方法与稳定码；成功路径的调用方身份传递是本仓 v2 backlog，`audit/mod.rs` 已文档化），MQ 不另建一套 |

### M6 分项（SDK、Ops、性能、混沌与 Jepsen）

| 项 | 状态 | 证据 / 未达标原因 |
|:--|:--|:--|
| M6-1 Java SDK/示例 | ✅ | **Rust 半边**：`coord-client` 补齐 topic 管理面（`describe_topic`/`list_topics`/`update_topic`/`delete_topic`/`get_topic_deletion`）与 `poll_dlq`（`MqDlqMessage` 带 `reason`/`detail`）；Agent `MqV2Proxy` 从 4 个 RPC 扩到 **18/19**（仅 best-effort `Subscribe` 保留 `UNIMPLEMENTED`，Rust 客户端无 Subscribe）。判据：`cargo test -p coord --test mq_v2_m2_cluster_test -- --test-threads=1`（第 6 段经 Agent 跑通 Describe/List/Join/Poll/Ack/Committed/DescribeGroup/ListGroups/Heartbeat/MoveToDlq/PollDlq/Update/Delete/GetTopicDeletion/Leave）。**Java 半边**：`cn.byteforce.coord.sdk.mq.v2` 的 31 个 record/enum、`client.mqV2()`（18/19 RPC，`Subscribe` 按 best-effort 不实现）与 `MqV2Consumer`（Join/Poll/Ack/Heartbeat + rejoin、topic epoch、offset reset 策略）。判据：`cd coord-java-sdk && mvn -B verify`（`MqV2ModelTest`/`MqV2ClientTest`/`MqV2ConsumerTest`，28 例）；`bash scripts/ci-java-it-cluster.sh start v2` 后 `mvn -B verify -Pit -Dtest=MqV2ClusterIntegrationTest`（3 例：建 topic/幂等重放/双成员消费与 committed·lag/DLQ/update/delete 回收；CI job `java example integration (real server + agent)`）；示例 `bash java-example/scripts/run-v2-example.sh mq` 退出码 0。`check-sdk-sync` 在 PROPOSED 下豁免，翻 COMMITTED 前必须补 |
| M6-2 Ops | ✅ | 告警：`monitoring/prometheus-rules.yml` 的新 group `coord-mq-v2`（9 条：发布非 Leader/拒绝/延迟、Poll 越界、Ack 拒绝、组再平衡/成员过期、DLQ、Subscribe 丢弃）；runbook：`docs/production/ops/runbook.md` §6 的 9 个同名处置小节；dashboard：`monitoring/grafana-dashboard.json` 的 `MQ v2: Publish / Poll` 与 `MQ v2: Groups / DLQ / Retention`；对照表：`docs/production/ops/observability.md` 第 12–14 行。判据：`bash scripts/check-gate-drills.sh`（33 条告警、锚点可达、无孤儿小节；改名注入负控制变红）、`bash scripts/check-doc-refs.sh`、`python3 -c 'import json;json.load(open("monitoring/grafana-dashboard.json"))'`。边界：lag/配额/保留无连续 gauge，按需 `DescribeGroup`/`DescribeTopic`/`GetTopicDeletion`（observability §2.3） |
| M6-3 性能/容量/浸泡 | ✅ | 基准入口 `PERF_GATE=1 cargo test --release -p coord --test mq_v2_bench -- --ignored --nocapture --test-threads=1`（本机 8 vCPU：顺序 283 ops/s / 并发 2133 ops/s / 批深度 5.90；poll 4783 msgs/s / ack 5563 acks/s；4 成员再平衡 0.14s；阈值 = 基线×0.6，`PERF_GATE` fail-closed）。浸泡 `PERF_GATE=1 MQ_V2_SOAK_SECS=600 cargo test --release -p coord --test mq_v2_soak -- --ignored --nocapture --test-threads=1`：261206 条 publish→poll→ack，每分区 offset 无洞/无重复、committed==published、lag 0。报告 `docs/production/ops/mq-v2-performance.md`（环境/命令/阈值/容量；未跑边界：跨机、多组 shard 聚合、72h、容量打满、快照安装） |
| M6-4 混沌/Jepsen | ✅ | 真实 3 进程故障矩阵 `cargo test -p coord --test mq_v2_m6_chaos_test -- --ignored --nocapture --test-threads=1`（CI chaos job 构建并执行）：① 分片 Leader `kill -9`（故障窗口 ≥100 次发布）后 offset 唯一/连续/不复用、重启后可读回；② 组 shard Leader `kill -9` 后 Ack 重试且 committed 单调、重启后保留；③ 配额打满整批原子拒绝、offset 无空洞、已提交可读；④ follower 对称网络分区下多数派可写、heal 后 offset 连续。**发现并修复**：`coord-client` 的分片/组 Leader 缓存原先只跟随 `NOT_LEADER` hint，进程级 kill 后无 hint 的 `UNAVAILABLE` 会把分区永久钉在死地址；`partition_call`/`group_call` 现在驱逐缓存后重新发现（`cargo test -p coord-client --lib` 72 例回归）。边界：快照安装中断由 KV-T chaos 覆盖（MQ 段随 Region 快照恢复），本套件不重复注入；Jepsen 是确定性故障矩阵 + ≥100 样本，不是随机生成器 |
| M6-5 备份/恢复/升级/回退 | ⏳ 未开始 | 整节点备份含 MQ 的演练未做 |

### M5 分项（v2 outbox、durable 与触发）

| 项 | 状态 | 证据 / 未达标原因 |
|:--|:--|:--|
| M5-1 v2 outbox | ✅ | `cargo test -p coord --test mq_v2_m5_durable_test -- --test-threads=1`：`durable_state_and_outbox_commit_in_one_v2_transaction`（实例状态与伴生 outbox 记录 `mod_revision` 相等 = 同一次 `coord.txn.v2`）、`durable_workflow_survives_agent_kill_and_never_duplicates_events`（真实 Agent 进程：relay 经 `coord.mq.v2` 投递、`ce-id` 作幂等键重投 `deduplicated=true`、kill -9 重启后状态/事件仍各一份）；投递面单测 `cargo test -p coord-agent --lib services::mq_v2_delivery`（4 例）。`v2_compat::kv_range` 的 `SAFE_TS_LAG` 重试见 `read_path_retries_only_transient_safe_ts_errors` |
| M5-2 durable 上 v2 | ✅ | 同一 e2e：`data_plane="v2"` + `workflow_durable=true` 的 Agent 启动成功（`Workflow outbox relay started`）、`ListPendingTasks` 可用（审批桥已注入）、重启后续跑；启动门禁 `cargo test -p coord-agent --test agent_workflow_durable_gate_test`（`test_v2_durable_requires_services_mq` / `test_v2_durable_requires_a_server_connection`）。纯 KV 路径不回归：`KvTV2ClusterIntegrationTest#workflowPureKvPathCompletesOnV2` |
| M5-3 Trigger 定义/分发器 | ✅ | 定义声明：顶层 `trigger:{topic,filter,max_attempts}`（ADR-0016 D12），CNCF SW 与遗留 DSL 两条解析路径共用顶层取值；Deploy/Rollback 校验（topic 1..=255 字节、`max_attempts` 1..=32 缺省 3），失败不落库 → `cargo test -p coord-core --lib workflow::trigger`（7 例）、`cargo test -p coord-agent --lib test_deploy_`（含 `test_deploy_rejects_a_malformed_trigger_without_storing_it`）。Start 幂等：`WorkflowStartRequest.idempotency_key=4`（`contracts/v1.12.0`）+ 确定性实例 id（`wf-sha256(definition_id,key)`）⇒ 同键返同一实例、不重置状态 → `start_with_the_same_idempotency_key_returns_the_same_instance`、`a_completed_instance_is_not_reset_by_a_replayed_start`。分发器：对账 5s、组名 `wf-trigger:<definition_id>`、`mq:<topic_id>:<partition>:<offset>` 作键、Start 持久化后 Ack、`max_attempts` 到限 `MoveToDlq`（reason=`workflow_trigger_max_attempts`）+ Ack、不自动建 topic、位点越界按 earliest 重置 → `cargo test -p coord-agent --lib services::workflow_trigger`（7 例，含毒消息/Ack 纪律）。端到端（真实 3 节点 v2 + 真实 Agent 进程）→ `cargo test -p coord --test mq_v2_m5_trigger_test -- --test-threads=1`（2 例：过滤命中/不命中与组位点=3；`kill -9` 重启后实例数不增、位点不倒退）。**边界**：e2e 未构造真实毒消息（毒消息的调用/reason/Ack 由分发器单测钉住，服务端 MoveToDlq/PollDlq wire 由 M4 用例钉住） |
| M5-4 全链路 e2e | ✅ | `cargo test -p coord --test mq_v2_m5_trigger_test -- --test-threads=1` 的 `the_chain_publish_trigger_workflow_outbox_next_trigger_completes`（真实 3 节点 v2 + 真实 Agent 进程）：publish `orders` → 触发器启动 A（`emit order.processed`）→ durable outbox 投递到事件 topic → B 的触发器（`filter.event_type=order.processed`）启动 B → B 完成；中途 `kill -9` 重启后 A/B 各仍 **1** 个实例（不重复触发），A/B 均 `COMPLETED`。过滤掉 `workflow.completed.v1` 是链路不成环的判据（B 自己的完成事件不回灌） |
| M5-5 文档/边界 | ✅ | B-KVT-6 与 `docs/production/workflow-semantics.md` §8 已按 M5-1/M5-2/M5-3 更新；`docs/production/consumer-contract.md` §5.7「工作流 MQ 触发器」给出声明形状、组名、信封、幂等键、Ack 纪律、毒消息 reason、能力与证据锚点；`bash scripts/check-doc-refs.sh` 0 悬空 |

## 3. 关闭 M7 的硬条件

1. M0–M6 每个 gate 的出口判据全绿，证据可在仓库用命令复跑。
2. **KV-T GA 已签字**（`docs/production/ops/kv-t-ga-gate.md` §4）；MQ v2 GA 不早于它。
3. v1 MQ 在 v2 集群仍 fail-closed；`[mq_v2].enabled=false` 时 v1 行为字节级不变。
4. v2 工作流 durable/审批/MQ trigger 的端到端证据（M5-4）通过。
5. 性能/容量/长稳/混沌/Jepsen 证据（M6-3/M6-4）通过；未达标项按 D13 停止，不静默降级。
6. 契约 `coord.mq.v2` 从 PROPOSED 翻 COMMITTED，且 `STATUS.md`/WHITEPAPER/
   consumer-contract/boundaries 同步。
7. §4 签字记录填写；未填即“未签”。

## 4. 签字记录

| 日期 | 结论 | 依据 commit / CI run | 未关闭项 | 签字人 |
|:--|:--|:--|:--|:--|
| — | **未签** | — | §3 全部 | — |

## 5. 回退/降级口径

- `[mq_v2].enabled=false` 回退：停 MQ v2 服务/后台任务；v2 KV/事务面不受影响；
  已写入的 MQ 数据留在 Region 状态机中，重新开启后可继续读。
- **不能**回退到 MQ v1：v2 集群不服务 v1 MQ，也没有 v1→v2 数据迁移（ADR-0016 D1）。
- 任一 gate 未达标 ⇒ 停在 gate，不静默降级语义；需要降级时新增 ADR 并记录风险。
- MQ v2 与 KV-T 的 gate 相互独立：KV-T 已 GA 后，MQ v2 出问题不回退 KV-T 语义。
