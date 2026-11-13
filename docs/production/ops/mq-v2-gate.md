# MQ v2 GA Gate（M0–M7 证据与签字）

> Owner: maintainers ｜ Last verified: 2026-11-13（M0–M3 落地；M4 进行中）
>
> **状态：M0、M1、M2、M3 完成（GO）**。M0-1（#178 链接 #332–#339）、M0-2（`coord.mq.v2`
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
| M4 DLQ/管理面 | 死信/删除回收/更新/能力 | ⏳ 进行中（M4-1/M4-3 ✅） | 见 §2d |
| M5 工作流 | v2 outbox/durable/审批/trigger | ⏳ 未开始 | — |
| M6 SDK/Ops/证据 | Rust/Java/Ops/性能/混沌/Jepsen | ⏳ 未开始 | — |
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
| M2-5 指标/基准 | ✅ | 指标写入点：`coord_mq_v2_publish_{requests,entries,bytes,duplicates,latency_us,not_leader,rejected}`、`coord_mq_v2_append_{batches,requests}`、`coord_mq_v2_retention_{truncate,segments}`（`cargo test -p coord-server --lib metrics::tests::test_metrics_mq_v2`）；基准入口 `cargo test --release -p coord --test mq_v2_bench -- --ignored --nocapture --test-threads=1`（本机：顺序 445 ops/s、并发 2115 ops/s、平均批深度 5.86 req/entry；数字进 M6-3 报告） |

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
| M4-2 DeleteTopic/GetTopicDeletion | ⏳ | 后续 PR：`MqDropPartition` + `MqTopicOp::DeletionProgress` + `MqGroupOp::DropTopic` + reaper 回收 |
| M4-4 能力/审计 | ⏳ | `Dlq` 能力开关与门禁用例已随 M4-1（`dlq_is_gated_by_the_dlq_capability`）；审计事件随 M4-2 的删除回收一起补 |

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
