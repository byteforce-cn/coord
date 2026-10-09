# 能力边界清单（Scope & non-goals）

> Owner: maintainers ｜ Last verified: 2026-10-09

本清单逐条列出各能力**明确不承诺**的边界，与承诺同等重要（见
`apis/contracts/WHITEPAPER.md`）。每条都带可核对的实现锚点；凡「不承诺」，
不得在对外材料里被写成承诺。列入本清单**不等于**放弃——每条都标了
「若要变成承诺，需要做什么」。

---

## 1. 工作流（workflow / scheduler）

| # | 不承诺 | 事实锚点 | 若要变成承诺 |
|:--|:--|:--|:--|
| B-WF-1 | **实例与定义是 append-only**：没有删除/保留策略 API。`WorkflowStore` 未声明任何 `remove/delete/retain/clear`，`MemoryWorkflowStore`（同时是 `KvWorkflowStore` 的本地读缓存）因此**永不收缩** | `coord-core/src/workflow/ports.rs:344`（trait 方法清单）、`coord-agent/src/services/workflow_store.rs:43` | 走 raft 增加 `DeleteInstance`/`DeleteDefinition` + 缓存失效路径；或增加 TTL/归档 |
| B-WF-2 | 子流程恢复扫描是**周期轮询**（5s）。稳态代价已从 O(全部实例) 降到 O(待恢复父子对)，但**仍是轮询**，且扫描任务本身**不受 supervisor 监督** | `coord-core/src/workflow/runtime.rs`（`SUBFLOW_SCAN_INTERVAL_SECS`、`pending_subflows`） | 事件驱动化 + 把扫描任务纳入监督并配 `coord_dead_background_tasks` |
| B-WF-3 | 子流程恢复时**若父定义暂时读不到**，该父实例会停在 `Running` 且不再被扫描器接管 | `runtime.rs::resume_parent_after_subflow`（`return false` 分支） | 增加「已恢复但未驱动」的持久标记 + 启动期重扫 |
| B-WF-4 | `MemoryWorkflowStore` **不是**生产存储（仅在无 KV 的测试/单机路径使用）；生产用 `KvWorkflowStore` | `coord-agent/src/services/workflow_store.rs:32-46` | 无需（这是设计），但接入方必须知道 |

## 2. 存储 / Raft

| # | 不承诺 | 事实锚点 | 若要变成承诺 |
|:--|:--|:--|:--|
| B-ST-1 | raft 日志回收**快照锚定**（ADR-0004）：缺省自动快照（每 5000 条）开启即回收（快照后保留窗口 `max_in_snapshot_log_to_keep`，默认 1000）；「不依赖快照的回收 / 只 compact 不 snapshot」**不实现**（上游 `purged ≤ snapshot` 不变量）。`snapshot_logs_since_last = 0` = 手动快照模式 ⇒ **不回收**（启动 WARN；手动快照/`purge_log` 触发面未开放） | `coord-server/src/raft/mod.rs`（`apply_tuning`）、`coord-server/src/raft/log_store.rs`（`purge` 守卫）、`coord/src/main.rs`；语义见 ADR-0004 | 需要时开放本节点手动快照 / 手动 purge 的运维出口（语义仍以快照为锚） |
| B-ST-3 | 对象存储：快照恢复落后的节点会**清空本地 chunk 并 rebuild**，期间 `Get` 返回 `UNAVAILABLE`；全集群配置必须一致 | `docs/production/volume-object-storage.md`、`docs/production/ops/runbook.md` | 增量 rebuild / 本地缓存保留策略 |

## 3. Multi-Raft / Region

| # | 不承诺 | 事实锚点 | 若要变成承诺 |
|:--|:--|:--|:--|
| B-RG-1 | **不支持动态增删 region**。`remove_region_raft` / `remove_region_runtime` 零调用点（含测试）；启动后新建的 region 也**拿不到 `CompactionManager`**（启动期一次性从 `list_regions()` 构建），其 changelog/tombstone 永不压缩、raft 日志永不回收 | `coord-server/src/raft/network.rs:1085`、`coord-server/src/raft/region.rs:458`、`coord/src/main.rs:3120-3145` | 接线 `remove_region_*` + 让 `_region_compaction_mgrs` 随 region 生命周期增删 |
| B-RG-2 | region 模式下 watch **拒绝跨 region 与全键空间前缀**（`prefix_successor` 为 `None` → `INVALID_ARGUMENT`） | `coord-server/src/server/mod.rs:553-581` | 增加跨 region watch 扇出 |

## 4. 连接与并发

| # | 不承诺 | 事实锚点 | 若要变成承诺 |
|:--|:--|:--|:--|
| B-CX-1 | 客户端口的**连接数**已有全局上限（`network.max_connections`，默认 4096，超限在 accept 后立即断开），但**没有连接寿命/空闲回收**（`max_connection_age` 类语义未实现）；被静默连接占满配额时新连接会被拒（`coord_grpc_connections_rejected_total` 可见） | `coord-server/src/server/connection_gate.rs`、`coord/src/main.rs`（装配）、`coord-server/src/metrics.rs` | 若需要「占坑」自愈，评估 tonic 的连接寿命/空闲超时能力后再接线（勿承诺不存在的语义） |
| B-CX-2 | 客户端连接池的**回收是机会式**的（挂在取连接路径上，窗口 = `idle_timeout`，最小节流 60s），不是定时后台回收 | 本仓 `coord-client/src/pool.rs`（`maybe_sweep`） | 若需要严格定时回收，改由调用方持有 runtime 时启动 reaper |

## 5. 鉴权 / 安全

| # | 不承诺 | 事实锚点 | 若要变成承诺 |
|:--|:--|:--|:--|
| B-SE-1 | **TLS 已默认 fail-closed**（2026-09-26 落地）：鉴权开启 + gRPC bind 非 loopback **必须**配置 `security.tls_cert/tls_key`（mTLS 另配 `tls_ca`），否则**拒绝启动**（`R-SEC-04`，不静默降级明文）；连同既有两条——(a) 鉴权关闭且 bind 非 loopback、(b) raft 端口非 loopback 且既无 raft mTLS 又无共享密钥——共三处拒绝共用同一判定实现 | `coord/src/main.rs`（6.5 节拒绝块 + `is_non_loopback_bind`）；判据：`coord/tests/plaintext_remote_failclosed_test.rs`（4 条正反双向，含逃生阀与 TLS 正例）；`README.md` / `README.zh-CN.md` 的 TLS 行 | dev/test 的唯一逃生阀是显式 `security.allow_plaintext_remote = true`（默认 false，启动 WARN 明示；如 jepsen lab）——不是缺口而是**显式 opt-in**；生产不得开启 |
| B-SE-2 | **`transit` 的 KEK 供给（生产路径）**：材料必须是**外部注入**的（环境变量/文件），且**不是外部 KMS**——材料落在 agent 主机上，主机被控即泄露。启用 `transit` 而未注入材料 ⇒ **拒绝启动**（不是降级）。**dev 例外：**`coord dev` 经进程内 builder 使用内建 dev 默认 KEK（见 B-SE-8），`agent` 子命令路径不受影响 | `coord-agent/src/services/transit.rs`（`TransitKekMaterial::resolve_with`）、`coord-agent/src/lib.rs`（`services.transit = true` 分支的 `Err` 返回）；判据：`test_resolve_without_any_material_is_fail_closed`、`test_kek_material_rejects_wrong_length`、`test_kek_comes_from_material_not_from_kek_id` | 接外部 KMS（需先立 ADR：引入外部可用性依赖 + `deny.toml` 依赖面）|
| B-SE-5 | **`transit` 默认关闭**（2026-09-22 起）：因启用它必须先注入 KEK 材料，不满足「启用即可用」；同理 `cache` / `workflow` 默认关闭 | `coord-agent/src/service.rs`（`impl Default` + `test_service_config_defaults`） | 无（这是口径，不是缺陷）|
| B-SE-6 | **历史材料下线前提**：KEK 多材料解密窗口已落地（G-TR-1）——主材料 + 历史材料并存，密文/DEK 记录携带材料标识，旧材料密文在新材料实例可读（旧记录无标识按「主→历史」试解）。**仍不承诺**：① 材料托管——材料仍是外部注入的 env/文件（非外部 KMS，见 B-SE-2）；② 移除历史材料后，未 rewrap 的旧材料密文不可解（解密报 `KEK material '<id>' is not injected`，fail-loud）——迁移完成前不得下线旧材料（流程与回退路径见 runbook §4.4） | `coord-agent/src/services/transit.rs`（`TransitKekKeyring` / `unwrap_dek` / `rewrap`）、`coord-agent/src/services/transit_store.rs`（`DekRecord.kek_id`）；判据：`test_multi_material_decrypt_window_and_rewrap`（含负控制）、`test_legacy_record_without_material_id_falls_back_to_try_all`、`test_keyring_old_material_parsing_rules` | 接外部 KMS（需先立 ADR，同 B-SE-2）|
| B-PKI-1 | **PKI：非 HSM + 无吊销**（设计口径与应急 playbook 见 `security.md` §3，G-PKI-3）：① `GetCertByCN` **返回私钥** ⇒ 权限按「私钥读取权」定级（`ListCerts` 才是无私钥摘要）；② 无 CRL/OCSP、无 revoke RPC——泄露应急＝消费侧剔除 + CN 轮换；**CA 侧无法阻止**旧证书在「不校验本 store」的消费方继续通过链验证；③ CA/签名私钥驻内存或文件（非 HSM），主机被控即私钥被控 | `apis/contracts/proto/coord/pki/v1/pki.proto`（RPC 面：无 revoke；`GetCertByCN` 头注释“含私钥”）、`coord-agent/src/pki.rs`（`issue_cert` / `rotate_cert` / `renew_cert` / `verify_cert`）；判据：`test_rotate_cert_replaces_expired_record`、`test_concurrent_rotate_no_lost_update`、`coord-agent/tests/agent_pki_test.rs::test_rotate_then_list_returns_active_and_retired` | 到期→换新与到期观测已随 G-PKI-1/PKI-2 落地；吊销面（CRL/OCSP 或在线校验出口）与 HSM/PKCS#11 需另行 ADR（触发条件见 `security.md` §3.3）|
| B-TR-1 | **不提供远程非对称签名**（G-TR-2 产品决策定谳）：transit 维持对称加密 + HMAC；PKI 维持「密钥交付持有方」模型。「私钥不出域」的签名场景（token / 审计锚点等）不在承诺内 | ADR-0010 §16；`apis/contracts/proto/coord/transit/v1/transit.proto`（无 Sign 类 RPC）；消费侧展示：`docs/production/consumer-contract.md` §10 / §13 | 组织 KMS 方向确定要求「私钥不出域」时另行立项评估（候选：transit 增 ES256 Sign，或独立 KMS 服务；先设计后实施）|
| B-SE-3 | 操作员**手工写** `@Bean(destroyMethod="close")` 生命周期；**不提供** Spring Boot starter（产品决策） | 同上 | 恢复 starter（已被明确否决）或继续文档化 recipe |
| B-SE-4 | 登录路径需要 raft quorum：`Authenticate` 要两次 `persist_session` 提案，**follower / 分区期间登录会失败**（客户端 60s 内轮换重试；选举窗口以内的瞬态失败由服务端**有界重试**吸收，见右）。**推论（必须告诉接入方）**：客户端 CCT 过期 + 集群无 quorum ⇒ 它**完全不可用**（即使不需要 quorum 的本地读也拿不到新凭据） | `coord-server/src/auth/service.rs`（`persist_session` / `session_persist_retryable`） | **已钉住的关键性质**（2026-09-21）：无 quorum 的登录失败必须携带**可重试**码（`UNAVAILABLE` / `DEADLINE_EXCEEDED`），**不得**与"密码错"混同为 `UNAUTHENTICATED` —— 判据 `auth::service::cct_tests::session_persist_failure_propagates_retryable_code`（含负控制：把它改成 `unauthenticated` ⇒ 必红）+ 对照 `wrong_password_yields_unauthenticated_not_retryable`。<br>**有界重试**（选举窗口）：仅 `NOT_LEADER` **无** leader hint 时在本节点重试（≤4 次 × 200ms ≈ 600ms）；有 hint 立即透传（客户端重定向，R-SVC-08）、`DEADLINE_EXCEEDED` 不重试 —— 判据 `auth::service::cct_tests::persist_session_*`（5 条，调用计数断言；负控制 3 项实跑命中）与 `authenticate_survives_leaderless_window`。<br>剩余边界：quorum 整体丢失 > 60s 属固有可用性属性（不得靠改断言消除） |
| B-SE-7 | **agent 非 loopback 绑定守卫的 dev 唯一旁路**：仅 `coord dev --allow-insecure` 可达——dev 非 loopback 绑定时 Raft 收敛 loopback（R-SEC-03 判定不动、无需密钥材料）、BFF/UI HTTP 随 bind 同口径，Agent 明文非 loopback 绑定经进程内 builder（`with_dev_allow_insecure_non_loopback`）放行并输出启动 WARN；开关不进 `AgentConfig` serde 面（`agent.toml` / `agent` 子命令不可达，见 ADR-0008） | `coord-agent/src/lib.rs`（`AgentServer` builder + 守卫块）、`coord/src/main.rs`（`run_dev`）；判据：`coord/tests/dev_insecure_bind_test.rs`（非 loopback 正例 + 无 flag 反例）、`coord-agent/tests/agent_non_loopback_guard_test.rs`（旁路正例 + 默认拒绝反例 + 「TOML 无法开启」） | 若要成为无旁路强约束：删除 builder 并让容器场景改走 TLS/auth（dev 专用，当前不计划） |
| B-SE-8 | **dev 专用 transit 默认 KEK**：`coord dev` 在未注入材料（env / 文件均缺）时回退到内建 dev 默认材料（`DEV_DEFAULT_KEK`，固定值 ⇒ **无保密性**，启动 WARN）。回退仅经进程内 builder（`with_dev_default_transit_kek`）可达——`agent.toml` / `agent` 子命令不可达，生产缺材料仍拒绝启动（B-SE-2 消息与语义逐字不变）；dev 数据用默认 KEK 加密后**不可移植**（换环境解不开，fail-loud） | `coord-agent/src/services/transit.rs`（`DEV_DEFAULT_KEK`）、`coord-agent/src/lib.rs`（builder + `Err(_) if dev_default_transit_kek` 回退分支）、`coord/src/main.rs`（`run_dev`）；判据：`coord-agent/tests/agent_dev_transit_kek_test.rs`（默认拒绝 + 「TOML 无法开启」+ 开关正例 gRPC 往返）、`coord/tests/dev_mode_services_test.rs`（dev 端到端 + transit 往返） | 若要成为无旁路强约束：删除 builder 并让 dev 自备材料（dev 专用，当前不计划）；生产形态本就不受影响 |

## 6. 插件 / Agent

| # | 不承诺 | 事实锚点 | 若要变成承诺 |
|:--|:--|:--|:--|
| B-PL-1 | JS/wasm 引擎队列**有上界 16**；wasm 引擎的 `WasmCommand` 队列必须同为有界（曾为无界） | `coord-agent/src/plugin/js_engine.rs:427`、`plugin/component_engine.rs:1490`、`plugin/wasm_engine.rs:1024` | 统一三引擎的背压语义并测试溢出行为 |
| B-PL-3 | 缓存容量上界（默认 1GB）**已由 reaper 强制**：数据面活跃字节记账 + TTL 回收 + 超界淘汰 + 单条超限写拒绝（`RESOURCE_EXHAUSTED`），但**仍不承诺**：<br>(a) 严格逐写上界 —— 周期收敛（默认 10s，周期内可短暂超界；`coord_agent_cache_active_bytes` 可观测）；<br>(b) 真 LRU —— 淘汰按「最后写入」新近度近似（读不刷新，get 零写放大）；<br>(c) 记账只含数据面活跃字节 —— 不含 ISR 复制日志（见 cache.rs 模块头保留声明）与 redb 页面开销（文件体积 > 记账值）；<br>(d) ISR 启用时淘汰/过期回收为**各节点本地行为**，不跨节点复制 | `coord-agent/src/services/cache.rs`（`reap_once` / `account_upsert_tx` / `CACHE_META_TABLE`）、`coord-agent/src/lib.rs`（cache 装配 + 指标采样）、`coord-agent/src/metrics.rs`（`coord_agent_cache_*`） | 逐写严格上界；读触达 LRU；记账覆盖复制日志与文件真空压缩；ISR 一致淘汰 |
| B-PL-4 | MQ 容量上界（默认 1GB）**已在 publish 入口强制**：消息 + DLQ 与数据同事务记账（redb 单写者 ⇒ **逐写严格上界**，无 reaper 周期收敛窗口）；超界与单条超限的 publish 均拒绝 `RESOURCE_EXHAUSTED`（不静默丢历史）。过期回收由 reaper（默认 10s）按 topic 的 `retention_secs` 清扫消息与 DLQ（`0` = 不按时间回收）。**仍不承诺**：<br>(a) 记账只含消息 + DLQ 物理字节 —— 不含消费位点 / 幂等索引 / ISR 复制日志（复制设计保留）与 redb 页面开销（文件体积 > 记账值）；<br>(b) ISR 启用时配额与回收为**各节点本地行为**（Follower 镜像 Leader 已提交的决定并照常记账，但**不做**配额检查——避免复制分叉）；<br>(c) `move_to_dlq` 为维护路径不受配额拒绝（净增仅 reason/detail 开销）；<br>(d) topic 删除的存量回收**仅经 `DeleteTopic`（RPC，全量：消息/DLQ/位点/幂等索引/next-offset + 配额归还）**；**内部分段方法 `delete_topic`（仅删配置行）仍存在**（其他维护路径使用），经它删除后存量行不被清扫但仍计入配额 —— 运营面不得使用内部方法代餐；<br>(e) 时钟回拨等极端情形下，被非过期行挡住的过期行会延迟到后续轮次回收 | `coord-agent/src/services/mq.rs`（`ensure_quota_tx` / `reap_once` / `purge_expired_in_table` / `delete_topic_full` / `MQ_META_TABLE`）、`coord-agent/src/lib.rs`（MQ 装配 + 指标采样）、`coord-agent/src/metrics.rs`（`coord_agent_mq_*`）；删除判据：`test_delete_topic_full_reclaims_all_state`、`test_leader_query_dlq_and_delete_across_agents` | per-topic size-retention（可选 opt-in）；记账覆盖复制日志与文件真空压缩 |

---

## 边界变更

任何一条从「不承诺」变成「承诺」，都必须：① 更新本清单；② 在
`apis/contracts/STATUS.md` 更新对应能力状态；③ 补**负控制**证据。未走完这三步的，
不得当作已承诺。
