# 能力边界清单（Scope & non-goals）

> Owner: maintainers ｜ Last verified: 2026-10-01

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
| B-ST-1 | `raft.snapshot_logs_since_last = 0`（= 禁用自动快照）⇒ **raft 日志永不回收**。`LogStore::purge` 要求 tracker 持有覆盖该 index 的持久快照，没有快照该判据恒不成立 | `coord-server/src/raft/mod.rs:76-81`；启动 WARN 在 `coord/src/main.rs`（`snapshot_logs_since_last == Some(0)` 分支） | 区分「禁用自动快照」与「禁用日志回收」两个开关；或显式支持「只 compact 不 snapshot」 |
| B-ST-2 | `compaction.rs` 的 `raft_log_retention_entries` / `tombstone_retention_revisions` 是**死配置**（无读取点），不要按注释理解它的作用 | `coord-server/src/storage/compaction.rs:29-31`（仅测试断言） | 接线或删字段（删字段是破坏性配置变更，需版本说明） |
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
| B-SE-2 | **`transit` 的 KEK 供给**：材料必须是**外部注入**的（环境变量/文件），且**不是外部 KMS**——材料落在 agent 主机上，主机被控即泄露。启用 `transit` 而未注入材料 ⇒ **拒绝启动**（不是降级） | `coord-agent/src/services/transit.rs`（`TransitKekMaterial::resolve_with`）、`coord-agent/src/lib.rs`（`services.transit = true` 分支的 `Err` 返回）；判据：`test_resolve_without_any_material_is_fail_closed`、`test_kek_material_rejects_wrong_length`、`test_kek_comes_from_material_not_from_kek_id` | 接外部 KMS（需先立 ADR：引入外部可用性依赖 + `deny.toml` 依赖面）|
| B-SE-5 | **`transit` 默认关闭**（2026-09-22 起）：因启用它必须先注入 KEK 材料，不满足「启用即可用」；同理 `cache` / `workflow` 默认关闭 | `coord-agent/src/service.rs`（`impl Default` + `test_service_config_defaults`） | 无（这是口径，不是缺陷）|
| B-SE-6 | **换了 KEK 材料 ⇒ 旧材料写下的 DEK 解不开**（密文需用旧材料实例解密后重加密）。**跨材料轮换没有内建流程** | `test_kek_comes_from_material_not_from_kek_id`（同 `kek_id`、不同材料 ⇒ Err） | 给 `transit` 加 KEK 多版本包裹（`kek_id` 已具备版本化语义但未接线多材料）|
| B-SE-3 | 操作员**手工写** `@Bean(destroyMethod="close")` 生命周期；**不提供** Spring Boot starter（产品决策） | 同上 | 恢复 starter（已被明确否决）或继续文档化 recipe |
| B-SE-4 | 登录路径需要 raft quorum：`Authenticate` 要两次 `persist_session` 提案，**follower / 分区期间登录会失败**（客户端 60s 内轮换重试）。**推论（必须告诉接入方）**：客户端 CCT 过期 + 集群无 quorum ⇒ 它**完全不可用**（即使不需要 quorum 的本地读也拿不到新凭据） | `coord-server/src/auth/service.rs:456`（`persist_session`） | **已钉住的关键性质**（2026-09-21）：无 quorum 的登录失败必须携带**可重试**码（`UNAVAILABLE` / `DEADLINE_EXCEEDED`），**不得**与"密码错"混同为 `UNAUTHENTICATED` —— 判据 `auth::service::cct_tests::session_persist_failure_propagates_retryable_code`（含负控制：把它改成 `unauthenticated` ⇒ 必红）+ 对照 `wrong_password_yields_unauthenticated_not_retryable`。<br>仍待办：给 `persist_session` 加**有界重试**以覆盖选举窗口；quorum 整体丢失 > 60s 属固有可用性属性（不得靠改断言消除） |

## 6. 插件 / Agent

| # | 不承诺 | 事实锚点 | 若要变成承诺 |
|:--|:--|:--|:--|
| B-PL-1 | JS/wasm 引擎队列**有上界 16**；wasm 引擎的 `WasmCommand` 队列必须同为有界（曾为无界） | `coord-agent/src/plugin/js_engine.rs:427`、`plugin/component_engine.rs:1490`、`plugin/wasm_engine.rs:1024` | 统一三引擎的背压语义并测试溢出行为 |
| B-PL-2 | agent 原生健康监听器（`http_addr`）**无 accept 上限、每连接一任务、单次 read 无超时** ⇒ 可被 slowloris 耗 fd | `coord-agent/src/health.rs:48-103`；生产已接线（`coord/src/main.rs` dev-mode、`coord-agent/src/lib.rs:1870`） | 加连接上限 + 读超时 |
| B-PL-3 | 缓存 `max_size_bytes` **在 agent 侧被丢弃**（`pub fn new(db_path, _max_size_bytes, …)`），调用点传 1GB 但**无 reaper、无上限** | `coord-agent/src/services/cache.rs:223`、`coord-agent/src/lib.rs:1245` | 落地 LRU/TTL reaper 并测试上界 |

---

## 边界变更

任何一条从「不承诺」变成「承诺」，都必须：① 更新本清单；② 在
`apis/contracts/STATUS.md` 更新对应能力状态；③ 补**负控制**证据。未走完这三步的，
不得当作已承诺。
