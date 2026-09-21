# 明确的「不承诺」边界清单（W1-4 / §10 规则 6 口径）

- **日期**：2026-09-21
- **锚点**：`docs/production/production-readiness-plan-2026-09-21.md` §10「逐条列出能力边界」、
  §4.2 G6「默认开关口径」；`apis/contracts/WHITEPAPER.md` §10 规则 4
  （「每个能力的**不承诺**与承诺同等重要」）
- **纪律**：本清单里的每一条都是**已核实的事实**（带 `file:line` 或可重跑命令）。
  凡「不承诺」，就不允许在对外材料里被写成承诺。
- **反向纪律**：列入本清单**不等于**放弃——每条都标了「若要变成承诺，需要做什么」。

---

## §1 工作流（workflow / scheduler）

| # | 不承诺 | 事实锚点 | 若要变成承诺 |
|:--|:--|:--|:--|
| B-WF-1 | **实例与定义是 append-only**：没有删除/保留策略 API。`WorkflowStore` 未声明任何 `remove/delete/retain/clear`，`MemoryWorkflowStore`（同时是 `KvWorkflowStore` 的本地读缓存）因此**永不收缩** | `coord-core/src/workflow/ports.rs:344`（trait 方法清单）、`coord-agent/src/services/workflow_store.rs:43` | 走 raft 增加 `DeleteInstance`/`DeleteDefinition` + 缓存失效路径；或增加 TTL/归档 |
| B-WF-2 | 子流程恢复扫描是**周期轮询**（5s）。本轮已把稳态代价从 O(全部实例) 降到 O(待恢复父子对)，但**仍是轮询**，且扫描任务本身**不受 supervisor 监督** | `coord-core/src/workflow/runtime.rs`（`SUBFLOW_SCAN_INTERVAL_SECS`、`pending_subflows`） | 事件驱动化 + 把扫描任务纳入监督并配 `coord_dead_background_tasks` |
| B-WF-3 | 子流程恢复时**若父定义暂时读不到**，该父实例会停在 `Running` 且不再被扫描器接管 | `runtime.rs::resume_parent_after_subflow`（`return false` 分支） | 增加「已恢复但未驱动」的持久标记 + 启动期重扫 |
| B-WF-4 | `MemoryWorkflowStore` **不是**生产存储（仅在无 KV 的测试/单机路径使用）；生产用 `KvWorkflowStore` | `coord-agent/src/services/workflow_store.rs:32-46` | 无需（这是设计），但接入方必须知道 |

## §2 存储 / Raft

| # | 不承诺 | 事实锚点 | 若要变成承诺 |
|:--|:--|:--|:--|
| B-ST-1 | `raft.snapshot_logs_since_last = 0`（= 禁用自动快照）⇒ **raft 日志永不回收**。`LogStore::purge` 要求 tracker 持有覆盖该 index 的持久快照，没有快照该判据恒不成立 | `coord-server/src/raft/mod.rs:76-81`；启动 WARN 在 `coord/src/main.rs`（`snapshot_logs_since_last == Some(0)` 分支） | 区分「禁用自动快照」与「禁用日志回收」两个开关；或显式支持「只 compact 不 snapshot」 |
| B-ST-2 | `compaction.rs` 的 `raft_log_retention_entries` / `tombstone_retention_revisions` 是**死配置**（无读取点），不要按注释理解它的作用 | `coord-server/src/storage/compaction.rs:29-31`（仅测试断言） | 接线或删字段（删字段是破坏性配置变更，需版本说明） |
| B-ST-3 | 对象存储：快照恢复落后的节点会**清空本地 chunk 并 rebuild**，期间 `Get` 返回 `UNAVAILABLE`；全集群配置必须一致 | `docs/production/volume-object-storage.md`、`docs/production/ops/runbook.md` §3.3 | 增量 rebuild / 本地缓存保留策略 |

## §3 Multi-Raft / Region

| # | 不承诺 | 事实锚点 | 若要变成承诺 |
|:--|:--|:--|:--|
| B-RG-1 | **不支持动态增删 region**。`remove_region_raft` / `remove_region_runtime` 零调用点（含测试）；启动后新建的 region 也**拿不到 `CompactionManager`**（启动期一次性从 `list_regions()` 构建），其 changelog/tombstone 永不压缩、raft 日志永不回收 | `coord-server/src/raft/network.rs:1085`、`coord-server/src/raft/region.rs:458`、`coord/src/main.rs:3120-3145` | 接线 `remove_region_*` + 让 `_region_compaction_mgrs` 随 region 生命周期增删 |
| B-RG-2 | region 模式下 watch **拒绝跨 region 与全键空间前缀**（`prefix_successor` 为 `None` → `INVALID_ARGUMENT`） | `coord-server/src/server/mod.rs:553-581` | 增加跨 region watch 扇出 |

## §4 连接与并发

| # | 不承诺 | 事实锚点 | 若要变成承诺 |
|:--|:--|:--|:--|
| B-CX-1 | **没有连接数上限**。`max_concurrent_streams` 限的是**每连接**的流数（512 客户端口 / 256 raft 口），不是连接数；唯一的 Semaphore 类原语是快照字节限流 | `coord-server/src/storage/snapshot_limiter.rs`、`coord-server/src/server/mod.rs`（tonic 装配） | 加 `ConcurrencyLimitLayer` / accept 限流 + `max_connection_age`，并配指标与告警 |
| B-CX-2 | 客户端连接池的**回收是机会式**的（挂在取连接路径上，窗口 = `idle_timeout`，最小节流 60s），不是定时后台回收 | 本仓 `coord-client/src/pool.rs`（`maybe_sweep`） | 若需要严格定时回收，改由调用方持有 runtime 时启动 reaper |

## §5 鉴权 / 安全

| # | 不承诺 | 事实锚点 | 若要变成承诺 |
|:--|:--|:--|:--|
| B-SE-1 | **TLS 不是 fail-closed**（0.2.0 现状）：`dev` 模式、以及「鉴权开启 + 有 `raft_shared_secret`」的集群仍可**明文启动**。只有 (a) 鉴权关闭且 bind 非 loopback、(b) raft 端口非 loopback 且既无 raft mTLS 又无共享密钥，这两种情况才拒绝启动 | `README.md:61` 自述；`coord/tests/dev_insecure_bind_test.rs` | 见 `docs/production/ops/security.md`（W4-1 方案，**未实施**） |
| B-SE-2 | **KEK 由配置串确定性派生**：拿到配置即可推导 KEK（非外部 KMS） | `apis/contracts/WHITEPAPER.md` §12.7 | 见 `security.md`（W4-2 / U-04，**待裁定**） |
| B-SE-3 | 操作员**手工写** `@Bean(destroyMethod="close")` 生命周期；**不提供** Spring Boot starter（产品决策，见 `remaining-known-gaps.md:99/:104-114`） | 同上 | 恢复 starter（已被明确否决）或继续文档化 recipe |
| B-SE-4 | 登录路径需要 raft quorum：`Authenticate` 要两次 `persist_session` 提案，**follower / 分区期间登录会失败**（客户端 60s 内轮换重试） | `coord-server/src/auth/service.rs:456`（`persist_session`）、`jepsen/docs/coord-findings.md` §F-05 | W1-2：给 `persist_session` 加有界重试以覆盖选举窗口；quorum 整体丢失 > 60s 属固有可用性属性，需 §5.4-④ 裁定 |

## §6 插件 / Agent

| # | 不承诺 | 事实锚点 | 若要变成承诺 |
|:--|:--|:--|:--|
| B-PL-1 | JS/wasm 引擎队列**有上界 16**；wasm 引擎的 `WasmCommand` 队列历史上是**无界**（第四轮 §3.10 i） | `coord-agent/src/plugin/js_engine.rs:427`、`plugin/component_engine.rs:1490`、`plugin/wasm_engine.rs:1024` | 统一三引擎的背压语义并测试溢出行为 |
| B-PL-2 | agent 原生健康监听器（`http_addr`）**无 accept 上限、每连接一任务、单次 read 无超时** ⇒ 可被 slowloris 耗 fd | `coord-agent/src/health.rs:48-103`；生产已接线（`coord/src/main.rs` dev-mode、`coord-agent/src/lib.rs:1870`） | 加连接上限 + 读超时 |
| B-PL-3 | 缓存 `max_size_bytes` **在 agent 侧被丢弃**（`pub fn new(db_path, _max_size_bytes, …)`），调用点传 1GB 但**无 reaper、无上限** | `coord-agent/src/services/cache.rs:223`、`coord-agent/src/lib.rs:1245` | 落地 LRU/TTL reaper 并测试上界 |

---

## §7 与门禁的关系

本清单是 §5.4 参数确认 ③ 的**输入之一**。任何一条从「不承诺」变成「承诺」，
都必须：① 改本清单；② 在 `STATUS.md` 更新能力状态；③ 补**负控制**证据
（见 §7 证据规范第 3 条）。**未走完这三步的，不得当作已承诺。**
