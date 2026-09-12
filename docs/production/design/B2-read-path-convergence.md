# B2 · 读路径收敛（配置中心 / 服务发现）

- **状态**：已定稿（2026-09-12）
- **来源**：架构评审复核项 B2（设计性建议）
- **适用范围**：`coord-agent` 高级服务（ConfigCenter / Registry）及其 SDK 封装

---

## 决策

**业务代码读取配置与服务发现结果，必须走 ConfigCenter / Registry API，禁止直读
`coord.kv`。**

| 用途 | 允许的 API | 禁止的做法 |
|:--|:--|:--|
| 动态配置 | `ConfigCenter`（Rust）/ `ConfigClient`（Java SDK） | 业务自己 `Range("/_config/...")` |
| 服务发现 | `Registry`（Rust）/ `Registry`（Java SDK） | 业务自己 `Range("/_registry/...")` |
| 分布式锁 | `LockService` / `LockClient` | 业务自己拼 `/_lock/` 的 Txn |
| Leader 选举 | `LeaderElectionService` | 业务自己拼 lease + KV |

## 为什么（不是风格偏好，是正确性）

1. **绕过本地读缓存语义**。`coord.kv.Range` 经 agent 代理层时可能命中本地 KV
   读缓存（B1 项）。而配置/发现数据的正确性依赖「变更立即可见」，因此
   ConfigCenter / Registry 的读路径**直连 Server 客户端**（`coord_client`），
   不经代理层缓存：
   - 配置：`coord-agent/src/services/config_center.rs` 的 `reload_from_server`
     直接 `inner.client.kv().range(prefix, range_end, 0, 0)`；
   - 发现：`coord-agent/src/services/registry.rs` 自带 RegistryCache、self-protection
     与对账逻辑。
   业务直接读 `coord.kv` 会失去这些保证（陈旧读、缺对账）。
2. **前缀约定与失效粒度**。`/_config/`、`/_registry/` 是内部存储布局，属于实现
   细节；一旦布局演进（如引入版本后缀/分片），直读 KV 的业务代码会静默失效。
3. **SDK 一致性**。Java SDK 只暴露 `ConfigClient` / `Registry` 门面；若业务绕开
   门面直拼 gRPC KV 调用，将无法获得 TLS/CCT 之外的一致性语义与错误码规范
   （`ErrorCode` 结构化错误）。

## 实施约束

- 新增配置/发现能力时，先扩 `ConfigCenter`/`Registry` 的门面方法，再在 SDK 暴露；
  不允许在业务模块里新写 `/_config/`、`/_registry/` 前缀字面量。
- 代码评审检查项：搜索业务代码中的 `"/_config/"`、`"/_registry/"` 字面量；
  命中即要求改走门面（`coord-agent` 内部实现文件除外）。

## 验证方式

- B1 整改后，`coord.kv.Range` 的本地读缓存**默认关闭**
  （`cache_kv_ttl_secs = 0`），因此「业务直读 KV」至少不再因缓存而产生陈旧读；
  但读路径收敛仍是**契约要求**（隔离实现细节 + 保留对账/自我保护语义）。
- 回归固化：`ConfigCenter` / `Registry` 的全量加载与对账行为由
  `coord-agent` 既有测试覆盖；后续新增门面方法时必须补对应测试。

## 未决项

- 独立 Agent 与 Server 侧的读一致性契约（线性读 vs 串行读）尚未在文档中区分；
  若引入线性读语义，需同步更新本记录。
