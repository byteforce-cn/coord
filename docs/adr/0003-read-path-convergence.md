# ADR-0003: 读路径收敛——配置与发现必须走门面 API

- 状态：accepted
- 日期：2026-09-12
- 决策者：维护团队

## 背景

业务代码若绕过 `ConfigCenter` / `Registry` 门面、直接读 `coord.kv`，会失去三类保证：

- **缓存语义**：`coord.kv.Range` 经 agent 代理层可能命中本地 KV 读缓存，而配置/发现
  数据依赖「变更立即可见」；门面读路径直连 Server 客户端（`coord_client`），不经代
  理层缓存。
- **对账与自我保护**：`Registry` 自带 RegistryCache、self-protection 与对账逻辑，
  直读 KV 拿不到这些保证（陈旧读、缺对账）。
- **实现隔离**：`/_config/`、`/_registry/` 前缀与失效粒度是内部存储布局；布局演进
  （如引入版本后缀/分片）时，直读 KV 的业务代码会静默失效。

## 决定

业务代码读取配置与服务发现结果，必须走 ConfigCenter / Registry API，禁止直读
`coord.kv`：

| 用途 | 允许的 API（Rust / Java SDK） | 禁止的做法 |
|:--|:--|:--|
| 动态配置 | `ConfigCenter` / `ConfigClient` | 业务自行 `Range("/_config/...")` |
| 服务发现 | `Registry` / `Registry` | 业务自行 `Range("/_registry/...")` |
| 分布式锁 | `LockService` / `LockClient` | 业务自行拼 `/_lock/` 的 Txn |
| Leader 选举 | `LeaderElectionService` | 业务自行拼 lease + KV |

## 约束

- 新增配置/发现能力时：先扩 `ConfigCenter` / `Registry` 的门面方法，再在 SDK 暴露；
  不允许在业务模块里新写 `/_config/`、`/_registry/` 前缀字面量。
- 代码评审检查项：搜索业务代码中的 `"/_config/"`、`"/_registry/"` 字面量，命中即要
  求改走门面（`coord-agent` 内部实现文件除外）。

## 后果

- `coord.kv.Range` 的本地读缓存默认关闭（`cache_kv_ttl_secs = 0`）；即便未来开启缓
  存，读路径收敛仍是契约要求（隔离实现细节 + 保留对账/自我保护语义）。
- Java SDK 只暴露 `ConfigClient` / `Registry` 门面；绕开门面直拼 gRPC KV 调用将得不
  到 TLS/CCT 之外的一致性语义与结构化错误码（`ErrorCode`）。
- 未决：独立 Agent 与 Server 侧的读一致性契约（线性读 vs 串行读）尚未区分；若引入
  线性读语义，需新增 ADR 更新本决策。

## 参考

- 代码：`coord-agent/src/services/config_center.rs`（`reload_from_server`）、
  `coord-agent/src/services/registry.rs`、`coord-agent/src/services/cache.rs`
