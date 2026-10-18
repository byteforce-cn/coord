# KV-T G0 契约冻结 + v2 壳 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 落地 ADR-0015 的 G0（issue #128–#135）：v2 契约包、错误码、配置开关、服务壳与门禁，默认关闭且 v1 行为不变。

**Architecture:** 语义单一归属仍是 `docs/adr/0015-kv-t.md`（本计划执行其 D7/D9/D11/D12/D13）。v2 proto 在契约目录与 `coord-proto` 内部副本同路径、逐字相同；`[kv_t] enabled=false` 时 v2 服务仍注册（reflection 可见）但一律 `FAILED_PRECONDITION`，v1 服务注册与路径不变。能力映射复用 v1 的能力名，只新增 `data:tso:allocate` 与 `data:lease:read`。

**Tech Stack:** Rust（coord / coord-core / coord-server / coord-proto）、tonic + prost、protobuf descriptor 校验、Java SDK 错误码枚举、bash CI 门禁。

**Spec:** ADR-0015（D2–D13）+ issue #128–#135（验收项以 issue 为准）。

**Base:** `origin/main@17c302a`，分支 `feat/kv-t-g0-contracts`（PR A）/ `feat/kv-t-g0-shell`（PR B）。

## Global Constraints

- `[kv_t].enabled=false`（默认）时 v1 行为、磁盘布局、服务注册**字节级不变**（ADR-0001）。
- 不实现 TSO/revision/事务/Commit Log/Split（G1+）；G0 只交付合约、开关、壳与门禁。
- 错误码只追加，Rust `CoordErrorCode` ↔ Java `ErrorCode` 逐字一致；禁止解析错误文案。
- 契约副本（`apis/contracts/proto/`）与内部副本（`coord-proto/src/proto/`）v2 文件逐字相同。
- 注释只写当前「为什么」；不新增 TODO/FIXME；临时文件只在 `/tmp`；文档引用 0 悬空。
- 提交前本地跑：`cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`cargo test`、`bash scripts/check-panics.sh`、`cargo audit`、`cargo deny check`、`bash scripts/check-doc-refs.sh`、契约脚本。

## Review Focus

1. **关闭时零副作用**：`enabled=false` 不得启动后台任务、不得改变 v1 服务/能力表/磁盘布局；启动日志只多一行。
2. **v2 契约漂移**：契约侧删字段/改字段号/改类型必须被 descriptor 置红（v2 归入 strict 层，不是 subset）。
3. **鉴权 fail-closed**：新增 v2 RPC 必须同时有 `rpc_capability` 映射与 scope 判定（unary 走 body 提取，Watch 走 handler 延迟判定）；漏登记即被能力门禁测试抓住。
4. **前置校验**：`kv_t.enabled=true` 但缺 multi_raft/PD，或与 `legacy_migration` 同时开启 → 启动失败且错误点名配置项。
5. **错误码跨语言**：新增码缺 Java 侧会在客户端静默降级为 INTERNAL；`check-error-code-contract.sh` 必须绿。
6. **reflection/门禁**：v2 服务未实现时返回 `FAILED_PRECONDITION`（不是 `UNIMPLEMENTED`/`UNAVAILABLE`），且不泄露内部实现状态。

---

### Task 1 (#128/#129): ADR 接受与 D12/D13 协议口径

**Files:**
- Modify: `docs/adr/0015-kv-t.md`（状态、D7 增补 `coord.tso.v2`、D12、D13）
- Modify: `docs/adr/README.md`（索引状态 accepted）

- [x] ADR 状态改 accepted，评审记录指向 #128/#129
- [x] D12：TSO 获取、服务端读集会话、CommitDecision/safe_ts 可见性、批语义、错误/路由
- [x] D13：v1 不服务、能力矩阵、无迁移/回滚边界、`legacy_migration` 互斥
- [x] 索引同步

### Task 2 (#130): v2 契约路径与工具链

**Files:**
- Create: `apis/contracts/proto/coord/{tso,kv,txn,watch,lease,storage}/v2/<domain>.proto`
- Create: `coord-proto/src/proto/coord/{tso,kv,txn,watch,lease,storage}/v2/<domain>.proto`（逐字相同）
- Modify: `coord-proto/build.rs`、`coord-proto/src/lib.rs`
- Modify: `apis/contracts/scripts/check-wire-sync.sh`（v2 循环）
- Modify: `apis/contracts/scripts/check-wire-descriptor.sh`（实现侧递归收集）
- Modify: `apis/contracts/scripts/descriptor_signature.py`（`.v2` → strict）
- Modify: `apis/contracts/scripts/check-sdk-sync.sh`（v2 数据面在 #172 前不要求 SDK impl）
- Modify: `apis/contracts/buf.yaml`（说明 v2 沿用同名 service/enum 例外）

- [x] 六个 v2 包落两侧、`cargo build -p coord-proto` 通过
- [x] `check-wire-sync.sh` / `check-wire-descriptor.sh` / `buf lint` 绿
- [x] descriptor 负向能力保留（v2 strict；删字段/改号即红）

### Task 3 (#131): kv/txn/watch v2 语义契约 + 错误码

**Files:**
- 已建：三个 v2 proto（字段/零值/幂等/上限/失败语义写在头注）
- Modify: `coord-core/src/error_code.rs`（8 个新码 + `ALL` + 必存测试）
- Modify: `coord-java-sdk/src/main/java/cn/byteforce/coord/sdk/ErrorCode.java`
- Modify: `coord-java-sdk/src/test/java/cn/byteforce/coord/sdk/ErrorCodeTest.java`
- Modify: `apis/contracts/WHITEPAPER.md` §6.1.1/§6.2

- [x] 8 个码双语言一致（`TXN_CONFLICT`/`TXN_NOT_FOUND`/`TXN_TOO_LARGE`/`READ_SET_TOO_LARGE`/`COMMIT_REJECTED`/`EPOCH_STALE`/`HISTORY_UNAVAILABLE`/`SAFE_TS_LAG`）
- [x] `check-error-code-contract.sh` 绿
- [x] WHITEPAPER 记录状态映射与重试纪律

### Task 4 (#132): lease/storage v2 契约 + `/obj/` 例外

- [x] `coord.lease.v2`：全局 lease 表、跨区 revoke 的 commit_ts、幂等、keepalive/TimeToLive
- [x] `coord.storage.v2`：manifest commit_ts、read_ts 快照读、`/obj/` 固定区间、chunk 不随 split 迁移
- [x] descriptor/wire 检查绿（与 Task 2 同一次运行）

### Task 5 (#133): `[kv_t]` 配置面与校验

**Files:**
- Modify: `coord/src/config.rs`（`KvTConfig` + `validate` 扩展 + tests）
- Modify: `config.example.toml`（默认 off + 注释引用 ADR-0015 D9）
- Modify: `docs/production/ops/boundaries.md`（仅登记配置口径指向 ADR）

**Interfaces:**
- Produces: `Config.kv_t: KvTConfig`；默认值逐字等于 D9；
  `KvTConfig::validate(&self, multi_raft_enabled: bool, pd_enabled: bool, legacy_migration: bool, allow_unmigrated: bool) -> Vec<String>`

- [ ] 字段：`enabled` / `tso_batch_size`(1024) / `commit_sequencer_batch`(256) / `commit_log_shard_records`(1_000_000) / `commit_log_shard_secs`(3600) / `shard_seal_timeout_secs`(300) / `watch_retention`(86400，1h–7d) / `max_txn_bytes`(4MiB) / `max_read_set`(4096) / `max_range`(10000) / `read_ts_wait_ms`(2000) / `txn_ttl_secs`(60)
- [ ] 测试：默认值、1h/7d 边界、enabled 缺 multi_raft/PD、与 legacy_migration/allow_unmigrated 互斥、越界值点名

### Task 6 (#134): v2 服务壳、能力门禁、启动校验

**Files:**
- Create: `coord-server/src/kvt/mod.rs`（`KvTState` / `KvTCapability` / gate）
- Create: `coord-server/src/kvt/services.rs`（v2 tonic trait impl）
- Modify: `coord-server/src/lib.rs`、`coord-server/src/server/mod.rs`（`CoordNode.kvt`）
- Modify: `coord/src/main.rs`（KvTState 装配、服务注册、启动日志）
- Modify: `coord-core/src/grpc_auth.rs`（v2 路径能力、streaming/deferred/scope 集合）
- Modify: `coord-agent/src/auth/interceptor.rs`（`SCOPE_BEARING_RPCS` v2 unary）
- Modify: `coord-server/src/auth/capability.rs`（`data:tso:allocate`、`data:lease:read`）
- Modify: `coord-server/src/metrics.rs`（`coord_kv_t_enabled`、`coord_kv_t_gated_rejections_total`）

**Interfaces:**
- Produces: `KvTState::{disabled, new, is_enabled, gate(cap) -> Result<(), Status>, config()}`；
  `KvTCapability::{Tso,Kv,Txn,Watch,Lease,Storage}`
- Gate 语义：`enabled=false` → `FAILED_PRECONDITION("kv_t disabled (…[kv_t].enabled=false)")`；
  `enabled=true` 但能力未交付 → `FAILED_PRECONDITION("kv_t enabled but <cap> not implemented")`

- [ ] 六个 v2 服务始终注册；关闭时全部 `FAILED_PRECONDITION`，v1 不变
- [ ] 能力映射 + scope 判定齐备；缺能力 `PERMISSION_DENIED`，有权限但未启用 `FAILED_PRECONDITION`
- [ ] 启动：enabled 缺前置 → 拒绝且可诊断；成功时日志/metrics 暴露开关
- [ ] 单测：gate 码/消息、能力门禁测试（descriptor 驱动）、配置装配

### Task 7 (#135): 文档登记

**Files:**
- Modify: `apis/contracts/STATUS.md`（v2 行：COMMITTED + 期限与 gate 对齐）
- Modify: `apis/contracts/CHANGELOG.md`、`apis/contracts/README.md`
- Modify: `apis/contracts/WHITEPAPER.md` §4/§10/§11
- Modify: `docs/production/consumer-contract.md`（v2 页 + v1 冻结）
- Modify: `docs/production/ops/boundaries.md`（B-RG-1/B-RG-2、`/obj/`、无迁移、保留窗口、serializable 上限、MQ 缺席）

- [ ] `STATUS.md` 的 v2 行能被 wire-sync 解析（`package coord.<domain>.v2;` 已落地）
- [ ] 每条边界有实现锚点或 ADR 引用；`check-doc-refs.sh` 0 悬空

## Execution

- PR A（Task 1–4）：`feat/kv-t-g0-contracts`；关闭 #128/#129/#130/#131/#132。
- PR B（Task 5–7）：`feat/kv-t-g0-shell`；关闭 #133/#134/#135。
- 每个 PR：本地门禁 → push → `gh pr create` → `gh pr checks --watch` → 全绿后 merge（coord 用 `--merge`）→ 删分支 → `gh issue close` 附证据。
