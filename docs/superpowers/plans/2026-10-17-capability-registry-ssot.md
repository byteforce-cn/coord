# capability registry 单一事实源 + 机检 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让 `CapabilityRegistry/List`（发现面）的 live 能力集合与 `rpc_capability`（执行面，89 个唯一 ID）**双向恒等**，并让 31 个无引用的历史名字以 `deprecated=true` + 替代项保留；新增机器门禁，任一侧漂移即红。

**Architecture:** 执行面不新增第二份映射（`coord-core/src/grpc_auth.rs` 一行不改）。`coord-server/src/auth/capability.rs` 的 registry 从「81 条手写 `def(...)`」改为两张表：`BUILTIN_CAPABILITY_META`（89 条 live）与 `DEPRECATED_CAPABILITIES`（31 条 + 替代项）。判据来自 **proto 编译期 FileDescriptorSet**（由 `coord-proto` 暴露）：枚举全部 proto 方法 → 映射/豁免分类 → 与 registry live 集合比对，双向差集必须为空。

**Tech Stack:** Rust（coord-proto / coord-server）、prost-types FileDescriptorSet、bash + cargo test、GitHub Actions。

**Spec:** 远程 issue #115「capability registry 与实际执行的能力 ID 双轨」，方案 A（单一事实源 + 机检）。

**Base:** `origin/main@0b0aea2`，分支 `feat/capability-registry-ssot`。

## Global Constraints

- **不改 wire**：`apis/contracts/proto/**` 与 `coord-proto/src/proto/**` 不动；`CapabilityDefinition` 不加字段（替代项写进 `description`）。
- **不动执行面**：`coord-core/src/grpc_auth.rs::rpc_capability` 的方法→能力映射本次不修改；只被测试**读取**。
- **不迁移存量数据**：角色授权落盘格式不变；`RoleGrantCapability` 的校验只影响新写入。
- **fail-closed**：所有比对空集合即视为失败；检查脚本必须断言期望测试真的执行过（未跑 ≠ 通过）。
- 注释只写当前「为什么」；不新增 TODO/FIXME；临时文件只在 `/tmp`；`bash scripts/check-doc-refs.sh` 保持 0 悬空。

## Review Focus

1. **新 RPC 漏登记**：新增 proto 方法但没写 `rpc_capability` 映射时，必须在 CI 红（豁免名单是逐字常量，不允许靠新增豁免悄悄过）。
2. **registry 与执行面单向漂移**：只补 registry 不补映射（或反之）必须红——两个方向都要比。
3. **deprecated 语义**：`deprecated=true` 的能力**永不执行**，且必须能指出替代项（或明确的「无替代」原因）；授权这类名字必须被拒。
4. **授权输入**：拼错/未知/已废弃的能力名 → `INVALID_ARGUMENT` 且**点名 ID**；撤销必须放行任意 ID（否则改造前写错的存量授权删不掉）。
5. **发现面可读性**：`coord capability list` 从 81 项变 120 项（89 live + 31 `[DEPRECATED]`），live 能力的类型/描述与新映射一致（如 `coord:mq:dlq` 不再查不到）。

---

### Task 1: 机检的输入（proto 描述符集）

**Files:**
- 已存在：`coord-proto/src/lib.rs` 的 `pub const FILE_DESCRIPTOR_SET`（`build.rs` 的 `file_descriptor_set_path`）

**Interfaces:**
- Produces: `pub const FILE_DESCRIPTOR_SET: &[u8]`（`prost_types::FileDescriptorSet::decode` 可解）。

- [x] **Step 1: 确认常量已存在**（无需改动；`grep -n FILE_DESCRIPTOR_SET coord-proto/src/lib.rs`）
- [x] **Step 2: `coord-server` 增加 dev-dependency `prost-types`**（仅测试用）
- [x] **Step 3: 提交**（并入 Task 2 的提交）

### Task 2: registry 目录表（89 live + 31 deprecated）

**Files:**
- Modify: `coord-server/src/auth/capability.rs:300-1286`（`builtin_capabilities` + 数量断言）
- Test: 同文件 `mod tests`

**Interfaces:**
- Produces: `fn builtin_capabilities() -> Vec<CapabilityDef>`；
  `pub fn is_live_builtin(capability_id: &str) -> bool`（89 条 live 之一）；
  `pub fn is_deprecated_builtin(capability_id: &str) -> bool`（31 条之一）；
  `fn deprecated_replacement(capability_id: &str) -> Option<&'static str>`（`None` = 未知；`Some("")` = 无替代）。
- Consumes: `coord_core::grpc_auth::rpc_capability`（测试侧）。

- [x] **Step 1: 先写失败测试**

```rust
#[test]
fn registry_live_set_equals_execution_face() {
    let executed = execution_face_capabilities(); // descriptor: 每个方法 rpc_capability(m)
    let live: BTreeSet<String> = builtin_capabilities()
        .into_iter()
        .filter(|c| !c.deprecated)
        .map(|c| c.id)
        .collect();
    assert_eq!(live, executed, "registry live 集合与 rpc_capability 输出集合必须双向恒等");
}

#[test]
fn deprecated_entries_name_a_live_replacement_or_reason() {
    for cap in builtin_capabilities().into_iter().filter(|c| c.deprecated) {
        assert!(!cap.description.is_empty(), "{} 缺少废弃说明", cap.id);
        if let Some(repl) = deprecated_replacement(&cap.id) {
            if !repl.is_empty() {
                assert!(is_live_builtin(repl), "{} 的替代项 {repl} 不是 live 能力", cap.id);
            }
        }
    }
}

#[test]
fn every_proto_method_is_mapped_or_capability_free() {
    // 全部 proto 方法 − rpc_capability 映射 = 约定豁免集合（逐字比对）
}
```

- [x] **Step 2: 运行并确认红** `cargo test -p coord-server --lib auth::capability::tests::registry_`（期望断言失败：live ≠ 执行面）
- [x] **Step 3: 用两张表替换 81 条 `def(...)`**：`BUILTIN_CAPABILITY_META: &[(&str, CapabilityType, &str)]`（89 条）、`DEPRECATED_CAPABILITIES: &[(&str, CapabilityType, &str, &str)]`（31 条，末列替代项，`""` = 无替代）；`builtin_capabilities()` 从 ID 解析 domain/service/action（三段式，测试钉住），deprecated 描述为 `"{原描述}（已废弃：替代能力 {repl}）"` / `"（已废弃：无替代，功能未实现）"`。
- [x] **Step 4: 更新数量断言** 81 → 120（89 live + 31 deprecated；域构成 49+26+10+4 / 23+7+1）
- [x] **Step 5: 全绿** `cargo test -p coord-server --lib auth::capability::`
- [x] **Step 6: 提交** `refactor(auth): derive capability registry from a single catalog table`

### Task 3: 授权校验（grant 拒未知/已废弃，revoke 放行）

**Files:**
- Modify: `coord-server/src/auth/service.rs:977-1046`
- Test: 同文件 `mod tests`（`AuthService::role_grant_capability` 走 trait 方法，auth 关闭态无需 CCT）

**Interfaces:**
- Consumes: `crate::auth::capability::{is_live_builtin, is_deprecated_builtin, deprecated_replacement}`
- Produces: 无新 API（仅 RPC 语义）

- [x] **Step 1: 写失败测试**

```rust
#[test]
fn role_grant_capability_rejects_unknown_id() {
    // INVALID_ARGUMENT，message 含 "coord:no:such" 与 "capability list"
}
#[test]
fn role_grant_capability_rejects_deprecated_id_with_replacement() {
    // data:mq:send → INVALID_ARGUMENT，message 含 coord:mq:publish
}
#[test]
fn role_revoke_capability_allows_unknown_id() {
    // 存量拼写错误必须可撤销：Ok
}
```

- [x] **Step 2: 运行并确认红**
- [x] **Step 3: 实现**：grant 在 `apply_auth_op` 之前校验 `is_live_builtin`，否则 `invalid_argument("unknown capability '{id}' ...")` / `"capability '{id}' is deprecated; use '{repl}'"`；revoke 仅对未知/已废弃落 `tracing::warn!` 后放行。
- [x] **Step 4: 全绿** 且既有 `test_role_grant_capability` 不回归
- [x] **Step 5: 提交** `feat(auth): reject granting unknown or deprecated capabilities`

### Task 4: 门禁脚本 + CI 接线 + 负控制

**Files:**
- Create: `scripts/check-capability-registry.sh`
- Modify: `.github/workflows/ci.yml`（lint job step + gate-self-check inject）

**Interfaces:**
- Consumes: Task 2 的测试名（逐字）

- [x] **Step 1: 脚本**：`cargo test -p coord-server --lib auth::capability::tests::` 的输出里，`registry_live_set_equals_execution_face` / `every_proto_method_is_mapped_or_capability_free` / `deprecated_entries_name_a_live_replacement_or_reason` 三条必须各出现一次 `... ok`，否则退出 1（未跑 = 红）。
- [x] **Step 2: 本地验证** `bash scripts/check-capability-registry.sh` 绿；把 `BUILTIN_CAPABILITY_META` 里 `coord:mq:dlq` 一行注释掉再跑，必须红；恢复。
- [x] **Step 3: CI** lint job 增加 `bash scripts/check-capability-registry.sh`；gate-self-check 增加注入步骤（注释掉 live 行 → 断言脚本红）。
- [x] **Step 4: 提交** `chore(ci): gate capability registry against execution face`

### Task 5: 文档

**Files:**
- Modify: `docs/production/consumer-contract.md` §0.1 + 头注 Last verified
- Modify: `coord-server/src/auth/capability.rs` 模块头注释（81 → 89 live + 31 deprecated）

- [x] **Step 1: §0.1 补四段**：① 执行面 = `rpc_capability`（唯一事实源），发现面 = `CapabilityRegistry/List`，二者 live 集合由 `scripts/check-capability-registry.sh` 机检恒等；② `deprecated=true` 永不被执行、`description` 写明替代项；③ `RoleGrantCapability` 拒绝未知/已废弃并点名，`RoleRevokeCapability` 放行（便于清理存量）；④ 运行时 `Register` 的自定义能力不在执行面（无 `rpc_capability` 映射），因此不参与授权校验。
- [x] **Step 2: `bash scripts/check-doc-refs.sh` 0 悬空**
- [x] **Step 3: 提交** `docs(consumer): document registry ↔ execution-face contract`
