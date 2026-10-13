# Transit HMAC 用途化密钥与轮换窗口实施计划（G-TR-3 / ADR-0012）

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `Transit/HmacSign`、`Transit/HmacVerify` 支持用途化密钥与 `key_id`（材料版本），使 KEK 轮换后存量签名在历史材料窗口内可复算、材料下线后 fail-loud。

**Architecture:** 单一注入材料 + 用途域分隔派生（HKDF-SHA256 单次、段长前缀 info）；`purpose` 必填、`key_id` 标识材料（sign 可选、缺省主材料；verify 必填，只按该材料复算）；历史材料同时参与 KEK 解密与 HMAC 签名/验签；错误码 `INVALID_ARGUMENT`（参数）/`FAILED_PRECONDITION`（材料未注入）。

**Tech Stack:** Rust（tonic/prost、hkdf、hmac、sha2）、protobuf 双副本 wire 一致、Java 21 SDK（gRPC + protobuf-maven-plugin）、bash 校验脚本。

**Spec:** `docs/adr/0012-transit-hmac-purpose-bound-keys.md`

## Global Constraints

- 契约版本 **`contracts/v1.5.0`（Minor）**：只新增字段（`purpose` / `key_id`），禁止改动既有字段编号/类型；`buf breaking` 基线 = 上一合同。
- 行为收紧（必须在 CHANGELOG 做消费者告知）：`HmacSign`/`HmacVerify` 的 `purpose` 必填、`HmacVerify` 的 `key_id` 必填；旧 SDK 二进制调用 → `INVALID_ARGUMENT`。
- 错误码口径：参数非法（`purpose` 空 / 含 NUL / >128 字节、验签 `key_id` 空、算法不支持）→ `INVALID_ARGUMENT`；指定材料未注入 → `FAILED_PRECONDITION`；签名不匹配 → `valid=false`（不是错误）。
- 派生公式（**规范形式，不得改动**）：`HKDF-SHA256(材料, salt=None, info)`，其中
  `info = "coord-transit-hmac-v1:" || u32be(len(kek_id)) || kek_id || u32be(len(purpose)) || purpose`。
- `purpose` 校验：非空、不含 NUL、字节长度 `<= 128`（Rust `str::len`，不是字符数）；**不 trim**（空白串是不透明标签）。
- 新签名缺省用主材料；`key_id` 显式指定只允许已注入材料；历史材料仅按 `key_id` 引用参与。
- 版本：只升**契约**版本（CHANGELOG / WHITEPAPER）；Rust workspace 与 Java SDK 的发布版本由 release chore 单独升（本计划不改 `Cargo.toml` / `pom.xml` 版本号）。
- 仓库纪律：注释只写「为什么」；不新增 TODO/FIXME；临时文件一律 `/tmp`；提交信息用中文、无一次性任务编号。

## Review Focus

1. `purpose` 只做字节级校验、不 trim / 不规范化：空白串与多字节 UTF-8 是合法不透明标签（长度按字节计）。
2. `HmacVerify.key_id` 为空 ⇒ `INVALID_ARGUMENT`；不得回落主材料、不得返回 `false`。
3. 指定 `key_id` 未注入 ⇒ `FAILED_PRECONDITION`（消息含 `is not injected`）；不得静默 `valid=false`。
4. 不同 `purpose` 互不可验：purpose A 下的签名在 purpose B 下必须 `false`。
5. 同材料 + 同 id + 同 purpose 跨实例派生一致；签名确定（同输入同输出），无 per-agent 随机量。

---

### Task 1: 设计文档入库（ADR-0012 + 索引 + 本计划）

**Files:**
- Create: `docs/superpowers/plans/2026-10-12-transit-hmac-purpose-bound-keys.md`（本文件）
- Modify: `docs/adr/README.md`（索引表末尾）
- 已存在（一并提交）：`docs/adr/0012-transit-hmac-purpose-bound-keys.md`

**Interfaces:**
- Consumes: —
- Produces: 设计事实源；后续任务以 ADR 的公式、错误码、字段号为唯一依据。

- [ ] **Step 1: 在 `docs/adr/README.md` 索引表末尾追加一行**（链接目标 = 相对 `docs/adr/` 的 `0012-transit-hmac-purpose-bound-keys.md`，与索引内其他行同格式；尖括号仅为展示占位，写入时去掉）

```markdown
| [ADR-0012](<0012-transit-hmac-purpose-bound-keys.md>) | Transit HMAC 用途化密钥与轮换窗口（G-TR-3） | accepted |
```

- [ ] **Step 2: 跑文档引用校验**

Run: `bash scripts/check-doc-refs.sh`
Expected: `check-doc-refs: OK`

- [ ] **Step 3: Commit**

```bash
git add docs/adr/0012-transit-hmac-purpose-bound-keys.md docs/adr/README.md docs/superpowers/plans/2026-10-12-transit-hmac-purpose-bound-keys.md
git commit -m "docs(adr): ADR-0012——Transit HMAC 用途化密钥与轮换窗口（G-TR-3）"
```

---

### Task 2: 契约 proto 双副本新增 HMAC 字段（contracts/v1.5.0）

**Files:**
- Modify: `apis/contracts/proto/coord/transit/v1/transit.proto`
- Modify: `coord-proto/src/proto/transit.proto`（字段逐字一致；仅文件头注释与 `go_package` 不同）

**Interfaces:**
- Consumes: —
- Produces: 生成类型 `TransitHmacSignRequest{purpose,key_id}`、`TransitHmacSignResponse{key_id}`、`TransitHmacVerifyRequest{purpose,key_id}`（Rust/Java 两侧）。

- [ ] **Step 1: 替换契约副本的 HMAC 消息块**（保留 `TransitHmacVerifyResponse` 不动）

```proto
// Transit HMAC 签名与验签（G-TR-3 / ADR-0012：用途化密钥 + 轮换窗口）
//
// purpose 必填：按用途域分隔派生 HMAC 密钥 ⇒ 不同用途互不可验；空/非法 ⇒ INVALID_ARGUMENT。
// key_id 为材料标识，承担 Vault Transit「密钥版本」角色；跨 agent 一致性要求：
// 同一集群所有 agent 必须注入相同材料与相同标识，同一 (purpose, data, key_id, algorithm)
// 在任一 agent 结果一致。

message TransitHmacSignRequest {
  bytes data = 1;
  string algorithm = 2; // 默认 "HMAC-SHA256"
  // 用途标签：非空、不含 NUL、≤128 字节；空/非法 ⇒ INVALID_ARGUMENT。
  string purpose = 3;
  // 材料标识：空 = 主材料（新写入口径）；显式给出 = 指定材料（轮换期散列变体复算）。
  string key_id = 4;
}

message TransitHmacSignResponse {
  bytes signature = 1;
  string algorithm = 2;
  // 服务端分配：实际参与签名的材料标识；调用方必须与签名一起持久化（验签必填）。
  string key_id = 3;
}

message TransitHmacVerifyRequest {
  bytes data = 1;
  bytes signature = 2;
  string algorithm = 3;
  // 用途标签：必填；签名时的用途。
  string purpose = 4;
  // 材料标识：必填；签名时的材料。未注入 ⇒ FAILED_PRECONDITION（fail-loud，不是 valid=false）。
  string key_id = 5;
}
```

- [ ] **Step 2: 把同一段内容逐字同步到 `coord-proto/src/proto/transit.proto`**（保留该文件既有头部注释与 `go_package`）

- [ ] **Step 3: wire 路径级一致性**

Run: `bash apis/contracts/scripts/check-wire-sync.sh`
Expected: 退出码 0（无 FAIL 行）

- [ ] **Step 4: descriptor 级一致性**

Run: `bash apis/contracts/scripts/check-wire-descriptor.sh`
Expected: 退出码 0

- [ ] **Step 5: 生成 Rust 类型编译**

Run: `cargo build -p coord-proto`
Expected: 编译通过（`TransitHmacSignRequest` 含 `purpose`/`key_id` 字段）

- [ ] **Step 6: Commit**

```bash
git add apis/contracts/proto/coord/transit/v1/transit.proto coord-proto/src/proto/transit.proto
git commit -m "feat(contracts): transit HMAC 用途化字段（purpose/key_id）——contracts/v1.5.0 Minor"
```

---

### Task 3: TransitService 用途化派生 + `hmac_sign` / `hmac_verify`（核心）

**Files:**
- Modify: `coord-agent/src/services/transit.rs`
- Test: `coord-agent/src/services/transit.rs`（`mod tests` 内；`cargo test -p coord-agent --lib services::transit`）

**Interfaces:**
- Consumes: Task 2 无直接依赖（服务层不碰 proto 类型）。
- Produces（后续任务依赖的精确签名）：
  - `TransitKekKeyring::material_for(&self, kek_id: &str) -> Option<&TransitKekMaterial>`
  - `fn derive_hmac_key(material: &TransitKekMaterial, kek_id: &str, purpose: &str) -> Result<[u8; DEK_LEN], String>`
  - `fn validate_hmac_purpose(purpose: &str) -> Result<(), String>`
  - `fn hmac_bytes(key: &[u8; DEK_LEN], data: &[u8], algorithm: &str) -> Result<Vec<u8>, String>`
  - `pub fn hmac_sign(&self, data: &[u8], purpose: &str, algorithm: &str, key_id: Option<&str>) -> Result<(Vec<u8>, String), String>`（返回 `(signature, 实际材料 id)`）
  - `pub fn hmac_verify(&self, data: &[u8], signature: &[u8], purpose: &str, algorithm: &str, key_id: &str) -> Result<bool, String>`
  - 错误消息稳定前缀：`invalid purpose:` / `invalid key_id:` / `unsupported HMAC algorithm:`；材料未注入消息含 `is not injected`。

- [ ] **Step 1: 写失败测试（替换现有 8 个 HMAC 测试并新增下列断言）**

现有测试 `test_hmac_key_is_domain_separated_from_material`、`test_hmac_sign_sha256_default`、`test_hmac_sign_sha512`、`test_hmac_verify_roundtrip`、`test_hmac_verify_tampered_data`、`test_hmac_verify_tampered_signature`、`test_hmac_deterministic`、`test_hmac_unsupported_algorithm` 一律改为新调用形状（断言不变）：`svc.hmac_sign(<data>, "p:test", <algorithm，空串表示默认>, None)` 与 `svc.hmac_verify(<data>, &sig, "p:test", <algorithm>, "default-kek")`。新增：

```rust
#[test]
fn test_hmac_purpose_must_not_be_empty() {
    let svc = test_svc(TransitConfig::default());
    let err = svc
        .hmac_sign(b"data", "", "HMAC-SHA256", None)
        .expect_err("empty purpose must fail");
    assert!(err.starts_with("invalid purpose"), "unexpected: {err}");
    let err = svc
        .hmac_verify(b"data", b"sig", "", "HMAC-SHA256", "default-kek")
        .expect_err("empty purpose must fail");
    assert!(err.starts_with("invalid purpose"), "unexpected: {err}");
}

#[test]
fn test_hmac_purpose_validation_is_byte_level_and_never_trims() {
    let svc = test_svc(TransitConfig::default());
    assert!(svc
        .hmac_sign(b"d", "a\0b", "", None)
        .unwrap_err()
        .starts_with("invalid purpose"));
    let max = "x".repeat(128);
    assert!(svc.hmac_sign(b"d", &max, "", None).is_ok(), "128 bytes must pass");
    let too_long = "x".repeat(129);
    assert!(svc
        .hmac_sign(b"d", &too_long, "", None)
        .unwrap_err()
        .starts_with("invalid purpose"));
    // 多字节按字节计：64 个 4 字节码位 = 256 字节 ⇒ 拒绝
    let multibyte = "🦀".repeat(64);
    assert!(svc
        .hmac_sign(b"d", &multibyte, "", None)
        .unwrap_err()
        .starts_with("invalid purpose"));
    // 空白串是合法不透明标签（不得 trim）
    assert!(svc.hmac_sign(b"d", " ", "", None).is_ok());
}

#[test]
fn test_hmac_sign_default_and_explicit_primary_return_key_id() {
    let svc = test_svc(TransitConfig::default());
    let (sig_default, id_default) =
        svc.hmac_sign(b"data", "p:id", "HMAC-SHA256", None).expect("sign");
    let (sig_empty, id_empty) = svc
        .hmac_sign(b"data", "p:id", "HMAC-SHA256", Some(""))
        .expect("sign with empty key_id = primary");
    assert_eq!(id_default, "default-kek");
    assert_eq!(id_empty, "default-kek");
    assert_eq!(sig_default, sig_empty);
}

#[test]
fn test_hmac_purposes_are_mutually_unverifiable() {
    let svc = test_svc(TransitConfig::default());
    let (sig, _) = svc.hmac_sign(b"same data", "p:a", "HMAC-SHA256", None).expect("sign");
    assert!(svc
        .hmac_verify(b"same data", &sig, "p:a", "HMAC-SHA256", "default-kek")
        .expect("same purpose"));
    assert!(!svc
        .hmac_verify(b"same data", &sig, "p:b", "HMAC-SHA256", "default-kek")
        .expect("other purpose"));
}

#[test]
fn test_hmac_verify_requires_non_empty_key_id() {
    let svc = test_svc(TransitConfig::default());
    let err = svc
        .hmac_verify(b"data", b"sig", "p:a", "HMAC-SHA256", "")
        .expect_err("empty key_id must fail");
    assert!(err.starts_with("invalid key_id"), "unexpected: {err}");
}

#[test]
fn test_hmac_unknown_key_id_fails_loud() {
    let svc = test_svc(TransitConfig::default());
    let err = svc
        .hmac_verify(b"data", b"sig", "p:a", "HMAC-SHA256", "ghost")
        .expect_err("not injected must fail loud");
    assert!(err.contains("is not injected"), "unexpected: {err}");
    assert!(
        svc.hmac_sign(b"data", "p:a", "HMAC-SHA256", Some("ghost")).is_err(),
        "sign with unknown material must fail"
    );
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-agent --lib services::transit::tests::test_hmac`
Expected: 编译失败（`hmac_sign` 参数个数不匹配）或断言失败

- [ ] **Step 3: 在 `TransitKekKeyring` 增加材料查找**

```rust
/// 按标识取材料（主或历史；未注入返回 None）
pub fn material_for(&self, kek_id: &str) -> Option<&TransitKekMaterial> {
    if kek_id == self.primary_id {
        Some(&self.primary)
    } else {
        self.historical
            .iter()
            .find(|(id, _)| id == kek_id)
            .map(|(_, m)| m)
    }
}
```

- [ ] **Step 4: 增加派生 / 校验 / HMAC 计算内核**（放在 `derive_key32` 旁）

```rust
/// `purpose` 字节上限（含多字节 UTF-8；不 trim）
pub const HMAC_PURPOSE_MAX_LEN: usize = 128;

/// 用途标签校验（ADR-0012）：非空、不含 NUL、字节长度 ≤128
fn validate_hmac_purpose(purpose: &str) -> Result<(), String> {
    if purpose.is_empty() {
        return Err("invalid purpose: must not be empty".into());
    }
    if purpose.len() > HMAC_PURPOSE_MAX_LEN {
        return Err(format!(
            "invalid purpose: must not exceed {HMAC_PURPOSE_MAX_LEN} bytes"
        ));
    }
    if purpose.contains('\0') {
        return Err("invalid purpose: must not contain NUL".into());
    }
    Ok(())
}

/// 用途化 HMAC 密钥（ADR-0012 规范形式；段长前缀编码消除歧义）
fn derive_hmac_key(
    material: &TransitKekMaterial,
    kek_id: &str,
    purpose: &str,
) -> Result<[u8; DEK_LEN], String> {
    let mut info = Vec::with_capacity(24 + 4 + kek_id.len() + 4 + purpose.len());
    info.extend_from_slice(b"coord-transit-hmac-v1:");
    info.extend_from_slice(&(kek_id.len() as u32).to_be_bytes());
    info.extend_from_slice(kek_id.as_bytes());
    info.extend_from_slice(&(purpose.len() as u32).to_be_bytes());
    info.extend_from_slice(purpose.as_bytes());
    let mut out = [0u8; DEK_LEN];
    Hkdf::<Sha256>::new(None, &*material.0)
        .expand(&info, &mut out)
        .map_err(|e| format!("HKDF expand for transit HMAC key failed: {e}"))?;
    Ok(out)
}

fn hmac_bytes(key: &[u8; DEK_LEN], data: &[u8], algorithm: &str) -> Result<Vec<u8>, String> {
    let algo = if algorithm.is_empty() {
        "HMAC-SHA256"
    } else {
        algorithm
    };
    match algo.to_uppercase().as_str() {
        "HMAC-SHA256" => {
            let mut mac: Hmac<Sha256> =
                hmac::digest::KeyInit::new_from_slice(key).map_err(|e| format!("HMAC-SHA256 init: {e}"))?;
            Mac::update(&mut mac, data);
            Ok(Mac::finalize(mac).into_bytes().to_vec())
        }
        "HMAC-SHA512" => {
            let mut mac: Hmac<Sha512> =
                hmac::digest::KeyInit::new_from_slice(key).map_err(|e| format!("HMAC-SHA512 init: {e}"))?;
            Mac::update(&mut mac, data);
            Ok(Mac::finalize(mac).into_bytes().to_vec())
        }
        other => Err(format!("unsupported HMAC algorithm: {other}")),
    }
}
```

- [ ] **Step 5: `TransitService` 持有 keyring；按 `key_id` 取材料派生**

`TransitService` 增加字段 `keyring: TransitKekKeyring`，删除 `hmac_key` 字段与 `HMAC_KEY_LEN`；`with_store_keyring` 里 `let hmac_key = ...` 删除，构造时把 keyring 存入结构体（先借 `keyring.primary()/historical()` 派生 KEKs，再 move 进结构体）。新增：

```rust
/// 按材料标识 + 用途派生 HMAC 密钥（未注入 ⇒ fail-loud）
fn hmac_key_for(&self, key_id: &str, purpose: &str) -> Result<[u8; DEK_LEN], String> {
    let material = self.keyring.material_for(key_id).ok_or_else(|| {
        let available: Vec<&str> = self.keks.iter().map(|(id, _)| id.as_str()).collect();
        format!(
            "KEK material '{key_id}' is not injected (available: {available:?}); inject it \
             (COORD_TRANSIT_KEK_OLD) to use it for HMAC"
        )
    })?;
    derive_hmac_key(material, key_id, purpose)
}
```

- [ ] **Step 6: 重写 `hmac_sign` / `hmac_verify`**

```rust
pub fn hmac_sign(
    &self,
    data: &[u8],
    purpose: &str,
    algorithm: &str,
    key_id: Option<&str>,
) -> Result<(Vec<u8>, String), String> {
    validate_hmac_purpose(purpose)?;
    let key_id = key_id
        .filter(|s| !s.is_empty())
        .unwrap_or(self.primary_kek_id());
    let key = self.hmac_key_for(key_id, purpose)?;
    Ok((hmac_bytes(&key, data, algorithm)?, key_id.to_string()))
}

pub fn hmac_verify(
    &self,
    data: &[u8],
    signature: &[u8],
    purpose: &str,
    algorithm: &str,
    key_id: &str,
) -> Result<bool, String> {
    validate_hmac_purpose(purpose)?;
    if key_id.is_empty() {
        return Err(
            "invalid key_id: must not be empty (persist the HmacSign key_id with the signature)"
                .into(),
        );
    }
    let key = self.hmac_key_for(key_id, purpose)?;
    let expected = hmac_bytes(&key, data, algorithm)?;
    // 常量时间比较
    Ok(expected.len() == signature.len()
        && expected
            .iter()
            .zip(signature.iter())
            .fold(0u8, |acc, (a, b)| acc | (a ^ b))
            == 0)
}
```

- [ ] **Step 7: 同步 gRPC handler 调用点至新签名（本步只保证编译；错误码映射留给 Task 5）**

`hmac_sign` handler 传 `&req.purpose` 与 `if req.key_id.is_empty() { None } else { Some(req.key_id.as_str()) }`，并用返回的材料 id 填充 `TransitHmacSignResponse.key_id`；`hmac_verify` handler 传 `&req.purpose` / `&req.key_id`。错误分支暂保持 `sanitized_internal`。

- [ ] **Step 8: 跑 transit 单测与工作区编译**

Run: `cargo test -p coord-agent --lib services::transit && cargo check --workspace --all-targets`
Expected: 全绿（若有其他调用点报错，一并修正并记录）

- [ ] **Step 9: Commit**

```bash
git add coord-agent/src/services/transit.rs coord-agent/src/services/grpc_handlers.rs
git commit -m "feat(transit): HMAC 用途化派生与 key_id 选择材料（G-TR-3 / ADR-0012）"
```

---

### Task 4: 轮换窗口、下线 fail-loud 与跨 agent 一致性验收测试

**Files:**
- Test: `coord-agent/src/services/transit.rs`（`mod tests`）

**Interfaces:**
- Consumes: Task 3 的 `hmac_sign` / `hmac_verify` / `with_store_keyring` 与测试助手 `material(u8)` / `old_env_for(&[(&str,u8)])`。

- [ ] **Step 1: 写验收测试**（应直接通过；若失败修 Task 3 实现，不得改弱断言）

```rust
/// 主材料 + 历史材料实例（HMAC 轮换窗口测试用）
fn svc_with_history(primary_id: &str, primary_byte: u8, old: &[(&str, u8)]) -> TransitService {
    let store: Arc<dyn TransitDekStore> = Arc::new(MemoryTransitDekStore::new());
    TransitService::with_store_keyring(
        TransitConfig {
            kek_id: primary_id.into(),
            ..Default::default()
        },
        store,
        TransitKekKeyring::resolve_with_env(
            std::path::Path::new("/nonexistent"),
            primary_id,
            material(primary_byte),
            if old.is_empty() {
                None
            } else {
                Some(old_env_for(old))
            },
        )
        .expect("keyring"),
    )
    .expect("svc")
}

#[test]
fn test_hmac_rotation_window_and_offline_fail_loud() {
    let purpose = "eis:hr:employee:id_number:v1";
    // 旧部署（材料 1 / id "old"）产生存量签名
    let old_svc = svc_with_history("old", 1, &[]);
    let (sig, key_id) = old_svc
        .hmac_sign(b"stored hash", purpose, "HMAC-SHA256", None)
        .expect("sign");
    assert_eq!(key_id, "old");

    // 轮换：主材料 2 / id "new" + 历史材料 1 / id "old" ⇒ 窗口内可验
    let rotated = svc_with_history("new", 2, &[("old", 1)]);
    assert!(rotated
        .hmac_verify(b"stored hash", &sig, purpose, "HMAC-SHA256", "old")
        .expect("window verify"));
    // 新签名用主材料
    let (sig_new, id_new) = rotated
        .hmac_sign(b"stored hash", purpose, "HMAC-SHA256", None)
        .expect("sign new");
    assert_eq!(id_new, "new");
    assert_ne!(sig, sig_new);
    // 旧材料复算散列变体（DRV-005 防重）
    let (variant, variant_id) = rotated
        .hmac_sign(b"stored hash", purpose, "HMAC-SHA256", Some("old"))
        .expect("variant");
    assert_eq!(variant_id, "old");
    assert_eq!(variant, sig, "同 (purpose,data,key_id) 必须可复算");

    // 历史材料下线（未注入）⇒ fail-loud（不是 valid=false）
    let offline = svc_with_history("new", 2, &[]);
    let err = offline
        .hmac_verify(b"stored hash", &sig, purpose, "HMAC-SHA256", "old")
        .expect_err("offline must fail loud");
    assert!(err.contains("is not injected"), "unexpected: {err}");
}

#[test]
fn test_hmac_cross_agent_consistency() {
    let cfg = || TransitConfig {
        kek_id: "k1".into(),
        ..Default::default()
    };
    let a = test_svc(cfg());
    let b = test_svc(cfg());
    let (sig_a, id_a) = a
        .hmac_sign(b"same data", "p:same", "HMAC-SHA256", None)
        .expect("a");
    let (sig_b, id_b) = b
        .hmac_sign(b"same data", "p:same", "HMAC-SHA256", None)
        .expect("b");
    assert_eq!(id_a, id_b);
    assert_eq!(sig_a, sig_b, "同材料+id+purpose ⇒ 同签名");
    assert!(b
        .hmac_verify(b"same data", &sig_a, "p:same", "HMAC-SHA256", &id_a)
        .expect("cross-agent verify"));
}
```

- [ ] **Step 2: 跑验收测试**

Run: `cargo test -p coord-agent --lib services::transit`
Expected: PASS（新增 2 项 + 既有全部）

- [ ] **Step 3: Commit**

```bash
git add coord-agent/src/services/transit.rs
git commit -m "test(transit): HMAC 轮换窗口/下线 fail-loud/跨 agent 一致性验收（G-TR-3）"
```

---

### Task 5: gRPC 错误码映射 + e2e

**Files:**
- Modify: `coord-agent/src/services/grpc_handlers.rs`（`impl Transit for TransitService`）
- Test: `coord-agent/tests/agent_transit_test.rs`

**Interfaces:**
- Consumes: Task 2 生成的 proto 字段；Task 3 的 `hmac_sign` / `hmac_verify` 错误消息前缀。
- Produces: `fn map_transit_hmac_error(e: String) -> Status`（文件内自由函数，靠近 `impl Transit`）。

- [ ] **Step 1: 写失败的 e2e 测试**（加在 `agent_transit_test.rs` 末尾；该文件尚未引入 server 夹具，需要新增 import）

```rust
use coord_agent::{AgentConfig, AgentServer, ServiceConfig};
use coord_proto::transit::v1::transit_client::TransitClient as TransitGrpcClient;
use coord_proto::transit::v1::{TransitHmacSignRequest, TransitHmacVerifyRequest};

/// 启动使用 dev 默认 KEK 的 agent（HMAC 语义与材料无关；与 ADR-0009 dev 路径同口径），
/// 返回 gRPC 地址与保活 TempDir。
async fn start_transit_agent() -> (String, tempfile::TempDir) {
    let tmpdir = tempfile::tempdir().unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let http_port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let config = AgentConfig {
        agent_addr: format!("127.0.0.1:{port}"),
        http_addr: format!("127.0.0.1:{http_port}"),
        data_dir: tmpdir.path().join("data").to_string_lossy().into_owned(),
        static_peers: vec![],
        services: ServiceConfig {
            transit: true,
            ..ServiceConfig::default()
        },
        ..Default::default()
    };
    let server = AgentServer::new(config).with_dev_default_transit_kek(true);
    tokio::spawn(async move {
        let _ = server.serve().await;
    });
    let addr = format!("127.0.0.1:{port}");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        assert!(tokio::time::Instant::now() < deadline, "agent did not start on {addr}");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    (addr, tmpdir)
}

#[tokio::test]
async fn test_grpc_hmac_purpose_key_id_and_fail_loud() {
    let (addr, _tmp) = start_transit_agent().await;
    let mut client = TransitGrpcClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");

    // purpose 缺失 → INVALID_ARGUMENT
    let err = client
        .hmac_sign(TransitHmacSignRequest {
            data: b"data".to_vec(),
            algorithm: String::new(),
            purpose: String::new(),
            key_id: String::new(),
        })
        .await
        .expect_err("empty purpose must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // 正常签名：响应携带 key_id
    let signed = client
        .hmac_sign(TransitHmacSignRequest {
            data: b"data".to_vec(),
            algorithm: String::new(),
            purpose: "eis:hr:employee:id_number:v1".into(),
            key_id: String::new(),
        })
        .await
        .expect("sign")
        .into_inner();
    assert!(!signed.key_id.is_empty());

    // 同用途 → valid=true；不同用途 → valid=false（互不可验）
    let ok = client
        .hmac_verify(TransitHmacVerifyRequest {
            data: b"data".to_vec(),
            signature: signed.signature.clone(),
            algorithm: String::new(),
            purpose: "eis:hr:employee:id_number:v1".into(),
            key_id: signed.key_id.clone(),
        })
        .await
        .expect("verify")
        .into_inner();
    assert!(ok.valid);
    let wrong = client
        .hmac_verify(TransitHmacVerifyRequest {
            data: b"data".to_vec(),
            signature: signed.signature.clone(),
            algorithm: String::new(),
            purpose: "eis:hr:employee:id_number:v2".into(),
            key_id: signed.key_id.clone(),
        })
        .await
        .expect("verify")
        .into_inner();
    assert!(!wrong.valid);

    // 指定材料未注入 → FAILED_PRECONDITION（fail-loud）
    let err = client
        .hmac_verify(TransitHmacVerifyRequest {
            data: b"data".to_vec(),
            signature: signed.signature.clone(),
            algorithm: String::new(),
            purpose: "eis:hr:employee:id_number:v1".into(),
            key_id: "ghost".into(),
        })
        .await
        .expect_err("not injected must fail loud");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    // 验签 key_id 空 → INVALID_ARGUMENT
    let err = client
        .hmac_verify(TransitHmacVerifyRequest {
            data: b"data".to_vec(),
            signature: vec![0u8; 32],
            algorithm: String::new(),
            purpose: "p".into(),
            key_id: String::new(),
        })
        .await
        .expect_err("empty key_id must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // 不支持算法 → INVALID_ARGUMENT
    let err = client
        .hmac_sign(TransitHmacSignRequest {
            data: b"data".to_vec(),
            algorithm: "HMAC-MD5".into(),
            purpose: "p".into(),
            key_id: String::new(),
        })
        .await
        .expect_err("unsupported algorithm must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
}
```

- [ ] **Step 2: 跑测试确认失败**

Run: `cargo test -p coord-agent --test agent_transit_test test_grpc_hmac_purpose_key_id_and_fail_loud`
Expected: 编译通过但断言失败（handler 目前返回 `INTERNAL`，不是 `INVALID_ARGUMENT` / `FAILED_PRECONDITION`）

- [ ] **Step 3: 实现 handler 映射与调用**

在 `impl Transit for TransitService` 前加：

```rust
/// HMAC 错误映射（G-TR-3 / ADR-0012）：
/// 参数非法（purpose / key_id / algorithm）→ INVALID_ARGUMENT；
/// 指定材料未注入 → FAILED_PRECONDITION（fail-loud，不是静默 valid=false）；
/// 其余 → sanitized_internal。
fn map_transit_hmac_error(e: String) -> Status {
    if e.contains("is not injected") {
        Status::failed_precondition(e)
    } else if e.starts_with("invalid purpose")
        || e.starts_with("invalid key_id")
        || e.starts_with("unsupported HMAC algorithm")
    {
        Status::invalid_argument(e)
    } else {
        sanitized_internal(e)
    }
}
```

替换两个 handler：

```rust
async fn hmac_sign(
    &self,
    request: Request<TransitHmacSignRequest>,
) -> Result<Response<TransitHmacSignResponse>, Status> {
    let req = request.into_inner();
    let key_id = if req.key_id.is_empty() {
        None
    } else {
        Some(req.key_id.as_str())
    };
    match self.hmac_sign(&req.data, &req.purpose, &req.algorithm, key_id) {
        Ok((signature, key_id)) => Ok(Response::new(TransitHmacSignResponse {
            signature,
            algorithm: req.algorithm,
            key_id,
        })),
        Err(e) => Err(map_transit_hmac_error(e)),
    }
}

async fn hmac_verify(
    &self,
    request: Request<TransitHmacVerifyRequest>,
) -> Result<Response<TransitHmacVerifyResponse>, Status> {
    let req = request.into_inner();
    match self.hmac_verify(&req.data, &req.signature, &req.purpose, &req.algorithm, &req.key_id) {
        Ok(valid) => Ok(Response::new(TransitHmacVerifyResponse { valid })),
        Err(e) => Err(map_transit_hmac_error(e)),
    }
}
```

- [ ] **Step 4: 跑 e2e + 全量 transit 测试**

Run: `cargo test -p coord-agent --test agent_transit_test && cargo test -p coord-agent --lib services::transit`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add coord-agent/src/services/grpc_handlers.rs coord-agent/tests/agent_transit_test.rs
git commit -m "feat(transit): gRPC HMAC 用途/材料参数与错误码映射 + e2e（G-TR-3）"
```

---

### Task 6: Java SDK 用途化 HMAC API

**Files:**
- Modify: `coord-java-sdk/src/main/java/cn/byteforce/coord/sdk/transit/TransitClient.java`
- Modify: `coord-java-sdk/src/main/java/cn/byteforce/coord/sdk/internal/rpc/TransitClientImpl.java`
- Create: `coord-java-sdk/src/main/java/cn/byteforce/coord/sdk/transit/TransitHmacSignature.java`
- Create: `coord-java-sdk/src/test/java/cn/byteforce/coord/sdk/internal/rpc/TransitHmacContractTest.java`
- Modify: `coord-java-sdk/README.md`（Transit 行）

**Interfaces:**
- Consumes: Task 2 生成的 Java 类型（`TransitHmacSignRequest.getPurpose()/getKeyId()` 等）。
- Produces:
  - `TransitHmacSignature`：`byte[] signature()` / `String keyId()` / `String algorithm()`
  - `TransitClient`：`TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose)`、`(..., String algorithm)`、`(..., String algorithm, String keyId)`；`boolean hmacVerifyWithPurpose(byte[] data, byte[] signature, String purpose, String keyId)`、`(..., String keyId, String algorithm)`。
  - **移除**旧 `hmacSign(byte[] [,String])` 与 `hmacVerify(byte[] , byte[] [,String])`（ADR-0012 §5；0.3.x 未发布窗口）。

- [ ] **Step 1: 写 `TransitHmacSignature`**

```java
package cn.byteforce.coord.sdk.transit;

/**
 * Result of {@link TransitClient#hmacSignWithPurpose} (G-TR-3 / ADR-0012).
 * <p>
 * {@link #keyId()} is the material identifier that produced the signature
 * (primary or historical). Persist it together with the signature: verification
 * requires it, and after material rotation it is what keeps old signatures
 * computable inside the historical-material window.
 */
public final class TransitHmacSignature {

    private final byte[] signature;
    private final String keyId;
    private final String algorithm;

    public TransitHmacSignature(byte[] signature, String keyId, String algorithm) {
        this.signature = signature == null ? new byte[0] : signature;
        this.keyId = keyId == null ? "" : keyId;
        this.algorithm = algorithm == null ? "" : algorithm;
    }

    /** Raw HMAC bytes (hex-encode at the domain layer if a hex field is required). */
    public byte[] signature() {
        return signature;
    }

    /** Material identifier to persist with the signature; pass back on verify. */
    public String keyId() {
        return keyId;
    }

    /** Algorithm echoed by the server (empty means the server default). */
    public String algorithm() {
        return algorithm;
    }

    @Override
    public String toString() {
        return "TransitHmacSignature{signatureLen=" + signature.length
                + ", keyId='" + keyId + "', algorithm='" + algorithm + "'}";
    }
}
```

- [ ] **Step 2: 改 `TransitClient` 接口**（删除旧四个 HMAC 方法，新增：）

```java
    // ──── HMAC 签名与验签（G-TR-3 / ADR-0012）────

    /**
     * Sign data with a purpose-bound HMAC key (default algorithm HMAC-SHA256),
     * using the current primary material.
     * <p>
     * The purpose is a required opaque label; different purposes derive different
     * keys and are mutually unverifiable. Persist {@code result.keyId()} with the
     * signature — verification requires it.
     *
     * @param data    the data to sign
     * @param purpose required purpose label (non-empty, no NUL, &le;128 bytes)
     * @return signature bytes plus the signing material id
     * @throws CoordException {@code INVALID_ARGUMENT} when the purpose is missing/invalid
     */
    TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose);

    /** Same as {@link #hmacSignWithPurpose(byte[], String)} with an explicit algorithm. */
    TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose, String algorithm);

    /**
     * Sign with an explicit material id (rotation window). Used to recompute
     * hash variants with a historical material for cross-rotation duplicate
     * detection (DRV-005); new writes must leave {@code keyId} empty/primary.
     *
     * @throws CoordException {@code FAILED_PRECONDITION} when the material is not injected
     */
    TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose, String algorithm, String keyId);

    /**
     * Verify a signature against the given purpose and material id.
     * <p>
     * Both parameters are required: the material id selects the key version used
     * at signing time. A missing (not injected) material fails loudly with
     * {@code FAILED_PRECONDITION} instead of returning {@code false}.
     *
     * @return true when the signature matches
     */
    boolean hmacVerifyWithPurpose(byte[] data, byte[] signature, String purpose, String keyId);

    /** Same as {@link #hmacVerifyWithPurpose(byte[], byte[], String, String)} with an explicit algorithm. */
    boolean hmacVerifyWithPurpose(byte[] data, byte[] signature, String purpose, String keyId, String algorithm);
```

- [ ] **Step 3: 改 `TransitClientImpl`**

删除旧 4 个 `@Override` 方法，新增实现（统一走 `setPurpose` / `setKeyId`）：

```java
    @Override
    public TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose) {
        return hmacSignWithPurpose(data, purpose, "HMAC-SHA256", "");
    }

    @Override
    public TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose, String algorithm) {
        return hmacSignWithPurpose(data, purpose, algorithm, "");
    }

    @Override
    public TransitHmacSignature hmacSignWithPurpose(byte[] data, String purpose, String algorithm, String keyId) {
        TransitHmacSignRequest request = TransitHmacSignRequest.newBuilder()
                .setData(ByteString.copyFrom(data))
                .setAlgorithm(algorithm != null ? algorithm : "HMAC-SHA256")
                .setPurpose(purpose != null ? purpose : "")
                .setKeyId(keyId != null ? keyId : "")
                .build();
        TransitHmacSignResponse response = callWithRetry(
                (ch, r) -> TransitGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .hmacSign((TransitHmacSignRequest) r),
                request, "transit.hmacSign");
        return new TransitHmacSignature(
                response.getSignature().toByteArray(), response.getKeyId(), response.getAlgorithm());
    }

    @Override
    public boolean hmacVerifyWithPurpose(byte[] data, byte[] signature, String purpose, String keyId) {
        return hmacVerifyWithPurpose(data, signature, purpose, keyId, "HMAC-SHA256");
    }

    @Override
    public boolean hmacVerifyWithPurpose(byte[] data, byte[] signature, String purpose,
                                         String keyId, String algorithm) {
        TransitHmacVerifyRequest request = TransitHmacVerifyRequest.newBuilder()
                .setData(ByteString.copyFrom(data))
                .setSignature(ByteString.copyFrom(signature))
                .setAlgorithm(algorithm != null ? algorithm : "HMAC-SHA256")
                .setPurpose(purpose != null ? purpose : "")
                .setKeyId(keyId != null ? keyId : "")
                .build();
        TransitHmacVerifyResponse response = callWithRetry(
                (ch, r) -> TransitGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .hmacVerify((TransitHmacVerifyRequest) r),
                request, "transit.hmacVerify");
        return response.getValid();
    }
```

（imports：新增 `TransitHmacSignature`；旧方法删除后 `log` 若仅旧方法使用则保留给其他方法。）

- [ ] **Step 4: 写契约测试 `TransitHmacContractTest`**

仿 `NewClientFacesTest` 的 in-process `Server` + `TransitGrpc.TransitImplBase`，断言真实请求字段：

```java
class TransitHmacContractTest {
    private Server server;
    private ManagedChannel channel;
    private final FakeTransit transit = new FakeTransit();
    private TransitClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = ServerBuilder.forPort(0).addService(transit).build().start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
        AgentChannelManager channelManager = mock(AgentChannelManager.class);
        when(channelManager.getChannel()).thenReturn(channel);
        client = new TransitClientImpl(channelManager, new ErrorMapper(), new RetryTemplate(),
                new ObservabilityProvider() {
                }, CoordConfig.builder().agentHost("localhost").build());
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void hmacSignWithPurposeSendsPurposeAndMapsKeyId() {
        transit.signKeyId = "k-primary";
        TransitHmacSignature result =
                client.hmacSignWithPurpose("data".getBytes(StandardCharsets.UTF_8), "eis:hr:id:v1");
        assertThat(transit.lastSign.getPurpose()).isEqualTo("eis:hr:id:v1");
        assertThat(transit.lastSign.getKeyId()).isEmpty(); // 新写入不指定材料
        assertThat(transit.lastSign.getAlgorithm()).isEqualTo("HMAC-SHA256");
        assertThat(result.keyId()).isEqualTo("k-primary");
        assertThat(result.signature()).hasSize(32);
    }

    @Test
    void hmacSignWithPurposeAndKeyIdSendsHistoricalMaterial() {
        client.hmacSignWithPurpose("data".getBytes(StandardCharsets.UTF_8),
                "eis:hr:id:v1", "HMAC-SHA256", "k-old");
        assertThat(transit.lastSign.getKeyId()).isEqualTo("k-old");
    }

    @Test
    void hmacVerifyWithPurposeSendsPurposeAndKeyId() {
        transit.valid = true;
        boolean valid = client.hmacVerifyWithPurpose("data".getBytes(StandardCharsets.UTF_8),
                new byte[32], "eis:hr:id:v1", "k-old");
        assertThat(valid).isTrue();
        assertThat(transit.lastVerify.getPurpose()).isEqualTo("eis:hr:id:v1");
        assertThat(transit.lastVerify.getKeyId()).isEqualTo("k-old");
    }

    private static final class FakeTransit extends TransitGrpc.TransitImplBase {
        volatile TransitHmacSignRequest lastSign;
        volatile TransitHmacVerifyRequest lastVerify;
        volatile String signKeyId = "";
        volatile boolean valid;

        @Override
        public void hmacSign(TransitHmacSignRequest request,
                             StreamObserver<TransitHmacSignResponse> observer) {
            lastSign = request;
            observer.onNext(TransitHmacSignResponse.newBuilder()
                    .setSignature(ByteString.copyFrom(new byte[32]))
                    .setAlgorithm(request.getAlgorithm())
                    .setKeyId(signKeyId)
                    .build());
            observer.onCompleted();
        }

        @Override
        public void hmacVerify(TransitHmacVerifyRequest request,
                               StreamObserver<TransitHmacVerifyResponse> observer) {
            lastVerify = request;
            observer.onNext(TransitHmacVerifyResponse.newBuilder().setValid(valid).build());
            observer.onCompleted();
        }
    }
}
```

- [ ] **Step 5: 跑 Java SDK 测试**

Run: `cd coord-java-sdk && mvn -B -q verify`
Expected: BUILD SUCCESS（CI 使用 JDK21；本地记录实际 JDK 版本）

- [ ] **Step 6: 更新 `coord-java-sdk/README.md` Transit 行**

把 `| Transit | client.transit() | envelope encryption + HMAC; rewrap migrates DEKs during KEK rotation |` 改为同时说明 purpose/key_id（英文行文，与其他行一致）：

```markdown
| Transit | `client.transit()` | envelope encryption + purpose-bound HMAC (`hmacSignWithPurpose` / `hmacVerifyWithPurpose`; persist the returned `keyId`); `rewrap` migrates DEKs during KEK rotation |
```

- [ ] **Step 7: Commit**

```bash
git add coord-java-sdk
git commit -m "feat(java-sdk): Transit HMAC 用途化 API（purpose/key_id）——ADR-0012"
```

---

### Task 7: 契约记录 + 生产文档 + 全门禁

**Files:**
- Modify: `apis/contracts/CHANGELOG.md`（新增 `[contracts/v1.5.0]`）
- Modify: `apis/contracts/STATUS.md`（transit 行）
- Modify: `apis/contracts/WHITEPAPER.md`（版本头 + §12 边界项）
- Modify: `docs/production/consumer-contract.md`（§10）
- Modify: `docs/production/ops/runbook.md`（§4.4）
- Modify: `docs/production/ops/boundaries.md`（B-SE-6）
- Modify: `docs/adr/0012-transit-hmac-purpose-bound-keys.md`（仅在需要记录实施期偏离时）

**Interfaces:**
- Consumes: 前面所有任务的最终行为与测试名。

- [ ] **Step 1: `CHANGELOG.md` 顶部插入条目**

```markdown
## [contracts/v1.5.0] — 2026-10-12（Minor：Transit HMAC 用途化密钥与轮换窗口，G-TR-3 / ADR-0012）

**发布口径**：本条目为 **Minor**（新增字段 + 注释级语义声明 + 行为收紧；
不改动任何既有字段编号与类型；`buf breaking` 基线 = `contracts/v1.4.0`，CI 强制）。

**新增字段（2 个消息，向后兼容）**

| 消息 | 字段 | 语义 |
|:--|:--|:--|
| `coord.transit.v1.TransitHmacSignRequest` | `string purpose = 3` | 必填用途标签（非空、不含 NUL、≤128 字节）；按用途域分隔派生 HMAC 密钥，不同用途互不可验 |
| 同上 | `string key_id = 4` | 可选材料标识：空 = 主材料（新写入）；显式 = 指定材料（轮换期散列变体复算） |
| `coord.transit.v1.TransitHmacSignResponse` | `string key_id = 3` | 服务端分配：实际签名材料标识；调用方必须与签名一起持久化 |
| `coord.transit.v1.TransitHmacVerifyRequest` | `string purpose = 4` | 必填：签名时的用途 |
| 同上 | `string key_id = 5` | 必填：签名时的材料；未注入 ⇒ `FAILED_PRECONDITION`（fail-loud） |

**行为收紧（消费者告知）**

- `HmacSign` / `HmacVerify` 的 `purpose` 与 `HmacVerify` 的 `key_id` 由缺省变为必填；
  未提供（含 coord-java-sdk 0.2.1 二进制）⇒ `INVALID_ARGUMENT`。
- 不支持算法由 `INTERNAL` 改为 `INVALID_ARGUMENT`（与参数非法同口径；无已知消费者依赖）。
- Java SDK：无用途/无 `key_id` 的旧 HMAC 方法被用途化 API 取代（0.3.x 未发布窗口内）。

**语义声明（wire 不变）**

- HMAC 密钥 = `HKDF-SHA256(注入材料, info = "coord-transit-hmac-v1:" || u32be(len(kek_id)) || kek_id || u32be(len(purpose)) || purpose)`；
  历史材料**参与** HMAC 派生（作废「仅派生解密 KEK」注释）。
- 轮换窗口：新签名用主材料；旧签名按其 `key_id` 在历史材料注入期可复算；材料下线后显式
  `key_id` ⇒ `FAILED_PRECONDITION`。HMAC 不参与 `Rewrap`（迁移 = 重散列回写）。
- 跨 agent 一致性：同集群所有 agent 必须注入相同材料与标识；同一
  `(purpose, data, key_id, algorithm)` 在任一 agent 结果一致。

**校验（本地已跑）**

- `apis/contracts/scripts/check-wire-sync.sh` exit 0；`check-wire-descriptor.sh` exit 0；
  `check-sdk-sync.sh` exit 0；`cargo fmt --check` / `clippy` / `cargo test` 全绿；
  `coord-java-sdk` `mvn verify` 全绿（记录实际 JDK 版本）。
```

- [ ] **Step 2: `STATUS.md` transit 行追加**

在该行「实现落点/边界说明」末尾追加：

```text
；**HMAC 用途化密钥 + key_id 轮换窗口（G-TR-3）已落地**：purpose 必填、互不可验；签名携带材料标识，历史材料窗口内可复算、下线后 FAILED_PRECONDITION（ADR-0012）
```

- [ ] **Step 3: `WHITEPAPER.md` 升版并补边界项**

- 版本头改为 `contracts/v1.5.0`、修订日期 `2026-10-12`，`本版变更` 注明：v1.4.0（ADR-0011）见 CHANGELOG；v1.5.0 为 Transit HMAC 用途化字段与行为收紧（ADR-0012）。
- §12 末尾（现第 8 项后）新增：

```markdown
9. **`coord.transit.v1` 的 HMAC 密钥为用途派生**（ADR-0012 / G-TR-3）：`purpose` 必填且
   互不可验；签名携带材料标识 `key_id`（`HmacSign` 缺省 = 主材料，显式 = 历史材料，仅用于
   轮换期变体复算）；`HmacVerify` 必填 `key_id`，材料未注入 ⇒ `FAILED_PRECONDITION`
   （fail-loud）；同集群须注入相同材料与标识。历史材料同时承担 DEK 解密与 HMAC 复算，
   下线前须两类依赖都清零（runbook §4.4）。
```

- [ ] **Step 4: `consumer-contract.md` §10 追加 HMAC 小节**

```markdown
- **HMAC 用途化与轮换窗口（G-TR-3，ADR-0012）**：`HmacSign` / `HmacVerify` 的 `purpose`
  必填（非空、不含 NUL、≤128 字节；空/非法 ⇒ `INVALID_ARGUMENT`），按用途域分隔派生、
  互不可验。签名的 `key_id` 是材料标识（Vault Transit 的 key version 等价物），**必须与
  签名一起持久化**；验签 `key_id` 必填，只按该材料复算：材料已注入 ⇒ 正常校验；未注入 ⇒
  `FAILED_PRECONDITION`（fail-loud，不是 `valid=false`）。新写入不传 `key_id`（主材料）；
  显式 `key_id` 用于轮换期按旧材料复算散列变体（DRV-005 防重）。轮换/下线/回退见 runbook
  §4.4。**跨 agent 一致性**：同集群所有 agent 必须注入相同材料与标识；同一
  `(purpose, data, key_id, algorithm)` 在任一 agent 结果一致。
```

- [ ] **Step 5: `runbook.md` §4.4 增补 HMAC 轮换子节**

在现有 KEK 三步流程与回退路径之后加：

```markdown
**HMAC 轮换（G-TR-3 / ADR-0012）**：

HMAC 与 DEK 共用材料集合，但**不参与 `Rewrap`**；迁移 = 业务按新主材料重散列并回写。

1. **写入侧**：`HmacSign` 必带 `purpose`；把响应的 `key_id` 与签名（散列）一起持久化——
   丢失 `key_id` 的存量在旧材料下线后既不可复算也不会 fail-loud。
2. **并存窗口**：按本节 KEK 步骤 1 注入旧材料；旧散列按 `key_id` 可验；防重按
   `HmacSign(purpose, data, key_id=旧)` 复算变体（DRV-005）。
3. **下线**：确认业务库无引用旧 `key_id` 的存量散列（或已在窗口内重建）后移除旧材料；
   此后按旧 `key_id` 验签/复算报
   `FAILED_PRECONDITION: KEK material '<id>' is not injected …`（fail-loud）。
4. **回退**：重新注入旧材料（同 id / 同材料）即恢复复算；未确认无依赖前不得下线。

**判据（自动化）**：`test_hmac_rotation_window_and_offline_fail_loud`（窗口可验 + 变体复算
+ 下线 fail-loud）、`test_hmac_cross_agent_consistency`、`test_hmac_purposes_are_mutually_unverifiable`、
`test_grpc_hmac_purpose_key_id_and_fail_loud`（错误码）。
```

- [ ] **Step 6: `boundaries.md` B-SE-6 改写**

把「仍不承诺 ②」扩为两类依赖，并补判据/实现锚点：

```markdown
**仍不承诺**：① 材料托管——材料仍是外部注入的 env/文件（非外部 KMS，见 B-SE-2）；
② 移除历史材料后，未 rewrap 的旧材料密文不可解、未重建的旧 `key_id` HMAC 不可复算
（分别报 `KEK material '<id>' is not injected` / `FAILED_PRECONDITION`，fail-loud）——
迁移完成前不得下线旧材料；HMAC 无 `Rewrap`，迁移 = 重散列回写（流程与回退路径见
runbook §4.4）。
```

实现锚点追加 `hmac_sign` / `hmac_verify`（用途派生 + key_id 选材料）；判据追加
`test_hmac_rotation_window_and_offline_fail_loud`、`test_hmac_purposes_are_mutually_unverifiable`、
`test_grpc_hmac_purpose_key_id_and_fail_loud`。

- [ ] **Step 7: 全门禁（逐条记录实际输出）**

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features
cargo test
bash apis/contracts/scripts/check-wire-sync.sh
bash apis/contracts/scripts/check-wire-descriptor.sh
bash apis/contracts/scripts/check-sdk-sync.sh
bash scripts/check-doc-refs.sh
bash scripts/check-panics.sh
bash scripts/check-consumer-doc-labels.sh
(cd coord-java-sdk && mvn -B -q verify)
(cd java-example && mvn -B -q verify -DskipTests)
```

Expected: 全部 exit 0；任何失败按「不把未跑伪装成通过」原则修复后重跑，并把实际结果写回
CHANGELOG「校验」段。

- [ ] **Step 8: Commit**

```bash
git add apis/contracts/CHANGELOG.md apis/contracts/STATUS.md apis/contracts/WHITEPAPER.md \
        docs/production/consumer-contract.md docs/production/ops/runbook.md docs/production/ops/boundaries.md
git commit -m "docs(contracts): contracts/v1.5.0 记录 + STATUS/consumer-contract/runbook/boundaries 同步（G-TR-3）"
```

---

## 验收对照（ADR-0012 / issue #54）

| 验收口径（issue #54） | 本计划的证据 |
|:--|:--|
| a) 两把用途密钥互不可验（负控制） | `test_hmac_purposes_are_mutually_unverifiable` + e2e 不同用途 `valid=false` |
| b) 轮换后旧签名在窗口内可验，下线后 fail-loud | `test_hmac_rotation_window_and_offline_fail_loud` + e2e `FAILED_PRECONDITION` |
| c) 同集群任一 agent 对 `(用途, data, key_id)` 结果一致 | `test_hmac_cross_agent_consistency` + 派生公式确定性（无随机量） |
| d) 文档给出旧材料下线流程与回退路径 | runbook §4.4 HMAC 子节 + consumer-contract §10 + B-SE-6 |
| DRV-005 散列变体防重 | `hmacSignWithPurpose(..., keyId)`（Rust/Java 双侧） |
