// TDD: Transit 信封加密服务测试
//
// 信封加密
// - DEK 本地生成（AES-256-GCM）
// - KEK 存 Server
// - DEK 用后即焚，仅存储加密后的 DEK
// - 支持密钥轮换（rewrap）
// - 持久化（B-06 / E1）：加密态 DEK 落共享存储 ⇒ 重启后仍可解密、单次使用跨实例成立

use coord_agent::services::transit::{TransitConfig, TransitService};
use coord_agent::{MemoryTransitDekStore, TransitDekStore};
use std::collections::HashMap;
use std::sync::Arc;

/// 验证 TransitConfig 默认值
#[test]
fn test_transit_config_defaults() {
    let config = TransitConfig::default();
    assert_eq!(config.dek_ttl_secs, 3600);
    assert_eq!(config.kek_id, "default-kek");
}

/// 验证加密后能成功解密
#[test]
fn test_transit_encrypt_decrypt_roundtrip() {
    let config = TransitConfig {
        kek_id: "test-kek".into(),
        ..Default::default()
    };
    let svc = TransitService::new(config).expect("创建 TransitService 失败");

    let plaintext = b"hello, this is a secret message";
    let (ciphertext, dek_id) = svc.encrypt(plaintext).expect("加密失败");

    // ciphertext should be different from plaintext
    assert_ne!(ciphertext, plaintext);
    assert!(!dek_id.is_empty());

    let decrypted = svc.decrypt(&ciphertext, &dek_id).expect("解密失败");
    assert_eq!(decrypted, plaintext);
}

/// 验证不同明文产生不同密文（随机 DEK/Nonce）
#[test]
fn test_transit_unique_ciphertexts() {
    let svc = TransitService::new(TransitConfig::default()).expect("创建失败");

    let (ct1, _) = svc.encrypt(b"message one").expect("加密失败");
    let (ct2, _) = svc.encrypt(b"message two").expect("加密失败");

    assert_ne!(ct1, ct2, "不同明文应产生不同密文");
}

/// 验证用后即焚：DEK 不能重复使用解密
#[test]
fn test_transit_dek_single_use() {
    let svc = TransitService::new(TransitConfig::default()).expect("创建失败");

    let (ciphertext, dek_id) = svc.encrypt(b"single-use secret").expect("加密失败");

    // First decryption works
    let first = svc.decrypt(&ciphertext, &dek_id).expect("第一次解密失败");
    assert_eq!(first, b"single-use secret");

    // Second decryption with same DEK should fail (DEK already destroyed)
    let second = svc.decrypt(&ciphertext, &dek_id);
    assert!(second.is_err(), "DEK 用后即焚：第二次解密应失败");
}

/// 验证密钥轮换（rewrap）
#[test]
fn test_transit_rewrap() {
    let svc = TransitService::new(TransitConfig::default()).expect("创建失败");

    let plaintext = b"data that needs key rotation";
    let (ciphertext, old_dek_id) = svc.encrypt(plaintext).expect("加密失败");

    // Rotate the key
    let new_dek_id = svc.rewrap(&old_dek_id).expect("密钥轮换失败");
    assert_ne!(new_dek_id, old_dek_id);

    // Decrypt with new DEK
    let decrypted = svc
        .decrypt(&ciphertext, &new_dek_id)
        .expect("新 DEK 解密失败");
    assert_eq!(decrypted, plaintext);

    // Old DEK should be invalid
    assert!(svc.decrypt(&ciphertext, &old_dek_id).is_err());
}

/// 验证上下文绑定（加密时绑定 context，解密时需匹配）
#[test]
fn test_transit_context_binding() {
    let svc = TransitService::new(TransitConfig::default()).expect("创建失败");

    let mut context = HashMap::new();
    context.insert("tenant".to_string(), "acme".to_string());

    let plaintext = b"tenant-scoped data";
    let (ciphertext, dek_id) = svc
        .encrypt_with_context(plaintext, &context)
        .expect("加密失败");

    // Decrypt with matching context
    let result = svc
        .decrypt_with_context(&ciphertext, &dek_id, &context)
        .expect("解密失败");
    assert_eq!(result, plaintext);

    // Decrypt with wrong context should fail
    let mut wrong_ctx = HashMap::new();
    wrong_ctx.insert("tenant".to_string(), "evilcorp".to_string());
    assert!(svc
        .decrypt_with_context(&ciphertext, &dek_id, &wrong_ctx)
        .is_err());
}

// ═══════════════════════════════════════════════════════════════════
// 持久化路径（B-06 / 计划书工作流 E1，2026-09-19）
//
// 用 GPU 无关的内存 store 模拟 coord-server 共享 KV：同一进程内构造两个
// `TransitService` 实例 = "重启前后"，内存注册表在第二个实例上是空的。
// ═══════════════════════════════════════════════════════════════════

fn svc_with(store: Arc<dyn TransitDekStore>) -> TransitService {
    TransitService::with_store(TransitConfig::default(), store).expect("创建 TransitService 失败")
}

/// 重启后仍能解密重启前产生的密文；且单次使用跨实例成立
#[tokio::test]
async fn test_transit_persisted_survives_restart() {
    let store: Arc<dyn TransitDekStore> = Arc::new(MemoryTransitDekStore::new());
    let before_restart = svc_with(store.clone());

    let plaintext = b"ciphertext produced before restart";
    let (ciphertext, dek_id) = before_restart
        .encrypt_persisted(plaintext)
        .await
        .expect("加密失败");

    // 重启：新实例（内存注册表为空），共享同一持久化后端
    let after_restart = svc_with(store.clone());
    assert_eq!(after_restart.local_dek_count(), 0);

    let decrypted = after_restart
        .decrypt_persisted(&ciphertext, "")
        .await
        .expect("重启后应能解密");
    assert_eq!(decrypted, plaintext);

    // 用后即焚：第三个实例不能再解密同一密文
    let later = svc_with(store);
    assert!(
        later.decrypt_persisted(&ciphertext, &dek_id).await.is_err(),
        "DEK 应已从共享存储删除"
    );
}

/// 轮换后的 DEK 也落盘：重启后用新 DEK 可解、旧 DEK 不可解
#[tokio::test]
async fn test_transit_persisted_rewrap_survives_restart() {
    let store: Arc<dyn TransitDekStore> = Arc::new(MemoryTransitDekStore::new());
    let before_restart = svc_with(store.clone());

    let plaintext = b"rotate across restart";
    let (ciphertext, old_dek_id) = before_restart
        .encrypt_persisted(plaintext)
        .await
        .expect("加密失败");
    let new_dek_id = before_restart
        .rewrap_persisted(&old_dek_id)
        .await
        .expect("轮换失败");
    assert_ne!(new_dek_id, old_dek_id);

    let after_restart = svc_with(store);
    assert!(
        after_restart
            .decrypt_persisted(&ciphertext, &old_dek_id)
            .await
            .is_err(),
        "旧 DEK 已从共享存储删除"
    );
    assert_eq!(
        after_restart
            .decrypt_persisted(&ciphertext, &new_dek_id)
            .await
            .expect("新 DEK 解密失败"),
        plaintext
    );
}
