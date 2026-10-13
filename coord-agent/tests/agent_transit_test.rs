// Transit 信封加密服务测试
//
// 信封加密
// - DEK 本地生成（AES-256-GCM）
// - KEK 存 Server
// - DEK 用后即焚，仅存储加密后的 DEK
// - 支持密钥轮换（rewrap）
// - 持久化：加密态 DEK 落共享存储 ⇒ 重启后仍可解密、单次使用跨实例成立

use coord_agent::services::transit::{TransitConfig, TransitKekMaterial, TransitService};
use coord_agent::{
    AgentConfig, AgentServer, MemoryTransitDekStore, ServiceConfig, TransitDekStore,
};
use coord_proto::transit::v1::transit_client::TransitClient as TransitGrpcClient;
use coord_proto::transit::v1::{TransitHmacSignRequest, TransitHmacVerifyRequest};
use std::collections::HashMap;
use std::sync::Arc;

/// 测试用 KEK 材料（32 字节，**非生产**）。
/// 生产由 `COORD_TRANSIT_KEK`（hex64）或 `<data_dir>/transit-kek.bin` 注入。
fn test_kek() -> TransitKekMaterial {
    TransitKekMaterial::from_bytes(&[0x5Au8; 32]).expect("32 字节")
}

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
    let svc = TransitService::new(config, test_kek()).expect("创建 TransitService 失败");

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
    let svc = TransitService::new(TransitConfig::default(), test_kek()).expect("创建失败");

    let (ct1, _) = svc.encrypt(b"message one").expect("加密失败");
    let (ct2, _) = svc.encrypt(b"message two").expect("加密失败");

    assert_ne!(ct1, ct2, "不同明文应产生不同密文");
}

/// 验证用后即焚：DEK 不能重复使用解密
#[test]
fn test_transit_dek_single_use() {
    let svc = TransitService::new(TransitConfig::default(), test_kek()).expect("创建失败");

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
    let svc = TransitService::new(TransitConfig::default(), test_kek()).expect("创建失败");

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
    let svc = TransitService::new(TransitConfig::default(), test_kek()).expect("创建失败");

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
// 持久化路径：DEK 落共享存储 ⇒ 重启后仍可解密。
//
// 用 GPU 无关的内存 store 模拟 coord-server 共享 KV：同一进程内构造两个
// `TransitService` 实例 = "重启前后"，内存注册表在第二个实例上是空的。
// ═══════════════════════════════════════════════════════════════════

fn svc_with(store: Arc<dyn TransitDekStore>) -> TransitService {
    TransitService::with_store(TransitConfig::default(), store, test_kek())
        .expect("创建 TransitService 失败")
}

/// 负控制（集成层）：无注入材料 ⇒ 拒绝，且错误信息给出两条注入路径。
/// 这是「KEK 供给」在进程外可复现的那一半（进程内启动拒绝另见
/// `coord-agent/src/lib.rs` 的 `services.transit = true` 分支）。
#[test]
fn test_transit_without_injected_kek_material_is_fail_closed() {
    let dir = tempfile::tempdir().expect("tempdir");
    let err = TransitKekMaterial::resolve_with(dir.path(), None).expect_err("未注入材料时必须拒绝");
    assert!(
        err.contains("COORD_TRANSIT_KEK") && err.contains("transit-kek.bin"),
        "错误信息必须同时给出环境变量与文件两条路径，实际: {err}"
    );
    assert!(
        err.contains("refusing to start"),
        "错误信息必须显式声明拒绝启动，实际: {err}"
    );
    // 反控制：同一目录下出现合法 32 字节文件 ⇒ 必须接受
    std::fs::write(dir.path().join("transit-kek.bin"), [0x11u8; 32]).expect("write");
    assert!(
        TransitKekMaterial::resolve_with(dir.path(), None).is_ok(),
        "文件存在且长度正确时必须接受"
    );
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

/// 启动使用 dev 默认 KEK 的 agent（HMAC 语义与材料无关；与 ADR-0009 dev 路径同口径），
/// 返回 gRPC 地址与保活 TempDir。
async fn start_transit_agent() -> (String, tempfile::TempDir) {
    let tmpdir = tempfile::tempdir().unwrap();
    let port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let http_port = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
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
        assert!(
            tokio::time::Instant::now() < deadline,
            "agent did not start on {addr}"
        );
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

    // sign 显式指定未注入材料 → 同口径 FAILED_PRECONDITION（不得退化成用新材料签名）
    let err = client
        .hmac_sign(TransitHmacSignRequest {
            data: b"data".to_vec(),
            algorithm: String::new(),
            purpose: "eis:hr:employee:id_number:v1".into(),
            key_id: "ghost".into(),
        })
        .await
        .expect_err("sign with not-injected key_id must fail loud");
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
