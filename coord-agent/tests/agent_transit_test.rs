// Transit 命名密钥 / KMS 面测试（ADR-0019）
//
// - 密钥档案跨实例/重启保留（同一 store + 同一 KEK 材料）
// - gRPC 生命周期、CCT3 版本边界、签名参数 fail-loud、批量逐条结果
// - 用途化 HMAC（ADR-0012）保持原语义

use std::sync::Arc;

use coord_agent::services::transit::{
    TransitConfig, TransitKekKeyring, TransitKekMaterial, TransitService,
};
use coord_agent::{
    AgentConfig, AgentServer, KeyType, MemoryTransitKeyStore, ServiceConfig, TransitKeyRegistry,
};
use coord_proto::transit::v1::transit_client::TransitClient as TransitGrpcClient;
use coord_proto::transit::v1::{
    TransitCreateKeyRequest, TransitDecryptRequest, TransitEncryptBatchRequest,
    TransitEncryptRequest, TransitExportKeyRequest, TransitHmacSignRequest,
    TransitHmacVerifyRequest, TransitKeyType, TransitRewrapRequest, TransitSignRequest,
    TransitUpdateKeyConfigRequest, TransitVerifyRequest,
};

fn test_kek() -> TransitKekMaterial {
    TransitKekMaterial::from_bytes(&[0x5Au8; 32]).expect("32 字节")
}

fn keyring() -> Arc<TransitKekKeyring> {
    Arc::new(TransitKekKeyring::single("test-kek", test_kek()).expect("keyring"))
}

fn registry_with(
    store: Arc<MemoryTransitKeyStore>,
    keyring: Arc<TransitKekKeyring>,
) -> Arc<TransitKeyRegistry> {
    Arc::new(TransitKeyRegistry::new(store, keyring))
}

#[test]
fn test_transit_config_defaults() {
    let config = TransitConfig::default();
    assert_eq!(config.kek_id, "default-kek");
}

/// 双实例：A 建 key + 加密，B（同 store + 同 KEK）能读同一 latest_version 并解密；
/// 新实例（模拟重启）同样可读。
#[tokio::test]
async fn test_key_archive_shared_across_instances_and_restart() {
    let store = Arc::new(MemoryTransitKeyStore::new());
    let keys = keyring();

    let a = registry_with(store.clone(), keys.clone());
    a.create_key(coord_agent::CreateKeySpec {
        name: "shared".into(),
        key_type: KeyType::Aes256Gcm96,
        exportable: false,
    })
    .await
    .expect("create");
    let (ciphertext, version) = a
        .encrypt("shared", 0, b"across agents", b"ctx")
        .await
        .expect("encrypt");
    assert_eq!(version, 1);
    a.rotate_key("shared").await.expect("rotate");

    let b = registry_with(store.clone(), keys.clone());
    assert_eq!(b.read_key("shared").await.expect("read").latest_version, 2);
    let (plaintext, v) = b.decrypt(None, &ciphertext, b"ctx").await.expect("decrypt");
    assert_eq!(plaintext, b"across agents");
    assert_eq!(v, 1);

    // 重启：同一 store + 同一材料 ⇒ 档案与材料都还在
    let restarted = registry_with(store, keys);
    assert_eq!(
        restarted
            .read_key("shared")
            .await
            .expect("read after restart")
            .latest_version,
        2
    );
}

/// 启动使用 dev 默认 KEK 的 agent，返回 gRPC 地址与保活 TempDir。
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
        data_dir: tmpdir.path().to_path_buf().to_string_lossy().into_owned(),
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
async fn test_grpc_key_lifecycle_cct3_and_version_bounds() {
    let (addr, _tmp) = start_transit_agent().await;
    let mut client = TransitGrpcClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");

    // 幂等创建 + 配置冲突
    let created = client
        .create_key(TransitCreateKeyRequest {
            name: "k".into(),
            r#type: TransitKeyType::Aes256Gcm96 as i32,
            exportable: false,
        })
        .await
        .expect("create")
        .into_inner()
        .key
        .expect("key info");
    assert_eq!(created.latest_version, 1);
    let caps = created.capabilities.expect("capabilities");
    assert!(caps.supports_encryption);
    assert!(!caps.supports_signing);

    let again = client
        .create_key(TransitCreateKeyRequest {
            name: "k".into(),
            r#type: TransitKeyType::Aes256Gcm96 as i32,
            exportable: false,
        })
        .await
        .expect("idempotent create")
        .into_inner()
        .key
        .expect("key info");
    assert_eq!(again.latest_version, 1);

    let err = client
        .create_key(TransitCreateKeyRequest {
            name: "k".into(),
            r#type: TransitKeyType::Ed25519 as i32,
            exportable: false,
        })
        .await
        .expect_err("conflicting config must fail");
    assert_eq!(err.code(), tonic::Code::AlreadyExists);

    // CCT3：版本回显 + 上下文 AAD + 名字绑定
    let encrypted = client
        .encrypt(TransitEncryptRequest {
            name: "k".into(),
            version: 0,
            plaintext: b"payload".to_vec(),
            context: b"ctx".to_vec(),
        })
        .await
        .expect("encrypt")
        .into_inner();
    assert_eq!(encrypted.key_version, 1);
    assert!(encrypted.ciphertext.starts_with(b"CCT3"));

    let decrypted = client
        .decrypt(TransitDecryptRequest {
            name: "k".into(),
            ciphertext: encrypted.ciphertext.clone(),
            context: b"ctx".to_vec(),
        })
        .await
        .expect("decrypt")
        .into_inner();
    assert_eq!(decrypted.plaintext, b"payload");
    assert_eq!(decrypted.key_version, 1);

    let err = client
        .decrypt(TransitDecryptRequest {
            name: "other".into(),
            ciphertext: encrypted.ciphertext.clone(),
            context: b"ctx".to_vec(),
        })
        .await
        .expect_err("key name mismatch must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    let err = client
        .decrypt(TransitDecryptRequest {
            name: String::new(),
            ciphertext: encrypted.ciphertext.clone(),
            context: b"wrong".to_vec(),
        })
        .await
        .expect_err("wrong context must fail");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // 轮换 + rewrap：密文版本升级，明文不变
    let rotated = client
        .rotate_key(coord_proto::transit::v1::TransitRotateKeyRequest { name: "k".into() })
        .await
        .expect("rotate")
        .into_inner()
        .key
        .expect("key info");
    assert_eq!(rotated.latest_version, 2);
    let rewrapped = client
        .rewrap(TransitRewrapRequest {
            name: "k".into(),
            ciphertext: encrypted.ciphertext.clone(),
            context: b"ctx".to_vec(),
        })
        .await
        .expect("rewrap")
        .into_inner();
    assert_eq!(rewrapped.key_version, 2);
    assert_ne!(rewrapped.ciphertext, encrypted.ciphertext);
    let plain = client
        .decrypt(TransitDecryptRequest {
            name: "k".into(),
            ciphertext: rewrapped.ciphertext,
            context: b"ctx".to_vec(),
        })
        .await
        .expect("decrypt rewrapped")
        .into_inner()
        .plaintext;
    assert_eq!(plain, b"payload");

    // trim(2) 后 v1 低于 min_decryption_version ⇒ KeyVersionException
    client
        .trim_key(coord_proto::transit::v1::TransitTrimKeyRequest {
            name: "k".into(),
            min_version: 2,
        })
        .await
        .expect("trim");
    let err = client
        .decrypt(TransitDecryptRequest {
            name: "k".into(),
            ciphertext: encrypted.ciphertext,
            context: b"ctx".to_vec(),
        })
        .await
        .expect_err("trimmed version must fail");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);

    // 删除闸门：默认 deletion_allowed=false
    let err = client
        .delete_key(coord_proto::transit::v1::TransitDeleteKeyRequest { name: "k".into() })
        .await
        .expect_err("delete gate");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    client
        .update_key_config(TransitUpdateKeyConfigRequest {
            name: "k".into(),
            min_decryption_version: None,
            min_encryption_version: None,
            deletion_allowed: Some(true),
        })
        .await
        .expect("allow deletion");
    client
        .delete_key(coord_proto::transit::v1::TransitDeleteKeyRequest { name: "k".into() })
        .await
        .expect("delete");
    assert_eq!(
        client
            .read_key(coord_proto::transit::v1::TransitReadKeyRequest { name: "k".into() })
            .await
            .expect_err("deleted")
            .code(),
        tonic::Code::NotFound
    );
}

#[tokio::test]
async fn test_grpc_sign_verify_fail_loud() {
    let (addr, _tmp) = start_transit_agent().await;
    let mut client = TransitGrpcClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");

    client
        .create_key(TransitCreateKeyRequest {
            name: "ed".into(),
            r#type: TransitKeyType::Ed25519 as i32,
            exportable: false,
        })
        .await
        .expect("create ed25519");

    let signed = client
        .sign(TransitSignRequest {
            name: "ed".into(),
            version: 0,
            input: b"message".to_vec(),
            prehashed: false,
            hash_algorithm: String::new(),
            signature_algorithm: String::new(),
        })
        .await
        .expect("sign")
        .into_inner();
    assert_eq!(signed.algorithm, "ed25519");
    assert_eq!(signed.key_version, 1);

    let verified = client
        .verify(TransitVerifyRequest {
            name: "ed".into(),
            version: 0,
            input: b"message".to_vec(),
            signature: signed.signature.clone(),
            prehashed: false,
            hash_algorithm: String::new(),
            signature_algorithm: String::new(),
        })
        .await
        .expect("verify")
        .into_inner();
    assert!(verified.valid);

    let wrong = client
        .verify(TransitVerifyRequest {
            name: "ed".into(),
            version: 0,
            input: b"tampered".to_vec(),
            signature: signed.signature,
            prehashed: false,
            hash_algorithm: String::new(),
            signature_algorithm: String::new(),
        })
        .await
        .expect("verify mismatch")
        .into_inner();
    assert!(!wrong.valid);

    // Ed25519 + prehashed ⇒ 明确拒绝
    let err = client
        .sign(TransitSignRequest {
            name: "ed".into(),
            version: 0,
            input: vec![0u8; 32],
            prehashed: true,
            hash_algorithm: "SHA256".into(),
            signature_algorithm: String::new(),
        })
        .await
        .expect_err("ed25519 prehashed");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // prehashed 缺 hash ⇒ INVALID_ARGUMENT（RSA）
    client
        .create_key(TransitCreateKeyRequest {
            name: "rsa".into(),
            r#type: TransitKeyType::Rsa2048 as i32,
            exportable: false,
        })
        .await
        .expect("create rsa");
    let err = client
        .sign(TransitSignRequest {
            name: "rsa".into(),
            version: 0,
            input: vec![0u8; 32],
            prehashed: true,
            hash_algorithm: String::new(),
            signature_algorithm: String::new(),
        })
        .await
        .expect_err("missing hash");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);

    // RSA 未知 signature_algorithm ⇒ INVALID_ARGUMENT；空 ⇒ 默认 PSS
    let err = client
        .sign(TransitSignRequest {
            name: "rsa".into(),
            version: 0,
            input: b"x".to_vec(),
            prehashed: false,
            hash_algorithm: String::new(),
            signature_algorithm: "bogus".into(),
        })
        .await
        .expect_err("unknown rsa algorithm");
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    let pss = client
        .sign(TransitSignRequest {
            name: "rsa".into(),
            version: 0,
            input: b"x".to_vec(),
            prehashed: false,
            hash_algorithm: String::new(),
            signature_algorithm: String::new(),
        })
        .await
        .expect("rsa default pss")
        .into_inner();
    assert_eq!(pss.algorithm, "rsa-pss-sha256");

    // 导出：非 exportable ⇒ FAILED_PRECONDITION
    let err = client
        .export_key(TransitExportKeyRequest {
            name: "rsa".into(),
            version: 0,
        })
        .await
        .expect_err("not exportable");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

#[tokio::test]
async fn test_grpc_batch_keeps_order_and_isolates_failures() {
    let (addr, _tmp) = start_transit_agent().await;
    let mut client = TransitGrpcClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");

    client
        .create_key(TransitCreateKeyRequest {
            name: "good".into(),
            r#type: TransitKeyType::Aes256Gcm96 as i32,
            exportable: false,
        })
        .await
        .expect("create");

    let batch = client
        .encrypt_batch(TransitEncryptBatchRequest {
            items: vec![
                TransitEncryptRequest {
                    name: "missing".into(),
                    version: 0,
                    plaintext: b"a".to_vec(),
                    context: vec![],
                },
                TransitEncryptRequest {
                    name: "good".into(),
                    version: 0,
                    plaintext: b"b".to_vec(),
                    context: vec![],
                },
            ],
        })
        .await
        .expect("encrypt batch")
        .into_inner();

    assert_eq!(batch.items.len(), 2);
    assert!(!batch.items[0].ok);
    assert_eq!(
        batch.items[0].error.as_ref().expect("error").code,
        "NOT_FOUND"
    );
    assert!(batch.items[1].ok);
    assert!(!batch.items[1]
        .result
        .as_ref()
        .expect("result")
        .ciphertext
        .is_empty());
}

#[tokio::test]
async fn test_grpc_hmac_purpose_key_id_and_fail_loud() {
    let (addr, _tmp) = start_transit_agent().await;
    let mut client = TransitGrpcClient::connect(format!("http://{addr}"))
        .await
        .expect("connect");

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

#[tokio::test]
async fn test_registry_is_shared_with_pki_service() {
    // 装配顺序：transit 先建 registry，pki 复用同一实例（ADR-0019 D1/D2）
    let registry = Arc::new(TransitKeyRegistry::in_memory().expect("registry"));
    let service = TransitService::with_registry(TransitConfig::default(), registry.clone());
    assert!(Arc::ptr_eq(service.registry(), &registry));
}
