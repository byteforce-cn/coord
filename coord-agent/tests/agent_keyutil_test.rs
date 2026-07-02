// TDD: Agent 端 KeyUtil 适配器测试 (Phase F — 待实施)
//
// v8.2 §4.12: 密钥管理适配层
// - 适配器模式，支持多种后端
// - 内核 keyring 后端（Linux logon/trusted 类型），推荐生产环境
// - 文件后端（加密的本地文件），适用于开发/测试环境
// - TPM 保留未来扩展点
//
// RED stage: KeyUtil / KeyStore trait / FileKeyStore 尚未定义。

use coord_agent::key_util::{KeyStore, KeyUtil, KeyStoreBackend, KeyUtilConfig, FileKeyStore};
use std::collections::HashSet;

/// 验证 KeyUtilConfig 默认值
#[test]
fn test_key_util_config_defaults() {
    let config = KeyUtilConfig::default();
    assert!(matches!(config.backend, KeyStoreBackend::File));
}

/// 验证 KeyUtilConfig 从 TOML 反序列化
#[test]
fn test_key_util_config_from_toml() {
    let toml_str = r#"
backend = "keyring"
"#;
    let config: KeyUtilConfig = toml::from_str(toml_str).expect("TOML 解析失败");
    assert!(matches!(config.backend, KeyStoreBackend::Keyring));

    let toml_file = r#"
backend = "file"
"#;
    let config: KeyUtilConfig = toml::from_str(toml_file).expect("TOML 解析失败");
    assert!(matches!(config.backend, KeyStoreBackend::File));
}

/// 验证 FileKeyStore 基本 CRUD 操作
#[test]
fn test_file_key_store_crud() {
    let tmpdir = tempfile::tempdir().expect("创建临时目录失败");
    let store = FileKeyStore::new(tmpdir.path().to_path_buf());

    // Store a key
    let key_id = "test-dek-001";
    let key_data = b"0123456789abcdef0123456789abcdef"; // 32 bytes
    store.store(key_id, key_data).expect("存储 key 失败");

    // Load the key
    let loaded = store.load(key_id).expect("加载 key 失败");
    assert_eq!(loaded, key_data);

    // List keys
    let keys: HashSet<String> = store.list_keys().expect("列出 keys 失败").into_iter().collect();
    assert!(keys.contains(key_id));

    // Delete the key
    store.delete(key_id).expect("删除 key 失败");
    assert!(store.load(key_id).is_err());

    // List should be empty
    let keys_after: Vec<String> = store.list_keys().expect("列出 keys 失败");
    assert!(keys_after.is_empty());
}

/// 验证 FileKeyStore 密钥数据在磁盘上加密存储
#[test]
fn test_file_key_store_encryption_at_rest() {
    let tmpdir = tempfile::tempdir().expect("创建临时目录失败");
    let store = FileKeyStore::new(tmpdir.path().to_path_buf());

    let key_id = "test-dek-002";
    let key_data = b"this-is-a-secret-key-123456789!!"; // 32 bytes
    store.store(key_id, key_data).expect("存储 key 失败");

    // 检查磁盘文件内容不包含明文 key
    let file_path = tmpdir.path().join(format!("{key_id}.enc"));
    let disk_content = std::fs::read(&file_path).expect("读取磁盘文件失败");

    // 磁盘存储应为加密格式：nonce(12B) + ciphertext(N B) + tag(16B)
    // 32 bytes 明文 → 32 + 16 = 48 bytes 密文 + 12 bytes nonce = 60 bytes
    assert_eq!(disk_content.len(), 60, "加密后应为 60 bytes (nonce + ciphertext + tag)");
    // 明文 key 不应出现在磁盘文件中
    assert!(
        !disk_content.windows(key_data.len()).any(|w| w == key_data),
        "明文 key 不应出现在磁盘文件中"
    );
}

/// 验证 KeyUtil facade 根据配置选择后端
#[test]
fn test_key_util_facade_file_backend() {
    let tmpdir = tempfile::tempdir().expect("创建临时目录失败");

    let config = KeyUtilConfig {
        backend: KeyStoreBackend::File,
        file_path: Some(tmpdir.path().to_path_buf()),
    };

    let key_util = KeyUtil::new(config).expect("创建 KeyUtil 失败");

    let key_id = "facade-test-key";
    let key_data = b"facade-key-data-000000000000001"; // 32 bytes

    key_util.store(key_id, key_data).expect("存储失败");
    let loaded = key_util.load(key_id).expect("加载失败");
    assert_eq!(loaded, key_data);
}

/// 验证 KeyUtil 支持 key 轮换（新旧 DEK 共存）
#[test]
fn test_key_util_key_rotation() {
    let tmpdir = tempfile::tempdir().expect("创建临时目录失败");
    let store = FileKeyStore::new(tmpdir.path().to_path_buf());

    // 存储 v1 key
    let v1_key = b"version-1-key-data-0123456789ab"; // 32 bytes
    store.store("dek-v1", v1_key).expect("存储 v1 失败");

    // 存储 v2 key（轮换）
    let v2_key = b"version-2-key-data-0123456789ab"; // 32 bytes
    store.store("dek-v2", v2_key).expect("存储 v2 失败");

    // 两个版本应共存
    assert_eq!(store.load("dek-v1").expect("加载 v1 失败"), v1_key);
    assert_eq!(store.load("dek-v2").expect("加载 v2 失败"), v2_key);

    let keys: HashSet<String> = store.list_keys().expect("列出 keys 失败").into_iter().collect();
    assert!(keys.contains("dek-v1"));
    assert!(keys.contains("dek-v2"));
}
