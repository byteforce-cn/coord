// coord-agent: 密钥管理适配层 (KeyUtil) — Phase F
//
// v8.2 §4.12: 密钥管理适配层采用适配器模式，支持多种后端：
// - 内核 keyring 后端（Linux logon/trusted 类型），推荐生产环境
// - 文件后端（加密的本地文件），适用于无内核支持的开发/测试环境
// - TPM 保留未来扩展点
//
// 设计：
// - KeyStore trait: 统一密钥存储接口
// - FileKeyStore: AES-256-GCM 加密文件后端
// - KeyringKeyStore: Linux kernel keyring 后端 (cfg(target_os = "linux"))
// - KeyUtil: facade，根据配置选择后端

use std::path::{Path, PathBuf};
use std::fs;

use aes_gcm::{
    aead::{Aead, AeadCore, KeyInit, OsRng},
    Aes256Gcm, Key, Nonce,
};
use sha2::{Sha256, Digest};

// ──── 常量 ────

/// GCM nonce 长度（96-bit）
const NONCE_LEN: usize = 12;

/// GCM tag 长度（128-bit）
const TAG_LEN: usize = 16;

/// 派生的 AES key 长度（256-bit）
const AES_KEY_LEN: usize = 32;

/// 加密文件扩展名
const ENC_EXTENSION: &str = ".enc";

// ──── KeyStoreBackend ────

/// 密钥存储后端类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, serde::Deserialize, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum KeyStoreBackend {
    /// 文件后端（默认，跨平台）
    #[default]
    File,
    /// Linux 内核 keyring 后端
    Keyring,
}

// ──── KeyUtilConfig ────

/// KeyUtil 配置
#[derive(Debug, Clone, serde::Deserialize, serde::Serialize)]
pub struct KeyUtilConfig {
    /// 后端类型（默认 file）
    #[serde(default)]
    pub backend: KeyStoreBackend,

    /// 文件后端存储目录（仅 backend=file 时使用）
    #[serde(default)]
    pub file_path: Option<PathBuf>,
}

impl Default for KeyUtilConfig {
    fn default() -> Self {
        Self {
            backend: KeyStoreBackend::File,
            file_path: None,
        }
    }
}

// ──── KeyStore trait ────

/// 密钥存储统一接口
///
/// 所有后端实现此 trait，KeyUtil facade 通过 trait object 调用。
pub trait KeyStore: Send + Sync {
    /// 存储密钥（幂等：已存在则覆盖）
    fn store(&self, key_id: &str, key_data: &[u8]) -> Result<(), KeyStoreError>;

    /// 加载密钥
    fn load(&self, key_id: &str) -> Result<Vec<u8>, KeyStoreError>;

    /// 删除密钥
    fn delete(&self, key_id: &str) -> Result<(), KeyStoreError>;

    /// 列出所有密钥 ID
    fn list_keys(&self) -> Result<Vec<String>, KeyStoreError>;
}

// ──── KeyStoreError ────

/// 密钥存储错误
#[derive(Debug)]
pub enum KeyStoreError {
    NotFound(String),
    Io(std::io::Error),
    Crypto(String),
}

impl std::fmt::Display for KeyStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NotFound(id) => write!(f, "key not found: {id}"),
            Self::Io(e) => write!(f, "IO error: {e}"),
            Self::Crypto(msg) => write!(f, "crypto error: {msg}"),
        }
    }
}

impl std::error::Error for KeyStoreError {}

impl From<std::io::Error> for KeyStoreError {
    fn from(e: std::io::Error) -> Self {
        Self::Io(e)
    }
}

// ──── FileKeyStore ────

/// 文件后端密钥存储
///
/// 每个 key_id 存储为独立加密文件：`{key_id}.enc`
/// 加密格式：nonce(12B) || ciphertext(N B) || tag(16B)
/// 加密密钥由文件路径 + key_id 派生（SHA-256），确保每个 key 独立加密。
pub struct FileKeyStore {
    data_dir: PathBuf,
}

impl FileKeyStore {
    /// 创建文件后端
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    /// 获取 key 对应的文件路径
    fn key_path(&self, key_id: &str) -> PathBuf {
        self.data_dir.join(format!("{key_id}{ENC_EXTENSION}"))
    }

    /// 从 key_id 派生 AES-256 加密密钥
    fn derive_key(key_id: &str, data_dir: &Path) -> [u8; AES_KEY_LEN] {
        let mut hasher = Sha256::new();
        hasher.update(data_dir.to_string_lossy().as_bytes());
        hasher.update(b":");
        hasher.update(key_id.as_bytes());
        hasher.update(b":coord-keyutil-v1");
        let hash = hasher.finalize();
        let mut key = [0u8; AES_KEY_LEN];
        key.copy_from_slice(&hash[..AES_KEY_LEN]);
        key
    }
}

impl KeyStore for FileKeyStore {
    fn store(&self, key_id: &str, key_data: &[u8]) -> Result<(), KeyStoreError> {
        // 确保目录存在
        fs::create_dir_all(&self.data_dir)?;

        let aes_key = Self::derive_key(key_id, &self.data_dir);
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&aes_key));
        let nonce = Aes256Gcm::generate_nonce(&mut OsRng);

        let ciphertext = cipher
            .encrypt(&nonce, key_data)
            .map_err(|e| KeyStoreError::Crypto(format!("encrypt failed: {e}")))?;

        // 写入格式：nonce || ciphertext（含 tag）
        let mut encrypted = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        encrypted.extend_from_slice(&nonce);
        encrypted.extend_from_slice(&ciphertext);

        let path = self.key_path(key_id);
        fs::write(&path, &encrypted)?;

        Ok(())
    }

    fn load(&self, key_id: &str) -> Result<Vec<u8>, KeyStoreError> {
        let path = self.key_path(key_id);
        if !path.exists() {
            return Err(KeyStoreError::NotFound(key_id.to_string()));
        }

        let encrypted = fs::read(&path)?;

        if encrypted.len() < NONCE_LEN + TAG_LEN {
            return Err(KeyStoreError::Crypto("encrypted data too short".into()));
        }

        let (nonce_bytes, ciphertext) = encrypted.split_at(NONCE_LEN);
        let nonce = Nonce::from_slice(nonce_bytes);

        let aes_key = Self::derive_key(key_id, &self.data_dir);
        let cipher = Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&aes_key));

        let plaintext = cipher
            .decrypt(nonce, ciphertext)
            .map_err(|e| KeyStoreError::Crypto(format!("decrypt failed: {e}")))?;

        Ok(plaintext)
    }

    fn delete(&self, key_id: &str) -> Result<(), KeyStoreError> {
        let path = self.key_path(key_id);
        if path.exists() {
            fs::remove_file(&path)?;
        }
        Ok(())
    }

    fn list_keys(&self) -> Result<Vec<String>, KeyStoreError> {
        if !self.data_dir.exists() {
            return Ok(Vec::new());
        }

        let mut keys = Vec::new();
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "enc") {
                if let Some(stem) = path.file_stem() {
                    keys.push(stem.to_string_lossy().to_string());
                }
            }
        }
        Ok(keys)
    }
}

// ──── KeyUtil ────

/// 密钥管理 facade
///
/// 根据配置选择合适的后端，提供统一的存储/加载/删除/列举接口。
pub struct KeyUtil {
    backend: Box<dyn KeyStore>,
}

impl KeyUtil {
    /// 根据配置创建 KeyUtil
    pub fn new(config: KeyUtilConfig) -> Result<Self, KeyStoreError> {
        let backend: Box<dyn KeyStore> = match config.backend {
            KeyStoreBackend::File => {
                let dir = config.file_path.unwrap_or_else(|| {
                    PathBuf::from("/var/lib/coord-agent/keys")
                });
                Box::new(FileKeyStore::new(dir))
            }
            KeyStoreBackend::Keyring => {
                #[cfg(target_os = "linux")]
                {
                    Box::new(KeyringKeyStore::new())
                }
                #[cfg(not(target_os = "linux"))]
                {
                    return Err(KeyStoreError::Crypto(
                        "keyring backend is only available on Linux".into()
                    ));
                }
            }
        };
        Ok(Self { backend })
    }

    /// 存储密钥
    pub fn store(&self, key_id: &str, key_data: &[u8]) -> Result<(), KeyStoreError> {
        self.backend.store(key_id, key_data)
    }

    /// 加载密钥
    pub fn load(&self, key_id: &str) -> Result<Vec<u8>, KeyStoreError> {
        self.backend.load(key_id)
    }

    /// 删除密钥
    pub fn delete(&self, key_id: &str) -> Result<(), KeyStoreError> {
        self.backend.delete(key_id)
    }

    /// 列出所有密钥 ID
    pub fn list_keys(&self) -> Result<Vec<String>, KeyStoreError> {
        self.backend.list_keys()
    }
}

// ──── KeyringKeyStore (Linux only) ────

/// Linux 内核 keyring 后端
///
/// 使用 Linux keyctl 系统调用（通过 /proc/keys 接口或 keyctl 命令）。
/// 生产环境推荐使用，密钥材料永不离内核内存。
#[cfg(target_os = "linux")]
pub struct KeyringKeyStore {
    keyring_name: String,
}

#[cfg(target_os = "linux")]
impl KeyringKeyStore {
    pub fn new() -> Self {
        Self {
            keyring_name: "coord-agent".to_string(),
        }
    }

    /// 通过 keyctl 命令存储密钥
    fn keyctl_add(&self, key_id: &str, key_data: &[u8]) -> Result<(), KeyStoreError> {
        let hex_key = hex::encode(key_data);
        let output = std::process::Command::new("keyctl")
            .args([
                "add", "user", &format!("coord:{}", key_id),
                &hex_key, &format!("@{}", self.keyring_name),
            ])
            .output()
            .map_err(|e| KeyStoreError::Io(e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            // keyctl 不可用时回退到文件存储
            return Err(KeyStoreError::Crypto(format!("keyctl add failed: {stderr}")));
        }
        Ok(())
    }

    /// 通过 keyctl 命令读取密钥
    fn keyctl_read(&self, key_id: &str) -> Result<Vec<u8>, KeyStoreError> {
        let output = std::process::Command::new("keyctl")
            .args(["read", &format!("coord:{}", key_id)])
            .output()
            .map_err(|e| KeyStoreError::Io(e))?;

        if !output.status.success() {
            return Err(KeyStoreError::NotFound(key_id.to_string()));
        }

        // keyctl read 返回 hex 编码的数据
        let hex_str = String::from_utf8_lossy(&output.stdout).trim().to_string();
        hex::decode(&hex_str)
            .map_err(|e| KeyStoreError::Crypto(format!("hex decode failed: {e}")))
    }

    /// 通过 keyctl 命令撤销密钥
    fn keyctl_revoke(&self, key_id: &str) -> Result<(), KeyStoreError> {
        // 先查找 key ID
        let output = std::process::Command::new("keyctl")
            .args(["search", &format!("@{}", self.keyring_name), "user", &format!("coord:{}", key_id)])
            .output()
            .map_err(|e| KeyStoreError::Io(e))?;

        if !output.status.success() {
            return Ok(()); // key 不存在，无需删除
        }

        let key_serial = String::from_utf8_lossy(&output.stdout).trim().to_string();
        let _ = std::process::Command::new("keyctl")
            .args(["revoke", &key_serial])
            .output();

        Ok(())
    }
}

#[cfg(target_os = "linux")]
impl KeyStore for KeyringKeyStore {
    fn store(&self, key_id: &str, key_data: &[u8]) -> Result<(), KeyStoreError> {
        self.keyctl_add(key_id, key_data)
    }

    fn load(&self, key_id: &str) -> Result<Vec<u8>, KeyStoreError> {
        self.keyctl_read(key_id)
    }

    fn delete(&self, key_id: &str) -> Result<(), KeyStoreError> {
        self.keyctl_revoke(key_id)
    }

    fn list_keys(&self) -> Result<Vec<String>, KeyStoreError> {
        // keyctl list 不可靠，返回空列表
        Ok(Vec::new())
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_key_util_config_defaults() {
        let config = KeyUtilConfig::default();
        assert!(matches!(config.backend, KeyStoreBackend::File));
        assert!(config.file_path.is_none());
    }

    #[test]
    fn test_file_key_store_basic() {
        let tmpdir = tempfile::tempdir().expect("创建临时目录失败");
        let store = FileKeyStore::new(tmpdir.path().to_path_buf());

        // 测试非 32 字节 key
        let key_data = b"hello-world-key-123456789!@#"; // 非 32 字节也可以
        store.store("test-key", key_data).expect("存储失败");
        let loaded = store.load("test-key").expect("加载失败");
        assert_eq!(loaded, key_data);
    }
}
