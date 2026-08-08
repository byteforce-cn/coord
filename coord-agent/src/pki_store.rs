// coord-agent: PkiStore —— PKI 状态共享存储抽象（Memory / Kv）
//
// 目标（ISSUE-000）：PKI 按 CN 幂等取回（get-or-create）。
// - CA 与已签发证书（含私钥）必须落在**共享存储**（coord-server KV，redb + Barrier 加密），
//   agent 变无状态：重启不丢、多 agent 共享同一 CA 根、跨 agent 可互验。
// - 并发 get-or-create 用 Txn CAS（Version==0）保证只产生一份密钥，无竞态双签发。
//
// Key 空间：
//   /_pki/v1/ca                     → CaRecord（CA 证书 + 私钥）
//   /_pki/v1/certs/{CN}             → CertRecord（当前 active）
//   /_pki/v1/history/{CN}/{serial}  → CertRecord（轮换后的 retired 历史，保留至 not_after）
//
// 参见 docs/client-agent-architecture.v8.2.md §4.12

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use coord_proto::kv::PutRequest;
use coord_proto::txn::compare::{CompareResult, Target, TargetValue};
use coord_proto::txn::request_op::Op;
use coord_proto::txn::{Compare, RequestOp};

use crate::proxy::AgentInner;
use crate::services::workflow_store::prefix_end;

// ──── 数据模型 ────

/// 证书状态
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum CertStatus {
    /// 当前有效（get-or-create 返回的唯一 active 记录）
    #[default]
    Active,
    /// 已轮换退役，保留至 not_after 供验签方按 kid 取用
    Retired,
}

/// 持久化的证书记录（含私钥；经 coord-server Barrier 加密落库）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CertRecord {
    /// 通用名称（CN）
    pub common_name: String,
    /// 证书 PEM
    pub cert_pem: String,
    /// 私钥 PEM
    pub key_pem: String,
    /// 生效时间（UNIX 秒）
    pub not_before: i64,
    /// 失效时间（UNIX 秒）
    pub not_after: i64,
    /// 序列号（十六进制）
    pub serial: String,
    /// 证书状态
    #[serde(default)]
    pub status: CertStatus,
    /// 轮换链：本证书由哪个 serial 轮换而来（None = 首次签发）
    #[serde(default)]
    pub parent_serial: Option<String>,
}

impl CertRecord {
    /// 是否已过期
    pub fn is_expired(&self, now_unix: i64) -> bool {
        now_unix > self.not_after
    }
}

/// CA 持久化记录（证书 + 私钥，经 Barrier 加密落库）
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CaRecord {
    /// CA 证书 PEM
    pub cert_pem: String,
    /// CA 私钥 PEM
    pub key_pem: String,
    /// CA 通用名称
    pub common_name: String,
}

// ──── PkiStoreError ────

/// PkiStore 错误类型
#[derive(Debug)]
pub enum PkiStoreError {
    /// 键已存在（CAS 冲突），get-or-create 的创建方应重读并返回胜者
    AlreadyExists(String),
    /// 未找到（按 serial 查不到任何记录）
    NotFound(String),
    /// 底层 KV/Txn 错误
    Kv(String),
    /// 序列化/反序列化错误
    Serialization(String),
}

impl std::fmt::Display for PkiStoreError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::AlreadyExists(key) => write!(f, "key already exists: {key}"),
            Self::NotFound(s) => write!(f, "not found: {s}"),
            Self::Kv(msg) => write!(f, "kv error: {msg}"),
            Self::Serialization(msg) => write!(f, "serialization error: {msg}"),
        }
    }
}

impl std::error::Error for PkiStoreError {}

// ──── PkiStore trait ────

/// PKI 状态存储抽象
///
/// 生产实现 [`KvPkiStore`]（coord-server 共享 KV + Txn CAS），
/// 开发/单测实现 [`MemoryPkiStore`]（替代历史纯内存行为）。
#[async_trait]
pub trait PkiStore: Send + Sync {
    /// 读取 CA 记录（未初始化返回 Ok(None)）
    async fn get_ca(&self) -> Result<Option<CaRecord>, PkiStoreError>;

    /// 原子创建 CA（Txn CAS Version==0）；已存在返回 AlreadyExists
    async fn create_ca(&self, record: &CaRecord) -> Result<(), PkiStoreError>;

    /// 读取当前 active 证书（无返回 Ok(None)）
    async fn get_cert(&self, cn: &str) -> Result<Option<CertRecord>, PkiStoreError>;

    /// 原子创建 active 证书（Txn CAS Version==0）；已存在返回 AlreadyExists
    async fn create_cert(&self, cn: &str, record: &CertRecord) -> Result<(), PkiStoreError>;

    /// 列出 CN 的 active + 未过期历史证书（验签方按 serial/kid 取用）
    async fn list_certs(&self, cn: &str) -> Result<Vec<CertRecord>, PkiStoreError>;

    /// 按 serial 全量查找（active + 历史），用于 renew 按 serial 还原真实 CN
    async fn get_cert_by_serial(&self, serial: &str) -> Result<Option<CertRecord>, PkiStoreError>;

    /// 原子轮换：旧 active 移入历史（retired），新 active 写入（同 Txn 两写）
    async fn replace_active_cert(
        &self,
        cn: &str,
        active: &CertRecord,
        retired: &CertRecord,
    ) -> Result<(), PkiStoreError>;
}

// ──── Key 构造（纯函数，可单测）────

/// CA 键
pub fn ca_key() -> Vec<u8> {
    b"/_pki/v1/ca".to_vec()
}

/// active 证书键
pub fn active_key(cn: &str) -> Vec<u8> {
    format!("/_pki/v1/certs/{cn}").into_bytes()
}

/// active 前缀（全量扫描用）
pub fn active_prefix() -> Vec<u8> {
    b"/_pki/v1/certs/".to_vec()
}

/// 历史前缀（按 CN 扫描用）
pub fn history_prefix(cn: &str) -> Vec<u8> {
    format!("/_pki/v1/history/{cn}/").into_bytes()
}

/// 历史全量前缀（按 serial 全量查找用）
pub fn history_root_prefix() -> Vec<u8> {
    b"/_pki/v1/history/".to_vec()
}

/// 历史记录键
pub fn history_key(cn: &str, serial: &str) -> Vec<u8> {
    format!("/_pki/v1/history/{cn}/{serial}").into_bytes()
}

// ──── 序列化（纯函数，可单测）────

pub fn serialize_ca(record: &CaRecord) -> Result<Vec<u8>, PkiStoreError> {
    serde_json::to_vec(record).map_err(|e| PkiStoreError::Serialization(e.to_string()))
}

pub fn deserialize_ca(bytes: &[u8]) -> Result<CaRecord, PkiStoreError> {
    serde_json::from_slice(bytes).map_err(|e| PkiStoreError::Serialization(e.to_string()))
}

pub fn serialize_cert(record: &CertRecord) -> Result<Vec<u8>, PkiStoreError> {
    serde_json::to_vec(record).map_err(|e| PkiStoreError::Serialization(e.to_string()))
}

pub fn deserialize_cert(bytes: &[u8]) -> Result<CertRecord, PkiStoreError> {
    serde_json::from_slice(bytes).map_err(|e| PkiStoreError::Serialization(e.to_string()))
}

// ──── Txn CAS 请求构造（纯函数，可单测）────

/// 构造「Version==0 才写入」的 CAS 请求操作（key 不存在才成功）
pub fn create_cas_ops(key: Vec<u8>, value: Vec<u8>) -> (Vec<Compare>, Vec<RequestOp>) {
    let compare = Compare {
        result: CompareResult::Equal as i32,
        target: Target::Version as i32,
        key: key.clone(),
        target_value: Some(TargetValue::Version(0)),
    };
    let put = RequestOp {
        op: Some(Op::RequestPut(PutRequest {
            key,
            value,
            lease_id: 0,
            prev_kv: false,
            request_id: vec![],
        })),
    };
    (vec![compare], vec![put])
}

// ──── MemoryPkiStore（开发 / 单测）────

/// 内存实现：替代历史纯内存行为，具备与 KvPkiStore 一致的 CAS 语义
/// （并发创建仅一份，冲突返回 AlreadyExists）。
#[derive(Default)]
pub struct MemoryPkiStore {
    ca: Mutex<Option<CaRecord>>,
    /// cn -> active
    active: Mutex<HashMap<String, CertRecord>>,
    /// cn -> retired 历史（按 serial）
    history: Mutex<HashMap<String, HashMap<String, CertRecord>>>,
}

impl MemoryPkiStore {
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl PkiStore for MemoryPkiStore {
    async fn get_ca(&self) -> Result<Option<CaRecord>, PkiStoreError> {
        Ok(self.ca.lock().clone())
    }

    async fn create_ca(&self, record: &CaRecord) -> Result<(), PkiStoreError> {
        let mut ca = self.ca.lock();
        if ca.is_some() {
            return Err(PkiStoreError::AlreadyExists("/_pki/v1/ca".into()));
        }
        *ca = Some(record.clone());
        Ok(())
    }

    async fn get_cert(&self, cn: &str) -> Result<Option<CertRecord>, PkiStoreError> {
        Ok(self.active.lock().get(cn).cloned())
    }

    async fn create_cert(&self, cn: &str, record: &CertRecord) -> Result<(), PkiStoreError> {
        let mut active = self.active.lock();
        if active.contains_key(cn) {
            return Err(PkiStoreError::AlreadyExists(
                String::from_utf8_lossy(&active_key(cn)).into_owned(),
            ));
        }
        active.insert(cn.to_string(), record.clone());
        Ok(())
    }

    async fn list_certs(&self, cn: &str) -> Result<Vec<CertRecord>, PkiStoreError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;
        let mut out = Vec::new();
        if let Some(active) = self.active.lock().get(cn) {
            if !active.is_expired(now) {
                out.push(active.clone());
            }
        }
        if let Some(hist) = self.history.lock().get(cn) {
            for record in hist.values() {
                if !record.is_expired(now) {
                    out.push(record.clone());
                }
            }
        }
        Ok(out)
    }

    async fn get_cert_by_serial(&self, serial: &str) -> Result<Option<CertRecord>, PkiStoreError> {
        for record in self.active.lock().values() {
            if record.serial == serial {
                return Ok(Some(record.clone()));
            }
        }
        for hist in self.history.lock().values() {
            if let Some(record) = hist.get(serial) {
                return Ok(Some(record.clone()));
            }
        }
        Ok(None)
    }

    async fn replace_active_cert(
        &self,
        cn: &str,
        active: &CertRecord,
        retired: &CertRecord,
    ) -> Result<(), PkiStoreError> {
        let mut act = self.active.lock();
        let mut hist = self.history.lock();
        // 旧 active 移入历史（按 retired.serial）
        if let Some(old) = act.get(cn) {
            let entry = hist.entry(cn.to_string()).or_default();
            entry.insert(old.serial.clone(), old.clone());
        }
        act.insert(cn.to_string(), active.clone());
        let _ = retired; // retired 已由调用方标记 status=Retired；历史记录以原 active 为准
        Ok(())
    }
}

// ──── KvPkiStore（生产：coord-server 共享 KV）────

/// 生产实现：通过 AgentInner.client 的 KV/Txn 能力访问 coord-server 共享存储。
///
/// - 值经 coord-server redb 持久化 + Barrier 加密落库（agent 侧零加密代码）；
/// - get-or-create 用 Txn CAS（Version==0）保证并发单份。
pub struct KvPkiStore {
    inner: Arc<AgentInner>,
}

impl KvPkiStore {
    pub fn new(inner: Arc<AgentInner>) -> Self {
        Self { inner }
    }
}

#[async_trait]
impl PkiStore for KvPkiStore {
    async fn get_ca(&self) -> Result<Option<CaRecord>, PkiStoreError> {
        let pairs = self
            .inner
            .client
            .kv()
            .range(&ca_key(), &[], 1, 0)
            .await
            .map_err(|e| PkiStoreError::Kv(e.to_string()))?;
        match pairs.first() {
            Some((_k, v)) => deserialize_ca(v).map(Some),
            None => Ok(None),
        }
    }

    async fn create_ca(&self, record: &CaRecord) -> Result<(), PkiStoreError> {
        let value = serialize_ca(record)?;
        let (compares, ops) = create_cas_ops(ca_key(), value);
        let resp = self
            .inner
            .client
            .txn()
            .txn(compares, ops, vec![])
            .await
            .map_err(|e| PkiStoreError::Kv(e.to_string()))?;
        if resp.succeeded {
            Ok(())
        } else {
            Err(PkiStoreError::AlreadyExists("/_pki/v1/ca".into()))
        }
    }

    async fn get_cert(&self, cn: &str) -> Result<Option<CertRecord>, PkiStoreError> {
        let pairs = self
            .inner
            .client
            .kv()
            .range(&active_key(cn), &[], 1, 0)
            .await
            .map_err(|e| PkiStoreError::Kv(e.to_string()))?;
        match pairs.first() {
            Some((_k, v)) => deserialize_cert(v).map(Some),
            None => Ok(None),
        }
    }

    async fn create_cert(&self, cn: &str, record: &CertRecord) -> Result<(), PkiStoreError> {
        let key = active_key(cn);
        let value = serialize_cert(record)?;
        let (compares, ops) = create_cas_ops(key.clone(), value);
        let resp = self
            .inner
            .client
            .txn()
            .txn(compares, ops, vec![])
            .await
            .map_err(|e| PkiStoreError::Kv(e.to_string()))?;
        if resp.succeeded {
            Ok(())
        } else {
            Err(PkiStoreError::AlreadyExists(
                String::from_utf8_lossy(&key).into_owned(),
            ))
        }
    }

    async fn list_certs(&self, cn: &str) -> Result<Vec<CertRecord>, PkiStoreError> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i64;

        let mut out = Vec::new();

        // active
        if let Some((_k, v)) = self
            .inner
            .client
            .kv()
            .range(&active_key(cn), &[], 1, 0)
            .await
            .map_err(|e| PkiStoreError::Kv(e.to_string()))?
            .first()
        {
            if let Ok(record) = deserialize_cert(v) {
                if !record.is_expired(now) {
                    out.push(record);
                }
            }
        }

        // 历史（按 CN 前缀）
        let prefix = history_prefix(cn);
        let range_end = prefix_end(&prefix);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&prefix, &range_end, 0, 0)
            .await
            .map_err(|e| PkiStoreError::Kv(e.to_string()))?;
        for (_k, v) in pairs {
            if let Ok(record) = deserialize_cert(&v) {
                if !record.is_expired(now) {
                    out.push(record);
                }
            }
        }

        Ok(out)
    }

    async fn get_cert_by_serial(&self, serial: &str) -> Result<Option<CertRecord>, PkiStoreError> {
        // active 全量扫描
        let active_pfx = active_prefix();
        let active_end = prefix_end(&active_pfx);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&active_pfx, &active_end, 0, 0)
            .await
            .map_err(|e| PkiStoreError::Kv(e.to_string()))?;
        for (_k, v) in pairs {
            if let Ok(record) = deserialize_cert(&v) {
                if record.serial == serial {
                    return Ok(Some(record));
                }
            }
        }

        // 历史全量扫描
        let hist_pfx = history_root_prefix();
        let hist_end = prefix_end(&hist_pfx);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&hist_pfx, &hist_end, 0, 0)
            .await
            .map_err(|e| PkiStoreError::Kv(e.to_string()))?;
        for (_k, v) in pairs {
            if let Ok(record) = deserialize_cert(&v) {
                if record.serial == serial {
                    return Ok(Some(record));
                }
            }
        }

        Ok(None)
    }

    async fn replace_active_cert(
        &self,
        cn: &str,
        active: &CertRecord,
        retired: &CertRecord,
    ) -> Result<(), PkiStoreError> {
        let new_active_value = serialize_cert(active)?;
        let retired_value = serialize_cert(retired)?;

        let history_put = RequestOp {
            op: Some(Op::RequestPut(PutRequest {
                key: history_key(cn, &retired.serial),
                value: retired_value,
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })),
        };
        let active_put = RequestOp {
            op: Some(Op::RequestPut(PutRequest {
                key: active_key(cn),
                value: new_active_value,
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })),
        };

        let resp = self
            .inner
            .client
            .txn()
            .txn(vec![], vec![history_put, active_put], vec![])
            .await
            .map_err(|e| PkiStoreError::Kv(e.to_string()))?;
        if resp.succeeded {
            Ok(())
        } else {
            Err(PkiStoreError::Kv("txn failed".into()))
        }
    }
}

// ═══════════════════════════════════════════════════════════════════
// 测试 (TDD)
// ═══════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_cert(cn: &str, serial: &str, status: CertStatus) -> CertRecord {
        CertRecord {
            common_name: cn.to_string(),
            cert_pem: format!("-----BEGIN CERTIFICATE-----\n{cn}\n-----END CERTIFICATE-----"),
            key_pem: format!("-----BEGIN PRIVATE KEY-----\n{serial}\n-----END PRIVATE KEY-----"),
            not_before: 1_700_000_000,
            not_after: 9_999_999_999, // 远期不过期
            serial: serial.to_string(),
            status,
            parent_serial: None,
        }
    }

    // ──── Key 构造 ────

    #[test]
    fn test_key_construction() {
        assert_eq!(ca_key(), b"/_pki/v1/ca");
        assert_eq!(active_key("iam-jwt"), b"/_pki/v1/certs/iam-jwt");
        assert_eq!(history_prefix("iam-jwt"), b"/_pki/v1/history/iam-jwt/");
        assert_eq!(
            history_key("iam-jwt", "abc"),
            b"/_pki/v1/history/iam-jwt/abc"
        );
        assert_eq!(active_prefix(), b"/_pki/v1/certs/");
        assert_eq!(history_root_prefix(), b"/_pki/v1/history/");
    }

    // ──── 序列化往返 ────

    #[test]
    fn test_cert_serialization_roundtrip() {
        let record = sample_cert("svc-a", "0x1", CertStatus::Active);
        let bytes = serialize_cert(&record).expect("serialize");
        let back = deserialize_cert(&bytes).expect("deserialize");
        assert_eq!(back, record);
    }

    #[test]
    fn test_cert_deserialize_missing_status_defaults_to_active() {
        // 旧格式无 status 字段 → 反序列化默认 Active（向后兼容）
        let json = r#"{
            "common_name":"svc-a",
            "cert_pem":"CERT",
            "key_pem":"KEY",
            "not_before":1700000000,
            "not_after":9999999999,
            "serial":"0x1"
        }"#;
        let record: CertRecord = serde_json::from_str(json).expect("parse");
        assert_eq!(record.status, CertStatus::Active);
        assert_eq!(record.parent_serial, None);
    }

    #[test]
    fn test_ca_serialization_roundtrip() {
        let record = CaRecord {
            cert_pem: "CA-CERT".into(),
            key_pem: "CA-KEY".into(),
            common_name: "coord-agent-ca".into(),
        };
        let bytes = serialize_ca(&record).expect("serialize");
        let back = deserialize_ca(&bytes).expect("deserialize");
        assert_eq!(back, record);
    }

    #[test]
    fn test_is_expired() {
        let record = sample_cert("svc-a", "0x1", CertStatus::Active);
        assert!(!record.is_expired(1_700_000_000));
        assert!(record.is_expired(10_000_000_000));
    }

    // ──── CAS 请求构造 ────

    #[test]
    fn test_create_cas_ops_uses_version_zero_compare() {
        let (compares, ops) = create_cas_ops(b"/_pki/v1/certs/x".to_vec(), b"v".to_vec());
        assert_eq!(compares.len(), 1);
        assert_eq!(compares[0].target, Target::Version as i32);
        assert_eq!(compares[0].result, CompareResult::Equal as i32);
        assert_eq!(
            compares[0].target_value,
            Some(TargetValue::Version(0)),
            "CAS 必须比较 Version==0（key 不存在才写入）"
        );
        assert_eq!(ops.len(), 1);
        assert!(matches!(ops[0].op, Some(Op::RequestPut(_))));
    }

    // ──── MemoryPkiStore 行为 ────

    #[tokio::test]
    async fn test_memory_create_and_get_cert() {
        let store = MemoryPkiStore::new();
        let record = sample_cert("svc-a", "0x1", CertStatus::Active);
        store.create_cert("svc-a", &record).await.expect("create");
        let got = store.get_cert("svc-a").await.expect("get");
        assert_eq!(got, Some(record));
        assert_eq!(store.get_cert("missing").await.unwrap(), None);
    }

    #[tokio::test]
    async fn test_memory_create_cert_conflict_returns_already_exists() {
        let store = MemoryPkiStore::new();
        let r1 = sample_cert("svc-a", "0x1", CertStatus::Active);
        let r2 = sample_cert("svc-a", "0x2", CertStatus::Active);
        store.create_cert("svc-a", &r1).await.expect("first create");
        let err = store.create_cert("svc-a", &r2).await.unwrap_err();
        assert!(matches!(err, PkiStoreError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn test_memory_ca_get_or_create() {
        let store = MemoryPkiStore::new();
        assert_eq!(store.get_ca().await.unwrap(), None);
        let ca = CaRecord {
            cert_pem: "C".into(),
            key_pem: "K".into(),
            common_name: "ca".into(),
        };
        store.create_ca(&ca).await.expect("create");
        assert_eq!(store.get_ca().await.unwrap(), Some(ca.clone()));
        let err = store.create_ca(&ca).await.unwrap_err();
        assert!(matches!(err, PkiStoreError::AlreadyExists(_)));
    }

    #[tokio::test]
    async fn test_memory_list_certs_includes_active_and_retired() {
        let store = MemoryPkiStore::new();
        let old = sample_cert("svc-a", "0x1", CertStatus::Active);
        let new = sample_cert("svc-a", "0x2", CertStatus::Active);
        store.create_cert("svc-a", &old).await.expect("create");
        let mut retired = old.clone();
        retired.status = CertStatus::Retired;
        store
            .replace_active_cert("svc-a", &new, &retired)
            .await
            .expect("rotate");
        let all = store.list_certs("svc-a").await.expect("list");
        assert_eq!(all.len(), 2, "active + retired 都应返回");
        let serials: Vec<&str> = all.iter().map(|r| r.serial.as_str()).collect();
        assert!(serials.contains(&"0x2"));
        assert!(serials.contains(&"0x1"));
    }

    #[tokio::test]
    async fn test_memory_replace_active_moves_old_to_history() {
        let store = MemoryPkiStore::new();
        let old = sample_cert("svc-a", "0x1", CertStatus::Active);
        let new = sample_cert("svc-a", "0x2", CertStatus::Active);
        let retired = sample_cert("svc-a", "0x1", CertStatus::Retired);
        store.create_cert("svc-a", &old).await.expect("create");
        store
            .replace_active_cert("svc-a", &new, &retired)
            .await
            .expect("rotate");

        assert_eq!(store.get_cert("svc-a").await.unwrap().unwrap().serial, "0x2");
        // 旧证书可通过 serial 找回（renew/验签按 serial 还原 CN）
        let found = store.get_cert_by_serial("0x1").await.unwrap().unwrap();
        assert_eq!(found.common_name, "svc-a");
        assert_eq!(found.serial, "0x1");
    }

    #[tokio::test]
    async fn test_memory_get_cert_by_serial() {
        let store = MemoryPkiStore::new();
        let a = sample_cert("svc-a", "0x1", CertStatus::Active);
        let b = sample_cert("svc-b", "0x2", CertStatus::Active);
        store.create_cert("svc-a", &a).await.unwrap();
        store.create_cert("svc-b", &b).await.unwrap();
        let found = store.get_cert_by_serial("0x2").await.unwrap().unwrap();
        assert_eq!(found.common_name, "svc-b");
        assert_eq!(store.get_cert_by_serial("0x99").await.unwrap(), None);
    }

    /// RED→GREEN: 并发首次签发同 CN → 仅一份密钥（CAS 单写，无竞态双签发）
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_memory_concurrent_create_cert_single_winner() {
        let store = Arc::new(MemoryPkiStore::new());
        let mut handles = Vec::new();
        for i in 0..8u64 {
            let store = store.clone();
            handles.push(tokio::spawn(async move {
                let record = sample_cert(
                    "svc-a",
                    &format!("serial-{i}"),
                    CertStatus::Active,
                );
                store.create_cert("svc-a", &record).await
            }));
        }
        let mut successes = 0;
        let mut conflicts = 0;
        for h in handles {
            match h.await.expect("task join") {
                Ok(()) => successes += 1,
                Err(PkiStoreError::AlreadyExists(_)) => conflicts += 1,
                Err(e) => panic!("unexpected error: {e}"),
            }
        }
        assert_eq!(successes, 1, "仅一个创建方成功");
        assert_eq!(conflicts, 7, "其余全部 CAS 冲突");
    }
}
