// Snapshot 导出/导入
//
// 实现 的状态机快照能力：
// - export_snapshot_data: 从 MvccStorage 导出全量数据
// - import_snapshot_data: 将快照数据恢复到 MvccStorage
//
// 快照格式：读路径三路（无前缀 bincode / 信封 V1 / 信封 V2-postcard，均精确
// 消费）；写路径当前为无前缀 bincode（P2b 统一切换 V2）。包含所有 KV 数据、
// 元数据和 Raft 检查点。

use std::path::PathBuf;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use coord_core::error::{Error, Result};
use coord_core::storage::StorageBackend;

use super::envelope;
use super::mvcc::{
    encode_kv_key, encode_kv_meta_key, AppliedLogId, KvMetadata, MvccStorage, CHANGELOG_PREFIX,
    META_COMPACT_REVISION, META_LAST_APPLIED, TABLE_CHANGELOG, TABLE_KV, TABLE_KV_META, TABLE_META,
};

// ──── Snapshot 数据结构 ────

/// 全量快照数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotData {
    /// 快照版本（用于向前兼容）
    pub version: u32,
    /// Raft 最后包含的 Log Index
    pub last_included_index: u64,
    /// Raft 最后包含的 Term
    pub last_included_term: u64,
    /// 全局 Revision 计数器（下一个可用 Revision）
    pub next_revision: u64,
    /// 已 Apply 的最大 Raft Index
    pub applied_index: u64,
    /// R-RFT-06：已 Apply LogId 的 term（与 index 同事务持久化，导入后完整恢复）
    pub applied_term: u64,
    /// R-RFT-06：已 Apply LogId 的 node_id
    pub applied_node_id: u64,
    /// 所有 KV 数据对（加密后的密文）
    pub kv_pairs: Vec<SnapshotKvPair>,
    /// 所有 KV 元数据
    pub kv_metadata: Vec<SnapshotKvMeta>,
    /// R-RFT-06：auth 域原始条目（`/_sys/auth/*` → bytes：用户/角色/会话/吊销登记）
    pub auth_entries: Vec<SnapshotRawEntry>,
    /// R-RFT-06：lease 域原始条目（`/_lease/*` → bytes）
    pub lease_entries: Vec<SnapshotRawEntry>,
    /// R-RFT-06：changelog 压缩水位（`META_COMPACT_REVISION`，0 = 未压缩）
    pub compacted_revision: u64,
    /// region 0 PD 全局队列域原始条目（`/_pd/*` → bytes）。
    ///
    /// region 0 状态机含 `/_pd/ops/{op_id}` operator 治理记录（全局去重
    /// 队列）。快照必须携带该域——导入时 `TABLE_KV` 全表清空重灌，缺此域会让
    /// 快照追平/恢复把队列清空（op 丢失、调度停滞）。
    pub pd_entries: Vec<SnapshotRawEntry>,
    /// region 0 其余 system 域原始条目（`/_sys/*` 中不属于 `/_sys/auth/*`
    /// 的行——迁移标记 `/_sys/migration/legacy-v1` 等）。恢复后启动闸/迁移
    /// 状态跨快照不丢。
    pub sys_entries: Vec<SnapshotRawEntry>,
    /// changelog 窗口原始条目（key = `/_changelog/{rev_be}`，value = `ChangeEvent`）。
    ///
    /// 快照必须携带**源节点当前持有的全部 changelog 行**（`apply_compact` 的
    /// 保留窗口 `>= compacted_revision`）：装快照会把 KV 状态整体替换到 S，而
    /// 历史读（`get_at_revision` / `range_at_revision`）与 watch 重放
    /// （`read_changelog_from`）都从 `TABLE_CHANGELOG` 重建。不携带（且导入不
    /// 清空）时，本地旧条目与新状态之间会形成「洞」(旧水位, S]，洞内 target 的
    /// 历史读会**静默返回前值**（与实时读不一致）。导入时**整体替换**（先清空再
    /// 灌入）。
    pub changelog_entries: Vec<SnapshotRawEntry>,
}

/// R-RFT-06：快照中的原始内部条目（非用户 KV 域）
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotRawEntry {
    /// 内部存储 key（`/_sys/auth/`、`/_lease/`、`/_pd/`、`/_sys/` 等内部前缀）
    pub internal_key: Vec<u8>,
    /// 原始 value 字节（密文/序列化字节，快照不接触明文）
    pub value: Vec<u8>,
}

/// 快照中的单条 KV 记录
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotKvPair {
    /// 用户 Key（不含 /kv/ 前缀）
    pub key: Vec<u8>,
    /// Value（加密后的密文，空表示 tombstone）
    pub value: Vec<u8>,
}

/// 快照中的单条 KV 元数据
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotKvMeta {
    /// 用户 Key（不含前缀）
    pub key: Vec<u8>,
    pub version: i64,
    pub create_revision: i64,
    pub mod_revision: i64,
    pub lease_id: i64,
    pub deleted: bool,
}

impl SnapshotData {
    /// 当前快照格式版本（版本号 +1；0.1.x 数据不承诺兼容）
    /// v3（R-RFT-06）：新增 auth/lease 域 + compacted 水位，导入写完整 LogId。
    /// v4：新增 region 0 `/_pd/*`（PD 队列）与 `/_sys/*`
    ///   非 auth 域（迁移标记等）原始条目——region 0 状态机的内部记录不再丢失。
    /// v5：新增 changelog 窗口（`changelog_entries`）——装快照后历史读/
    ///   watch 重放不再出现「洞」（静默返回前值）。v4 及更早格式经
    ///   `from_bytes_migrating` 迁移：changelog 置空并把 compacted 水位抬到
    ///   applied（洞内 target 的历史读显式报 `RevisionCompacted`，不静默错答）。
    const CURRENT_VERSION: u32 = 5;

    /// 创建空快照
    pub fn new(last_included_index: u64, last_included_term: u64) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            last_included_index,
            last_included_term,
            next_revision: 1,
            applied_index: 0,
            applied_term: 0,
            applied_node_id: 0,
            kv_pairs: Vec::new(),
            kv_metadata: Vec::new(),
            auth_entries: Vec::new(),
            lease_entries: Vec::new(),
            compacted_revision: 0,
            pd_entries: Vec::new(),
            sys_entries: Vec::new(),
            changelog_entries: Vec::new(),
        }
    }

    /// 序列化为字节（用于网络传输和磁盘存储）。
    ///
    /// 写路径当前为无前缀 bincode（P2b 统一切换 V2-postcard）。
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::Internal(format!("snapshot serialize: {e}")))
    }

    /// 从字节反序列化（当前版本，不迁移）。
    ///
    /// 三路读（P2a）：信封 V2 ⇒ postcard（仅承载 v5，`version != 5` 显式错）；
    /// 信封 V1 / 无前缀 ⇒ bincode。三条均精确消费（拒绝尾随字节）。
    pub fn from_bytes(data: &[u8]) -> Result<Self> {
        match envelope::classify(data) {
            Ok(envelope::Envelope::V2(payload)) => Self::decode_v2_v5(payload),
            Ok(envelope::Envelope::V1(payload)) | Ok(envelope::Envelope::Legacy(payload)) => {
                envelope::decode_bincode_exact::<Self>(payload)
                    .map_err(|e| Error::Internal(format!("snapshot deserialize: {e}")))
            }
            Err(e) => Err(Error::Internal(format!("snapshot envelope: {e}"))),
        }
    }

    /// V2（postcard）腿：只承载当前版本（v5）。
    ///
    /// postcard payload 只会由本轮写路径产生，没有迁移阶梯；不是 v5 的行
    /// 不可能来自正常写入 ⇒ 显式报错（不得按旧结构试解）。
    fn decode_v2_v5(payload: &[u8]) -> Result<Self> {
        let snapshot: Self = envelope::decode_postcard_exact(payload)
            .map_err(|e| Error::Internal(format!("snapshot V2 deserialize: {e}")))?;
        if snapshot.version != Self::CURRENT_VERSION {
            return Err(Error::Internal(format!(
                "unsupported snapshot version: {} (expected {})",
                snapshot.version,
                Self::CURRENT_VERSION
            )));
        }
        Ok(snapshot)
    }

    /// R-TST-21：反序列化 + 旧格式迁移（数据格式升级兼容）。
    ///
    /// 按格式分区（[`envelope::classify`]，读路径按标记/魔数分派，不做“先试
    /// 一种再回落另一种”的试错解码）：
    /// - 信封 V2 ⇒ postcard，仅承载 v5（`version != 5` 显式错，尾随拒绝）；
    /// - 信封 V1 / 无前缀（历史行）⇒ bincode 迁移阶梯。
    ///
    /// bincode 阶梯：直接解析成功且版本匹配 → 原样返回；否则逐级回退
    /// **v4 → v3 → v2** 迁移
    /// （顺序敏感：bincode 为位置编码，旧格式是更新格式的**前缀**布局，必须从
    /// 最新的旧版本开始试）：
    /// - v4：无 changelog 窗口 → 迁移为空窗口，并把 compacted
    ///   水位抬到 `applied_index`（导入者无法重建 `(水位, applied]` 的历史 ——
    ///   抬水位让洞内 target 的历史读**显式报 `RevisionCompacted`**，而不是
    ///   静默返回前值）；
    /// - v3 = 之前：无 `/_pd/*` 与 `/_sys/*`（非 auth）域 → 迁移后
    ///   补空域（region 0 内部记录本就丢失，运行时会重新经 raft 收敛）；
    /// - v2 = R-RFT-06 之前：无 auth/lease 域、无 compacted 水位、applied
    ///   term/node_id 不持久化 → 迁移结果域置空、水位 0、applied 回退 0。
    pub fn from_bytes_migrating(data: &[u8]) -> Result<Self> {
        match envelope::classify(data) {
            Err(e) => Err(Error::Internal(format!("snapshot envelope: {e}"))),
            Ok(envelope::Envelope::V2(payload)) => Self::decode_v2_v5(payload),
            Ok(envelope::Envelope::V1(payload)) | Ok(envelope::Envelope::Legacy(payload)) => {
                Self::migrate_from_bincode(payload)
            }
        }
    }

    /// bincode 迁移阶梯（v5 → v4 → v3 → v2；各腿均精确消费）。
    fn migrate_from_bincode(data: &[u8]) -> Result<Self> {
        match envelope::decode_bincode_exact::<Self>(data) {
            Ok(snapshot) if snapshot.version == Self::CURRENT_VERSION => Ok(snapshot),
            Ok(snapshot) => Err(Error::Internal(format!(
                "unsupported snapshot version: {} (expected {})",
                snapshot.version,
                Self::CURRENT_VERSION
            ))),
            Err(_) => {
                // 尝试 v4 迁移
                match envelope::decode_bincode_exact::<SnapshotDataV4>(data) {
                    Ok(v4) if v4.version == 4 => {
                        tracing::warn!(
                            "snapshot v4 detected; migrating to v{} (changelog window empty; \
                             history watermark raised to applied — gap reads will fail explicitly \
                             instead of returning stale values)",
                            Self::CURRENT_VERSION
                        );
                        Ok(Self::migrate_v4_to_v5(v4))
                    }
                    Ok(v4) => Err(Error::Internal(format!(
                        "unsupported snapshot version: {} (expected 2, 3, 4 or {})",
                        v4.version,
                        Self::CURRENT_VERSION
                    ))),
                    Err(_) => {
                        // 尝试 v3 迁移
                        match envelope::decode_bincode_exact::<SnapshotDataV3>(data) {
                            Ok(v3) if v3.version == 3 => {
                                tracing::warn!(
                                    "snapshot v3 detected; migrating to v{} (pd/sys internal \
                                     domains empty)",
                                    Self::CURRENT_VERSION
                                );
                                Ok(Self::migrate_v3_to_v4(v3))
                            }
                            Ok(v3) => Err(Error::Internal(format!(
                                "unsupported snapshot version: {} (expected 2, 3, 4 or {})",
                                v3.version,
                                Self::CURRENT_VERSION
                            ))),
                            Err(_) => {
                                // 尝试 v2 迁移
                                let v2: SnapshotDataV2 = envelope::decode_bincode_exact(data)
                                    .map_err(|e| {
                                        Error::Internal(format!(
                                            "snapshot deserialize (v5+v4+v3+v2): {e}"
                                        ))
                                    })?;
                                if v2.version != 2 {
                                    return Err(Error::Internal(format!(
                                        "unsupported snapshot version: {} (expected 2, 3, 4 or {})",
                                        v2.version,
                                        Self::CURRENT_VERSION
                                    )));
                                }
                                tracing::warn!(
                                    "snapshot v2 detected; migrating to v{} (auth/lease/pd/sys \
                                     empty, compacted=0, applied standalone)",
                                    Self::CURRENT_VERSION
                                );
                                Ok(Self::migrate_v2_to_v4(v2))
                            }
                        }
                    }
                }
            }
        }
    }

    /// v4 → v5 迁移。v4 快照不携带 changelog ⇒ 迁移为空窗口，并把
    /// compacted 水位抬到 `applied_index`：
    ///
    /// 导入 v4 快照后，本节点无法重建 `(原水位, applied]` 区间任一 target 的
    /// 历史（本地旧 changelog 会被导入流程清空）——抬水位使该区间的历史读在
    /// `ensure_history_reconstructable` 处**显式报 `RevisionCompacted`**，
    /// 绝不再静默返回前值。
    fn migrate_v4_to_v5(v4: SnapshotDataV4) -> Self {
        let compacted = v4.compacted_revision.max(v4.applied_index);
        Self {
            version: Self::CURRENT_VERSION,
            last_included_index: v4.last_included_index,
            last_included_term: v4.last_included_term,
            next_revision: v4.next_revision,
            applied_index: v4.applied_index,
            applied_term: v4.applied_term,
            applied_node_id: v4.applied_node_id,
            kv_pairs: v4.kv_pairs,
            kv_metadata: v4.kv_metadata,
            auth_entries: v4.auth_entries,
            lease_entries: v4.lease_entries,
            compacted_revision: compacted,
            pd_entries: v4.pd_entries,
            sys_entries: v4.sys_entries,
            changelog_entries: Vec::new(),
        }
    }

    /// v3 → 当前版本迁移：补空 pd/sys 域（v3 无 region 0 内部记录域）。
    ///
    /// v3 同样不携带 changelog ⇒ compacted 水位取 `max(原水位, applied)`，
    /// 使洞内 target 的历史读显式报错（见 [`Self::migrate_v4_to_v5`] 注释）。
    fn migrate_v3_to_v4(v3: SnapshotDataV3) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            last_included_index: v3.last_included_index,
            last_included_term: v3.last_included_term,
            next_revision: v3.next_revision,
            applied_index: v3.applied_index,
            applied_term: v3.applied_term,
            applied_node_id: v3.applied_node_id,
            kv_pairs: v3.kv_pairs,
            kv_metadata: v3.kv_metadata,
            auth_entries: v3.auth_entries,
            lease_entries: v3.lease_entries,
            compacted_revision: v3.compacted_revision.max(v3.applied_index),
            pd_entries: Vec::new(),
            sys_entries: Vec::new(),
            changelog_entries: Vec::new(),
        }
    }

    /// v2 → 当前版本迁移：补空 auth/lease/pd/sys 域、applied term/node_id 回退 0。
    ///
    /// v2 无 compacted 水位且无 changelog ⇒ 水位取 `applied_index`
    /// （`(0, applied]` 的历史在该导入者上不可重建，必须显式报错而非静默错答）。
    fn migrate_v2_to_v4(v2: SnapshotDataV2) -> Self {
        Self {
            version: Self::CURRENT_VERSION,
            last_included_index: v2.last_included_index,
            last_included_term: v2.last_included_term,
            next_revision: v2.next_revision,
            applied_index: v2.applied_index,
            applied_term: 0,
            applied_node_id: 0,
            kv_pairs: v2.kv_pairs,
            kv_metadata: v2.kv_metadata,
            auth_entries: Vec::new(),
            lease_entries: Vec::new(),
            compacted_revision: v2.applied_index,
            pd_entries: Vec::new(),
            sys_entries: Vec::new(),
            changelog_entries: Vec::new(),
        }
    }
}

/// v4 快照格式（changelog 窗口之前）。字段顺序与 v4 时点一致，
/// 仅用于旧数据升级迁移，不参与导出。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotDataV4 {
    version: u32,
    last_included_index: u64,
    last_included_term: u64,
    next_revision: u64,
    applied_index: u64,
    applied_term: u64,
    applied_node_id: u64,
    kv_pairs: Vec<SnapshotKvPair>,
    kv_metadata: Vec<SnapshotKvMeta>,
    auth_entries: Vec<SnapshotRawEntry>,
    lease_entries: Vec<SnapshotRawEntry>,
    compacted_revision: u64,
    pd_entries: Vec<SnapshotRawEntry>,
    sys_entries: Vec<SnapshotRawEntry>,
}

/// v3 快照格式（PD/迁移内部域之前）。字段顺序与 v3 时点
/// 一致，仅用于旧数据升级迁移，不参与导出。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotDataV3 {
    version: u32,
    last_included_index: u64,
    last_included_term: u64,
    next_revision: u64,
    applied_index: u64,
    applied_term: u64,
    applied_node_id: u64,
    kv_pairs: Vec<SnapshotKvPair>,
    kv_metadata: Vec<SnapshotKvMeta>,
    auth_entries: Vec<SnapshotRawEntry>,
    lease_entries: Vec<SnapshotRawEntry>,
    compacted_revision: u64,
}

/// R-TST-21：v2 快照格式（R-RFT-06 之前）。字段顺序与 v2 时点一致，
/// 仅用于旧数据升级迁移，不参与导出。
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SnapshotDataV2 {
    version: u32,
    last_included_index: u64,
    last_included_term: u64,
    next_revision: u64,
    applied_index: u64,
    kv_pairs: Vec<SnapshotKvPair>,
    kv_metadata: Vec<SnapshotKvMeta>,
}

// ──── 导出/导入函数 ────

/// 从 MvccStorage 导出快照数据
///
/// 遍历所有 KV 数据和元数据，包含 Raft 检查点。
/// 导出的 Value 是加密后的密文（不经过 Barrier 解密），保证 Snapshot 不接触明文。
///
/// R-RFT-06：
/// - **单读事务**导出全部表（redb 读事务提供一致性视图），消除
///   applied / kv / kv_meta 三次独立读事务的撕裂快照；
/// - 补充 auth 域（`/_sys/auth/*`）、lease 域（`/_lease/*`）与 compacted 水位。
///
///
/// - 补充 region 0 `/_pd/*`（PD 全局队列）与 `/_sys/*` 非 auth 域（迁移标记
///   等）——region 0 状态机内部记录随快照导出，导入/追平不丢。
pub fn export_snapshot_data<B: StorageBackend>(
    storage: &MvccStorage<B>,
    last_included_index: u64,
    last_included_term: u64,
) -> Result<SnapshotData> {
    let mut data = SnapshotData::new(last_included_index, last_included_term);

    let backend = storage.backend();
    let (
        applied_bytes,
        compacted_bytes,
        kv_rows,
        meta_rows,
        auth_rows,
        lease_rows,
        pd_rows,
        sys_rows,
        changelog_rows,
    ) = backend.read(|tx| {
        let applied = tx.get(TABLE_META, META_LAST_APPLIED)?;
        let compacted = tx.get(TABLE_META, META_COMPACT_REVISION)?;
        let kv_prefix = encode_kv_key(b"");
        let kv_rows = tx.iter_prefix(TABLE_KV, &kv_prefix)?;
        let meta_prefix = encode_kv_meta_key(b"");
        let meta_rows = tx.iter_prefix(TABLE_KV_META, &meta_prefix)?;
        // R-RFT-06：auth / lease 域随快照导出（恢复后用户/角色/会话/租约不丢）
        let auth_rows = tx.iter_prefix(TABLE_KV, b"/_sys/auth/")?;
        let lease_rows = tx.iter_prefix(TABLE_KV, b"/_lease/")?;
        // region 0 PD 队列 / 其余 system 域随快照导出
        let pd_rows = tx.iter_prefix(TABLE_KV, b"/_pd/")?;
        let sys_rows = tx.iter_prefix(TABLE_KV, b"/_sys/")?;
        // changelog 窗口（`apply_compact` 保留的 `>= compacted` 行）——
        // 装快照后的历史读/watch 重放依赖它，不能缺席（见 SnapshotData 字段注释）。
        let changelog_rows = tx.iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)?;
        Ok((
            applied,
            compacted,
            kv_rows,
            meta_rows,
            auth_rows,
            lease_rows,
            pd_rows,
            sys_rows,
            changelog_rows,
        ))
    })?;

    let applied = applied_bytes.as_deref().and_then(AppliedLogId::from_bytes);
    data.applied_index = applied.map(|a| a.index).unwrap_or(last_included_index);
    data.applied_term = applied.map(|a| a.term).unwrap_or(0);
    data.applied_node_id = applied.map(|a| a.node_id).unwrap_or(0);
    data.next_revision = data.applied_index.saturating_add(1);
    data.compacted_revision = compacted_bytes
        .as_deref()
        .and_then(|b| {
            let arr: [u8; 8] = b.try_into().ok()?;
            Some(u64::from_be_bytes(arr))
        })
        .unwrap_or(0);

    // 导出 KV 数据（密文，直接读取不经过 Barrier）
    for (internal_key, value) in kv_rows.into_iter() {
        if let Some(user_key) = super::mvcc::decode_kv_key(&internal_key) {
            data.kv_pairs.push(SnapshotKvPair {
                key: user_key.to_vec(),
                value,
            });
        }
    }

    // 导出 KV 元数据
    for (internal_key, meta_bytes) in meta_rows.into_iter() {
        // 提取用户 Key：去掉 /_kv_meta/ 前缀
        let kv_meta_prefix = b"/_kv_meta/";
        if let Some(user_key) = internal_key.strip_prefix(kv_meta_prefix) {
            if let Some(meta) = KvMetadata::from_bytes(&meta_bytes) {
                data.kv_metadata.push(SnapshotKvMeta {
                    key: user_key.to_vec(),
                    version: meta.version,
                    create_revision: meta.create_revision,
                    mod_revision: meta.mod_revision,
                    lease_id: meta.lease_id,
                    deleted: meta.deleted,
                });
            }
        }
    }

    // R-RFT-06：auth / lease 原始条目
    data.auth_entries = auth_rows
        .into_iter()
        .map(|(internal_key, value)| SnapshotRawEntry {
            internal_key,
            value,
        })
        .collect();
    data.lease_entries = lease_rows
        .into_iter()
        .map(|(internal_key, value)| SnapshotRawEntry {
            internal_key,
            value,
        })
        .collect();

    // `/_pd/*` 全量；`/_sys/*` 中去掉已入 auth 域的行
    // （`/_sys/auth/*`），其余（迁移标记等）进 sys_entries。
    data.pd_entries = pd_rows
        .into_iter()
        .map(|(internal_key, value)| SnapshotRawEntry {
            internal_key,
            value,
        })
        .collect();
    data.sys_entries = sys_rows
        .into_iter()
        .filter(|(internal_key, _)| !internal_key.starts_with(b"/_sys/auth/"))
        .map(|(internal_key, value)| SnapshotRawEntry {
            internal_key,
            value,
        })
        .collect();

    // changelog 窗口（原样导出/灌入；导入侧先清空再回填）
    data.changelog_entries = changelog_rows
        .into_iter()
        .map(|(internal_key, value)| SnapshotRawEntry {
            internal_key,
            value,
        })
        .collect();

    Ok(data)
}

/// 将快照数据导入到 MvccStorage
///
/// 清空现有数据后写入快照中的全部 KV 数据、元数据、auth/lease 域。
/// Barrier 加密/解密不介入——快照导入的是原始密文。
///
/// R-RFT-06：
/// - `META_LAST_APPLIED` 写入快照携带的完整 LogId（term/node_id/index）；
/// - 恢复 compacted 水位，保证压缩语义跨快照一致。
pub fn import_snapshot_data<B: StorageBackend>(
    storage: &MvccStorage<B>,
    data: &SnapshotData,
) -> Result<()> {
    if data.version != SnapshotData::CURRENT_VERSION {
        return Err(Error::Internal(format!(
            "unsupported snapshot version: {} (expected {})",
            data.version,
            SnapshotData::CURRENT_VERSION
        )));
    }

    let backend = storage.backend();

    backend.write(|tx| {
        // 清空 KV 表全表（含 /_kv/、/_sys/auth/、/_lease/ 所有域）
        let existing_kv: Vec<Vec<u8>> = tx
            .iter_prefix(TABLE_KV, b"")?
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        for key in &existing_kv {
            tx.remove(TABLE_KV, key)?;
        }

        // 清空 KV 元数据表
        let existing_meta: Vec<Vec<u8>> = tx
            .iter_prefix(TABLE_KV_META, b"")?
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        for key in &existing_meta {
            tx.remove(TABLE_KV_META, key)?;
        }

        // 清空本地 changelog —— 快照携带的是**源节点**的窗口；不清空会让
        // 本地旧条目（旧窗口）与新状态混在一起，在 (旧水位, S] 区间形成「洞」，
        // 洞内 target 的历史读会静默返回前值。
        let existing_changelog: Vec<Vec<u8>> = tx
            .iter_prefix(TABLE_CHANGELOG, CHANGELOG_PREFIX)?
            .into_iter()
            .map(|(k, _)| k)
            .collect();
        for key in &existing_changelog {
            tx.remove(TABLE_CHANGELOG, key)?;
        }

        // 写入 KV 数据
        for pair in &data.kv_pairs {
            let internal_key = encode_kv_key(&pair.key);
            tx.insert(TABLE_KV, &internal_key, &pair.value)?;
        }

        // 写入 KV 元数据
        for meta in &data.kv_metadata {
            let meta_key = encode_kv_meta_key(&meta.key);
            let meta_bytes = KvMetadata {
                version: meta.version,
                create_revision: meta.create_revision,
                mod_revision: meta.mod_revision,
                lease_id: meta.lease_id,
                deleted: meta.deleted,
            }
            .to_bytes();
            tx.insert(TABLE_KV_META, &meta_key, &meta_bytes)?;
        }

        // R-RFT-06：恢复 auth / lease 域原始条目
        for entry in &data.auth_entries {
            tx.insert(TABLE_KV, &entry.internal_key, &entry.value)?;
        }
        for entry in &data.lease_entries {
            tx.insert(TABLE_KV, &entry.internal_key, &entry.value)?;
        }

        // 恢复 region 0 `/_pd/*`（PD 队列）与 `/_sys/*`
        // 非 auth 域（迁移标记等）原始条目——TABLE_KV 全表清空后一并回填。
        for entry in &data.pd_entries {
            tx.insert(TABLE_KV, &entry.internal_key, &entry.value)?;
        }
        for entry in &data.sys_entries {
            tx.insert(TABLE_KV, &entry.internal_key, &entry.value)?;
        }

        // 灌入快照携带的 changelog 窗口（“整体替换”的第二步；对 v4 迁移
        // 快照为空窗口，此时旧条目已在上面清空、compacted 水位已抬到 applied）。
        for entry in &data.changelog_entries {
            tx.insert(TABLE_CHANGELOG, &entry.internal_key, &entry.value)?;
        }

        // R-RFT-06：持久化 applied 状态（完整 term/node_id，来自快照导出时的真实 LogId）
        tx.insert(
            TABLE_META,
            META_LAST_APPLIED,
            &AppliedLogId {
                term: data.applied_term,
                node_id: data.applied_node_id,
                index: data.applied_index,
            }
            .to_bytes(),
        )?;

        // R-RFT-06：恢复 compacted 水位
        if data.compacted_revision > 0 {
            tx.insert(
                TABLE_META,
                META_COMPACT_REVISION,
                &data.compacted_revision.to_be_bytes(),
            )?;
        } else {
            tx.remove(TABLE_META, META_COMPACT_REVISION)?;
        }

        Ok(())
    })?;

    Ok(())
}

// ──── SnapshotTracker：purge 前置条件守卫 ────

/// 已持久化到磁盘的快照元数据（供 LogStore::purge 前置校验与启动检查）
#[derive(Debug, Clone)]
pub struct DurableSnapshot {
    pub index: u64,
    pub term: u64,
    pub path: PathBuf,
}

/// 记录"最新一份已落盘（fsync + 原子 rename）快照"的共享状态
///
/// StateMachineStore 在快照文件持久化成功后调用 `record_durable`；
/// LogStore::purge 在删除日志前调用 `durable_covers` 校验（openraft 仅在
/// 快照构建成功后触发 purge，守卫保证"无快照 + 日志已删"的不可恢复状态不出现）。
#[derive(Debug, Default)]
pub struct SnapshotTracker {
    durable: Mutex<Option<DurableSnapshot>>,
}

impl SnapshotTracker {
    /// 记录一份已持久化快照（仅当 index 不小于当前记录时覆盖）
    pub fn record_durable(&self, index: u64, term: u64, path: PathBuf) {
        let mut durable = self.durable.lock();
        let should_replace = durable.as_ref().map(|d| index >= d.index).unwrap_or(true);
        if should_replace {
            *durable = Some(DurableSnapshot { index, term, path });
        }
    }

    /// 是否存在覆盖指定 index 的持久化快照（S-RCV-01：同时校验文件仍在磁盘上）
    pub fn durable_covers(&self, index: u64) -> bool {
        self.durable
            .lock()
            .as_ref()
            .map(|d| d.index >= index && d.path.is_file())
            .unwrap_or(false)
    }

    /// 读取当前记录的持久化快照
    pub fn latest(&self) -> Option<DurableSnapshot> {
        self.durable.lock().clone()
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::redb_backend::RedbBackend;
    use coord_core::types::StorageConfig;
    use tempfile::TempDir;

    fn setup_storage() -> (TempDir, MvccStorage<RedbBackend>) {
        let tmp = TempDir::new().unwrap();
        let config = StorageConfig::default();
        let backend = RedbBackend::open(tmp.path(), &config).unwrap();
        let storage = MvccStorage::new(backend).unwrap();
        (tmp, storage)
    }

    #[test]
    fn test_empty_snapshot_roundtrip() {
        let (_tmp, storage) = setup_storage();

        // 导出空快照
        let data = export_snapshot_data(&storage, 5, 3).unwrap();
        assert_eq!(data.last_included_index, 5);
        assert_eq!(data.last_included_term, 3);
        assert_eq!(data.kv_pairs.len(), 0);
        assert_eq!(data.kv_metadata.len(), 0);

        // 序列化/反序列化
        let bytes = data.to_bytes().unwrap();
        let restored = SnapshotData::from_bytes(&bytes).unwrap();
        assert_eq!(restored.last_included_index, 5);
        assert_eq!(restored.last_included_term, 3);

        // 导入空快照
        import_snapshot_data(&storage, &restored).unwrap();
    }

    #[test]
    fn test_snapshot_with_kv_data() {
        let (_tmp, storage) = setup_storage();

        // 写入一些数据
        storage.put(b"/app/config", b"value1", None).unwrap();
        storage.put(b"/app/secret", b"value2", None).unwrap();
        storage
            .put(b"/service/addr", b"127.0.0.1:8080", None)
            .unwrap();

        // 导出版本（不含 Barrier，直接读密文）
        let data = export_snapshot_data(&storage, 10, 2).unwrap();
        assert_eq!(data.kv_pairs.len(), 3);
        assert_eq!(data.kv_metadata.len(), 3);

        // 序列化往返
        let bytes = data.to_bytes().unwrap();
        let restored = SnapshotData::from_bytes(&bytes).unwrap();
        assert_eq!(restored.kv_pairs.len(), 3);
        assert_eq!(restored.kv_metadata.len(), 3);

        // 导入到新 storage
        let tmp2 = TempDir::new().unwrap();
        let config2 = StorageConfig::default();
        let backend2 = RedbBackend::open(tmp2.path(), &config2).unwrap();
        let storage2 = MvccStorage::new(backend2).unwrap();
        import_snapshot_data(&storage2, &restored).unwrap();

        // 验证数据可读
        let v1 = storage2.get(b"/app/config").unwrap();
        assert_eq!(v1, Some(b"value1".to_vec()));
        let v2 = storage2.get(b"/app/secret").unwrap();
        assert_eq!(v2, Some(b"value2".to_vec()));
        let v3 = storage2.get(b"/service/addr").unwrap();
        assert_eq!(v3, Some(b"127.0.0.1:8080".to_vec()));
    }

    #[test]
    fn test_snapshot_with_delete_tombstone() {
        let (_tmp, storage) = setup_storage();

        storage.put(b"/key1", b"val1", None).unwrap();
        storage.put(b"/key2", b"val2", None).unwrap();
        storage.delete(b"/key1").unwrap();

        let data = export_snapshot_data(&storage, 1, 1).unwrap();
        // key1 存在但 value 为空 (tombstone), key2 有值
        assert_eq!(data.kv_pairs.len(), 2);

        let bytes = data.to_bytes().unwrap();
        let restored = SnapshotData::from_bytes(&bytes).unwrap();

        let tmp2 = TempDir::new().unwrap();
        let config2 = StorageConfig::default();
        let backend2 = RedbBackend::open(tmp2.path(), &config2).unwrap();
        let storage2 = MvccStorage::new(backend2).unwrap();
        import_snapshot_data(&storage2, &restored).unwrap();

        // key1 应为 tombstone（None）
        assert!(storage2.get(b"/key1").unwrap().is_none());
        // key2 应有值
        assert_eq!(storage2.get(b"/key2").unwrap(), Some(b"val2".to_vec()));
    }

    #[test]
    fn test_snapshot_many_keys() {
        let (_tmp, storage) = setup_storage();

        // 写入 100 个 key
        for i in 0..100u32 {
            let key = format!("/test/key{:04}", i);
            let val = format!("value{}", i);
            storage.put(key.as_bytes(), val.as_bytes(), None).unwrap();
        }

        let data = export_snapshot_data(&storage, 100, 5).unwrap();
        assert_eq!(data.kv_pairs.len(), 100);
        assert_eq!(data.kv_metadata.len(), 100);

        // 序列化大小合理
        let bytes = data.to_bytes().unwrap();
        assert!(bytes.len() < 100_000, "snapshot should be compact");

        let restored = SnapshotData::from_bytes(&bytes).unwrap();

        let tmp2 = TempDir::new().unwrap();
        let config2 = StorageConfig::default();
        let backend2 = RedbBackend::open(tmp2.path(), &config2).unwrap();
        let storage2 = MvccStorage::new(backend2).unwrap();
        import_snapshot_data(&storage2, &restored).unwrap();

        for i in 0..100u32 {
            let key = format!("/test/key{:04}", i);
            let val = format!("value{}", i);
            assert_eq!(
                storage2.get(key.as_bytes()).unwrap(),
                Some(val.into_bytes())
            );
        }
    }

    // ──── R-RFT-06：auth/lease 域与完整 LogId、compacted 水位 ────

    #[test]
    fn test_snapshot_preserves_auth_lease_and_full_logid() {
        let (_tmp, storage) = setup_storage();
        storage.put(b"/user/key", b"v", None).unwrap();

        // 直接写 auth / lease 域原始条目（模拟 raft apply 后的持久化状态）
        let backend = storage.backend();
        backend
            .write(|tx| {
                tx.insert(TABLE_KV, b"/_sys/auth/user/alice", b"hash-bytes")?;
                tx.insert(TABLE_KV, b"/_lease/42", &[1u8, 2, 3])?;
                Ok(())
            })
            .unwrap();

        // 模拟完整 applied LogId（term/node_id 非零）
        backend
            .write(|tx| {
                tx.insert(
                    TABLE_META,
                    META_LAST_APPLIED,
                    &AppliedLogId {
                        term: 7,
                        node_id: 3,
                        index: 9,
                    }
                    .to_bytes(),
                )?;
                Ok(())
            })
            .unwrap();

        let data = export_snapshot_data(&storage, 9, 7).unwrap();
        assert_eq!(data.applied_term, 7);
        assert_eq!(data.applied_node_id, 3);
        assert_eq!(data.applied_index, 9);
        assert_eq!(data.auth_entries.len(), 1);
        assert_eq!(data.lease_entries.len(), 1);

        // 导入新 storage
        let tmp2 = TempDir::new().unwrap();
        let backend2 = RedbBackend::open(tmp2.path(), &StorageConfig::default()).unwrap();
        let storage2 = MvccStorage::new(backend2).unwrap();
        import_snapshot_data(&storage2, &data).unwrap();

        // auth / lease 域逐字段一致
        let backend2 = storage2.backend();
        let auth_val = backend2
            .read(|tx| tx.get(TABLE_KV, b"/_sys/auth/user/alice"))
            .unwrap()
            .unwrap();
        assert_eq!(auth_val, b"hash-bytes".to_vec());
        let lease_val = backend2
            .read(|tx| tx.get(TABLE_KV, b"/_lease/42"))
            .unwrap()
            .unwrap();
        assert_eq!(lease_val, vec![1u8, 2, 3]);

        // applied 恢复完整 LogId（不得用 standalone 的置零 term/node_id）
        let applied = storage2.get_applied_log_id().unwrap().unwrap();
        assert_eq!(applied.term, 7);
        assert_eq!(applied.node_id, 3);
        assert_eq!(applied.index, 9);
    }

    // ──── region 0 `/_pd/` 与 `/_sys/`（非 auth）域 ────

    #[test]
    fn test_snapshot_preserves_pd_queue_and_sys_domains() {
        let (_tmp, storage) = setup_storage();

        // 直接写 region 0 内部域原始条目：`/_pd/` 队列 + `/_sys/auth/`（已有
        // 域，确认不重复进 sys）+ `/_sys/migration/` 标记
        let backend = storage.backend();
        backend
            .write(|tx| {
                tx.insert(TABLE_KV, b"/_pd/ops/0000000000000001", b"pd-entry-1")?;
                tx.insert(TABLE_KV, b"/_pd/ops/0000000000000002", b"pd-entry-2")?;
                tx.insert(TABLE_KV, b"/_sys/auth/user/alice", b"hash-bytes")?;
                tx.insert(TABLE_KV, b"/_sys/migration/legacy-v1", b"done")?;
                Ok(())
            })
            .unwrap();

        let data = export_snapshot_data(&storage, 9, 7).unwrap();
        assert_eq!(data.auth_entries.len(), 1, "auth domain unchanged");
        assert!(data.lease_entries.is_empty());
        assert_eq!(data.pd_entries.len(), 2, "pd queue domain exported");
        assert_eq!(
            data.sys_entries.len(),
            1,
            "non-auth /_sys/ rows (migration marker) exported"
        );
        assert_eq!(
            data.sys_entries[0].internal_key, b"/_sys/migration/legacy-v1",
            "auth rows must NOT leak into sys_entries"
        );

        // 导入新 storage → 内部域逐字段一致
        let tmp2 = TempDir::new().unwrap();
        let backend2 = RedbBackend::open(tmp2.path(), &StorageConfig::default()).unwrap();
        let storage2 = MvccStorage::new(backend2).unwrap();
        import_snapshot_data(&storage2, &data).unwrap();

        let backend2 = storage2.backend();
        let pd1 = backend2
            .read(|tx| tx.get(TABLE_KV, b"/_pd/ops/0000000000000001"))
            .unwrap()
            .unwrap();
        assert_eq!(pd1, b"pd-entry-1".to_vec());
        let pd2 = backend2
            .read(|tx| tx.get(TABLE_KV, b"/_pd/ops/0000000000000002"))
            .unwrap()
            .unwrap();
        assert_eq!(pd2, b"pd-entry-2".to_vec());
        let marker = backend2
            .read(|tx| tx.get(TABLE_KV, b"/_sys/migration/legacy-v1"))
            .unwrap()
            .unwrap();
        assert_eq!(marker, b"done".to_vec());
        let auth = backend2
            .read(|tx| tx.get(TABLE_KV, b"/_sys/auth/user/alice"))
            .unwrap()
            .unwrap();
        assert_eq!(auth, b"hash-bytes".to_vec());

        // 序列化往返（当前版本直接解析）
        let bytes = data.to_bytes().unwrap();
        let restored = SnapshotData::from_bytes_migrating(&bytes).unwrap();
        assert_eq!(restored.version, SnapshotData::CURRENT_VERSION);
        assert_eq!(restored.pd_entries.len(), 2);
        assert_eq!(restored.sys_entries.len(), 1);
    }

    #[test]
    fn test_snapshot_compacted_revision_roundtrip() {
        let (_tmp, storage) = setup_storage();
        storage.put(b"/a", b"1", None).unwrap();

        // 写入 compacted 水位
        storage
            .backend()
            .write(|tx| {
                tx.insert(TABLE_META, META_COMPACT_REVISION, &42u64.to_be_bytes())?;
                Ok(())
            })
            .unwrap();

        let data = export_snapshot_data(&storage, 1, 1).unwrap();
        assert_eq!(data.compacted_revision, 42);

        let tmp2 = TempDir::new().unwrap();
        let backend2 = RedbBackend::open(tmp2.path(), &StorageConfig::default()).unwrap();
        let storage2 = MvccStorage::new(backend2).unwrap();
        import_snapshot_data(&storage2, &data).unwrap();
        assert_eq!(storage2.compacted_revision().unwrap(), 42);

        // 未压缩的快照导入后 compacted 保持 0
        let data0 = export_snapshot_data(&storage2, 1, 1).unwrap();
        let tmp3 = TempDir::new().unwrap();
        let backend3 = RedbBackend::open(tmp3.path(), &StorageConfig::default()).unwrap();
        let storage3 = MvccStorage::new(backend3).unwrap();
        // 构造无压缩水位的数据
        let mut data_empty = data0.clone();
        data_empty.compacted_revision = 0;
        import_snapshot_data(&storage3, &data_empty).unwrap();
        assert_eq!(storage3.compacted_revision().unwrap(), 0);
    }

    // ──── R-TST-21：数据格式升级兼容（v2 / v3 → v4 迁移）────

    #[test]
    fn test_snapshot_v3_upgrade_migration() {
        // 构造一条 v3 格式快照（之前：无 `/_pd/` 与 `/_sys/` 非 auth 域）
        let v3 = SnapshotDataV3 {
            version: 3,
            last_included_index: 11,
            last_included_term: 6,
            next_revision: 12,
            applied_index: 11,
            applied_term: 7,
            applied_node_id: 3,
            kv_pairs: vec![SnapshotKvPair {
                key: b"/legacy/key".to_vec(),
                value: b"legacy-value".to_vec(),
            }],
            kv_metadata: Vec::new(),
            auth_entries: vec![SnapshotRawEntry {
                internal_key: b"/_sys/auth/user/alice".to_vec(),
                value: b"hash-bytes".to_vec(),
            }],
            lease_entries: Vec::new(),
            compacted_revision: 5,
        };
        let v3_bytes = bincode::serialize(&v3).unwrap();

        // v4 结构直接解析失败 → v3 迁移路径成功（域补空；既有域保留）
        assert!(SnapshotData::from_bytes(&v3_bytes).is_err());
        let migrated =
            SnapshotData::from_bytes_migrating(&v3_bytes).expect("v3 快照应可迁移为当前版本");
        assert_eq!(migrated.version, SnapshotData::CURRENT_VERSION);
        assert_eq!(migrated.last_included_index, 11);
        assert_eq!(migrated.applied_term, 7, "v3 applied term 保留");
        assert_eq!(migrated.applied_node_id, 3);
        assert_eq!(migrated.auth_entries.len(), 1, "v3 auth 域保留");
        assert_eq!(
            migrated.compacted_revision, 11,
            "v3 无 changelog ⇒ 水位抬到 max(原水位 5, applied 11)"
        );
        assert!(migrated.pd_entries.is_empty(), "v3 无 pd 域 → 补空");
        assert!(migrated.sys_entries.is_empty(), "v3 无 sys 域 → 补空");
        assert!(
            migrated.changelog_entries.is_empty(),
            "v3 无 changelog 窗口 → 补空"
        );

        // 迁移后可正常导入恢复
        let tmp = TempDir::new().unwrap();
        let backend = RedbBackend::open(tmp.path(), &StorageConfig::default()).unwrap();
        let storage = MvccStorage::new(backend).unwrap();
        import_snapshot_data(&storage, &migrated).unwrap();
        assert_eq!(
            storage.get(b"/legacy/key").unwrap(),
            Some(b"legacy-value".to_vec())
        );
        let applied = storage.get_applied_log_id().unwrap().unwrap();
        assert_eq!(applied.index, 11);
        assert_eq!(applied.term, 7);
    }

    #[test]
    fn test_snapshot_v2_upgrade_migration() {
        // 构造一条 v2 格式快照（R-RFT-06 之前的字段序，无 auth/lease/水位）
        let v2 = SnapshotDataV2 {
            version: 2,
            last_included_index: 11,
            last_included_term: 6,
            next_revision: 12,
            applied_index: 11,
            kv_pairs: vec![SnapshotKvPair {
                key: b"/legacy/key".to_vec(),
                value: b"legacy-value".to_vec(),
            }],
            kv_metadata: vec![SnapshotKvMeta {
                key: b"/legacy/key".to_vec(),
                version: 1,
                create_revision: 3,
                mod_revision: 3,
                lease_id: 0,
                deleted: false,
            }],
        };
        let v2_bytes = bincode::serialize(&v2).unwrap();

        // 直接解析（当前/v4/v3 结构）失败 → 迁移路径成功
        assert!(SnapshotData::from_bytes(&v2_bytes).is_err());
        let migrated =
            SnapshotData::from_bytes_migrating(&v2_bytes).expect("v2 快照应可迁移为当前版本");
        assert_eq!(migrated.version, SnapshotData::CURRENT_VERSION);
        assert_eq!(migrated.last_included_index, 11);
        assert_eq!(migrated.last_included_term, 6);
        assert_eq!(migrated.applied_index, 11);
        assert_eq!(migrated.applied_term, 0, "v2 无 applied term → 回退 0");
        assert_eq!(migrated.applied_node_id, 0);
        assert!(migrated.auth_entries.is_empty());
        assert!(migrated.lease_entries.is_empty());
        assert!(migrated.pd_entries.is_empty());
        assert!(migrated.sys_entries.is_empty());
        assert_eq!(
            migrated.compacted_revision, 11,
            "v2 无 changelog ⇒ 水位取 applied_index"
        );
        assert!(migrated.changelog_entries.is_empty());
        assert_eq!(migrated.kv_pairs.len(), 1);
        assert_eq!(migrated.kv_pairs[0].value, b"legacy-value");

        // 迁移后的 v4 快照可正常导入恢复
        let tmp = TempDir::new().unwrap();
        let backend = RedbBackend::open(tmp.path(), &StorageConfig::default()).unwrap();
        let storage = MvccStorage::new(backend).unwrap();
        import_snapshot_data(&storage, &migrated).unwrap();
        assert_eq!(
            storage.get(b"/legacy/key").unwrap(),
            Some(b"legacy-value".to_vec())
        );
        let applied = storage.get_applied_log_id().unwrap().unwrap();
        assert_eq!(applied.index, 11);

        // 未知版本拒绝（v1 等）
        let mut v1 = v2.clone();
        v1.version = 1;
        let v1_bytes = bincode::serialize(&v1).unwrap();
        assert!(SnapshotData::from_bytes_migrating(&v1_bytes).is_err());
    }

    // ──── 装快照后洞内历史读（判据 + 旧格式迁移）────

    /// 判据（正）：快照携带 changelog 窗口 ⇒ 装快照后洞内 target 的历史读
    /// 返回**真值**。
    ///
    /// 现场形态：A 写 K=v_old(r1) / K=v_new(r3)；B 只 apply 到 r1 后导入 A 的
    /// 快照（S≥r3）。洞内 target 若不携带 changelog，`B.get_at_revision(K, r3)`
    /// 会返回 v_old（静默错答）；必须返回 v_new。
    #[test]
    fn test_snapshot_carries_changelog_gap_reads_return_correct_value() {
        let (_tmp_a, storage_a) = setup_storage();
        // r1..r4（含其他 key，模拟真实日志密度）
        storage_a
            .put_at_revision(b"/k", b"v_old", None, 1, AppliedLogId::standalone(1))
            .unwrap();
        storage_a
            .put_at_revision(b"/other", b"x", None, 2, AppliedLogId::standalone(2))
            .unwrap();
        storage_a
            .put_at_revision(b"/k", b"v_new", None, 3, AppliedLogId::standalone(3))
            .unwrap();
        storage_a
            .put_at_revision(b"/other", b"y", None, 4, AppliedLogId::standalone(4))
            .unwrap();

        let data = export_snapshot_data(&storage_a, 5, 1).unwrap();
        assert!(
            !data.changelog_entries.is_empty(),
            "快照必须携带 changelog 窗口（不得为空）"
        );

        // B：只 apply 到 r1（K=v_old）；(1, S] 对 B 原本是「洞」
        let tmp_b = TempDir::new().unwrap();
        let backend_b = RedbBackend::open(tmp_b.path(), &StorageConfig::default()).unwrap();
        let storage_b = MvccStorage::new(backend_b).unwrap();
        storage_b
            .put_at_revision(b"/k", b"v_old", None, 1, AppliedLogId::standalone(1))
            .unwrap();

        import_snapshot_data(&storage_b, &data).unwrap();

        // 洞内 target：必须返回 v_new（不得静默返回 v_old）
        assert_eq!(
            storage_b.get_at_revision(b"/k", 3).unwrap(),
            Some(b"v_new".to_vec()),
            "装快照后洞内 target 的历史读必须返回真值"
        );
        // 边界：r2 之前仍是 v_old；r1 亦然
        assert_eq!(
            storage_b.get_at_revision(b"/k", 2).unwrap(),
            Some(b"v_old".to_vec())
        );
        assert_eq!(
            storage_b.get_at_revision(b"/k", 1).unwrap(),
            Some(b"v_old".to_vec())
        );
        // watch 重放：从 r2 起可读到 r3 的事件（该区间不得缺失）
        let events = storage_b.read_changelog_entries_strict(2).unwrap();
        assert!(
            events.iter().any(|e| e.revision == 3),
            "快照窗口内的事件必须可回放（watch resume）"
        );
        // 启动一致性校验：applied 与 changelog 尾部一致（不再断链）
        let (applied, tail) = storage_b.verify_consistency().unwrap();
        assert_eq!(applied, 4);
        assert_eq!(tail, Some(4), "changelog 尾部应覆盖到最新 apply");
    }

    /// 判据（旧格式迁移）：v4 快照（不带 changelog）迁移后水位抬到 applied；
    /// 导入到带旧 changelog 的节点上时，洞内 target 的读必须**显式报
    /// `RevisionCompacted`**，绝不静默返回前值。
    #[test]
    fn test_v4_snapshot_migration_makes_gap_reads_explicit() {
        let (_tmp_a, storage_a) = setup_storage();
        storage_a
            .put_at_revision(b"/k", b"v_old", None, 1, AppliedLogId::standalone(1))
            .unwrap();
        storage_a
            .put_at_revision(b"/k", b"v_new", None, 3, AppliedLogId::standalone(3))
            .unwrap();
        let data = export_snapshot_data(&storage_a, 3, 1).unwrap();

        // 构造 v4 格式字节（无 changelog 字段）
        let v4 = SnapshotDataV4 {
            version: 4,
            last_included_index: data.last_included_index,
            last_included_term: data.last_included_term,
            next_revision: data.next_revision,
            applied_index: data.applied_index,
            applied_term: data.applied_term,
            applied_node_id: data.applied_node_id,
            kv_pairs: data.kv_pairs.clone(),
            kv_metadata: data.kv_metadata.clone(),
            auth_entries: data.auth_entries.clone(),
            lease_entries: data.lease_entries.clone(),
            compacted_revision: data.compacted_revision,
            pd_entries: data.pd_entries.clone(),
            sys_entries: data.sys_entries.clone(),
        };
        let v4_bytes = bincode::serialize(&v4).unwrap();
        let migrated = SnapshotData::from_bytes_migrating(&v4_bytes).expect("v4 快照应可迁移");
        assert_eq!(migrated.version, SnapshotData::CURRENT_VERSION);
        assert!(migrated.changelog_entries.is_empty());
        assert_eq!(
            migrated.compacted_revision, 3,
            "v4 无 changelog ⇒ 水位抬到 applied（3）"
        );

        // 导入到带旧 changelog 的 B（本地有 r1）：旧条目必须被清掉，
        // 洞内 target 必须显式报错（不能静默返回 v_old）
        let tmp_b = TempDir::new().unwrap();
        let backend_b = RedbBackend::open(tmp_b.path(), &StorageConfig::default()).unwrap();
        let storage_b = MvccStorage::new(backend_b).unwrap();
        storage_b
            .put_at_revision(b"/k", b"v_old", None, 1, AppliedLogId::standalone(1))
            .unwrap();
        import_snapshot_data(&storage_b, &migrated).unwrap();

        assert_eq!(storage_b.compacted_revision().unwrap(), 3);
        let err = storage_b
            .get_at_revision(b"/k", 2)
            .expect_err("v4 迁移快照的洞内历史读必须显式报错");
        assert!(
            matches!(err, Error::RevisionCompacted { .. }),
            "错误必须是 RevisionCompacted，实际：{err:?}"
        );
        // 水位之上的新区域不受影响：当前状态读正常、新 revision 历史读可重建
        assert_eq!(storage_b.get(b"/k").unwrap(), Some(b"v_new".to_vec()));
        assert_eq!(
            storage_b.get_at_revision(b"/k", 4).unwrap(),
            Some(b"v_new".to_vec())
        );
    }

    // ──── 格式信封（P2a：快照三路读 + 精确消费）────

    fn envelope_sample_snapshot() -> SnapshotData {
        let mut data = SnapshotData::new(7, 3);
        data.next_revision = 8;
        data.applied_index = 7;
        data.applied_term = 3;
        data.applied_node_id = 1;
        data.kv_pairs = vec![SnapshotKvPair {
            key: b"/k".to_vec(),
            value: b"v".to_vec(),
        }];
        data.changelog_entries = vec![SnapshotRawEntry {
            internal_key: b"/_changelog/0000000000000007".to_vec(),
            value: b"ev".to_vec(),
        }];
        data
    }

    /// V2 行（postcard，仅承载 v5）⇒ 解码成功（from_bytes / from_bytes_migrating）。
    /// 负控制：删除 V2 读腿 ⇒ 本用例必红。
    #[test]
    fn test_snapshot_v2_row_decodes() {
        let data = envelope_sample_snapshot();
        let bytes = envelope::encode_v2(&data);

        let restored = SnapshotData::from_bytes_migrating(&bytes).expect("V2 快照必须可解码");
        assert_eq!(restored.version, SnapshotData::CURRENT_VERSION);
        assert_eq!(restored.last_included_index, 7);
        assert_eq!(restored.kv_pairs.len(), 1);
        assert_eq!(restored.changelog_entries.len(), 1);

        let direct = SnapshotData::from_bytes(&bytes).expect("from_bytes 必须读 V2");
        assert_eq!(direct.applied_term, 3);
    }

    /// V2 腿版本不是 v5 ⇒ 显式报错（V2 无迁移阶梯，不得按旧结构试解）。
    /// 负控制：去掉 `version == CURRENT_VERSION` 检查 ⇒ 本用例必红。
    #[test]
    fn test_snapshot_v2_wrong_version_rejected() {
        let mut data = envelope_sample_snapshot();
        data.version = 4;
        let bytes = envelope::encode_v2(&data);

        let err = SnapshotData::from_bytes_migrating(&bytes)
            .expect_err("V2 行 version != 5 必须显式报错");
        assert!(
            format!("{err:?}").contains("unsupported snapshot version"),
            "错误必须是版本不受支持，实际：{err:?}"
        );
        assert!(SnapshotData::from_bytes(&bytes).is_err());
    }

    /// V2 行尾随字节 ⇒ 显式失败（精确消费）。
    /// 负控制：去掉 postcard remainder 空断言 ⇒ 本用例必红。
    #[test]
    fn test_snapshot_v2_trailing_bytes_rejected() {
        let data = envelope_sample_snapshot();
        let mut bytes = envelope::encode_v2(&data);
        bytes.extend_from_slice(&[0xDE, 0xAD]);
        assert!(SnapshotData::from_bytes_migrating(&bytes).is_err());
        assert!(SnapshotData::from_bytes(&bytes).is_err());
    }

    /// V2 行魔数 / 版本字节篡改 ⇒ 显式失败（不得按旧格式静默解出偏差值）。
    /// 负控制：去掉魔数比较 / 版本检查放宽 ⇒ 本用例必红。
    #[test]
    fn test_snapshot_v2_tampered_prefix_rejected() {
        let data = envelope_sample_snapshot();

        let mut magic = envelope::encode_v2(&data);
        magic[0] = 0x03;
        assert!(
            SnapshotData::from_bytes_migrating(&magic).is_err(),
            "魔数破坏后不得静默解出快照"
        );

        let mut version = envelope::encode_v2(&data);
        version[envelope::MAGIC.len()] = 9;
        let err = SnapshotData::from_bytes_migrating(&version).expect_err("未知版本必须显式报错");
        assert!(
            format!("{err:?}").contains("unsupported envelope version"),
            "错误必须是未知信封版本，实际：{err:?}"
        );
    }

    /// V2 行截断 ⇒ 显式失败。
    #[test]
    fn test_snapshot_v2_truncated_rejected() {
        let data = envelope_sample_snapshot();
        let bytes = envelope::encode_v2(&data);
        assert!(SnapshotData::from_bytes_migrating(&bytes[..bytes.len() - 1]).is_err());
    }

    /// V1 行（bincode payload）读：v5 直接成功；V1 包裹的 v4 行走阶梯迁移。
    /// 负控制：V1 分区不接 bincode 阶梯 ⇒ 本用例必红。
    #[test]
    fn test_snapshot_v1_row_decodes_with_ladder() {
        let data = envelope_sample_snapshot();
        let v1 = envelope::encode(&data).unwrap();
        let restored = SnapshotData::from_bytes_migrating(&v1).expect("V1 v5 行必须可解码");
        assert_eq!(restored.last_included_index, 7);

        // V1 包裹的 v4 行（无 changelog 窗口）⇒ 阶梯迁移，水位抬到 applied
        let v4 = SnapshotDataV4 {
            version: 4,
            last_included_index: 11,
            last_included_term: 6,
            next_revision: 12,
            applied_index: 11,
            applied_term: 7,
            applied_node_id: 3,
            kv_pairs: Vec::new(),
            kv_metadata: Vec::new(),
            auth_entries: Vec::new(),
            lease_entries: Vec::new(),
            compacted_revision: 5,
            pd_entries: Vec::new(),
            sys_entries: Vec::new(),
        };
        let payload = bincode::serialize(&v4).unwrap();
        let mut v1_v4 = Vec::with_capacity(envelope::PREFIX_LEN + payload.len());
        v1_v4.extend_from_slice(&envelope::MAGIC);
        v1_v4.push(envelope::VERSION);
        v1_v4.extend_from_slice(&payload);
        let migrated =
            SnapshotData::from_bytes_migrating(&v1_v4).expect("V1 包裹的 v4 行必须走阶梯迁移");
        assert_eq!(migrated.version, SnapshotData::CURRENT_VERSION);
        assert!(migrated.changelog_entries.is_empty());
        assert_eq!(migrated.compacted_revision, 11);
    }

    /// 混读：同一快照的无前缀 / V1 / V2 三种编码均可解码，内容一致。
    /// 负控制：任一读腿移除 ⇒ 本用例必红。
    #[test]
    fn test_snapshot_mixed_encodings_decode() {
        let data = envelope_sample_snapshot();
        let legacy = bincode::serialize(&data).unwrap();
        let v1 = envelope::encode(&data).unwrap();
        let v2 = envelope::encode_v2(&data);
        for (label, bytes) in [("legacy", legacy), ("v1", v1), ("v2", v2)] {
            let restored = SnapshotData::from_bytes_migrating(&bytes)
                .unwrap_or_else(|e| panic!("{label} 快照必须可解码：{e:?}"));
            assert_eq!(restored.last_included_index, 7, "{label}");
            assert_eq!(restored.applied_node_id, 1, "{label}");
        }
    }

    /// 无前缀 / V1 行尾随字节 ⇒ 显式失败（旧格式读收窄为精确消费）。
    /// 负控制：bincode 侧改回 allow_trailing ⇒ 本用例必红。
    #[test]
    fn test_snapshot_legacy_and_v1_trailing_bytes_rejected() {
        let data = envelope_sample_snapshot();

        let mut legacy = bincode::serialize(&data).unwrap();
        legacy.extend_from_slice(&[0xDE, 0xAD]);
        assert!(SnapshotData::from_bytes_migrating(&legacy).is_err());

        let mut v1 = envelope::encode(&data).unwrap();
        v1.extend_from_slice(&[0xDE, 0xAD]);
        assert!(SnapshotData::from_bytes_migrating(&v1).is_err());
    }
}
