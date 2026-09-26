// Lease 租约管理模块
//
// 提供 TTL 租约的创建、续约、撤销和自动过期管理：
// - LeaseGrant：分配 LeaseID，设置 TTL，注册到时间轮
// - LeaseRevoke：手动撤销 Lease，清理绑定 Key
// - LeaseKeepAlive：续约，重置 TTL 倒计时
// - 自动过期：时间轮触发 → 构造 Delete Txn 清理绑定 Key
// - Leader 独占：时间轮仅在 Leader 节点运行
//
// Lease 是协调层核心原语之一，依赖 Timer Wheel 和 MVCC Storage。

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::RwLock;

use coord_core::error::{Error, Result};
use coord_core::types::LeaseID;

use crate::metrics::Metrics;
use crate::timer::TimerWheelHandle;

// ──── Lease 状态 ────

/// 单个 Lease 的完整状态
#[derive(Debug, Clone)]
pub struct Lease {
    /// Lease 唯一标识
    pub id: LeaseID,
    /// 租约 TTL（秒）
    pub ttl_seconds: i64,
    /// 租约过期时刻（单调时钟 Instant）
    pub deadline: tokio::time::Instant,
    /// 绑定到此 Lease 的 Key 列表
    pub attached_keys: Vec<Vec<u8>>,
}

impl Lease {
    /// 检查是否已过期（基于单调时钟）
    pub fn is_expired(&self) -> bool {
        tokio::time::Instant::now() >= self.deadline
    }

    /// 获取剩余 TTL（秒）
    pub fn remaining_ttl_secs(&self) -> f64 {
        let now = tokio::time::Instant::now();
        if now >= self.deadline {
            0.0
        } else {
            (self.deadline - now).as_secs_f64()
        }
    }
}

// ──── Lease Action（操作通知） ────

/// Lease 生命周期操作（发送给 Raft 层处理）
#[derive(Debug, Clone)]
pub enum LeaseAction {
    /// Lease 到期：需清理绑定的 Key
    Expired {
        lease_id: LeaseID,
        attached_keys: Vec<Vec<u8>>,
    },
}

// ──── LeaseManager ────

/// Lease ID 分配器
static NEXT_LEASE_ID: AtomicI64 = AtomicI64::new(1);

/// 活跃 Lease 内部记录
struct LeaseRecord {
    lease: Lease,
    /// 对应的时间轮任务 ID
    timer_id: u64,
    /// **F-27**：已判定过期、其 revoke 尚未**确认提交**（"待提交"态）。
    ///
    /// 处于该态的记录**保留在本地管理器内**，直到 revoke 经 raft 提交成功
    /// （`finish_expired`）。旧实现在 `check_expired()` 返回 action 前就把记录移除，
    /// 于是随后的 `client_write` 失败时记录已丢 ⇒ revoke **永久丢失**、
    /// 绑定 Key 泄漏至下一次 leader rebuild（不再换主则永久泄漏）。
    ///
    /// 对外语义与旧行为一致：调用方（KeepAlive / Attach / Get / 计数）
    /// 将该记录视为**不存在**（过期 Lease 对调用方即不存在）。
    expired_pending_revoke: bool,
}

/// Lease 管理器
///
/// 管理 Lease 生命周期，通过 TimerWheel 实现 TTL 倒计时。
/// Leader 独占运行。
/// 调用方应在事件循环中周期性调用 `check_expired()` 来检测过期 Lease。
pub struct LeaseManager {
    /// 活跃 Lease 映射（LeaseID → LeaseRecord）
    leases: Arc<RwLock<HashMap<LeaseID, LeaseRecord>>>,
    /// 时间轮句柄
    timer: TimerWheelHandle,
    /// 指标注册表（R-OBS-10：active/expired，可选）
    metrics: Option<Arc<Metrics>>,
}

impl LeaseManager {
    /// 创建 LeaseManager
    pub fn new(timer: TimerWheelHandle) -> Self {
        Self {
            leases: Arc::new(RwLock::new(HashMap::new())),
            timer,
            metrics: None,
        }
    }

    /// 挂载指标注册表（R-OBS-10）。
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// 检测已过期的 Lease 并返回需要下发 revoke 的动作。
    ///
    /// 应在事件循环中周期性调用（例如每 100ms）。
    ///
    /// **F-27 语义（幂等重试）**：过期记录**不再在返回前移除**，而是标记为
    /// `expired_pending_revoke` 并继续保留：
    /// - 本轮新过期的：标记 + 结算指标（active−1 / expired+1，**只在跃迁时一次**）
    ///   + 返回 action；
    /// - 此前已标记、revoke 仍无确认提交的：**继续返回 action**（即重试），不重复计指标。
    ///
    /// 调用方在 revoke **确认提交后**必须调用 [`LeaseManager::finish_expired`] 移除记录。
    /// 这样：提交失败 = 记录仍在 ⇒ 下个 tick 自然重试；提交成功 = 记录移除。
    pub fn check_expired(&self) -> Vec<LeaseAction> {
        let mut expired = Vec::new();
        let mut leases = self.leases.write();

        for (lease_id, record) in leases.iter_mut() {
            if record.expired_pending_revoke {
                // 已判定过期但 revoke 尚未确认提交 —— 继续上报以重试（下发幂等）
                expired.push(LeaseAction::Expired {
                    lease_id: *lease_id,
                    attached_keys: record.lease.attached_keys.clone(),
                });
                continue;
            }
            if record.lease.is_expired() {
                record.expired_pending_revoke = true;
                // R-OBS-10：active → expired，**仅在状态跃迁时结算一次**
                // （重试轮次不得重复递减 active，否则 gauge 会被多计）
                if let Some(metrics) = &self.metrics {
                    metrics.dec_lease_active();
                    metrics.inc_lease_expired();
                }
                expired.push(LeaseAction::Expired {
                    lease_id: *lease_id,
                    attached_keys: record.lease.attached_keys.clone(),
                });
            }
        }

        expired
    }

    /// **F-27**：过期 revoke **已确认提交**（raft apply 成功 / 单节点 apply 成功）后，
    /// 移除本地记录并取消时间轮任务。
    ///
    /// 幂等（记录不存在时返回 `false`）。指标已在 `check_expired()` 判定过期时结算
    /// （active−1 / expired+1），故此处**不再**动 `lease_active_total`。
    pub async fn finish_expired(&self, lease_id: LeaseID) -> bool {
        let removed = self.leases.write().remove(&lease_id);
        match removed {
            Some(record) => {
                let _ = self.timer.cancel(record.timer_id).await;
                true
            }
            None => false,
        }
    }

    /// Grant 一个 Lease，支持指定 ID 或自动分配
    ///
    /// 若指定 `requested_id`（非 0），尝试使用该 ID；
    /// 若 ID 已被占用则返回错误。
    /// 若 `requested_id` 为 0，自动分配新 ID。
    /// 返回 LeaseID。
    pub async fn grant_with_id(&self, ttl_seconds: i64, requested_id: LeaseID) -> Result<LeaseID> {
        self.grant_with_id_checked(ttl_seconds, requested_id, |_| false)
            .await
    }

    /// C1：分配 Lease ID 时**同时核对状态机**（raft 复制视图），而不仅是进程内 map。
    ///
    /// `id_taken(id)` 由调用方提供（典型实现：查 `/_lease/{id}` 状态机记录）。
    /// 为何必要：新 leader 上任到 `rebuild()` 完成之间存在窗口，此时进程内计数器
    /// 可能落后于状态机已有 ID，仅查内存 map 会分配到与既有租约**相同**的 ID，
    /// 造成状态机内 Lease 记录被静默覆盖。
    pub async fn grant_with_id_checked<F>(
        &self,
        ttl_seconds: i64,
        requested_id: LeaseID,
        id_taken: F,
    ) -> Result<LeaseID>
    where
        F: Fn(LeaseID) -> bool,
    {
        if ttl_seconds <= 0 || ttl_seconds > 86400 {
            return Err(Error::LeaseTTLOutOfRange {
                ttl: ttl_seconds,
                min: 1,
                max: 86400,
            });
        }

        let lease_id = if requested_id != 0 {
            // 指定 ID：内存 map 与状态机**都**必须空闲
            if self.leases.read().contains_key(&requested_id) || id_taken(requested_id) {
                return Err(Error::AlreadyExists {
                    resource: "lease",
                    key: format!("lease_id={requested_id}"),
                });
            }
            // 更新自动分配计数器（确保不会冲突）
            let current = NEXT_LEASE_ID.load(Ordering::SeqCst);
            if requested_id >= current {
                NEXT_LEASE_ID.store(requested_id + 1, Ordering::SeqCst);
            }
            requested_id
        } else {
            // 自动分配：跳过内存 map / 状态机已占用的 ID（有界探测，避免病态循环）
            const MAX_PROBES: usize = 65_536;
            let mut candidate = 0i64;
            for _ in 0..MAX_PROBES {
                let probe = NEXT_LEASE_ID.fetch_add(1, Ordering::SeqCst);
                if !self.leases.read().contains_key(&probe) && !id_taken(probe) {
                    candidate = probe;
                    break;
                }
            }
            if candidate == 0 {
                return Err(Error::Internal(
                    "lease id allocation exhausted (state machine reports all probe IDs taken)"
                        .to_string(),
                ));
            }
            candidate
        };

        let deadline = tokio::time::Instant::now() + Duration::from_secs(ttl_seconds as u64);
        let lease = Lease {
            id: lease_id,
            ttl_seconds,
            deadline,
            attached_keys: Vec::new(),
        };

        // 插入到时间轮
        //
        // 第三轮 §4.2①：`insert` 在时间轮任务死亡时返回 `None`。**必须 fail-closed**：
        // 旧实现把失败退化为 `timer_id = 0` 并存进租约记录，而 `cancel(0)` 是空操作 →
        // 该租约永不触发到期 → 绑定 Key 静默无界泄漏（无报错、无日志）。
        let timeout = Duration::from_secs(ttl_seconds as u64);
        let Some(timer_id) = self.timer.insert(timeout).await else {
            return Err(Error::Internal(
                "timer wheel unavailable: refusing to grant a lease that could never expire"
                    .to_string(),
            ));
        };

        // C1（TOCTOU 收口）：上面的"ID 未被占用"检查与这里的插入之间隔着
        // `timer.insert(..).await`，两个并发 grant 可能都通过检查 → 后者静默覆盖
        // 前者 → 两个客户端共享同一 lease。插入前在**写锁内**再确认一次（检查与
        // 插入在同一临界区，且临界区内无 await —— parking_lot guard 不跨 await）。
        let conflict = {
            let mut leases = self.leases.write();
            // C1（TOCTOU 收口）：检查与插入在**同一**写锁临界区内，且临界区内无
            // await（parking_lot guard 不跨 await）。
            //
            // 用 `entry()` 一次完成"查 + 插"：语义与之前的 `contains_key` + `insert`
            // 完全一致，但不再触发 `clippy::map_entry`——**该 lint 在 `37b0161` 上
            // 就已存在，会让 CI 的 `clippy -D warnings` 门禁必然为红**（第三轮复核
            // 复核 CI 有效性时发现：fmt 与 clippy 两道门禁在基线提交上都不通过）。
            match leases.entry(lease_id) {
                std::collections::hash_map::Entry::Occupied(_) => true,
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(LeaseRecord {
                        lease,
                        timer_id,
                        expired_pending_revoke: false,
                    });
                    false
                }
            }
        };
        if conflict {
            // 刚插入的定时器要归还（否则时间轮会留下一个悬空任务）
            let _ = self.timer.cancel(timer_id).await;
            return Err(Error::AlreadyExists {
                resource: "lease",
                key: format!("lease_id={lease_id}"),
            });
        }

        // R-OBS-10：活跃 Lease +1
        if let Some(metrics) = &self.metrics {
            metrics.inc_lease_active();
        }

        Ok(lease_id)
    }

    /// Grant 一个 Lease（自动分配 ID）
    pub async fn grant(&self, ttl_seconds: i64) -> Result<LeaseID> {
        self.grant_with_id(ttl_seconds, 0).await
    }

    /// Revoke 一个 Lease
    ///
    /// 取消时间轮定时器，清理 Lease 记录。
    pub async fn revoke(&self, lease_id: LeaseID) -> Result<()> {
        let record = {
            let mut leases = self.leases.write();
            leases
                .remove(&lease_id)
                .ok_or(Error::LeaseNotFound { lease_id })?
        };

        // 取消时间轮任务（忽略结果，任务可能已到期）
        let _ = self.timer.cancel(record.timer_id).await;

        // R-OBS-10：活跃 Lease -1
        //
        // F-27：若该记录已被 `check_expired()` 判定为过期（active 已在跃迁时减过），
        // 则**不得重复递减** —— 否则显式 revoke 与过期清理竞争时 gauge 会被多计。
        if !record.expired_pending_revoke {
            if let Some(metrics) = &self.metrics {
                metrics.dec_lease_active();
            }
        }

        Ok(())
    }

    /// KeepAlive 续约
    ///
    /// 重置 Lease TTL 倒计时。
    ///
    /// **F-27 fail-closed**：已判定过期（revoke 待提交）的 Lease **不得被复活** ——
    /// 否则"客户端以为续期成功"与"服务端即将删除绑定 Key"会同时成立。
    /// 返回 `LeaseNotFound`，与旧行为（过期记录已被移出管理器）一致。
    pub async fn keep_alive(&self, lease_id: LeaseID) -> Result<(LeaseID, i64)> {
        let (timer_id, ttl) = {
            let mut leases = self.leases.write();
            let record = leases
                .get_mut(&lease_id)
                .ok_or(Error::LeaseNotFound { lease_id })?;

            if record.expired_pending_revoke {
                return Err(Error::LeaseNotFound { lease_id });
            }

            record.lease.deadline =
                tokio::time::Instant::now() + Duration::from_secs(record.lease.ttl_seconds as u64);
            (record.timer_id, record.lease.ttl_seconds)
        };

        // 重新调度时间轮
        let timeout = Duration::from_secs(ttl as u64);
        let ok = self.timer.reschedule(timer_id, timeout).await;

        if !ok {
            return Err(Error::LeaseNotFound { lease_id });
        }

        Ok((lease_id, ttl))
    }

    /// 将 Key 绑定到 Lease
    ///
    /// F-27：已判定过期的 Lease 视为不存在（不得再挂新 Key）。
    pub fn attach_key(&self, lease_id: LeaseID, key: &[u8]) -> Result<()> {
        let mut leases = self.leases.write();
        let record = leases
            .get_mut(&lease_id)
            .ok_or(Error::LeaseNotFound { lease_id })?;

        if record.expired_pending_revoke {
            return Err(Error::LeaseNotFound { lease_id });
        }

        record.lease.attached_keys.push(key.to_vec());
        Ok(())
    }

    /// 将 Key 从 Lease 解绑
    ///
    /// F-27：已判定过期的 Lease 视为不存在。
    pub fn detach_key(&self, lease_id: LeaseID, key: &[u8]) -> Result<()> {
        let mut leases = self.leases.write();
        let record = leases
            .get_mut(&lease_id)
            .ok_or(Error::LeaseNotFound { lease_id })?;

        if record.expired_pending_revoke {
            return Err(Error::LeaseNotFound { lease_id });
        }

        record.lease.attached_keys.retain(|k| k != key);
        Ok(())
    }

    /// 获取并清空 Lease 关联的所有 Key（用于 Revoke 时批量删除）
    ///
    /// F-27：已判定过期的 Lease 视为不存在（返回空列表）。
    pub fn take_attached_keys(&self, lease_id: LeaseID) -> Vec<Vec<u8>> {
        let mut leases = self.leases.write();
        match leases.get_mut(&lease_id) {
            Some(record) if !record.expired_pending_revoke => {
                std::mem::take(&mut record.lease.attached_keys)
            }
            _ => Vec::new(),
        }
    }

    /// 获取 Lease 信息
    ///
    /// F-27：已判定过期的 Lease 返回 `None`（对调用方即不存在）。
    pub fn get_lease(&self, lease_id: LeaseID) -> Option<Lease> {
        self.leases
            .read()
            .get(&lease_id)
            .filter(|r| !r.expired_pending_revoke)
            .map(|r| r.lease.clone())
    }

    /// 获取活跃 Lease 数量
    ///
    /// F-27：**不含**已判定过期、revoke 尚待确认的记录（与旧行为一致）。
    pub fn active_lease_count(&self) -> usize {
        self.leases
            .read()
            .values()
            .filter(|r| !r.expired_pending_revoke)
            .count()
    }

    /// 从持久化 Lease 记录重建内存 TTL 视图（B.4.4 failover；**F-70 合并语义**）
    ///
    /// 状态机是租约**存在性**的权威；本地视图只是 **TTL 调度缓存**。
    /// 因此本函数**只增不删**。旧实现"先清空再装载"有一个致命竞态：
    /// reconciler 读状态机快照时，本次 Grant 可能尚未 apply（快照缺该租约），
    /// 而 `grant_with_id_checked` 已把记录插进本地管理器 ⇒ 清空把它抹掉 ⇒
    /// KeepAlive 本地查不到（NOT_FOUND）、过期任务不再调度 ⇒ 绑定 Key 直到下一次
    /// failover 前**永不删除**。2026-09-25 的 2h soak 实测到该形态：
    /// `ka/7636` grant+put 均成功、首次 keepalive 即 `lease 1 not found`，
    /// 随后 41 次读观测 Key 始终在（`jepsen/docs/coord-findings.md` F-70）。
    ///
    /// 合并规则：
    /// - 快照有、本地无 → 插入（未过期按剩余墙钟时长；已过期立即到期，交过期
    ///   worker 经 raft propose Revoke 清理）；
    /// - 快照有、本地也有 → 取两者**更晚**的截止时刻（中途可能被其他 Leader
    ///   续过期；取早 = 提前删除绑定 Key = 安全违约，取晚 = 契约允许的"略晚"）；
    /// - 本地有、快照无（快照读取之后才提交/apply 的 Grant）→ **保留**；
    /// - 不删除任何本地记录：被其他 Leader 吊销后留下的本地残留会在本地下一次
    ///   apply/过期时收敛（Revoke 幂等），代价至多是一条空转记录。
    ///
    /// 同时推进全局 LeaseID 分配器（覆盖快照与本地两者的最大 ID），避免重启/
    /// 换主后自动分配与存量 ID 冲突。
    pub async fn rebuild(
        &self,
        records: Vec<(LeaseID, crate::storage::mvcc::LeaseRecord)>,
    ) -> usize {
        let now_wall_ms = wall_clock_now_ms();
        let mut max_id = 0i64;

        for (id, record) in records {
            max_id = max_id.max(id);
            let remaining_ms = record.deadline_wall_ms.saturating_sub(now_wall_ms);
            let snapshot_timeout = Duration::from_millis(remaining_ms.max(1) as u64);

            // 本地已有该租约：只做"取更晚截止"，绝不降级/删除（F-70）
            let existing = self
                .leases
                .read()
                .get(&id)
                .map(|r| (r.timer_id, r.expired_pending_revoke, r.lease.deadline));
            if let Some((timer_id, pending, local_deadline)) = existing {
                if pending {
                    // 已判定过期、revoke 待提交：不得复活（F-27 fail-closed），
                    // 保持原样由过期 worker 继续重试
                    continue;
                }
                let snapshot_deadline = tokio::time::Instant::now() + snapshot_timeout;
                if snapshot_deadline > local_deadline {
                    if !self.timer.reschedule(timer_id, snapshot_timeout).await {
                        tracing::warn!(
                            lease_id = id,
                            "timer wheel reschedule failed during lease rebuild \
                             (deadline is still enforced by the periodic expiry check)"
                        );
                    }
                    let mut leases = self.leases.write();
                    if let Some(rec) = leases.get_mut(&id) {
                        if !rec.expired_pending_revoke && snapshot_deadline > rec.lease.deadline {
                            rec.lease.deadline = snapshot_deadline;
                            if record.ttl > 0 {
                                rec.lease.ttl_seconds = record.ttl;
                            }
                        }
                    }
                }
                continue;
            }

            // 快照新增：按剩余时长插入（已过期者立即到期，交过期 worker 清理）
            let (deadline, timeout) = if remaining_ms > 0 {
                (
                    tokio::time::Instant::now() + snapshot_timeout,
                    snapshot_timeout,
                )
            } else {
                // 已过期：立即到期，由过期 worker 经 raft 清理（不得直写本地存储）
                (tokio::time::Instant::now(), Duration::from_millis(1))
            };
            // §4.2①：重建路径同样不得把 `None` 当成有效 timer id。
            // 这里遇到时间轮不可用时**跳过该条记录**并告警（不写入伪造的 0）：
            // 缺失的内存 TTL 视图会让该租约只能靠 KeepAlive/Revoke 收敛，
            // 但至少不会把一个"永不取消的 timer"写进记录。
            let Some(timer_id) = self.timer.insert(timeout).await else {
                tracing::error!(
                    lease_id = id,
                    "timer wheel unavailable during lease rebuild: skipping lease record"
                );
                continue;
            };
            // 与 grant 同款 TOCTOU 收口：检查与插入在同一写锁临界区，且无 await。
            let inserted = {
                let mut leases = self.leases.write();
                match leases.entry(id) {
                    std::collections::hash_map::Entry::Occupied(_) => false,
                    std::collections::hash_map::Entry::Vacant(slot) => {
                        slot.insert(LeaseRecord {
                            lease: Lease {
                                id,
                                ttl_seconds: record.ttl,
                                deadline,
                                attached_keys: Vec::new(),
                            },
                            timer_id,
                            // F-27：rebuild 只把 deadline 搬进来；是否过期统一由
                            // check_expired() 判定（revoke 未提交前记录保留、可重试）。
                            expired_pending_revoke: false,
                        });
                        true
                    }
                }
            };
            if !inserted {
                // 并发路径已经建立了该记录：归还刚插入的定时器
                let _ = self.timer.cancel(timer_id).await;
                continue;
            }
            // R-OBS-10：与 grant 对称计数（跃迁到 expired 时由 check_expired 递减一次）
            if let Some(metrics) = &self.metrics {
                metrics.inc_lease_active();
            }
        }

        // 本地残留记录可能带着更大的 ID（快照读取之后接受的 Grant）⇒ 一并纳入上界
        {
            let leases = self.leases.read();
            for id in leases.keys() {
                max_id = max_id.max(*id);
            }
        }
        if max_id > 0 {
            NEXT_LEASE_ID.fetch_max(max_id + 1, Ordering::SeqCst);
        }

        self.leases.read().len()
    }

    /// **F-70**：从状态机记录为本地 TTL 视图「补水」。
    ///
    /// 用于「本地视图缺失、但状态机权威记录仍在」的窗口（failover 重建尚未轮到、
    /// 快照读取滞后、KeepAlive 落到刚上任的新 leader）。语义：
    /// - 记录**未过期** ⇒ 按剩余墙钟时长插入时间轮并返回 `Ok(true)`；
    /// - 记录**已过期** ⇒ 以"立即到期"插入（过期 worker 下一 tick 经 raft
    ///   propose Revoke ⇒ 绑定 Key 级联删除 —— 这也是"泄漏租约"的自愈路径），
    ///   返回 `Ok(false)`（对外语义 = 已不存在）；
    /// - 本地已有记录（并发路径已建立）⇒ 按其当前状态回答，不重复插入。
    ///
    /// 失败（时间轮不可用）返回 `Err`：调用方**不得**把"无法跟踪"当作"租约不存在"
    /// 回给客户端 —— 那会把系统性故障伪装成 NOT_FOUND。
    pub async fn rehydrate(
        &self,
        lease_id: LeaseID,
        ttl_seconds: i64,
        deadline_wall_ms: i64,
        now_wall_ms: i64,
    ) -> Result<bool> {
        // 已有记录：按其自身状态回答（不重置计时）
        if let Some(record) = self.leases.read().get(&lease_id) {
            return Ok(!record.expired_pending_revoke && !record.lease.is_expired());
        }

        let remaining_ms = deadline_wall_ms.saturating_sub(now_wall_ms);
        let (deadline, timeout) = if remaining_ms > 0 {
            (
                tokio::time::Instant::now() + Duration::from_millis(remaining_ms as u64),
                Duration::from_millis(remaining_ms as u64),
            )
        } else {
            (tokio::time::Instant::now(), Duration::from_millis(1))
        };

        let Some(timer_id) = self.timer.insert(timeout).await else {
            return Err(Error::Internal(
                "timer wheel unavailable: refusing to track a rehydrated lease that could \
                 never expire"
                    .to_string(),
            ));
        };

        let inserted = {
            let mut leases = self.leases.write();
            match leases.entry(lease_id) {
                std::collections::hash_map::Entry::Occupied(_) => false,
                std::collections::hash_map::Entry::Vacant(slot) => {
                    slot.insert(LeaseRecord {
                        lease: Lease {
                            id: lease_id,
                            ttl_seconds: ttl_seconds.max(1),
                            deadline,
                            attached_keys: Vec::new(),
                        },
                        timer_id,
                        expired_pending_revoke: false,
                    });
                    true
                }
            }
        };
        if !inserted {
            let _ = self.timer.cancel(timer_id).await;
        } else if let Some(metrics) = &self.metrics {
            // 与 grant 对称计数；若该记录已过期，check_expired 的跃迁会递减一次
            metrics.inc_lease_active();
        }

        Ok(self
            .leases
            .read()
            .get(&lease_id)
            .map(|r| !r.expired_pending_revoke && !r.lease.is_expired())
            .unwrap_or(false))
    }
}

/// 当前墙钟毫秒（deadline 由 leader 在 propose 前计算，保证 apply 确定性）
pub fn wall_clock_now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// 按 deadline 计算剩余 TTL 秒数（向上取整；已过期返回 0）
pub fn remaining_ttl_from_deadline(deadline_wall_ms: i64, now_wall_ms: i64) -> i64 {
    let remaining_ms = deadline_wall_ms.saturating_sub(now_wall_ms);
    if remaining_ms <= 0 {
        0
    } else {
        (remaining_ms + 999) / 1000
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timer::TimerWheel;

    #[test]
    fn test_lease_basics() {
        let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
        let lease = Lease {
            id: 1,
            ttl_seconds: 10,
            deadline,
            attached_keys: vec![],
        };
        assert_eq!(lease.id, 1);
        assert_eq!(lease.ttl_seconds, 10);
        assert!(!lease.is_expired());
        assert!(lease.remaining_ttl_secs() > 0.0);
        assert!(lease.remaining_ttl_secs() <= 10.0);
    }

    #[test]
    fn test_lease_lifecycle() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = TimerWheel::start();
            let manager = LeaseManager::new(handle);

            // Grant
            let lease_id = manager.grant(60).await.unwrap();
            assert!(lease_id > 0);
            assert_eq!(manager.active_lease_count(), 1);

            let lease = manager.get_lease(lease_id).unwrap();
            assert_eq!(lease.ttl_seconds, 60);
            assert!(!lease.is_expired());

            // KeepAlive
            let (returned_id, ttl) = manager.keep_alive(lease_id).await.unwrap();
            assert_eq!(returned_id, lease_id);
            assert_eq!(ttl, 60);

            // Revoke
            manager.revoke(lease_id).await.unwrap();
            assert_eq!(manager.active_lease_count(), 0);
        });
    }

    /// C1 回归固化：并发指定同一 lease ID 时**只能有一个**成功。
    ///
    /// 此前"ID 占用检查"与"写入 map"之间夹着 `timer.insert(..).await`，
    /// 并发 grant 会双双通过检查 → 后者覆盖前者 → 两个客户端共享同一 lease
    /// （到期后误删他人 key）。
    #[test]
    fn test_concurrent_grant_with_same_id_grants_exactly_one() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = TimerWheel::start();
            let manager = Arc::new(LeaseManager::new(handle));

            const LEASE_ID: LeaseID = 4242;
            let mut tasks = Vec::new();
            for _ in 0..16 {
                let manager = Arc::clone(&manager);
                tasks.push(tokio::spawn(async move {
                    manager
                        .grant_with_id_checked(60, LEASE_ID, |_| false)
                        .await
                        .is_ok()
                }));
            }
            let mut succeeded = 0;
            for t in tasks {
                if t.await.unwrap() {
                    succeeded += 1;
                }
            }
            assert_eq!(
                succeeded, 1,
                "exactly one concurrent grant of lease_id={LEASE_ID} may succeed"
            );
            assert_eq!(manager.active_lease_count(), 1);
            // 该 ID 仍然只有一个记录（后续 revoke 只影响这一个）
            assert!(manager.get_lease(LEASE_ID).is_some());
        });
    }

    /// **F-27 回归**：过期记录在 revoke **确认提交前必须保留**，且下一轮
    /// `check_expired()` 必须**继续上报**（= 重试）；`finish_expired()` 之后才消失。
    ///
    /// 旧实现：`check_expired()` 返回前就 `retain(.., false)` 把过期 Lease 移出本地管理器
    /// ⇒ 随后的 `client_write` 失败只 `warn!`，记录已丢 ⇒ revoke **永久丢失**。
    /// （`jepsen/docs/coord-findings.md` F-27。）
    #[test]
    fn test_f27_expired_lease_retained_until_revoke_confirmed() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = TimerWheel::start();
            let manager = LeaseManager::new(handle);

            let lease_id = manager.grant(60).await.unwrap();
            manager.attach_key(lease_id, b"/k1").unwrap();

            // 把 deadline 拨到过去（子模块可访问父模块私有字段），免去真实等待
            {
                let mut leases = manager.leases.write();
                let rec = leases.get_mut(&lease_id).expect("record exists");
                rec.lease.deadline = tokio::time::Instant::now() - Duration::from_secs(1);
            }

            // 第一轮：判定过期并上报，但**记录仍在**（revoke 尚未确认提交）
            let first = manager.check_expired();
            assert_eq!(first.len(), 1, "expiry must be reported exactly once");
            match &first[0] {
                LeaseAction::Expired {
                    lease_id: id,
                    attached_keys,
                } => {
                    assert_eq!(*id, lease_id);
                    assert_eq!(attached_keys, &[b"/k1".to_vec()]);
                }
            }
            assert!(
                manager.leases.read().contains_key(&lease_id),
                "F-27: the record must be retained while the revoke is unconfirmed"
            );

            // 对外语义不变：过期即"不存在"
            assert_eq!(manager.active_lease_count(), 0);
            assert!(manager.get_lease(lease_id).is_none());
            assert!(manager.take_attached_keys(lease_id).is_empty());

            // 第二轮（= 提交仍然失败的下一个 tick）：必须继续上报，否则 revoke 会丢
            let second = manager.check_expired();
            assert_eq!(
                second.len(),
                1,
                "F-27: an unconfirmed expiry must be retried on the next tick"
            );

            // revoke 确认提交 ⇒ 移除，且不再上报
            assert!(manager.finish_expired(lease_id).await);
            assert!(!manager.leases.read().contains_key(&lease_id));
            assert!(manager.check_expired().is_empty());
            // 幂等
            assert!(!manager.finish_expired(lease_id).await);
        });
    }

    /// **F-27 fail-closed**：已判定过期（revoke 待提交）的 Lease 不得被 KeepAlive 复活，
    /// 也不得再挂新 Key、不得被显式 Revoke 二次结算指标。
    ///
    /// 否则"客户端以为续期成功"与"服务端正在删除绑定 Key"会同时成立 —— 那是把
    /// 「过期」换成了「假活」，比丢 revoke 更糟。
    #[test]
    fn test_f27_expired_pending_lease_is_absent_for_callers() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = TimerWheel::start();
            let manager = LeaseManager::new(handle);

            let lease_id = manager.grant(60).await.unwrap();
            {
                let mut leases = manager.leases.write();
                let rec = leases.get_mut(&lease_id).unwrap();
                rec.lease.deadline = tokio::time::Instant::now() - Duration::from_secs(1);
            }
            assert_eq!(manager.check_expired().len(), 1);

            assert!(
                manager.keep_alive(lease_id).await.is_err(),
                "an expired (revoke-pending) lease must not be revived by KeepAlive"
            );
            assert!(
                manager.attach_key(lease_id, b"/k2").is_err(),
                "no new key may be attached to an expired (revoke-pending) lease"
            );
            assert!(manager.detach_key(lease_id, b"/k2").is_err());

            // 显式 revoke 仍然可用（幂等收口），且不得把 active 再减一次
            manager.revoke(lease_id).await.unwrap();
            assert!(!manager.leases.read().contains_key(&lease_id));
            assert!(!manager.finish_expired(lease_id).await);
        });
    }

    /// **F-27 指标口径**：`active → expired` 的结算**只在跃迁时发生一次**；
    /// 重试轮次不得重复递减 `lease_active_total`。
    #[test]
    fn test_f27_expiry_metrics_settled_once_across_retries() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = TimerWheel::start();
            let metrics = Arc::new(Metrics::default());
            let manager = LeaseManager::new(handle).with_metrics(Arc::clone(&metrics));

            let lease_id = manager.grant(60).await.unwrap();
            assert_eq!(metrics.lease_counters(), (1, 0));
            {
                let mut leases = manager.leases.write();
                let rec = leases.get_mut(&lease_id).unwrap();
                rec.lease.deadline = tokio::time::Instant::now() - Duration::from_secs(1);
            }

            assert_eq!(manager.check_expired().len(), 1);
            assert_eq!(metrics.lease_counters(), (0, 1));

            // 重试轮次：仍然上报，但指标不动
            assert_eq!(manager.check_expired().len(), 1);
            assert_eq!(
                metrics.lease_counters(),
                (0, 1),
                "retries must not decrement lease_active_total again"
            );

            manager.finish_expired(lease_id).await;
            assert_eq!(manager.check_expired().len(), 0);
        });
    }

    #[test]
    fn test_lease_ttl_validation() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = TimerWheel::start();
            let manager = LeaseManager::new(handle);

            assert!(manager.grant(0).await.is_err());
            assert!(manager.grant(-1).await.is_err());
            assert!(manager.grant(90000).await.is_err());
        });
    }

    #[test]
    fn test_lease_nonexistent_operations() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = TimerWheel::start();
            let manager = LeaseManager::new(handle);

            assert!(manager.revoke(999).await.is_err());
            assert!(manager.keep_alive(999).await.is_err());
            assert!(manager.attach_key(999, b"key").is_err());
        });
    }

    #[test]
    fn test_lease_attach_detach() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = TimerWheel::start();
            let manager = LeaseManager::new(handle);

            let lease_id = manager.grant(60).await.unwrap();

            manager.attach_key(lease_id, b"/svc/a").unwrap();
            manager.attach_key(lease_id, b"/svc/b").unwrap();

            let lease = manager.get_lease(lease_id).unwrap();
            assert_eq!(lease.attached_keys.len(), 2);

            manager.detach_key(lease_id, b"/svc/a").unwrap();
            let lease = manager.get_lease(lease_id).unwrap();
            assert_eq!(lease.attached_keys.len(), 1);
        });
    }

    #[test]
    fn test_lease_check_expired() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = TimerWheel::start();
            let manager = LeaseManager::new(handle);

            // Grant 短 TTL Lease（1 秒），不使用时间轮到期
            let lease_id = manager.grant(1).await.unwrap();
            manager.attach_key(lease_id, b"/ephemeral/key").unwrap();

            // 等待 Lease 过期
            tokio::time::sleep(Duration::from_millis(1200)).await;

            // check_expired() 应检测到过期
            let actions = manager.check_expired();
            assert_eq!(actions.len(), 1);
            if let LeaseAction::Expired {
                lease_id: id,
                attached_keys,
            } = &actions[0]
            {
                assert_eq!(*id, lease_id);
                assert_eq!(attached_keys, &vec![b"/ephemeral/key".to_vec()]);
            } else {
                panic!("expected Expired action");
            }

            // Lease 已被 check_expired 清理
            assert!(manager.get_lease(lease_id).is_none());
        });
    }

    // ──── failover 重建 ────

    fn persisted_record(ttl: i64, deadline_wall_ms: i64) -> crate::storage::mvcc::LeaseRecord {
        crate::storage::mvcc::LeaseRecord {
            ttl,
            deadline_wall_ms,
            keepalive_revision: 1,
        }
    }

    #[test]
    fn test_remaining_ttl_from_deadline() {
        assert_eq!(remaining_ttl_from_deadline(10_000, 0), 10);
        // 向上取整
        assert_eq!(remaining_ttl_from_deadline(10_500, 0), 11);
        // 已过期 → 0
        assert_eq!(remaining_ttl_from_deadline(1_000, 2_000), 0);
        assert_eq!(remaining_ttl_from_deadline(2_000, 2_000), 0);
    }

    #[test]
    fn test_rebuild_from_persisted_records() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = TimerWheel::start();
            let manager = LeaseManager::new(handle);

            let now = wall_clock_now_ms();
            let records = vec![
                // 未过期：剩余 60s
                (5, persisted_record(60, now + 60_000)),
                // 已过期：立即到期
                (6, persisted_record(1, now - 1_000)),
            ];
            let rebuilt = manager.rebuild(records).await;
            assert_eq!(rebuilt, 2);
            assert_eq!(manager.active_lease_count(), 2);

            let live = manager.get_lease(5).unwrap();
            assert!(!live.is_expired());
            assert!(live.remaining_ttl_secs() > 50.0);

            let expired = manager.get_lease(6).unwrap();
            assert!(expired.is_expired());

            // 已过期者由 check_expired 检出（由过期 worker 经 raft 清理）
            let actions = manager.check_expired();
            assert_eq!(actions.len(), 1);
            assert_eq!(manager.active_lease_count(), 1);
        });
    }

    /// **F-70 回归**：rebuild 不得抹掉「快照里没有、但本地已接受」的租约。
    ///
    /// 竞态形态（2026-09-25 2h soak 实测）：`grant_with_id_checked` 先插本地记录、
    /// 后入 raft 日志；reconciler 读状态机快照若发生在 apply 之前，快照就没有该租约。
    /// 旧实现「先清空再装载」会把它从本地抹掉 ⇒ KeepAlive 本地 NOT_FOUND、
    /// 过期任务丢失 ⇒ 绑定 Key 直到下一次 failover 前**永不删除**（`ka/7636`）。
    #[test]
    fn test_f70_rebuild_keeps_inflight_grant_and_keeps_expiry() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = TimerWheel::start();
            let manager = LeaseManager::new(handle);

            // 本地已接受（模拟 grant 的本地插入）—— raft apply 尚未发生
            let inflight = manager
                .grant_with_id_checked(1, 7, |_| false)
                .await
                .unwrap();

            // reconciler 读到的快照为空（本次 Grant 尚未 apply）
            let tracked = manager.rebuild(vec![]).await;
            assert_eq!(tracked, 1, "the in-flight grant must still be tracked");
            assert!(
                manager.get_lease(inflight).is_some(),
                "F-70: rebuild must not wipe a locally accepted (in-flight) lease"
            );

            // 到期仍必须被上报 —— 否则过期 revoke 不会下发、绑定 Key 永不删除
            tokio::time::sleep(Duration::from_millis(1200)).await;
            let actions = manager.check_expired();
            assert_eq!(
                actions.len(),
                1,
                "the in-flight lease must still expire after the racy rebuild"
            );
            match &actions[0] {
                LeaseAction::Expired { lease_id, .. } => assert_eq!(*lease_id, inflight),
            }
        });
    }

    /// **F-70 合并语义**：快照与本地都有 ⇒ 取**更晚**截止时刻（中途被其他 Leader
    /// 续过期不得被旧记录提前收走 —— 提前删除绑定 Key 是安全违约）；
    /// 快照里没有的本地记录保留；快照里的新记录装载。
    #[test]
    fn test_f70_rebuild_merges_deadlines_and_keeps_local_only_records() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = TimerWheel::start();
            let manager = LeaseManager::new(handle);

            // 本地：ttl 60（deadline ≈ now+60s）；快照：同一租约已被续到 now+120s
            let _ = manager
                .grant_with_id_checked(60, 100, |_| false)
                .await
                .unwrap();
            let now = wall_clock_now_ms();
            let records = vec![
                (100, persisted_record(60, now + 120_000)),
                // 快照独有（本地没有）⇒ 装载
                (101, persisted_record(30, now + 30_000)),
            ];
            let tracked = manager.rebuild(records).await;
            assert_eq!(tracked, 2, "local-only + snapshot records must merge");

            let merged = manager.get_lease(100).expect("merged record must stay");
            assert!(
                merged.remaining_ttl_secs() > 100.0,
                "the later (snapshot) deadline must win; got {}s",
                merged.remaining_ttl_secs()
            );
            assert!(
                manager.get_lease(101).is_some(),
                "snapshot-only record must be loaded"
            );

            // 反向：快照更早 ⇒ 不得把本地已续期的租约提前收走（安全侧）
            let _ = manager
                .grant_with_id_checked(60, 200, |_| false)
                .await
                .unwrap();
            let now = wall_clock_now_ms();
            manager
                .rebuild(vec![(200, persisted_record(60, now + 1_000))])
                .await;
            let kept = manager.get_lease(200).expect("local record must stay");
            assert!(
                kept.remaining_ttl_secs() > 50.0,
                "a shorter snapshot deadline must not shorten a locally tracked lease"
            );
        });
    }

    /// **F-70 自愈路径**：状态机记录存在而本地视图缺失时，`rehydrate` 必须恢复
    /// 跟踪（未过期 ⇒ `true`）；已过期的记录以「立即到期」插入并返回 `false`，
    /// 由过期 worker 经 raft Revoke 删除绑定 Key（泄漏租约的自愈）。
    #[test]
    fn test_f70_rehydrate_restores_tracking_and_heals_expired() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = TimerWheel::start();
            let manager = LeaseManager::new(handle);

            let now = wall_clock_now_ms();
            // 未过期：恢复跟踪并回答「存活」
            assert!(manager
                .rehydrate(9001, 30, now + 30_000, now)
                .await
                .unwrap());
            assert!(manager.get_lease(9001).is_some());
            assert_eq!(manager.active_lease_count(), 1);
            // 幂等：重复补水不改结论
            assert!(manager
                .rehydrate(9001, 30, now + 30_000, now)
                .await
                .unwrap());

            // 已过期：回答「已不存在」，但记录立即到期 ⇒ 下一轮 check_expired 上报（自愈）
            assert!(!manager.rehydrate(9002, 1, now - 1_000, now).await.unwrap());
            let actions = manager.check_expired();
            assert_eq!(actions.len(), 1);
            match &actions[0] {
                LeaseAction::Expired { lease_id, .. } => assert_eq!(*lease_id, 9002),
            }
            // 判定过期（revoke 待提交）后，对调用方即「不存在」
            assert!(
                manager.get_lease(9002).is_none(),
                "a revoke-pending record is absent for callers"
            );
        });
    }
}
