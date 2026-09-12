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

    /// 检测并清理已过期的 Lease
    ///
    /// 遍历所有活跃 Lease，将已过期的移出并返回操作列表。
    /// 应在事件循环中周期性调用（例如每 100ms）。
    pub fn check_expired(&self) -> Vec<LeaseAction> {
        let mut expired = Vec::new();
        let mut leases = self.leases.write();

        leases.retain(|lease_id, record| {
            if record.lease.is_expired() {
                expired.push(LeaseAction::Expired {
                    lease_id: *lease_id,
                    attached_keys: record.lease.attached_keys.clone(),
                });
                false // 移除过期 Lease
            } else {
                true // 保留
            }
        });

        // R-OBS-10：过期计数（active 减、expired 加）
        if let Some(metrics) = &self.metrics {
            let n = expired.len();
            if n > 0 {
                for _ in 0..n {
                    metrics.dec_lease_active();
                    metrics.inc_lease_expired();
                }
            }
        }

        expired
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
        let timeout = Duration::from_secs(ttl_seconds as u64);
        let timer_id = self.timer.insert(timeout).await;

        // C1（TOCTOU 收口）：上面的"ID 未被占用"检查与这里的插入之间隔着
        // `timer.insert(..).await`，两个并发 grant 可能都通过检查 → 后者静默覆盖
        // 前者 → 两个客户端共享同一 lease。插入前在**写锁内**再确认一次（检查与
        // 插入在同一临界区，且临界区内无 await —— parking_lot guard 不跨 await）。
        let conflict = {
            let mut leases = self.leases.write();
            if leases.contains_key(&lease_id) {
                true
            } else {
                leases.insert(lease_id, LeaseRecord { lease, timer_id });
                false
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
        if let Some(metrics) = &self.metrics {
            metrics.dec_lease_active();
        }

        Ok(())
    }

    /// KeepAlive 续约
    ///
    /// 重置 Lease TTL 倒计时。
    pub async fn keep_alive(&self, lease_id: LeaseID) -> Result<(LeaseID, i64)> {
        let (timer_id, ttl) = {
            let mut leases = self.leases.write();
            let record = leases
                .get_mut(&lease_id)
                .ok_or(Error::LeaseNotFound { lease_id })?;

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
    pub fn attach_key(&self, lease_id: LeaseID, key: &[u8]) -> Result<()> {
        let mut leases = self.leases.write();
        let record = leases
            .get_mut(&lease_id)
            .ok_or(Error::LeaseNotFound { lease_id })?;

        record.lease.attached_keys.push(key.to_vec());
        Ok(())
    }

    /// 将 Key 从 Lease 解绑
    pub fn detach_key(&self, lease_id: LeaseID, key: &[u8]) -> Result<()> {
        let mut leases = self.leases.write();
        let record = leases
            .get_mut(&lease_id)
            .ok_or(Error::LeaseNotFound { lease_id })?;

        record.lease.attached_keys.retain(|k| k != key);
        Ok(())
    }

    /// 获取并清空 Lease 关联的所有 Key（用于 Revoke 时批量删除）
    pub fn take_attached_keys(&self, lease_id: LeaseID) -> Vec<Vec<u8>> {
        let mut leases = self.leases.write();
        if let Some(record) = leases.get_mut(&lease_id) {
            std::mem::take(&mut record.lease.attached_keys)
        } else {
            Vec::new()
        }
    }

    /// 获取 Lease 信息
    pub fn get_lease(&self, lease_id: LeaseID) -> Option<Lease> {
        self.leases.read().get(&lease_id).map(|r| r.lease.clone())
    }

    /// 获取活跃 Lease 数量
    pub fn active_lease_count(&self) -> usize {
        self.leases.read().len()
    }

    /// 从持久化 Lease 记录重建内存视图（B.4.4 failover）
    ///
    /// 新 Leader 接管时调用：清空旧视图与定时器，按状态机 `/_lease/` 记录重建。
    /// - `deadline_wall_ms` 未到 → 以剩余时长插入时间轮（"at-least TTL" 语义，
    ///   failover 期间按墙钟重估，即 B.4.5 的重估）；
    /// - 已过期 → 立即到期，由 leader 过期 worker 经 raft propose Revoke 清理。
    ///   同时推进全局 LeaseID 分配器，避免重启后自动分配与存量 ID 冲突。
    pub async fn rebuild(
        &self,
        records: Vec<(LeaseID, crate::storage::mvcc::LeaseRecord)>,
    ) -> usize {
        // 清空旧视图并取消旧定时器
        let stale_timers: Vec<u64> = {
            let mut leases = self.leases.write();
            let ids = leases.values().map(|r| r.timer_id).collect();
            leases.clear();
            ids
        };
        for timer_id in stale_timers {
            let _ = self.timer.cancel(timer_id).await;
        }

        let now_wall_ms = wall_clock_now_ms();
        let mut rebuilt = 0usize;
        let mut max_id = 0i64;

        for (id, record) in records {
            max_id = max_id.max(id);
            let remaining_ms = record.deadline_wall_ms.saturating_sub(now_wall_ms);
            let (deadline, timeout) = if remaining_ms > 0 {
                (
                    tokio::time::Instant::now() + Duration::from_millis(remaining_ms as u64),
                    Duration::from_millis(remaining_ms as u64),
                )
            } else {
                // 已过期：立即到期，由过期 worker 经 raft 清理（不得直写本地存储）
                (tokio::time::Instant::now(), Duration::from_millis(1))
            };
            let timer_id = self.timer.insert(timeout).await;
            self.leases.write().insert(
                id,
                LeaseRecord {
                    lease: Lease {
                        id,
                        ttl_seconds: record.ttl,
                        deadline,
                        attached_keys: Vec::new(),
                    },
                    timer_id,
                },
            );
            rebuilt += 1;
        }

        // 推进分配器，防止重启后自动分配与存量 ID 冲突
        if max_id > 0 {
            NEXT_LEASE_ID.fetch_max(max_id + 1, Ordering::SeqCst);
        }
        rebuilt
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

    #[test]
    fn test_lease_ttl_validation() {        let rt = tokio::runtime::Runtime::new().unwrap();
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

    #[test]
    fn test_rebuild_replaces_previous_view() {
        let rt = tokio::runtime::Runtime::new().unwrap();
        rt.block_on(async {
            let handle = TimerWheel::start();
            let manager = LeaseManager::new(handle);

            let old_id = manager.grant(60).await.unwrap();
            assert_eq!(manager.active_lease_count(), 1);

            let now = wall_clock_now_ms();
            let records = vec![(42, persisted_record(30, now + 30_000))];
            manager.rebuild(records).await;
            assert_eq!(manager.active_lease_count(), 1);
            assert!(manager.get_lease(old_id).is_none());
            assert!(manager.get_lease(42).is_some());
        });
    }
}
