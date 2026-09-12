// coord-agent: Leader 选举 (Leader Election Service)
//
// 实现 BaseService trait，提供分布式 Leader 选举能力。
// 基于 Coord 核心原语（Lease + Watch + Txn）构建。
//
// 架构（v3.0）:
// - 封装选举逻辑，提供角色变化回调
// - 支持单 Leader / 多 Leader 分组选举
// - Leader 持有 Lease，Follower Watch 等待

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use parking_lot::RwLock as ParkingRwLock;
use tokio::sync::{broadcast, watch};

use crate::proxy::AgentInner;
use crate::service::{BaseService, ServiceResult};

// ──── 类型定义 ────

/// Leader 选举角色
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LeaderRole {
    /// 当前是 Leader
    Leader,
    /// 当前是 Follower
    Follower,
    /// 选举进行中
    Electing,
}

impl LeaderRole {
    pub fn is_leader(&self) -> bool {
        matches!(self, LeaderRole::Leader)
    }
}

/// 选举组信息
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct ElectionGroup {
    /// 选举组名称（如 "scheduler", "job-runner"）
    pub name: String,
    /// 当前 Leader 的候选人 ID
    pub leader_id: String,
    /// 绑定的 Lease ID
    pub lease_id: i64,
    /// 选举时间（Unix 时间戳，秒）
    pub elected_at: u64,
    /// Lease TTL（秒）
    pub ttl_secs: u64,
}

impl ElectionGroup {
    pub fn new(
        name: impl Into<String>,
        leader_id: impl Into<String>,
        lease_id: i64,
        ttl_secs: u64,
    ) -> Self {
        Self {
            name: name.into(),
            leader_id: leader_id.into(),
            lease_id,
            elected_at: unix_ts(),
            ttl_secs,
        }
    }

    /// 构造 Server 存储 key
    pub fn storage_key(name: &str) -> Vec<u8> {
        format!("/_election/{name}").into_bytes()
    }
}

fn unix_ts() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ──── ElectionCache ────

/// Leader 选举本地缓存
pub struct ElectionCache {
    /// 当前角色：group_name → (role, election_info)
    groups: BTreeMap<String, (LeaderRole, Option<ElectionGroup>)>,
}

impl ElectionCache {
    pub fn new() -> Self {
        Self {
            groups: BTreeMap::new(),
        }
    }

    /// 设置角色
    pub fn set_role(&mut self, group: &str, role: LeaderRole, info: Option<ElectionGroup>) {
        self.groups.insert(group.to_string(), (role, info));
    }

    /// 获取角色
    pub fn get_role(&self, group: &str) -> Option<LeaderRole> {
        self.groups.get(group).map(|(r, _)| *r)
    }

    /// 检查是否为 Leader
    pub fn is_leader(&self, group: &str) -> bool {
        self.groups
            .get(group)
            .map(|(r, _)| r.is_leader())
            .unwrap_or(false)
    }

    /// 获取选举组信息
    pub fn get_group(&self, group: &str) -> Option<&ElectionGroup> {
        self.groups.get(group).and_then(|(_, info)| info.as_ref())
    }

    /// 获取所有 Leader 角色
    pub fn leader_groups(&self) -> Vec<&str> {
        self.groups
            .iter()
            .filter(|(_, (r, _))| r.is_leader())
            .map(|(k, _)| k.as_str())
            .collect()
    }

    /// 移除组
    pub fn remove_group(&mut self, group: &str) {
        self.groups.remove(group);
    }

    /// 组数量
    pub fn len(&self) -> usize {
        self.groups.len()
    }

    /// 是否为空
    pub fn is_empty(&self) -> bool {
        self.groups.is_empty()
    }
}

impl Default for ElectionCache {
    fn default() -> Self {
        Self::new()
    }
}

// ──── LeaderElectionService ────

/// Leader 选举服务
///
/// 实现 `BaseService` trait，为应用提供分布式 Leader 选举能力。
/// 支持多选举组并行选举。
pub struct LeaderElectionService {
    /// 到 Server 集群的内部客户端（共享）
    inner: Arc<AgentInner>,
    /// 本地选举状态缓存
    cache: Arc<ParkingRwLock<ElectionCache>>,
    /// 角色变更广播
    role_change_tx: broadcast::Sender<(String, LeaderRole, Option<ElectionGroup>)>,
    /// 健康状态
    healthy: ParkingRwLock<bool>,
    /// 关闭信号
    shutdown_tx: ParkingRwLock<Option<watch::Sender<()>>>,
}

    /// 角色变更广播类型别名（C3：退位需主动广播，供业务回调停止以 leader 自居）。
type RoleChangeTx = broadcast::Sender<(String, LeaderRole, Option<ElectionGroup>)>;

/// C3：连续续期失败达到该次数即退位（快速失败兜底）。
const MAX_RENEW_FAILURES: u32 = 2;

/// C3：距上次**成功**续期超过 TTL/2 即退位。
///
/// 这是双主窗口的硬上界：lease 的 TTL 尚未到，本节点就先退出 leader 角色，
/// 因此"旧主仍自认 leader + 新主已选出"的重叠窗口 ≤ TTL/2（< TTL）。
/// 此前是「3 次失败 × (10s 轮询 + TTL/2 超时)」≈ 45s（TTL=30s）——**超过 TTL**，
/// 双主窗口无法收敛。
const STEP_DOWN_AFTER_TTL_FRACTION: u64 = 2;

/// C3：退位后自动重新参选的退避基数（TTL 的倍数）与最大尝试次数。
const REELECT_BACKOFF_TTL_MULTIPLIER: u64 = 1;
const MAX_REELECT_ATTEMPTS: u32 = 3;

/// C3：退位核心逻辑（`step_down` 与后台续期任务共用）。
///
/// 1. best-effort 撤销 leader lease（服务端立即清 leader key；失败也无妨，lease 会按 TTL 过期）；
/// 2. 本地角色置为 Follower 并移除组信息；
/// 3. 广播角色变更（业务回调据此停止以 leader 自居）。
async fn perform_step_down(
    inner: &Arc<AgentInner>,
    cache: &Arc<ParkingRwLock<ElectionCache>>,
    role_change_tx: &RoleChangeTx,
    group: &str,
    candidate_id: &str,
    reason: &str,
) {
    let lease_id = cache.read().get_group(group).map(|info| info.lease_id);
    if let Some(lease_id) = lease_id {
        if let Err(e) = inner.client.lease().revoke(lease_id).await {
            tracing::warn!(
                "LeaderElection: step-down revoke for group '{group}' failed ({e}); 
                 server-side lease will expire by TTL"
            );
        }
    }
    cache.write().remove_group(group);
    let _ = role_change_tx.send((group.to_string(), LeaderRole::Follower, None));
    tracing::error!(
        "LeaderElection: '{candidate_id}' STEPPED DOWN from group '{group}': {reason}"
    );
}

/// C3：竞选实现（`campaign` 与后台"退位后自动重新参选"共用）。
///
/// 语义：grant lease → put_lease 选举 key；成功即 leader，键已存在即 follower
/// （并归还 lease），其他错误原样上报。
async fn campaign_shared(
    inner: &Arc<AgentInner>,
    cache: &Arc<ParkingRwLock<ElectionCache>>,
    role_change_tx: &RoleChangeTx,
    group: &str,
    candidate_id: &str,
    ttl_secs: u64,
) -> ServiceResult<LeaderRole> {
    let storage_key = ElectionGroup::storage_key(group);

    // 尝试获取 Leader：创建 Lease + 写入选举 key
    let lease_id = inner
        .client
        .lease()
        .grant(ttl_secs as i64)
        .await
        .map_err(|e| format!("failed to grant election lease: {e}"))?;

    let group_info = ElectionGroup::new(group, candidate_id, lease_id, ttl_secs);
    let value =
        serde_json::to_vec(&group_info).map_err(|e| format!("serialize election group: {e}"))?;

    match inner
        .client
        .kv()
        .put_lease(&storage_key, &value, lease_id)
        .await
    {
        Ok(_) => {
            // 竞选成功，成为 Leader
            cache
                .write()
                .set_role(group, LeaderRole::Leader, Some(group_info.clone()));
            let _ = role_change_tx.send((group.to_string(), LeaderRole::Leader, Some(group_info)));
            tracing::info!("LeaderElection: '{candidate_id}' won election for group '{group}'");
            Ok(LeaderRole::Leader)
        }
        Err(e) => {
            // 竞选失败，释放 Lease
            let _ = inner.client.lease().revoke(lease_id).await;

            let err_msg = e.to_string();
            if err_msg.contains("already exists") || err_msg.contains("AlreadyExists") {
                // 已有 Leader，作为 Follower
                cache.write().set_role(group, LeaderRole::Follower, None);
                let _ = role_change_tx.send((group.to_string(), LeaderRole::Follower, None));
                tracing::info!("LeaderElection: '{candidate_id}' is follower for group '{group}'");
                Ok(LeaderRole::Follower)
            } else {
                Err(format!("election failed for group '{group}': {e}").into())
            }
        }
    }
}

impl LeaderElectionService {
    pub const NAME: &'static str = "leader_election";
    pub fn new(inner: Arc<AgentInner>, broadcast_capacity: usize) -> Self {
        let (tx, _) = broadcast::channel(broadcast_capacity);
        Self {
            inner,
            cache: Arc::new(ParkingRwLock::new(ElectionCache::new())),
            role_change_tx: tx,
            healthy: ParkingRwLock::new(false),
            shutdown_tx: ParkingRwLock::new(None),
        }
    }

    /// 参与选举（竞选 Leader）
    ///
    /// 尝试通过 Lease + KV 写入获取 Leader 地位。
    /// 成功则成为 Leader，失败则作为 Follower Watch 等待。
    pub async fn campaign(
        &self,
        group: &str,
        candidate_id: &str,
        ttl_secs: u64,
    ) -> ServiceResult<LeaderRole> {
        campaign_shared(
            &self.inner,
            &self.cache,
            &self.role_change_tx,
            group,
            candidate_id,
            ttl_secs,
        )
        .await
    }

    /// C3：主动退位（续期失败 / 超时）。
    ///
    /// 服务端 lease 一旦过期，另一节点即可 campaign 成功；若本节点仍自认
    /// leader，业务层会出现**双主**。故续期失败必须退位，而不是只打一条 warn。
    pub async fn step_down(
        &self,
        group: &str,
        candidate_id: &str,
        reason: &str,
    ) -> ServiceResult<()> {
        perform_step_down(
            &self.inner,
            &self.cache,
            &self.role_change_tx,
            group,
            candidate_id,
            reason,
        )
        .await;
        Ok(())
    }

    /// 放弃 Leader 地位
    pub async fn resign(&self, group: &str, candidate_id: &str) -> ServiceResult<()> {
        let _storage_key = ElectionGroup::storage_key(group);

        // 验证当前 Leader
        let group_info = match self.cache.read().get_group(group) {
            Some(info) if info.leader_id == candidate_id => info.clone(),
            _ => {
                return Err(
                    format!("'{candidate_id}' is not the leader of group '{group}'").into(),
                );
            }
        };

        // 撤销 Lease
        self.inner
            .client
            .lease()
            .revoke(group_info.lease_id)
            .await
            .map_err(|e| format!("failed to resign from group '{group}': {e}"))?;

        self.cache.write().remove_group(group);
        let _ = self
            .role_change_tx
            .send((group.to_string(), LeaderRole::Follower, None));

        tracing::info!("LeaderElection: '{candidate_id}' resigned from group '{group}'");
        Ok(())
    }

    /// 查询当前角色
    pub fn get_role(&self, group: &str) -> Option<LeaderRole> {
        self.cache.read().get_role(group)
    }

    /// 检查是否为 Leader
    pub fn is_leader(&self, group: &str) -> bool {
        self.cache.read().is_leader(group)
    }

    /// 订阅角色变更事件
    pub fn subscribe_role_changes(
        &self,
    ) -> broadcast::Receiver<(String, LeaderRole, Option<ElectionGroup>)> {
        self.role_change_tx.subscribe()
    }

    /// 查询选举组信息
    pub fn get_group_info(&self, group: &str) -> Option<ElectionGroup> {
        self.cache.read().get_group(group).cloned()
    }

    /// 所有 Leader 组
    pub fn leader_groups(&self) -> Vec<String> {
        self.cache
            .read()
            .leader_groups()
            .into_iter()
            .map(|s| s.to_string())
            .collect()
    }
}

#[async_trait]
impl BaseService for LeaderElectionService {
    fn name(&self) -> &'static str {
        Self::NAME
    }

    async fn start(&self) -> ServiceResult<()> {
        tracing::info!("LeaderElectionService: starting");
        *self.healthy.write() = true;

        let (_tx, mut rx) = watch::channel::<()>(());
        *self.shutdown_tx.write() = Some(_tx);

        let cache = self.cache.clone();
        let inner = self.inner.clone();
        let role_change_tx = self.role_change_tx.clone();
        tokio::spawn(async move {
            // C3：每个选举组的连续续期失败计数（成功即清零）。
            let mut renew_failures: HashMap<String, u32> = HashMap::new();
            // C3：每个选举组"最近一次确认 lease 有效"的时刻（用于 TTL/2 硬判据）。
            let mut last_confirmed: HashMap<String, tokio::time::Instant> = HashMap::new();
            // C3：退位后待重新参选：(group → (candidate, ttl, due_at, attempts))。
            let mut reelection: HashMap<String, (String, u64, tokio::time::Instant, u32)> =
                HashMap::new();
            loop {
                // C3：轮询间隔自适应 TTL —— 固定 10s 在 TTL < 10s 时**保证租约先过期**，
                // 每次续期都太晚。取最小 TTL 的 1/4（限制在 [1s, 10s]）。
                let tick = {
                    let guard = cache.read();
                    guard
                        .leader_groups()
                        .iter()
                        .filter_map(|g| guard.get_group(g).map(|i| i.ttl_secs))
                        .min()
                        .map(|ttl| Duration::from_secs((ttl / 4).clamp(1, 10)))
                        .unwrap_or_else(|| Duration::from_secs(10))
                };
                tokio::select! {
                    _ = rx.changed() => {
                        tracing::info!("LeaderElectionService: renew background task shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(tick) => {
                        // 自动续期 Leader Lease
                        let leader_groups: Vec<String> = cache.read().leader_groups().into_iter().map(|s| s.to_string()).collect();
                        // 先收集需要续期的信息（在锁外进行）：group / lease / ttl / leader_id
                        let renewals: Vec<(String, i64, u64, String)> = {
                            let guard = cache.read();
                            leader_groups.iter()
                                .filter_map(|g| guard.get_group(g).map(|info| {
                                    (g.clone(), info.lease_id, info.ttl_secs, info.leader_id.clone())
                                }))
                                .collect()
                        };
                        for (group, lease_id, ttl_secs, candidate_id) in renewals {
                            // C3：单次续期超过 TTL/4 即视为失败（不能无限等）。
                            // server 侧 keep_alive 已有 lease_timeout（C2），这里再加一层
                            // 客户端硬上限，保证分区/掉 quorum 时能在 TTL 内响应。
                            let attempt_timeout = Duration::from_secs((ttl_secs / 4).max(1));
                            let outcome = tokio::time::timeout(
                                attempt_timeout,
                                inner.client.lease().keep_alive(lease_id),
                            )
                            .await;

                            let failure_reason = match outcome {
                                Ok(Ok(_)) => {
                                    renew_failures.remove(&group);
                                    last_confirmed.insert(group.clone(), tokio::time::Instant::now());
                                    None
                                }
                                Ok(Err(e)) => Some(format!("renew failed: {e}")),
                                Err(_) => Some(format!(
                                    "renew exceeded TTL/4 ({attempt_timeout:?}) — no quorum or partition?"
                                )),
                            };

                            if let Some(reason) = failure_reason {
                                let count = renew_failures.entry(group.clone()).or_insert(0);
                                *count = count.saturating_add(1);
                                let count = *count;
                                // C3 硬判据：距上次成功确认已超过 TTL/2 → 无论失败次数
                                // 多少都立即退位（把双主窗口压到 TTL/2 以内）。
                                let since_confirmed = last_confirmed
                                    .get(&group)
                                    .map(|t| t.elapsed());
                                let stale = since_confirmed
                                    .map(|d| d >= Duration::from_secs((ttl_secs / STEP_DOWN_AFTER_TTL_FRACTION).max(1)))
                                    .unwrap_or(count >= MAX_RENEW_FAILURES);
                                tracing::warn!(
                                    "LeaderElectionService: failed to renew leader lease for \
                                     group '{group}' ({reason}); consecutive failures: {count}, \
                                     since last confirmed: {since_confirmed:?}, stale: {stale}"
                                );
                                if stale || count >= MAX_RENEW_FAILURES {
                                    let reason = format!(
                                        "{count} consecutive lease renewals failed ({reason}); \
                                         last confirmed {since_confirmed:?} ago (TTL={ttl_secs}s)"
                                    );
                                    perform_step_down(
                                        &inner,
                                        &cache,
                                        &role_change_tx,
                                        &group,
                                        &candidate_id,
                                        &reason,
                                    )
                                    .await;
                                    renew_failures.remove(&group);
                                    last_confirmed.remove(&group);
                                    // C3：退位后安排一次自动重新参选（避免"误退即永久
                                    // follower"，业务需要手工重新 campaign）。
                                    let backoff = Duration::from_secs(
                                        (ttl_secs * REELECT_BACKOFF_TTL_MULTIPLIER).max(1),
                                    );
                                    reelection.insert(
                                        group.clone(),
                                        (
                                            candidate_id.clone(),
                                            ttl_secs,
                                            tokio::time::Instant::now() + backoff,
                                            1,
                                        ),
                                    );
                                }
                            }
                        }

                        // C3：到点的重新参选（campaign 本身是安全的：要么当选，要么
                        // 成为 follower；失败退避重试，达上限则明确告警后放弃）。
                        let due: Vec<String> = reelection
                            .iter()
                            .filter(|(_, (_, _, due_at, _))| tokio::time::Instant::now() >= *due_at)
                            .map(|(g, _)| g.clone())
                            .collect();
                        for group in due {
                            let Some((candidate_id, ttl_secs, _, attempts)) =
                                reelection.get(&group).cloned()
                            else {
                                continue;
                            };
                            match campaign_shared(
                                &inner,
                                &cache,
                                &role_change_tx,
                                &group,
                                &candidate_id,
                                ttl_secs,
                            )
                            .await
                            {
                                Ok(LeaderRole::Leader) => {
                                    tracing::info!(
                                        "LeaderElection: '{candidate_id}' re-won election for group \
                                         '{group}' after step-down"
                                    );
                                    reelection.remove(&group);
                                    last_confirmed
                                        .insert(group.clone(), tokio::time::Instant::now());
                                }
                                Ok(other) => {
                                    tracing::info!(
                                        "LeaderElection: re-election for group '{group}' ended as \
                                         {other:?} — another candidate holds it; standing by"
                                    );
                                    reelection.remove(&group);
                                }
                                Err(e) => {
                                    if attempts >= MAX_REELECT_ATTEMPTS {
                                        tracing::error!(
                                            "LeaderElection: giving up automatic re-election for \
                                             group '{group}' after {attempts} attempts: {e}"
                                        );
                                        reelection.remove(&group);
                                    } else {
                                        let backoff = Duration::from_secs(
                                            (ttl_secs * REELECT_BACKOFF_TTL_MULTIPLIER).max(1),
                                        );
                                        tracing::warn!(
                                            "LeaderElection: re-election attempt {attempts} for group \
                                             '{group}' failed ({e}); retrying in {backoff:?}"
                                        );
                                        reelection.insert(
                                            group,
                                            (
                                                candidate_id,
                                                ttl_secs,
                                                tokio::time::Instant::now() + backoff,
                                                attempts + 1,
                                            ),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }
            }
        });

        Ok(())
    }

    async fn stop(&self) -> ServiceResult<()> {
        tracing::info!("LeaderElectionService: stopping");
        if let Some(tx) = self.shutdown_tx.write().take() {
            let _ = tx.send(());
        }
        *self.healthy.write() = false;
        Ok(())
    }

    fn health_check(&self) -> bool {
        *self.healthy.read()
    }
}

impl std::fmt::Debug for LeaderElectionService {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LeaderElectionService")
            .field("groups", &self.cache.read().len())
            .field("healthy", &self.health_check())
            .finish()
    }
}

// ──── tests ────

#[cfg(test)]
mod tests {
    use super::*;

    // ──── LeaderRole 测试 ────

    #[test]
    fn test_leader_role_is_leader() {
        assert!(LeaderRole::Leader.is_leader());
        assert!(!LeaderRole::Follower.is_leader());
        assert!(!LeaderRole::Electing.is_leader());
    }

    #[test]
    fn test_leader_role_serialization() {
        let leader = LeaderRole::Leader;
        let json = serde_json::to_string(&leader).unwrap();
        assert_eq!(json, "\"leader\"");

        let restored: LeaderRole = serde_json::from_str("\"follower\"").unwrap();
        assert_eq!(restored, LeaderRole::Follower);
    }

    // ──── ElectionGroup 测试 ────

    #[test]
    fn test_election_group_creation() {
        let group = ElectionGroup::new("scheduler", "node1", 5001, 30);
        assert_eq!(group.name, "scheduler");
        assert_eq!(group.leader_id, "node1");
        assert_eq!(group.lease_id, 5001);
        assert_eq!(group.ttl_secs, 30);
        assert!(group.elected_at > 0);
    }

    #[test]
    fn test_election_group_storage_key() {
        let key = ElectionGroup::storage_key("scheduler");
        assert_eq!(String::from_utf8_lossy(&key), "/_election/scheduler");
    }

    #[test]
    fn test_election_group_serialization_roundtrip() {
        let group = ElectionGroup {
            name: "test".into(),
            leader_id: "n1".into(),
            lease_id: 42,
            elected_at: 1700000000,
            ttl_secs: 30,
        };
        let json = serde_json::to_vec(&group).unwrap();
        let restored: ElectionGroup = serde_json::from_slice(&json).unwrap();
        assert_eq!(restored, group);
    }

    // ──── ElectionCache 测试 ────

    #[test]
    fn test_election_cache_set_and_get_role() {
        let mut cache = ElectionCache::new();
        cache.set_role("group-a", LeaderRole::Leader, None);

        assert_eq!(cache.len(), 1);
        assert_eq!(cache.get_role("group-a"), Some(LeaderRole::Leader));
        assert!(cache.is_leader("group-a"));
        assert!(!cache.is_leader("group-b"));
    }

    #[test]
    fn test_election_cache_role_transition() {
        let mut cache = ElectionCache::new();
        cache.set_role("g1", LeaderRole::Electing, None);
        assert_eq!(cache.get_role("g1"), Some(LeaderRole::Electing));

        cache.set_role("g1", LeaderRole::Leader, None);
        assert!(cache.is_leader("g1"));

        cache.set_role("g1", LeaderRole::Follower, None);
        assert!(!cache.is_leader("g1"));
    }

    #[test]
    fn test_election_cache_leader_groups() {
        let mut cache = ElectionCache::new();
        cache.set_role("a", LeaderRole::Leader, None);
        cache.set_role("b", LeaderRole::Follower, None);
        cache.set_role("c", LeaderRole::Leader, None);

        let leaders = cache.leader_groups();
        assert_eq!(leaders.len(), 2);
        assert!(leaders.contains(&"a"));
        assert!(leaders.contains(&"c"));
        assert!(!leaders.contains(&"b"));
    }

    #[test]
    fn test_election_cache_remove_group() {
        let mut cache = ElectionCache::new();
        cache.set_role("g1", LeaderRole::Leader, None);
        assert_eq!(cache.len(), 1);

        cache.remove_group("g1");
        assert!(cache.is_empty());
        assert_eq!(cache.get_role("g1"), None);
    }

    #[test]
    fn test_election_cache_default() {
        let cache = ElectionCache::default();
        assert!(cache.is_empty());
    }

    // ──── LeaderElectionService 名称常量 ────

    #[test]
    fn test_leader_election_name_constant() {
        assert_eq!(LeaderElectionService::NAME, "leader_election");
    }
}
