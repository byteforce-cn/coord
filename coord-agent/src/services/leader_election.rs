// coord-agent: Leader 选举 (Leader Election Service)
//
// 实现 BaseService trait，提供分布式 Leader 选举能力。
// 基于 Coord 核心原语（Lease + Watch + Txn）构建。
//
// 架构（v3.0）:
// - 封装选举逻辑，提供角色变化回调
// - 支持单 Leader / 多 Leader 分组选举
// - Leader 持有 Lease，Follower Watch 等待
//
// 参见 docs/client-agent-architecture-v3.md §5.8。

use std::collections::BTreeMap;
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
    pub fn new(name: impl Into<String>, leader_id: impl Into<String>, lease_id: i64, ttl_secs: u64) -> Self {
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
        let storage_key = ElectionGroup::storage_key(group);

        // 尝试获取 Leader：创建 Lease + 写入选举 key
        let lease_id = self
            .inner
            .client
            .lease()
            .grant(ttl_secs as i64)
            .await
            .map_err(|e| format!("failed to grant election lease: {e}"))?;

        let group_info = ElectionGroup::new(group, candidate_id, lease_id, ttl_secs);
        let value = serde_json::to_vec(&group_info)
            .map_err(|e| format!("serialize election group: {e}"))?;

        match self
            .inner
            .client
            .kv()
            .put_lease(&storage_key, &value, lease_id)
            .await
        {
            Ok(_) => {
                // 竞选成功，成为 Leader
                self.cache
                    .write()
                    .set_role(group, LeaderRole::Leader, Some(group_info.clone()));
                let _ = self.role_change_tx.send((
                    group.to_string(),
                    LeaderRole::Leader,
                    Some(group_info),
                ));
                tracing::info!(
                    "LeaderElection: '{candidate_id}' won election for group '{group}'"
                );
                Ok(LeaderRole::Leader)
            }
            Err(e) => {
                // 竞选失败，释放 Lease
                let _ = self.inner.client.lease().revoke(lease_id).await;

                let err_msg = e.to_string();
                if err_msg.contains("already exists") || err_msg.contains("AlreadyExists") {
                    // 已有 Leader，作为 Follower
                    self.cache
                        .write()
                        .set_role(group, LeaderRole::Follower, None);
                    let _ = self.role_change_tx.send((
                        group.to_string(),
                        LeaderRole::Follower,
                        None,
                    ));
                    tracing::info!(
                        "LeaderElection: '{candidate_id}' is follower for group '{group}'"
                    );
                    Ok(LeaderRole::Follower)
                } else {
                    Err(format!("election failed for group '{group}': {e}").into())
                }
            }
        }
    }

    /// 放弃 Leader 地位
    pub async fn resign(&self, group: &str, candidate_id: &str) -> ServiceResult<()> {
        let _storage_key = ElectionGroup::storage_key(group);

        // 验证当前 Leader
        let group_info = match self.cache.read().get_group(group) {
            Some(info) if info.leader_id == candidate_id => info.clone(),
            _ => {
                return Err(format!(
                    "'{candidate_id}' is not the leader of group '{group}'"
                )
                .into());
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
        let _ = self.role_change_tx.send((group.to_string(), LeaderRole::Follower, None));

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
        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = rx.changed() => {
                        tracing::info!("LeaderElectionService: renew background task shutting down");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(10)) => {
                        // 自动续期 Leader Lease
                        let leader_groups: Vec<String> = cache.read().leader_groups().into_iter().map(|s| s.to_string()).collect();
                        // 先收集需要续期的 lease_id（在锁外进行）
                        let renewals: Vec<(String, i64)> = {
                            let guard = cache.read();
                            leader_groups.iter()
                                .filter_map(|g| guard.get_group(g).map(|info| (g.clone(), info.lease_id)))
                                .collect()
                        };
                        for (group, lease_id) in renewals {
                            if let Err(e) = inner.client.lease().keep_alive(lease_id).await {
                                tracing::warn!("LeaderElectionService: failed to renew leader lease for group '{group}': {e}");
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
