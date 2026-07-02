// coord-client: Leader 发现
//
// 维护当前 Leader 地址缓存，支持初始发现和运行时更新。
// ADP §10.3.1 定义三种策略：初始发现、运行时跟踪、全量刷新。

use std::sync::Arc;
use parking_lot::RwLock;

/// Leader 发现状态
#[derive(Debug, Clone)]
pub struct LeaderDiscovery {
    inner: Arc<RwLock<LeaderState>>,
}

#[derive(Debug)]
struct LeaderState {
    /// 当前已知的 Leader 地址（None 表示尚未发现）
    current_leader: Option<String>,
    /// 所有已知端点
    endpoints: Vec<String>,
    /// 当前轮询索引（用于 Round-Robin 初始发现）
    poll_index: usize,
}

impl LeaderDiscovery {
    /// 创建 Leader 发现器
    pub fn new(endpoints: Vec<String>) -> Self {
        Self {
            inner: Arc::new(RwLock::new(LeaderState {
                current_leader: None,
                endpoints,
                poll_index: 0,
            })),
        }
    }

    /// 获取当前缓存的 Leader 地址
    pub fn get_leader(&self) -> Option<String> {
        self.inner.read().current_leader.clone()
    }

    /// 更新 Leader 地址（成功请求后调用）
    pub fn set_leader(&self, addr: String) {
        let mut state = self.inner.write();
        state.current_leader = Some(addr);
    }

    /// 清除 Leader 缓存（收到 NotLeader 错误后调用）
    pub fn clear_leader(&self) {
        self.inner.write().current_leader = None;
    }

    /// 获取下一个端点用于初始发现（Round-Robin）
    pub fn next_endpoint(&self) -> Option<String> {
        let mut state = self.inner.write();
        if state.endpoints.is_empty() {
            return None;
        }
        let idx = state.poll_index % state.endpoints.len();
        state.poll_index = idx.wrapping_add(1);
        Some(state.endpoints[idx].clone())
    }

    /// 尝试更新 Leader（从 NotLeader 错误中提取 hint）
    pub fn try_update_from_hint(&self, hint: Option<&str>) -> bool {
        if let Some(addr) = hint {
            if !addr.is_empty() {
                self.set_leader(addr.to_string());
                return true;
            }
        }
        // 无 hint，清除缓存以便重新发现
        self.clear_leader();
        false
    }

    /// 获取所有已知端点
    pub fn endpoints(&self) -> Vec<String> {
        self.inner.read().endpoints.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_leader_discovery_initial_empty() {
        let ld = LeaderDiscovery::new(vec!["a:1".into(), "b:2".into()]);
        assert!(ld.get_leader().is_none());
    }

    #[test]
    fn test_leader_discovery_set_and_get() {
        let ld = LeaderDiscovery::new(vec!["a:1".into()]);
        ld.set_leader("leader:50051".into());
        assert_eq!(ld.get_leader(), Some("leader:50051".into()));
    }

    #[test]
    fn test_leader_discovery_clear() {
        let ld = LeaderDiscovery::new(vec!["a:1".into()]);
        ld.set_leader("leader:50051".into());
        ld.clear_leader();
        assert!(ld.get_leader().is_none());
    }

    #[test]
    fn test_leader_discovery_round_robin() {
        let ld = LeaderDiscovery::new(vec!["a:1".into(), "b:2".into(), "c:3".into()]);

        let first = ld.next_endpoint();
        let second = ld.next_endpoint();
        let third = ld.next_endpoint();
        let fourth = ld.next_endpoint(); // wraps around

        // All should be valid endpoints
        assert!(first.is_some());
        assert!(second.is_some());
        assert!(third.is_some());
        assert_eq!(fourth, first);
    }

    #[test]
    fn test_try_update_from_hint() {
        let ld = LeaderDiscovery::new(vec!["a:1".into()]);

        // With valid hint
        assert!(ld.try_update_from_hint(Some("new-leader:50051")));
        assert_eq!(ld.get_leader(), Some("new-leader:50051".into()));

        // With empty hint
        ld.try_update_from_hint(Some(""));
        assert!(ld.get_leader().is_none());

        // Re-set and test with None hint
        ld.set_leader("leader:50051".into());
        ld.try_update_from_hint(None);
        assert!(ld.get_leader().is_none());
    }
}
