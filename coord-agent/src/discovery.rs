// coord-agent: 静态配置成员发现实现
//
// 实现 coord_core::discovery::MemberDiscovery trait。
// 从配置文件读取 Server 列表，不自动发现新节点。
//
// 参见 docs/client-agent-architecture.md §5.2。

use std::net::SocketAddr;
use std::sync::RwLock;

use coord_core::discovery::{DiscoveryEvent, MemberDiscovery};

/// 静态配置成员发现实现
///
/// 从配置文件/命令行参数读取 Server 节点列表。
/// Leader 缓存通过 set_leader/clear_leader 手动维护。
/// 不产生 DiscoveryEvent（watch_changes 始终返回 None）。
#[derive(Debug)]
pub struct StaticDiscovery {
    peers: Vec<SocketAddr>,
    leader: RwLock<Option<SocketAddr>>,
}

impl StaticDiscovery {
    /// 创建 StaticDiscovery
    ///
    /// `peers` 应为有效的 SocketAddr 列表。
    /// 如果解析失败（空字符串或无效地址），该条目将被跳过。
    pub fn new(peer_addrs: Vec<SocketAddr>) -> Self {
        Self {
            peers: peer_addrs,
            leader: RwLock::new(None),
        }
    }

    /// 从字符串列表创建，自动解析 SocketAddr
    pub fn from_strings(peer_strs: &[String]) -> Self {
        let peers: Vec<SocketAddr> = peer_strs
            .iter()
            .filter_map(|s| s.parse().ok())
            .collect();
        Self::new(peers)
    }
}

impl MemberDiscovery for StaticDiscovery {
    fn peers(&self) -> Vec<SocketAddr> {
        self.peers.clone()
    }

    fn leader_hint(&self) -> Option<SocketAddr> {
        *self.leader.read().unwrap()
    }

    fn set_leader(&self, addr: SocketAddr) {
        *self.leader.write().unwrap() = Some(addr);
    }

    fn clear_leader(&self) {
        *self.leader.write().unwrap() = None;
    }

    fn watch_changes(&self) -> Option<tokio::sync::mpsc::Receiver<DiscoveryEvent>> {
        None
    }

    fn is_healthy(&self) -> bool {
        !self.peers.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_discovery_empty() {
        let sd = StaticDiscovery::new(vec![]);
        assert!(!sd.is_healthy());
        assert!(sd.peers().is_empty());
    }

    #[test]
    fn test_static_discovery_with_peers() {
        let addr: SocketAddr = "10.0.0.1:50051".parse().unwrap();
        let sd = StaticDiscovery::new(vec![addr]);
        assert!(sd.is_healthy());
        assert_eq!(sd.peers().len(), 1);
    }

    #[test]
    fn test_static_discovery_leader_lifecycle() {
        let addr1: SocketAddr = "10.0.0.1:50051".parse().unwrap();
        let addr2: SocketAddr = "10.0.0.2:50051".parse().unwrap();

        let sd = StaticDiscovery::new(vec![addr1, addr2]);
        assert!(sd.leader_hint().is_none());

        sd.set_leader(addr1);
        assert_eq!(sd.leader_hint(), Some(addr1));

        // 切换 Leader
        sd.set_leader(addr2);
        assert_eq!(sd.leader_hint(), Some(addr2));

        sd.clear_leader();
        assert!(sd.leader_hint().is_none());
    }

    #[test]
    fn test_static_discovery_from_strings() {
        let peer_strs = vec![
            "10.0.0.1:50051".to_string(),
            "10.0.0.2:50052".to_string(),
            "invalid".to_string(), // 应被跳过
        ];
        let sd = StaticDiscovery::from_strings(&peer_strs);
        assert_eq!(sd.peers().len(), 2);
    }
}
