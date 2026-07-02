// coord-core: 成员发现适配器
//
// 定义 MemberDiscovery trait 和 DiscoveryEvent 类型，统一静态配置和 Gossip 协议。
// 参见 docs/client-agent-architecture.md §5。

use std::net::SocketAddr;

/// 成员发现事件（Gossip 实现时生效，Static 实现不产生事件）
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DiscoveryEvent {
    /// 新节点加入集群
    PeerAdded(SocketAddr),
    /// 节点离开集群
    PeerRemoved(SocketAddr),
    /// Leader 发生变更
    LeaderChanged(SocketAddr),
}

/// 成员发现适配器：统一静态配置和 Gossip 协议
///
/// # Object Safety
///
/// 所有方法接收 `&self`，因此该 trait 是 object-safe 的，可以
/// 作为 `Box<dyn MemberDiscovery>` 或 `Arc<dyn MemberDiscovery>` 使用。
pub trait MemberDiscovery: Send + Sync {
    /// 获取当前已知的所有 Server 节点地址
    fn peers(&self) -> Vec<SocketAddr>;

    /// 获取当前 Raft Leader 地址（如果已知）
    fn leader_hint(&self) -> Option<SocketAddr>;

    /// 更新 Leader 地址
    fn set_leader(&self, addr: SocketAddr);

    /// 清除 Leader 缓存（收到 NotLeader 后调用）
    fn clear_leader(&self);

    /// 订阅成员变更事件（Gossip 实现时生效，Static 返回 None）
    fn watch_changes(&self) -> Option<tokio::sync::mpsc::Receiver<DiscoveryEvent>> {
        None
    }

    /// 健康检查：至少已知一个节点
    fn is_healthy(&self) -> bool;
}
