// TDD: discovery trait 测试
//
// Phase A1 — RED stage: 在 MemberDiscovery trait 还不存在时此测试应编译失败。

use std::net::SocketAddr;

use coord_core::discovery::{DiscoveryEvent, MemberDiscovery};

/// 一个最小化的存根实现，用于验证 trait 是否可以实现。
struct StubDiscovery {
    peers: Vec<SocketAddr>,
    leader: std::sync::RwLock<Option<SocketAddr>>,
}

impl MemberDiscovery for StubDiscovery {
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

#[test]
fn trait_can_be_implemented() {
    let addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();
    let d = StubDiscovery {
        peers: vec![addr],
        leader: std::sync::RwLock::new(None),
    };

    assert!(d.is_healthy());
    assert_eq!(d.peers().len(), 1);
    assert!(d.leader_hint().is_none());

    d.set_leader(addr);
    assert_eq!(d.leader_hint(), Some(addr));

    d.clear_leader();
    assert!(d.leader_hint().is_none());

    // Static discovery returns None for watch_changes
    assert!(d.watch_changes().is_none());
}

#[test]
fn discovery_event_variants() {
    let addr: SocketAddr = "10.0.0.1:50051".parse().unwrap();

    let added = DiscoveryEvent::PeerAdded(addr);
    let removed = DiscoveryEvent::PeerRemoved(addr);
    let changed = DiscoveryEvent::LeaderChanged(addr);

    assert!(matches!(added, DiscoveryEvent::PeerAdded(_)));
    assert!(matches!(removed, DiscoveryEvent::PeerRemoved(_)));
    assert!(matches!(changed, DiscoveryEvent::LeaderChanged(_)));
}

#[test]
fn trait_is_object_safe() {
    // 验证 trait 可以作为 trait object 使用
    let addr: SocketAddr = "127.0.0.1:50051".parse().unwrap();
    let d = StubDiscovery {
        peers: vec![addr],
        leader: std::sync::RwLock::new(None),
    };
    let _boxed: Box<dyn MemberDiscovery> = Box::new(d);
}
