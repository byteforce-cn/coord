//! KV-T G1（#138）revision 解耦验收：
//!
//! v2 Region 按显式 `commit_ts` 记 revision（不是 raft log index）；回退的
//! `commit_ts` 返回 `CommitRejected` 且不让 raft apply 返回 `Err`，之后仍可写。

use std::sync::Arc;
use std::time::Duration;

use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
use coord_server::raft::type_config::{Command, Response};
use coord_server::raft::{RaftConfig, WatchReceiver};
use coord_server::storage::mvcc::RevisionMode;

fn find_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn put_v2(key: &[u8], commit_ts: u64) -> Command {
    Command::PutV2 {
        key: key.to_vec(),
        value: b"v".to_vec(),
        lease_id: None,
        commit_ts,
    }
}

#[tokio::test]
async fn test_v2_region_uses_commit_ts_revision() {
    let tmpdir = tempfile::tempdir().unwrap();
    let base = tmpdir.path().to_path_buf();

    let factory = RaftNetworkFactoryImpl::new(1);
    let raft_addr = format!("127.0.0.1:{}", find_port());
    factory.register_node(1, raft_addr.clone());

    let manager = Arc::new(RegionManager::new(1));
    let rpc = RaftRpcService::new();
    let spec = RegionRuntimeSpec {
        meta: RegionMeta {
            region_id: 1,
            start_key: vec![],
            end_key: vec![],
            epoch: RegionEpoch::initial(),
            peers: vec![Peer {
                node_id: 1,
                raft_addr: raft_addr.clone(),
                role: PeerRole::Voter,
            }],
            approximate_size: 0,
            approximate_keys: 0,
        },
        data_dir: region_data_dir(&base, 1),
        raft_config: Arc::new(RaftConfig {
            heartbeat_interval: 200,
            election_timeout_min: 800,
            election_timeout_max: 1500,
            ..Default::default()
        }),
        object_store: None,
        revision_mode: RevisionMode::CommitTs,
    };
    manager
        .spawn_region(&factory, &rpc, spec, true)
        .await
        .expect("spawn region");

    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(rt) = manager.runtime(1) {
            let m = rt.raft.metrics();
            let m = m.borrow_watched();
            if m.last_quorum_acked.is_some() && m.current_leader == Some(1) {
                break;
            }
        }
        assert!(tokio::time::Instant::now() < deadline, "region leader");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    let rt = manager.runtime(1).expect("runtime");

    let resp = rt
        .raft
        .client_write(put_v2(b"k", 1_000))
        .await
        .expect("put v2");
    assert!(matches!(resp.response(), Response::Put { revision: 1_000 }));
    assert_eq!(rt.mvcc.current_revision(), 1_000);
    assert_ne!(
        rt.mvcc.get_applied_log_id().unwrap().unwrap().index,
        1_000,
        "revision 不得等于 raft log index"
    );

    // 回退 commit_ts：用户级拒绝，不得让 raft apply Err（D11.5）。
    let resp = rt
        .raft
        .client_write(put_v2(b"k2", 999))
        .await
        .expect("rejection must not fail raft");
    assert!(matches!(
        resp.response(),
        Response::CommitRejected {
            commit_ts: 999,
            current_revision: 1_000
        }
    ));
    assert_eq!(rt.mvcc.current_revision(), 1_000);
    assert_eq!(rt.mvcc.get(b"k2").unwrap(), None);

    // 拒绝之后仍可继续写（applied 已推进，客户端没有卡住）。
    let resp = rt
        .raft
        .client_write(put_v2(b"k", 1_001))
        .await
        .expect("post-reject write");
    assert!(matches!(resp.response(), Response::Put { revision: 1_001 }));
    assert_eq!(rt.mvcc.current_revision(), 1_001);
}
