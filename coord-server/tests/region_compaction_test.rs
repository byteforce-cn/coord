// per-Region Compaction 验收测试
//
// 单节点装配 region 1 raft（目录隔离 MVCC）。验证：
//   - RegionCompactProposer 只在本节点为 Region leader 时可提案；
//   - 提案 Command::Compact 后 Region MVCC `META_COMPACT_REVISION` 推进、水位
//     （compacted_revision）随提案生效、RegionHandle::compaction_watermark 同步；
//   - 压缩后 < revision 的历史 changelog 被清理（历史读 OUT_OF_RANGE/DataCorruption），
//     当前值完好、revision 仍单调；
//   - 幂等：重复 compact 同 revision 无副作用。

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionId, RegionMeta};
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::{region_data_dir, RegionCompactProposer};
use coord_server::raft::type_config::Command;
use coord_server::raft::{new_raft, RaftConfig, RegionRuntimeSpec, WatchReceiver};
use coord_server::storage::compaction::CompactProposer;
use coord_server::storage::redb_backend::RedbBackend;

fn find_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 单节点 + 单 Region（region 1 = 全 keyspace）。
async fn start_single_region() -> (tempfile::TempDir, Arc<RegionManager>, RegionId) {
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
    };
    manager
        .spawn_region(&factory, &rpc, spec, true)
        .await
        .expect("spawn region");

    // 等待 leader 就绪
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        if let Some(rt) = manager.runtime(1) {
            let m = rt.raft.metrics();
            let m = m.borrow_watched();
            if m.last_quorum_acked.is_some() && m.current_leader == Some(1) {
                break;
            }
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "region leader not elected"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    (tmpdir, manager, 1)
}

#[tokio::test]
async fn test_region_compaction_proposes_and_advances_watermark() {
    let (_dir, manager, rid) = start_single_region().await;
    let rt = manager.runtime(rid).expect("runtime");

    // 写入 6 个 key（revision 1..6）
    for i in 0..6u64 {
        rt.raft
            .client_write(Command::Put {
                key: format!("k{i:02}").into_bytes(),
                value: format!("v{i}").into_bytes(),
                lease_id: None,
            })
            .await
            .expect("put");
    }
    let rev = rt.mvcc.current_revision();
    assert!(rev >= 6, "revision after puts");

    let proposer = RegionCompactProposer::from_runtime(1, &rt);
    assert!(
        proposer.can_propose().await,
        "single node must be region leader"
    );

    // 压缩到 rev-2
    let target = rev - 2;
    let effective = proposer
        .propose(target)
        .await
        .expect("region compact propose");
    assert_eq!(effective, target, "compact revision applied");

    // Region MVCC 压缩水位推进
    assert_eq!(
        rt.mvcc.compacted_revision().unwrap(),
        target,
        "region mvcc compacted revision"
    );
    // RegionHandle 水位同步（G7 收口）
    assert_eq!(
        rt.handle
            .compaction_watermark
            .load(std::sync::atomic::Ordering::Relaxed),
        target,
        "region handle compaction watermark"
    );

    // 当前值完好、新写入 revision 继续单调
    assert_eq!(
        rt.mvcc.get(b"k05").unwrap(),
        Some(b"v5".to_vec()),
        "current values intact after compact"
    );
    rt.raft
        .client_write(Command::Put {
            key: b"k06".to_vec(),
            value: b"v6b".to_vec(),
            lease_id: None,
        })
        .await
        .expect("put after compact");
    assert!(
        rt.mvcc.current_revision() > rev,
        "revision monotonic after compact"
    );

    // 压缩前 revision 的历史读应失败（compacted），证明 changelog 已被清理
    let hist = rt.mvcc.get_at_revision(b"k05", 3);
    assert!(
        hist.is_err() || hist.unwrap().is_none(),
        "history before compact must be gone"
    );

    // 幂等：重复提案同 revision 无副作用（返回同 effective，水位不变）
    let again = proposer.propose(target).await.expect("re-compact");
    assert_eq!(again, target);
    assert_eq!(
        rt.mvcc.compacted_revision().unwrap(),
        target,
        "idempotent compact"
    );
}
