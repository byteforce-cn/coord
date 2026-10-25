//! 重启装配：**已 initialize 但从未提交过条目**的 Region 组必须能直接复用。
//!
//! 多副本组在 quorum 不在线时 `initialize()` 只把成员表写进日志、把 vote 落盘，
//! `committed` 元数据行要等第一条条目提交后才有。重启时若按 `is_initialized()`
//! （只看 committed）判断，会把这种组当成「新组」再次 initialize：
//! - openraft 返回 `NotAllowed`；
//! - 失败前创建的 core 仍持有 store 锁，同进程重试撞 `Database already open`，
//!   于是启动期该 Region 组（或 Commit Log shard 组）装配失败。
//!
//! 判据：同一数据目录第二次装配（仍 `initialize=true`）必须一次成功。

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;

use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta};
use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
use coord_server::raft::region::RegionManager;
use coord_server::raft::region_runtime::region_data_dir;
use coord_server::raft::{RaftConfig, RegionRuntimeSpec};
use coord_server::storage::mvcc::RevisionMode;

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn raft_addresses() -> BTreeMap<u64, String> {
    (1..=3u64)
        .map(|id| (id, format!("127.0.0.1:{}", find_port())))
        .collect()
}

fn region_meta(raft_addrs: &BTreeMap<u64, String>) -> RegionMeta {
    RegionMeta {
        region_id: 1,
        start_key: Vec::new(),
        end_key: Vec::new(),
        epoch: RegionEpoch::initial(),
        peers: raft_addrs
            .iter()
            .map(|(node_id, raft_addr)| Peer {
                node_id: *node_id,
                raft_addr: raft_addr.clone(),
                role: PeerRole::Voter,
            })
            .collect(),
        approximate_size: 0,
        approximate_keys: 0,
    }
}

fn spec(data_dir: std::path::PathBuf, raft_addrs: &BTreeMap<u64, String>) -> RegionRuntimeSpec {
    RegionRuntimeSpec {
        meta: region_meta(raft_addrs),
        data_dir,
        raft_config: Arc::new(RaftConfig {
            heartbeat_interval: 200,
            // 单节点在线时不要因为选举超时把组推进到别的状态：本用例只关心
            // 「initialize 过但未提交」这一持久化形态。
            election_timeout_min: 2_000,
            election_timeout_max: 4_000,
            ..Default::default()
        }),
        object_store: None,
        revision_mode: RevisionMode::LogIndex,
    }
}

#[tokio::test]
async fn test_restart_reuses_an_initialized_but_uncommitted_region_group() {
    let dir = tempfile::tempdir().unwrap();
    let base = dir.path().to_path_buf();
    let raft_addrs = raft_addresses();

    // 第一次装配：只有 node 1 在线，成员表进日志但无法提交（quorum 缺失）。
    {
        let factory = RaftNetworkFactoryImpl::new(1);
        for (id, addr) in &raft_addrs {
            factory.register_node(*id, addr.clone());
        }
        let rpc = RaftRpcService::new();
        let manager = Arc::new(RegionManager::new(1));
        let runtime = manager
            .spawn_region(
                &factory,
                &rpc,
                spec(region_data_dir(&base, 1), &raft_addrs),
                true,
            )
            .await
            .expect("first assembly must initialize the group");

        assert!(
            !runtime.raft_log_store.is_initialized().unwrap(),
            "前提：quorum 不在线时 initialize 只写成员表/vote，没有 committed 行"
        );
        let mut store = runtime.raft_log_store.clone();
        assert!(
            store.has_prior_state().await.unwrap(),
            "initialize 之后必须被判定为「已有 raft 状态」，否则重启会重复 initialize"
        );

        let _ = runtime.raft.shutdown().await;
    }

    // 第二次装配：同一数据目录 + 仍 bootstrap。重复 initialize 会让 openraft
    // 拒绝，且失败路径遗留的 core 会锁住 store.db。
    let factory = RaftNetworkFactoryImpl::new(1);
    for (id, addr) in &raft_addrs {
        factory.register_node(*id, addr.clone());
    }
    let rpc = RaftRpcService::new();
    let manager = Arc::new(RegionManager::new(1));
    let runtime = manager
        .spawn_region(
            &factory,
            &rpc,
            spec(region_data_dir(&base, 1), &raft_addrs),
            true,
        )
        .await
        .expect("restart must reuse the existing group instead of re-initializing it");
    let _ = runtime.raft.shutdown().await;
}
