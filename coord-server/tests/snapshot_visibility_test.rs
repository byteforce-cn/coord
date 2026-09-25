// F-69 验收测试（L2 进程内，真实 openraft 机制）：
// 快照构建后，复制路径读取的 `Raft::get_snapshot()`（经 SM worker 的
// `GetSnapshot` → 主实例 `get_current_snapshot()`）必须能看到刚构建的快照。
//
// 修复前形态：`build_snapshot()` 在 `get_snapshot_builder()` 的克隆上执行，
// 构建结果只写进克隆自己的内存槽 ⇒ 主实例 `get_current_snapshot()` 永远
// 返回 `None` ⇒ 落后 follower 需要快照时 openraft 将「无快照可送」升级为
// 存储错误，RaftCore 进入 fatal（全集群写入永久失败，2026-09-25 2h soak
// 实测 85 分钟失能）。
//
// 判据分两段：
// 1. 小快照策略（LogsSinceLast(20)）下写 40 条 ⇒ 轮询 `get_snapshot()` 必须
//    在超时前返回 `Some`（修复前恒为 `None`）；
// 2. 快照出现后继续 `client_write` 必须成功（节点未因快照发送需求进入 fatal）。

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::RaftNetworkFactoryImpl;
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::{Command, Response};
use coord_server::raft::{apply_tuning, new_basic_node, new_raft, RaftConfig, RaftTuning};
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

#[tokio::test]
async fn test_get_snapshot_sees_built_snapshot_via_real_raft() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();

    let backend =
        RedbBackend::open(&data_dir, &StorageConfig::default()).expect("open redb backend");
    let mvcc = Arc::new(MvccStorage::new(backend).expect("create mvcc"));
    let tracker = Arc::new(SnapshotTracker::default());

    let log_store = LogStore::new(&data_dir)
        .await
        .expect("create raft log store")
        .with_snapshot_tracker(Arc::clone(&tracker));
    let sm_store = StateMachineStore::new(
        Arc::clone(&mvcc),
        data_dir.join("snapshots"),
        Arc::clone(&tracker),
    );

    let raft_addr = format!("127.0.0.1:{}", find_port());
    let network_factory = RaftNetworkFactoryImpl::new(1);
    network_factory.register_node(1, raft_addr.clone());

    // 小快照阈值：20 条日志即触发一次快照构建（默认 5000 会让测试过慢）
    let mut raft_config = RaftConfig::default();
    apply_tuning(
        &mut raft_config,
        &RaftTuning {
            snapshot_logs_since_last: Some(20),
            ..RaftTuning::default()
        },
    );

    let raft = new_raft(
        1,
        Arc::new(raft_config),
        network_factory,
        log_store,
        sm_store,
    )
    .await
    .expect("create raft instance");

    let mut members = BTreeMap::new();
    members.insert(1, new_basic_node(&raft_addr));
    raft.initialize(members).await.expect("raft initialize");

    // 等待领导权
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline && raft.current_leader().await != Some(1) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        raft.current_leader().await,
        Some(1),
        "single node must elect itself"
    );

    // 写 40 条 ⇒ 触发快照构建（≥20 条处触发一次，≥40 条处再触发一次）
    for i in 0..40u64 {
        let cmd = Command::Put {
            key: format!("/f69/k{i:03}").into_bytes(),
            value: format!("v{i}").into_bytes(),
            lease_id: None,
        };
        raft.client_write(cmd)
            .await
            .expect("client_write before snapshot");
    }

    // 判据 1：构建完成后，复制路径读取的快照必须可见（修复前恒 None ⇒ 超时）
    let deadline = tokio::time::Instant::now() + Duration::from_secs(60);
    let snapshot = loop {
        match raft.get_snapshot().await {
            Ok(Some(s)) => break s,
            Ok(None) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "get_snapshot 在 60s 内始终为 None：快照构建结果对主状态机不可见（F-69 回归）"
                );
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            Err(e) => panic!("get_snapshot 返回错误（SM worker 故障）: {e}"),
        }
    };
    assert!(
        snapshot.meta.last_log_id.is_some(),
        "快照 meta 必须携带 last_log_id"
    );
    assert!(
        !snapshot.snapshot.get_ref().is_empty(),
        "快照数据不得为空字节"
    );

    // 判据 2：快照需求出现后节点仍可继续写入（不得进入 fatal）
    let resp = raft
        .client_write(Command::Put {
            key: b"/f69/after-snapshot".to_vec(),
            value: b"ok".to_vec(),
            lease_id: None,
        })
        .await
        .expect("client_write after snapshot must not hit Fatal(StorageError)");
    assert!(matches!(resp.response(), Response::Put { .. }));
}
