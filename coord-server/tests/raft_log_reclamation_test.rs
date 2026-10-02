// 受控实验（L2 进程内，真实 openraft + 真实 redb 日志存储）——ADR-0004 的
// 「有界性口径」验收：缺省语义下日志尾部条数上界 ≈
// `max_in_snapshot_log_to_keep + snapshot_logs_since_last`。
//
// 用例 A（回收开启：节奏 20、窗口 0）：持续写入 160 条 ⇒ 尾部有界（≤ 4×节奏，
// 远小于线性值）；再写 160 条（N 倍增）⇒ 尾部仍保持同一上界。
// 用例 B（负控制语义：`snapshot_logs_since_last = 0` ⇒ `Never`）：不产生快照 ⇒
// 不回收，`purged` 恒为 None、日志随写入单调增长（与 A 的有界形成对照）。
//
// 开发期负控制（实跑验证，结论记录于对应 PR）：
//   ① `apply_tuning` 忽略节奏、策略恒为 `Never`（等价「关闭回收开关」）⇒ `purged`
//      停滞 ⇒ 用例 A 的轮询断言必红（实测 90s fail-loud）；
//   ② 移除 `max_in_snapshot_log_to_keep` 透传（窗口回落到缺省 1000）⇒ 回收不推进
//      （需再攒满 1000 条才到回收点）⇒ 用例 A 必红（实测 last=161 purged=None）。

use std::collections::BTreeMap;
use std::net::TcpListener;
use std::sync::Arc;
use std::time::Duration;

use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_server::raft::log_store::LogStore;
use coord_server::raft::network::RaftNetworkFactoryImpl;
use coord_server::raft::state_machine::StateMachineStore;
use coord_server::raft::type_config::Command;
use coord_server::raft::{
    apply_tuning, new_basic_node, new_raft, CoordRaft, RaftConfig, RaftLogStorage, RaftTuning,
};
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::storage::snapshot::SnapshotTracker;

/// 快照节奏（条）：每 20 条提交触发一次快照构建（缺省 5000 会让实验过慢）。
const CADENCE: u64 = 20;
/// 保留窗口：0 = 允许回收紧贴快照点（松开与快照的距离，便于快速收敛）。
const KEEP: u64 = 0;
/// 每批写入条数。
const WRITES: u64 = 160;
/// 有界断言上界：4×节奏（线性失效时尾部 ≈ WRITES，必超界）。
const TAIL_BOUND: u64 = 4 * CADENCE;

struct Node {
    /// 数据目录在用例期间必须存活（drop 即删除）。
    _tmp: tempfile::TempDir,
    raft: CoordRaft,
    /// 与 raft 内部日志存储共享同一 redb 实例的只读句柄（检查回收水位用）。
    reader: LogStore,
}

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// 起一个单节点真实 raft，返回可检查日志水位的句柄。
async fn start_node(
    snapshot_logs_since_last: Option<u64>,
    max_in_snapshot_log_to_keep: Option<u64>,
) -> Node {
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
    let reader = log_store.clone();

    let sm_store = StateMachineStore::new(
        Arc::clone(&mvcc),
        data_dir.join("snapshots"),
        Arc::clone(&tracker),
    );

    let raft_addr = format!("127.0.0.1:{}", find_port());
    let network_factory = RaftNetworkFactoryImpl::new(1);
    network_factory.register_node(1, raft_addr.clone());

    let mut raft_config = RaftConfig::default();
    apply_tuning(
        &mut raft_config,
        &RaftTuning {
            snapshot_logs_since_last,
            max_in_snapshot_log_to_keep,
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

    // 就绪等待 fail-loud（与 R-TST-21 同口径）：10s 内必须选出自己。
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline && raft.current_leader().await != Some(1) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert_eq!(
        raft.current_leader().await,
        Some(1),
        "single node must elect itself"
    );

    Node {
        _tmp: tmp,
        raft,
        reader,
    }
}

async fn write_entries(raft: &CoordRaft, start: u64, count: u64) {
    for i in start..start + count {
        let cmd = Command::Put {
            key: format!("/adr4/k{i:04}").into_bytes(),
            value: format!("v{i}").into_bytes(),
            lease_id: None,
        };
        raft.client_write(cmd).await.expect("client_write");
    }
}

/// 返回（最后日志 index，已回收水位 index，尾部保留条数）。
async fn log_tail(reader: &mut LogStore) -> (u64, Option<u64>, u64) {
    let st = reader.get_log_state().await.expect("get_log_state");
    let last = st.last_log_id.as_ref().map(|l| l.index).unwrap_or(0);
    let purged = st.last_purged_log_id.as_ref().map(|l| l.index);
    let retained = last.saturating_sub(purged.unwrap_or(0));
    (last, purged, retained)
}

/// 轮询直至回收水位（purged）达到 target；超时给出 fail-loud 诊断。
async fn wait_purged_at_least(node: &mut Node, target: u64, secs: u64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(secs);
    loop {
        let (last, purged, retained) = log_tail(&mut node.reader).await;
        if purged.map(|p| p >= target).unwrap_or(false) {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "{secs}s 内回收未推进到 {target}（last={last} purged={purged:?} retained={retained}）——\
             回收路径失效时先在这一步暴露"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn log_entries_stay_bounded_under_sustained_writes() {
    let mut node = start_node(Some(CADENCE), Some(KEEP)).await;

    write_entries(&node.raft, 0, WRITES).await;
    wait_purged_at_least(&mut node, WRITES - 3 * CADENCE, 90).await;

    let (last, purged, retained) = log_tail(&mut node.reader).await;
    assert!(purged.is_some(), "回收开启时 purged 不得为 None");
    assert!(last >= WRITES, "写入应全部落盘：last={last}");
    assert!(
        retained <= TAIL_BOUND,
        "日志尾部必须有界（不随写入线性增长）：last={last} purged={purged:?} \
         retained={retained} bound={TAIL_BOUND}"
    );

    // N 倍增：再写一批，尾部必须仍保持同一上界（回收失效时此处 ≈ 2×WRITES）。
    write_entries(&node.raft, WRITES, WRITES).await;
    wait_purged_at_least(&mut node, 2 * WRITES - 3 * CADENCE, 90).await;

    let (last2, purged2, retained2) = log_tail(&mut node.reader).await;
    assert!(
        retained2 <= TAIL_BOUND,
        "两批写入后尾部仍有界：last={last2} purged={purged2:?} \
         retained={retained2} bound={TAIL_BOUND}"
    );
}

#[tokio::test]
async fn negative_control_never_policy_means_no_reclamation() {
    // `snapshot_logs_since_last = 0` 是显式选择的手动快照模式：不产生新快照 ⇒
    // 无回收路径（上游 `purged ≤ snapshot` 不变量）。
    let mut node = start_node(Some(0), None).await;

    write_entries(&node.raft, 0, 60).await;
    // 留出观察窗口：任何"漏网"的自动回收路径都会在此暴露。
    tokio::time::sleep(Duration::from_secs(1)).await;

    let (last_1, purged_1, retained_1) = log_tail(&mut node.reader).await;
    assert!(
        purged_1.is_none(),
        "Never 策略下不得发生任何回收：purged={purged_1:?}"
    );
    assert!(last_1 >= 60, "日志单调增长：last={last_1}");
    assert_eq!(retained_1, last_1, "无回收 ⇒ 尾部 = 全部日志");
    assert!(
        node.raft
            .get_snapshot()
            .await
            .expect("get_snapshot")
            .is_none(),
        "Never 策略下不应产生快照"
    );

    // N 倍增：无界增长（与用例 A 的有界形成负控制对照）。
    write_entries(&node.raft, 60, 60).await;
    let (last_2, purged_2, _) = log_tail(&mut node.reader).await;
    assert!(
        purged_2.is_none(),
        "Never 策略下不得发生任何回收：purged={purged_2:?}"
    );
    assert!(last_2 >= 120, "日志随写入单调增长（无回收）：last={last_2}");
}
