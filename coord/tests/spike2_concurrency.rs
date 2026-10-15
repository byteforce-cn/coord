// Spike 2 —— 单写者并发演练（真实 Raft 上的 CAS + 实例租约 + fencing）
//
// 本文件是 `spike/2026-10/spike-2-concurrency/` 的可执行证据：每个 `#[tokio::test]`
// 对应 README.md 证据索引里的一条结论。全部跑在**进程内真实单节点 Raft**上
// （`common::start_test_server()`：真实 KV/Txn/Lease gRPC + 真实状态机）。
//
// 复现：`CARGO_BUILD_JOBS=3 cargo test -p coord --test spike2_concurrency -- --nocapture`

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use coord_agent::services::workflow_lease::KvInstanceLease;
use coord_agent::services::workflow_store::KvWorkflowStore;
use coord_core::workflow::engine::WorkflowExecutor;
use coord_core::workflow::expression::ExpressionEvaluator;
use coord_core::workflow::model::{
    CallTask, CallType, Document, InstanceStatus, NamedTask, Task, WorkflowDefinition,
    WorkflowInstance,
};
use coord_core::workflow::ports::{
    DispatchResult, NoopEventProvider, SystemClock, TaskDispatcher, WorkflowStore,
};
use coord_core::workflow::runtime::WorkflowRuntime;
use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::{KeyValue, PutRequest, RangeRequest};
use coord_proto::lease::lease_client::LeaseClient;
use coord_proto::lease::{LeaseGrantRequest, LeaseRevokeRequest};
use coord_proto::txn::compare::{CompareResult, Target, TargetValue};
use coord_proto::txn::request_op::Op;
use coord_proto::txn::txn_client::TxnClient;
use coord_proto::txn::{Compare, RequestOp, TxnRequest};
use tonic::transport::Channel;

// ─── 连接与 KV/Txn 夹具 ───

async fn connect(addr: &str) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .expect("valid endpoint")
        .connect()
        .await
        .expect("connect to test server")
}

fn compare_mod_rev(key: &[u8], expected: i64) -> Compare {
    Compare {
        result: CompareResult::Equal as i32,
        target: Target::ModRev as i32,
        key: key.to_vec(),
        target_value: Some(TargetValue::ModRevision(expected)),
    }
}

fn compare_version(key: &[u8], expected: i64) -> Compare {
    Compare {
        result: CompareResult::Equal as i32,
        target: Target::Version as i32,
        key: key.to_vec(),
        target_value: Some(TargetValue::Version(expected)),
    }
}

fn compare_value(key: &[u8], expected: &[u8]) -> Compare {
    Compare {
        result: CompareResult::Equal as i32,
        target: Target::Value as i32,
        key: key.to_vec(),
        target_value: Some(TargetValue::Value(expected.to_vec())),
    }
}

fn put_op(key: &[u8], value: &[u8], lease_id: i64) -> RequestOp {
    RequestOp {
        op: Some(Op::RequestPut(PutRequest {
            key: key.to_vec(),
            value: value.to_vec(),
            lease_id,
            prev_kv: false,
            request_id: vec![],
        })),
    }
}

async fn put_raw(kv: &mut KvClient<Channel>, key: &[u8], value: &[u8], lease_id: i64) -> i64 {
    kv.put(PutRequest {
        key: key.to_vec(),
        value: value.to_vec(),
        lease_id,
        prev_kv: false,
        request_id: vec![],
    })
    .await
    .expect("put")
    .into_inner()
    .revision
}

/// 线性一致读单 key，返回完整 KeyValue（含 mod_revision / version / lease_id）。
async fn read_kv(kv: &mut KvClient<Channel>, key: &[u8]) -> Option<KeyValue> {
    kv.range(RangeRequest {
        key: key.to_vec(),
        range_end: vec![],
        limit: 0,
        revision: 0,
        keys_only: false,
        count_only: false,
    })
    .await
    .expect("range")
    .into_inner()
    .kvs
    .into_iter()
    .next()
}

/// 有界轮询直到 key 不存在（替代可变捕获的 `wait_until`：读需要 `&mut kv`）。
/// 超时即断言失败（不得把「没等到」当作「通过」）。
async fn await_key_gone(kv: &mut KvClient<Channel>, key: &[u8], what: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while read_kv(kv, key).await.is_some() {
        assert!(tokio::time::Instant::now() < deadline, "not met: {what}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

async fn txn_cas(
    txn: &mut TxnClient<Channel>,
    compares: Vec<Compare>,
    puts: Vec<RequestOp>,
) -> bool {
    txn.txn(TxnRequest {
        compare: compares,
        success: puts,
        failure: vec![],
        request_id: vec![],
    })
    .await
    .expect("txn")
    .into_inner()
    .succeeded
}

/// 单个 CAS 写：Compare(key ModRev == expected) → Put(key=value)。
async fn cas_put(
    txn: &mut TxnClient<Channel>,
    key: &[u8],
    expected_mod_rev: i64,
    value: &[u8],
) -> bool {
    txn_cas(
        txn,
        vec![compare_mod_rev(key, expected_mod_rev)],
        vec![put_op(key, value, 0)],
    )
    .await
}

// ─── 场景 1：两个写者同修订号竞争，恰一个赢 ───

#[tokio::test(flavor = "multi_thread")]
async fn cas_two_writers_same_rev_one_wins() {
    let (addr, _sd, _g, _r, _tmp) = common::start_test_server().await;
    let ch = connect(&addr).await;
    let mut kv = KvClient::new(ch.clone());
    let txn = TxnClient::new(ch.clone());

    let key = b"/_workflow/v3/instances/cas-writers";
    put_raw(&mut kv, key, b"v0", 0).await;
    let before = read_kv(&mut kv, key).await.expect("initial value");
    assert_eq!(before.value, b"v0");
    assert_eq!(before.version, 1, "fresh key must have version 1");

    // 两个并发 CAS，均以 before.mod_revision 为期望。
    let mut t1 = txn.clone();
    let mut t2 = txn.clone();
    let (w1, w2) = tokio::join!(
        cas_put(&mut t1, key, before.mod_revision, b"writer-A"),
        cas_put(&mut t2, key, before.mod_revision, b"writer-B"),
    );

    assert!(
        w1 ^ w2,
        "exactly one CAS must win (writer-A={w1}, writer-B={w2})"
    );

    let after = read_kv(&mut kv, key).await.expect("final value");
    if w1 {
        assert_eq!(after.value, b"writer-A", "winner's value must persist");
    } else {
        assert_eq!(after.value, b"writer-B", "winner's value must persist");
    }
    // 只有一次成功写生效 ⇒ mod_revision 只推进一次 ⇒ version 只 +1。
    assert_eq!(
        after.version,
        before.version + 1,
        "exactly one write must have taken effect"
    );
    assert!(
        after.mod_revision > before.mod_revision,
        "mod_revision must advance"
    );
}

// ─── 场景 2：旧修订号 CAS 被拒绝，不覆盖新值 ───

#[tokio::test(flavor = "multi_thread")]
async fn cas_stale_revision_rejected() {
    let (addr, _sd, _g, _r, _tmp) = common::start_test_server().await;
    let ch = connect(&addr).await;
    let mut kv = KvClient::new(ch.clone());
    let mut txn = TxnClient::new(ch.clone());

    let key = b"/_workflow/v3/instances/cas-stale";
    put_raw(&mut kv, key, b"v1", 0).await;
    let rev1 = read_kv(&mut kv, key).await.expect("v1").mod_revision;

    // 另一写者推进：v1 → v2。
    put_raw(&mut kv, key, b"v2", 0).await;
    let cur = read_kv(&mut kv, key).await.expect("v2");
    assert_eq!(cur.value, b"v2");

    // 旧修订号（rev1）CAS 必须失败，且不覆盖 v2。
    let stale_ok = cas_put(&mut txn, key, rev1, b"stale-overwrite").await;
    assert!(!stale_ok, "stale CAS must be rejected");

    let after = read_kv(&mut kv, key).await.expect("value after stale CAS");
    assert_eq!(after.value, b"v2", "stale CAS must not overwrite");
    assert_eq!(
        after.mod_revision, cur.mod_revision,
        "stale CAS must not advance revision"
    );
}

// ─── 场景 3：实例租约互斥（LeaseGrant + create-if-absent） ───

/// 一次租约获取：Grant → Txn(Compare(Version==0) → Put(key, token, lease_id))。
/// 失败方撤销自己刚授的空租约，返回 `None`。
async fn try_acquire_lease(
    lease: &mut LeaseClient<Channel>,
    txn: &mut TxnClient<Channel>,
    key: &[u8],
    token: &[u8],
) -> Option<i64> {
    let lease_id = lease
        .lease_grant(LeaseGrantRequest { ttl: 30, id: 0 })
        .await
        .expect("grant")
        .into_inner()
        .id;
    let got = txn_cas(
        txn,
        vec![compare_version(key, 0)],
        vec![put_op(key, token, lease_id)],
    )
    .await;
    if got {
        Some(lease_id)
    } else {
        lease
            .lease_revoke(LeaseRevokeRequest { id: lease_id })
            .await
            .expect("revoke loser's empty lease");
        None
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn lease_mutex_exclusive() {
    let (addr, _sd, _g, _r, _tmp) = common::start_test_server().await;
    let ch = connect(&addr).await;
    let mut kv = KvClient::new(ch.clone());

    let key = b"/_workflow/v3/drivers/lease-mutex";
    let mut l1 = LeaseClient::new(ch.clone());
    let mut l2 = LeaseClient::new(ch.clone());
    let mut t1 = TxnClient::new(ch.clone());
    let mut t2 = TxnClient::new(ch.clone());

    let (a, b) = tokio::join!(
        try_acquire_lease(&mut l1, &mut t1, key, b"driver-A"),
        try_acquire_lease(&mut l2, &mut t2, key, b"driver-B"),
    );

    assert!(
        a.is_some() ^ b.is_some(),
        "exactly one driver must acquire the instance lease (A={a:?}, B={b:?})"
    );

    let holder = read_kv(&mut kv, key).await.expect("lease key exists");
    assert_eq!(holder.version, 1, "lease key created exactly once");
    let winner_lease_id = a.or(b).expect("winner lease id");
    assert_eq!(
        holder.lease_id, winner_lease_id,
        "lease key must be bound to the winner's lease"
    );
    assert!(
        holder.value == b"driver-A" || holder.value == b"driver-B",
        "lease key must carry the winner token"
    );
}

// ─── 场景 4：租约撤销/过期后重新竞争 ───

#[tokio::test(flavor = "multi_thread")]
async fn lease_expiry_reacquire() {
    let (addr, _sd, _g, _r, _tmp) = common::start_test_server().await;
    let ch = connect(&addr).await;
    let mut kv = KvClient::new(ch.clone());
    let mut lease = LeaseClient::new(ch.clone());
    let mut txn = TxnClient::new(ch.clone());

    let key = b"/_workflow/v3/drivers/lease-expiry";

    // 首个持有者：grant + create-if-absent。
    let first = try_acquire_lease(&mut lease, &mut txn, key, b"driver-1")
        .await
        .expect("first holder acquires");
    assert!(read_kv(&mut kv, key).await.is_some());

    // 撤销租约 → 服务端级联删除绑定 Key。
    lease
        .lease_revoke(LeaseRevokeRequest { id: first })
        .await
        .expect("revoke holder lease");
    await_key_gone(&mut kv, key, "lease key cascaded-deleted after revoke").await;

    // 第二个获取者成功（版本从 0 重新开始）。
    let second = try_acquire_lease(&mut lease, &mut txn, key, b"driver-2")
        .await
        .expect("second holder acquires after revoke");
    assert_ne!(first, second, "reacquire must use a fresh lease");
    let holder = read_kv(&mut kv, key).await.expect("lease key recreated");
    assert_eq!(holder.value, b"driver-2");
    assert_eq!(holder.lease_id, second);
}

// ─── 场景 5：fencing —— 旧驱动者的迟到写被拒绝 ───

#[tokio::test(flavor = "multi_thread")]
async fn fenced_write_rejects_stale_driver() {
    let (addr, _sd, _g, _r, _tmp) = common::start_test_server().await;
    let ch = connect(&addr).await;
    let mut kv = KvClient::new(ch.clone());
    let mut lease = LeaseClient::new(ch.clone());
    let mut txn = TxnClient::new(ch.clone());

    let state_key = b"/_workflow/v3/instances/fenced";
    let driver_key = b"/_workflow/v3/drivers/fenced";

    // 初始实例状态 + 初始修订号 r0。
    put_raw(&mut kv, state_key, b"state-v0", 0).await;
    let r0 = read_kv(&mut kv, state_key)
        .await
        .expect("state v0")
        .mod_revision;

    // driver-1 获取租约（epoch = driver_key 的 mod_revision）。
    let lease1 = try_acquire_lease(&mut lease, &mut txn, driver_key, b"token-1")
        .await
        .expect("driver-1 acquires");
    let epoch1 = read_kv(&mut kv, driver_key)
        .await
        .expect("driver key")
        .mod_revision;

    // driver-1 的 fencing 写：同时比较 state.mod_revision == r0 与 driver_key.mod_revision == epoch1。
    let w1 = txn_cas(
        &mut txn,
        vec![
            compare_mod_rev(state_key, r0),
            compare_mod_rev(driver_key, epoch1),
            compare_value(driver_key, b"token-1"),
        ],
        vec![put_op(state_key, b"state-by-driver-1", 0)],
    )
    .await;
    assert!(w1, "driver-1 fenced write must succeed while holding lease");
    let r1 = read_kv(&mut kv, state_key)
        .await
        .expect("state after driver-1")
        .mod_revision;

    // driver-1 失去租约（撤销 → driver_key 级联删除）。
    lease
        .lease_revoke(LeaseRevokeRequest { id: lease1 })
        .await
        .expect("revoke driver-1");
    await_key_gone(&mut kv, driver_key, "driver-1 lease key deleted").await;

    // driver-2 重新获取：driver_key 的 mod_revision 变为新 epoch。
    let _lease2 = try_acquire_lease(&mut lease, &mut txn, driver_key, b"token-2")
        .await
        .expect("driver-2 acquires after driver-1 lost lease");
    let epoch2 = read_kv(&mut kv, driver_key)
        .await
        .expect("driver-2 key")
        .mod_revision;
    assert!(epoch2 > epoch1, "new epoch must be greater than old");

    // 旧驱动者的迟到写：仍用 epoch1/token-1 → 必须被拒绝。
    let stale = txn_cas(
        &mut txn,
        vec![
            compare_mod_rev(state_key, r1),
            compare_mod_rev(driver_key, epoch1),
            compare_value(driver_key, b"token-1"),
        ],
        vec![put_op(state_key, b"state-by-stale-driver-1", 0)],
    )
    .await;
    assert!(!stale, "stale driver's fenced write must be rejected");
    assert_eq!(
        read_kv(&mut kv, state_key).await.expect("state").value,
        b"state-by-driver-1",
        "stale write must not overwrite current state"
    );

    // 新驱动者的 fencing 写成功。
    let w2 = txn_cas(
        &mut txn,
        vec![
            compare_mod_rev(state_key, r1),
            compare_mod_rev(driver_key, epoch2),
            compare_value(driver_key, b"token-2"),
        ],
        vec![put_op(state_key, b"state-by-driver-2", 0)],
    )
    .await;
    assert!(w2, "current driver's fenced write must succeed");
    assert_eq!(
        read_kv(&mut kv, state_key).await.expect("state").value,
        b"state-by-driver-2"
    );
}

// ─── 场景 6：引擎级单写者 —— 单效果 vs 无租约重复 ───

/// 输出一行并发时间线事件（供 `spike/2026-10/spike-2-concurrency/scripts/repro.sh` 抽取到
/// `evidence/timeline-*.jsonl`）。
fn timeline(test: &str, event: &str, detail: &str) {
    let ts_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    eprintln!(
        "TIMELINE {{\"test\":\"{test}\",\"ts_ms\":{ts_ms},\"event\":\"{event}\",\"detail\":\"{detail}\"}}"
    );
}

/// 计数 + 会合派发器：每实例 dispatch 计数，等到 `expect` 个并发到达
/// （或最短停留后）再返回成功，从而让两个驱动者稳定地「同时」产生副作用。
struct RendezvousDispatcher {
    label: &'static str,
    calls: Arc<AtomicUsize>,
    expect: usize,
}

#[tonic::async_trait]
impl TaskDispatcher for RendezvousDispatcher {
    async fn dispatch(
        &self,
        _service: &str,
        _with: Option<&serde_json::Value>,
        _input: &serde_json::Value,
    ) -> DispatchResult {
        let n = self.calls.fetch_add(1, Ordering::SeqCst) + 1;
        timeline(self.label, "side_effect_dispatch", &format!("call#{n}"));
        // 最短停留：给竞争驱动者时间尝试获取租约（有租约时其会被跳过）。
        let start = tokio::time::Instant::now();
        loop {
            let elapsed = start.elapsed();
            if self.calls.load(Ordering::SeqCst) >= self.expect
                && elapsed >= Duration::from_millis(400)
            {
                break;
            }
            if elapsed >= Duration::from_secs(3) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        DispatchResult::Success {
            data: serde_json::json!({"ok": true}),
        }
    }
}

fn single_effect_definition() -> WorkflowDefinition {
    WorkflowDefinition {
        id: None,
        document: Document {
            dsl: "1.0.0".into(),
            namespace: "spike2".into(),
            name: "single-effect".into(),
            version: "1.0".into(),
            title: None,
            summary: None,
            tags: None,
        },
        do_tasks: vec![NamedTask {
            name: "effect".into(),
            task: Task::Call(CallTask {
                call: CallType::Function("effector".into()),
                with: None,
            }),
        }],
        input: None,
        output: None,
        timeout: None,
        use_components: None,
        schedule: Default::default(),
        auth: Default::default(),
        secrets: Default::default(),
        constants: Default::default(),
        task_meta: Default::default(),
        raw_yaml: None,
    }
}

async fn agent_inner(addr: &str) -> Arc<coord_agent::AgentInner> {
    Arc::new(
        coord_agent::AgentInner::new(
            vec![addr.to_string()],
            coord_agent::cache::AgentCache::new(128, 0, 128, 0),
            None,
            // 测试环境不开服务端鉴权：回退凭据留空。
            Arc::new(coord_client::credential::CachedTokenProvider::new(None)),
        )
        .await
        .expect("connect agent inner"),
    )
}

/// 预置一个处于 `Running`（任务索引 0）的实例，供 `drive_instance` 直接驱动。
async fn seed_running_instance(
    store: &KvWorkflowStore,
    def: &WorkflowDefinition,
) -> WorkflowInstance {
    store.save_definition(def).await.expect("save definition");
    let mut inst = WorkflowInstance::new(def, serde_json::json!({}), 1_000);
    inst.status = InstanceStatus::Running;
    store.save_instance(&inst).await.expect("seed instance");
    inst
}

/// 轮询直到实例进入终态。
///
/// 此处用 `load_instance_with_rev`（线性一致读）而**非** `load_instance`：后者是
/// 缓存优先，而本测试未调用 `KvWorkflowStore::init()`（无 Watch 使缓存失效），
/// 读一次就会把陈旧快照固定住。
async fn await_terminal(store: &KvWorkflowStore, id: &str) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        let inst = store
            .load_instance_with_rev(id)
            .await
            .ok()
            .flatten()
            .map(|(i, _)| i);
        if inst
            .as_ref()
            .map(|i| i.status.is_terminal())
            .unwrap_or(false)
        {
            return;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "workflow {id} did not reach terminal state"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

fn make_runtime(
    inner: Arc<coord_agent::AgentInner>,
    dispatcher: Arc<RendezvousDispatcher>,
    with_lease: bool,
) -> WorkflowRuntime<
    ExpressionEvaluator,
    SystemClock,
    KvWorkflowStore,
    Arc<RendezvousDispatcher>,
    NoopEventProvider,
> {
    let store = KvWorkflowStore::new(inner.clone());
    let executor = WorkflowExecutor::new(ExpressionEvaluator::new(), SystemClock::new());
    let rt = WorkflowRuntime::new(
        executor,
        SystemClock::new(),
        store,
        dispatcher,
        NoopEventProvider,
    );
    if with_lease {
        rt.with_instance_lease(Arc::new(KvInstanceLease::new(inner, 30)))
    } else {
        rt
    }
}

/// 注入实例租约：两个 `WorkflowRuntime` 并发驱动同一实例，副作用恰好一次；
/// 竞争失败方跳过驱动（`lease_skipped_total >= 1`）。
#[tokio::test(flavor = "multi_thread")]
async fn engine_concurrent_drivers_single_effect() {
    let (addr, _sd, _g, _r, _tmp) = common::start_test_server().await;
    let inner = agent_inner(&addr).await;

    let def = single_effect_definition();
    let seed_store = KvWorkflowStore::new(inner.clone());
    let inst = seed_running_instance(&seed_store, &def).await;
    drop(seed_store);

    let calls = Arc::new(AtomicUsize::new(0));
    let dispatcher = Arc::new(RendezvousDispatcher {
        label: "single_effect",
        calls: Arc::clone(&calls),
        expect: 1,
    });

    let rt_a = make_runtime(inner.clone(), Arc::clone(&dispatcher), true);
    let rt_b = make_runtime(inner.clone(), Arc::clone(&dispatcher), true);

    timeline("single_effect", "drive_requested", "runtime-A,runtime-B");
    let (ra, rb) = tokio::join!(rt_a.drive_instance(&inst.id), rt_b.drive_instance(&inst.id));
    ra.expect("runtime A drive_instance");
    rb.expect("runtime B drive_instance");

    let store = KvWorkflowStore::new(inner.clone());
    await_terminal(&store, &inst.id).await;
    timeline("single_effect", "terminal", "reached");

    assert_eq!(
        calls.load(Ordering::SeqCst),
        1,
        "with a single-writer lease the side effect must fire exactly once"
    );
    let skipped = rt_a.lease_skipped_total() + rt_b.lease_skipped_total();
    assert!(
        skipped >= 1,
        "the losing driver must have skipped due to the lease (skipped={skipped})"
    );
}

/// 对照：**不**注入租约时，两个并发驱动者各自派发 ⇒ 副作用重复。
#[tokio::test(flavor = "multi_thread")]
async fn engine_concurrent_drivers_duplicate_without_lease() {
    let (addr, _sd, _g, _r, _tmp) = common::start_test_server().await;
    let inner = agent_inner(&addr).await;

    let def = single_effect_definition();
    let seed_store = KvWorkflowStore::new(inner.clone());
    let inst = seed_running_instance(&seed_store, &def).await;
    drop(seed_store);

    let calls = Arc::new(AtomicUsize::new(0));
    let dispatcher = Arc::new(RendezvousDispatcher {
        label: "no_lease_contrast",
        calls: Arc::clone(&calls),
        expect: 2,
    });

    let rt_a = make_runtime(inner.clone(), Arc::clone(&dispatcher), false);
    let rt_b = make_runtime(inner.clone(), Arc::clone(&dispatcher), false);

    timeline(
        "no_lease_contrast",
        "drive_requested",
        "runtime-A,runtime-B",
    );
    let (ra, rb) = tokio::join!(rt_a.drive_instance(&inst.id), rt_b.drive_instance(&inst.id));
    ra.expect("runtime A drive_instance");
    rb.expect("runtime B drive_instance");

    let store = KvWorkflowStore::new(inner.clone());
    await_terminal(&store, &inst.id).await;
    timeline("no_lease_contrast", "terminal", "reached");

    assert_eq!(
        calls.load(Ordering::SeqCst),
        2,
        "without a lease both concurrent drivers dispatch the side effect"
    );
}
