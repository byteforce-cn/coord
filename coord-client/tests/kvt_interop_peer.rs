//! KV-T G6 跨语言互操作（#172 残余）：Rust `coord-client` 与 Java SDK 在**同一套**
//! 真实 v2 集群上对账。
//!
//! 两个用例都 `#[ignore]`：它们需要一个已就绪的 v2 集群，以及由
//! `scripts/check-kvt-interop.sh` 放置的证据文件（方向 A 由本文件写出、方向 B 由
//! Java 侧写出）。默认 `cargo test` 不跑，也不该跑——没有集群时它只会红。
//!
//! 对账口径只钉 ADR-0015 已冻结的事实：`revision == commit_ts`、同一事务的所有 key
//! 共享一个 `commit_ts`、`read_ts` 是快照读、Watch 的一个事务 = 一个原子批。跨语言
//! 双方对同一条 wire 语义给出同一个 `commit_ts`/`mod_revision` 才是互操作证据。

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::time::Duration;

use coord_client::txn_v2::{put_mutation, TxnCommit};
use coord_client::watch_v2::{WatchV2Batch, WatchV2Options};
use coord_client::{Client, Config};

/// Watch 批的等待上限：事件在提交后异步投递，但不能无限等。
const WATCH_TIMEOUT: Duration = Duration::from_secs(15);

#[tokio::test]
#[ignore = "needs a real v2 cluster and the evidence dir created by scripts/check-kvt-interop.sh"]
async fn rust_writes_for_java_to_read() {
    let dir = evidence_dir();
    let run = run_id();
    let root = format!("/interop/{run}/");
    let txn_prefix = format!("{root}txn/");
    let direct_key = format!("{root}direct/k");

    let client = connect().await;
    let kv = client.kv_v2();

    // 从「尚不存在」的 revision 起步：流建立晚于提交时服务端仍会回放该位点，
    // 因此不需要用 sleep 去赌 create 帧先到（与 SDK 的 v2 IT 同一做法）。
    let watch_base = kv.get_commit_ts().await.expect("tso get_ts");
    let mut watch = client
        .watch_v2()
        .watch(
            WatchV2Options::new(txn_prefix.as_bytes())
                .with_range(prefix_end(txn_prefix.as_bytes()))
                .from_revision(watch_base + 1)
                .with_prev_kv(true),
        )
        .await
        .expect("open coord.watch.v2 stream");

    // 方向 A 的另一半：单键直写（显式 commit_ts），Java 侧用它验证直写与事务写的
    // mod_revision 都等于各自的 commit_ts。
    let direct_ts = kv.get_commit_ts().await.expect("tso get_ts");
    kv.put(direct_key.as_bytes(), b"rust-direct", direct_ts, false, b"")
        .await
        .expect("coord.kv.v2 Put");

    // 跨键原子写只能走 coord.txn.v2：一个 commit_ts 覆盖整个写集。
    let session = client.txn_v2().begin(0, 0).await.expect("txn begin");
    let start_ts = session.start_ts();
    let (_, read_ts) = session
        .read(direct_key.as_bytes(), b"", 0, false)
        .await
        .expect("txn read");
    assert_eq!(read_ts, start_ts, "Txn Read must run at start_ts");
    let commit_ts = match session
        .commit(
            vec![
                put_mutation(format!("{txn_prefix}a").as_bytes(), b"rust-txn-a", 0),
                put_mutation(format!("{txn_prefix}b").as_bytes(), b"rust-txn-b", 0),
            ],
            b"",
        )
        .await
        .expect("txn commit")
    {
        TxnCommit::Committed { commit_ts } => commit_ts,
        TxnCommit::Conflict => panic!("a fresh transaction must not conflict"),
    };
    assert!(commit_ts > start_ts, "commit_ts must exceed start_ts");

    // 自己写的事务必须作为**一个**原子批回到自己的 Watch 上。
    let batch = await_watch_batch(&mut watch, commit_ts).await;
    let event = batch
        .events
        .iter()
        .find(|event| event.commit_ts == commit_ts)
        .expect("watch batch carries the committed transaction");
    assert_eq!(event.changes.len(), 2, "one transaction = one atomic batch");
    let watched: Vec<String> = event
        .changes
        .iter()
        .map(|change| String::from_utf8_lossy(&change.key).into_owned())
        .collect();
    assert!(watched.contains(&format!("{txn_prefix}a")));
    assert!(watched.contains(&format!("{txn_prefix}b")));

    // 快照读用 commit_ts：两个 key 与直写 key 都在各自的时间戳上可见。
    let (kvs, _, echo) = kv
        .range(
            txn_prefix.as_bytes(),
            &prefix_end(txn_prefix.as_bytes()),
            0,
            commit_ts,
            false,
            false,
        )
        .await
        .expect("range at commit_ts");
    assert_eq!(echo, commit_ts, "Range echoes the read_ts it used");
    assert_eq!(kvs.len(), 2, "range at commit_ts sees both txn keys");
    for stored in &kvs {
        assert_eq!(
            stored.mod_revision, commit_ts as i64,
            "revision == commit_ts for every key of the transaction"
        );
        assert_eq!(stored.create_revision, commit_ts as i64);
        assert_eq!(stored.version, 1);
    }

    write_props(
        &dir.join("rust-to-java.properties"),
        &[
            ("run", run.as_str()),
            ("prefix.txn", txn_prefix.as_str()),
            ("txn.key.a", format!("{txn_prefix}a").as_str()),
            ("txn.value.a", "rust-txn-a"),
            ("txn.key.b", format!("{txn_prefix}b").as_str()),
            ("txn.value.b", "rust-txn-b"),
            ("txn.commit_ts", commit_ts.to_string().as_str()),
            ("direct.key", direct_key.as_str()),
            ("direct.value", "rust-direct"),
            ("direct.commit_ts", direct_ts.to_string().as_str()),
            ("watch.commit_ts", event.commit_ts.to_string().as_str()),
        ],
    );
    println!(
        "[interop] rust -> java evidence written: txn.commit_ts={commit_ts} direct.commit_ts={direct_ts}"
    );

    watch.cancel().await;
}

#[tokio::test]
#[ignore = "needs a real v2 cluster and the evidence dir created by scripts/check-kvt-interop.sh"]
async fn rust_reads_what_java_wrote() {
    let dir = evidence_dir();
    let props = read_props(&dir.join("java-to-rust.properties"));
    let txn_prefix = prop(&props, "prefix.txn");
    let txn_commit_ts = prop(&props, "txn.commit_ts")
        .parse::<u64>()
        .expect("txn.commit_ts");
    let direct_key = prop(&props, "direct.key");
    let direct_ts = prop(&props, "direct.commit_ts")
        .parse::<u64>()
        .expect("direct.commit_ts");

    let client = connect().await;
    let kv = client.kv_v2();

    // 按 Java 侧给出的**精确 commit_ts** 读：同一条 revision 在两种语言里必须是
    // 同一份快照，否则说明某一侧的 wire 语义漂了。
    for suffix in ["a", "b"] {
        let key = format!("{txn_prefix}{suffix}");
        let expected = prop(&props, &format!("txn.value.{suffix}"));
        let (kvs, _, echo) = kv
            .range(key.as_bytes(), b"", 0, txn_commit_ts, false, false)
            .await
            .expect("range at java commit_ts");
        assert_eq!(echo, txn_commit_ts);
        assert_eq!(
            kvs.len(),
            1,
            "key {key} must be visible at read_ts={txn_commit_ts}"
        );
        let stored = &kvs[0];
        assert_eq!(String::from_utf8_lossy(&stored.value), expected);
        assert_eq!(
            stored.mod_revision, txn_commit_ts as i64,
            "mod_revision == commit_ts"
        );
        assert_eq!(stored.create_revision, txn_commit_ts as i64);
        assert_eq!(stored.version, 1);
    }

    let (kvs, _, _) = kv
        .range(
            txn_prefix.as_bytes(),
            &prefix_end(txn_prefix.as_bytes()),
            0,
            txn_commit_ts,
            false,
            false,
        )
        .await
        .expect("prefix range at java commit_ts");
    assert_eq!(
        kvs.len(),
        2,
        "both Java transaction keys share one commit_ts"
    );
    assert!(
        kvs.iter()
            .all(|stored| stored.mod_revision == txn_commit_ts as i64),
        "one transaction = one commit_ts on every key"
    );

    let (kvs, _, _) = kv
        .range(direct_key.as_bytes(), b"", 0, direct_ts, false, false)
        .await
        .expect("range at java direct commit_ts");
    assert_eq!(
        kvs.len(),
        1,
        "Java direct write must be visible at its commit_ts"
    );
    assert_eq!(
        String::from_utf8_lossy(&kvs[0].value),
        prop(&props, "direct.value")
    );
    assert_eq!(kvs[0].mod_revision, direct_ts as i64);

    println!(
        "[interop] rust read java writes at exact commit_ts: txn.commit_ts={txn_commit_ts} direct.commit_ts={direct_ts}"
    );
}

async fn connect() -> Client {
    let endpoint = env("COORD_INTEROP_SERVER", "127.0.0.1:51051");
    Client::new(Config::new(vec![endpoint.clone()]))
        .await
        .unwrap_or_else(|error| {
            panic!(
                "coord server at {endpoint} is not usable ({error}); \
                 start a v2 dev cluster with `bash scripts/ci-java-it-cluster.sh start v2` \
                 (server gRPC 127.0.0.1:51051)"
            )
        })
}

async fn await_watch_batch(
    watch: &mut coord_client::watch_v2::WatchV2Subscription,
    commit_ts: u64,
) -> WatchV2Batch {
    loop {
        let batch = tokio::time::timeout(WATCH_TIMEOUT, watch.recv())
            .await
            .unwrap_or_else(|_| panic!("no watch batch within {WATCH_TIMEOUT:?}"))
            .expect("watch stream ended before delivering commit_ts")
            .unwrap_or_else(|error| panic!("watch stream failed: {error}"));
        if batch
            .events
            .iter()
            .any(|event| event.commit_ts == commit_ts)
        {
            return batch;
        }
    }
}

/// 前缀区间的 range_end（末字节 +1）；全 0xFF 时无有限上界。
fn prefix_end(prefix: &[u8]) -> Vec<u8> {
    let mut end = prefix.to_vec();
    while let Some(last) = end.pop() {
        if last < 0xFF {
            end.push(last + 1);
            return end;
        }
    }
    Vec::new()
}

fn env(name: &str, fallback: &str) -> String {
    std::env::var(name)
        .ok()
        .filter(|value| !value.trim().is_empty())
        .unwrap_or_else(|| fallback.to_string())
}

fn evidence_dir() -> PathBuf {
    let raw = std::env::var("COORD_INTEROP_EVIDENCE_DIR").unwrap_or_else(|_| {
        panic!(
            "COORD_INTEROP_EVIDENCE_DIR is required: the two directions exchange evidence through \
             files written by scripts/check-kvt-interop.sh (never inside the repo)"
        )
    });
    let dir = PathBuf::from(raw);
    std::fs::create_dir_all(&dir)
        .unwrap_or_else(|error| panic!("cannot create evidence dir {dir:?}: {error}"));
    dir
}

fn run_id() -> String {
    let id = env("COORD_INTEROP_RUN_ID", "");
    assert!(
        !id.is_empty(),
        "COORD_INTEROP_RUN_ID is required: both languages must use the same key namespace"
    );
    assert!(
        id.chars().all(|c| c.is_ascii_alphanumeric() || c == '-'),
        "run id must stay ASCII alphanumeric/dash so it is Properties-safe: {id}"
    );
    id
}

fn write_props(path: &PathBuf, entries: &[(&str, &str)]) {
    let mut text = String::new();
    for (key, value) in entries {
        text.push_str(key);
        text.push('=');
        text.push_str(value);
        text.push('\n');
    }
    std::fs::write(path, text).unwrap_or_else(|error| panic!("cannot write {path:?}: {error}"));
}

fn read_props(path: &PathBuf) -> BTreeMap<String, String> {
    let text = std::fs::read_to_string(path).unwrap_or_else(|error| {
        panic!(
            "{path:?} is missing ({error}); run `bash scripts/check-kvt-interop.sh` end-to-end \
             instead of invoking the two phases by hand"
        )
    });
    let mut props = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let (key, value) = line
            .split_once('=')
            .unwrap_or_else(|| panic!("malformed evidence line in {path:?}: {line}"));
        props.insert(key.to_string(), value.to_string());
    }
    props
}

fn prop(props: &BTreeMap<String, String>, name: &str) -> String {
    props
        .get(name)
        .unwrap_or_else(|| panic!("evidence file lacks {name}"))
        .clone()
}
