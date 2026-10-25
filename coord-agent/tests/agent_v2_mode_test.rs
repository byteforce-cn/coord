//! v2 模式（`data_plane = "v2"`）的 v1 数据面冻结与 v2 服务注册。
//!
//! v2 集群只服务 `coord.*.v2`（ADR-0015 D7/D13）。Agent 内部服务的 v1 形状
//! 调用会被适配到 v2，但**对外**的 v1 代理不能变成兼容 facade：handler 必须在
//! 本地返回与服务端逐字一致的 `FAILED_PRECONDITION`，先于任何后端调用。

use std::time::Duration;

use coord_agent::{AgentConfig, AgentDataPlane, AgentServer};
use coord_core::error_code::error_code_of;
use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::v2::kv_client::KvClient as KvV2Client;
use coord_proto::kv::v2::RangeRequest as RangeRequestV2;
use coord_proto::kv::{PutRequest, RangeRequest};
use coord_proto::lease::lease_client::LeaseClient;
use coord_proto::lease::LeaseGrantRequest;
use coord_proto::storage::storage_client::StorageClient;
use coord_proto::storage::PutRequest as StoragePutRequest;
use coord_proto::txn::txn_client::TxnClient;
use coord_proto::txn::TxnRequest;
use coord_proto::watch::watch_client::WatchClient;
use coord_proto::watch::WatchRequest;

fn find_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

fn test_config(port: u16) -> AgentConfig {
    AgentConfig {
        agent_addr: format!("127.0.0.1:{port}"),
        http_addr: format!("127.0.0.1:{}", find_port()),
        data_dir: std::env::temp_dir()
            .join(format!("coord-agent-v2-mode-{port}-{}", std::process::id()))
            .to_string_lossy()
            .into_owned(),
        data_plane: AgentDataPlane::V2,
        static_peers: vec![],
        ..Default::default()
    }
}

async fn connect_ready(addr: &str) -> tonic::transport::Channel {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    loop {
        match tonic::transport::Channel::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
        {
            Ok(ch) => return ch,
            Err(_) if tokio::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("agent gRPC never became ready at {addr}: {e}"),
        }
    }
}

fn assert_frozen(status: &tonic::Status, surface: &str) {
    assert_eq!(
        status.code(),
        tonic::Code::FailedPrecondition,
        "{surface}: {status:?}"
    );
    assert!(
        status
            .message()
            .contains("v1 data plane is frozen on a v2 cluster"),
        "{surface}: stable diagnostic anchor missing: {:?}",
        status.message()
    );
    assert_eq!(
        error_code_of(status).as_deref(),
        Some("FAILED_PRECONDITION"),
        "{surface}: wire code must be preserved"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn v1_data_plane_is_frozen_and_v2_stays_registered() {
    let port = find_port();
    let config = test_config(port);
    let addr = config.agent_addr.clone();
    let server = AgentServer::new(config);
    tokio::spawn(async move {
        let _ = server.serve().await;
    });
    let channel = connect_ready(&addr).await;

    // v1 KV / Txn / Lease / Watch / Storage：FAILED_PRECONDITION + 稳定锚点。
    let mut kv = KvClient::new(channel.clone());
    let err = kv
        .put(PutRequest {
            key: b"/_lock/a".to_vec(),
            value: b"v".to_vec(),
            lease_id: 0,
            prev_kv: false,
            request_id: Vec::new(),
        })
        .await
        .expect_err("v1 KV Put must be rejected");
    assert_frozen(&err, "coord.kv.Put");

    let mut kv = KvClient::new(channel.clone());
    let err = kv
        .range(RangeRequest {
            key: b"/_lock/a".to_vec(),
            range_end: Vec::new(),
            limit: 0,
            revision: 0,
            keys_only: false,
            count_only: false,
        })
        .await
        .expect_err("v1 KV Range must be rejected");
    assert_frozen(&err, "coord.kv.Range");

    let mut txn = TxnClient::new(channel.clone());
    let err = txn
        .txn(TxnRequest {
            compare: Vec::new(),
            success: Vec::new(),
            failure: Vec::new(),
            request_id: Vec::new(),
        })
        .await
        .expect_err("v1 Txn must be rejected");
    assert_frozen(&err, "coord.txn.Txn");

    let mut lease = LeaseClient::new(channel.clone());
    let err = lease
        .lease_grant(LeaseGrantRequest { ttl: 10, id: 0 })
        .await
        .expect_err("v1 LeaseGrant must be rejected");
    assert_frozen(&err, "coord.lease.LeaseGrant");

    let mut watch = WatchClient::new(channel.clone());
    let err = watch
        .watch(tokio_stream::empty::<WatchRequest>())
        .await
        .expect_err("v1 Watch must be rejected");
    assert_frozen(&err, "coord.watch.Watch");

    let mut storage = StorageClient::new(channel.clone());
    let err = storage
        .put(tokio_stream::empty::<StoragePutRequest>())
        .await
        .expect_err("v1 Storage Put must be rejected");
    assert_frozen(&err, "coord.storage.Put");

    // v2 服务仍注册；skeleton（无 Server 连接）返回 UNAVAILABLE，而不是冻结码。
    let mut kv_v2 = KvV2Client::new(channel);
    let err = kv_v2
        .range(RangeRequestV2 {
            key: b"/_lock/a".to_vec(),
            range_end: Vec::new(),
            limit: 0,
            read_ts: 0,
            keys_only: true,
            count_only: false,
        })
        .await
        .expect_err("skeleton v2 Range must fail closed");
    assert_eq!(err.code(), tonic::Code::Unavailable, "{err:?}");
    assert_eq!(
        error_code_of(&err).as_deref(),
        Some("UNAVAILABLE"),
        "{err:?}"
    );
}

#[test]
fn data_plane_config_defaults_to_v1_and_parses_v2() {
    let default: AgentConfig = toml::from_str("agent_addr = \"127.0.0.1:19527\"").unwrap();
    assert_eq!(default.data_plane, AgentDataPlane::V1);

    let v2: AgentConfig = toml::from_str("data_plane = \"v2\"").unwrap();
    assert_eq!(v2.data_plane, AgentDataPlane::V2);
}
