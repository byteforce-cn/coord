// TDD: Agent gRPC Server 启动测试 (Phase B1 — RED)
//
// 验证 Agent 可以：
// 1. 在指定端口启动 gRPC server
// 2. 注册全部 5 个 gRPC 服务（KV/Txn/Lease/Watch/Maintenance）
// 3. 接受客户端连接并响应请求
//
// RED 阶段：run_agent 尚未实现 gRPC server 启动，此测试预期失败。

use std::time::Duration;

use coord_agent::{AgentConfig, AgentServer};
use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::PutRequest;
use coord_proto::txn::txn_client::TxnClient;
use coord_proto::txn::TxnRequest;
use coord_proto::lease::lease_client::LeaseClient;
use coord_proto::lease::LeaseGrantRequest;
use coord_proto::watch::watch_client::WatchClient;
use coord_proto::maintenance::maintenance_client::MaintenanceClient;
use coord_proto::maintenance::StatusRequest;

/// Find an available TCP port on localhost
fn find_port() -> u16 {
    use std::net::TcpListener;
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

fn test_config(port: u16) -> AgentConfig {
    AgentConfig {
        agent_addr: format!("127.0.0.1:{}", port),
        http_addr: format!("127.0.0.1:{}", find_port()),
        data_dir: "/tmp/coord-agent-test".into(),
        static_peers: vec![],  // B1 骨架模式：不连接真实 Server
        ..Default::default()
    }
}

/// 隔离数据目录测试配置：避免持久化状态跨用例污染（Cache/MQ 数据面）
fn test_config_isolated(port: u16, tag: &str) -> AgentConfig {
    let mut config = test_config(port);
    config.data_dir = std::env::temp_dir()
        .join(format!("coord-agent-{}-test-{}-{}", tag, port, std::process::id()))
        .to_string_lossy()
        .into_owned();
    config
}

/// MQ 测试配置：使用独立数据目录，避免持久化状态跨用例污染
fn test_config_mq(port: u16) -> AgentConfig {
    test_config_isolated(port, "mq")
}

/// B1.1: Agent gRPC server 能启动并监听指定端口
#[tokio::test]
async fn test_agent_grpc_server_starts_and_listens() {
    let port = find_port();
    let config = test_config(port);
    let addr = config.agent_addr.clone();

    // 启动 Agent gRPC server（后台任务）
    let server = AgentServer::new(config.clone());
    let handle = tokio::spawn(async move {
        server.serve().await.unwrap();
    });

    // 等待 server 启动
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 验证端口已监听
    let stream = tokio::net::TcpStream::connect(&addr).await;
    assert!(stream.is_ok(), "Agent gRPC server should listen on {addr}");
    drop(stream);

    handle.abort();
}

/// B1.2: KV 服务已注册，可接受 gRPC 调用
#[tokio::test]
async fn test_agent_kv_service_registered() {
    let port = find_port();
    let config = test_config(port);
    let addr = config.agent_addr.clone();

    let server = AgentServer::new(config);
    let handle = tokio::spawn(async move {
        server.serve().await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    // 建立 gRPC 连接并调用 KV::Put
    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("should connect to agent gRPC");

    let mut kv_client = KvClient::new(channel);
    let resp = kv_client
        .put(PutRequest {
            key: b"test_key".to_vec(),
            value: b"test_value".to_vec(),
            ..Default::default()
        })
        .await;

    // RED 阶段：当前 Agent 未实现 KV 代理，预期失败
    // GREEN 阶段：应返回 Ok 响应
    assert!(resp.is_ok(), "KV Put should succeed: {resp:?}");

    handle.abort();
}

/// B1.3: Txn/Lease/Watch/Maintenance 服务全部注册
#[tokio::test]
async fn test_agent_all_services_registered() {
    let port = find_port();
    let config = test_config(port);
    let addr = config.agent_addr.clone();

    let server = AgentServer::new(config);
    let handle = tokio::spawn(async move {
        server.serve().await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("should connect");

    // Txn service
    let mut txn_client = TxnClient::new(channel.clone());
    let txn_resp = txn_client
        .txn(TxnRequest::default())
        .await;
    assert!(txn_resp.is_ok(), "Txn should be registered: {txn_resp:?}");

    // Lease service
    let mut lease_client = LeaseClient::new(channel.clone());
    let lease_resp = lease_client
        .lease_grant(LeaseGrantRequest { ttl: 30, id: 0 })
        .await;
    assert!(lease_resp.is_ok(), "Lease should be registered: {lease_resp:?}");

    // Watch service (bidirectional streaming — 验证服务已注册)
    let watch_client = WatchClient::new(channel.clone());
    // 仅验证 stub 可以构造并连接（stream 调用在 Phase B4 详细测试）
    let _ = watch_client; // 服务注册验证：若服务未注册，构造不会失败但首帧会报错

    // Maintenance service
    let mut maint_client = MaintenanceClient::new(channel.clone());
    let status_resp = maint_client
        .status(StatusRequest {})
        .await;
    assert!(status_resp.is_ok(), "Maintenance should be registered: {status_resp:?}");

    handle.abort();
}

/// B1.4: 自定义 `coord.agent.Health/Check` 已注册并返回 SERVING
///
/// Java SDK `healthCheck()` 调用的是 agent_api.proto 中自定义的
/// `coord.agent.Health/Check`（非标准 grpc.health.v1.Health）。
/// 此前 Agent 未注册该服务 → UNIMPLEMENTED → NOT_SERVING 误报
/// （注册/ID 生成等服务实际可用）。注册后应返回 SERVING。
#[tokio::test]
async fn test_agent_custom_health_service_serving() {
    let port = find_port();
    let config = test_config(port);
    let addr = config.agent_addr.clone();

    let server = AgentServer::new(config);
    let handle = tokio::spawn(async move {
        server.serve().await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(200)).await;

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("should connect to agent gRPC");

    let mut health_client = coord_proto::agent::health_client::HealthClient::new(channel);
    let resp = health_client
        .check(coord_proto::agent::HealthCheckRequest {})
        .await
        .expect("coord.agent.Health/Check should be registered");

    let status = resp.into_inner().status;
    assert_eq!(
        status,
        coord_proto::agent::health_check_response::ServingStatus::Serving as i32,
        "custom Health/Check should return SERVING (not a false NOT_SERVING)"
    );

    handle.abort();
}

/// MQ 数据面端到端（Phase 1 — Poll RPC，RED→GREEN）
///
/// createTopic → publish（递增 offset）→ poll（按 offset 增量拉取）→ ack
/// （提交消费组偏移）→ 从已确认 offset 继续拉取（at-least-once）。
#[tokio::test]
async fn test_agent_mq_poll_end_to_end() {
    let port = find_port();
    let mut config = test_config_mq(port);
    config.services.mq = true; // MQ 服务默认关闭，需显式启用
    let addr = config.agent_addr.clone();

    let server = AgentServer::new(config);
    let handle = tokio::spawn(async move {
        server.serve().await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("should connect to agent gRPC");

    use coord_proto::agent::mq_client::MqClient;
    use coord_proto::agent::{
        MqAckRequest, MqCreateTopicRequest, MqPollRequest, MqPublishRequest,
    };

    let mut mq = MqClient::new(channel);

    // createTopic
    mq.create_topic(MqCreateTopicRequest {
        topic: "orders".into(),
        partitions: 1,
    })
    .await
    .expect("create topic should succeed");

    // publish × 3 → 递增 offset
    let mut offsets = Vec::new();
    for i in 0..3u8 {
        let resp = mq.publish(MqPublishRequest {
            topic: "orders".into(),
            partition: 0,
            key: vec![],
            payload: vec![i],
            idempotency_key: String::new(),
        })
        .await
        .expect("publish should succeed");
        offsets.push(resp.into_inner().offset);
    }
    assert_eq!(offsets, vec![0, 1, 2]);

    // poll：按 offset 增量拉取（最多 2 条）
    let poll = mq.poll(MqPollRequest {
        topic: "orders".into(),
        partition: 0,
        consumer_group: "cg1".into(),
        start_offset: 0,
        max_count: 2,
    })
    .await
    .expect("poll should be implemented");
    let msgs = poll.into_inner().messages;
    assert_eq!(msgs.len(), 2);
    assert_eq!(msgs[0].offset, 0);
    assert_eq!(msgs[0].payload, vec![0]);
    assert_eq!(msgs[1].offset, 1);

    // ack：提交消费组偏移
    mq.ack(MqAckRequest {
        topic: "orders".into(),
        consumer_group: "cg1".into(),
        partition: 0,
        offset: 1,
    })
    .await
    .expect("ack should succeed");

    // 从已确认 offset 继续拉取（at-least-once：ack 后不重复）
    let poll2 = mq.poll(MqPollRequest {
        topic: "orders".into(),
        partition: 0,
        consumer_group: "cg1".into(),
        start_offset: 2,
        max_count: 10,
    })
    .await
    .expect("poll 2 should succeed");
    let msgs2 = poll2.into_inner().messages;
    assert_eq!(msgs2.len(), 1);
    assert_eq!(msgs2[0].offset, 2);

    handle.abort();
}

/// Cache 数据面：RPop / LLen（Phase 2，原子出队）
///
/// lpush 入队 → llen 计数 → rpop 出队（FIFO 语义 rpop 取队尾）→ 再次 llen。
#[tokio::test]
async fn test_agent_cache_rpop_llen() {
    let port = find_port();
    let config = test_config_isolated(port, "cache"); // cache 服务默认启用；独立数据目录防污染
    let addr = config.agent_addr.clone();

    let server = AgentServer::new(config);
    let handle = tokio::spawn(async move {
        server.serve().await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("should connect to agent gRPC");

    use coord_proto::agent::cache_client::CacheClient;
    use coord_proto::agent::{CacheLLenRequest, CacheLPushRequest, CacheRPopRequest};

    let mut cache = CacheClient::new(channel);

    // 入队 3 个（lpush → 队头，队尾为最早 push 的元素）
    for i in 0..3u8 {
        cache
            .l_push(CacheLPushRequest {
                key: "q".into(),
                value: vec![i],
            })
            .await
            .expect("lpush should succeed");
    }

    // llen
    let len = cache
        .l_len(CacheLLenRequest { key: "q".into() })
        .await
        .expect("llen should be implemented")
        .into_inner()
        .length;
    assert_eq!(len, 3);

    // rpop 出队（队尾 = 最早 push 的元素 0）
    let pop = cache
        .r_pop(CacheRPopRequest { key: "q".into() })
        .await
        .expect("rpop should be implemented")
        .into_inner();
    assert!(pop.found);
    assert_eq!(pop.value, vec![0]);

    // 空列表 rpop → found=false
    let empty = cache
        .r_pop(CacheRPopRequest { key: "empty-q".into() })
        .await
        .expect("rpop empty should succeed")
        .into_inner();
    assert!(!empty.found);

    let len2 = cache
        .l_len(CacheLLenRequest { key: "q".into() })
        .await
        .expect("llen 2 should succeed")
        .into_inner()
        .length;
    assert_eq!(len2, 2);

    handle.abort();
}

/// MQ 流式 subscribe（Phase 4）：回放已提交偏移后的消息 + produce 实时推送
#[tokio::test]
async fn test_agent_mq_subscribe_stream() {
    let port = find_port();
    let mut config = test_config_mq(port);
    config.services.mq = true;
    let addr = config.agent_addr.clone();

    let server = AgentServer::new(config);
    let handle = tokio::spawn(async move {
        server.serve().await.unwrap();
    });
    tokio::time::sleep(Duration::from_millis(300)).await;

    let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect()
        .await
        .expect("should connect to agent gRPC");

    use coord_proto::agent::mq_client::MqClient;
    use coord_proto::agent::{MqCreateTopicRequest, MqPublishRequest, MqSubscribeRequest};

    let mut mq = MqClient::new(channel);
    mq.create_topic(MqCreateTopicRequest {
        topic: "push".into(),
        partitions: 1,
    })
    .await
    .expect("create topic");

    // 先入 2 条 → 订阅时应回放
    mq.publish(MqPublishRequest {
        topic: "push".into(),
        partition: 0,
        key: vec![],
        payload: b"replay-1".to_vec(),
        idempotency_key: String::new(),
    })
    .await
    .unwrap();
    mq.publish(MqPublishRequest {
        topic: "push".into(),
        partition: 0,
        key: vec![],
        payload: b"replay-2".to_vec(),
        idempotency_key: String::new(),
    })
    .await
    .unwrap();

    // 订阅流
    let mut stream = mq
        .subscribe(MqSubscribeRequest {
            topic: "push".into(),
            consumer_group: "cg-push".into(),
        })
        .await
        .expect("subscribe")
        .into_inner();

    // 回放 2 条
    let m1 = stream.message().await.expect("msg1").expect("ok");
    assert_eq!(m1.payload, b"replay-1");
    let m2 = stream.message().await.expect("msg2").expect("ok");
    assert_eq!(m2.payload, b"replay-2");

    // 实时推送
    mq.publish(MqPublishRequest {
        topic: "push".into(),
        partition: 0,
        key: vec![],
        payload: b"live-3".to_vec(),
        idempotency_key: String::new(),
    })
    .await
    .unwrap();
    let m3 = tokio::time::timeout(Duration::from_secs(3), stream.message())
        .await
        .expect("live push timeout")
        .expect("msg3")
        .expect("ok");
    assert_eq!(m3.payload, b"live-3");

    handle.abort();
}
