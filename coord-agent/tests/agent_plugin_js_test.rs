// coord-agent 插件引擎端到端测试（Phase 1 验收：Invoke → 插件 CCT/身份 → KV 落盘）
//
// 每个用例启动单节点真实 coord-server + 真实 agent（`[plugins]` 指向临时插件目录），
// 经 agent 的 `coord.plugin.Plugin/Invoke` 驱动 JS 插件，插件内用宿主 SDK 写协调 KV。
//
// 覆盖：
// - JS 插件经 Invoke 调用 `coord.kv.put` → 真实 server 落盘（读回校验）；
// - 插件作用域越界（agent 侧第一道防御）→ 调用失败；
// - List 观测面报告 runtime=js / status=started；
// - 未加载插件 → NOT_FOUND。

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;
    use std::time::Duration;

    use coord_agent::plugin::manifest::{
        PluginEngineConfig, PluginLimits, PluginManifest, PluginRuntime, PluginSource, PluginTrust,
    };
    use coord_agent::{AgentConfig, AgentServer};
    use coord_core::storage::StorageBackend;
    use coord_core::types::StorageConfig;
    use coord_proto::kv::kv_client::KvClient;
    use coord_proto::kv::kv_server::KvServer;
    use coord_proto::kv::{PutRequest, RangeRequest};
    use coord_proto::lease::lease_server::LeaseServer;
    use coord_proto::maintenance::maintenance_server::MaintenanceServer;
    use coord_proto::plugin::plugin_client::PluginClient;
    use coord_proto::plugin::{InvokeRequest, ListPluginsRequest};
    use coord_proto::storage::storage_server::StorageServer;
    use coord_proto::txn::txn_server::TxnServer;
    use coord_proto::watch::watch_server::WatchServer;
    use coord_server::lease::LeaseManager;
    use coord_server::raft::log_store::LogStore;
    use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcServer, RaftRpcService};
    use coord_server::raft::state_machine::StateMachineStore;
    use coord_server::raft::{new_basic_node, new_raft, RaftConfig};
    use coord_server::server::CoordNode;
    use coord_server::storage::mvcc::MvccStorage;
    use coord_server::storage::object_store::{ChunkStore, ObjectLimits};
    use coord_server::storage::redb_backend::RedbBackend;
    use coord_server::timer::TimerWheel;
    use coord_server::watch::WatchDispatcher;

    const PLUGIN_JS: &str = r#"
export async function handleInvoke(method, payload) {
  if (method === "put") {
    const r = await coord.kv.put("/app/counter/a", payload);
    return coord.util.encode(String(r.revision));
  }
  if (method === "get") {
    const r = await coord.kv.range("/app/counter/a");
    return r.kvs.length ? r.kvs[0].value : coord.util.encode("");
  }
  if (method === "outside") {
    await coord.kv.put("/other/x", "1");
    return coord.util.encode("ALLOWED");
  }
  if (method === "idgen") {
    const first = await coord.idgen.next("/app/counter/id");
    const second = await coord.idgen.next("/app/counter/id");
    return coord.util.encode(String(first) + "," + String(second));
  }
  if (method === "storage-unknown") {
    // 省略 totalSize = 未知长度：commit 时按实际字节定长
    const w = await coord.storage.openWrite("inbox", "obj-u");
    await w.write(coord.util.encode("12345"));
    await w.write(coord.util.encode("678"));
    const c = await w.commit();
    return coord.util.encode(String(w.totalSize) + "," + String(c.size));
  }
  throw new Error("unknown method " + method);
}
"#;

    fn find_port() -> u16 {
        use std::net::TcpListener;
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind ephemeral");
        listener.local_addr().expect("local_addr").port()
    }

    /// 启动单节点 coord-server；返回 (grpc_addr, 关闭句柄, 任务句柄, 临时目录)。
    async fn start_test_server() -> (
        String,
        tokio::sync::oneshot::Sender<()>,
        Vec<tokio::task::JoinHandle<()>>,
        tempfile::TempDir,
    ) {
        let tmpdir = tempfile::tempdir().expect("tempdir");
        let data_dir = tmpdir.path().to_path_buf();

        let grpc_addr = format!("127.0.0.1:{}", find_port());
        let raft_addr = format!("127.0.0.1:{}", find_port());

        let storage_config = StorageConfig::default();
        let backend = RedbBackend::open(&data_dir, &storage_config).expect("open redb backend");
        let mvcc = Arc::new(MvccStorage::new(backend).expect("create mvcc"));
        let snapshot_tracker =
            Arc::new(coord_server::storage::snapshot::SnapshotTracker::default());

        let watch_dispatcher = Arc::new(WatchDispatcher::start());
        let log_store = LogStore::new(&data_dir)
            .await
            .expect("create raft log store")
            .with_snapshot_tracker(Arc::clone(&snapshot_tracker));
        let object_limits = Arc::new(ObjectLimits::default());
        let chunk_store =
            ChunkStore::new(&data_dir, Arc::clone(&object_limits), None).expect("chunk store");
        let mut sm_store = StateMachineStore::new(
            Arc::clone(&mvcc),
            data_dir.join("snapshots"),
            Arc::clone(&snapshot_tracker),
        );
        // 对象存储 apply 在状态机侧落 chunk 文件（否则 Chunk op 会 raft 错误）
        sm_store.set_object_chunk_store(Some(Arc::clone(&chunk_store)));

        let network_factory = RaftNetworkFactoryImpl::new(1);
        network_factory.register_node(1, raft_addr.clone());

        let raft_config = RaftConfig {
            heartbeat_interval: 200,
            election_timeout_min: 800,
            election_timeout_max: 1500,
            ..Default::default()
        };

        let raft_rpc_service = RaftRpcService::new();
        let raft = new_raft(
            1,
            Arc::new(raft_config),
            network_factory,
            log_store,
            sm_store,
        )
        .await
        .expect("create raft instance");
        raft_rpc_service.set_raft(raft.clone());

        let mut members = BTreeMap::new();
        members.insert(1, new_basic_node(&raft_addr));
        raft.initialize(members).await.expect("raft initialize");
        let raft = Arc::new(raft);

        let mut node = CoordNode::new(Arc::clone(&mvcc));
        node.node_id = 1;
        node.watch_dispatcher = Some(Arc::clone(&watch_dispatcher));
        node.raft = Some(Arc::clone(&raft));

        let timer_handle = TimerWheel::start();
        let lease_manager = Arc::new(LeaseManager::new(timer_handle));
        node.lease_manager = Some(Arc::clone(&lease_manager));
        // 对象存储（批次 11 端到端：插件未知长度流式上传）
        node.object_limits = Some(Arc::clone(&object_limits));
        node.chunk_store = Some(Arc::clone(&chunk_store));
        let node = Arc::new(node);

        let kv_svc = KvServer::from_arc(Arc::clone(&node));
        let txn_svc = TxnServer::from_arc(Arc::clone(&node));
        let lease_svc = LeaseServer::from_arc(Arc::clone(&node));
        let watch_svc = WatchServer::from_arc(Arc::clone(&node));
        let maint_svc = MaintenanceServer::from_arc(Arc::clone(&node));
        let storage_svc = StorageServer::from_arc(Arc::clone(&node));

        let raft_rpc_svc = RaftRpcServer::new(raft_rpc_service);
        let raft_addr_parse: std::net::SocketAddr = raft_addr.parse().expect("raft addr");
        let raft_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(raft_rpc_svc)
                .serve(raft_addr_parse)
                .await;
        });

        let grpc_addr_parse: std::net::SocketAddr = grpc_addr.parse().expect("grpc addr");
        let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
        let grpc_handle = tokio::spawn(async move {
            let _ = tonic::transport::Server::builder()
                .add_service(kv_svc)
                .add_service(txn_svc)
                .add_service(lease_svc)
                .add_service(watch_svc)
                .add_service(maint_svc)
                .add_service(storage_svc)
                .serve_with_shutdown(grpc_addr_parse, async {
                    let _ = shutdown_rx.await;
                })
                .await;
        });

        tokio::time::sleep(Duration::from_millis(300)).await;
        for _ in 0..30 {
            if raft.current_leader().await.is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        (
            grpc_addr,
            shutdown_tx,
            vec![grpc_handle, raft_handle],
            tmpdir,
        )
    }

    fn plugin_manifest() -> PluginManifest {
        PluginManifest {
            name: "counter".into(),
            version: "1.0.0".into(),
            runtime: PluginRuntime::Js,
            trust: PluginTrust::FirstParty,
            entry: "index.js".into(),
            capabilities: vec![
                coord_agent::plugin::PluginCapability {
                    id: "data:kv:read".into(),
                    scope: "/app/counter/".into(),
                },
                coord_agent::plugin::PluginCapability {
                    id: "data:kv:write".into(),
                    scope: "/app/counter/".into(),
                },
                coord_agent::plugin::PluginCapability {
                    id: "data:txn:execute".into(),
                    scope: String::new(),
                },
                coord_agent::plugin::PluginCapability {
                    id: "data:storage:write".into(),
                    scope: String::new(),
                },
            ],
            limits: PluginLimits::default(),
            hooks: false,
            source: PluginSource::default(),
        }
    }

    /// Phase 3.3 验收插件：TAS（锁 + 隔离令牌守卫）与领导选举。
    const TAS_JS: &str = r#"
let leaderHandle = null;

function _u(s) { return coord.util.encode(s); }
function _n(rec) { return rec.kvs.length ? Number(coord.util.decode(rec.kvs[0].value)) : 0; }

async function _tas(args) {
  // args: lockKey | counterKey | fenceKey | owner | holdMs | iterations
  const lockKey = args[0], counterKey = args[1], fenceKey = args[2];
  const owner = args[3], holdMs = Number(args[4]), iters = Number(args[5]);
  let committed = 0, stale = 0, timeouts = 0, retried = 0;

  for (let i = 0; i < iters; i++) {
    const lock = await coord.lock.acquire(lockKey, { owner: owner, ttlMs: 5000, waitMs: 4000 });
    if (lock === null) { timeouts++; continue; }
    try {
      // 资源守卫：拒绝令牌不新的持有者（fencing token 单调性检查）
      const maxFence = _n(await coord.kv.range(fenceKey));
      if (lock.fencing <= maxFence) { stale++; continue; }

      await coord.util.sleep(holdMs);

      for (let attempt = 0; attempt < 8; attempt++) {
        const seen = _n(await coord.kv.range(fenceKey));
        if (lock.fencing <= seen) { stale++; break; }
        const cur = _n(await coord.kv.range(counterKey));
        const txn = await coord.txn(
          [{ key: fenceKey, target: "value", op: "equal", value: _u(String(seen)) }],
          [
            { type: "put", key: counterKey, value: _u(String(cur + 1)) },
            { type: "put", key: fenceKey, value: _u(String(lock.fencing)) },
          ],
          []
        );
        if (txn.succeeded) { committed++; break; }
        retried++;
      }
    } finally {
      await lock.release();
    }
  }
  return coord.util.encode([owner, committed, stale, timeouts, retried].join("|"));
}

export async function handleInvoke(method, payload) {
  const args = coord.util.decode(payload).split("|");
  if (method === "tas") {
    return _tas(args);
  }
  if (method === "campaign") {
    // args: key | owner | waitMs
    const h = await coord.election.campaign(args[0], {
      owner: args[1],
      ttlMs: 5000,
      waitMs: Number(args[2]),
    });
    if (h === null) return coord.util.encode("NONE");
    leaderHandle = h;
    return coord.util.encode(["GOT", args[1], String(h.fencing), String(await h.leader())].join("|"));
  }
  if (method === "leader-view") {
    const v = await coord.election.leader(args[0]);
    return coord.util.encode(v === null ? "null" : v.owner + "|" + String(v.version));
  }
  if (method === "still-leader") {
    if (leaderHandle === null) return coord.util.encode("NONE");
    return coord.util.encode(String(await leaderHandle.leader()));
  }
  if (method === "resign") {
    if (leaderHandle === null) return coord.util.encode("NONE");
    await leaderHandle.resign();
    leaderHandle = null;
    return coord.util.encode("RESIGNED");
  }
  throw new Error("unknown method " + method);
}
"#;

    /// TAS / 选举插件 manifest：锁所需的全部能力（lease 类必须空 scope）。
    fn tas_manifest(name: &str) -> PluginManifest {
        PluginManifest {
            name: name.into(),
            version: "1.0.0".into(),
            runtime: PluginRuntime::Js,
            trust: PluginTrust::FirstParty,
            entry: "index.js".into(),
            capabilities: vec![
                coord_agent::plugin::PluginCapability {
                    id: "data:kv:read".into(),
                    scope: "/app/tas/".into(),
                },
                coord_agent::plugin::PluginCapability {
                    id: "data:kv:write".into(),
                    scope: "/app/tas/".into(),
                },
                coord_agent::plugin::PluginCapability {
                    id: "data:txn:execute".into(),
                    scope: String::new(),
                },
                coord_agent::plugin::PluginCapability {
                    id: "data:lease:grant".into(),
                    scope: String::new(),
                },
                coord_agent::plugin::PluginCapability {
                    id: "data:lease:revoke".into(),
                    scope: String::new(),
                },
                coord_agent::plugin::PluginCapability {
                    id: "data:lease:keepalive".into(),
                    scope: String::new(),
                },
            ],
            limits: PluginLimits::default(),
            hooks: false,
            source: PluginSource::default(),
        }
    }

    /// Phase 3.3 验收（§13）：**真实双插件 TAS 竞争**。
    ///
    /// 两个插件（各自 isolate）在同一把锁上并发进入临界区：读计数 → 睡 20ms → 写回 +1，
    /// 同时把隔离令牌写入资源守卫键。断言：
    /// - 所有临界区串行化（计数 == 提交次数，无丢失更新）；
    /// - 无 `stale`（令牌被守卫拒绝）——锁的互斥性成立，不存在两个同时有效的持有者；
    /// - 无超时（等待预算足够）。
    #[tokio::test(flavor = "multi_thread")]
    async fn js_lock_tas_is_mutually_exclusive_across_plugins_on_real_server() {
        let (server_addr, _shutdown, _server_tasks, _server_tmp) = start_test_server().await;
        let (agent_addr, agent_handle, _plugin_dir, _agent_tmp) = start_agent_with_entries(
            &server_addr,
            TAS_JS,
            vec![tas_manifest("lock-a"), tas_manifest("lock-b")],
        )
        .await;

        // 初始化计数与令牌守卫（值必须存在，"0" 作为令牌下界）
        let mut kv = kv_client(&server_addr).await;
        for key in [b"/app/tas/counter".as_slice(), b"/app/tas/fence".as_slice()] {
            kv.put(PutRequest {
                key: key.to_vec(),
                value: b"0".to_vec(),
                ..Default::default()
            })
            .await
            .expect("seed");
        }

        // 两个插件各 3 个并发调用点，每个调用点 3 次临界区 → 18 次提交
        const TASKS_PER_PLUGIN: usize = 3;
        const ITERS: usize = 3;
        let plugins = ["lock-a", "lock-b"];
        let mut handles = Vec::new();
        for plugin in plugins {
            for task in 0..TASKS_PER_PLUGIN {
                let addr = agent_addr.clone();
                handles.push(tokio::spawn(async move {
                    let owner = format!("{plugin}#{task}");
                    let payload =
                        format!("/app/tas/lock|/app/tas/counter|/app/tas/fence|{owner}|20|{ITERS}");
                    let mut client = plugin_client(&addr).await;
                    client
                        .invoke(InvokeRequest {
                            plugin_id: plugin.to_string(),
                            method: "tas".into(),
                            payload: payload.into_bytes(),
                        })
                        .await
                        .expect("invoke tas")
                        .into_inner()
                        .payload
                }));
            }
        }

        let mut committed = 0usize;
        let mut stale = 0usize;
        let mut timeouts = 0usize;
        let mut retried = 0usize;
        for h in handles {
            let out = h.await.expect("join");
            let text = String::from_utf8(out).expect("utf8");
            let fields: Vec<&str> = text.split('|').collect();
            assert_eq!(fields.len(), 5, "unexpected TAS result: {text}");
            committed += fields[1].parse::<usize>().expect("committed");
            stale += fields[2].parse::<usize>().expect("stale");
            timeouts += fields[3].parse::<usize>().expect("timeouts");
            retried += fields[4].parse::<usize>().expect("retried");
        }

        let expected = TASKS_PER_PLUGIN * ITERS * plugins.len();
        assert_eq!(timeouts, 0, "锁等待超时（等待预算不足或互斥未释放）");
        assert_eq!(stale, 0, "出现令牌被守卫拒绝的持有者 → 互斥性被破坏");
        assert_eq!(
            committed, expected,
            "临界区提交数与预期不符（丢失更新？retried={retried}）"
        );

        // 计数器落盘值 == 提交次数（无丢失更新）
        let range = kv
            .range(RangeRequest {
                key: b"/app/tas/counter".to_vec(),
                ..Default::default()
            })
            .await
            .expect("range counter")
            .into_inner();
        assert_eq!(range.kvs.len(), 1);
        let counter: usize = String::from_utf8_lossy(&range.kvs[0].value)
            .parse()
            .expect("counter value");
        assert_eq!(counter, expected, "计数丢失：{counter} != {expected}");

        agent_handle.abort();
    }

    /// Phase 3.3 验收：真实 server 上的领导选举互斥与令牌单调交接。
    #[tokio::test(flavor = "multi_thread")]
    async fn js_election_leadership_is_exclusive_and_hands_over_on_real_server() {
        let (server_addr, _shutdown, _server_tasks, _server_tmp) = start_test_server().await;
        let (agent_addr, agent_handle, _plugin_dir, _agent_tmp) = start_agent_with_entries(
            &server_addr,
            TAS_JS,
            vec![tas_manifest("lock-a"), tas_manifest("lock-b")],
        )
        .await;

        let mut a = plugin_client(&agent_addr).await;
        let mut b = plugin_client(&agent_addr).await;

        let campaign_a = a
            .invoke(InvokeRequest {
                plugin_id: "lock-a".into(),
                method: "campaign".into(),
                payload: b"/app/tas/election|alpha|0".to_vec(),
            })
            .await
            .expect("campaign a")
            .into_inner()
            .payload;
        let fields: Vec<String> = String::from_utf8_lossy(&campaign_a)
            .split('|')
            .map(str::to_string)
            .collect();
        assert_eq!(fields[0], "GOT", "alpha 应当选");
        assert_eq!(fields[3], "true");
        let fence_a: i64 = fields[2].parse().expect("fence a");
        assert!(fence_a > 0);

        // 竞争者：同一选举键上不应当选
        let campaign_b = b
            .invoke(InvokeRequest {
                plugin_id: "lock-b".into(),
                method: "campaign".into(),
                payload: b"/app/tas/election|beta|0".to_vec(),
            })
            .await
            .expect("campaign b")
            .into_inner()
            .payload;
        assert_eq!(campaign_b, b"NONE");

        // 观测面：领导为 alpha；a 仍自认领导
        let view = a
            .invoke(InvokeRequest {
                plugin_id: "lock-a".into(),
                method: "leader-view".into(),
                payload: b"/app/tas/election".to_vec(),
            })
            .await
            .expect("leader view")
            .into_inner()
            .payload;
        let view = String::from_utf8_lossy(&view).to_string();
        assert!(view.starts_with("alpha|"), "unexpected view: {view}");
        let still = a
            .invoke(InvokeRequest {
                plugin_id: "lock-a".into(),
                method: "still-leader".into(),
                payload: Vec::new(),
            })
            .await
            .expect("still leader")
            .into_inner()
            .payload;
        assert_eq!(still, b"true");

        // alpha 卸任 → beta 可当选，且令牌严格更大（可比较的隔离令牌）
        let resigned = a
            .invoke(InvokeRequest {
                plugin_id: "lock-a".into(),
                method: "resign".into(),
                payload: Vec::new(),
            })
            .await
            .expect("resign")
            .into_inner()
            .payload;
        assert_eq!(resigned, b"RESIGNED");

        let campaign_b = b
            .invoke(InvokeRequest {
                plugin_id: "lock-b".into(),
                method: "campaign".into(),
                payload: b"/app/tas/election|beta|2000".to_vec(),
            })
            .await
            .expect("campaign b after resign")
            .into_inner()
            .payload;
        let fields: Vec<String> = String::from_utf8_lossy(&campaign_b)
            .split('|')
            .map(str::to_string)
            .collect();
        assert_eq!(fields[0], "GOT", "beta 应在 alpha 卸任后当选");
        let fence_b: i64 = fields[2].parse().expect("fence b");
        assert!(
            fence_b > fence_a,
            "隔离令牌必须跨任期严格单调：{fence_b} > {fence_a}"
        );

        let _ = b
            .invoke(InvokeRequest {
                plugin_id: "lock-b".into(),
                method: "resign".into(),
                payload: Vec::new(),
            })
            .await;

        agent_handle.abort();
    }

    /// 启动 agent（插件引擎启用，插件目录含给定源文件）。
    async fn start_agent_with_entries(
        server_addr: &str,
        source: &str,
        entries: Vec<PluginManifest>,
    ) -> (
        String,
        tokio::task::JoinHandle<()>,
        tempfile::TempDir,
        tempfile::TempDir,
    ) {
        let plugin_dir = tempfile::tempdir().expect("plugin tempdir");
        std::fs::write(plugin_dir.path().join("index.js"), source).expect("write plugin");

        let agent_tmp = tempfile::tempdir().expect("agent tempdir");
        let agent_addr = format!("127.0.0.1:{}", find_port());
        let config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", find_port()),
            data_dir: agent_tmp.path().to_string_lossy().into_owned(),
            static_peers: vec![server_addr.to_string()],
            plugins: PluginEngineConfig {
                enabled: true,
                dir: plugin_dir.path().to_string_lossy().into_owned(),
                hooks_enabled: true,
                default_limits: PluginLimits::default(),
                env: BTreeMap::new(),
                entries,
            },
            ..Default::default()
        };

        let server = AgentServer::new(config);
        let handle = tokio::spawn(async move {
            server.serve().await.expect("agent serve");
        });
        // 等就绪（见 `connect_ready` 注释：固定 sleep 在并发跑全量套件时会不够）
        connect_ready(&agent_addr).await;
        (agent_addr, handle, plugin_dir, agent_tmp)
    }

    /// 启动 agent（插件引擎启用，插件目录为临时目录）。
    async fn start_agent_with_plugin(
        server_addr: &str,
    ) -> (
        String,
        tokio::task::JoinHandle<()>,
        tempfile::TempDir,
        tempfile::TempDir,
    ) {
        start_agent_with_entries(server_addr, PLUGIN_JS, vec![plugin_manifest()]).await
    }

    async fn plugin_client(addr: &str) -> PluginClient<tonic::transport::Channel> {
        PluginClient::new(connect_ready(addr).await)
    }

    async fn kv_client(addr: &str) -> KvClient<tonic::transport::Channel> {
        KvClient::new(connect_ready(addr).await)
    }

    /// 等本地 gRPC 端点就绪后建链。
    ///
    /// 此前是「`sleep(500ms)` + 一次性 connect」：测试进程内的 `serve()` 是
    /// 异步 spawn 的，**并不保证**在 sleep 结束时已经 bind。并发跑全量套件时
    /// （8 核跑多个进程级套件）500ms 不够 → 随机 `ConnectionRefused` 假红。
    /// 这里改为有界重试（上限 15s），既不再依赖机器负载，也不会无限等。
    async fn connect_ready(addr: &str) -> tonic::transport::Channel {
        let endpoint = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .expect("endpoint");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            match endpoint.clone().connect().await {
                Ok(channel) => return channel,
                Err(e) => {
                    if tokio::time::Instant::now() >= deadline {
                        panic!("agent gRPC endpoint {addr} never became ready: {e}");
                    }
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn js_plugin_writes_to_real_server_through_host_sdk() {
        let (server_addr, _shutdown, _server_tasks, _server_tmp) = start_test_server().await;
        let (agent_addr, agent_handle, _plugin_dir, _agent_tmp) =
            start_agent_with_plugin(&server_addr).await;

        let mut plugin = plugin_client(&agent_addr).await;

        // List：插件已加载并运行
        let list = plugin
            .list(ListPluginsRequest {})
            .await
            .expect("list")
            .into_inner();
        let counter = list
            .plugins
            .iter()
            .find(|p| p.name == "counter")
            .expect("counter plugin listed");
        assert_eq!(counter.runtime, "js");
        assert_eq!(counter.status, "started");

        // Invoke → 插件内 coord.kv.put → 真实 server
        let resp = plugin
            .invoke(InvokeRequest {
                plugin_id: "counter".into(),
                method: "put".into(),
                payload: b"42".to_vec(),
            })
            .await
            .expect("invoke put")
            .into_inner();
        let revision: u64 = String::from_utf8_lossy(&resp.payload)
            .parse()
            .expect("revision payload");
        assert!(revision > 0, "expected server revision, got {revision}");

        // 直接查 server 验证落盘
        let mut kv = kv_client(&server_addr).await;
        let range = kv
            .range(RangeRequest {
                key: b"/app/counter/a".to_vec(),
                ..Default::default()
            })
            .await
            .expect("range")
            .into_inner();
        assert_eq!(range.kvs.len(), 1, "plugin write should be persisted");
        assert_eq!(range.kvs[0].value, b"42");

        // 插件内读回（走插件自己的 SDK 面）
        let resp = plugin
            .invoke(InvokeRequest {
                plugin_id: "counter".into(),
                method: "get".into(),
                payload: Vec::new(),
            })
            .await
            .expect("invoke get")
            .into_inner();
        assert_eq!(resp.payload, b"42");

        agent_handle.abort();
    }

    /// 批次 11：插件**未知长度**流式上传（`openWrite` 省略 `totalSize`）经真实
    /// server：`totalSize === null`，`commit` 返回的实际字节数 = 8。
    #[tokio::test(flavor = "multi_thread")]
    async fn js_plugin_streams_unknown_length_object_to_real_server() {
        let (server_addr, _shutdown, _server_tasks, _server_tmp) = start_test_server().await;
        let (agent_addr, agent_handle, _plugin_dir, _agent_tmp) =
            start_agent_with_plugin(&server_addr).await;

        let mut plugin = plugin_client(&agent_addr).await;
        let resp = plugin
            .invoke(InvokeRequest {
                plugin_id: "counter".into(),
                method: "storage-unknown".into(),
                payload: Vec::new(),
            })
            .await
            .expect("invoke storage-unknown")
            .into_inner();
        assert_eq!(
            String::from_utf8_lossy(&resp.payload),
            "null,8",
            "unknown-length upload must commit with the actual byte count"
        );

        agent_handle.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn plugin_scope_violation_is_rejected_before_reaching_server() {
        let (server_addr, _shutdown, _server_tasks, _server_tmp) = start_test_server().await;
        let (agent_addr, agent_handle, _plugin_dir, _agent_tmp) =
            start_agent_with_plugin(&server_addr).await;

        let mut plugin = plugin_client(&agent_addr).await;
        let err = plugin
            .invoke(InvokeRequest {
                plugin_id: "counter".into(),
                method: "outside".into(),
                payload: Vec::new(),
            })
            .await
            .expect_err("out-of-scope write must fail");
        assert!(
            err.message().contains("ErrForbidden"),
            "expected ErrForbidden, got {}",
            err.message()
        );

        // 越界 key 未落盘
        let mut kv = kv_client(&server_addr).await;
        let range = kv
            .range(RangeRequest {
                key: b"/other/x".to_vec(),
                ..Default::default()
            })
            .await
            .expect("range")
            .into_inner();
        assert!(range.kvs.is_empty(), "out-of-scope write must not persist");

        agent_handle.abort();
    }

    /// Phase 3.3（部分）验收：内置 JS SDK 的 `coord.idgen.next` 在真实 server 上
    /// 单调递增（txn version-CAS 语义正确，包括「键不存在」的创建分支）。
    #[tokio::test(flavor = "multi_thread")]
    async fn js_stdlib_idgen_is_monotonic_against_real_server() {
        let (server_addr, _shutdown, _server_tasks, _server_tmp) = start_test_server().await;
        let (agent_addr, agent_handle, _plugin_dir, _agent_tmp) =
            start_agent_with_plugin(&server_addr).await;

        let mut plugin = plugin_client(&agent_addr).await;
        let resp = plugin
            .invoke(InvokeRequest {
                plugin_id: "counter".into(),
                method: "idgen".into(),
                payload: Vec::new(),
            })
            .await
            .expect("invoke idgen")
            .into_inner();
        assert_eq!(resp.payload, b"1,2", "idgen must be monotonic");

        // 计数已落盘，可被其它客户端观察到
        let mut kv = kv_client(&server_addr).await;
        let range = kv
            .range(RangeRequest {
                key: b"/app/counter/id".to_vec(),
                ..Default::default()
            })
            .await
            .expect("range")
            .into_inner();
        assert_eq!(range.kvs.len(), 1);
        assert_eq!(range.kvs[0].value, b"2");

        agent_handle.abort();
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn invoking_unknown_plugin_is_not_found() {
        let (server_addr, _shutdown, _server_tasks, _server_tmp) = start_test_server().await;
        let (agent_addr, agent_handle, _plugin_dir, _agent_tmp) =
            start_agent_with_plugin(&server_addr).await;

        let mut plugin = plugin_client(&agent_addr).await;
        let err = plugin
            .invoke(InvokeRequest {
                plugin_id: "missing".into(),
                method: "put".into(),
                payload: Vec::new(),
            })
            .await
            .expect_err("unknown plugin");
        assert_eq!(err.code(), tonic::Code::NotFound);

        agent_handle.abort();
    }
}
