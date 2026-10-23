//! KV-T G2（#145）`coord.lease.v2` 验收：
//!
//! - region 0 租约表：Grant / KeepAlive / TimeToLive / Revoke；
//! - KV v2 `lease_id` 绑定，revoke/到期后按 key 所在 Region 清理；
//! - 重复 Grant（同 request_id）/重复 Revoke 幂等；显式 id 冲突不覆盖；
//! - revoker 启动扫描续跑已 revoke 但未清理的记录；
//! - `[kv_t].enabled=false` 或 Lease 能力未交付时门禁 `FAILED_PRECONDITION`。

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::SocketAddr;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::Arc;
    use std::time::Duration;

    use async_trait::async_trait;

    use coord_core::error::Result as CoordResult;
    use coord_core::storage::StorageBackend;
    use coord_core::types::{Peer, PeerRole, RegionEpoch, RegionMeta, StorageConfig};
    use coord_proto::kv::v2::kv_client::KvClient;
    use coord_proto::kv::v2::kv_server::KvServer;
    use coord_proto::kv::v2::PutRequest as KvPutRequest;
    use coord_proto::lease::v2::lease_client::LeaseClient;
    use coord_proto::lease::v2::lease_server::{Lease as LeaseV2, LeaseServer};
    use coord_proto::lease::v2::{
        LeaseGrantRequest, LeaseKeepAliveRequest, LeaseRevokeRequest, LeaseTimeToLiveRequest,
    };
    use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
    use coord_server::raft::log_store::LogStore;
    use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
    use coord_server::raft::region::RegionManager;
    use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
    use coord_server::raft::state_machine::StateMachineStore;
    use coord_server::raft::tso::{TsoBatch, TsoClient, TsoSource};
    use coord_server::raft::type_config::{Command, LeaseV2Op, Response};
    use coord_server::raft::{new_basic_node, new_raft, RaftConfig};
    use coord_server::server::CoordNode;
    use coord_server::storage::mvcc::{AppliedLogId, MvccStorage, RevisionMode};
    use coord_server::storage::redb_backend::RedbBackend;
    use coord_server::storage::snapshot::SnapshotTracker;
    use tempfile::TempDir;
    use tokio::net::TcpListener;
    use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
    use tonic::transport::Server;
    use tonic::Code;

    fn find_port() -> u16 {
        std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port()
    }

    fn raft_config() -> RaftConfig {
        RaftConfig {
            heartbeat_interval: 100,
            election_timeout_min: 400,
            election_timeout_max: 800,
            ..Default::default()
        }
    }

    struct FakeTso {
        high_water: AtomicU64,
    }

    #[async_trait]
    impl TsoSource for FakeTso {
        async fn is_leader(&self) -> bool {
            true
        }

        async fn allocate(&self, batch_size: u32) -> CoordResult<TsoBatch> {
            let start = self
                .high_water
                .fetch_add(u64::from(batch_size), Ordering::SeqCst);
            Ok(TsoBatch {
                start_ts: start,
                end_ts: start + u64::from(batch_size),
            })
        }
    }

    /// 单节点：region 0 system raft（租约表）+ 2 个数据 Region + 真实 gRPC 服务。
    struct G2Node {
        node: Arc<CoordNode>,
        manager: Arc<RegionManager>,
        addr: SocketAddr,
        _dir: TempDir,
        _factory: RaftNetworkFactoryImpl,
        _rpc: RaftRpcService,
        _server: tokio::task::JoinHandle<()>,
    }

    impl G2Node {
        async fn start() -> Self {
            let dir = tempfile::tempdir().unwrap();
            let base = dir.path().to_path_buf();
            let raft_addr = format!("127.0.0.1:{}", find_port());

            let root_dir = base.join("region-0");
            std::fs::create_dir_all(&root_dir).unwrap();
            let backend = RedbBackend::open(&root_dir, &StorageConfig::default()).unwrap();
            let mvcc = Arc::new(
                MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap(),
            );
            let root_factory = RaftNetworkFactoryImpl::new(1);
            root_factory.register_node(1, raft_addr.clone());
            let root_tracker = Arc::new(SnapshotTracker::default());
            let root_log = LogStore::new(&root_dir).await.unwrap();
            let mut root_sm =
                StateMachineStore::new(Arc::clone(&mvcc), root_dir.join("snapshots"), root_tracker);
            let (revoke_tx, revoke_rx) = tokio::sync::mpsc::unbounded_channel();
            root_sm.set_lease_v2_revoke_tx(revoke_tx);
            let root_sm = root_sm;
            let root_raft = Arc::new(
                new_raft(1, Arc::new(raft_config()), root_factory, root_log, root_sm)
                    .await
                    .expect("create region 0 raft"),
            );
            root_raft
                .initialize(BTreeMap::from([(1u64, new_basic_node(&raft_addr))]))
                .await
                .expect("initialize region 0 raft");

            let factory = RaftNetworkFactoryImpl::new(1);
            factory.register_node(1, raft_addr);
            let rpc = RaftRpcService::new();
            let manager = Arc::new(RegionManager::new(1));
            for (region_id, start, end) in [
                (1u64, b"".to_vec(), b"m".to_vec()),
                (2u64, b"m".to_vec(), b"".to_vec()),
            ] {
                let meta = RegionMeta {
                    region_id,
                    start_key: start,
                    end_key: end,
                    epoch: RegionEpoch::initial(),
                    peers: vec![Peer {
                        node_id: 1,
                        raft_addr: format!("127.0.0.1:{}", find_port()),
                        role: PeerRole::Voter,
                    }],
                    approximate_size: 0,
                    approximate_keys: 0,
                };
                manager
                    .spawn_region(
                        &factory,
                        &rpc,
                        RegionRuntimeSpec {
                            meta,
                            data_dir: region_data_dir(&base, region_id),
                            raft_config: Arc::new(raft_config()),
                            object_store: None,
                            revision_mode: RevisionMode::CommitTs,
                        },
                        true,
                    )
                    .await
                    .expect("spawn data region");
            }

            let mut node = CoordNode::new(Arc::clone(&mvcc));
            node.node_id = 1;
            node.raft = Some(Arc::clone(&root_raft));
            node.region_manager = Some(Arc::clone(&manager));
            node.set_kvt_state(
                KvTState::new(
                    true,
                    KvTRuntimeConfig {
                        read_ts_wait_ms: 50,
                        ..KvTRuntimeConfig::default()
                    },
                )
                .with_implemented([
                    KvTCapability::Kv,
                    KvTCapability::Tso,
                    KvTCapability::Lease,
                ]),
            );
            node.set_tso_client(TsoClient::new(
                Arc::new(FakeTso {
                    high_water: AtomicU64::new(1),
                }),
                8,
                None,
            ));
            let node = Arc::new(node);
            wait_for_leaders(&node, &manager).await;
            node.start_kvt_lease_expiry_worker();
            node.start_kvt_lease_revoker(revoke_rx);

            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let server_node = Arc::clone(&node);
            let server = tokio::spawn(async move {
                Server::builder()
                    .add_service(LeaseServer::from_arc(Arc::clone(&server_node)))
                    .add_service(KvServer::from_arc(server_node))
                    .serve_with_incoming(TcpListenerStream::new(listener))
                    .await
                    .unwrap();
            });
            tokio::time::sleep(Duration::from_millis(100)).await;

            Self {
                node,
                manager,
                addr,
                _dir: dir,
                _factory: factory,
                _rpc: rpc,
                _server: server,
            }
        }

        async fn lease(&self) -> LeaseClient<tonic::transport::Channel> {
            LeaseClient::connect(format!("http://{}", self.addr))
                .await
                .unwrap()
        }

        async fn kv(&self) -> KvClient<tonic::transport::Channel> {
            KvClient::connect(format!("http://{}", self.addr))
                .await
                .unwrap()
        }

        async fn next_ts(&self) -> u64 {
            self.node
                .tso_client()
                .expect("tso client")
                .get_ts_above(0)
                .await
                .expect("next ts")
        }

        async fn put_bound(&self, key: &[u8], value: &[u8], lease_id: i64) -> u64 {
            let commit_ts = self.next_ts().await;
            let mut kv = self.kv().await;
            kv.put(KvPutRequest {
                key: key.to_vec(),
                value: value.to_vec(),
                lease_id,
                commit_ts: commit_ts as i64,
                ..Default::default()
            })
            .await
            .expect("put with lease");
            commit_ts
        }

        fn key_live(&self, key: &[u8]) -> bool {
            let rt = self.manager.route_runtime(key).expect("route key");
            rt.mvcc.get(key).unwrap().is_some()
        }
    }

    async fn wait_for_leaders(node: &Arc<CoordNode>, manager: &Arc<RegionManager>) {
        for _ in 0..100 {
            let root_ok = node.raft.as_ref().unwrap().current_leader().await == Some(1);
            let mut regions_ok = true;
            for rt in manager.list_runtimes() {
                if rt.raft.current_leader().await.is_none() {
                    regions_ok = false;
                    break;
                }
            }
            if root_ok && regions_ok {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("leaders not elected");
    }

    async fn wait_gone(h: &G2Node, key: &[u8]) {
        for _ in 0..120 {
            if !h.key_live(key) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!(
            "key {:?} still live after revoke/expiry",
            String::from_utf8_lossy(key)
        );
    }

    #[tokio::test]
    async fn test_lease_v2_multiregion_revoke_and_idempotency() {
        let h = G2Node::start().await;
        let mut lease = h.lease().await;

        let grant = lease
            .lease_grant(LeaseGrantRequest {
                ttl_seconds: 60,
                lease_id: 0,
                request_id: b"grant-1".to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(grant.lease_id > 0);
        assert_eq!(grant.ttl_seconds, 60);
        assert!(grant.granted_ts > 0);

        // 同 request_id 重试返回同一租约，不重复分配。
        let replay = lease
            .lease_grant(LeaseGrantRequest {
                ttl_seconds: 60,
                lease_id: 0,
                request_id: b"grant-1".to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(replay.lease_id, grant.lease_id);
        assert_eq!(replay.granted_ts, grant.granted_ts);

        // 显式 id 冲突：fail-closed，不覆盖已有租约。
        let mut explicit = lease.clone();
        explicit
            .lease_grant(LeaseGrantRequest {
                ttl_seconds: 60,
                lease_id: 777_001,
                request_id: b"grant-explicit".to_vec(),
            })
            .await
            .unwrap();
        let conflict = explicit
            .lease_grant(LeaseGrantRequest {
                ttl_seconds: 60,
                lease_id: 777_001,
                request_id: b"grant-explicit-2".to_vec(),
            })
            .await
            .unwrap_err();
        assert_eq!(conflict.code(), Code::AlreadyExists);

        h.put_bound(b"a-key", b"a", grant.lease_id).await;
        h.put_bound(b"n-key", b"n", grant.lease_id).await;

        // KeepAlive 流：返回剩余 TTL（非配置 TTL）。
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(LeaseKeepAliveRequest {
            lease_id: grant.lease_id,
        })
        .await
        .unwrap();
        let mut stream = lease
            .lease_keep_alive(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        let alive = tokio::time::timeout(Duration::from_secs(2), stream.message())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(alive.lease_id, grant.lease_id);
        assert!(alive.ttl_seconds > 0 && alive.ttl_seconds <= 60);
        drop(stream);
        drop(tx);

        let ttl = lease
            .lease_time_to_live(LeaseTimeToLiveRequest {
                lease_id: grant.lease_id,
                keys: true,
            })
            .await
            .unwrap()
            .into_inner();
        assert!(ttl.ttl_seconds > 0);
        assert_eq!(ttl.granted_ttl_seconds, 60);
        let mut keys = ttl.keys;
        keys.sort();
        assert_eq!(keys, vec![b"a-key".to_vec(), b"n-key".to_vec()]);

        let revoked = lease
            .lease_revoke(LeaseRevokeRequest {
                lease_id: grant.lease_id,
                request_id: b"revoke-1".to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        assert!(revoked.commit_ts > 0);
        wait_gone(&h, b"a-key").await;
        wait_gone(&h, b"n-key").await;

        // 重复 revoke 幂等：返回 0，且不会重复删。
        let again = lease
            .lease_revoke(LeaseRevokeRequest {
                lease_id: grant.lease_id,
                request_id: b"revoke-1".to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(again.commit_ts, 0);

        // 已 revoke 的租约不能再次绑定写入。
        let commit_ts = h.next_ts().await;
        let mut kv = h.kv().await;
        let err = kv
            .put(KvPutRequest {
                key: b"late-key".to_vec(),
                value: b"x".to_vec(),
                lease_id: grant.lease_id,
                commit_ts: commit_ts as i64,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(!h.key_live(b"late-key"));
    }

    #[tokio::test]
    async fn test_lease_v2_unknown_lease_errors() {
        let h = G2Node::start().await;
        let mut lease = h.lease().await;

        let revoked = lease
            .lease_revoke(LeaseRevokeRequest {
                lease_id: 424_242,
                request_id: Vec::new(),
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(revoked.commit_ts, 0);

        let ttl_err = lease
            .lease_time_to_live(LeaseTimeToLiveRequest {
                lease_id: 424_242,
                keys: false,
            })
            .await
            .unwrap_err();
        assert_eq!(ttl_err.code(), Code::NotFound);

        let (tx, rx) = tokio::sync::mpsc::channel(4);
        tx.send(LeaseKeepAliveRequest { lease_id: 424_242 })
            .await
            .unwrap();
        let mut stream = lease
            .lease_keep_alive(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        let err = tokio::time::timeout(Duration::from_secs(2), stream.message())
            .await
            .unwrap()
            .unwrap_err();
        assert_eq!(err.code(), Code::NotFound);

        // 未知 lease 的 KV 绑定 fail-closed。
        let commit_ts = h.next_ts().await;
        let mut kv = h.kv().await;
        let err = kv
            .put(KvPutRequest {
                key: b"unbound".to_vec(),
                value: b"x".to_vec(),
                lease_id: 424_242,
                commit_ts: commit_ts as i64,
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
    }

    #[tokio::test]
    async fn test_lease_v2_expiry_deletes_bound_keys() {
        let h = G2Node::start().await;
        let mut lease = h.lease().await;

        let grant = lease
            .lease_grant(LeaseGrantRequest {
                ttl_seconds: 1,
                lease_id: 0,
                request_id: b"expiring".to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        h.put_bound(b"expiring-key", b"v", grant.lease_id).await;
        assert!(h.key_live(b"expiring-key"));

        wait_gone(&h, b"expiring-key").await;

        // 已吊销/过期清理后的租约对调用方不存在。
        let ttl_err = lease
            .lease_time_to_live(LeaseTimeToLiveRequest {
                lease_id: grant.lease_id,
                keys: true,
            })
            .await
            .unwrap_err();
        assert_eq!(ttl_err.code(), Code::NotFound);
    }

    #[tokio::test]
    async fn test_lease_v2_revoker_resumes_after_decision_without_cleanup() {
        let h = G2Node::start().await;
        let mut lease = h.lease().await;

        let grant = lease
            .lease_grant(LeaseGrantRequest {
                ttl_seconds: 60,
                lease_id: 0,
                request_id: b"resume".to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        h.put_bound(b"resume-key", b"v", grant.lease_id).await;

        // 模拟「region 0 已写 revoke 决策、清理尚未执行」：直接 apply 状态机
        // 记录，不经过 raft（因此没有 revoke 广播），key 仍在。
        let commit_ts = h.next_ts().await;
        h.node
            .storage
            .apply_lease_v2_op(
                &LeaseV2Op::Revoke {
                    lease_id: grant.lease_id,
                    commit_ts,
                    expected_deadline_wall_ms: 0,
                    request_id: Vec::new(),
                },
                10_000,
                AppliedLogId {
                    term: 1,
                    node_id: 1,
                    index: 10_000,
                },
            )
            .unwrap();
        assert!(h.key_live(b"resume-key"));

        // 重启后的 revoker：启动扫描已 revoke 记录并续跑清理。
        let (_tx, rx) = tokio::sync::mpsc::unbounded_channel();
        h.node.start_kvt_lease_revoker(rx);
        wait_gone(&h, b"resume-key").await;
    }

    /// 迁移窗口内的租约清理必须整体延后：fence 的 Region 既不落删除，也不为
    /// 同一条清理重新取 `commit_ts`；窗口结束（fence 清除）后按当时的表重扇出，
    /// key 才被清掉。
    #[tokio::test]
    async fn test_lease_v2_cleanup_defers_on_fenced_region_until_cutover() {
        let h = G2Node::start().await;
        let mut lease = h.lease().await;

        let grant = lease
            .lease_grant(LeaseGrantRequest {
                ttl_seconds: 60,
                lease_id: 0,
                request_id: b"defer-on-migration".to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        h.put_bound(b"a-fenced", b"v", grant.lease_id).await;
        h.put_bound(b"m-fenced", b"v", grant.lease_id).await;

        // Region 2 进入迁移窗口（split 源 / merge donor 的 fence 标记）。
        let region = h.manager.runtime(2).expect("region 2 runtime");
        let armed = region
            .raft
            .client_write(Command::FenceDirectWrites {
                start_key: b"m".to_vec(),
                end_key: Vec::new(),
                op_id: 77,
            })
            .await
            .expect("arm migration fence")
            .response()
            .clone();
        assert!(matches!(armed, Response::DirectWritesFenced { op_id: 77 }));

        lease
            .lease_revoke(LeaseRevokeRequest {
                lease_id: grant.lease_id,
                request_id: Vec::new(),
            })
            .await
            .unwrap();
        let recorded_ts = h
            .node
            .storage
            .get_lease_v2_record(grant.lease_id)
            .unwrap()
            .expect("revoke record")
            .revoked_commit_ts;

        // 未 fence 的 Region 立即清理；fence 的 Region 延后。
        wait_gone(&h, b"a-fenced").await;
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            h.key_live(b"m-fenced"),
            "a region inside a migration window must defer lease cleanup"
        );
        assert!(
            region.mvcc.direct_write_fence().unwrap().is_some(),
            "the window is still open"
        );
        assert_eq!(
            h.node
                .storage
                .get_lease_v2_record(grant.lease_id)
                .unwrap()
                .expect("revoke record")
                .revoked_commit_ts,
            recorded_ts,
            "the deferred cleanup must not re-allocate commit_ts while fenced",
        );

        // cutover 完成（fence 清除）：清理按当时的表重扇出并落在这个 Region。
        region
            .raft
            .client_write(Command::ClearDirectWriteFence { op_id: 77 })
            .await
            .expect("clear migration fence");
        wait_gone(&h, b"m-fenced").await;
    }

    #[tokio::test]
    async fn test_lease_v2_gate_without_capability() {
        let dir = tempfile::tempdir().unwrap();
        let backend = RedbBackend::open(dir.path(), &StorageConfig::default()).unwrap();
        let mvcc =
            Arc::new(MvccStorage::new_with_revision_mode(backend, RevisionMode::CommitTs).unwrap());
        let mut node = CoordNode::new(mvcc);
        node.set_kvt_state(
            KvTState::new(true, KvTRuntimeConfig::default()).with_implemented([KvTCapability::Kv]),
        );
        let node = Arc::new(node);

        let err = node
            .lease_grant(tonic::Request::new(LeaseGrantRequest {
                ttl_seconds: 30,
                lease_id: 0,
                request_id: Vec::new(),
            }))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::FailedPrecondition);
    }
}
