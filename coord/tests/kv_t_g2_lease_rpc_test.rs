//! KV-T G2（#145）`coord.lease.v2` 验收：
//!
//! - region 0 租约表：Grant / KeepAlive / TimeToLive / Revoke；
//! - KV v2 `lease_id` 绑定，revoke/到期后按 key 所在 Region 清理；
//! - 重复 Grant（同 request_id）/重复 Revoke 幂等；显式 id 冲突不覆盖；
//! - revoker 启动扫描续跑已 revoke 但未清理的记录；
//! - `[kv_t].enabled=false` 或 Lease 能力未交付时门禁 `FAILED_PRECONDITION`。

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};
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
    use coord_server::kvt::decision_apply::apply_pending_decisions;
    use coord_server::kvt::sequencer::{CommitCandidate, CommitSequencer};
    use coord_server::kvt::{KvTCapability, KvTRuntimeConfig, KvTState};
    use coord_server::raft::log_store::LogStore;
    use coord_server::raft::network::{RaftNetworkFactoryImpl, RaftRpcService};
    use coord_server::raft::region::RegionManager;
    use coord_server::raft::region_runtime::{region_data_dir, RegionRuntimeSpec};
    use coord_server::raft::state_machine::StateMachineStore;
    use coord_server::raft::tso::{TsoBatch, TsoClient, TsoSource};
    use coord_server::raft::type_config::{
        Command, CommitDecisionApplyLog, LeaseV2Op, Response, SequencerMutation, SequencerOutcome,
        SequencerValidationLimits,
    };
    use coord_server::raft::{new_basic_node, new_raft, RaftConfig};
    use coord_server::server::CoordNode;
    use coord_server::storage::commit_log::COMMIT_LOG_STAGING_SHARD_ID;
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
            // 直写次序守卫只把 `commit_ts - 1` 发布到 TSO 已发放高水位之内；
            // 进程内 TSO 不落高水位，这里（`initialize` 之后，否则 applied 标记会让
            // raft 拒绝初始化）给足发放空间，否则守卫会把合法直写判成「时间戳高于
            // 本集群已发放范围」。
            mvcc.apply_tso_allocate(1024, 1, AppliedLogId::standalone(0))
                .expect("raise TSO high water");

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
                        // 租约用例验证的是「清理/直写是否越过 pending 决策」，
                        // 不是 fence 发布延迟；50ms 在 CI 并发负载下会偶发
                        // SAFE_TS_LAG 超时（put 本身应该成功）。
                        read_ts_wait_ms: 5_000,
                        ..KvTRuntimeConfig::default()
                    },
                )
                .with_implemented([
                    KvTCapability::Kv,
                    KvTCapability::Tso,
                    KvTCapability::Txn,
                    KvTCapability::Lease,
                ]),
            );
            // sequencer 与租约 RPC 共用同一个 TSO 客户端：`commit_ts` 顺序决定
            // 「清理是否越过未 apply 的决策」。
            let tso = TsoClient::new(
                Arc::new(FakeTso {
                    high_water: AtomicU64::new(1),
                }),
                8,
                None,
            );
            node.set_tso_client(Arc::clone(&tso));
            node.set_commit_sequencer(Arc::new(CommitSequencer::new(
                root_raft.as_ref().clone(),
                1,
                tso,
                Arc::clone(&mvcc),
                256,
                4 * 1024 * 1024,
                SequencerValidationLimits {
                    max_read_set: 4_096,
                    max_range: 10_000,
                },
                Duration::from_secs(5),
            )));
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

        /// 写一条事务决策到 region 0，但**不** apply 到数据 Region。
        async fn commit(&self, txn: &[u8], writes: &[(&[u8], &[u8])]) -> u64 {
            let sequencer = self.node.commit_sequencer().expect("commit sequencer");
            let outcomes = sequencer
                .validate_and_commit(vec![CommitCandidate {
                    txn_id: txn.to_vec(),
                    start_ts: 0,
                    request_id: txn.to_vec(),
                    read_set: BTreeSet::new(),
                    read_ranges: Vec::new(),
                    mutations: writes
                        .iter()
                        .map(|(key, value)| SequencerMutation::Put {
                            key: key.to_vec(),
                            value: value.to_vec(),
                            lease_id: None,
                        })
                        .collect(),
                }])
                .await
                .expect("validate and commit");
            match outcomes.into_iter().next() {
                Some(SequencerOutcome::Committed { commit_ts }) => commit_ts,
                other => panic!("expected Committed, got {other:?}"),
            }
        }

        /// 把一条决策只 apply 到某个 Region（决策逐 Region 到达，不同步）。
        async fn apply_decision_on(&self, region_id: u64, commit_ts: u64, txn: &[u8]) {
            let rt = self.manager.runtime(region_id).expect("region runtime");
            rt.raft
                .client_write(Command::ApplyCommitDecisionsWithLog {
                    max_txn_bytes: 4 * 1024 * 1024,
                    decisions: vec![CommitDecisionApplyLog {
                        shard_id: COMMIT_LOG_STAGING_SHARD_ID,
                        region_id,
                        commit_ts,
                        txn_id: txn.to_vec(),
                        start_ts: 0,
                        mutations: Vec::new(),
                    }],
                })
                .await
                .expect("apply decision on one region");
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
        // 守卫每拍最多等 `read_ts_wait_ms`（5s）才轮到下一个 Region 的清理；
        // 这里只验证最终被清理，不用 6s 的紧上限赌调度。
        for _ in 0..600 {
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
                // applied 标记不能假装在 raft 日志前面：`applied_contains` 按
                // index 判定「已应用」，提前记 10_000 会让 region 0 后续的
                // raft 条目全被当成重放（fence 不再发布）。
                AppliedLogId::standalone(0),
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

    /// lease revoke 也是「调用方自带 `commit_ts` 的直写」：事务 commit 后立刻
    /// revoke，清理若先落盘，Region 的 `current_revision` 会越过尚未 apply 的
    /// 决策 ⇒ 决策永久无法 apply、`safe_ts` 卡死。守卫必须把这个 Region 留在
    /// pending，下一拍用**同一** `commit_ts` 重试。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_lease_revoke_does_not_pass_a_pending_decision() {
        let h = G2Node::start().await;
        let mut lease = h.lease().await;

        let grant = lease
            .lease_grant(LeaseGrantRequest {
                ttl_seconds: 60,
                lease_id: 0,
                request_id: b"revoke-behind-decision".to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        let put_ts = h.put_bound(b"apple", b"a", grant.lease_id).await;
        // 决策已落 region 0，但还没 apply 到数据 Region。
        let decision_ts = h.commit(b"txn-behind-decision", &[(b"apple2", b"b")]).await;
        assert!(
            decision_ts > put_ts,
            "the decision must commit after the direct write: {decision_ts} vs {put_ts}"
        );

        let revoked = lease
            .lease_revoke(LeaseRevokeRequest {
                lease_id: grant.lease_id,
                request_id: Vec::new(),
            })
            .await
            .unwrap()
            .into_inner();
        let revoke_ts = u64::try_from(revoked.commit_ts).expect("positive revoke ts");
        assert!(
            revoke_ts > decision_ts,
            "the revoke must be ordered after the decision: {revoke_ts} vs {decision_ts}"
        );

        // 清理停在决策之后：不推 Region 水位、不删 key、不重取时间戳。
        let region = h.manager.runtime(1).expect("region 1 runtime");
        for _ in 0..6 {
            tokio::time::sleep(Duration::from_millis(50)).await;
            assert!(
                h.key_live(b"apple"),
                "lease cleanup must wait for the pending decision"
            );
            assert_eq!(
                region.mvcc.current_revision(),
                put_ts,
                "the deferred cleanup must not push the region watermark"
            );
        }
        assert_eq!(
            region.mvcc.last_applied_decision_ts().unwrap(),
            0,
            "the decision is still pending"
        );

        // 决策照常 apply；之后清理用同一 `commit_ts` 追上，key 最终删除。
        assert_eq!(
            apply_pending_decisions(&h.node, 16)
                .await
                .expect("apply decisions"),
            2,
            "one apply command per region"
        );
        assert_eq!(
            region.mvcc.last_applied_decision_ts().unwrap(),
            decision_ts,
            "the decision applied once the cleanup stopped blocking it"
        );
        wait_gone(&h, b"apple").await;
        assert!(h.key_live(b"apple2"), "the committed row must stay");
        assert!(
            h.node.safe_ts() >= decision_ts,
            "safe_ts must catch up with the applied decision, got {}",
            h.node.safe_ts()
        );
        assert_eq!(
            h.node
                .storage
                .get_lease_v2_record(grant.lease_id)
                .unwrap()
                .expect("revoke record")
                .revoked_commit_ts,
            revoke_ts,
            "the deferred cleanup must keep its commit_ts",
        );
    }

    /// 跨 Region 扇出时逐 Region 互不阻塞：Region 1 的清理被 pending 决策挡住
    /// 不能拖住 Region 2 的清理。
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_lease_cleanup_is_per_region() {
        let h = G2Node::start().await;
        let mut lease = h.lease().await;

        let blocked = lease
            .lease_grant(LeaseGrantRequest {
                ttl_seconds: 60,
                lease_id: 0,
                request_id: b"per-region-blocked".to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        let free = lease
            .lease_grant(LeaseGrantRequest {
                ttl_seconds: 60,
                lease_id: 0,
                request_id: b"per-region-free".to_vec(),
            })
            .await
            .unwrap()
            .into_inner();
        let blocked_ts = h.put_bound(b"apple", b"a", blocked.lease_id).await;
        h.put_bound(b"melon", b"m", free.lease_id).await;
        // 只有 Region 1 有未 apply 的决策。
        let decision_ts = h.commit(b"txn-per-region", &[(b"apple2", b"b")]).await;
        assert!(
            decision_ts > blocked_ts,
            "the decision must commit after the bound put: {decision_ts} vs {blocked_ts}"
        );
        // 决策先到 Region 2（没有本 Region key，只推游标）；Region 1 还停在它之前。
        h.apply_decision_on(2, decision_ts, b"txn-per-region").await;

        for (lease_id, request_id) in [
            (blocked.lease_id, b"revoke-blocked".to_vec()),
            (free.lease_id, b"revoke-free".to_vec()),
        ] {
            lease
                .lease_revoke(LeaseRevokeRequest {
                    lease_id,
                    request_id,
                })
                .await
                .unwrap();
        }
        let blocked_revoke_ts = h
            .node
            .storage
            .get_lease_v2_record(blocked.lease_id)
            .unwrap()
            .expect("blocked record")
            .revoked_commit_ts;

        // Region 2 的清理照常完成，Region 1 继续等决策 apply。
        wait_gone(&h, b"melon").await;
        assert!(
            h.key_live(b"apple"),
            "a pending decision on one region must not block another region's cleanup"
        );
        assert_eq!(
            h.manager
                .runtime(1)
                .expect("region 1 runtime")
                .mvcc
                .current_revision(),
            blocked_ts,
            "region 1 must stay behind its pending decision"
        );

        assert_eq!(
            apply_pending_decisions(&h.node, 16)
                .await
                .expect("apply decisions"),
            1,
            "only region 1 still has the decision to apply"
        );
        wait_gone(&h, b"apple").await;
        assert_eq!(
            h.node
                .storage
                .get_lease_v2_record(blocked.lease_id)
                .unwrap()
                .expect("blocked record")
                .revoked_commit_ts,
            blocked_revoke_ts,
            "waiting must not re-allocate the cleanup commit_ts",
        );
        assert!(h.node.safe_ts() >= decision_ts);
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
