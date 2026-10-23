//! KV-T G6（#172）Rust client 的 `coord.lease.v2` / `coord.storage.v2`：
//!
//! - 语义字段按契约透传（`request_id`、`read_ts`、`commit_ts`、chunk 顺序）；
//! - 首次请求不带 routing epoch，`EPOCH_STALE` 后学新版本并重试（重试带新版本，
//!   后续请求直接携带）；
//! - 本地先拒绝超出 i64 wire 范围的时间戳，不发请求；
//! - `NOT_FOUND` 等业务结果不被 epoch 重试掩盖。

use std::str::FromStr;
use std::sync::{Arc, Mutex};

use coord_client::Client;
use coord_core::error::Error;
use coord_core::error_code::{attach, CoordErrorCode};
use coord_proto::lease::v2 as lease;
use coord_proto::storage::v2 as storage;
use storage::get_response;
use storage::put_request;
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::metadata::MetadataValue;
use tonic::transport::Server;
use tonic::{Request, Response, Status, Streaming};

const EPOCH: &str = "coord-routing-epoch";

#[derive(Default)]
struct Calls {
    grant: Vec<(Option<u64>, i64, i64, Vec<u8>)>,
    revoke: Vec<(Option<u64>, i64, Vec<u8>)>,
    keep_alive: Vec<i64>,
    ttl: Vec<(i64, bool)>,
    put: Vec<(Option<storage::PutMeta>, Vec<usize>)>,
    put_epochs: Vec<Option<u64>>,
    get: Vec<(Option<u64>, i64)>,
    stat: Vec<i64>,
    delete: Vec<(Option<u64>, Vec<u8>)>,
    grant_stale: usize,
    put_stale: usize,
}

fn epoch_of<T>(request: &Request<T>) -> Option<u64> {
    request
        .metadata()
        .get(EPOCH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse().ok())
}

/// 与服务端 `guard_routing_epoch` 同形：`UNAVAILABLE` + `EPOCH_STALE` 码 + trailer。
fn stale_epoch(server: &str) -> Status {
    let mut status = attach(
        Status::unavailable("stale epoch; refresh route table"),
        CoordErrorCode::EpochStale,
    );
    status.metadata_mut().insert(
        EPOCH,
        MetadataValue::from_str(server).expect("ascii metadata"),
    );
    status
}

struct Mock {
    calls: Arc<Mutex<Calls>>,
}

#[tonic::async_trait]
impl lease::lease_server::Lease for Mock {
    type LeaseKeepAliveStream = ReceiverStream<Result<lease::LeaseKeepAliveResponse, Status>>;

    async fn lease_grant(
        &self,
        request: Request<lease::LeaseGrantRequest>,
    ) -> Result<Response<lease::LeaseGrantResponse>, Status> {
        let epoch = epoch_of(&request);
        let inner = request.into_inner();
        let mut calls = self.calls.lock().expect("calls");
        calls.grant.push((
            epoch,
            inner.ttl_seconds,
            inner.lease_id,
            inner.request_id.clone(),
        ));
        if calls.grant_stale > 0 {
            calls.grant_stale -= 1;
            drop(calls);
            return Err(stale_epoch("3"));
        }
        drop(calls);
        Ok(Response::new(lease::LeaseGrantResponse {
            lease_id: 42,
            ttl_seconds: inner.ttl_seconds,
            granted_ts: 7,
        }))
    }

    async fn lease_revoke(
        &self,
        request: Request<lease::LeaseRevokeRequest>,
    ) -> Result<Response<lease::LeaseRevokeResponse>, Status> {
        let epoch = epoch_of(&request);
        let inner = request.into_inner();
        self.calls
            .lock()
            .expect("calls")
            .revoke
            .push((epoch, inner.lease_id, inner.request_id));
        Ok(Response::new(lease::LeaseRevokeResponse { commit_ts: 0 }))
    }

    async fn lease_keep_alive(
        &self,
        request: Request<Streaming<lease::LeaseKeepAliveRequest>>,
    ) -> Result<Response<Self::LeaseKeepAliveStream>, Status> {
        let mut stream = request.into_inner();
        let calls = Arc::clone(&self.calls);
        let (tx, rx) = mpsc::channel(1);
        tokio::spawn(async move {
            if let Ok(Some(msg)) = stream.message().await {
                calls.lock().expect("calls").keep_alive.push(msg.lease_id);
                let _ = tx
                    .send(Ok(lease::LeaseKeepAliveResponse {
                        lease_id: msg.lease_id,
                        ttl_seconds: 15,
                    }))
                    .await;
            }
        });
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn lease_time_to_live(
        &self,
        request: Request<lease::LeaseTimeToLiveRequest>,
    ) -> Result<Response<lease::LeaseTimeToLiveResponse>, Status> {
        let inner = request.into_inner();
        self.calls
            .lock()
            .expect("calls")
            .ttl
            .push((inner.lease_id, inner.keys));
        if inner.lease_id == 99 {
            return Err(Status::not_found("lease not found"));
        }
        Ok(Response::new(lease::LeaseTimeToLiveResponse {
            lease_id: inner.lease_id,
            ttl_seconds: 12,
            granted_ttl_seconds: 30,
            keys: vec![b"k1".to_vec()],
        }))
    }
}

fn object_stat(bucket: &str, object_id: &[u8]) -> storage::ObjectStat {
    storage::ObjectStat {
        bucket: bucket.to_string(),
        object_id: object_id.to_vec(),
        size: 7,
        chunks: 3,
        commit_ts: 9,
        create_ts: 3,
        exists: true,
        committed: true,
    }
}

#[tonic::async_trait]
impl storage::storage_server::Storage for Mock {
    type GetStream = ReceiverStream<Result<storage::GetResponse, Status>>;

    async fn put(
        &self,
        request: Request<Streaming<storage::PutRequest>>,
    ) -> Result<Response<storage::PutResponse>, Status> {
        let epoch = epoch_of(&request);
        {
            let mut calls = self.calls.lock().expect("calls");
            calls.put_epochs.push(epoch);
            if calls.put_stale > 0 {
                calls.put_stale -= 1;
                drop(calls);
                return Err(stale_epoch("5"));
            }
        }
        let mut stream = request.into_inner();
        let mut meta = None;
        let mut chunk_lens = Vec::new();
        while let Some(msg) = stream.message().await? {
            match msg.part {
                Some(put_request::Part::Meta(m)) => meta = Some(m),
                Some(put_request::Part::Chunk(chunk)) => chunk_lens.push(chunk.len()),
                None => {}
            }
        }
        let meta = meta.ok_or_else(|| Status::invalid_argument("missing meta"))?;
        let total: usize = chunk_lens.iter().sum();
        self.calls
            .lock()
            .expect("calls")
            .put
            .push((Some(meta), chunk_lens.clone()));
        Ok(Response::new(storage::PutResponse {
            commit_ts: 9,
            size: total as i64,
            chunks: chunk_lens.len() as i64,
        }))
    }

    async fn get(
        &self,
        request: Request<storage::GetRequest>,
    ) -> Result<Response<Self::GetStream>, Status> {
        let epoch = epoch_of(&request);
        let inner = request.into_inner();
        self.calls
            .lock()
            .expect("calls")
            .get
            .push((epoch, inner.read_ts));
        let stat = object_stat(&inner.bucket, &inner.object_id);
        let (tx, rx) = mpsc::channel(4);
        let _ = tx.try_send(Ok(storage::GetResponse {
            part: Some(get_response::Part::Stat(stat)),
        }));
        let _ = tx.try_send(Ok(storage::GetResponse {
            part: Some(get_response::Part::Chunk(b"abcd".to_vec())),
        }));
        let _ = tx.try_send(Ok(storage::GetResponse {
            part: Some(get_response::Part::Chunk(b"efg".to_vec())),
        }));
        drop(tx);
        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn stat(
        &self,
        request: Request<storage::StatRequest>,
    ) -> Result<Response<storage::StatResponse>, Status> {
        let inner = request.into_inner();
        self.calls.lock().expect("calls").stat.push(inner.read_ts);
        if inner.object_id == b"missing" {
            return Err(Status::not_found("object not found"));
        }
        Ok(Response::new(storage::StatResponse {
            stat: Some(object_stat(&inner.bucket, &inner.object_id)),
        }))
    }

    async fn delete(
        &self,
        request: Request<storage::DeleteRequest>,
    ) -> Result<Response<storage::DeleteResponse>, Status> {
        let epoch = epoch_of(&request);
        let inner = request.into_inner();
        self.calls
            .lock()
            .expect("calls")
            .delete
            .push((epoch, inner.request_id));
        Ok(Response::new(storage::DeleteResponse {
            deleted: true,
            commit_ts: 11,
        }))
    }
}

async fn start_server(calls: Arc<Mutex<Calls>>) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("addr");
    let lease_service = lease::lease_server::LeaseServer::new(Mock {
        calls: Arc::clone(&calls),
    });
    let storage_service = storage::storage_server::StorageServer::new(Mock { calls });
    tokio::spawn(async move {
        Server::builder()
            .add_service(lease_service)
            .add_service(storage_service)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .expect("serve");
    });
    addr.to_string()
}

async fn client_and_calls(calls: Calls) -> (Client, Arc<Mutex<Calls>>) {
    let calls = Arc::new(Mutex::new(calls));
    let addr = start_server(Arc::clone(&calls)).await;
    let client = Client::connect_via_agent(addr).await.expect("client");
    (client, calls)
}

#[tokio::test]
async fn lease_v2_grant_carries_ttl_id_and_request_id() {
    let (client, calls) = client_and_calls(Calls::default()).await;

    let grant = client
        .lease_v2()
        .grant(30, 0, b"req-1")
        .await
        .expect("grant");
    assert_eq!(grant.lease_id, 42);
    assert_eq!(grant.ttl_seconds, 30);
    assert_eq!(grant.granted_ts, 7);
    assert_eq!(
        calls.lock().expect("calls").grant,
        vec![(None, 30, 0, b"req-1".to_vec())],
        "first request carries no epoch and passes the contract fields through"
    );
}

#[tokio::test]
async fn lease_v2_grant_retries_a_stale_epoch() {
    let (client, calls) = client_and_calls(Calls {
        grant_stale: 1,
        ..Default::default()
    })
    .await;

    client
        .lease_v2()
        .grant(30, 0, b"req-1")
        .await
        .expect("grant after stale epoch");
    assert_eq!(
        calls.lock().expect("calls").grant.len(),
        2,
        "the rejected grant is retried once"
    );
    let epochs: Vec<Option<u64>> = calls
        .lock()
        .expect("calls")
        .grant
        .iter()
        .map(|(epoch, ..)| *epoch)
        .collect();
    assert_eq!(
        epochs,
        vec![None, Some(3)],
        "first attempt has no epoch; the retry carries the learned one"
    );

    client
        .lease_v2()
        .grant(30, 0, b"req-2")
        .await
        .expect("second grant");
    let epochs: Vec<Option<u64>> = calls
        .lock()
        .expect("calls")
        .grant
        .iter()
        .map(|(epoch, ..)| *epoch)
        .collect();
    assert_eq!(
        epochs,
        vec![None, Some(3), Some(3)],
        "a learned epoch is attached without waiting for another rejection"
    );
}

#[tokio::test]
async fn lease_v2_revoke_returns_zero_commit_ts_for_replays() {
    let (client, calls) = client_and_calls(Calls::default()).await;

    let commit_ts = client
        .lease_v2()
        .revoke(7, b"revoke-1")
        .await
        .expect("revoke");
    assert_eq!(commit_ts, 0, "0 = no new decision (idempotent replay)");
    assert_eq!(
        calls.lock().expect("calls").revoke,
        vec![(None, 7, b"revoke-1".to_vec())]
    );
}

#[tokio::test]
async fn lease_v2_keep_alive_sends_one_frame() {
    let (client, calls) = client_and_calls(Calls::default()).await;

    let ttl = client.lease_v2().keep_alive(7).await.expect("keep alive");
    assert_eq!(ttl, 15);
    assert_eq!(calls.lock().expect("calls").keep_alive, vec![7]);
}

#[tokio::test]
async fn lease_v2_time_to_live_passes_keys_flag_and_maps_not_found() {
    let (client, calls) = client_and_calls(Calls::default()).await;

    let state = client
        .lease_v2()
        .time_to_live(7, true)
        .await
        .expect("time to live");
    assert_eq!(state.lease_id, 7);
    assert_eq!(state.ttl_seconds, 12);
    assert_eq!(state.granted_ttl_seconds, 30);
    assert_eq!(state.keys, vec![b"k1".to_vec()]);
    assert_eq!(calls.lock().expect("calls").ttl, vec![(7, true)]);

    let err = client
        .lease_v2()
        .time_to_live(99, false)
        .await
        .expect_err("unknown lease");
    assert!(
        matches!(err, Error::NotFound { .. }),
        "NOT_FOUND must surface as a business error, not be retried: {err:?}"
    );
}

#[tokio::test]
async fn storage_v2_put_streams_meta_then_chunks() {
    let (client, calls) = client_and_calls(Calls::default()).await;

    let result = client
        .storage_v2()
        .put_chunked("bucket", b"obj-1", b"abcdefg", 3)
        .await
        .expect("put");
    assert_eq!(result.commit_ts, 9);
    assert_eq!(result.size, 7);
    assert_eq!(result.chunks, 3);

    let calls = calls.lock().expect("calls");
    assert_eq!(calls.put.len(), 1);
    assert_eq!(calls.put_epochs, vec![None]);
    let (meta, chunk_lens) = &calls.put[0];
    let meta = meta.as_ref().expect("meta");
    assert_eq!(meta.bucket, "bucket");
    assert_eq!(meta.object_id, b"obj-1");
    assert_eq!(meta.total_size, 7);
    assert_eq!(chunk_lens, &vec![3, 3, 1]);
}

#[tokio::test]
async fn storage_v2_put_retries_a_stale_epoch() {
    let (client, calls) = client_and_calls(Calls {
        put_stale: 1,
        ..Default::default()
    })
    .await;

    client
        .storage_v2()
        .put_chunked("bucket", b"obj-1", b"abcdefg", 3)
        .await
        .expect("put after stale epoch");
    assert_eq!(
        calls.lock().expect("calls").put_epochs,
        vec![None, Some(5)],
        "the whole upload is replayed once with the learned epoch"
    );

    client
        .storage_v2()
        .put_chunked("bucket", b"obj-2", b"abcdefg", 3)
        .await
        .expect("second put");
    assert_eq!(
        calls.lock().expect("calls").put_epochs,
        vec![None, Some(5), Some(5)]
    );
}

#[tokio::test]
async fn storage_v2_get_passes_read_ts_and_assembles_chunks() {
    let (client, calls) = client_and_calls(Calls::default()).await;

    let object = client
        .storage_v2()
        .get("bucket", b"obj-1", 5)
        .await
        .expect("get");
    assert_eq!(object.stat.commit_ts, 9);
    assert_eq!(object.data, b"abcdefg");
    assert_eq!(calls.lock().expect("calls").get, vec![(None, 5)]);

    let err = client
        .storage_v2()
        .get("bucket", b"obj-1", u64::MAX)
        .await
        .expect_err("read_ts beyond i64 must be rejected locally");
    assert!(matches!(err, Error::InvalidArgument(_)), "{err:?}");
    assert_eq!(
        calls.lock().expect("calls").get.len(),
        1,
        "an out-of-range read_ts must not reach the server"
    );
}

#[tokio::test]
async fn storage_v2_stat_maps_not_found_to_none() {
    let (client, calls) = client_and_calls(Calls::default()).await;

    let stat = client
        .storage_v2()
        .stat("bucket", b"obj-1", 0)
        .await
        .expect("stat");
    assert_eq!(stat.expect("existing object").commit_ts, 9);

    let missing = client
        .storage_v2()
        .stat("bucket", b"missing", 0)
        .await
        .expect("stat missing");
    assert!(missing.is_none(), "NOT_FOUND is an absence, not an error");
    assert_eq!(calls.lock().expect("calls").stat, vec![0, 0]);
}

#[tokio::test]
async fn storage_v2_delete_carries_request_id_and_commit_ts() {
    let (client, calls) = client_and_calls(Calls::default()).await;

    let (deleted, commit_ts) = client
        .storage_v2()
        .delete_full("bucket", b"obj-1", b"delete-1")
        .await
        .expect("delete");
    assert!(deleted);
    assert_eq!(commit_ts, 11);
    assert_eq!(
        calls.lock().expect("calls").delete,
        vec![(None, b"delete-1".to_vec())]
    );
}
