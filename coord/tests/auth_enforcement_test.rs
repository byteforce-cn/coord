// 验收套件：鉴权强制执行（auth_enforcement_test）
//
// §四 验收标准：
// - 无 token 调 KV/Txn/Lease/Watch/Maintenance 返回 UNAUTHENTICATED；
// - 低权限 token 越权返回 PERMISSION_DENIED；
// - Authenticate 与健康检查匿名可访问；
// - 默认配置启动即鉴权开启（不传 --auth-enabled，验证 auth_enabled 默认 true）。
//
// 实现方式：spawn 真实 `coord` 二进制（CARGO_BIN_EXE_coord）以 server 模式启动，
// 通过 COORD_ROOT_PASSWORD 提供 root 密码；就绪探测走 HTTP /healthz（BFF 匿名）。

use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use tonic::metadata::MetadataValue;
use tonic::transport::Channel;
use tonic::{Request, Status};

use coord_proto::auth::auth_client::AuthClient;
use coord_proto::auth::{AuthenticateRequest, RoleAddRequest, RoleGrantPermissionRequest};
use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::{PutRequest, RangeRequest};
use coord_proto::lease::lease_client::LeaseClient;
use coord_proto::lease::LeaseGrantRequest;
use coord_proto::maintenance::maintenance_client::MaintenanceClient;
use coord_proto::maintenance::StatusRequest;
use coord_proto::txn::txn_client::TxnClient;
use coord_proto::txn::TxnRequest;
use coord_proto::watch::watch_client::WatchClient;
use coord_proto::watch::WatchRequest;

/// 找一个空闲端口（bind:0 后释放）
fn find_free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap().port()
}

/// spawn server 模式二进制（默认 auth_enabled=true，不传 --auth-enabled）
fn spawn_server(data_dir: &std::path::Path, grpc_port: u16, raft_port: u16) -> std::process::Child {
    let bin = env!("CARGO_BIN_EXE_coord");
    let log_file = std::fs::File::create(data_dir.join("server.log")).unwrap();
    Command::new(bin)
        .arg("server")
        .arg("--id")
        .arg("1")
        .arg("--bootstrap")
        .arg("--addr")
        .arg(format!("127.0.0.1:{grpc_port}"))
        .arg("--raft-addr")
        .arg(format!("127.0.0.1:{raft_port}"))
        .arg("--data-dir")
        .arg(data_dir)
        .env("COORD_ROOT_PASSWORD", "test-root-password-123")
        .env("RUST_LOG", "coord=debug")
        .stdout(Stdio::from(log_file.try_clone().unwrap()))
        .stderr(Stdio::from(log_file))
        .spawn()
        .expect("spawn coord server")
}

/// 就绪探测：HTTP /healthz（BFF 匿名端点，端口 = grpc + 10）
async fn wait_ready(grpc_port: u16, timeout: Duration) {
    let deadline = Instant::now() + timeout;
    loop {
        if let Ok(stream) = tokio::net::TcpStream::connect(("127.0.0.1", grpc_port + 10)).await {
            use tokio::io::AsyncReadExt;
            stream.writable().await.ok();
            let req = b"GET /healthz HTTP/1.0\r\nHost: localhost\r\n\r\n";
            if stream.try_write(req).is_ok() {
                let mut buf = [0u8; 256];
                if let Ok(stream) = tokio::time::timeout(Duration::from_millis(500), async {
                    let mut stream = stream;
                    let n = stream.read(&mut buf).await?;
                    Ok::<usize, std::io::Error>(n)
                })
                .await
                {
                    if stream.is_ok() {
                        return;
                    }
                }
            }
        }
        assert!(
            Instant::now() < deadline,
            "server did not become ready within {timeout:?}"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
}

/// 连接指定 gRPC 地址
async fn channel(addr: &str) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_timeout(Duration::from_secs(3))
        .connect()
        .await
        .unwrap()
}

fn with_token<T>(req: T, token: &str) -> Request<T> {
    let mut req = Request::new(req);
    req.metadata_mut().insert(
        "authorization",
        MetadataValue::try_from(format!("Bearer {token}")).unwrap(),
    );
    req
}

/// 服务端日志尾部（失败诊断用；与本仓其它进程级套件的做法一致）。
fn server_log_tail(data_dir: &std::path::Path, lines: usize) -> String {
    let log = std::fs::read_to_string(data_dir.join("server.log")).unwrap_or_default();
    let all: Vec<&str> = log.lines().collect();
    let start = all.len().saturating_sub(lines);
    all[start..].join("\n")
}

/// **就绪敏感**的首个 `Authenticate`：在窗口内重试瞬时传输错误，失败时附带服务端日志。
///
/// 为什么需要：`wait_ready` 探测的是 BFF 的 `/healthz`（端口 = `grpc + 10`），它与
/// gRPC 监听器是**两个不同的监听器** —— "HTTP 就绪"并不等于"gRPC 可服务"。CI 上
/// 观测到紧随 `/healthz` 成功之后的首个 `Authenticate` 返回
/// `transport error ... Kind(ConnectionReset)`（成功探测后 <1s 内即失败）。
///
/// 断言没有被放宽：只有**瞬时**错误（传输层 / UNAVAILABLE / DEADLINE_EXCEEDED）
/// 允许在窗口内重试，超时仍失败，并且失败信息里带服务端日志尾部 —— 这样下一次
/// 出现时能直接区分"gRPC 未就绪"与"服务端启动后退出"。
///
/// `Authenticate` 是**匿名**端点，所以能在没有任何凭据的情况下用作就绪探针
/// （这正是本套件第 2 条验收标准）。
async fn authenticate_ready(
    auth: &mut AuthClient<Channel>,
    data_dir: &std::path::Path,
    name: &str,
    password: &str,
) -> coord_proto::auth::AuthenticateResponse {
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        match auth
            .authenticate(AuthenticateRequest {
                name: name.to_string(),
                password: password.to_string(),
            })
            .await
        {
            Ok(resp) => return resp.into_inner(),
            Err(s) => {
                let transient = matches!(
                    s.code(),
                    tonic::Code::Unavailable | tonic::Code::DeadlineExceeded
                ) || s.message().contains("transport error");
                assert!(
                    transient && Instant::now() < deadline,
                    "authenticate({name}) must succeed anonymously: {s:?}\n\
                     --- server log tail ---\n{}",
                    server_log_tail(data_dir, 40)
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }
    }
}

/// 期待 UNAUTHENTICATED
fn expect_unauthenticated(status: Status, rpc: &str) {
    assert_eq!(
        status.code(),
        tonic::Code::Unauthenticated,
        "{rpc} without token should be UNAUTHENTICATED, got: {status}"
    );
}

/// 主场景：匿名拒绝 + 匿名白名单 + root 全权限 + 低权限拒绝
#[tokio::test]
async fn test_auth_enforcement_full_matrix() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let grpc_port = find_free_port();
    let raft_port = find_free_port();
    let addr = format!("127.0.0.1:{grpc_port}");

    let mut child = spawn_server(&data_dir, grpc_port, raft_port);
    wait_ready(grpc_port, Duration::from_secs(60)).await;

    // ─── 1. 无 token：KV/Txn/Lease/Maintenance/Watch 一律 UNAUTHENTICATED ───
    let mut kv = KvClient::new(channel(&addr).await);
    let status = kv
        .range(RangeRequest {
            key: b"/k".to_vec(),
            range_end: vec![],
            limit: 1,
            revision: 0,
            keys_only: false,
            count_only: false,
        })
        .await
        .unwrap_err();
    expect_unauthenticated(status, "KV/Range");

    let mut txn = TxnClient::new(channel(&addr).await);
    expect_unauthenticated(txn.txn(TxnRequest::default()).await.unwrap_err(), "Txn/Txn");

    let mut lease = LeaseClient::new(channel(&addr).await);
    expect_unauthenticated(
        lease
            .lease_grant(LeaseGrantRequest { ttl: 60, id: 0 })
            .await
            .unwrap_err(),
        "Lease/LeaseGrant",
    );

    let mut maint = MaintenanceClient::new(channel(&addr).await);
    expect_unauthenticated(
        maint.status(StatusRequest {}).await.unwrap_err(),
        "Maintenance/Status",
    );

    let mut watch = WatchClient::new(channel(&addr).await);
    let watch_stream_in = tokio_stream::once(WatchRequest {
        request: Some(coord_proto::watch::watch_request::Request::Create(
            coord_proto::watch::WatchCreateRequest {
                key: b"/k".to_vec(),
                range_end: vec![],
                start_revision: 1,
                prev_kv: false,
            },
        )),
    });
    // Watch 流式 RPC：无 token 时可能在调用点即失败（UNAUTHENTICATED）
    match watch.watch(tonic::Request::new(watch_stream_in)).await {
        Err(status) => {
            expect_unauthenticated(status, "Watch");
        }
        Ok(resp) => {
            let mut watch_stream = resp.into_inner();
            let first = tokio::time::timeout(Duration::from_secs(5), watch_stream.message())
                .await
                .unwrap()
                .unwrap_err();
            assert_eq!(
                first.code(),
                tonic::Code::Unauthenticated,
                "Watch without token should be UNAUTHENTICATED, got: {first}"
            );
        }
    }

    // ─── 2. 匿名白名单：健康检查 + Authenticate 匿名可访问───
    let mut health = tonic_health::pb::health_client::HealthClient::new(channel(&addr).await);
    health
        .check(tonic_health::pb::HealthCheckRequest {
            service: String::new(),
        })
        .await
        .expect("health check must be anonymous");

    let mut auth = AuthClient::new(channel(&addr).await);
    let login = authenticate_ready(&mut auth, &data_dir, "root", "test-root-password-123").await;
    let root_cct = login.cct.clone();
    assert!(!root_cct.is_empty(), "root login must issue a CCT");
    assert_eq!(login.roles, vec!["root"]);

    // ─── 3. root token 全权限（引导管理员）───
    let put = kv
        .put(with_token(
            PutRequest {
                key: b"/app/x".to_vec(),
                value: b"v1".to_vec(),
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            },
            &root_cct,
        ))
        .await
        .expect("root token should allow Put");
    assert!(put.into_inner().revision > 0, "Put must consume a revision");

    // ─── 4. 低权限用户：读放行、写拒绝、管理接口拒绝 ───
    auth.user_add(with_token(
        coord_proto::auth::UserAddRequest {
            name: "reader".to_string(),
            password: "reader-pw".to_string(),
        },
        &root_cct,
    ))
    .await
    .expect("root may add user");
    auth.role_add(with_token(
        RoleAddRequest {
            name: "reader-role".to_string(),
        },
        &root_cct,
    ))
    .await
    .expect("root may add role");
    auth.role_grant_permission(with_token(
        RoleGrantPermissionRequest {
            name: "reader-role".to_string(),
            permission: Some(coord_proto::auth::Permission {
                r#type: coord_proto::auth::PermissionType::Read as i32,
                key: vec![], // 空前缀 = 全键读（server 直连场景的能力级授权）
                range_end: vec![],
            }),
        },
        &root_cct,
    ))
    .await
    .expect("root may grant permission");
    auth.user_grant_role(with_token(
        coord_proto::auth::UserGrantRoleRequest {
            user: "reader".to_string(),
            role: "reader-role".to_string(),
        },
        &root_cct,
    ))
    .await
    .expect("root may grant role");

    let reader_login = auth
        .authenticate(AuthenticateRequest {
            name: "reader".to_string(),
            password: "reader-pw".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    let reader_cct = reader_login.cct;

    // 读放行（遗留权限映射 data:kv:read，空前缀）
    let range = kv
        .range(with_token(
            RangeRequest {
                key: b"/app/x".to_vec(),
                range_end: vec![],
                limit: 1,
                revision: 0,
                keys_only: false,
                count_only: false,
            },
            &reader_cct,
        ))
        .await
        .expect("reader token should allow Range");
    assert_eq!(range.into_inner().kvs.len(), 1);

    // 写拒绝（PERMISSION_DENIED，非 UNAUTHENTICATED）
    let denied = kv
        .put(with_token(
            PutRequest {
                key: b"/app/y".to_vec(),
                value: b"v".to_vec(),
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            },
            &reader_cct,
        ))
        .await
        .unwrap_err();
    assert_eq!(
        denied.code(),
        tonic::Code::PermissionDenied,
        "reader Put should be PERMISSION_DENIED, got: {denied}"
    );

    // 管理接口拒绝
    let denied = auth
        .auth_status(with_token(
            coord_proto::auth::AuthStatusRequest {},
            &reader_cct,
        ))
        .await
        .unwrap_err();
    assert_eq!(
        denied.code(),
        tonic::Code::PermissionDenied,
        "reader AuthStatus should be PERMISSION_DENIED, got: {denied}"
    );

    // ─── 5. 未知 RPC / 伪造 token → 拒绝（fail-closed）───
    let denied = kv
        .range(with_token(
            RangeRequest {
                key: b"/k".to_vec(),
                range_end: vec![],
                limit: 1,
                revision: 0,
                keys_only: false,
                count_only: false,
            },
            "not-a-real-token",
        ))
        .await
        .unwrap_err();
    assert_eq!(
        denied.code(),
        tonic::Code::Unauthenticated,
        "forged token should be UNAUTHENTICATED, got: {denied}"
    );

    child.kill().unwrap();
    let _ = child.wait();
}

/// 发送 HTTP 请求（BFF 端口 = grpc + 10），返回原始响应文本。
async fn http_request(port: u16, req: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut stream = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .unwrap();
    stream.write_all(req.as_bytes()).await.unwrap();
    let mut buf = vec![0u8; 8192];
    let mut out = String::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    loop {
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            break;
        }
        match tokio::time::timeout(remaining, stream.read(&mut buf)).await {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => {
                out.push_str(&String::from_utf8_lossy(&buf[..n]));
            }
            _ => break,
        }
    }
    out
}

/// C.6 验收：user_add 经 raft 持久化，kill -9 重启后用户仍在（root 密码不变）。
#[tokio::test]
async fn test_auth_users_survive_restart() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let grpc_port = find_free_port();
    let raft_port = find_free_port();
    let addr = format!("127.0.0.1:{grpc_port}");

    // 第一次启动：创建用户 alice
    let mut child = spawn_server(&data_dir, grpc_port, raft_port);
    wait_ready(grpc_port, Duration::from_secs(60)).await;

    let mut auth = AuthClient::new(channel(&addr).await);
    let root_cct = authenticate_ready(&mut auth, &data_dir, "root", "test-root-password-123")
        .await
        .cct;
    auth.user_add(with_token(
        coord_proto::auth::UserAddRequest {
            name: "alice".to_string(),
            password: "alice-pw".to_string(),
        },
        &root_cct,
    ))
    .await
    .expect("root may add user");

    // kill -9（不经优雅停机）
    child.kill().unwrap();
    let _ = child.wait();

    // 重启：root 与 alice 都应仍在
    let mut child = spawn_server(&data_dir, grpc_port, raft_port);
    wait_ready(grpc_port, Duration::from_secs(60)).await;

    let mut auth2 = AuthClient::new(channel(&addr).await);
    let alice_login = authenticate_ready(&mut auth2, &data_dir, "alice", "alice-pw").await;
    assert!(!alice_login.cct.is_empty(), "alice must survive restart");
    // root 也仍在（经 raft 持久化）
    let root_login =
        authenticate_ready(&mut auth2, &data_dir, "root", "test-root-password-123").await;
    assert!(!root_login.cct.is_empty(), "root must survive restart");

    child.kill().unwrap();
    let _ = child.wait();
}

/// C.5 验收：BFF revoke → raft RevokeJti → 该 CCT 立即被拦截器拒绝。
#[tokio::test]
async fn test_cct_revocation_via_bff() {
    let tmp = tempfile::tempdir().unwrap();
    let data_dir = tmp.path().to_path_buf();
    let grpc_port = find_free_port();
    let raft_port = find_free_port();
    let addr = format!("127.0.0.1:{grpc_port}");

    let mut child = spawn_server(&data_dir, grpc_port, raft_port);
    wait_ready(grpc_port, Duration::from_secs(60)).await;

    // 创建用户 bob + 登录获取 CCT
    let mut auth = AuthClient::new(channel(&addr).await);
    let root_cct = authenticate_ready(&mut auth, &data_dir, "root", "test-root-password-123")
        .await
        .cct;
    auth.user_add(with_token(
        coord_proto::auth::UserAddRequest {
            name: "bob".to_string(),
            password: "bob-pw".to_string(),
        },
        &root_cct,
    ))
    .await
    .unwrap();
    // 授予全键读（空前缀）能力
    auth.role_add(with_token(
        RoleAddRequest {
            name: "reader-role".to_string(),
        },
        &root_cct,
    ))
    .await
    .unwrap();
    auth.role_grant_permission(with_token(
        RoleGrantPermissionRequest {
            name: "reader-role".to_string(),
            permission: Some(coord_proto::auth::Permission {
                r#type: coord_proto::auth::PermissionType::Read as i32,
                key: vec![],
                range_end: vec![],
            }),
        },
        &root_cct,
    ))
    .await
    .unwrap();
    auth.user_grant_role(with_token(
        coord_proto::auth::UserGrantRoleRequest {
            user: "bob".to_string(),
            role: "reader-role".to_string(),
        },
        &root_cct,
    ))
    .await
    .unwrap();
    let bob_cct = auth
        .authenticate(AuthenticateRequest {
            name: "bob".to_string(),
            password: "bob-pw".to_string(),
        })
        .await
        .unwrap()
        .into_inner()
        .cct;

    // 吊销前：bob CCT 可用
    let mut kv = KvClient::new(channel(&addr).await);
    let ok = kv
        .range(with_token(
            RangeRequest {
                key: b"/k".to_vec(),
                range_end: vec![],
                limit: 1,
                revision: 0,
                keys_only: false,
                count_only: false,
            },
            &bob_cct,
        ))
        .await;
    assert!(ok.is_ok(), "bob CCT should work before revocation: {ok:?}");

    // 经 BFF revoke 端点吊销（raft RevokeJti）
    // 诊断：先验证 HTTP helper 与 BFF 端口可用
    let probe = http_request(
        grpc_port + 10,
        "GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n",
    )
    .await;
    assert!(
        probe.starts_with("HTTP/1.1 200") || probe.starts_with("HTTP/1.0 200"),
        "healthz probe failed: {probe}"
    );

    let req = format!(
        "POST /api/v1/auth/revoke HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\nConnection: close\r\nAuthorization: Bearer {bob_cct}\r\n\r\n"
    );
    let resp = http_request(grpc_port + 10, &req).await;
    if !(resp.starts_with("HTTP/1.0 200") || resp.starts_with("HTTP/1.1 200")) {
        // 诊断：dump 服务端日志
        let log = std::fs::read_to_string(data_dir.join("server.log")).unwrap_or_default();
        let tail: Vec<&str> = log.lines().rev().take(20).collect();
        panic!(
            "revoke endpoint should return 200, got: {resp}\nserver log tail:\n{}\n",
            tail.join("\n")
        );
    }

    // 吊销后：bob CCT 被拒绝（UNAUTHENTICATED）
    let denied = kv
        .range(with_token(
            RangeRequest {
                key: b"/k".to_vec(),
                range_end: vec![],
                limit: 1,
                revision: 0,
                keys_only: false,
                count_only: false,
            },
            &bob_cct,
        ))
        .await
        .unwrap_err();
    assert_eq!(
        denied.code(),
        tonic::Code::Unauthenticated,
        "revoked CCT should be UNAUTHENTICATED, got: {denied}"
    );

    child.kill().unwrap();
    let _ = child.wait();
}
