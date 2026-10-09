// G-POL-1: OPA bundle 分发端到端测试（ADR-0010）
//
// 验证契约承诺的「bundle 分发 / 启动加载 / 收敛」语义（进度口径见 policy.proto 头注）：
// 1. A 上 PutBundle → A 本地立即生效；
// 2. B（未处理写入）在收敛时限（≤10s 量级）内同判（Watch 传播）；
// 3. 重启 A（本地内存清空、KV 不变）→ 启动加载恢复判定；
// 4. SetEnabled(false) / Rollback 全 agent 收敛一致；
// 5. 并发 Put 版本单调（连续递增、互异、无丢更新）。

mod common;

#[cfg(test)]
mod tests {
    use std::time::{Duration, Instant};

    use coord_agent::{AgentConfig, AgentServer};
    use coord_proto::policy::v1::policy_client::PolicyClient;
    use coord_proto::policy::v1::{
        PolicyEvaluateRequest, PolicyPutBundleRequest, PolicyRollbackBundleRequest,
        PolicySetBundleEnabledRequest,
    };

    // 共享夹具（单一事实来源）：find_port()/start_test_server() 收敛在
    // tests/common/mod.rs，不得在本文件里再复制一份。
    use crate::common::{find_port, start_test_server, wait_tcp_ready};

    type PolicyGrpc = PolicyClient<tonic::transport::Channel>;

    /// 轮询等待异步条件成立；超时即断言失败（收敛时限的机器判据）。
    ///
    /// 用宏而非 `FnMut() -> Future` 闭包：条件常需同时可变借用两个 gRPC 客户端，
    /// 闭包体返回的 future 会逃逸借用（captured variable cannot escape FnMut）。
    macro_rules! wait_for {
        ($timeout:expr, $what:expr, $cond:expr) => {{
            let deadline = Instant::now() + $timeout;
            loop {
                if $cond.await {
                    break;
                }
                assert!(
                    Instant::now() < deadline,
                    "convergence timeout after {:?}: {}",
                    $timeout,
                    $what
                );
                tokio::time::sleep(Duration::from_millis(200)).await;
            }
        }};
    }

    /// 启动一个连接测试 server 的 agent（policy 服务显式启用）
    async fn start_agent(
        server_addr: &str,
        data_dir: &std::path::Path,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let agent_port = find_port();
        let agent_addr = format!("127.0.0.1:{agent_port}");
        let mut config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", find_port()),
            data_dir: data_dir.to_string_lossy().to_string(),
            static_peers: vec![server_addr.to_string()],
            ..Default::default()
        };
        config.services.policy = true;

        let server = AgentServer::new(config);
        let handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });

        wait_tcp_ready(&agent_addr, Duration::from_secs(30)).await;
        (agent_addr, handle)
    }

    async fn connect(addr: &str) -> PolicyGrpc {
        let channel = tonic::transport::Endpoint::from_shared(format!("http://{addr}"))
            .unwrap()
            .connect()
            .await
            .expect("connect to agent");
        PolicyClient::new(channel)
    }

    /// 求值 `data.<pkg>.allow`（input.subject 由调用方给定）
    async fn eval_allow(client: &mut PolicyGrpc, pkg: &str, subject: &str) -> bool {
        let resp = client
            .evaluate(PolicyEvaluateRequest {
                query: format!("data.{pkg}.allow"),
                input: format!("{{\"subject\":\"{subject}\"}}").into_bytes(),
            })
            .await
            .expect("evaluate");
        let v: serde_json::Value =
            serde_json::from_slice(&resp.get_ref().result).expect("result JSON");
        v.as_bool().unwrap_or(false)
    }

    const REGO_V1: &str = r#"
package test.pol

default allow := false

allow if {
    input.subject == "alice"
}
"#;

    const REGO_V2: &str = r#"
package test.pol

default allow := false

allow if {
    input.subject == "bob"
}
"#;

    /// G-POL-1 验收锚点：分发 / 启动加载 / 收敛 / 版本一致（单测试串行推进，
    /// 避免每步重复付 2 个 agent 的启动成本）
    #[tokio::test(flavor = "multi_thread")]
    async fn test_bundle_distribution_load_convergence_and_versioning() {
        let _ = tracing_subscriber::fmt()
            .with_env_filter("coord=info")
            .try_init();

        let (server_addr, _shutdown_tx, _grpc, _raft, _tmpdir) = start_test_server().await;

        let dir_a = tempfile::tempdir().unwrap();
        let dir_b = tempfile::tempdir().unwrap();
        let (addr_a, handle_a) = start_agent(&server_addr, dir_a.path()).await;
        let (addr_b, handle_b) = start_agent(&server_addr, dir_b.path()).await;

        let mut client_a = connect(&addr_a).await;
        let mut client_b = connect(&addr_b).await;

        // 0. 无 bundle：双方默认拒绝
        assert!(
            !eval_allow(&mut client_a, "test.pol", "alice").await,
            "无 bundle 时不得放行"
        );
        assert!(!eval_allow(&mut client_b, "test.pol", "alice").await);

        // 1. A 上 PutBundle → A 本地立即生效（put 返回前已同步本地引擎）
        let put = client_a
            .put_bundle(PolicyPutBundleRequest {
                tenant_id: "t1".into(),
                namespace: "default".into(),
                name: "pol".into(),
                rego_content: REGO_V1.into(),
            })
            .await
            .expect("put bundle through agent A")
            .into_inner();
        assert_eq!(put.version, 1, "首次上传 = v1");
        assert!(
            eval_allow(&mut client_a, "test.pol", "alice").await,
            "A 上 Put 必须本地立即生效"
        );

        // 2. B（未处理写入）在收敛时限内同判
        wait_for!(
            Duration::from_secs(10),
            "agent B must converge to alice=allow within ~10s",
            eval_allow(&mut client_b, "test.pol", "alice")
        );
        assert!(
            !eval_allow(&mut client_b, "test.pol", "bob").await,
            "规则不是常量：bob 仍应拒绝"
        );

        // 3. 重启 A（本地内存清空、KV 不变）→ 启动加载恢复判定
        handle_a.abort();
        let (addr_a2, handle_a2) = start_agent(&server_addr, dir_a.path()).await;
        let mut client_a2 = connect(&addr_a2).await;
        assert!(
            eval_allow(&mut client_a2, "test.pol", "alice").await,
            "重启后必须从 KV 启动加载 bundle（判定不变）"
        );

        // 4a. SetEnabled(false) → 全 agent 收敛为拒绝
        client_a2
            .set_bundle_enabled(PolicySetBundleEnabledRequest {
                bundle_id: "t1/default/pol".into(),
                enabled: false,
            })
            .await
            .expect("set enabled false");
        wait_for!(
            Duration::from_secs(10),
            "both agents must converge to disabled",
            async {
                !eval_allow(&mut client_a2, "test.pol", "alice").await
                    && !eval_allow(&mut client_b, "test.pol", "alice").await
            }
        );

        // 4b. 重新启用 + v2（allow bob）→ B 收敛；Rollback 到 v1 → B 再收敛回 alice
        client_a2
            .set_bundle_enabled(PolicySetBundleEnabledRequest {
                bundle_id: "t1/default/pol".into(),
                enabled: true,
            })
            .await
            .expect("set enabled true");
        let put_v2 = client_a2
            .put_bundle(PolicyPutBundleRequest {
                tenant_id: "t1".into(),
                namespace: "default".into(),
                name: "pol".into(),
                rego_content: REGO_V2.into(),
            })
            .await
            .expect("put v2")
            .into_inner();
        assert_eq!(put_v2.version, 2, "第二次上传版本必须 +1");
        wait_for!(
            Duration::from_secs(10),
            "B must converge to v2 (bob) semantics",
            async {
                eval_allow(&mut client_b, "test.pol", "bob").await
                    && !eval_allow(&mut client_b, "test.pol", "alice").await
            }
        );

        let rollback = client_a2
            .rollback_bundle(PolicyRollbackBundleRequest {
                bundle_id: "t1/default/pol".into(),
                version: 1,
            })
            .await
            .expect("rollback to v1")
            .into_inner();
        assert_eq!(rollback.version, 3, "回滚 = 新版本（v3）");
        wait_for!(
            Duration::from_secs(10),
            "B must converge to rolled-back v1 semantics",
            async {
                eval_allow(&mut client_b, "test.pol", "alice").await
                    && !eval_allow(&mut client_b, "test.pol", "bob").await
            }
        );

        // 5. 并发 Put（fresh bundle）→ 版本互异且连续递增（CAS 无丢更新）
        let n: i64 = 5;
        let mut tasks = Vec::new();
        for i in 0..n {
            let addr = addr_a2.clone();
            tasks.push(tokio::spawn(async move {
                let mut c = connect(&addr).await;
                c.put_bundle(PolicyPutBundleRequest {
                    tenant_id: "t1".into(),
                    namespace: "default".into(),
                    name: "mono".into(),
                    rego_content: format!(
                        "package test.mono\n\ndefault allow := false\n\nallow if {{\n    input.n == {i}\n}}\n"
                    ),
                })
                .await
                .expect("concurrent put")
                .into_inner()
                .version
            }));
        }
        let mut versions = Vec::new();
        for t in tasks {
            versions.push(t.await.expect("join"));
        }
        versions.sort_unstable();
        assert_eq!(
            versions,
            (1..=n).collect::<Vec<_>>(),
            "并发 Put 版本必须互异且连续（单调、无丢更新）"
        );

        handle_b.abort();
        handle_a2.abort();
    }
}
