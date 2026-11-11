//! `[kv_t].enabled` 三态默认（ADR-0017）：多 Region + PD 集群不写该键也启动 v2。
//!
//! 判据：生成的配置里 `[kv_t]` 段没有 `enabled`；v2 TSO/直写可用；v1 KV 按 v2
//! 冻结策略拒绝（证明装配走的是 v2，不是「缺键就退回 v1」）；`coord_kv_t_enabled`
//! 与装配同口径。

#[path = "common/mod.rs"]
mod common;

use std::time::{Duration, Instant};

use common::v2_cluster::{
    allocate_ts, direct_put, metric_value, runtime, V2Cluster, V2ClusterOptions,
};
use coord_proto::kv::kv_client::KvClient;
use coord_proto::kv::PutRequest;
use tonic::Code;

#[test]
fn test_multi_region_cluster_without_kv_t_key_serves_v2() {
    let cluster = V2Cluster::start_with(V2ClusterOptions {
        explicit_kv_t_enabled: false,
        ..Default::default()
    });
    let cfg = cluster.nodes[0].config_toml(false);
    let kv_t_section = &cfg[cfg.find("[kv_t]").expect("[kv_t] section")..];
    assert!(
        !kv_t_section.contains("enabled"),
        "缺省语义必须靠三态自动（ADR-0017），配置里不得写 enabled：{kv_t_section}"
    );

    let addrs = cluster.addrs();
    // /metrics 的监听就绪晚于 leader 收敛：轮询到超时，不把「还没起来」当「没启用」。
    let deadline = Instant::now() + Duration::from_secs(30);
    loop {
        if metric_value(cluster.nodes[0].http_port, "coord_kv_t_enabled") == Some(1) {
            break;
        }
        assert!(
            Instant::now() < deadline,
            "coord_kv_t_enabled 未在 30s 内变为 1（自动启用未生效）"
        );
        std::thread::sleep(Duration::from_millis(100));
    }

    runtime().block_on(async {
        assert!(allocate_ts(&addrs).await > 0, "自动启用后 v2 TSO 必须可用");
        assert!(
            direct_put(&addrs, b"/auto-on/k", b"v").await > 0,
            "自动启用后 v2 直写必须可用"
        );

        // v1 数据面在 v2 集群上按策略冻结：这是「真的走了 v2」的行为判据。
        let mut kv = KvClient::connect(format!("http://{}", addrs[0]))
            .await
            .expect("connect v1 kv");
        let err = kv
            .put(PutRequest {
                key: b"/auto-on/v1".to_vec(),
                value: b"x".to_vec(),
                ..Default::default()
            })
            .await
            .expect_err("v1 Put must be frozen on an auto-enabled v2 cluster");
        assert_eq!(err.code(), Code::FailedPrecondition);
        assert!(
            err.message()
                .contains("v1 data plane is frozen on a v2 cluster"),
            "{}",
            err.message()
        );
    });
}
