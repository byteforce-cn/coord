// Config 快照 + 位点续订（resync）端到端测试（#46/#47）
//
// 判据（issue #46 DoD「端到端判据」）：
//   1. `List` 返回快照位点 `revision`（>0；来自 Server KV 全局 revision）；
//   2. 快照之后被**删除**的 key，能从位点事件
//      （`Watch(start_revision = 快照 revision)`）可靠收敛——删除事件以
//      `new_value` **缺省**（presence 语义）下发，且位点晚于快照位点；
//   3. 实时路径（`start_revision = 0`）照常下发值事件（`new_value` 在场）。
//
// 环境：真实 in-process server + 进程内 agent（config_center 显式启用）；
// 删除经 Agent KV 代理直删存储行（模拟带外 / 其他工具的写入面，
// 与消费方真实场景一致——Config 面本身没有 Delete RPC）。

mod common;

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use coord_agent::service::ServiceConfig;
    use coord_agent::{AgentConfig, AgentServer};
    use coord_proto::config::v1::config_client::ConfigClient;
    use coord_proto::config::v1::{
        ConfigGetRequest, ConfigListRequest, ConfigPutRequest, ConfigWatchRequest,
    };
    use coord_proto::kv::kv_client::KvClient;
    use coord_proto::kv::DeleteRequest;

    use crate::common::{find_port, start_test_server};

    const PREFIX: &str = "resync-e2e.";
    const KEY1: &str = "resync-e2e.k1";
    const KEY2: &str = "resync-e2e.k2";

    async fn connect(addr: &str) -> tonic::transport::Channel {
        let endpoint =
            tonic::transport::Endpoint::from_shared(format!("http://{addr}")).expect("endpoint");
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            match endpoint.clone().connect().await {
                Ok(channel) => return channel,
                Err(e) => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "agent {addr} never became ready: {e}"
                    );
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
        }
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_config_snapshot_then_deleted_key_converges_from_revision() {
        let (server_addr, _shutdown_tx, _grpc, _raft, _tmpdir) = start_test_server().await;

        let agent_port = find_port();
        let agent_addr = format!("127.0.0.1:{agent_port}");
        let data_dir = std::env::temp_dir().join(format!(
            "coord-config-resync-{}-{}",
            std::process::id(),
            agent_port
        ));
        let agent_config = AgentConfig {
            agent_addr: agent_addr.clone(),
            http_addr: format!("127.0.0.1:{}", find_port()),
            data_dir: data_dir.to_string_lossy().into_owned(),
            static_peers: vec![server_addr.clone()],
            services: ServiceConfig {
                config_center: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let server = AgentServer::new(agent_config);
        let agent_handle = tokio::spawn(async move {
            server.serve().await.unwrap();
        });

        let channel = connect(&agent_addr).await;
        let mut config = ConfigClient::new(channel.clone());

        // 1) 写入 k1 → 等待快照收敛（entries 含 k1 且 revision > 0）
        config
            .put(ConfigPutRequest {
                key: KEY1.into(),
                value: "v1".into(),
            })
            .await
            .expect("Config.Put 必须成功");

        let w;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let resp = config
                .list(ConfigListRequest {
                    prefix: PREFIX.into(),
                })
                .await
                .expect("Config.List 必须成功")
                .into_inner();
            if resp.entries.contains_key(KEY1) && resp.revision > 0 {
                w = resp.revision;
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "等待快照收敛（entries+revision>0）超时"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // 2) 快照之后：直删存储行（带外删除；Config 面没有 Delete RPC）
        let mut kv = KvClient::new(channel.clone());
        let deleted = kv
            .delete(DeleteRequest {
                key: format!("/_config/{KEY1}").into_bytes(),
                range_end: vec![],
                prev_kv: false,
                request_id: vec![],
            })
            .await
            .expect("直连 KV 删除必须成功")
            .into_inner();
        assert!(deleted.deleted >= 1, "删除必须命中存储行");

        // 3) 从快照位点起订（Watch(start_revision = W)）：必须收到 k1 的删除事件
        //    （new_value 缺省），且位点晚于 W
        let mut stream = config
            .watch(ConfigWatchRequest {
                prefix: PREFIX.into(),
                start_revision: w,
            })
            .await
            .expect("Config.Watch 订阅必须成功")
            .into_inner();

        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut saw_delete = false;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_secs(5), stream.message()).await {
                Ok(Ok(Some(ev))) => {
                    if ev.key == KEY1 && ev.new_value.is_none() {
                        assert!(
                            ev.revision > w,
                            "删除事件位点必须晚于快照位点：ev.revision={} W={w}",
                            ev.revision
                        );
                        saw_delete = true;
                        break;
                    }
                    // 回放段可能重播快照已含的 put（duplicate = 幂等范畴），跳过
                }
                Ok(Ok(None)) => panic!("watch 流被服务端提前关闭（未收到删除事件）"),
                Ok(Err(e)) => panic!("watch 流错误：{e}"),
                Err(_) => {} // 单次 5s 无事件，继续等（总预算 15s）
            }
        }
        assert!(
            saw_delete,
            "快照后被删除的 key 必须能从位点事件可靠收敛（new_value 缺省的删除事件）"
        );

        // 4) 同 agent 缓存也必须收敛到删除（后台 KV watch 应用删除）
        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        loop {
            let got = config
                .get(ConfigGetRequest { key: KEY1.into() })
                .await
                .expect("Config.Get 必须成功")
                .into_inner();
            if !got.found {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "agent 本地缓存未收敛到删除"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }

        // 5) 实时路径（start_revision = 0）：后续 Put 必须下发值事件（presence=Some）
        let mut live = config
            .watch(ConfigWatchRequest {
                prefix: PREFIX.into(),
                start_revision: 0,
            })
            .await
            .expect("Config.Watch(0) 订阅必须成功")
            .into_inner();
        // 订阅在服务端异步注册；给注册留出窗口（与 agent_watch_test 同口径）
        tokio::time::sleep(Duration::from_millis(500)).await;

        config
            .put(ConfigPutRequest {
                key: KEY2.into(),
                value: "v2".into(),
            })
            .await
            .expect("Config.Put 必须成功");

        let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
        let mut saw_put = false;
        while tokio::time::Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_secs(5), live.message()).await {
                Ok(Ok(Some(ev))) => {
                    if ev.key == KEY2 {
                        assert!(
                            ev.new_value.as_deref() == Some("v2"),
                            "值事件必须携带 new_value（presence=Some）: {ev:?}"
                        );
                        saw_put = true;
                        break;
                    }
                }
                Ok(Ok(None)) => panic!("live watch 流被服务端提前关闭"),
                Ok(Err(e)) => panic!("live watch 流错误：{e}"),
                Err(_) => {}
            }
        }
        assert!(saw_put, "实时路径必须下发值事件");

        agent_handle.abort();
    }
}
