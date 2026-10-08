// dev 专用 transit 默认 KEK（见 ADR-0009）
//
// `coord dev` 在未注入 KEK 材料时回退到内建 dev 默认材料；该回退**仅**经
// `AgentServer::with_dev_default_transit_kek(true)` 的调用链可达：
//   - 不开开关：transit + 无材料 ⇒ 拒绝启动（生产 fail-closed 口径与消息不变）；
//   - `agent.toml` 的同名未知键无法开启（配置文件不可达）；
//   - 开关开启后 Transit gRPC 真实可调用（Encrypt/Decrypt 往返）。

use std::net::TcpListener;

use coord_agent::{AgentConfig, AgentServer, ServiceConfig};

fn find_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

/// transit 开启、无静态对端（skeleton）、数据目录指向临时目录（保证无文件材料）。
fn transit_config(port: u16, tag: &str) -> (AgentConfig, tempfile::TempDir) {
    let tmpdir = tempfile::tempdir().unwrap();
    let config = AgentConfig {
        agent_addr: format!("127.0.0.1:{port}"),
        http_addr: format!("127.0.0.1:{}", find_port()),
        data_dir: tmpdir
            .path()
            .join(format!("data-{tag}"))
            .to_string_lossy()
            .into_owned(),
        static_peers: vec![],
        services: ServiceConfig {
            transit: true,
            ..ServiceConfig::default()
        },
        ..Default::default()
    };
    (config, tmpdir)
}

/// 环境变量注入的材料会使「无材料」前提失效——CI 上不应设置；设了就跳过（防假红）。
fn env_material_injected() -> bool {
    std::env::var("COORD_TRANSIT_KEK").is_ok()
}

/// 不开 dev 开关：transit + 无材料 ⇒ 拒绝启动，错误消息保持生产口径。
#[tokio::test]
async fn test_transit_without_material_is_fail_closed_without_dev_switch() {
    if env_material_injected() {
        eprintln!("skip: COORD_TRANSIT_KEK is set in this environment");
        return;
    }
    let (config, _tmpdir) = transit_config(find_port(), "closed");
    let err = AgentServer::new(config)
        .serve()
        .await
        .expect_err("transit without KEK material must refuse startup");
    let msg = err.to_string();
    assert!(
        msg.contains("KEK injection failed") && msg.contains("COORD_TRANSIT_KEK"),
        "production refusal message must stay intact, got: {msg}"
    );
}

/// 打开 dev 开关：内建默认材料使 agent 可启动，Transit gRPC 往返成功。
#[tokio::test]
async fn test_dev_switch_activates_default_kek_and_serves_transit() {
    let port = find_port();
    let (config, _tmpdir) = transit_config(port, "positive");
    let server = AgentServer::new(config).with_dev_default_transit_kek(true);
    let handle = tokio::spawn(async move {
        let _ = server.serve().await;
    });

    // 端口就绪（含服务启动）
    let addr = format!("127.0.0.1:{port}");
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(15);
    loop {
        if tokio::net::TcpStream::connect(&addr).await.is_ok() {
            break;
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "agent with the dev default KEK never became ready"
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    let mut client =
        coord_proto::transit::v1::transit_client::TransitClient::connect(format!("http://{addr}"))
            .await
            .expect("transit service must be mounted under the dev switch");

    let plaintext = b"dev-default-kek-probe";
    let ciphertext = client
        .encrypt(coord_proto::transit::v1::TransitEncryptRequest {
            plaintext: plaintext.to_vec(),
            context: vec![],
        })
        .await
        .expect("Encrypt must succeed with the dev default KEK")
        .into_inner()
        .ciphertext;
    let decrypted = client
        .decrypt(coord_proto::transit::v1::TransitDecryptRequest {
            ciphertext,
            context: vec![],
        })
        .await
        .expect("Decrypt must succeed with the dev default KEK")
        .into_inner()
        .plaintext;
    assert_eq!(decrypted, plaintext);

    handle.abort();
    let _ = tokio::time::timeout(std::time::Duration::from_secs(3), handle).await;
}

/// 配置文件不可达：`agent.toml` 的同名未知键无法开启 dev 回退
/// （load 失败 = fail-closed；load 成功但缺材料仍拒绝——二者均满足）。
#[tokio::test]
async fn test_toml_cannot_enable_dev_default_kek() {
    if env_material_injected() {
        eprintln!("skip: COORD_TRANSIT_KEK is set in this environment");
        return;
    }
    let port = find_port();
    let tmpdir = tempfile::tempdir().unwrap();
    let cfg_path = tmpdir.path().join("agent.toml");
    std::fs::write(
        &cfg_path,
        format!(
            "agent_addr = \"127.0.0.1:{port}\"\n\
             http_addr = \"127.0.0.1:{}\"\n\
             data_dir = \"{}\"\n\
             dev_default_transit_kek = true\n\
             [services]\n\
             transit = true\n",
            find_port(),
            tmpdir.path().join("data").display()
        ),
    )
    .unwrap();

    if let Ok(config) = AgentConfig::from_file(&cfg_path) {
        let err = AgentServer::new(config)
            .serve()
            .await
            .expect_err("config file must not be able to enable the dev default KEK");
        assert!(
            err.to_string().contains("KEK injection failed"),
            "expected the production KEK refusal, got: {err}"
        );
    }
}
