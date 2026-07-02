// TDD: 线程池资源隔离测试 (Phase A — 待实施)
//
// v8.2 §3.2: 独立线程池隔离
// - proxy-core: 核心代理路径（高优先级）
// - dataplane: 数据面读写、复制流（中优先级）
// - background: Watch 同步、淘汰、心跳（低优先级）
//
// RED stage: 此测试应编译失败 — ThreadPoolConfig 尚未定义。

use coord_agent::AgentConfig;

/// 验证 ThreadPoolConfig 默认值
#[test]
fn test_thread_pool_config_defaults() {
    let config = coord_agent::ThreadPoolConfig::default();

    // 默认值与 v8.2 §3.2 一致
    assert_eq!(config.proxy_core_size, 8, "proxy-core 默认 8 线程");
    assert_eq!(config.dataplane_size, 4, "dataplane 默认 4 线程");
    assert_eq!(config.background_size, 2, "background 默认 2 线程");
}

/// 验证 ThreadPoolConfig 从 TOML 反序列化
#[test]
fn test_thread_pool_config_from_toml() {
    let toml_str = r#"
[agent.threadpools]
proxy_core_size = 16
dataplane_size = 8
background_size = 4
"#;

    // 通过 AgentConfig 间接反序列化
    #[derive(serde::Deserialize)]
    struct ThreadPoolWrapper {
        agent: AgentThreadPoolWrapper,
    }
    #[derive(serde::Deserialize)]
    struct AgentThreadPoolWrapper {
        threadpools: coord_agent::ThreadPoolConfig,
    }

    let wrapper: ThreadPoolWrapper = toml::from_str(toml_str).expect("TOML 解析失败");
    let config = wrapper.agent.threadpools;

    assert_eq!(config.proxy_core_size, 16);
    assert_eq!(config.dataplane_size, 8);
    assert_eq!(config.background_size, 4);
}

/// 验证 ThreadPoolConfig 集成到 AgentConfig 中
#[test]
fn test_agent_config_includes_thread_pools() {
    let config = AgentConfig::default();

    // AgentConfig 默认应包含线程池配置
    let pools = &config.thread_pools;
    assert_eq!(pools.proxy_core_size, 8);
    assert_eq!(pools.dataplane_size, 4);
    assert_eq!(pools.background_size, 2);
}

/// 验证 AgentThreadPools 可创建并 spawn 任务到对应池
#[test]
fn test_thread_pools_spawn_tasks() {
    let rt = tokio::runtime::Runtime::new().expect("创建 runtime 失败");

    rt.block_on(async {
        let config = coord_agent::ThreadPoolConfig::default();
        let pools = coord_agent::AgentThreadPools::new(config);

        // spawn 到 proxy_core 池
        let (tx1, mut rx1) = tokio::sync::mpsc::unbounded_channel();
        pools.spawn_proxy_core(async move {
            let _ = tx1.send("proxy_core_task");
        });

        // spawn 到 dataplane 池
        let (tx2, mut rx2) = tokio::sync::mpsc::unbounded_channel();
        pools.spawn_dataplane(async move {
            let _ = tx2.send("dataplane_task");
        });

        // spawn 到 background 池
        let (tx3, mut rx3) = tokio::sync::mpsc::unbounded_channel();
        pools.spawn_background(async move {
            let _ = tx3.send("background_task");
        });

        // 验证所有任务执行完毕
        assert_eq!(rx1.recv().await, Some("proxy_core_task"));
        assert_eq!(rx2.recv().await, Some("dataplane_task"));
        assert_eq!(rx3.recv().await, Some("background_task"));
    });
}
