// TDD: Agent 配置热加载测试 (Phase C4 — RED)
//
// 验证文件监听 + 原子替换功能。
//
// RED 阶段：config_watcher 模块尚不存在，此测试预期编译失败。

use std::path::PathBuf;
use std::sync::Arc;

use coord_agent::config_watcher::ConfigWatcher;

/// 写入 TOML 配置文件
fn write_config(path: &std::path::Path, agent_addr: &str) {
    let content = format!(
        r#"
agent_addr = "{}"
http_addr = "127.0.0.1:19528"
data_dir = "/tmp/coord-agent-test"
discovery_mode = "static"
static_peers = ["10.0.1.1:50051"]
"#,
        agent_addr
    );
    std::fs::write(path, content).unwrap();
}

/// C4.1: ConfigWatcher 初始加载
#[tokio::test]
async fn test_config_watcher_initial_load() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("agent.toml");

    write_config(&config_path, "127.0.0.1:19527");

    let watcher = ConfigWatcher::new(&config_path).unwrap();
    let current = watcher.current_config();
    assert_eq!(current.agent_addr, "127.0.0.1:19527");
}

/// C4.2: ConfigWatcher 检测文件变更并重新加载
#[tokio::test]
async fn test_config_watcher_reload_on_change() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("agent.toml");

    write_config(&config_path, "127.0.0.1:19527");

    let watcher = ConfigWatcher::new(&config_path).unwrap();
    assert_eq!(watcher.current_config().agent_addr, "127.0.0.1:19527");

    // 修改文件
    write_config(&config_path, "0.0.0.0:19527");

    // 触发重新加载
    watcher.reload().unwrap();

    let config = watcher.current_config();
    assert_eq!(config.agent_addr, "0.0.0.0:19527");
}

/// C4.3: ConfigWatcher 对不存在的文件返回错误
#[test]
fn test_config_watcher_missing_file() {
    let result = ConfigWatcher::new(&PathBuf::from("/nonexistent/path/config.toml"));
    assert!(result.is_err());
}

/// C4.4: ConfigWatcher 原子替换（读-修改-写期间读取者看到一致状态）
#[tokio::test]
async fn test_config_watcher_atomic_swap() {
    let dir = tempfile::tempdir().unwrap();
    let config_path = dir.path().join("agent.toml");

    write_config(&config_path, "127.0.0.1:19527");

    let watcher = Arc::new(ConfigWatcher::new(&config_path).unwrap());

    // 验证初始值
    let c1 = watcher.current_config();
    assert_eq!(c1.agent_addr, "127.0.0.1:19527");

    // 原子替换后所有引用者看到新值
    write_config(&config_path, "10.0.0.1:19527");
    watcher.reload().unwrap();

    let c2 = watcher.current_config();
    assert_eq!(c2.agent_addr, "10.0.0.1:19527");
    // c1 仍是旧值（Arc 快照）
    assert_eq!(c1.agent_addr, "127.0.0.1:19527");
}
