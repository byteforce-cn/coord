// coord-client: 客户端配置
//
// 定义连接参数、重试策略、Leader 发现行为等配置项。

use std::time::Duration;

/// 客户端配置
#[derive(Debug, Clone)]
pub struct Config {
    /// gRPC 端点列表（如 `["127.0.0.1:50051", "127.0.0.1:50052"]`）
    pub endpoints: Vec<String>,

    /// 请求超时（默认 5 秒）
    pub request_timeout: Duration,

    /// 连接超时（默认 3 秒）
    pub connect_timeout: Duration,

    /// Leader 发现全量刷新间隔（默认 30 秒）
    pub leader_refresh_interval: Duration,

    /// 最大重试次数（默认 5）
    pub max_retries: u32,

    /// 重试退避起始间隔（默认 100ms）
    pub retry_initial_backoff: Duration,

    /// 重试退避最大间隔（默认 1.6s）
    pub retry_max_backoff: Duration,

    /// 每端点 gRPC 连接数（默认 2）
    pub connections_per_endpoint: usize,

    /// 连接空闲超时（超过此时间自动关闭，默认 5 分钟）
    pub connection_idle_timeout: Duration,
}

impl Config {
    /// 使用默认参数创建配置，指定至少一个端点。
    ///
    /// # 示例
    /// ```ignore
    /// let config = Config::new(vec!["127.0.0.1:50051".into()]);
    /// ```
    pub fn new(endpoints: Vec<String>) -> Self {
        assert!(!endpoints.is_empty(), "at least one endpoint required");
        Self {
            endpoints,
            request_timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(3),
            leader_refresh_interval: Duration::from_secs(30),
            max_retries: 5,
            retry_initial_backoff: Duration::from_millis(100),
            retry_max_backoff: Duration::from_millis(1600),
            connections_per_endpoint: 2,
            connection_idle_timeout: Duration::from_secs(300),
        }
    }

    /// 设置请求超时
    pub fn with_request_timeout(mut self, timeout: Duration) -> Self {
        self.request_timeout = timeout;
        self
    }

    /// 设置最大重试次数
    pub fn with_max_retries(mut self, max_retries: u32) -> Self {
        self.max_retries = max_retries;
        self
    }
}

impl Default for Config {
    fn default() -> Self {
        Self::new(vec!["127.0.0.1:50051".into()])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config = Config::new(vec!["localhost:50051".into()]);
        assert_eq!(config.endpoints.len(), 1);
        assert_eq!(config.request_timeout, Duration::from_secs(5));
        assert_eq!(config.max_retries, 5);
        assert_eq!(config.connections_per_endpoint, 2);
    }

    #[test]
    fn test_config_builder() {
        let config = Config::new(vec!["a:1".into(), "b:2".into()])
            .with_request_timeout(Duration::from_secs(10))
            .with_max_retries(3);
        assert_eq!(config.endpoints.len(), 2);
        assert_eq!(config.request_timeout, Duration::from_secs(10));
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    #[should_panic(expected = "at least one endpoint")]
    fn test_config_empty_endpoints_panics() {
        Config::new(vec![]);
    }
}
