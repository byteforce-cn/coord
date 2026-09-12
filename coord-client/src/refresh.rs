// coord-client: 会话自动续期（长时 CLI / 运维脚本）
//
// 背景（计划 Phase 5 遗留项）：CLI 的凭据此前是**进程级一次性注入**
// （`--token` / `COORD_TOKEN`）——CCT 过期（默认 15 分钟）后，长时脚本的后续
// 管理命令会突然 `permission denied`，只能靠调用方自行重新登录。
//
// 本模块提供：
// - [`SessionTokens`]：一次认证/续期的结果（CCT + 单次使用的 refresh token + 到期时刻）；
// - [`SessionGateway`]：认证门面抽象（生产实现 = [`crate::AuthClient`]；测试注入 stub）；
// - [`spawn_session_refresher`]：后台续期循环——到期前 `REFRESH_LEAD_SECS` 用
//   refresh token 换新会话（服务端保证单次使用），失败则标记失效（fail-closed）。
//
// 续期只写 [`CachedTokenProvider`]（读取廉价、无锁竞争），因此请求热路径零额外开销。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use crate::credential::CachedTokenProvider;

/// 续期提前量（秒）：在 CCT 到期前多久开始换新。
pub const REFRESH_LEAD_SECS: i64 = 120;

/// 单次续期检查的最小间隔（秒）——防止 expires_at 异常导致忙循环。
const MIN_REFRESH_INTERVAL_SECS: u64 = 5;

/// 续期节奏参数（默认 = 生产值；测试传入极小值以秒级完成）。
#[derive(Debug, Clone, Copy)]
pub struct RefreshOptions {
    /// 到期前提前量（秒）
    pub lead_secs: i64,
    /// 两次续期之间的最小间隔（秒）
    pub min_interval_secs: u64,
}

impl Default for RefreshOptions {
    fn default() -> Self {
        Self {
            lead_secs: REFRESH_LEAD_SECS,
            min_interval_secs: MIN_REFRESH_INTERVAL_SECS,
        }
    }
}

/// 一次认证 / 续期签发的会话。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTokens {
    /// 受限 CCT
    pub cct: String,
    /// refresh token（服务端单次使用；`None` = 未签发）
    pub refresh_token: Option<String>,
    /// 到期时刻（Unix 秒；`0` = 未知 → 按固定间隔续期）
    pub expires_at: i64,
}

/// 认证门面（生产 = `AuthClient`；测试注入 stub）。
#[async_trait]
pub trait SessionGateway: Send + Sync + 'static {
    /// 用 refresh token 换新会话（单次使用语义由服务端保证）。
    async fn refresh(&self, refresh_token: &str) -> Result<SessionTokens, String>;
}

/// 当前 Unix 秒。
fn now_secs() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or(0)
}

/// 根据 `expires_at` 计算下一次续期的等待时长。
pub fn next_refresh_delay(expires_at: i64, now: i64, options: RefreshOptions) -> Duration {
    if expires_at <= 0 {
        return Duration::from_secs(600);
    }
    let lead = (expires_at - now - options.lead_secs).max(0);
    Duration::from_secs(
        u64::try_from(lead)
            .unwrap_or(0)
            .max(options.min_interval_secs),
    )
}

/// 启动后台续期循环；返回任务句柄（调用方 drop 句柄即停止续期）。
///
/// - `initial.expires_at` 决定首次等待时长（到期前 [`REFRESH_LEAD_SECS`] 秒）；
/// - 每次续期成功 → 覆盖 `provider` 中的 CCT 并记录新的 refresh token；
/// - 续期失败（refresh 过期/已消费/网络故障）→ **清除** `provider` 凭据并退出循环
///   （fail-closed：宁可让后续请求明确失败，也不静默携带过期凭据）。
pub fn spawn_session_refresher(
    gateway: Arc<dyn SessionGateway>,
    provider: Arc<CachedTokenProvider>,
    initial: SessionTokens,
    options: RefreshOptions,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut refresh_token = initial.refresh_token;
        let mut expires_at = initial.expires_at;
        loop {
            let sleep_for = next_refresh_delay(expires_at, now_secs(), options);
            tracing::debug!("session refresher: next refresh in {sleep_for:?}");
            tokio::time::sleep(sleep_for).await;

            let Some(rt) = refresh_token.clone() else {
                tracing::debug!("session refresher: no refresh token available; stopping");
                return;
            };
            match gateway.refresh(&rt).await {
                Ok(next) => {
                    provider.set(next.cct.clone());
                    refresh_token = next.refresh_token;
                    expires_at = next.expires_at;
                    tracing::debug!("session refresher: CCT renewed (expires_at={expires_at})");
                }
                Err(e) => {
                    tracing::warn!("session refresher: refresh failed ({e}); clearing credential");
                    provider.clear();
                    return;
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::credential::TokenProvider;
    use std::sync::atomic::{AtomicUsize, Ordering};

    /// 测试用节奏：保留生产提前量（120s > 测试会话剩余寿命 → 立即续期），
    /// 无最小间隔（秒级完成）。
    const FAST: RefreshOptions = RefreshOptions {
        lead_secs: REFRESH_LEAD_SECS,
        min_interval_secs: 0,
    };

    struct StubGateway {
        calls: AtomicUsize,
        fail: bool,
    }

    #[async_trait]
    impl SessionGateway for StubGateway {
        async fn refresh(&self, _refresh_token: &str) -> Result<SessionTokens, String> {
            let n = self.calls.fetch_add(1, Ordering::SeqCst);
            if self.fail {
                return Err("refresh token expired".into());
            }
            Ok(SessionTokens {
                cct: format!("cct-{}", n + 2),
                refresh_token: Some(format!("rt-{}", n + 2)),
                expires_at: now_secs() + 60,
            })
        }
    }

    #[test]
    fn next_refresh_delay_uses_lead_and_floor() {
        let now = 1_000;
        let opts = RefreshOptions::default();
        // 到期前 120s 触发
        assert_eq!(next_refresh_delay(1_600, now, opts).as_secs(), 480);
        // 已进入提前量窗口 → 最小间隔兜底
        assert_eq!(
            next_refresh_delay(1_050, now, opts).as_secs(),
            MIN_REFRESH_INTERVAL_SECS
        );
        // 已过期同理
        assert_eq!(
            next_refresh_delay(900, now, opts).as_secs(),
            MIN_REFRESH_INTERVAL_SECS
        );
        // 未知到期时刻 → 固定间隔
        assert_eq!(next_refresh_delay(0, now, opts).as_secs(), 600);
    }

    /// 续期循环用新 refresh token 覆盖 provider（服务端单次使用语义）。
    #[tokio::test]
    async fn refresher_renews_and_rotates_refresh_token() {
        let gateway = Arc::new(StubGateway {
            calls: AtomicUsize::new(0),
            fail: false,
        });
        let provider = Arc::new(CachedTokenProvider::new(Some("cct-1".into())));
        let handle = spawn_session_refresher(
            Arc::clone(&gateway) as Arc<dyn SessionGateway>,
            Arc::clone(&provider),
            SessionTokens {
                cct: "cct-1".into(),
                refresh_token: Some("rt-1".into()),
                expires_at: now_secs() + 1,
            },
            FAST,
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while gateway.calls.load(Ordering::SeqCst) < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "refresher must renew repeatedly"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let token = provider.current_token().unwrap_or_default();
        assert!(
            token.starts_with("cct-") && token != "cct-1",
            "provider must hold a renewed CCT, got {token:?}"
        );
        handle.abort();
    }

    /// 续期失败 → 清除凭据并停止（fail-closed）。
    #[tokio::test]
    async fn refresher_clears_credential_on_failure() {
        let gateway = Arc::new(StubGateway {
            calls: AtomicUsize::new(0),
            fail: true,
        });
        let provider = Arc::new(CachedTokenProvider::new(Some("cct-old".into())));
        let handle = spawn_session_refresher(
            gateway as Arc<dyn SessionGateway>,
            Arc::clone(&provider),
            SessionTokens {
                cct: "cct-old".into(),
                refresh_token: Some("rt-old".into()),
                expires_at: now_secs() + 1,
            },
            FAST,
        );
        let _ = handle;

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while provider.is_set() {
            assert!(
                std::time::Instant::now() < deadline,
                "credential must be cleared after a failed refresh"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(provider.current_token(), None);
    }

    /// 无 refresh token → 循环立即退出（不产生无意义的续期请求）。
    #[tokio::test]
    async fn refresher_stops_without_refresh_token() {
        let gateway = Arc::new(StubGateway {
            calls: AtomicUsize::new(0),
            fail: false,
        });
        let provider = Arc::new(CachedTokenProvider::new(Some("cct-1".into())));
        let handle = spawn_session_refresher(
            Arc::clone(&gateway) as Arc<dyn SessionGateway>,
            Arc::clone(&provider),
            SessionTokens {
                cct: "cct-1".into(),
                refresh_token: None,
                expires_at: now_secs() + 1,
            },
            FAST,
        );
        tokio::time::timeout(Duration::from_secs(10), handle)
            .await
            .expect("refresher must exit when no refresh token is available")
            .expect("task must not panic");
        assert_eq!(gateway.calls.load(Ordering::SeqCst), 0);
        assert_eq!(provider.current_token().as_deref(), Some("cct-1"));
    }
}
