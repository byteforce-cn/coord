// coord-client: 会话自动续期（长时 CLI / 运维脚本）
//
// 背景：CLI 凭据若只做**进程级一次性注入**（`--token` / `COORD_TOKEN`），
// CCT 过期（默认 15 分钟）后，长时脚本的后续管理命令会突然
// `permission denied`，只能靠调用方自行重新登录。
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

/// 恢复重试的初始退避（秒）。
pub const RECOVERY_INITIAL_BACKOFF_SECS: u64 = 5;

/// 恢复重试的退避上限（秒）。
pub const RECOVERY_MAX_BACKOFF_SECS: u64 = 120;

/// 续期节奏参数（默认 = 生产值；测试传入极小值以秒级完成）。
#[derive(Debug, Clone, Copy)]
pub struct RefreshOptions {
    /// 到期前提前量（秒）
    pub lead_secs: i64,
    /// 两次续期之间的最小间隔（秒）
    pub min_interval_secs: u64,
    /// 恢复失败的退避起点（秒；每次 ×2，封顶
    /// [`RECOVERY_MAX_BACKOFF_SECS`]）。仅 [`spawn_session_refresher_with_recovery`]
    /// 使用。
    pub recovery_initial_backoff_secs: u64,
}

impl Default for RefreshOptions {
    fn default() -> Self {
        Self {
            lead_secs: REFRESH_LEAD_SECS,
            min_interval_secs: MIN_REFRESH_INTERVAL_SECS,
            recovery_initial_backoff_secs: RECOVERY_INITIAL_BACKOFF_SECS,
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

/// 续期失败后的**恢复动作**。
///
/// 背景：refresh token 单次使用且有 24h TTL，一旦失效（被消费/轮换竞争/服务端
/// 清理），[`spawn_session_refresher`] 的缺省行为是「清除凭据并停止」——
/// 对一次性 CLI 是正确的 fail-closed，对长驻进程（agent）则意味着自流量能力
/// **永久死亡**：锁续期与角色同步凭据（一次性引导 CCT）会持续失败，
/// 且无恢复路径。
///
/// 实现方通常用**持久化密码**重新 `Authenticate`（agent 的账户密码落盘在
/// `data_dir`，见 `coord-agent::plugin::identity`）。恢复动作会被反复调用
/// （失败退避重试），直到成功为止——恢复失败**不等于**任务结束。
#[async_trait]
pub trait SessionRecovery: Send + Sync + 'static {
    /// 重建会话（成功 = 新的 CCT + refresh token；失败 = 错误串）。
    async fn recover(&self) -> Result<SessionTokens, String>;
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
///
/// 长驻进程（agent）需要「失败后自动恢复」时用
/// [`spawn_session_refresher_with_recovery`]。
pub fn spawn_session_refresher(
    gateway: Arc<dyn SessionGateway>,
    provider: Arc<CachedTokenProvider>,
    initial: SessionTokens,
    options: RefreshOptions,
) -> tokio::task::JoinHandle<()> {
    spawn_refresh_loop(gateway, provider, initial, options, None)
}

/// 带**恢复动作**的续期循环。
///
/// 与 [`spawn_session_refresher`] 的唯一差别：refresh 失败（或 refresh token 缺失）
/// 时不清空后退出，而是：
/// 1. `provider.clear()`（fail-closed：恢复期间不携带死凭据）；
/// 2. 反复调用 `recovery.recover()`，失败按指数退避（
///    `options.recovery_initial_backoff_secs` → 封顶 [`RECOVERY_MAX_BACKOFF_SECS`]）
///    **无限重试**（进程级长驻任务；正常场景一次即恢复）；
/// 3. 恢复成功 → 写回凭据并回到正常续期节奏。
///
/// 判据：注入 refresh 失效 ⇒ 必须恢复（不得停摆）；恢复失败期间
/// 每次尝试都有 WARN 日志（恢复动作自身另行接指标）。
pub fn spawn_session_refresher_with_recovery(
    gateway: Arc<dyn SessionGateway>,
    provider: Arc<CachedTokenProvider>,
    initial: SessionTokens,
    options: RefreshOptions,
    recovery: Arc<dyn SessionRecovery>,
) -> tokio::task::JoinHandle<()> {
    spawn_refresh_loop(gateway, provider, initial, options, Some(recovery))
}

fn spawn_refresh_loop(
    gateway: Arc<dyn SessionGateway>,
    provider: Arc<CachedTokenProvider>,
    initial: SessionTokens,
    options: RefreshOptions,
    recovery: Option<Arc<dyn SessionRecovery>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut refresh_token = initial.refresh_token;
        let mut expires_at = initial.expires_at;
        loop {
            let sleep_for = next_refresh_delay(expires_at, now_secs(), options);
            tracing::debug!("session refresher: next refresh in {sleep_for:?}");
            tokio::time::sleep(sleep_for).await;

            let refreshed = match refresh_token.clone() {
                Some(rt) => gateway.refresh(&rt).await,
                None => {
                    if recovery.is_none() {
                        tracing::debug!("session refresher: no refresh token available; stopping");
                        return;
                    }
                    Err("no refresh token available".to_string())
                }
            };
            match refreshed {
                Ok(next) => {
                    provider.set(next.cct.clone());
                    refresh_token = next.refresh_token;
                    expires_at = next.expires_at;
                    tracing::debug!("session refresher: CCT renewed (expires_at={expires_at})");
                }
                Err(e) => {
                    tracing::warn!("session refresher: refresh failed ({e})");
                    let Some(recovery) = &recovery else {
                        tracing::warn!(
                            "session refresher: clearing credential and stopping (no recovery \
                             configured)"
                        );
                        provider.clear();
                        return;
                    };
                    // fail-closed：恢复期间不携带死凭据
                    provider.clear();
                    let mut backoff = Duration::from_secs(options.recovery_initial_backoff_secs);
                    loop {
                        match recovery.recover().await {
                            Ok(next) => {
                                provider.set(next.cct.clone());
                                refresh_token = next.refresh_token;
                                expires_at = next.expires_at;
                                tracing::info!(
                                    "session refresher: recovered after failure \
                                     (expires_at={expires_at})"
                                );
                                break;
                            }
                            Err(re) => {
                                tracing::warn!(
                                    "session refresher: recovery failed ({re}); retrying in \
                                     {backoff:?}"
                                );
                                tokio::time::sleep(backoff).await;
                                backoff = backoff
                                    .saturating_mul(2)
                                    .min(Duration::from_secs(RECOVERY_MAX_BACKOFF_SECS));
                            }
                        }
                    }
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
    /// 无最小间隔、恢复零退避（秒级完成）。
    const FAST: RefreshOptions = RefreshOptions {
        lead_secs: REFRESH_LEAD_SECS,
        min_interval_secs: 0,
        recovery_initial_backoff_secs: 0,
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

    // ──── 失败恢复（长驻进程不再永久死亡）────

    /// 判据①（正）：refresh 失效（现场形态 `refresh token not found`）⇒
    /// 恢复动作被调用并成功 → 凭据回写、续期循环**继续**（不停摆）。
    #[tokio::test]
    async fn refresher_recovers_after_refresh_failure_and_keeps_looping() {
        /// 第一次 refresh 失败（模拟 token 已失效），之后成功。
        struct OneShotFailGateway {
            calls: AtomicUsize,
        }
        #[async_trait]
        impl SessionGateway for OneShotFailGateway {
            async fn refresh(&self, _rt: &str) -> Result<SessionTokens, String> {
                let n = self.calls.fetch_add(1, Ordering::SeqCst);
                if n == 0 {
                    return Err("refresh token not found".into());
                }
                Ok(SessionTokens {
                    cct: "cct-after".into(),
                    refresh_token: Some("rt-after".into()),
                    expires_at: now_secs() + 60,
                })
            }
        }

        struct PasswordRecoveryStub {
            calls: AtomicUsize,
        }
        #[async_trait]
        impl SessionRecovery for PasswordRecoveryStub {
            async fn recover(&self) -> Result<SessionTokens, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Ok(SessionTokens {
                    cct: "cct-recovered".into(),
                    refresh_token: Some("rt-recovered".into()),
                    expires_at: now_secs() + 60,
                })
            }
        }

        let gateway = Arc::new(OneShotFailGateway {
            calls: AtomicUsize::new(0),
        });
        let recovery = Arc::new(PasswordRecoveryStub {
            calls: AtomicUsize::new(0),
        });
        let provider = Arc::new(CachedTokenProvider::new(Some("cct-old".into())));
        let handle = spawn_session_refresher_with_recovery(
            Arc::clone(&gateway) as Arc<dyn SessionGateway>,
            Arc::clone(&provider),
            SessionTokens {
                cct: "cct-old".into(),
                refresh_token: Some("rt-old".into()),
                expires_at: now_secs() + 1,
            },
            FAST,
            Arc::clone(&recovery) as Arc<dyn SessionRecovery>,
        );

        // 第一次 refresh 失败 → 恢复 → 后续 refresh 成功（循环继续）
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while gateway.calls.load(Ordering::SeqCst) < 2 || recovery.calls.load(Ordering::SeqCst) < 1
        {
            assert!(
                std::time::Instant::now() < deadline,
                "refresher must recover after the failure and keep looping"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let token = provider.current_token().unwrap_or_default();
        assert!(
            token == "cct-recovered" || token == "cct-after",
            "provider must hold a recovered token, got {token:?}"
        );
        handle.abort();
    }

    /// 判据②（负控制/持续失败）：恢复动作持续失败 → 循环**不得退出**，
    /// 必须继续重试；期间凭据保持清空（fail-closed，绝不带死凭据）。
    #[tokio::test]
    async fn refresher_keeps_retrying_while_recovery_fails() {
        struct FailingRecovery {
            calls: AtomicUsize,
        }
        #[async_trait]
        impl SessionRecovery for FailingRecovery {
            async fn recover(&self) -> Result<SessionTokens, String> {
                self.calls.fetch_add(1, Ordering::SeqCst);
                Err("server unreachable".into())
            }
        }

        let gateway = Arc::new(StubGateway {
            calls: AtomicUsize::new(0),
            fail: true,
        });
        let recovery = Arc::new(FailingRecovery {
            calls: AtomicUsize::new(0),
        });
        let provider = Arc::new(CachedTokenProvider::new(Some("cct-old".into())));
        let handle = spawn_session_refresher_with_recovery(
            gateway as Arc<dyn SessionGateway>,
            Arc::clone(&provider),
            SessionTokens {
                cct: "cct-old".into(),
                refresh_token: Some("rt-old".into()),
                expires_at: now_secs() + 1,
            },
            FAST,
            Arc::clone(&recovery) as Arc<dyn SessionRecovery>,
        );

        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        while recovery.calls.load(Ordering::SeqCst) < 2 {
            assert!(
                std::time::Instant::now() < deadline,
                "recovery must keep retrying (not stop) while it fails"
            );
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert_eq!(
            provider.current_token(),
            None,
            "恢复期间凭据必须保持清空（fail-closed）"
        );
        assert!(
            !handle.is_finished(),
            "恢复重试期间续期循环不得退出（不得停摆）"
        );
        handle.abort();
    }
}
