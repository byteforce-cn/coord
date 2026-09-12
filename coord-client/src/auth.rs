// coord-client: Auth 子客户端
//
// 覆盖认证与会话（Authenticate / RefreshToken / Bootstrap）、用户与角色管理，
// 以及 CCT v3 能力授予（RoleGrantCapability / RoleRevokeCapability）。
//
// 写操作走 `execute_write_with_retry`：follower 上自动按 leader hint 重定向；
// 读操作直取 leader 通道。

use coord_core::error::Result;

use coord_proto::auth::auth_client::AuthClient as AuthStub;
use coord_proto::auth::{
    AuthEnableRequest, AuthStatusRequest, AuthenticateRequest, AuthenticateResponse,
    BootstrapRequest, BootstrapResponse, BootstrapTokenInfo, BootstrapTokenIssueRequest,
    BootstrapTokenIssueResponse, BootstrapTokenListRequest, BootstrapTokenRevokeRequest,
    GetRevocationDeltaRequest, GetRevocationDeltaResponse, ListRolesRequest, ListRolesResponse,
    RefreshTokenRequest, RefreshTokenResponse, Role, RoleAddRequest, RoleDeleteRequest,
    RoleGrantCapabilityRequest, RoleListRequest, RoleRevokeCapabilityRequest, User, UserAddRequest,
    UserChangePasswordRequest, UserDeleteRequest, UserGetRequest, UserGrantRoleRequest,
    UserListRequest, UserRevokeRoleRequest,
};

use crate::client::{from_status, Client};

/// Auth 操作客户端。
#[derive(Clone)]
pub struct AuthClient {
    client: Client,
}

impl AuthClient {
    pub(crate) fn new(client: Client) -> Self {
        Self { client }
    }

    // ──── 认证 / 会话 ────

    /// 用户名密码认证：返回 CCT + refresh token。
    pub async fn authenticate(&self, name: &str, password: &str) -> Result<AuthenticateResponse> {
        let name = name.to_string();
        let password = password.to_string();
        self.client
            .execute_write_with_retry(move |channel| {
                let mut stub = AuthStub::new(channel);
                let req = AuthenticateRequest {
                    name: name.clone(),
                    password: password.clone(),
                };
                async move { stub.authenticate(req).await.map(|r| r.into_inner()) }
            })
            .await
    }

    /// 用 refresh token 换新会话（单次使用语义由服务端保证）。
    pub async fn refresh_token(&self, refresh_token: &str) -> Result<RefreshTokenResponse> {
        let token = refresh_token.to_string();
        self.client
            .execute_write_with_retry(move |channel| {
                let mut stub = AuthStub::new(channel);
                let req = RefreshTokenRequest {
                    refresh_token: token.clone(),
                };
                async move { stub.refresh_token(req).await.map(|r| r.into_inner()) }
            })
            .await
    }

    /// Bootstrap：用一次性 bootstrap token 换短期 agent-bootstrap CCT。
    pub async fn bootstrap(&self, bootstrap_token: &str) -> Result<BootstrapResponse> {
        let token = bootstrap_token.to_string();
        self.client
            .execute_write_with_retry(move |channel| {
                let mut stub = AuthStub::new(channel);
                let req = BootstrapRequest {
                    bootstrap_token: token.clone(),
                };
                async move { stub.bootstrap(req).await.map(|r| r.into_inner()) }
            })
            .await
    }

    /// 查询鉴权是否已启用。
    pub async fn status(&self) -> Result<bool> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = AuthStub::new(channel.clone());
        let result = stub
            .auth_status(AuthStatusRequest {})
            .await
            .map(|r| r.into_inner().enabled)
            .map_err(from_status);
        self.client.return_channel(&endpoint, channel);
        result
    }

    // ──── 动态 bootstrap 令牌（TTL + 一次性） ────

    /// 签发 bootstrap 令牌（明文仅在本次响应中返回一次）。
    pub async fn bootstrap_token_issue(
        &self,
        label: &str,
        ttl_secs: i64,
    ) -> Result<BootstrapTokenIssueResponse> {
        let label = label.to_string();
        self.client
            .execute_write_with_retry(move |channel| {
                let mut stub = AuthStub::new(channel);
                let req = BootstrapTokenIssueRequest {
                    label: label.clone(),
                    ttl_secs,
                };
                async move {
                    stub.bootstrap_token_issue(req)
                        .await
                        .map(|r| r.into_inner())
                }
            })
            .await
    }

    /// 列出 bootstrap 令牌（不含明文，仅元数据 + 是否已消费）。
    pub async fn bootstrap_token_list(&self) -> Result<Vec<BootstrapTokenInfo>> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = AuthStub::new(channel.clone());
        let result = stub
            .bootstrap_token_list(BootstrapTokenListRequest {})
            .await
            .map(|r| r.into_inner().tokens)
            .map_err(from_status);
        self.client.return_channel(&endpoint, channel);
        result
    }

    /// 撤销 bootstrap 令牌（幂等：返回是否真的撤销了存在的令牌）。
    pub async fn bootstrap_token_revoke(&self, id: &str) -> Result<bool> {
        let id = id.to_string();
        self.client
            .execute_write_with_retry(move |channel| {
                let mut stub = AuthStub::new(channel);
                let req = BootstrapTokenRevokeRequest { id: id.clone() };
                async move {
                    stub.bootstrap_token_revoke(req)
                        .await
                        .map(|r| r.into_inner().revoked)
                }
            })
            .await
    }

    // ──── 用户管理 ────

    /// 创建用户。
    pub async fn user_add(&self, name: &str, password: &str) -> Result<()> {
        let name = name.to_string();
        let password = password.to_string();
        self.client
            .execute_write_with_retry(move |channel| {
                let mut stub = AuthStub::new(channel);
                let req = UserAddRequest {
                    name: name.clone(),
                    password: password.clone(),
                };
                async move { stub.user_add(req).await.map(|_| ()) }
            })
            .await
    }

    /// 删除用户。
    pub async fn user_delete(&self, name: &str) -> Result<()> {
        let name = name.to_string();
        self.client
            .execute_write_with_retry(move |channel| {
                let mut stub = AuthStub::new(channel);
                let req = UserDeleteRequest { name: name.clone() };
                async move { stub.user_delete(req).await.map(|_| ()) }
            })
            .await
    }

    /// 修改用户密码。
    pub async fn user_change_password(&self, name: &str, password: &str) -> Result<()> {
        let name = name.to_string();
        let password = password.to_string();
        self.client
            .execute_write_with_retry(move |channel| {
                let mut stub = AuthStub::new(channel);
                let req = UserChangePasswordRequest {
                    name: name.clone(),
                    password: password.clone(),
                };
                async move { stub.user_change_password(req).await.map(|_| ()) }
            })
            .await
    }

    /// 列出全部用户。
    pub async fn user_list(&self) -> Result<Vec<User>> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = AuthStub::new(channel.clone());
        let result = stub
            .user_list(UserListRequest {})
            .await
            .map(|r| r.into_inner().users)
            .map_err(from_status);
        self.client.return_channel(&endpoint, channel);
        result
    }

    /// 查询用户角色。
    pub async fn user_get(&self, name: &str) -> Result<Vec<String>> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = AuthStub::new(channel.clone());
        let result = stub
            .user_get(UserGetRequest {
                name: name.to_string(),
            })
            .await
            .map(|r| r.into_inner().roles)
            .map_err(from_status);
        self.client.return_channel(&endpoint, channel);
        result
    }

    // ──── 角色管理 ────

    /// 创建角色。
    pub async fn role_add(&self, name: &str) -> Result<()> {
        let name = name.to_string();
        self.client
            .execute_write_with_retry(move |channel| {
                let mut stub = AuthStub::new(channel);
                let req = RoleAddRequest { name: name.clone() };
                async move { stub.role_add(req).await.map(|_| ()) }
            })
            .await
    }

    /// 删除角色。
    pub async fn role_delete(&self, name: &str) -> Result<()> {
        let name = name.to_string();
        self.client
            .execute_write_with_retry(move |channel| {
                let mut stub = AuthStub::new(channel);
                let req = RoleDeleteRequest { name: name.clone() };
                async move { stub.role_delete(req).await.map(|_| ()) }
            })
            .await
    }

    /// 列出全部角色（含能力授予）。
    pub async fn role_list(&self) -> Result<Vec<Role>> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = AuthStub::new(channel.clone());
        let result = stub
            .role_list(RoleListRequest {})
            .await
            .map(|r| r.into_inner().roles)
            .map_err(from_status);
        self.client.return_channel(&endpoint, channel);
        result
    }

    /// 给角色授予能力（capability_id + scope）；经 raft 持久化。
    pub async fn role_grant_capability(
        &self,
        role: &str,
        capability_id: &str,
        scope: &str,
    ) -> Result<()> {
        let role = role.to_string();
        let capability_id = capability_id.to_string();
        let scope = scope.to_string();
        self.client
            .execute_write_with_retry(move |channel| {
                let mut stub = AuthStub::new(channel);
                let req = RoleGrantCapabilityRequest {
                    role: role.clone(),
                    capability_id: capability_id.clone(),
                    scope: scope.clone(),
                };
                async move { stub.role_grant_capability(req).await.map(|_| ()) }
            })
            .await
    }

    /// 撤销角色的能力授予（精确匹配 capability_id + scope）。
    pub async fn role_revoke_capability(
        &self,
        role: &str,
        capability_id: &str,
        scope: &str,
    ) -> Result<()> {
        let role = role.to_string();
        let capability_id = capability_id.to_string();
        let scope = scope.to_string();
        self.client
            .execute_write_with_retry(move |channel| {
                let mut stub = AuthStub::new(channel);
                let req = RoleRevokeCapabilityRequest {
                    role: role.clone(),
                    capability_id: capability_id.clone(),
                    scope: scope.clone(),
                };
                async move { stub.role_revoke_capability(req).await.map(|_| ()) }
            })
            .await
    }

    // ──── 用户-角色关联 ────

    /// 给用户授予角色。
    pub async fn user_grant_role(&self, user: &str, role: &str) -> Result<()> {
        let user = user.to_string();
        let role = role.to_string();
        self.client
            .execute_write_with_retry(move |channel| {
                let mut stub = AuthStub::new(channel);
                let req = UserGrantRoleRequest {
                    user: user.clone(),
                    role: role.clone(),
                };
                async move { stub.user_grant_role(req).await.map(|_| ()) }
            })
            .await
    }

    /// 撤销用户角色。
    pub async fn user_revoke_role(&self, user: &str, role: &str) -> Result<()> {
        let user = user.to_string();
        let role = role.to_string();
        self.client
            .execute_write_with_retry(move |channel| {
                let mut stub = AuthStub::new(channel);
                let req = UserRevokeRoleRequest {
                    user: user.clone(),
                    role: role.clone(),
                };
                async move { stub.user_revoke_role(req).await.map(|_| ()) }
            })
            .await
    }

    // ──── agent 同步 ────

    /// Agent 角色同步：全量拉取角色→能力映射。
    pub async fn list_roles(&self) -> Result<ListRolesResponse> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = AuthStub::new(channel.clone());
        let result = stub
            .list_roles(ListRolesRequest {})
            .await
            .map(|r| r.into_inner())
            .map_err(from_status);
        self.client.return_channel(&endpoint, channel);
        result
    }

    /// Token 吊销增量同步。
    pub async fn get_revocation_delta(
        &self,
        since_version: i64,
    ) -> Result<GetRevocationDeltaResponse> {
        let (endpoint, channel) = self.client.get_leader_channel().await?;
        let mut stub = AuthStub::new(channel.clone());
        let result = stub
            .get_revocation_delta(GetRevocationDeltaRequest { since_version })
            .await
            .map(|r| r.into_inner())
            .map_err(from_status);
        self.client.return_channel(&endpoint, channel);
        result
    }

    /// 启用鉴权（需 admin:auth:enable）。
    pub async fn enable(&self) -> Result<()> {
        self.client
            .execute_write_with_retry(move |channel| {
                let mut stub = AuthStub::new(channel);
                async move { stub.auth_enable(AuthEnableRequest {}).await.map(|_| ()) }
            })
            .await
    }
}
