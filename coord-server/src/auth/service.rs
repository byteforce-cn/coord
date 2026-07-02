// Auth gRPC Service — implements auth.proto Auth service (ADP §14)
//
// Provides:
// - Auth enable/disable/status
// - User CRUD + password management
// - Role CRUD + permission management
// - User-role assignment
// - Authentication (password → token)

use std::sync::Arc;

use coord_proto::auth::auth_server::Auth as AuthTrait;
use coord_proto::auth::*;

use crate::auth::manager::AuthManager;
use crate::auth::token::TokenManager;

/// gRPC Auth service implementation
pub struct AuthService {
    auth_manager: Arc<AuthManager>,
    token_manager: Arc<TokenManager>,
}

impl AuthService {
    pub fn new(auth_manager: Arc<AuthManager>, token_manager: Arc<TokenManager>) -> Self {
        Self {
            auth_manager,
            token_manager,
        }
    }
}

// ──── Auth state management ────

#[tonic::async_trait]
impl AuthTrait for AuthService {
    async fn auth_enable(
        &self,
        _request: tonic::Request<AuthEnableRequest>,
    ) -> Result<tonic::Response<AuthEnableResponse>, tonic::Status> {
        self.auth_manager.enable();
        tracing::info!("Auth enabled");
        Ok(tonic::Response::new(AuthEnableResponse {}))
    }

    async fn auth_disable(
        &self,
        _request: tonic::Request<AuthDisableRequest>,
    ) -> Result<tonic::Response<AuthDisableResponse>, tonic::Status> {
        self.auth_manager.disable();
        tracing::info!("Auth disabled");
        Ok(tonic::Response::new(AuthDisableResponse {}))
    }

    async fn auth_status(
        &self,
        _request: tonic::Request<AuthStatusRequest>,
    ) -> Result<tonic::Response<AuthStatusResponse>, tonic::Status> {
        Ok(tonic::Response::new(AuthStatusResponse {
            enabled: self.auth_manager.is_enabled(),
        }))
    }

    // ──── User management ────

    async fn user_add(
        &self,
        request: tonic::Request<UserAddRequest>,
    ) -> Result<tonic::Response<UserAddResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .user_add(&req.name, &req.password)
            .map_err(|e| tonic::Status::already_exists(e.to_string()))?;
        tracing::info!("User added: {}", req.name);
        Ok(tonic::Response::new(UserAddResponse {}))
    }

    async fn user_delete(
        &self,
        request: tonic::Request<UserDeleteRequest>,
    ) -> Result<tonic::Response<UserDeleteResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .user_delete(&req.name)
            .map_err(|e| tonic::Status::not_found(e.to_string()))?;
        tracing::info!("User deleted: {}", req.name);
        Ok(tonic::Response::new(UserDeleteResponse {}))
    }

    async fn user_change_password(
        &self,
        request: tonic::Request<UserChangePasswordRequest>,
    ) -> Result<tonic::Response<UserChangePasswordResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .user_change_password(&req.name, &req.password)
            .map_err(|e| tonic::Status::not_found(e.to_string()))?;
        tracing::info!("Password changed for user: {}", req.name);
        Ok(tonic::Response::new(UserChangePasswordResponse {}))
    }

    async fn user_list(
        &self,
        _request: tonic::Request<UserListRequest>,
    ) -> Result<tonic::Response<UserListResponse>, tonic::Status> {
        let usernames = self.auth_manager.user_list();
        let users = usernames
            .into_iter()
            .map(|name| {
                let roles = self
                    .auth_manager
                    .user_get_roles(&name)
                    .unwrap_or_default();
                User {
                    name,
                    roles,
                }
            })
            .collect();
        Ok(tonic::Response::new(UserListResponse { users }))
    }

    async fn user_get(
        &self,
        request: tonic::Request<UserGetRequest>,
    ) -> Result<tonic::Response<UserGetResponse>, tonic::Status> {
        let req = request.into_inner();
        let roles = self
            .auth_manager
            .user_get_roles(&req.name)
            .map_err(|e| tonic::Status::not_found(e.to_string()))?;
        Ok(tonic::Response::new(UserGetResponse { roles }))
    }

    // ──── Role management ────

    async fn role_add(
        &self,
        request: tonic::Request<RoleAddRequest>,
    ) -> Result<tonic::Response<RoleAddResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .role_add(&req.name)
            .map_err(|e| tonic::Status::already_exists(e.to_string()))?;
        tracing::info!("Role added: {}", req.name);
        Ok(tonic::Response::new(RoleAddResponse {}))
    }

    async fn role_delete(
        &self,
        request: tonic::Request<RoleDeleteRequest>,
    ) -> Result<tonic::Response<RoleDeleteResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .role_delete(&req.name)
            .map_err(|e| tonic::Status::not_found(e.to_string()))?;
        tracing::info!("Role deleted: {}", req.name);
        Ok(tonic::Response::new(RoleDeleteResponse {}))
    }

    async fn role_grant_permission(
        &self,
        request: tonic::Request<RoleGrantPermissionRequest>,
    ) -> Result<tonic::Response<RoleGrantPermissionResponse>, tonic::Status> {
        let req = request.into_inner();
        let perm = req
            .permission
            .ok_or_else(|| tonic::Status::invalid_argument("missing permission"))?;

        let perm_type = match PermissionType::try_from(perm.r#type) {
            Ok(PermissionType::Read) => crate::auth::manager::PermissionType::Read,
            Ok(PermissionType::Write) => crate::auth::manager::PermissionType::Write,
            Ok(PermissionType::Readwrite) => crate::auth::manager::PermissionType::ReadWrite,
            Err(_) => return Err(tonic::Status::invalid_argument("invalid permission type")),
        };

        self.auth_manager
            .role_grant_permission(&req.name, perm_type, perm.key, perm.range_end)
            .map_err(|e| tonic::Status::internal(e.to_string()))?;
        Ok(tonic::Response::new(RoleGrantPermissionResponse {}))
    }

    async fn role_revoke_permission(
        &self,
        request: tonic::Request<RoleRevokePermissionRequest>,
    ) -> Result<tonic::Response<RoleRevokePermissionResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .role_revoke_permission(&req.name, &req.key, &req.range_end)
            .map_err(|e| tonic::Status::not_found(e.to_string()))?;
        Ok(tonic::Response::new(RoleRevokePermissionResponse {}))
    }

    async fn role_list(
        &self,
        _request: tonic::Request<RoleListRequest>,
    ) -> Result<tonic::Response<RoleListResponse>, tonic::Status> {
        let roles = self.auth_manager.role_list();
        let proto_roles = roles
            .into_iter()
            .map(|r| Role {
                name: r.name,
                permissions: r
                    .permissions
                    .into_iter()
                    .map(|p| Permission {
                        r#type: match p.perm_type {
                            crate::auth::manager::PermissionType::Read => PermissionType::Read as i32,
                            crate::auth::manager::PermissionType::Write => PermissionType::Write as i32,
                            crate::auth::manager::PermissionType::ReadWrite => PermissionType::Readwrite as i32,
                        },
                        key: p.key_prefix,
                        range_end: p.range_end,
                    })
                    .collect(),
            })
            .collect();
        Ok(tonic::Response::new(RoleListResponse { roles: proto_roles }))
    }

    // ──── User-Role assignment ────

    async fn user_grant_role(
        &self,
        request: tonic::Request<UserGrantRoleRequest>,
    ) -> Result<tonic::Response<UserGrantRoleResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .user_grant_role(&req.user, &req.role)
            .map_err(|e| tonic::Status::not_found(e.to_string()))?;
        tracing::info!("Role '{}' granted to user '{}'", req.role, req.user);
        Ok(tonic::Response::new(UserGrantRoleResponse {}))
    }

    async fn user_revoke_role(
        &self,
        request: tonic::Request<UserRevokeRoleRequest>,
    ) -> Result<tonic::Response<UserRevokeRoleResponse>, tonic::Status> {
        let req = request.into_inner();
        self.auth_manager
            .user_revoke_role(&req.user, &req.role)
            .map_err(|e| tonic::Status::not_found(e.to_string()))?;
        tracing::info!("Role '{}' revoked from user '{}'", req.role, req.user);
        Ok(tonic::Response::new(UserRevokeRoleResponse {}))
    }

    // ──── Authentication ────

    async fn authenticate(
        &self,
        request: tonic::Request<AuthenticateRequest>,
    ) -> Result<tonic::Response<AuthenticateResponse>, tonic::Status> {
        let req = request.into_inner();

        // Verify password
        self.auth_manager
            .authenticate(&req.name, &req.password)
            .map_err(|e| tonic::Status::unauthenticated(e.to_string()))?;

        // Issue token
        let auth_token = self.token_manager.issue_token(&req.name);
        tracing::info!("User '{}' authenticated, token issued", req.name);

        Ok(tonic::Response::new(AuthenticateResponse {
            token: auth_token.token,
        }))
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_auth_service_new() {
        let auth_mgr = Arc::new(crate::auth::manager::AuthManager::new());
        let token_mgr = Arc::new(crate::auth::token::TokenManager::with_defaults());
        let _service = AuthService::new(auth_mgr, token_mgr);
    }

    #[test]
    fn test_auth_service_new_with_custom_token_manager() {
        let auth_mgr = Arc::new(crate::auth::manager::AuthManager::new());
        // 0-second TTL for testing immediate expiry
        let token_mgr = Arc::new(crate::auth::token::TokenManager::new(0, 0));
        let _service = AuthService::new(auth_mgr, token_mgr);
    }
}
