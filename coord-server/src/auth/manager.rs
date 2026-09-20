// Auth Manager — User/Role/Permission management with RBAC
//
// Manages:
// - Users (name, password hash)
// - Roles (name, permissions)
// - User-Role assignments
// - Auth enable/disable state
//
// 密码哈希：新密码一律 Argon2id（PHC 字符串）；遗留 SHA256 哈希首次
// 登录成功时透明迁移。
// Permissions control Read/Write/ReadWrite on Key prefix ranges.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use argon2::password_hash::rand_core::OsRng;
use argon2::password_hash::{PasswordHash as Argon2Hash, SaltString};
use argon2::{Argon2, PasswordHasher, PasswordVerifier};
use parking_lot::RwLock;
use sha2::{Digest, Sha256};

use coord_core::auth::trie::{prefix_successor, scope_covers_interval, ScopeTrie};
use coord_core::error::{Error, Result};

// ──── Permission ────

/// Permission type for key access
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PermissionType {
    Read,
    Write,
    ReadWrite,
}

/// A permission entry: type + key range
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Permission {
    pub perm_type: PermissionType,
    /// Key prefix this permission applies to
    pub key_prefix: Vec<u8>,
    /// Range end (empty = exact prefix match)
    pub range_end: Vec<u8>,
}

impl Permission {
    /// Check if this permission allows reading a specific key
    pub fn allows_read(&self, key: &[u8]) -> bool {
        if self.perm_type == PermissionType::Write {
            return false;
        }
        self.key_matches(key)
    }

    /// Check if this permission allows writing a specific key
    pub fn allows_write(&self, key: &[u8]) -> bool {
        if self.perm_type == PermissionType::Read {
            return false;
        }
        self.key_matches(key)
    }

    /// Check if the given key falls within this permission's range
    fn key_matches(&self, key: &[u8]) -> bool {
        if !key.starts_with(&self.key_prefix) {
            return false;
        }
        if self.range_end.is_empty() {
            // Exact prefix match only — key must start with key_prefix
            return true;
        }
        // Range match: key_prefix <= key < range_end
        key >= &self.key_prefix[..] && key < &self.range_end[..]
    }

    /// 本权限是否覆盖整个区间 `[key, range_end)`（A1：区间必须整体包含）。
    pub fn allows_read_range(&self, key: &[u8], range_end: &[u8]) -> bool {
        if self.perm_type == PermissionType::Write {
            return false;
        }
        self.range_matches(key, range_end)
    }

    /// 本权限是否覆盖整个区间 `[key, range_end)`（写语义）。
    pub fn allows_write_range(&self, key: &[u8], range_end: &[u8]) -> bool {
        if self.perm_type == PermissionType::Read {
            return false;
        }
        self.range_matches(key, range_end)
    }

    /// 范围包含判定：整个 `[key, range_end)` 必须落在本权限允许的 key 区间内。
    ///
    /// 与 A1 的 scope 判定保持**同一套字节区间语义**（`key >= P && range_end <= U`），
    /// 避免"遗留权限路径放行、scope 路径拒绝"这类两条授权路径行为不一致的陷阱。
    fn range_matches(&self, key: &[u8], range_end: &[u8]) -> bool {
        if range_end.is_empty() {
            return self.key_matches(key);
        }
        // 空前缀 + 空 range_end = 覆盖全部 key（与 key_matches 一致）→ 无上界，
        // 任意区间（含 range_end = "\0"）都被覆盖。
        if self.key_prefix.is_empty() && self.range_end.is_empty() {
            return true;
        }
        if range_end == b"\0" {
            return false; // 无上界，有界权限不能覆盖
        }
        if key < self.key_prefix.as_slice() {
            return false;
        }
        let upper: Vec<u8> = if self.range_end.is_empty() {
            match prefix_successor(&self.key_prefix) {
                Some(u) => u,
                None => return false, // 前缀全 0xFF → 无上界，fail-closed
            }
        } else {
            self.range_end.clone()
        };
        range_end <= upper.as_slice()
    }
}

// ──── Role ────

/// A capability grant assigned to a role.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityGrant {
    /// Full capability ID: "data:kv:read"
    pub capability_id: String,
    /// Scope restriction (empty = no restriction)
    pub scope: String,
}

#[derive(Debug, Clone)]
pub struct Role {
    pub name: String,
    /// Legacy key-prefix permissions (deprecated, kept for backward compat)
    pub permissions: Vec<Permission>,
    /// Capability-based grants
    pub capability_grants: Vec<CapabilityGrant>,
    /// Whether this role is high-sensitivity (forces server lookup every request)
    pub high_sensitive: bool,
}
impl Role {
    /// 转换为持久化记录。
    pub fn to_record(&self) -> AuthRoleRecord {
        AuthRoleRecord {
            name: self.name.clone(),
            permissions: self
                .permissions
                .iter()
                .map(|p| AuthPermissionRecord {
                    perm_type: match p.perm_type {
                        PermissionType::Read => 0,
                        PermissionType::Write => 1,
                        PermissionType::ReadWrite => 2,
                    },
                    key_prefix: p.key_prefix.clone(),
                    range_end: p.range_end.clone(),
                })
                .collect(),
            capability_grants: self
                .capability_grants
                .iter()
                .map(|g| AuthGrantRecord {
                    capability_id: g.capability_id.clone(),
                    scope: g.scope.clone(),
                })
                .collect(),
            high_sensitive: self.high_sensitive,
        }
    }

    /// 从持久化记录重建。
    pub fn from_record(rec: AuthRoleRecord) -> Self {
        Self {
            name: rec.name,
            permissions: rec
                .permissions
                .into_iter()
                .map(|p| Permission {
                    perm_type: match p.perm_type {
                        0 => PermissionType::Read,
                        1 => PermissionType::Write,
                        _ => PermissionType::ReadWrite,
                    },
                    key_prefix: p.key_prefix,
                    range_end: p.range_end,
                })
                .collect(),
            capability_grants: rec
                .capability_grants
                .into_iter()
                .map(|g| CapabilityGrant {
                    capability_id: g.capability_id,
                    scope: g.scope,
                })
                .collect(),
            high_sensitive: rec.high_sensitive,
        }
    }
}
// ──── User ────

#[derive(Debug, Clone)]
struct UserEntry {
    /// 密码哈希：Argon2id PHC 字符串（前缀 `$argon2id$`）或遗留 SHA256 摘要
    password_hash: Vec<u8>,
    /// Assigned role names
    roles: HashSet<String>,
}

// ──── Auth Manager ────

/// Manages users, roles, permissions, and auth state
pub struct AuthManager {
    /// Whether auth is enabled
    enabled: Arc<RwLock<bool>>,
    /// Users: name → UserEntry
    users: Arc<RwLock<HashMap<String, UserEntry>>>,
    /// Roles: name → Role
    roles: Arc<RwLock<HashMap<String, Role>>>,
    /// 动态 bootstrap 令牌：id → 记录（明文只存 SHA256；一次性语义）
    bootstrap_tokens: Arc<RwLock<HashMap<String, AuthBootstrapTokenRecord>>>,
}

/// 引导管理员角色名：该角色在**服务端与 agent 两侧**能力判定中全能力放行。
///
/// 定义已上移到 [`coord_core::auth::ROOT_ROLE`]（F-32：agent 侧此前没有同口径旁路，
/// 导致 auth 开启后 root 经 agent 的调用被全部拒绝）。这里保留同名再导出，
/// 既保证两侧用的是**同一个字符串常量**，又不破坏 `coord_server::auth::ROOT_ROLE`
/// 既有引用方。
pub use coord_core::auth::ROOT_ROLE;

// ──── 持久化记录（`/_sys/auth/` 前缀，bincode 序列化）────

/// 用户条目存储前缀 `/_sys/auth/user/{name}`
pub const AUTH_USER_PREFIX: &[u8] = b"/_sys/auth/user/";
/// 角色条目存储前缀 `/_sys/auth/role/{role}`
pub const AUTH_ROLE_PREFIX: &[u8] = b"/_sys/auth/role/";
/// 吊销登记存储前缀 `/_sys/auth/revoked/{jti}`
pub const AUTH_REVOKED_PREFIX: &[u8] = b"/_sys/auth/revoked/";
/// 会话落盘存储前缀 `/_sys/auth/sessions/{hash_hex}`
pub const AUTH_SESSION_PREFIX: &[u8] = b"/_sys/auth/sessions/";
/// 动态 bootstrap 令牌存储前缀 `/_sys/auth/bootstrap/{id}`
pub const AUTH_BOOTSTRAP_PREFIX: &[u8] = b"/_sys/auth/bootstrap/";

/// 动态 bootstrap 令牌的持久化记录（明文不落盘，仅 SHA256 hex）。
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AuthBootstrapTokenRecord {
    pub id: String,
    /// 令牌的 SHA256 hex
    pub hash_hex: String,
    pub label: String,
    pub created_by: String,
    pub created_at_unix: u64,
    pub expires_at_unix: u64,
    /// 一次性语义：Some = 已使用（保留记录供审计）
    pub consumed_at_unix: Option<u64>,
}

impl AuthBootstrapTokenRecord {
    /// 是否已被使用。
    pub fn is_consumed(&self) -> bool {
        self.consumed_at_unix.is_some()
    }

    /// 是否已过期（`now` 为 Unix 秒）。
    pub fn is_expired(&self, now_unix: u64) -> bool {
        now_unix >= self.expires_at_unix
    }

    /// 是否可用于换取 bootstrap CCT（未使用且未过期）。
    pub fn is_usable(&self, now_unix: u64) -> bool {
        !self.is_consumed() && !self.is_expired(now_unix)
    }

    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::Internal(format!("serialize bootstrap: {e}")))
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }
}

/// 持久化的会话条目（token 明文不入盘，仅存 SHA256 hex 为键）
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AuthSessionRecord {
    pub username: String,
    pub expires_at_unix: u64,
    pub is_refresh: bool,
}

impl AuthSessionRecord {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::Internal(format!("serialize session: {e}")))
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }
}

/// 持久化的用户条目
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AuthUserRecord {
    pub name: String,
    pub password_hash: Vec<u8>,
    pub roles: Vec<String>,
}

impl AuthUserRecord {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::Internal(format!("encode auth user: {e}")))
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }
}

/// 持久化的权限条目
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AuthPermissionRecord {
    /// 0=Read, 1=Write, 2=ReadWrite（与 `PermissionType` 对应）
    pub perm_type: u8,
    pub key_prefix: Vec<u8>,
    pub range_end: Vec<u8>,
}

/// 持久化的能力授权条目
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AuthGrantRecord {
    pub capability_id: String,
    pub scope: String,
}

/// 持久化的角色条目
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AuthRoleRecord {
    pub name: String,
    pub permissions: Vec<AuthPermissionRecord>,
    pub capability_grants: Vec<AuthGrantRecord>,
    pub high_sensitive: bool,
}

impl AuthRoleRecord {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::Internal(format!("encode auth role: {e}")))
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }
}

/// 持久化的吊销登记值（`/_sys/auth/revoked/{jti}`）
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct AuthRevocationRecord {
    pub jti: String,
    pub revoked_at: i64,
}

impl AuthRevocationRecord {
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        bincode::serialize(self).map_err(|e| Error::Internal(format!("encode revocation: {e}")))
    }

    pub fn from_bytes(bytes: &[u8]) -> Option<Self> {
        bincode::deserialize(bytes).ok()
    }
}

impl Default for AuthManager {
    fn default() -> Self {
        Self::new()
    }
}

impl AuthManager {
    /// Create a new AuthManager (auth disabled by default).
    ///
    /// 创建 root/root 默认用户（**仅限 dev 模式**；server 模式必须用
    /// [`AuthManager::new_empty`] + 强制 root 密码）。
    pub fn new() -> Self {
        let manager = Self::new_empty();

        // Default root user (dev mode only)
        let root_entry = UserEntry {
            password_hash: match hash_password_argon2id("root") {
                Ok(hash) => hash,
                Err(e) => {
                    tracing::error!("dev default root password hashing failed: {e}");
                    Vec::new()
                }
            },
            roles: {
                let mut set = HashSet::new();
                set.insert(ROOT_ROLE.to_string());
                set
            },
        };
        manager
            .users
            .write()
            .insert(ROOT_ROLE.to_string(), root_entry);

        manager
    }

    /// Create an empty AuthManager（server 模式）：
    /// 保留 root 角色（引导管理员），**不**创建 root/root 用户。
    pub fn new_empty() -> Self {
        let manager = Self {
            enabled: Arc::new(RwLock::new(false)),
            users: Arc::new(RwLock::new(HashMap::new())),
            roles: Arc::new(RwLock::new(HashMap::new())),
            bootstrap_tokens: Arc::new(RwLock::new(HashMap::new())),
        };

        // root role: ReadWrite on all keys, all capabilities, high_sensitive
        let root_role = Role {
            name: ROOT_ROLE.to_string(),
            permissions: vec![Permission {
                perm_type: PermissionType::ReadWrite,
                key_prefix: vec![],
                range_end: vec![],
            }],
            capability_grants: vec![],
            high_sensitive: true,
        };
        manager
            .roles
            .write()
            .insert(ROOT_ROLE.to_string(), root_role);

        manager
    }

    // ──── Auth state ────

    /// Check if auth is enabled
    pub fn is_enabled(&self) -> bool {
        *self.enabled.read()
    }

    /// Enable auth
    pub fn enable(&self) {
        *self.enabled.write() = true;
    }

    /// Disable auth
    pub fn disable(&self) {
        *self.enabled.write() = false;
    }

    // ──── User management ────

    /// Add a new user
    pub fn user_add(&self, name: &str, password: &str) -> Result<()> {
        let mut users = self.users.write();
        if users.contains_key(name) {
            return Err(Error::UserAlreadyExists {
                name: name.to_string(),
            });
        }
        users.insert(
            name.to_string(),
            UserEntry {
                password_hash: hash_password_argon2id(password).map_err(Error::Internal)?,
                roles: HashSet::new(),
            },
        );
        Ok(())
    }

    /// Delete a user
    pub fn user_delete(&self, name: &str) -> Result<()> {
        let mut users = self.users.write();
        if users.remove(name).is_none() {
            return Err(Error::NotFound {
                resource: "user",
                key: name.to_string(),
            });
        }
        Ok(())
    }

    /// Change a user's password
    pub fn user_change_password(&self, name: &str, new_password: &str) -> Result<()> {
        let mut users = self.users.write();
        let entry = users.get_mut(name).ok_or_else(|| Error::NotFound {
            resource: "user",
            key: name.to_string(),
        })?;
        entry.password_hash = hash_password_argon2id(new_password).map_err(Error::Internal)?;
        Ok(())
    }

    /// List all users
    pub fn user_list(&self) -> Vec<String> {
        self.users.read().keys().cloned().collect()
    }

    /// Get roles for a user
    pub fn user_get_roles(&self, name: &str) -> Result<Vec<String>> {
        let users = self.users.read();
        let entry = users.get(name).ok_or_else(|| Error::NotFound {
            resource: "user",
            key: name.to_string(),
        })?;
        Ok(entry.roles.iter().cloned().collect())
    }

    /// Authenticate a user with password.
    ///
    /// 遗留 SHA256 哈希验证成功后透明升级为 Argon2id。
    pub fn authenticate(&self, name: &str, password: &str) -> Result<()> {
        // 先读校验
        let (stored, is_legacy, valid) = {
            let users = self.users.read();
            let entry = users
                .get(name)
                .ok_or_else(|| Error::Unauthenticated("invalid credentials".to_string()))?;
            let hash = entry.password_hash.clone();
            let (legacy, ok) = verify_password(&hash, password);
            (hash, legacy, ok)
        };
        if !valid {
            return Err(Error::Unauthenticated("invalid credentials".to_string()));
        }
        // 透明迁移：遗留 SHA256 → Argon2id
        if is_legacy {
            let mut users = self.users.write();
            if let Some(entry) = users.get_mut(name) {
                if entry.password_hash == stored {
                    entry.password_hash =
                        hash_password_argon2id(password).map_err(Error::Internal)?;
                    tracing::info!("user '{name}' password transparently migrated to Argon2id");
                }
            }
        }
        Ok(())
    }

    // ──── 服务端能力判定（删除"有任意 role 即放行"兜底）────

    /// 按用户角色判定服务端能力（第二道防线，取代 `!roles.is_empty()` 兜底）。
    ///
    /// 规则：
    /// 1. `root` 角色为引导管理员，全能力放行；
    /// 2. 角色的 `capability_grants` 精确匹配 `capability_id`：
    ///    - grant.scope 为空 → 能力级放行；
    ///    - grant.scope 非空 → 须 `scope_key` 存在且 ScopeTrie 命中（fail-closed）；
    /// 3. 遗留 `Permission`（key 前缀）映射到数据面能力
    ///    （`data:kv:read`/`data:kv:write`/`data:kv:delete`/`data:txn:execute`），
    ///    同样要求 `scope_key` 存在且命中前缀。
    ///    无任何匹配 → 拒绝。
    pub fn check_capability(
        &self,
        roles: &[String],
        capability_id: &str,
        scope_key: Option<&str>,
    ) -> bool {
        let roles_map = self.roles.read();

        // 引导管理员全能力放行（F-32 之前这只存在于服务端）。
        // 提到循环外："root ⇒ 放行一切"是整条判定的前置条件，
        // 不该表现为"恰好第一个 role 是 root"的循环副作用。
        if coord_core::auth::is_root(roles) {
            return true;
        }

        for role_name in roles {
            let Some(role) = roles_map.get(role_name) else {
                continue;
            };

            // 2. capability grants
            for grant in &role.capability_grants {
                if grant.capability_id != capability_id {
                    continue;
                }
                if grant.scope.is_empty() {
                    return true;
                }
                if let Some(key) = scope_key {
                    let mut trie = ScopeTrie::new();
                    if trie.insert(&grant.scope).is_ok() && trie.matches(key) {
                        return true;
                    }
                }
                // scope 非空但 scope_key 缺失 → 无法验证，fail-closed
            }

            // 3. 遗留 key 前缀权限 → 数据面能力映射
            let key = scope_key.unwrap_or_default().as_bytes();
            let legacy_ok = match capability_id {
                "data:kv:read" => role.permissions.iter().any(|p| p.allows_read(key)),
                "data:kv:write" | "data:kv:delete" => {
                    role.permissions.iter().any(|p| p.allows_write(key))
                }
                "data:txn:execute" => role.permissions.iter().any(|p| p.allows_write(key)),
                _ => false,
            };
            if legacy_ok {
                return true;
            }
        }

        false
    }

    /// 区间版能力判定（A1）：整个 `[key, range_end)` 必须被授权覆盖。
    ///
    /// 与 [`Self::check_capability`] 规则一致，但 scope 判定为**区间包含**而非
    /// 单 key 命中：持 `/app/a/` 者不可用 `Range(key=/app/a/x, range_end=/zzz)` 读全库。
    pub fn check_capability_range(
        &self,
        roles: &[String],
        capability_id: &str,
        key: &[u8],
        range_end: &[u8],
    ) -> bool {
        let roles_map = self.roles.read();

        // 引导管理员全能力放行（与 `check_capability` 同口径）。
        if coord_core::auth::is_root(roles) {
            return true;
        }

        for role_name in roles {
            let Some(role) = roles_map.get(role_name) else {
                continue;
            };

            for grant in &role.capability_grants {
                if grant.capability_id != capability_id {
                    continue;
                }
                if grant.scope.is_empty() {
                    return true;
                }
                if scope_covers_interval(&grant.scope, key, range_end) {
                    return true;
                }
            }

            let legacy_ok = match capability_id {
                "data:kv:read" => role
                    .permissions
                    .iter()
                    .any(|p| p.allows_read_range(key, range_end)),
                "data:kv:write" | "data:kv:delete" | "data:txn:execute" => role
                    .permissions
                    .iter()
                    .any(|p| p.allows_write_range(key, range_end)),
                _ => false,
            };
            if legacy_ok {
                return true;
            }
        }

        false
    }

    // ──── apply 派生视图更新与启动装载 ────

    /// apply 后同步内存缓存视图（`AuthManager` 不再是权威数据源）。
    pub fn apply_auth_op_to_view(&self, op: &crate::raft::type_config::AuthOp) {
        use crate::raft::type_config::AuthOp;
        match op {
            AuthOp::UserAdd { name, hash, roles } => {
                self.users.write().insert(
                    name.clone(),
                    UserEntry {
                        password_hash: hash.as_bytes().to_vec(),
                        roles: roles.iter().cloned().collect(),
                    },
                );
            }
            AuthOp::UserDelete { name } => {
                self.users.write().remove(name);
            }
            AuthOp::UserSetPassword { name, hash } => {
                if let Some(entry) = self.users.write().get_mut(name) {
                    entry.password_hash = hash.as_bytes().to_vec();
                }
            }
            AuthOp::UserGrantRole { name, role } => {
                if let Some(entry) = self.users.write().get_mut(name) {
                    entry.roles.insert(role.clone());
                }
            }
            AuthOp::UserRevokeRole { name, role } => {
                if let Some(entry) = self.users.write().get_mut(name) {
                    entry.roles.remove(role);
                }
            }
            AuthOp::RoleAdd { role } => {
                self.roles
                    .write()
                    .entry(role.clone())
                    .or_insert_with(|| Role {
                        name: role.clone(),
                        permissions: Vec::new(),
                        capability_grants: Vec::new(),
                        high_sensitive: false,
                    });
            }
            AuthOp::RoleDelete { role } => {
                self.roles.write().remove(role);
                for entry in self.users.write().values_mut() {
                    entry.roles.remove(role);
                }
            }
            AuthOp::RoleGrantPermission {
                role,
                perm_type,
                key,
                range_end,
            } => {
                if let Some(r) = self.roles.write().get_mut(role) {
                    let pt = match perm_type {
                        0 => PermissionType::Read,
                        1 => PermissionType::Write,
                        _ => PermissionType::ReadWrite,
                    };
                    let is_dup = r.permissions.iter().any(|p| {
                        p.perm_type == pt && p.key_prefix == *key && p.range_end == *range_end
                    });
                    if !is_dup {
                        r.permissions.push(Permission {
                            perm_type: pt,
                            key_prefix: key.clone(),
                            range_end: range_end.clone(),
                        });
                    }
                }
            }
            AuthOp::RoleRevokePermission {
                role,
                key,
                range_end,
            } => {
                if let Some(r) = self.roles.write().get_mut(role) {
                    r.permissions
                        .retain(|p| !(p.key_prefix == *key && p.range_end == *range_end));
                }
            }
            AuthOp::RevokeJti { .. } => {
                // 吊销登记由 RevocationStore 处理（state_machine apply 钩子）
            }
            AuthOp::IssueSession { .. }
            | AuthOp::ConsumeSession { .. }
            | AuthOp::ConsumeSessions { .. } => {
                // 会话表由 TokenManager 视图处理（state_machine apply 钩子）
            }
            AuthOp::RoleGrantCapability {
                role,
                capability_id,
                scope,
            } => {
                if let Some(r) = self.roles.write().get_mut(role) {
                    let is_dup = r
                        .capability_grants
                        .iter()
                        .any(|g| g.capability_id == *capability_id && g.scope == *scope);
                    if !is_dup {
                        r.capability_grants.push(CapabilityGrant {
                            capability_id: capability_id.clone(),
                            scope: scope.clone(),
                        });
                    }
                }
            }
            AuthOp::RoleRevokeCapability {
                role,
                capability_id,
                scope,
            } => {
                if let Some(r) = self.roles.write().get_mut(role) {
                    r.capability_grants
                        .retain(|g| !(g.capability_id == *capability_id && g.scope == *scope));
                }
            }
            AuthOp::IssueBootstrapToken {
                id,
                hash_hex,
                label,
                created_by,
                created_at_unix,
                expires_at_unix,
            } => {
                self.bootstrap_tokens.write().insert(
                    id.clone(),
                    AuthBootstrapTokenRecord {
                        id: id.clone(),
                        hash_hex: hash_hex.clone(),
                        label: label.clone(),
                        created_by: created_by.clone(),
                        created_at_unix: *created_at_unix,
                        expires_at_unix: *expires_at_unix,
                        consumed_at_unix: None,
                    },
                );
            }
            AuthOp::ConsumeBootstrapToken {
                id,
                consumed_at_unix,
            } => {
                if let Some(rec) = self.bootstrap_tokens.write().get_mut(id) {
                    rec.consumed_at_unix = Some(*consumed_at_unix);
                }
            }
            AuthOp::RevokeBootstrapToken { id } => {
                self.bootstrap_tokens.write().remove(id);
            }
        }
    }

    // ──── 动态 bootstrap 令牌 ────

    /// 按明文令牌的 SHA256 hex 查找记录（明文不落盘，只能按哈希索引）。
    pub fn bootstrap_token_by_hash(&self, hash_hex: &str) -> Option<AuthBootstrapTokenRecord> {
        self.bootstrap_tokens
            .read()
            .values()
            .find(|r| r.hash_hex == hash_hex)
            .cloned()
    }

    /// 按 ID 查找记录。
    pub fn bootstrap_token(&self, id: &str) -> Option<AuthBootstrapTokenRecord> {
        self.bootstrap_tokens.read().get(id).cloned()
    }

    /// 全部记录（按创建时间/ID 排序，供 List）。
    pub fn bootstrap_tokens(&self) -> Vec<AuthBootstrapTokenRecord> {
        let mut out: Vec<_> = self.bootstrap_tokens.read().values().cloned().collect();
        out.sort_by(|a, b| {
            a.created_at_unix
                .cmp(&b.created_at_unix)
                .then_with(|| a.id.cmp(&b.id))
        });
        out
    }

    /// **原子**消费：在写锁内校验（未使用 + 未过期）并标记 `consumed_at_unix`。
    ///
    /// 返回 `Some(record)` 表示本次调用抢到了该令牌（可继续签发 CCT）；
    /// `None` = 不存在 / 已消费 / 已过期。并发调用只有一个能拿到 `Some`。
    pub fn consume_bootstrap_token_atomic(
        &self,
        hash_hex: &str,
        now_unix: u64,
    ) -> Option<AuthBootstrapTokenRecord> {
        let mut tokens = self.bootstrap_tokens.write();
        let id = tokens
            .iter()
            .find(|(_, r)| r.hash_hex == hash_hex && r.is_usable(now_unix))
            .map(|(id, _)| id.clone())?;
        let rec = tokens.get_mut(&id)?;
        rec.consumed_at_unix = Some(now_unix);
        Some(rec.clone())
    }

    /// 回滚一次消费（raft 提案失败等**持久化未达成**路径）。
    ///
    /// 只在 `consumed_at_unix == expected` 时清除，避免误回滚已被
    /// 其他路径消费的记录。
    pub fn revert_bootstrap_token_consumption(&self, id: &str, expected: u64) -> bool {
        if let Some(rec) = self.bootstrap_tokens.write().get_mut(id) {
            if rec.consumed_at_unix == Some(expected) {
                rec.consumed_at_unix = None;
                return true;
            }
        }
        false
    }

    /// 移除过期且未被消费的令牌（启动/定期清理用）；返回清理数量。
    pub fn prune_expired_bootstrap_tokens(&self, now_unix: u64) -> usize {
        let mut tokens = self.bootstrap_tokens.write();
        let before = tokens.len();
        tokens.retain(|_, r| !(r.is_expired(now_unix) && !r.is_consumed()));
        before - tokens.len()
    }

    /// 启动装载：从 `/_sys/auth/` 前缀的原始条目重建内存视图。
    ///
    /// 保留 root 角色（引导管理员）；吊销条目由调用方交给 RevocationStore。
    /// 无任何持久化条目（首次启动）时保持现有内存状态（含刚创建的 root）。
    pub fn load_from_entries(&self, entries: Vec<(Vec<u8>, Vec<u8>)>) {
        if entries.is_empty() {
            // 首次启动：无持久化鉴权状态，保留内存中刚创建的 root
            return;
        }
        let root_role = self.roles.read().get(ROOT_ROLE).cloned();
        {
            let mut users = self.users.write();
            let mut roles = self.roles.write();
            let mut bootstrap_tokens = self.bootstrap_tokens.write();
            users.clear();
            roles.clear();
            bootstrap_tokens.clear();
            if let Some(root) = root_role {
                roles.insert(ROOT_ROLE.to_string(), root);
            }
            for (key, value) in entries {
                if key.starts_with(AUTH_USER_PREFIX) {
                    if let Some(rec) = AuthUserRecord::from_bytes(&value) {
                        users.insert(
                            rec.name.clone(),
                            UserEntry {
                                password_hash: rec.password_hash,
                                roles: rec.roles.into_iter().collect(),
                            },
                        );
                    }
                } else if key.starts_with(AUTH_ROLE_PREFIX) {
                    if let Some(rec) = AuthRoleRecord::from_bytes(&value) {
                        roles.insert(rec.name.clone(), Role::from_record(rec));
                    }
                } else if key.starts_with(AUTH_BOOTSTRAP_PREFIX) {
                    if let Some(rec) = AuthBootstrapTokenRecord::from_bytes(&value) {
                        bootstrap_tokens.insert(rec.id.clone(), rec);
                    }
                }
                // 其它前缀（如 revoked）由调用方处理
            }
        }
    }

    // ──── Role management ────

    /// Add a new role
    pub fn role_add(&self, name: &str) -> Result<()> {
        let mut roles = self.roles.write();
        if roles.contains_key(name) {
            return Err(Error::RoleAlreadyExists {
                name: name.to_string(),
            });
        }
        roles.insert(
            name.to_string(),
            Role {
                name: name.to_string(),
                permissions: Vec::new(),
                capability_grants: Vec::new(),
                high_sensitive: false,
            },
        );
        Ok(())
    }

    /// Delete a role
    pub fn role_delete(&self, name: &str) -> Result<()> {
        let mut roles = self.roles.write();
        if roles.remove(name).is_none() {
            return Err(Error::NotFound {
                resource: "role",
                key: name.to_string(),
            });
        }
        // Remove this role from all users
        let mut users = self.users.write();
        for entry in users.values_mut() {
            entry.roles.remove(name);
        }
        Ok(())
    }

    /// Grant a permission to a role
    pub fn role_grant_permission(
        &self,
        role_name: &str,
        perm_type: PermissionType,
        key_prefix: Vec<u8>,
        range_end: Vec<u8>,
    ) -> Result<()> {
        let mut roles = self.roles.write();
        let role = roles.get_mut(role_name).ok_or_else(|| Error::NotFound {
            resource: "role",
            key: role_name.to_string(),
        })?;

        // Check for duplicate
        let is_dup = role.permissions.iter().any(|p| {
            p.perm_type == perm_type && p.key_prefix == key_prefix && p.range_end == range_end
        });
        if is_dup {
            return Err(Error::AlreadyExists {
                resource: "permission",
                key: format!("{}/{:?}", role_name, String::from_utf8_lossy(&key_prefix)),
            });
        }

        role.permissions.push(Permission {
            perm_type,
            key_prefix,
            range_end,
        });
        Ok(())
    }

    /// Revoke a permission from a role
    pub fn role_revoke_permission(
        &self,
        role_name: &str,
        key_prefix: &[u8],
        range_end: &[u8],
    ) -> Result<()> {
        let mut roles = self.roles.write();
        let role = roles.get_mut(role_name).ok_or_else(|| Error::NotFound {
            resource: "role",
            key: role_name.to_string(),
        })?;

        let before = role.permissions.len();
        role.permissions
            .retain(|p| !(p.key_prefix == key_prefix && p.range_end == range_end));

        if role.permissions.len() == before {
            return Err(Error::NotFound {
                resource: "permission",
                key: format!("{}/{:?}", role_name, String::from_utf8_lossy(key_prefix)),
            });
        }
        Ok(())
    }

    /// List all roles
    pub fn role_list(&self) -> Vec<Role> {
        self.roles.read().values().cloned().collect()
    }

    // ──── Capability Grant management ────

    /// Grant a capability to a role.
    pub fn role_grant_capability(
        &self,
        role_name: &str,
        capability_id: &str,
        scope: &str,
    ) -> Result<()> {
        let mut roles = self.roles.write();
        let role = roles.get_mut(role_name).ok_or_else(|| Error::NotFound {
            resource: "role",
            key: role_name.to_string(),
        })?;

        // Check for duplicate
        let is_dup = role
            .capability_grants
            .iter()
            .any(|g| g.capability_id == capability_id && g.scope == scope);
        if is_dup {
            return Err(Error::AlreadyExists {
                resource: "capability-grant",
                key: format!("{role_name}:{capability_id}"),
            });
        }

        role.capability_grants.push(CapabilityGrant {
            capability_id: capability_id.to_string(),
            scope: scope.to_string(),
        });
        Ok(())
    }

    /// Revoke a capability grant from a role.
    pub fn role_revoke_capability(
        &self,
        role_name: &str,
        capability_id: &str,
        scope: &str,
    ) -> Result<()> {
        let mut roles = self.roles.write();
        let role = roles.get_mut(role_name).ok_or_else(|| Error::NotFound {
            resource: "role",
            key: role_name.to_string(),
        })?;

        let before = role.capability_grants.len();
        role.capability_grants
            .retain(|g| !(g.capability_id == capability_id && g.scope == scope));

        if role.capability_grants.len() == before {
            return Err(Error::NotFound {
                resource: "capability-grant",
                key: format!("{role_name}:{capability_id}"),
            });
        }
        Ok(())
    }

    /// Set the high_sensitive flag on a role.
    pub fn role_set_high_sensitive(&self, role_name: &str, sensitive: bool) -> Result<()> {
        let mut roles = self.roles.write();
        let role = roles.get_mut(role_name).ok_or_else(|| Error::NotFound {
            resource: "role",
            key: role_name.to_string(),
        })?;
        role.high_sensitive = sensitive;
        Ok(())
    }

    /// Get capability grants for a role.
    pub fn role_get_capability_grants(&self, role_name: &str) -> Result<Vec<CapabilityGrant>> {
        let roles = self.roles.read();
        let role = roles.get(role_name).ok_or_else(|| Error::NotFound {
            resource: "role",
            key: role_name.to_string(),
        })?;
        Ok(role.capability_grants.clone())
    }

    // ──── User-Role assignment ────

    /// Grant a role to a user
    pub fn user_grant_role(&self, username: &str, role_name: &str) -> Result<()> {
        // Verify role exists
        {
            let roles = self.roles.read();
            if !roles.contains_key(role_name) {
                return Err(Error::NotFound {
                    resource: "role",
                    key: role_name.to_string(),
                });
            }
        }

        let mut users = self.users.write();
        let entry = users.get_mut(username).ok_or_else(|| Error::NotFound {
            resource: "user",
            key: username.to_string(),
        })?;
        entry.roles.insert(role_name.to_string());
        Ok(())
    }

    /// Revoke a role from a user
    pub fn user_revoke_role(&self, username: &str, role_name: &str) -> Result<()> {
        let mut users = self.users.write();
        let entry = users.get_mut(username).ok_or_else(|| Error::NotFound {
            resource: "user",
            key: username.to_string(),
        })?;

        if !entry.roles.remove(role_name) {
            return Err(Error::NotFound {
                resource: "role-assignment",
                key: format!("{}:{}", username, role_name),
            });
        }
        Ok(())
    }

    // ──── Authorization check ────

    /// Check if a user has permission to read a key
    pub fn authorize_read(&self, username: &str, key: &[u8]) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }
        self.check_permission(username, key, |p| p.allows_read(key))
    }

    /// Check if a user has permission to write a key
    pub fn authorize_write(&self, username: &str, key: &[u8]) -> Result<()> {
        if !self.is_enabled() {
            return Ok(());
        }
        self.check_permission(username, key, |p| p.allows_write(key))
    }

    /// Internal permission check
    fn check_permission<F>(&self, username: &str, key: &[u8], check: F) -> Result<()>
    where
        F: Fn(&Permission) -> bool,
    {
        let users = self.users.read();
        let entry = users.get(username).ok_or_else(|| Error::NotFound {
            resource: "user",
            key: username.to_string(),
        })?;

        let roles = self.roles.read();
        for role_name in &entry.roles {
            if let Some(role) = roles.get(role_name) {
                for perm in &role.permissions {
                    if check(perm) {
                        return Ok(());
                    }
                }
            }
        }

        Err(Error::PermissionDenied(format!(
            "user '{}' lacks permission for key '{}'",
            username,
            String::from_utf8_lossy(key)
        )))
    }
}

// ──── Password hashing（Argon2id + 遗留 SHA256 迁移）────

/// Argon2id PHC 字符串前缀（用于区分遗留 SHA256 摘要）
pub const ARGON2ID_PREFIX: &str = "$argon2id$";

/// Argon2id 密码哈希；pub 供 AuthService 构造 AuthOp 使用。
pub fn hash_password_argon2id(password: &str) -> std::result::Result<Vec<u8>, String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string().into_bytes())
        .map_err(|e| format!("argon2id hashing failed: {e}"))
}

/// 遗留 SHA256（无盐）哈希 —— 仅用于兼容存量数据。
fn hash_password_legacy(password: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hasher.finalize().to_vec()
}

/// 校验密码。返回 `(is_legacy, valid)`：
/// `is_legacy=true` 表示哈希为遗留 SHA256（校验通过后由调用方透明迁移）。
fn verify_password(stored: &[u8], password: &str) -> (bool, bool) {
    if stored.starts_with(ARGON2ID_PREFIX.as_bytes()) {
        let parsed = match std::str::from_utf8(stored)
            .ok()
            .and_then(|s| Argon2Hash::new(s).ok())
        {
            Some(phc) => phc,
            None => return (false, false),
        };
        return (
            false,
            Argon2::default()
                .verify_password(password.as_bytes(), &parsed)
                .is_ok(),
        );
    }
    // 遗留 SHA256
    let candidate = hash_password_legacy(password);
    (true, candidate == stored)
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::raft::type_config::AuthOp;

    /// 能力授予经 raft apply 派生视图：scope 命中放行、越界拒绝；撤销即失效。
    #[test]
    fn test_role_grant_capability_scope_via_apply() {
        use crate::raft::type_config::AuthOp;

        let manager = AuthManager::new_empty();
        manager.role_add("plugin-role").unwrap();
        manager.apply_auth_op_to_view(&AuthOp::RoleGrantCapability {
            role: "plugin-role".into(),
            capability_id: "data:kv:read".into(),
            scope: "/app/counter/".into(),
        });

        let roles = vec!["plugin-role".to_string()];
        assert!(manager.check_capability(&roles, "data:kv:read", Some("/app/counter/x")));
        // scope 越界（fail-closed）
        assert!(!manager.check_capability(&roles, "data:kv:read", Some("/other/x")));
        // scope 非空但无 scope_key → 无法验证，拒绝
        assert!(!manager.check_capability(&roles, "data:kv:read", None));
        // 未授予的能力
        assert!(!manager.check_capability(&roles, "data:kv:write", Some("/app/counter/x")));

        // 重复授予幂等
        manager.apply_auth_op_to_view(&AuthOp::RoleGrantCapability {
            role: "plugin-role".into(),
            capability_id: "data:kv:read".into(),
            scope: "/app/counter/".into(),
        });
        assert_eq!(
            manager
                .role_get_capability_grants("plugin-role")
                .unwrap()
                .len(),
            1
        );

        // 撤销后失效
        manager.apply_auth_op_to_view(&AuthOp::RoleRevokeCapability {
            role: "plugin-role".into(),
            capability_id: "data:kv:read".into(),
            scope: "/app/counter/".into(),
        });
        assert!(!manager.check_capability(&roles, "data:kv:read", Some("/app/counter/x")));
    }

    /// 空 scope = 能力级放行（不依赖 scope_key）。
    #[test]
    fn test_role_grant_capability_unscoped() {
        use crate::raft::type_config::AuthOp;
        let manager = AuthManager::new_empty();
        manager.role_add("r").unwrap();
        manager.apply_auth_op_to_view(&AuthOp::RoleGrantCapability {
            role: "r".into(),
            capability_id: "data:storage:read".into(),
            scope: String::new(),
        });
        let roles = vec!["r".to_string()];
        assert!(manager.check_capability(&roles, "data:storage:read", None));
        assert!(manager.check_capability(&roles, "data:storage:read", Some("anything")));
    }

    /// AuthOp bincode 变体索引固定：能力授予两变体**末尾追加**，
    /// 既有变体（含 ConsumeSession）索引不漂移（旧日志/快照升级兼容）。
    #[test]
    fn test_auth_op_bincode_variant_indices_appended() {
        use crate::raft::type_config::AuthOp;
        fn variant_index(op: &AuthOp) -> u32 {
            let bytes = bincode::serialize(op).unwrap();
            u32::from_le_bytes(bytes[0..4].try_into().unwrap())
        }
        assert_eq!(
            variant_index(&AuthOp::UserAdd {
                name: "u".into(),
                hash: "h".into(),
                roles: vec![]
            }),
            0
        );
        assert_eq!(
            variant_index(&AuthOp::ConsumeSession {
                hash_hex: "x".into()
            }),
            11
        );
        assert_eq!(
            variant_index(&AuthOp::RoleGrantCapability {
                role: "r".into(),
                capability_id: "c".into(),
                scope: "s".into(),
            }),
            12
        );
        assert_eq!(
            variant_index(&AuthOp::RoleRevokeCapability {
                role: "r".into(),
                capability_id: "c".into(),
                scope: "s".into(),
            }),
            13
        );
        // 批次 9：bootstrap 令牌三变体追加（14/15/16），既有索引不漂移
        assert_eq!(
            variant_index(&AuthOp::IssueBootstrapToken {
                id: "i".into(),
                hash_hex: "h".into(),
                label: "l".into(),
                created_by: "root".into(),
                created_at_unix: 0,
                expires_at_unix: 0,
            }),
            14
        );
        assert_eq!(
            variant_index(&AuthOp::ConsumeBootstrapToken {
                id: "i".into(),
                consumed_at_unix: 0,
            }),
            15
        );
        assert_eq!(
            variant_index(&AuthOp::RevokeBootstrapToken { id: "i".into() }),
            16
        );
        // 第四轮 §3.6 b：批量会话清理末尾追加（17），既有索引不漂移
        assert_eq!(
            variant_index(&AuthOp::ConsumeSessions {
                hash_hexes: vec!["x".into()]
            }),
            17
        );
    }

    // ──── 动态 bootstrap 令牌（TTL + 一次性） ────

    fn bootstrap_record(id: &str, hash: &str, expires_at: u64) -> AuthBootstrapTokenRecord {
        AuthBootstrapTokenRecord {
            id: id.into(),
            hash_hex: hash.into(),
            label: "test".into(),
            created_by: "root".into(),
            created_at_unix: 0,
            expires_at_unix: expires_at,
            consumed_at_unix: None,
        }
    }

    #[test]
    fn bootstrap_token_atomic_consume_is_one_time() {
        let mgr = AuthManager::new_empty();
        mgr.apply_auth_op_to_view(&AuthOp::IssueBootstrapToken {
            id: "t1".into(),
            hash_hex: "abc".into(),
            label: "l".into(),
            created_by: "root".into(),
            created_at_unix: 0,
            expires_at_unix: 1_000,
        });

        // 首次消费成功；第二次拿不到（一次性）
        let first = mgr.consume_bootstrap_token_atomic("abc", 100);
        assert!(first.is_some());
        assert_eq!(first.unwrap().id, "t1");
        assert!(mgr.consume_bootstrap_token_atomic("abc", 100).is_none());

        // 记录保留（审计）且标记为已消费
        let rec = mgr.bootstrap_token("t1").unwrap();
        assert!(rec.is_consumed());
        assert_eq!(rec.consumed_at_unix, Some(100));
    }

    #[test]
    fn bootstrap_token_expiry_blocks_consume() {
        let mgr = AuthManager::new_empty();
        mgr.apply_auth_op_to_view(&AuthOp::IssueBootstrapToken {
            id: "t2".into(),
            hash_hex: "def".into(),
            label: String::new(),
            created_by: "root".into(),
            created_at_unix: 0,
            expires_at_unix: 500,
        });
        assert!(mgr.consume_bootstrap_token_atomic("def", 499).is_some());
        assert!(mgr.consume_bootstrap_token_atomic("def", 500).is_none());
    }

    #[test]
    fn bootstrap_token_revert_allows_retry_after_proposal_failure() {
        let mgr = AuthManager::new_empty();
        mgr.apply_auth_op_to_view(&AuthOp::IssueBootstrapToken {
            id: "t3".into(),
            hash_hex: "ghi".into(),
            label: String::new(),
            created_by: "root".into(),
            created_at_unix: 0,
            expires_at_unix: 1_000,
        });
        assert!(mgr.consume_bootstrap_token_atomic("ghi", 10).is_some());
        // 模拟 raft 提案失败 → 回滚
        assert!(mgr.revert_bootstrap_token_consumption("t3", 10));
        assert!(!mgr.bootstrap_token("t3").unwrap().is_consumed());
        // 回滚后可再次消费
        assert!(mgr.consume_bootstrap_token_atomic("ghi", 11).is_some());
        // 回滚带错误 expected → 不生效（避免误回滚他人消费）
        assert!(!mgr.revert_bootstrap_token_consumption("t3", 10));
        assert!(mgr.bootstrap_token("t3").unwrap().is_consumed());
    }

    #[test]
    fn bootstrap_token_revoke_and_prune() {
        let mgr = AuthManager::new_empty();
        for (id, hash, exp) in [("a", "h1", 100u64), ("b", "h2", 10_000u64)] {
            mgr.apply_auth_op_to_view(&AuthOp::IssueBootstrapToken {
                id: id.into(),
                hash_hex: hash.into(),
                label: String::new(),
                created_by: "root".into(),
                created_at_unix: 0,
                expires_at_unix: exp,
            });
        }
        assert_eq!(mgr.bootstrap_tokens().len(), 2);
        // 清理过期未消费（a@100 过期；b 仍有效）
        assert_eq!(mgr.prune_expired_bootstrap_tokens(200), 1);
        assert!(mgr.bootstrap_token("a").is_none());
        assert!(mgr.bootstrap_token("b").is_some());
        // 撤销
        mgr.apply_auth_op_to_view(&AuthOp::RevokeBootstrapToken { id: "b".into() });
        assert!(mgr.bootstrap_tokens().is_empty());
    }

    #[test]
    fn bootstrap_token_record_roundtrip() {
        let rec = bootstrap_record("id1", "hash1", 1234);
        let bytes = rec.to_bytes().unwrap();
        assert_eq!(AuthBootstrapTokenRecord::from_bytes(&bytes), Some(rec));
    }

    #[test]
    fn test_user_add_and_authenticate() {
        let mgr = AuthManager::new();
        mgr.user_add("alice", "password123").unwrap();
        mgr.authenticate("alice", "password123").unwrap();
    }

    #[test]
    fn test_authenticate_wrong_password() {
        let mgr = AuthManager::new();
        mgr.user_add("bob", "correct").unwrap();
        assert!(mgr.authenticate("bob", "wrong").is_err());
    }

    // ──── Argon2id 与遗留迁移 ────

    #[test]
    fn test_user_add_uses_argon2id() {
        let mgr = AuthManager::new();
        mgr.user_add("argon-user", "password123").unwrap();
        let hash = mgr
            .users
            .read()
            .get("argon-user")
            .unwrap()
            .password_hash
            .clone();
        let hash_str = String::from_utf8(hash).unwrap();
        assert!(
            hash_str.starts_with(ARGON2ID_PREFIX),
            "new password must be Argon2id PHC, got: {hash_str}"
        );
        // 正确密码可登录、错误密码拒绝
        assert!(mgr.authenticate("argon-user", "password123").is_ok());
        assert!(mgr.authenticate("argon-user", "wrong").is_err());
    }

    #[test]
    fn test_legacy_sha256_transparent_migration() {
        let mgr = AuthManager::new();
        mgr.user_add("legacy-user", "old-pass").unwrap();
        // 手动将哈希降级为遗留 SHA256（模拟存量数据）
        mgr.users
            .write()
            .get_mut("legacy-user")
            .unwrap()
            .password_hash = hash_password_legacy("old-pass");

        // 登录成功 → 透明迁移为 Argon2id
        mgr.authenticate("legacy-user", "old-pass").unwrap();
        let hash = mgr
            .users
            .read()
            .get("legacy-user")
            .unwrap()
            .password_hash
            .clone();
        assert!(
            hash.starts_with(ARGON2ID_PREFIX.as_bytes()),
            "legacy hash must be transparently upgraded to Argon2id"
        );
        // 迁移后仍可登录
        assert!(mgr.authenticate("legacy-user", "old-pass").is_ok());
    }

    #[test]
    fn test_argon2id_salt_makes_hashes_unique() {
        let h1 = hash_password_argon2id("same-password").unwrap();
        let h2 = hash_password_argon2id("same-password").unwrap();
        assert_ne!(h1, h2, "Argon2id random salt must produce distinct hashes");
    }

    #[test]
    fn test_user_add_duplicate_fails() {
        let mgr = AuthManager::new();
        mgr.user_add("carol", "pass1").unwrap();
        assert!(mgr.user_add("carol", "pass2").is_err());
    }

    #[test]
    fn test_role_add_and_list() {
        let mgr = AuthManager::new();
        mgr.role_add("reader").unwrap();
        mgr.role_add("writer").unwrap();

        let roles = mgr.role_list();
        assert_eq!(roles.len(), 3); // root + reader + writer
    }

    #[test]
    fn test_role_grant_and_check_permission() {
        let mgr = AuthManager::new();
        mgr.user_add("dave", "pass").unwrap();
        mgr.role_add("reader").unwrap();
        mgr.role_grant_permission("reader", PermissionType::Read, b"/app/".to_vec(), vec![])
            .unwrap();
        mgr.user_grant_role("dave", "reader").unwrap();

        mgr.enable();

        // Dave can read /app/config
        assert!(mgr.authorize_read("dave", b"/app/config").is_ok());
        // Dave cannot write /app/config
        assert!(mgr.authorize_write("dave", b"/app/config").is_err());
        // Dave cannot read /other
        assert!(mgr.authorize_read("dave", b"/other/data").is_err());
    }

    #[test]
    fn test_auth_disabled_allows_all() {
        let mgr = AuthManager::new();
        mgr.user_add("eve", "pass").unwrap();

        // Auth disabled — all operations allowed
        assert!(mgr.authorize_read("nonexistent", b"/any/key").is_ok());
        assert!(mgr.authorize_write("nonexistent", b"/any/key").is_ok());
    }

    #[test]
    fn test_permission_readwrite() {
        let mgr = AuthManager::new();
        mgr.user_add("frank", "pass").unwrap();
        mgr.role_add("admin").unwrap();
        mgr.role_grant_permission(
            "admin",
            PermissionType::ReadWrite,
            b"/data/".to_vec(),
            vec![],
        )
        .unwrap();
        mgr.user_grant_role("frank", "admin").unwrap();

        mgr.enable();

        assert!(mgr.authorize_read("frank", b"/data/file").is_ok());
        assert!(mgr.authorize_write("frank", b"/data/file").is_ok());
    }

    #[test]
    fn test_revoke_role() {
        let mgr = AuthManager::new();
        mgr.user_add("grace", "pass").unwrap();
        mgr.role_add("temp").unwrap();
        mgr.role_grant_permission("temp", PermissionType::Read, b"/tmp/".to_vec(), vec![])
            .unwrap();
        mgr.user_grant_role("grace", "temp").unwrap();

        mgr.enable();
        assert!(mgr.authorize_read("grace", b"/tmp/x").is_ok());

        mgr.user_revoke_role("grace", "temp").unwrap();
        assert!(mgr.authorize_read("grace", b"/tmp/x").is_err());
    }

    #[test]
    fn test_delete_user() {
        let mgr = AuthManager::new();
        mgr.user_add("heidi", "pass").unwrap();
        assert_eq!(mgr.user_list().len(), 2); // root + heidi

        mgr.user_delete("heidi").unwrap();
        assert_eq!(mgr.user_list().len(), 1); // just root
        assert!(mgr.authenticate("heidi", "pass").is_err());
    }
}
