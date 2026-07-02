// Auth Manager — User/Role/Permission management with RBAC (ADP §14)
//
// Manages:
// - Users (name, password hash)
// - Roles (name, permissions)
// - User-Role assignments
// - Auth enable/disable state
//
// Passwords are hashed with SHA256 (production should use argon2).
// Permissions control Read/Write/ReadWrite on Key prefix ranges.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use parking_lot::RwLock;
use sha2::{Sha256, Digest};

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
}

// ──── Role ────

#[derive(Debug, Clone)]
pub struct Role {
    pub name: String,
    pub permissions: Vec<Permission>,
}

// ──── User ────

#[derive(Debug, Clone)]
struct UserEntry {
    /// SHA256 hash of password
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
}

impl AuthManager {
    /// Create a new AuthManager (auth disabled by default)
    pub fn new() -> Self {
        let manager = Self {
            enabled: Arc::new(RwLock::new(false)),
            users: Arc::new(RwLock::new(HashMap::new())),
            roles: Arc::new(RwLock::new(HashMap::new())),
        };

        // Create default root user and admin role
        manager.create_default_admin();

        manager
    }

    /// Create default admin user and role
    fn create_default_admin(&self) {
        // Default root user (password must be set via init or env)
        let root_entry = UserEntry {
            password_hash: hash_password("root"),
            roles: {
                let mut set = HashSet::new();
                set.insert("root".to_string());
                set
            },
        };
        self.users.write().insert("root".to_string(), root_entry);

        // root role: ReadWrite on all keys
        let root_role = Role {
            name: "root".to_string(),
            permissions: vec![Permission {
                perm_type: PermissionType::ReadWrite,
                key_prefix: vec![],
                range_end: vec![],
            }],
        };
        self.roles.write().insert("root".to_string(), root_role);
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
            return Err(Error::UserAlreadyExists { name: name.to_string() });
        }
        users.insert(
            name.to_string(),
            UserEntry {
                password_hash: hash_password(password),
                roles: HashSet::new(),
            },
        );
        Ok(())
    }

    /// Delete a user
    pub fn user_delete(&self, name: &str) -> Result<()> {
        let mut users = self.users.write();
        if users.remove(name).is_none() {
            return Err(Error::NotFound { resource: "user", key: name.to_string() });
        }
        Ok(())
    }

    /// Change a user's password
    pub fn user_change_password(&self, name: &str, new_password: &str) -> Result<()> {
        let mut users = self.users.write();
        let entry = users
            .get_mut(name)
            .ok_or_else(|| Error::NotFound { resource: "user", key: name.to_string() })?;
        entry.password_hash = hash_password(new_password);
        Ok(())
    }

    /// List all users
    pub fn user_list(&self) -> Vec<String> {
        self.users.read().keys().cloned().collect()
    }

    /// Get roles for a user
    pub fn user_get_roles(&self, name: &str) -> Result<Vec<String>> {
        let users = self.users.read();
        let entry = users
            .get(name)
            .ok_or_else(|| Error::NotFound { resource: "user", key: name.to_string() })?;
        Ok(entry.roles.iter().cloned().collect())
    }

    /// Authenticate a user with password
    pub fn authenticate(&self, name: &str, password: &str) -> Result<()> {
        let users = self.users.read();
        let entry = users
            .get(name)
            .ok_or_else(|| Error::Unauthenticated("invalid credentials".to_string()))?;

        let password_hash = hash_password(password);
        if entry.password_hash != password_hash {
            return Err(Error::Unauthenticated("invalid credentials".to_string()));
        }

        Ok(())
    }

    // ──── Role management ────

    /// Add a new role
    pub fn role_add(&self, name: &str) -> Result<()> {
        let mut roles = self.roles.write();
        if roles.contains_key(name) {
            return Err(Error::RoleAlreadyExists { name: name.to_string() });
        }
        roles.insert(
            name.to_string(),
            Role {
                name: name.to_string(),
                permissions: Vec::new(),
            },
        );
        Ok(())
    }

    /// Delete a role
    pub fn role_delete(&self, name: &str) -> Result<()> {
        let mut roles = self.roles.write();
        if roles.remove(name).is_none() {
            return Err(Error::NotFound { resource: "role", key: name.to_string() });
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
        let role = roles
            .get_mut(role_name)
            .ok_or_else(|| Error::NotFound { resource: "role", key: role_name.to_string() })?;

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
        let role = roles
            .get_mut(role_name)
            .ok_or_else(|| Error::NotFound { resource: "role", key: role_name.to_string() })?;

        let before = role.permissions.len();
        role.permissions.retain(|p| {
            !(p.key_prefix == key_prefix && p.range_end == range_end)
        });

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

    // ──── User-Role assignment ────

    /// Grant a role to a user
    pub fn user_grant_role(&self, username: &str, role_name: &str) -> Result<()> {
        // Verify role exists
        {
            let roles = self.roles.read();
            if !roles.contains_key(role_name) {
                return Err(Error::NotFound { resource: "role", key: role_name.to_string() });
            }
        }

        let mut users = self.users.write();
        let entry = users
            .get_mut(username)
            .ok_or_else(|| Error::NotFound { resource: "user", key: username.to_string() })?;
        entry.roles.insert(role_name.to_string());
        Ok(())
    }

    /// Revoke a role from a user
    pub fn user_revoke_role(&self, username: &str, role_name: &str) -> Result<()> {
        let mut users = self.users.write();
        let entry = users
            .get_mut(username)
            .ok_or_else(|| Error::NotFound { resource: "user", key: username.to_string() })?;

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
        let entry = users
            .get(username)
            .ok_or_else(|| Error::NotFound { resource: "user", key: username.to_string() })?;

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

// ──── Password hashing ────

fn hash_password(password: &str) -> Vec<u8> {
    let mut hasher = Sha256::new();
    hasher.update(password.as_bytes());
    hasher.finalize().to_vec()
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;

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
        mgr.role_grant_permission("admin", PermissionType::ReadWrite, b"/data/".to_vec(), vec![])
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
