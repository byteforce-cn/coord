// Role Cache — Local cache of Role→Capability mappings
//
// Agent periodically syncs the full Role→Capability map from Server (every 5 min).
// High-sensitivity roles (e.g., admin, security-manager, root) bypass the cache
// and force a server lookup on every request.
//
// See docs/capability-auth-implementation.md §3.2, §6.3.

use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;

use coord_core::auth::trie::ScopeTrie;

// ──── Capability Grant ────

/// A capability grant assigned to a role, with optional scope restriction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CapabilityGrant {
    /// Full capability ID: "data:kv:read"
    pub capability_id: String,
    /// Scope restriction (empty = no restriction)
    pub scope: String,
}

// ──── Role Entry ────

/// Cached role information including capability grants.
#[derive(Debug, Clone)]
pub struct RoleEntry {
    pub name: String,
    pub grants: Vec<CapabilityGrant>,
    /// Whether this role is high-sensitivity (forces server lookup every request)
    pub high_sensitive: bool,
}

impl RoleEntry {
    /// Build a ScopeTrie from all grants for this role.
    pub fn build_scope_trie(&self, capability_id: &str) -> Option<ScopeTrie> {
        let mut trie = ScopeTrie::new();
        let mut has_match = false;

        for grant in &self.grants {
            if grant.capability_id == capability_id {
                has_match = true;
                // Ignore insert errors for invalid scopes (validated at registration)
                let _ = trie.insert(&grant.scope);
            }
        }

        if has_match { Some(trie) } else { None }
    }
}

// ──── Role Cache ────

/// Thread-safe cache for Role→Capability mappings.
pub struct RoleCache {
    /// role_name → RoleEntry
    map: Arc<RwLock<HashMap<String, RoleEntry>>>,
    /// Timestamp of last successful sync (Unix seconds)
    last_sync: AtomicI64,
    /// Whether the cache has been populated at least once
    initialized: AtomicI64,
}

impl RoleCache {
    /// Create a new empty role cache.
    pub fn new() -> Self {
        Self {
            map: Arc::new(RwLock::new(HashMap::new())),
            last_sync: AtomicI64::new(0),
            initialized: AtomicI64::new(0),
        }
    }

    /// Get grants for a specific role.
    pub fn get(&self, role_name: &str) -> Option<Vec<CapabilityGrant>> {
        self.map.read().get(role_name).map(|e| e.grants.clone())
    }

    /// Check if a role is high-sensitivity (forces server lookup).
    pub fn is_high_sensitive(&self, role_name: &str) -> bool {
        self.map
            .read()
            .get(role_name)
            .map(|e| e.high_sensitive)
            .unwrap_or(false)
    }

    /// Get the set of granted capability IDs for a role.
    pub fn get_capability_ids(&self, role_name: &str) -> Vec<String> {
        self.map
            .read()
            .get(role_name)
            .map(|e| e.grants.iter().map(|g| g.capability_id.clone()).collect())
            .unwrap_or_default()
    }

    /// Check if any of the given roles grant a specific capability, returning
    /// the union of ScopeTries for that capability.
    pub fn check_capability(
        &self,
        roles: &[String],
        capability_id: &str,
    ) -> (bool, Option<ScopeTrie>) {
        let map = self.map.read();
        let mut combined_trie = ScopeTrie::new();
        let mut granted = false;

        for role_name in roles {
            if let Some(entry) = map.get(role_name) {
                if let Some(trie) = entry.build_scope_trie(capability_id) {
                    granted = true;
                    // Merge: if any role has empty scope (match-all), the combined trie
                    // should also match-all
                    if trie.is_empty() {
                        // An empty trie means match-all — mark root as endpoint
                        let _ = combined_trie.insert("");
                    } else {
                        // For non-empty tries, we need to merge. Since we can't merge
                        // tries efficiently, we just return match-all if any grant
                        // has no scope restriction.
                        for grant in &entry.grants {
                            if grant.capability_id == capability_id {
                                if grant.scope.is_empty() {
                                    let _ = combined_trie.insert("");
                                } else {
                                    let _ = combined_trie.insert(&grant.scope);
                                }
                            }
                        }
                    }
                }
            }
        }

        if granted {
            (true, Some(combined_trie))
        } else {
            (false, None)
        }
    }

    /// Replace all cached entries with a new map.
    pub fn sync_full(&self, roles: Vec<RoleEntry>) {
        let mut map = self.map.write();
        map.clear();
        for role in roles {
            map.insert(role.name.clone(), role);
        }
        self.last_sync.store(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
            Ordering::Release,
        );
        self.initialized.store(1, Ordering::Release);
    }

    /// Whether the cache has been populated at least once.
    pub fn is_initialized(&self) -> bool {
        self.initialized.load(Ordering::Acquire) == 1
    }

    /// Unix timestamp of last successful sync.
    pub fn last_sync_time(&self) -> i64 {
        self.last_sync.load(Ordering::Acquire)
    }

    /// Number of cached roles.
    pub fn role_count(&self) -> usize {
        self.map.read().len()
    }
}

impl Default for RoleCache {
    fn default() -> Self {
        Self::new()
    }
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_role(name: &str, grants: Vec<(&str, &str)>, high_sensitive: bool) -> RoleEntry {
        RoleEntry {
            name: name.to_string(),
            grants: grants
                .into_iter()
                .map(|(cap_id, scope)| CapabilityGrant {
                    capability_id: cap_id.to_string(),
                    scope: scope.to_string(),
                })
                .collect(),
            high_sensitive,
        }
    }

    // ──── Phase 3.1: Role cache CRUD ────

    #[test]
    fn test_cache_empty_on_creation() {
        let cache = RoleCache::new();
        assert!(!cache.is_initialized());
        assert_eq!(cache.role_count(), 0);
        assert!(cache.get("nonexistent").is_none());
    }

    #[test]
    fn test_cache_sync_and_retrieve() {
        let cache = RoleCache::new();
        let roles = vec![
            make_role("reader", vec![("data:kv:read", "/app/")], false),
            make_role("writer", vec![("data:kv:read", "/app/"), ("data:kv:write", "/app/")], false),
        ];

        cache.sync_full(roles);
        assert!(cache.is_initialized());
        assert_eq!(cache.role_count(), 2);

        let reader_grants = cache.get("reader").unwrap();
        assert_eq!(reader_grants.len(), 1);
        assert_eq!(reader_grants[0].capability_id, "data:kv:read");

        let writer_grants = cache.get("writer").unwrap();
        assert_eq!(writer_grants.len(), 2);
    }

    #[test]
    fn test_high_sensitive_detection() {
        let cache = RoleCache::new();
        let roles = vec![
            make_role("admin", vec![], true),
            make_role("reader", vec![("data:kv:read", "")], false),
        ];

        cache.sync_full(roles);
        assert!(cache.is_high_sensitive("admin"));
        assert!(!cache.is_high_sensitive("reader"));
        assert!(!cache.is_high_sensitive("nonexistent"));
    }

    #[test]
    fn test_sync_replaces_previous_data() {
        let cache = RoleCache::new();

        // First sync
        cache.sync_full(vec![make_role("reader", vec![("data:kv:read", "/app/")], false)]);
        assert_eq!(cache.role_count(), 1);

        // Second sync replaces
        cache.sync_full(vec![make_role("writer", vec![("data:kv:write", "/app/")], false)]);
        assert_eq!(cache.role_count(), 1);
        assert!(cache.get("reader").is_none());
        assert!(cache.get("writer").is_some());
    }

    #[test]
    fn test_capability_check_single_role() {
        let cache = RoleCache::new();
        cache.sync_full(vec![make_role(
            "reader",
            vec![("data:kv:read", "/app/order/"), ("data:watch:subscribe", "")],
            false,
        )]);

        // Should have data:kv:read with scope /app/order/
        let (granted, trie) = cache.check_capability(&["reader".to_string()], "data:kv:read");
        assert!(granted);
        let trie = trie.unwrap();
        assert!(trie.matches("/app/order/123"));
        assert!(!trie.matches("/app/payment/456"));

        // Should not have data:kv:write
        let (granted, _) = cache.check_capability(&["reader".to_string()], "data:kv:write");
        assert!(!granted);
    }

    #[test]
    fn test_capability_check_multiple_roles_union() {
        let cache = RoleCache::new();
        cache.sync_full(vec![
            make_role("reader", vec![("data:kv:read", "/app/order/")], false),
            make_role("config_reader", vec![("coord:config:read", "/app/config/")], false),
        ]);

        // Both roles together grant both capabilities
        let (granted, trie) =
            cache.check_capability(&["reader".to_string(), "config_reader".to_string()], "data:kv:read");
        assert!(granted);
        let trie = trie.unwrap();
        assert!(trie.matches("/app/order/123"));
        assert!(!trie.matches("/app/config/db"));

        let (granted, trie) =
            cache.check_capability(&["reader".to_string(), "config_reader".to_string()], "coord:config:read");
        assert!(granted);
        let trie = trie.unwrap();
        assert!(trie.matches("/app/config/db"));
        assert!(!trie.matches("/app/order/123"));
    }

    #[test]
    fn test_empty_scope_match_all() {
        let cache = RoleCache::new();
        cache.sync_full(vec![make_role(
            "superuser",
            vec![("data:kv:read", "")], // empty scope = match-all
            true,
        )]);

        let (granted, trie) = cache.check_capability(&["superuser".to_string()], "data:kv:read");
        assert!(granted);
        let trie = trie.unwrap();
        assert!(trie.matches("/anything"));
        assert!(trie.matches("/app/anything/deep/path"));
    }
}
