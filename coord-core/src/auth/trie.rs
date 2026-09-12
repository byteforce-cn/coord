// Scope Prefix Trie — O(len(scope)) scope matching
//
// Builds a prefix tree from scope patterns, enabling efficient scope matching
// against request keys.
//
// Scope pattern rules:
// - Only [a-zA-Z0-9/_\-*] characters allowed
// - "*" at the end means "prefix wildcard" (e.g., "/app/*" matches "/app/anything")
// - Empty scope means "no restriction" (matches everything)

use std::collections::HashMap;

// ──── Trie Node ────

#[derive(Debug, Default, Clone)]
struct TrieNode {
    /// Child nodes keyed by the next path segment
    children: HashMap<String, TrieNode>,
    /// Whether this node (and its prefix) is an allowed scope endpoint
    is_endpoint: bool,
    /// Whether this node has a wildcard child ("*")
    has_wildcard: bool,
}

// ──── Scope Trie ────

/// A prefix tree for efficient scope matching.
///
/// Scope patterns like `/app/order-service/` or `/app/*` are inserted,
/// then request keys like `/app/order-service/order-123` are matched.
#[derive(Debug, Default, Clone)]
pub struct ScopeTrie {
    root: TrieNode,
}

impl ScopeTrie {
    /// Create a new empty scope trie.
    pub fn new() -> Self {
        Self::default()
    }

    /// Insert a scope pattern into the trie.
    ///
    /// An empty scope means "match everything" (no restriction).
    /// Patterns ending with "/*" are treated as prefix wildcards.
    pub fn insert(&mut self, scope: &str) -> Result<(), String> {
        if scope.is_empty() {
            // Empty scope = match-all, mark root as endpoint
            self.root.is_endpoint = true;
            return Ok(());
        }

        validate_scope_chars(scope)?;

        let segments = split_scope(scope);
        if segments.is_empty() {
            // Scope like "/" or "" → match-all at root
            self.root.is_endpoint = true;
            return Ok(());
        }

        let mut node = &mut self.root;

        for (i, segment) in segments.iter().enumerate() {
            let is_last = i == segments.len() - 1;

            if segment == "*" {
                if !is_last {
                    return Err("wildcard '*' must be the last segment".to_string());
                }
                node.has_wildcard = true;
                return Ok(());
            }

            node = node.children.entry(segment.clone()).or_default();

            if is_last {
                node.is_endpoint = true;
            }
        }

        Ok(())
    }

    /// Check if a given resource key matches any scope in the trie.
    ///
    /// Returns `true` if:
    /// - The trie has the root marked (match-all), OR
    /// - The key matches an exact endpoint, OR
    /// - Any ancestor node is an endpoint (prefix match — scope is a prefix of resource), OR
    /// - Any ancestor node has a wildcard that matches.
    pub fn matches(&self, resource: &str) -> bool {
        // Root endpoint means match-all (empty scope)
        if self.root.is_endpoint {
            return true;
        }

        let segments = split_scope(resource);
        let mut node = &self.root;

        for segment in &segments {
            // Check wildcard at current node (matches everything below)
            if node.has_wildcard {
                return true;
            }

            // Check if current node is an endpoint (prefix match — the scope
            // covers everything under this prefix)
            if node.is_endpoint {
                return true;
            }

            // Traverse to child
            match node.children.get(segment) {
                Some(child) => node = child,
                None => return false,
            }
        }

        // Exact match at final endpoint, or wildcard at final node
        node.is_endpoint || node.has_wildcard
    }

    /// Returns true if the trie is empty (no scopes inserted).
    pub fn is_empty(&self) -> bool {
        self.root.children.is_empty() && !self.root.is_endpoint && !self.root.has_wildcard
    }
}

// ──── Helpers ────

/// Validate scope characters: only [a-zA-Z0-9/_\-*]
fn validate_scope_chars(scope: &str) -> Result<(), String> {
    for (i, ch) in scope.chars().enumerate() {
        if !ch.is_ascii_alphanumeric() && ch != '/' && ch != '_' && ch != '-' && ch != '*' {
            return Err(format!(
                "invalid character '{}' at position {} in scope '{}'",
                ch, i, scope
            ));
        }
    }
    Ok(())
}

/// Split a scope/resource path into segments by '/'.
/// Leading/trailing slashes produce empty first/last segments.
fn split_scope(path: &str) -> Vec<String> {
    if path.is_empty() {
        return vec![];
    }
    path.split('/')
        .filter(|s| !s.is_empty())
        .map(|s| s.to_string())
        .collect()
}

// ──── 区间（range）覆盖判定 ────

/// 计算以 `prefix` 开头的所有字节串在字典序下的上确界（最小上界）。
///
/// 即大于所有 `prefix` 前缀串的最小字节串；`prefix` 全为 `0xFF` 时返回 `None`
/// （不存在这样的上界，按无界处理 → 调用方需 fail-closed）。
pub fn prefix_successor(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut out = prefix.to_vec();
    while let Some(last) = out.pop() {
        if last != 0xFF {
            out.push(last.wrapping_add(1));
            return Some(out);
        }
    }
    None
}

/// 判断 scope 模式是否**整体覆盖**区间 `[key, range_end)`。
///
/// scope 语义与 [`ScopeTrie`] 一致（按 `/` 分段的前缀匹配，末段可为 `*` 通配）。
/// 关键：不能只校验 `key`，否则持 `/app/a/` 的凭据发
/// `Range(key=/app/a/x, range_end=/zzz)` 就能读全库（A1 越权）。
///
/// 实现：把 scope 模式归约为一个**安全字面前缀** `P`（任何以 `P` 开头的字节串都
/// 必然命中该 scope），于是 `[key, range_end) ⊆ [P, succ(P))` 只需 `key` 与
/// `range_end` 都以 `P` 开头。无法安全归约时 fail-closed。
pub fn scope_covers_interval(scope: &str, key: &[u8], range_end: &[u8]) -> bool {
    // match-all：空 scope、"/"、"//"、"/*"
    if scope.is_empty() || scope.chars().all(|c| c == '/') || scope == "/*" {
        return true;
    }
    // 单 key 请求：沿用逐点匹配语义
    if range_end.is_empty() {
        let mut trie = ScopeTrie::new();
        return trie.insert(scope).is_ok()
            && trie.matches(std::str::from_utf8(key).unwrap_or("\u{FFFD}"));
    }
    // range_end == "\0" 表示"从 key 到无穷"（etcd 语义）：有界 scope 不可能覆盖
    if range_end == b"\0" {
        return false;
    }
    let Some(prefix) = scope_literal_prefix(scope) else {
        return false;
    };
    if prefix.is_empty() {
        return true; // 归一为空前缀 = match-all（如 "/*"）
    }
    key.starts_with(&prefix) && range_end.starts_with(&prefix)
}

/// 将 scope 模式归约为一个**安全字面前缀**。
///
/// 安全要求：任何以返回值开头的字节串，按 `ScopeTrie` 分段语义都必须命中该 scope。
/// 无法安全归约时返回 `None`（调用方 fail-closed）。
fn scope_literal_prefix(scope: &str) -> Option<Vec<u8>> {
    if let Some(base) = scope.strip_suffix("/*") {
        if base.is_empty() {
            return Some(Vec::new()); // "/*" = match-all
        }
        let mut p = base.to_string();
        if !p.ends_with('/') {
            p.push('/');
        }
        if p.contains('*') {
            return None;
        }
        return Some(p.into_bytes());
    }
    // 中间/末尾内嵌的 `*`（非独立末段）无法安全归约
    if scope.contains('*') {
        return None;
    }
    // 只有以 '/' 结尾时，「字节前缀 ⊆ 分段前缀」才成立
    if !scope.ends_with('/') {
        return None;
    }
    Some(scope.as_bytes().to_vec())
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;

    // ──── Scope Trie basic operations ────

    #[test]
    fn test_empty_scope_matches_all() {
        let mut trie = ScopeTrie::new();
        trie.insert("").expect("empty scope should be valid");

        assert!(trie.matches("/anything"));
        assert!(trie.matches("/app/order/123"));
        assert!(trie.matches(""));
    }

    #[test]
    fn test_exact_scope_match() {
        let mut trie = ScopeTrie::new();
        trie.insert("/app/order-service/").unwrap();

        assert!(trie.matches("/app/order-service/order-123"));
        assert!(!trie.matches("/app/payment-service/payment-456"));
        assert!(!trie.matches("/other"));
    }

    #[test]
    fn test_wildcard_scope_match() {
        let mut trie = ScopeTrie::new();
        trie.insert("/app/*").unwrap();

        assert!(trie.matches("/app/order-service/order-123"));
        assert!(trie.matches("/app/payment-service/payment-456"));
        assert!(trie.matches("/app/anything"));
        assert!(!trie.matches("/other/data"));
    }

    #[test]
    fn test_multiple_scopes() {
        let mut trie = ScopeTrie::new();
        trie.insert("/app/order-service/").unwrap();
        trie.insert("/app/config/").unwrap();
        trie.insert("/data/public/*").unwrap();

        // Should match order-service scope
        assert!(trie.matches("/app/order-service/order-123"));
        // Should match config scope
        assert!(trie.matches("/app/config/db"));
        // Should match wildcard
        assert!(trie.matches("/data/public/metrics/cpu"));
        // Should NOT match
        assert!(!trie.matches("/app/secret/keys"));
        assert!(!trie.matches("/data/private/data"));
    }

    #[test]
    fn test_wildcard_in_middle_rejected() {
        let mut trie = ScopeTrie::new();
        let result = trie.insert("/app/*/config");
        assert!(result.is_err(), "wildcard in middle should be rejected");
    }

    #[test]
    fn test_invalid_scope_characters() {
        let mut trie = ScopeTrie::new();
        assert!(trie.insert("/app/path with spaces").is_err());
        assert!(trie.insert("/app/../../etc").is_err()); // dots are invalid
        assert!(trie.insert("/app/path$special").is_err());
    }

    #[test]
    fn test_scope_match_root() {
        let mut trie = ScopeTrie::new();
        trie.insert("/").unwrap();
        // "/" splits into empty segments (filtered out), so insert marks root endpoint
        assert!(trie.matches("/anything"));
    }

    #[test]
    fn test_scope_with_dashes_and_underscores() {
        let mut trie = ScopeTrie::new();
        trie.insert("/app/my-service/v1_config/").unwrap();
        assert!(trie.matches("/app/my-service/v1_config/entry"));
        assert!(!trie.matches("/app/other-service/v1_config/entry"));
    }
}
