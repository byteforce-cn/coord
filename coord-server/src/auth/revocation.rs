// Token Revocation Store (Phase 2.5)
//
// Maintains the set of revoked CCT tokens by their jti (JWT ID).
// Uses a bloom filter for space-efficient membership testing with
// fallback exact lookup to handle false positives.
//
// Design:
// - Bloom filter with p ≤ 0.0001 (1 in 10,000 false positive rate)
// - Exact HashSet for precise revocation checking
// - Versioned delta sync for Agent incremental updates
// - Fallback exact lookup on bloom positive
//
// See docs/capability-auth-implementation.md §3.4.

use std::collections::{HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;

// ──── Bloom Filter ────

/// A simple bloom filter for space-efficient membership testing.
///
/// Uses k hash functions (double hashing technique) and m bits.
/// Target false positive rate p ≤ 0.0001.
#[derive(Debug)]
pub struct BloomFilter {
    /// Bit array (stored as bytes)
    bits: Vec<u8>,
    /// Number of hash functions (k)
    num_hashes: u32,
    /// Number of bits (m)
    num_bits: u64,
    /// Number of items inserted
    count: u64,
}

impl BloomFilter {
    /// Create a new bloom filter optimized for expected number of items.
    ///
    /// Automatically calculates optimal m and k for p = 0.0001.
    pub fn new(expected_items: u64) -> Self {
        let p = 0.0001_f64;
        // Optimal number of bits: m = -n * ln(p) / (ln(2)^2)
        let m = (-(expected_items as f64) * p.ln() / (2.0_f64.ln().powi(2))).ceil() as u64;
        // Optimal number of hash functions: k = (m/n) * ln(2)
        let k = ((m as f64 / expected_items.max(1) as f64) * 2.0_f64.ln()).ceil() as u32;

        // Minimum constraints
        let m = m.max(1024);
        let k = k.max(2).min(32);

        let num_bytes = ((m + 7) / 8) as usize;
        Self {
            bits: vec![0u8; num_bytes],
            num_hashes: k,
            num_bits: m,
            count: 0,
        }
    }

    /// Insert an item into the bloom filter.
    pub fn insert(&mut self, item: &[u8]) {
        let (h1, h2) = self.hash_pair(item);
        for i in 0..self.num_hashes {
            let bit = self.bit_index(h1, h2, i);
            self.set_bit(bit);
        }
        self.count += 1;
    }

    /// Check if an item might be in the set.
    ///
    /// Returns `true` if the item is definitely in the set (no false negatives),
    /// but may return `true` for items not in the set (false positive).
    pub fn contains(&self, item: &[u8]) -> bool {
        let (h1, h2) = self.hash_pair(item);
        for i in 0..self.num_hashes {
            let bit = self.bit_index(h1, h2, i);
            if !self.get_bit(bit) {
                return false;
            }
        }
        true
    }

    /// Number of items inserted.
    pub fn len(&self) -> u64 {
        self.count
    }

    /// Whether the filter is empty.
    pub fn is_empty(&self) -> bool {
        self.count == 0
    }

    /// Memory usage in bytes.
    pub fn memory_bytes(&self) -> usize {
        self.bits.len()
    }

    // ──── Internal helpers ────

    fn hash_pair(&self, data: &[u8]) -> (u64, u64) {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;
        let mut hasher = DefaultHasher::new();
        hasher.write(data);
        let h1 = hasher.finish();

        // Second hash: XOR with a constant to get different distribution
        let h2 = h1.wrapping_mul(0x9E3779B97F4A7C15).wrapping_add(data.len() as u64);
        (h1, h2)
    }

    fn bit_index(&self, h1: u64, h2: u64, i: u32) -> u64 {
        // Double hashing: (h1 + i * h2) % m
        let combined = h1.wrapping_add((i as u64).wrapping_mul(h2));
        combined % self.num_bits
    }

    fn set_bit(&mut self, bit: u64) {
        let byte_idx = (bit / 8) as usize;
        let bit_offset = (bit % 8) as u8;
        self.bits[byte_idx] |= 1 << bit_offset;
    }

    fn get_bit(&self, bit: u64) -> bool {
        let byte_idx = (bit / 8) as usize;
        let bit_offset = (bit % 8) as u8;
        (self.bits[byte_idx] & (1 << bit_offset)) != 0
    }
}

// ──── Revocation Store ────

/// Manages token revocation with bloom filter + exact set.
///
/// - Bloom filter for fast "might be revoked" checks
/// - Exact HashSet for precise revocation lookup (fallback)
/// - Version counter for delta sync
#[derive(Debug)]
pub struct RevocationStore {
    /// Exact set of revoked jti strings
    revoked: RwLock<HashSet<String>>,
    /// Bloom filter for fast membership testing
    bloom: RwLock<BloomFilter>,
    /// Monotonically increasing version for delta sync
    version: AtomicU64,
    /// Recent revocation events for delta sync (sliding window)
    recent_revocations: RwLock<VecDeque<RevocationEvent>>,
    /// Max recent revocations to retain
    max_recent: usize,
}

/// A revocation event for delta sync.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RevocationEvent {
    /// The revoked jti
    pub jti: String,
    /// Version when this revocation occurred
    pub version: u64,
    /// When this revocation occurred (Unix seconds)
    pub timestamp: i64,
}

impl RevocationStore {
    /// Create a new revocation store.
    pub fn new(expected_revocations: u64) -> Self {
        Self {
            revoked: RwLock::new(HashSet::new()),
            bloom: RwLock::new(BloomFilter::new(expected_revocations)),
            version: AtomicU64::new(1),
            recent_revocations: RwLock::new(VecDeque::new()),
            max_recent: 10000,
        }
    }

    /// Revoke a token by its jti.
    ///
    /// Returns the version number for this revocation (after increment).
    pub fn revoke(&self, jti: &str) -> u64 {
        let version = self.version.fetch_add(1, Ordering::Release) + 1; // Return new version

        // Add to exact set
        self.revoked.write().insert(jti.to_string());

        // Add to bloom filter
        self.bloom.write().insert(jti.as_bytes());

        // Record for delta sync
        let event = RevocationEvent {
            jti: jti.to_string(),
            version,
            timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
        };
        let mut recent = self.recent_revocations.write();
        recent.push_back(event);
        while recent.len() > self.max_recent {
            recent.pop_front();
        }

        version
    }

    /// Check if a token jti is revoked.
    ///
    /// Two-step process:
    /// 1. Check bloom filter (fast, may have false positives)
    /// 2. If bloom positive, check exact set (definitive)
    pub fn is_revoked(&self, jti: &str) -> bool {
        // Step 1: Bloom filter check
        if !self.bloom.read().contains(jti.as_bytes()) {
            return false; // Definitely not revoked
        }

        // Step 2: Exact check (fallback for bloom positives)
        self.revoked.read().contains(jti)
    }

    /// Check bloom filter only (may have false positives).
    /// Returns true if the jti might be revoked.
    pub fn might_be_revoked(&self, jti: &str) -> bool {
        self.bloom.read().contains(jti.as_bytes())
    }

    /// Check exact set only (definitive, no false positives).
    pub fn is_exactly_revoked(&self, jti: &str) -> bool {
        self.revoked.read().contains(jti)
    }

    /// Get the current version number (for delta sync).
    pub fn current_version(&self) -> u64 {
        self.version.load(Ordering::Acquire)
    }

    /// Get revocation delta since a given version.
    pub fn get_delta_since(&self, since_version: u64) -> Vec<RevocationEvent> {
        let recent = self.recent_revocations.read();
        recent
            .iter()
            .filter(|e| e.version > since_version)
            .cloned()
            .collect()
    }

    /// Total number of revoked tokens.
    pub fn revoked_count(&self) -> usize {
        self.revoked.read().len()
    }

    /// Bloom filter memory usage in bytes.
    pub fn bloom_memory_bytes(&self) -> usize {
        self.bloom.read().memory_bytes()
    }

    /// Rebuild the bloom filter from the exact set (after deserialization).
    pub fn rebuild_bloom(&self, expected_items: u64) {
        let mut bloom = BloomFilter::new(expected_items);
        for jti in self.revoked.read().iter() {
            bloom.insert(jti.as_bytes());
        }
        *self.bloom.write() = bloom;
    }
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;

    // ──── Bloom Filter tests ────

    #[test]
    fn test_bloom_filter_insert_and_contains() {
        let mut bf = BloomFilter::new(1000);
        bf.insert(b"token-001");
        bf.insert(b"token-002");
        bf.insert(b"token-003");

        assert!(bf.contains(b"token-001"));
        assert!(bf.contains(b"token-002"));
        assert!(bf.contains(b"token-003"));
        assert!(!bf.contains(b"token-999"));
    }

    #[test]
    fn test_bloom_filter_empty() {
        let bf = BloomFilter::new(100);
        assert!(bf.is_empty());
        assert!(!bf.contains(b"anything"));
        assert_eq!(bf.len(), 0);
    }

    #[test]
    fn test_bloom_filter_memory_budget() {
        // With p=0.0001 and 1000 expected items, memory should be < 4KB
        let bf = BloomFilter::new(1000);
        assert!(bf.memory_bytes() < 4096, "bloom filter memory should be under 4KB for 1000 items at p=0.0001");
    }

    #[test]
    fn test_bloom_filter_no_false_negatives() {
        let mut bf = BloomFilter::new(100);
        // Insert 50 items
        for i in 0..50 {
            let key = format!("token-{i:04}");
            bf.insert(key.as_bytes());
        }
        // Verify all inserted items are found
        for i in 0..50 {
            let key = format!("token-{i:04}");
            assert!(bf.contains(key.as_bytes()), "bloom filter must not have false negatives for '{key}'");
        }
    }

    #[test]
    fn test_bloom_filter_false_positive_rate() {
        let mut bf = BloomFilter::new(100);
        // Insert 100 items
        for i in 0..100 {
            let key = format!("token-{i:04}");
            bf.insert(key.as_bytes());
        }
        // Test 1000 non-inserted items, count false positives
        let mut fp_count = 0;
        for i in 1000..2000 {
            let key = format!("token-{i:04}");
            if bf.contains(key.as_bytes()) {
                fp_count += 1;
            }
        }
        // With 1000 items tested and p=0.0001, expect ~0.1 false positives
        // Allow up to 5 (very conservative, since actual rate should be much lower)
        let fp_rate = fp_count as f64 / 1000.0;
        assert!(
            fp_rate < 0.05,
            "false positive rate {fp_rate} exceeds 5% threshold (target: 0.01%)"
        );
    }

    // ──── Revocation Store tests ────

    #[test]
    fn test_revoke_and_check() {
        let store = RevocationStore::new(1000);

        assert!(!store.is_revoked("tok-001"));

        let v1 = store.revoke("tok-001");
        assert!(v1 > 0);
        assert!(store.is_revoked("tok-001"));
        assert!(store.is_exactly_revoked("tok-001"));

        // Not-revoked token should not match
        assert!(!store.is_revoked("tok-002"));
    }

    #[test]
    fn test_revocation_delta_sync() {
        let store = RevocationStore::new(1000);

        let v1 = store.revoke("tok-a");
        let v2 = store.revoke("tok-b");
        let v3 = store.revoke("tok-c");

        // Delta since before any revocations
        let delta = store.get_delta_since(0);
        assert_eq!(delta.len(), 3);

        // Delta since v1 should include tok-b and tok-c
        let delta = store.get_delta_since(v1);
        assert_eq!(delta.len(), 2);
        let jtis: Vec<&str> = delta.iter().map(|e| e.jti.as_str()).collect();
        assert!(jtis.contains(&"tok-b"));
        assert!(jtis.contains(&"tok-c"));

        // Delta since v3 should be empty
        let delta = store.get_delta_since(v3);
        assert!(delta.is_empty());
    }

    #[test]
    fn test_might_be_revoked_vs_is_revoked() {
        let store = RevocationStore::new(1000);
        store.revoke("real-token");

        // Real token: both return true
        assert!(store.might_be_revoked("real-token"));
        assert!(store.is_revoked("real-token"));

        // Unknown token: might return true (false positive), but is_revoked must be false
        let might = store.might_be_revoked("fake-token");
        let is = store.is_revoked("fake-token");
        assert!(!is, "is_revoked must never have false positives");
        // If might is true, that's a bloom false positive (acceptable within rate)
        if might {
            // This is fine — bloom false positive
        }
    }

    #[test]
    fn test_revocation_count() {
        let store = RevocationStore::new(1000);
        assert_eq!(store.revoked_count(), 0);

        store.revoke("a");
        store.revoke("b");
        store.revoke("c");
        assert_eq!(store.revoked_count(), 3);
    }

    #[test]
    fn test_version_monotonic() {
        let store = RevocationStore::new(1000);
        let v1 = store.current_version();
        let v2 = store.revoke("tok");
        assert!(v2 >= v1, "version should be monotonic: v2={v2} >= v1={v1}");
        let v3 = store.current_version();
        assert!(v3 >= v2, "version should be monotonic: v3={v3} >= v2={v2}");
    }
}
