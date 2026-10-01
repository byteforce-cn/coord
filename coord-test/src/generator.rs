// coord-test: 随机测试数据生成器
//
// 提供随机 Key/Value/Revision 生成器，用于模糊测试和集成测试。

use rand::Rng;

/// 随机数据生成器。
#[derive(Debug, Clone)]
pub struct TestDataGenerator {
    rng: rand::rngs::StdRng,
}

impl TestDataGenerator {
    /// 使用固定种子创建生成器（保证可复现）。
    pub fn new(seed: u64) -> Self {
        use rand::SeedableRng;
        Self {
            rng: rand::rngs::StdRng::seed_from_u64(seed),
        }
    }

    /// 生成随机 Key（格式：`/test/{random_hex}`）。
    pub fn gen_key(&mut self) -> Vec<u8> {
        let id: u64 = self.rng.gen();
        format!("/test/{id:016x}").into_bytes()
    }

    /// 生成带前缀的随机 Key。
    pub fn gen_key_with_prefix(&mut self, prefix: &str) -> Vec<u8> {
        let id: u64 = self.rng.gen();
        format!("{prefix}{id:016x}").into_bytes()
    }

    /// 生成随机 Value（指定长度范围）。
    pub fn gen_value(&mut self, min_len: usize, max_len: usize) -> Vec<u8> {
        let len = self.rng.gen_range(min_len..=max_len);
        let mut value = vec![0u8; len];
        self.rng.fill(&mut value[..]);
        value
    }

    /// 生成随机小 Value（1-256 bytes）。
    pub fn gen_small_value(&mut self) -> Vec<u8> {
        self.gen_value(1, 256)
    }

    /// 生成随机 Revision（1..=1_000_000）。
    pub fn gen_revision(&mut self) -> u64 {
        self.rng.gen_range(1..=1_000_000)
    }

    /// 生成随机 Lease ID（1..=10_000）。
    pub fn gen_lease_id(&mut self) -> i64 {
        self.rng.gen_range(1..=10_000)
    }

    /// 生成随机 TTL（1..=86400 秒）。
    pub fn gen_ttl_seconds(&mut self) -> i64 {
        self.rng.gen_range(1..=86400)
    }

    /// 生成一批随机键值对。
    pub fn gen_kv_pairs(&mut self, count: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|_| (self.gen_key(), self.gen_small_value()))
            .collect()
    }

    /// 生成一批带同前缀的随机键值对。
    pub fn gen_kv_pairs_with_prefix(
        &mut self,
        prefix: &str,
        count: usize,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..count)
            .map(|_| (self.gen_key_with_prefix(prefix), self.gen_small_value()))
            .collect()
    }
}

impl Default for TestDataGenerator {
    /// 使用随机种子创建生成器。
    fn default() -> Self {
        use rand::RngCore;
        let seed = rand::thread_rng().next_u64();
        Self::new(seed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deterministic_generation() {
        let mut g1 = TestDataGenerator::new(42);
        let mut g2 = TestDataGenerator::new(42);

        // 相同种子的生成器应产生相同的序列
        for _ in 0..10 {
            assert_eq!(g1.gen_key(), g2.gen_key());
            assert_eq!(g1.gen_small_value(), g2.gen_small_value());
            assert_eq!(g1.gen_revision(), g2.gen_revision());
        }
    }

    #[test]
    fn test_different_seeds_produce_different_data() {
        let mut g1 = TestDataGenerator::new(1);
        let mut g2 = TestDataGenerator::new(2);

        let key1 = g1.gen_key();
        let key2 = g2.gen_key();
        assert_ne!(key1, key2);
    }

    #[test]
    fn test_gen_key_format() {
        let mut g = TestDataGenerator::new(100);
        let key = g.gen_key();

        // Key 应以 "/test/" 开头
        let key_str = String::from_utf8(key).unwrap();
        assert!(key_str.starts_with("/test/"));
        assert_eq!(key_str.len(), "/test/".len() + 16); // 16 hex chars
    }

    #[test]
    fn test_gen_kv_pairs() {
        let mut g = TestDataGenerator::new(200);
        let pairs = g.gen_kv_pairs(100);

        assert_eq!(pairs.len(), 100);
        // 所有 Key 应唯一（概率极高）
        let mut keys: Vec<_> = pairs.iter().map(|(k, _)| k.clone()).collect();
        keys.sort();
        keys.dedup();
        assert_eq!(keys.len(), 100);
    }
}
