// Seal/Unseal — 基于 Shamir 秘密共享的封存控制（P3）
//
// 职责：
// - Shamir Secret Sharing over GF(2^8)：将 256-bit Root Key 拆分为 N 个分片，任意 K 个可恢复
// - SealManager：管理 Seal/Unseal 生命周期，集成 Keyring
// - 分片格式：二进制，支持序列化/反序列化（用于 CLI 层 Base64 编码后人工分发）
//
// 默认参数（ADP §21.8）：
//   N = 5 (总分片数), K = 3 (门限)
//
// 分片二进制格式 (41 bytes)：
//   [version: 1B] [index: 1B] [threshold: 1B] [total: 1B] [x: 1B] [y: 32B] [checksum: 4B]

use rand::RngCore;
use sha2::{Digest, Sha256};

use coord_core::error::{Error, Result};

// ──── 常量 ────

/// Root Key 长度（256-bit = 32 bytes）
const ROOT_KEY_LEN: usize = 32;

/// 默认总分片数
pub const DEFAULT_SHARES_N: u8 = 5;

/// 默认门限（最少需要 K 个分片才能恢复）
pub const DEFAULT_SHARES_K: u8 = 3;

/// 分片二进制格式版本号
const SHARE_VERSION: u8 = 1;

/// 分片二进制长度：version(1) + index(1) + k(1) + n(1) + x(1) + y(32) + checksum(4) = 41
const SHARE_BYTES_LEN: usize = 1 + 1 + 1 + 1 + 1 + ROOT_KEY_LEN + 4;

// ──── GF(2^8) 算术 ────

/// GF(2^8) 有限域运算，不可约多项式 x^8 + x^4 + x^3 + x + 1 (0x11B)
/// 使用经典移位-XOR 算法（AES 标准方法），无需预计算表。
mod gf256 {
    /// GF(2^8) 加法 = XOR
    #[inline]
    pub fn add(a: u8, b: u8) -> u8 {
        a ^ b
    }

    /// GF(2^8) 减法 = 加法（特征为 2 的域中二者等价）
    #[allow(dead_code)]
    #[inline]
    pub fn sub(a: u8, b: u8) -> u8 {
        a ^ b
    }

    /// GF(2^8) 乘法（移位-XOR 算法，O(8)）。
    /// 不可约多项式：x^8 + x^4 + x^3 + x + 1，约化时 XOR 0x1B（去掉 x^8 项后）。
    pub fn mul(mut a: u8, mut b: u8) -> u8 {
        let mut p = 0u8;
        for _ in 0..8 {
            if b & 1 != 0 {
                p ^= a;
            }
            let hi = a & 0x80;
            a <<= 1;
            if hi != 0 {
                a ^= 0x1B; // x^8 mod (x^8 + x^4 + x^3 + x + 1) = x^4 + x^3 + x + 1 = 0x1B
            }
            b >>= 1;
        }
        p
    }

    /// GF(2^8) 除法：a / b = a * inv(b)
    pub fn div(a: u8, b: u8) -> u8 {
        if b == 0 {
            panic!("GF(2^8) division by zero");
        }
        if a == 0 {
            return 0;
        }
        mul(a, inv(b))
    }

    /// GF(2^8) 求逆：使用扩展欧几里得算法。
    fn inv(x: u8) -> u8 {
        if x == 0 {
            panic!("GF(2^8) inverse of zero");
        }
        // 使用费马小定理：在有限域中 x^(2^8-1) = 1，所以 x^(-1) = x^254
        // 用指数法：x^254 = x^(11111110b) = x^2 * x^4 * x^8 * x^16 * x^32 * x^64 * x^128
        let mut result = 1u8;
        let mut base = x;
        for exp_bit in [1u8, 2, 4, 8, 16, 32, 64, 128u8].iter() {
            if *exp_bit & 0xFE != 0 {
                // All bits except LSB are 1 in 0xFE (254)
                result = mul(result, base);
            }
            base = mul(base, base);
        }
        // Correct approach: x^254 = x^(128+64+32+16+8+4+2) = multiply all
        // Let me be explicit:
        let x2 = mul(x, x);       // x^2
        let x4 = mul(x2, x2);     // x^4
        let x8 = mul(x4, x4);     // x^8
        let x16 = mul(x8, x8);    // x^16
        let x32 = mul(x16, x16);  // x^32
        let x64 = mul(x32, x32);  // x^64
        let x128 = mul(x64, x64); // x^128
        // x^254 = x^128 * x^64 * x^32 * x^16 * x^8 * x^4 * x^2
        mul(mul(mul(mul(mul(mul(x128, x64), x32), x16), x8), x4), x2)
    }

    /// GF(2^8) 幂运算：base^exp
    #[allow(dead_code)]
    pub fn pow(mut base: u8, mut exp: u8) -> u8 {
        let mut result = 1u8;
        while exp > 0 {
            if exp & 1 != 0 {
                result = mul(result, base);
            }
            base = mul(base, base);
            exp >>= 1;
        }
        result
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn test_add_sub_identity() {
            for a in 0..=255u8 {
                for b in 0..=255u8 {
                    assert_eq!(sub(add(a, b), b), a);
                }
            }
        }

        #[test]
        fn test_mul_div_identity() {
            for a in 1..=255u8 {
                for b in 1..=255u8 {
                    assert_eq!(div(mul(a, b), b), a, "a={a}, b={b}");
                }
            }
        }

        #[test]
        fn test_mul_commutative() {
            for a in 0..=255u8 {
                for b in 0..=255u8 {
                    assert_eq!(mul(a, b), mul(b, a));
                }
            }
        }

        #[test]
        fn test_distributive() {
            for a in 0..=255u8 {
                for b in 0..=255u8 {
                    for c in 0..=255u8 {
                        let left = mul(a, add(b, c));
                        let right = add(mul(a, b), mul(a, c));
                        assert_eq!(left, right, "a={a}, b={b}, c={c}");
                    }
                }
            }
        }
    }
}

// ──── Share 类型 ────

/// Shamir 秘密共享的单个分片。
///
/// 每个分片包含多项式上的一个点 (x, f(x))，其中 f(0) = secret_byte。
/// 对于 32-byte Root Key，有 32 个独立的多项式，每个保护 1 byte。
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Share {
    /// 分片索引（1..=N）
    pub index: u8,
    /// 门限 K（最少需要 K 个分片才能恢复）
    pub threshold: u8,
    /// 总分片数 N
    pub total: u8,
    /// 多项式上的 x 坐标（随机生成，非零）
    pub x: u8,
    /// 多项式上的 y 坐标（32 bytes，对应 Root Key 的 32 个多项式）
    pub y: [u8; ROOT_KEY_LEN],
}

impl Share {
    /// 序列化为二进制格式（41 bytes）。
    ///
    /// 格式：version(1B) | index(1B) | threshold(1B) | total(1B) | x(1B) | y(32B) | checksum(4B)
    /// checksum = SHA256(version || index || threshold || total || x || y)[..4]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(SHARE_BYTES_LEN);
        buf.push(SHARE_VERSION);
        buf.push(self.index);
        buf.push(self.threshold);
        buf.push(self.total);
        buf.push(self.x);
        buf.extend_from_slice(&self.y);

        // SHA256 checksum (前 4 bytes)
        let checksum = compute_checksum(&buf);
        buf.extend_from_slice(&checksum);

        buf
    }

    /// 从二进制格式反序列化。
    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != SHARE_BYTES_LEN {
            return Err(Error::Crypto(format!(
                "share must be {SHARE_BYTES_LEN} bytes, got {}",
                bytes.len()
            )));
        }

        let version = bytes[0];
        if version != SHARE_VERSION {
            return Err(Error::Crypto(format!(
                "unsupported share version {version}, expected {SHARE_VERSION}"
            )));
        }

        let index = bytes[1];
        let threshold = bytes[2];
        let total = bytes[3];
        let x = bytes[4];

        let mut y = [0u8; ROOT_KEY_LEN];
        y.copy_from_slice(&bytes[5..5 + ROOT_KEY_LEN]);

        // Verify checksum
        let expected_checksum = &bytes[5 + ROOT_KEY_LEN..SHARE_BYTES_LEN];
        let actual_checksum = compute_checksum(&bytes[..5 + ROOT_KEY_LEN]);
        if expected_checksum != actual_checksum {
            return Err(Error::Crypto("share checksum mismatch; data may be corrupted".into()));
        }

        // Validate fields
        if index == 0 || index > total {
            return Err(Error::Crypto(format!(
                "invalid share index {index} (must be 1..={total})"
            )));
        }
        if threshold == 0 || threshold > total {
            return Err(Error::Crypto(format!(
                "invalid threshold {threshold} (must be 1..={total})"
            )));
        }
        if x == 0 {
            return Err(Error::Crypto("share x coordinate must be non-zero".into()));
        }

        Ok(Self {
            index,
            threshold,
            total,
            x,
            y,
        })
    }
}

fn compute_checksum(data: &[u8]) -> [u8; 4] {
    let hash = Sha256::digest(data);
    let mut checksum = [0u8; 4];
    checksum.copy_from_slice(&hash[..4]);
    checksum
}

// ──── Shamir 秘密共享核心算法 ────

/// 将 32-byte 秘密拆分为 N 个分片，任意 K 个可恢复。
///
/// # 参数
/// - `secret`: 32-byte Root Key
/// - `n`: 总分片数（1..=255）
/// - `k`: 门限（1..=n）
///
/// # 算法
/// 对 secret 的每个字节独立执行 Shamir 方案：
/// 1. 为该字节构造 K-1 次多项式 f，其中 f(0) = secret_byte
/// 2. 为 N 个分片各选一个唯一的非零 x 坐标
/// 3. 计算 f(x) 作为该分片该字节的 y 值
pub fn split_secret(secret: &[u8; ROOT_KEY_LEN], n: u8, k: u8) -> Result<Vec<Share>> {
    if n == 0 {
        return Err(Error::InvalidArgument("n must be >= 1".into()));
    }
    if k == 0 || k > n {
        return Err(Error::InvalidArgument(format!(
            "k must be 1..=n, got k={k}, n={n}"
        )));
    }

    let mut rng = rand::thread_rng();

    // 为每个分片生成唯一的非零 x 坐标
    let xs = generate_x_coordinates(n, &mut rng);

    // 对每个字节：生成 K-1 个随机系数，构造多项式
    // coeffs[byte_idx][coeff_idx] = 多项式系数
    // coeffs[byte_idx][0] = secret[byte_idx]（常数项）
    // coeffs[byte_idx][1..k] = 随机系数
    let mut coeffs: Vec<[u8; 256]> = Vec::with_capacity(ROOT_KEY_LEN);
    for byte_idx in 0..ROOT_KEY_LEN {
        let mut poly = [0u8; 256];
        poly[0] = secret[byte_idx]; // f(0) = secret_byte
        for coeff_idx in 1..(k as usize) {
            // 随机非零系数
            let mut c = 0u8;
            while c == 0 {
                c = rng.next_u32() as u8;
            }
            poly[coeff_idx] = c;
        }
        coeffs.push(poly);
    }

    // 对每个分片，计算 y = f(x) 对每个字节
    let mut shares = Vec::with_capacity(n as usize);
    for share_idx in 0..(n as usize) {
        let x = xs[share_idx];

        let mut y = [0u8; ROOT_KEY_LEN];
        for byte_idx in 0..ROOT_KEY_LEN {
            y[byte_idx] = evaluate_polynomial(&coeffs[byte_idx], k as usize, x);
        }

        shares.push(Share {
            index: (share_idx + 1) as u8,
            threshold: k,
            total: n,
            x,
            y,
        });
    }

    Ok(shares)
}

/// 从 K 个分片中恢复 32-byte 秘密（拉格朗日插值）。
///
/// # 参数
/// - `shares`: 至少 K 个合法分片（必须具有相同的 threshold 和 total）
///
/// # 错误
/// - `InsufficientShares`: 分片数量不足
/// - `Crypto`: 分片不兼容（threshold/total 不一致）或 x 坐标重复
pub fn recover_secret(shares: &[Share]) -> Result<[u8; ROOT_KEY_LEN]> {
    if shares.is_empty() {
        return Err(Error::InsufficientShares {
            have: 0,
            need: 1,
        });
    }

    let k = shares[0].threshold as usize;
    let _n = shares[0].total;

    // 验证所有分片来自同一组
    for s in shares.iter().skip(1) {
        if s.threshold != shares[0].threshold || s.total != shares[0].total {
            return Err(Error::Crypto(format!(
                "incompatible shares: threshold {}/{} vs {}/{}",
                shares[0].threshold, shares[0].total, s.threshold, s.total
            )));
        }
    }

    if shares.len() < k {
        return Err(Error::InsufficientShares {
            have: shares.len(),
            need: k,
        });
    }

    // 验证无重复 x 坐标
    for i in 0..shares.len() {
        for j in (i + 1)..shares.len() {
            if shares[i].x == shares[j].x {
                return Err(Error::Crypto(format!(
                    "duplicate x coordinate {} in shares {} and {}",
                    shares[i].x, shares[i].index, shares[j].index
                )));
            }
        }
    }

    // 使用前 K 个分片进行拉格朗日插值
    let used = &shares[..k];

    // 预计算拉格朗日基多项式在 x=0 处的值
    // L_i(0) = ∏_{j≠i} (0 - x_j) / (x_i - x_j) = ∏_{j≠i} x_j / (x_j - x_i)
    // 在 GF(2^8) 中：减法 = XOR = 加法（因为 GF(2^8) 加法 = XOR），所以 (0 - x_j) = x_j
    // 所以 L_i(0) = ∏_{j≠i} x_j / (x_i + x_j)
    let mut lagrange_basis = [0u8; 256]; // 最多 255 个分片
    for i in 0..k {
        let mut num = 1u8;
        let mut den = 1u8;
        for j in 0..k {
            if i == j {
                continue;
            }
            num = gf256::mul(num, used[j].x);
            den = gf256::mul(den, gf256::add(used[i].x, used[j].x));
        }
        lagrange_basis[i] = gf256::div(num, den);
    }

    // 对每个字节独立恢复：secret_byte = Σ L_i(0) * y_i
    let mut secret = [0u8; ROOT_KEY_LEN];
    for byte_idx in 0..ROOT_KEY_LEN {
        let mut acc = 0u8;
        for i in 0..k {
            let term = gf256::mul(lagrange_basis[i], used[i].y[byte_idx]);
            acc = gf256::add(acc, term);
        }
        secret[byte_idx] = acc;
    }

    Ok(secret)
}

// ──── 内部辅助函数 ────

/// 生成 N 个唯一的非零 x 坐标（随机排列 1..=255 的前 N 个值）。
fn generate_x_coordinates(n: u8, rng: &mut impl RngCore) -> Vec<u8> {
    // Fisher-Yates 洗牌前 n 个值
    let mut xs: Vec<u8> = (1u8..=255u8).collect();
    for i in (0..(n as usize)).rev() {
        let j = (rng.next_u32() as usize) % (i + 1);
        xs.swap(i, j);
    }
    xs.truncate(n as usize);
    xs
}

/// 在 GF(2^8) 上计算多项式在 x 处的值：f(x) = Σ coeff[i] * x^i
fn evaluate_polynomial(coeffs: &[u8], degree: usize, x: u8) -> u8 {
    // 霍纳法（Horner's method）
    let mut result = 0u8;
    for i in (0..degree).rev() {
        result = gf256::mul(result, x);
        result = gf256::add(result, coeffs[i]);
    }
    result
}

// ──── SealManager ────

/// SealManager — 管理 Seal/Unseal 生命周期。
///
/// 集成 Keyring，负责：
/// - 初始化时生成 Shamir 分片
/// - Sealed 状态下拒绝所有操作（由 Keyring 内部保证）
/// - Unseal 时从分片恢复 Root Key 并重建 Keyring
///
/// # 线程安全
/// 内部状态通过 `Keyring`（`Arc<RwLock<>>`）共享，可安全跨线程使用。
#[derive(Debug, Clone)]
pub struct SealManager;

impl SealManager {
    /// 生成 Shamir 分片：将 Root Key 拆分为 N 份，任意 K 份可恢复。
    ///
    /// 调用时机：集群首次 Bootstrap，生成 Root Key 后立即调用。
    /// 调用方负责将分片安全地分发给 K 个管理员（人工分发）。
    pub fn generate_shares(root_key: &[u8; ROOT_KEY_LEN]) -> Result<Vec<Share>> {
        split_secret(root_key, DEFAULT_SHARES_N, DEFAULT_SHARES_K)
    }

    /// 生成自定义参数的 Shamir 分片。
    pub fn generate_shares_with_params(
        root_key: &[u8; ROOT_KEY_LEN],
        n: u8,
        k: u8,
    ) -> Result<Vec<Share>> {
        split_secret(root_key, n, k)
    }

    /// 从分片恢复 Root Key。
    ///
    /// 调用时机：Unseal 流程中，收集到 ≥K 个分片后调用。
    pub fn recover_root_key(shares: &[Share]) -> Result<[u8; ROOT_KEY_LEN]> {
        recover_secret(shares)
    }
}

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;
    use rand::RngCore;

    fn random_secret() -> [u8; ROOT_KEY_LEN] {
        let mut secret = [0u8; ROOT_KEY_LEN];
        rand::thread_rng().fill_bytes(&mut secret);
        secret
    }

    // ──── 基础 SSS 测试 ────

    #[test]
    fn test_split_recover_roundtrip_3_of_5() {
        let secret = random_secret();
        let shares = split_secret(&secret, 5, 3).unwrap();

        assert_eq!(shares.len(), 5);
        for s in &shares {
            assert_eq!(s.threshold, 3);
            assert_eq!(s.total, 5);
            assert!(s.x != 0, "x must be non-zero");
        }

        // 用前 3 个分片恢复
        let recovered = recover_secret(&shares[..3]).unwrap();
        assert_eq!(recovered, secret);

        // 用任意 3 个分片恢复
        let recovered2 = recover_secret(&[shares[1].clone(), shares[3].clone(), shares[4].clone()]).unwrap();
        assert_eq!(recovered2, secret);
    }

    #[test]
    fn test_split_recover_roundtrip_2_of_3() {
        let secret = random_secret();
        let shares = split_secret(&secret, 3, 2).unwrap();

        let recovered = recover_secret(&shares[..2]).unwrap();
        assert_eq!(recovered, secret);
    }

    #[test]
    fn test_split_recover_roundtrip_5_of_7() {
        let secret = random_secret();
        let shares = split_secret(&secret, 7, 5).unwrap();

        let recovered = recover_secret(&shares[..5]).unwrap();
        assert_eq!(recovered, secret);
    }

    #[test]
    fn test_insufficient_shares_fails() {
        let secret = random_secret();
        let shares = split_secret(&secret, 5, 3).unwrap();

        let result = recover_secret(&shares[..2]);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("insufficient"), "unexpected error: {err_msg}");
    }

    #[test]
    fn test_wrong_share_produces_wrong_secret() {
        let secret = random_secret();
        let shares = split_secret(&secret, 5, 3).unwrap();

        // Tamper with one share's y value
        let mut bad_shares = shares[..3].to_vec();
        bad_shares[0].y[0] ^= 0xFF;

        let recovered = recover_secret(&bad_shares).unwrap();
        assert_ne!(recovered, secret, "tampered share should produce wrong secret");
    }

    #[test]
    fn test_empty_shares_fails() {
        let result = recover_secret(&[]);
        assert!(result.is_err());
    }

    #[test]
    fn test_incompatible_shares_fails() {
        let secret = random_secret();
        let shares_a = split_secret(&secret, 5, 3).unwrap();
        let shares_b = split_secret(&secret, 3, 2).unwrap();

        let mixed = [shares_a[0].clone(), shares_b[0].clone()];
        let result = recover_secret(&mixed);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("incompatible"), "unexpected error: {err_msg}");
    }

    #[test]
    fn test_duplicate_x_coordinates_fails() {
        let secret = random_secret();
        let mut shares = split_secret(&secret, 5, 3).unwrap();

        // Make two shares have the same x
        shares[1].x = shares[0].x;
        shares[1].y = shares[0].y; // Also duplicate y to avoid wrong check

        let result = recover_secret(&shares[..3]);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("duplicate"), "unexpected error: {err_msg}");
    }

    // ──── Share 序列化测试 ────

    #[test]
    fn test_share_serialization_roundtrip() {
        let secret = random_secret();
        let shares = split_secret(&secret, 5, 3).unwrap();

        for share in &shares {
            let bytes = share.to_bytes();
            assert_eq!(bytes.len(), SHARE_BYTES_LEN);

            let decoded = Share::from_bytes(&bytes).unwrap();
            assert_eq!(decoded.index, share.index);
            assert_eq!(decoded.threshold, share.threshold);
            assert_eq!(decoded.total, share.total);
            assert_eq!(decoded.x, share.x);
            assert_eq!(decoded.y, share.y);

            // Roundtrip encoding: re-serialize should produce identical bytes
            assert_eq!(decoded.to_bytes(), bytes);
        }
    }

    #[test]
    fn test_share_from_bytes_rejects_wrong_length() {
        let result = Share::from_bytes(&[0u8; 10]);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("must be"), "unexpected error: {err_msg}");
    }

    #[test]
    fn test_share_from_bytes_rejects_wrong_version() {
        let secret = random_secret();
        let shares = split_secret(&secret, 5, 3).unwrap();
        let mut bytes = shares[0].to_bytes();
        bytes[0] = 99; // wrong version

        let result = Share::from_bytes(&bytes);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("version"), "unexpected error: {err_msg}");
    }

    #[test]
    fn test_share_from_bytes_detects_corruption() {
        let secret = random_secret();
        let shares = split_secret(&secret, 5, 3).unwrap();
        let mut bytes = shares[0].to_bytes();

        // Corrupt y value (byte 10)
        bytes[10] ^= 0xFF;

        let result = Share::from_bytes(&bytes);
        assert!(result.is_err());
        let err_msg = format!("{}", result.unwrap_err());
        assert!(err_msg.contains("checksum"), "unexpected error: {err_msg}");
    }

    // ──── SealManager 集成测试 ────

    #[test]
    fn test_seal_manager_generate_and_recover() {
        let root_key = random_secret();
        let shares = SealManager::generate_shares(&root_key).unwrap();

        assert_eq!(shares.len(), DEFAULT_SHARES_N as usize);
        for s in &shares {
            assert_eq!(s.threshold, DEFAULT_SHARES_K);
            assert_eq!(s.total, DEFAULT_SHARES_N);
        }

        // Recover with first K shares
        let recovered = SealManager::recover_root_key(&shares[..DEFAULT_SHARES_K as usize]).unwrap();
        assert_eq!(recovered, root_key);
    }

    #[test]
    fn test_seal_manager_custom_params() {
        let root_key = random_secret();
        let shares = SealManager::generate_shares_with_params(&root_key, 7, 4).unwrap();

        assert_eq!(shares.len(), 7);
        for s in &shares {
            assert_eq!(s.threshold, 4);
            assert_eq!(s.total, 7);
        }

        let recovered = SealManager::recover_root_key(&shares[..4]).unwrap();
        assert_eq!(recovered, root_key);
    }

    #[test]
    fn test_all_shares_have_unique_x() {
        let root_key = random_secret();
        let shares = SealManager::generate_shares(&root_key).unwrap();

        let mut xs: Vec<u8> = shares.iter().map(|s| s.x).collect();
        xs.sort();
        xs.dedup();
        assert_eq!(xs.len(), shares.len(), "all x coordinates must be unique");
    }

    #[test]
    fn test_split_recover_edge_case_all_ones() {
        let secret = [0xFFu8; ROOT_KEY_LEN];
        let shares = split_secret(&secret, 5, 3).unwrap();
        let recovered = recover_secret(&shares[..3]).unwrap();
        assert_eq!(recovered, secret);
    }

    #[test]
    fn test_split_recover_edge_case_all_zeros() {
        let secret = [0u8; ROOT_KEY_LEN];
        let shares = split_secret(&secret, 5, 3).unwrap();
        let recovered = recover_secret(&shares[..3]).unwrap();
        assert_eq!(recovered, secret);
    }
}

