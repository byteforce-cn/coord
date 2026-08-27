// 安全存储层模块（P2-P3）
//
// 包含：
// - key_management: 密钥生命周期管理（Root Key → KEK → DEK）
// - dek_rotation:   DEK 自动轮换（P2-05，默认 90 天）
// - dek_store:      DEK 持久化存储（R-SEC-01：/_meta/dek/ 布局，DekRotationStore 实现）
// - barrier:        Storage Barrier — AES-256-GCM 加密/解密
// - seal:           Seal/Unseal — Shamir 秘密共享封存控制

pub mod barrier;
pub mod dek_rotation;
pub mod dek_store;
pub mod key_management;
pub mod seal;
