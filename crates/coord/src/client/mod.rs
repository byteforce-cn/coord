//! Client 代理模式子模块。
//!
//! 提供三项能力：
//! 1. AP 服务发现缓存（dashmap + TTL）
//! 2. Gossip 成员管理（chitchat UDP 协议）
//! 3. CP 操作透传（gRPC → coord-server leader）

pub(crate) mod agent;
pub(crate) mod gossip;
pub(crate) mod health;
pub(crate) mod proxy;
