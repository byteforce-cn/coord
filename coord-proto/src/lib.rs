// coord-proto: Protobuf/gRPC 契约定义
//
// 本 Crate 通过 tonic-build + prost-build 从 .proto 文件生成 gRPC 存根。
// 所有生成代码在 build.rs 中配置，编译后位于 OUT_DIR，在此重新导出。

pub mod kv {
    tonic::include_proto!("coord.kv");
}

pub mod txn {
    tonic::include_proto!("coord.txn");
}

pub mod lease {
    tonic::include_proto!("coord.lease");
}

pub mod watch {
    tonic::include_proto!("coord.watch");
}

pub mod maintenance {
    tonic::include_proto!("coord.maintenance");
}

pub mod storage {
    tonic::include_proto!("coord.storage");
}

pub mod raft {
    tonic::include_proto!("coord.raft");
}

pub mod auth {
    tonic::include_proto!("coord.auth");
}

pub mod capability {
    tonic::include_proto!("coord.capability");
}

// ──── 契约面（contracts/v1.2.0）────
//
// 每个 domain 一个模块，与 `apis/contracts/proto/coord/<domain>/v1/<domain>.proto`
// 的 package 逐字对应：`package coord.<domain>.v1` ⇒ `crate::<domain>::v1`。
// 生成源是 `coord-proto/src/proto/<domain>.proto`（实现副本），其内容与契约
// 副本逐字一致，由 `apis/contracts/scripts/check-wire-sync.sh` 双向卡口保证。

pub mod registry {
    pub mod v1 {
        tonic::include_proto!("coord.registry.v1");
    }
}

pub mod lock {
    pub mod v1 {
        tonic::include_proto!("coord.lock.v1");
    }
}

pub mod election {
    pub mod v1 {
        tonic::include_proto!("coord.election.v1");
    }
}

pub mod idgen {
    pub mod v1 {
        tonic::include_proto!("coord.idgen.v1");
    }
}

pub mod event {
    pub mod v1 {
        tonic::include_proto!("coord.event.v1");
    }
}

pub mod config {
    pub mod v1 {
        tonic::include_proto!("coord.config.v1");
    }
}

pub mod pki {
    pub mod v1 {
        tonic::include_proto!("coord.pki.v1");
    }
}

pub mod policy {
    pub mod v1 {
        tonic::include_proto!("coord.policy.v1");
    }
}

pub mod circuitbreaker {
    pub mod v1 {
        tonic::include_proto!("coord.circuitbreaker.v1");
    }
}

pub mod ratelimiter {
    pub mod v1 {
        tonic::include_proto!("coord.ratelimiter.v1");
    }
}

pub mod transit {
    pub mod v1 {
        tonic::include_proto!("coord.transit.v1");
    }
}

pub mod cache {
    pub mod v1 {
        tonic::include_proto!("coord.cache.v1");
    }
}

pub mod mq {
    pub mod v1 {
        tonic::include_proto!("coord.mq.v1");
    }
}

pub mod workflow {
    pub mod v1 {
        tonic::include_proto!("coord.workflow.v1");
    }
}

pub mod scheduler {
    pub mod v1 {
        tonic::include_proto!("coord.scheduler.v1");
    }
}

pub mod featureflags {
    pub mod v1 {
        tonic::include_proto!("coord.featureflags.v1");
    }
}

/// 内部面：`coord.agent` —— 只保留 Handshake / Health / Replica
/// （协议协商 / 探活 / ISR 复制通道），三者均不建对外契约包。
///
/// 同时提供**兼容重导出**：迁移（contracts/v1.2.0）把 16 个服务从 `coord.agent`
/// 拆到各自契约包后，既有 65 处 `coord_proto::agent::*` 引用无需同时改动。
/// 新代码应直接使用 `coord_proto::<domain>::v1::*`；本兼容面随 v0.3.0 收敛。
pub mod agent {
    tonic::include_proto!("coord.agent");

    pub use crate::cache::v1::*;
    pub use crate::circuitbreaker::v1::*;
    pub use crate::config::v1::*;
    pub use crate::election::v1::*;
    pub use crate::event::v1::*;
    pub use crate::featureflags::v1::*;
    pub use crate::idgen::v1::*;
    pub use crate::lock::v1::*;
    pub use crate::mq::v1::*;
    pub use crate::pki::v1::*;
    pub use crate::policy::v1::*;
    pub use crate::ratelimiter::v1::*;
    pub use crate::registry::v1::*;
    pub use crate::scheduler::v1::*;
    pub use crate::transit::v1::*;
    pub use crate::workflow::v1::*;
}

pub mod plugin {
    tonic::include_proto!("coord.plugin");
}

/// Encoded file descriptor set for gRPC Server Reflection.
/// Generated at compile time by `build.rs`.
pub const FILE_DESCRIPTOR_SET: &[u8] =
    include_bytes!(concat!(env!("OUT_DIR"), "/coord_descriptor.bin"));

// ──── 测试 ────

#[cfg(test)]
mod tests {
    use super::*;

    // ──── KV proto ────

    #[test]
    fn test_kv_put_request_default() {
        let req = kv::PutRequest::default();
        assert!(req.key.is_empty());
        assert!(req.value.is_empty());
    }

    #[test]
    fn test_kv_put_request_with_data() {
        let req = kv::PutRequest {
            key: b"hello".to_vec(),
            value: b"world".to_vec(),
            lease_id: 42,
            prev_kv: false,
            request_id: vec![],
        };
        assert_eq!(req.key, b"hello");
        assert_eq!(req.value, b"world");
        assert_eq!(req.lease_id, 42);
    }

    #[test]
    fn test_kv_range_request_default() {
        let req = kv::RangeRequest::default();
        assert!(req.key.is_empty());
        assert_eq!(req.limit, 0);
    }

    #[test]
    fn test_kv_delete_request_default() {
        let req = kv::DeleteRequest::default();
        assert!(req.key.is_empty());
    }

    // ──── Txn proto ────

    #[test]
    fn test_txn_request_default() {
        let req = txn::TxnRequest::default();
        assert!(req.compare.is_empty());
        assert!(req.success.is_empty());
        assert!(req.failure.is_empty());
    }

    #[test]
    fn test_txn_compare_values() {
        // Verify compare result enum values
        assert_eq!(txn::compare::CompareResult::Equal as i32, 0);
        assert_eq!(txn::compare::CompareResult::Greater as i32, 1);
        assert_eq!(txn::compare::CompareResult::Less as i32, 2);
        assert_eq!(txn::compare::CompareResult::NotEqual as i32, 3);
    }

    // ──── Lease proto ────

    #[test]
    fn test_lease_grant_request() {
        let req = lease::LeaseGrantRequest { id: 0, ttl: 30 };
        assert_eq!(req.ttl, 30);
    }

    // ──── Watch proto ────

    #[test]
    fn test_watch_request_default() {
        let req = watch::WatchRequest::default();
        assert!(req.request.is_none());
    }

    // ──── Maintenance proto ────

    #[test]
    fn test_status_request_default() {
        let req = maintenance::StatusRequest::default();
        // StatusRequest has no fields, this just verifies it compiles
        let _ = req;
    }

    // ──── Auth proto ────

    #[test]
    fn test_auth_enable_request_default() {
        let req = auth::AuthEnableRequest::default();
        let _ = req;
    }

    #[test]
    fn test_authenticate_request() {
        let req = auth::AuthenticateRequest {
            name: "alice".to_string(),
            password: "secret".to_string(),
        };
        assert_eq!(req.name, "alice");
        assert_eq!(req.password, "secret");
    }

    // ──── Raft proto ────

    #[test]
    fn test_raft_message_default() {
        let msg = raft::RaftMessage::default();
        assert!(msg.payload.is_empty());
    }
}
