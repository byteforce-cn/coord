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

pub mod raft {
    tonic::include_proto!("coord.raft");
}

pub mod auth {
    tonic::include_proto!("coord.auth");
}

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
