// Watch handler 侧 scope 判定（W1-6）的 handler 级正反双测。
//
// 为什么这些用例必须在 **handler 级**跑（而不是只测判定函数）：
//
// * 判定函数（`coord_core::grpc_auth::check_deferred_scope`）算得对，不等于 handler
//   **调了它**。少了那一行调用，行为就是"有约束的订阅被放行、无人判定"（fail-open），
//   而只测判定函数的测试仍然全绿。
// * 反过来，多调一次 body 缓存就会复现第四轮 P0：请求永久挂起 —— 那种缺陷在
//   handler 级表现为"永远拿不到响应"，本文件的用例至少能覆盖到"拒绝/放行"两条路径
//   都确实返回。
//
// 本模块用真实的 `tonic::Streaming`（由 gRPC 帧字节构造）喂给 `WatchProxy::watch`，
// 因此走的是生产同一条解码路径（ProstDecoder + 5 字节帧头）。
#[cfg(test)]
mod watch_scope_tests {
    use crate::proxy::WatchProxy;

    use bytes::Bytes;
    use coord_core::grpc_auth::{watch_create_access, DeferredScopeGrants};
    use coord_proto::watch::watch_server::Watch;
    use coord_proto::watch::WatchRequest;
    use http_body_util::Full;
    use prost::Message;
    use tonic_prost::ProstDecoder;
    /// 把若干 `WatchRequest` 编成 gRPC 帧流（1 字节压缩标志 + 4 字节大端长度 + 消息）。
    fn streaming_watch(msgs: Vec<WatchRequest>) -> tonic::Streaming<WatchRequest> {
        let mut buf = Vec::new();
        for m in msgs {
            let body = m.encode_to_vec();
            buf.push(0u8);
            buf.extend_from_slice(&(body.len() as u32).to_be_bytes());
            buf.extend_from_slice(&body);
        }
        tonic::Streaming::new_request(
            ProstDecoder::<WatchRequest>::default(),
            Full::new(Bytes::from(buf)),
            None,
            None,
        )
    }

    fn create(key: &[u8], range_end: &[u8]) -> WatchRequest {
        WatchRequest {
            request: Some(coord_proto::watch::watch_request::Request::Create(
                coord_proto::watch::WatchCreateRequest {
                    key: key.to_vec(),
                    range_end: range_end.to_vec(),
                    ..Default::default()
                },
            )),
        }
    }

    fn scoped_grants() -> DeferredScopeGrants {
        DeferredScopeGrants {
            capability_id: "data:watch:subscribe".to_string(),
            grant_scopes: vec!["/app/counter/".to_string()],
        }
    }

    fn request_with_grants(
        msgs: Vec<WatchRequest>,
        grants: Option<DeferredScopeGrants>,
    ) -> tonic::Request<tonic::Streaming<WatchRequest>> {
        let mut req = tonic::Request::new(streaming_watch(msgs));
        if let Some(g) = grants {
            req.extensions_mut().insert(g);
        }
        req
    }

    /// 反：范围**外**的前缀订阅必须被拒，且错误码是 PERMISSION_DENIED（不是 UNAUTHENTICATED）。
    ///
    /// `inner = None`（骨架模式）让本用例能证明拒绝发生在**任何上游订阅之前**：
    /// 骨架模式本会返回一条空流（`Ok`），若这里拿到 `Ok` 就说明判定没生效。
    #[tokio::test]
    async fn watch_handler_denies_out_of_scope_prefix() {
        let proxy = WatchProxy::new(None);
        let req = request_with_grants(vec![create(b"/other/secret", b"")], Some(scoped_grants()));

        let status = proxy
            .watch(req)
            .await
            .err()
            .expect("范围外的订阅必须被拒绝（骨架模式的空流不等于放行）");
        assert_eq!(
            status.code(),
            tonic::Code::PermissionDenied,
            "身份有效但权限不足 ⇒ PERMISSION_DENIED；实际消息：{}",
            status.message()
        );
        assert_eq!(
            coord_core::error_code::error_code_of(&status).as_deref(),
            Some("PERMISSION_DENIED"),
            "拒绝必须带结构化错误码（Java 侧据此决定不可重试）"
        );
        assert!(
            status.message().contains("data:watch:subscribe"),
            "拒绝原因必须可归因到能力，实际：{}",
            status.message()
        );
    }

    /// 正：范围**内**的前缀订阅必须放行（这是 W1-6 修掉的功能损失）。
    ///
    /// 订阅 key 与 scope 同为 `/app/counter/` 前缀 —— 区间
    /// `["/app/counter/", "/app/counter0")` 整体落在 scope 的字节前缀内。
    #[tokio::test]
    async fn watch_handler_allows_in_scope_prefix() {
        let proxy = WatchProxy::new(None);
        let req = request_with_grants(vec![create(b"/app/counter/", b"")], Some(scoped_grants()));
        assert!(
            proxy.watch(req).await.is_ok(),
            "范围内的订阅必须放行（否则有约束角色仍然用不了 Watch）"
        );
    }

    /// 边界（最容易被"看起来对"的实现漏掉的一条）：无尾斜杠的**兄弟前缀**不得被放行。
    ///
    /// 订阅 `key="/app/counter"`（无尾斜杠）按投递侧语义是**字节前缀订阅**，会覆盖
    /// `/app/counters...`、`/app/counter-/...` 等 key；而 scope `/app/counter/`
    /// 只覆盖 `/app/counter/...`。若判定按"单键/裸 starts_with"来写，就会把这类订阅放行
    /// —— 判定与实际投递集合于是分叉。本用例与 `watch_create_access` 的等价性测试
    /// （`coord-core`）一起把这条钉住。
    #[tokio::test]
    async fn watch_handler_denies_sibling_prefix_not_covered_by_scope() {
        let proxy = WatchProxy::new(None);
        let req = request_with_grants(vec![create(b"/app/counter", b"")], Some(scoped_grants()));
        let status = proxy.watch(req).await.err().expect(
            "`/app/counter`（无尾斜杠）会覆盖 /app/counters*，scope `/app/counter/` 不得放行",
        );
        assert_eq!(status.code(), tonic::Code::PermissionDenied);
    }

    /// 无快照 = 不施加约束（鉴权关闭 / root / 未经鉴权层）：不得误伤 watch 的可用性。
    #[tokio::test]
    async fn watch_handler_allows_when_no_grant_snapshot() {
        let proxy = WatchProxy::new(None);
        let req = request_with_grants(vec![create(b"/anything", b"")], None);
        assert!(
            proxy.watch(req).await.is_ok(),
            "无授权快照 ⇒ 不施加 scope 约束（与鉴权关闭/root 同口径）"
        );
    }

    /// 判定用的是**与投递侧同源**的区间：拒绝的依据是"**实际会投递的 key 集合**"而不是
    /// "请求里字面写了什么"。
    ///
    /// 两种容易写反的情形都在这里钉住：
    ///
    /// * `key="/app/"` + `range_end="/app/counter0"`：实际集合是
    ///   `starts_with("/app/") ∩ (-∞, "/app/counter0")`，**含** `/app/aaa` 等
    ///   scope 之外的 key ⇒ 必须拒绝（`range_end` 收窄不掩掉 key 前缀越界）。
    /// * `key="/app/counter/"` + `range_end="/app/zzz"`：上界被**前缀后继**收窄为
    ///   `/app/counter0`（投递侧语义），实际集合完全在 scope 内 ⇒ 放行。
    ///   若实现把 `range_end` 当成裸上界（`[key, range_end)` 的"理想化"写法），
    ///   就会把这条误判为越界 —— 那是与 `key_matches` 分叉的第二种语义。
    #[tokio::test]
    async fn watch_handler_denies_range_whose_delivered_set_exceeds_scope() {
        let proxy = WatchProxy::new(None);

        // ① 实际投递集合越过 scope 的 key 前缀 ⇒ 拒绝
        let wide_key = request_with_grants(
            vec![create(b"/app/", b"/app/counter0")],
            Some(scoped_grants()),
        );
        let status = proxy
            .watch(wide_key)
            .await
            .err()
            .expect("`/app/` 前缀会投递 /app/aaa 等 scope 外的 key ⇒ 必须拒绝");
        assert_eq!(status.code(), tonic::Code::PermissionDenied);

        // ② 字面 range_end 越出 scope，但投递集合被前缀收窄 ⇒ 放行
        let clamped = request_with_grants(
            vec![create(b"/app/counter/", b"/app/zzz")],
            Some(scoped_grants()),
        );
        assert!(
            proxy.watch(clamped).await.is_ok(),
            "range_end 超过前缀后继时会被收窄（投递侧语义），不得按裸区间误判为越界"
        );

        // ③ 区间整体落在 scope 内 ⇒ 放行
        let inside = request_with_grants(
            vec![create(b"/app/counter/", b"/app/counter/a")],
            Some(scoped_grants()),
        );
        assert!(proxy.watch(inside).await.is_ok());
    }

    /// 无约束快照（`""`）⇒ 任意前缀放行：这是"有授权、无 scope 限制"的正常角色。
    #[tokio::test]
    async fn watch_handler_allows_unrestricted_grant() {
        let proxy = WatchProxy::new(None);
        let unrestricted = DeferredScopeGrants {
            capability_id: "data:watch:subscribe".to_string(),
            grant_scopes: vec![String::new()],
        };
        let req = request_with_grants(vec![create(b"/anywhere", b"")], Some(unrestricted));
        assert!(proxy.watch(req).await.is_ok());
    }

    /// 首帧不是 Create ⇒ INVALID_ARGUMENT（与 scope 无关的既有契约，顺手钉住，
    /// 以便确认上面的判定没有把解码顺序改坏）。
    #[tokio::test]
    async fn watch_handler_rejects_non_create_first_frame() {
        let proxy = WatchProxy::new(None);
        let bad = WatchRequest { request: None };
        let req = request_with_grants(vec![bad], None);
        let status = proxy
            .watch(req)
            .await
            .err()
            .expect("首帧非 Create 必须拒绝");
        assert_eq!(status.code(), tonic::Code::InvalidArgument);
    }

    /// 判定函数与 handler 判定用的区间是同一个：本用例直接对 `watch_create_access`
    /// 断言一次前缀订阅区间，作为"handler 没自造一套语义"的机械证据。
    #[tokio::test]
    async fn watch_handler_uses_the_shared_interval_helper() {
        let access = watch_create_access(b"/app/counter/", b"");
        assert_eq!(
            access,
            coord_core::grpc_auth::ScopeAccess::range(
                b"/app/counter/".to_vec(),
                b"/app/counter0".to_vec()
            ),
            "前缀订阅的区间必须是 [prefix, prefix_successor)——与投递侧 key_matches 同源"
        );
    }
}
