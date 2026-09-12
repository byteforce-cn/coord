// 第三轮复核 P0-1 回归测试：Txn 空 `range_end` 跨 scope 越权读
//
// 复现口径（与评估方 PoC 一致，但固化为**永久**用例）：
//   ① 鉴权层从请求 body 提取出的访问区间（`extract_scope_access`）；
//   ② 服务端对**同一** Txn op 实际执行的返回集合。
//
// 修复前：① 判为「对 `/app/a` 的单键访问」而放行（ScopeTrie 分段匹配下
// `/app/a` 命中 `scope="/app/a/"`），② 却回退为**字节前缀扫描**，于是返回
// `/app/abc/config`、`/app/admin/root-token` 等 scope 外的兄弟命名空间数据。
//
// 本套件固化的不变量（两层必须同时成立）：
//   (a) 请求要能通过鉴权 ⇒ 鉴权层建模的每个访问区间都必须被授权 scope 覆盖；
//   (b) 服务端返回的每个 Key 都必须落在鉴权层建模的某个访问区间内。
// 二者合起来 ⇒ 「通过鉴权的请求不可能拿到 scope 外的数据」。
//
// 协议依据：`coord-proto/src/proto/kv.proto` 对 `range_end` 的注释即为
// 「空表示单 Key 精确查询」——所以修复方向是让 Txn 内层 op 与顶层 `KV/Range`
// 取**同一**语义（`coord_core::kv_range::RangeSemantics`），而不是给鉴权层
// 加一个与执行层不同的解读。

use coord_core::auth::trie::scope_covers_interval;
use coord_core::storage::StorageBackend;
use coord_core::types::StorageConfig;
use coord_server::auth::interceptor::{extract_scope_access, ScopeAccess};
use coord_server::storage::mvcc::MvccStorage;
use coord_server::storage::redb_backend::RedbBackend;
use coord_server::txn::{TxnOp, TxnOpResponse};
use prost::Message;
use tempfile::TempDir;

fn storage() -> (TempDir, MvccStorage<RedbBackend>) {
    let dir = TempDir::new().unwrap();
    let config = StorageConfig {
        data_dir: dir.path().to_string_lossy().to_string(),
        ..Default::default()
    };
    let backend = RedbBackend::open(dir.path(), &config).unwrap();
    let mvcc = MvccStorage::new(backend).unwrap();
    (dir, mvcc)
}

/// 构造 `Txn{success:[Range(key, range_end)]}` 的裸 protobuf body。
fn txn_range_body(key: &[u8], range_end: &[u8]) -> Vec<u8> {
    let req = coord_proto::txn::TxnRequest {
        compare: vec![],
        success: vec![coord_proto::txn::RequestOp {
            op: Some(coord_proto::txn::request_op::Op::RequestRange(
                coord_proto::kv::RangeRequest {
                    key: key.to_vec(),
                    range_end: range_end.to_vec(),
                    limit: 0,
                    revision: 0,
                    keys_only: false,
                    count_only: false,
                },
            )),
        }],
        failure: vec![],
        request_id: Vec::new(),
    };
    req.encode_to_vec()
}

/// 执行与请求等价的 Txn Range op，返回实际交给客户端的 Key 列表。
fn server_range_keys(
    mvcc: &MvccStorage<RedbBackend>,
    key: &[u8],
    range_end: &[u8],
) -> Vec<Vec<u8>> {
    let out = mvcc
        .execute_txn(
            &[],
            &[TxnOp::Range {
                key: key.to_vec(),
                range_end: range_end.to_vec(),
                limit: 0,
            }],
            &[],
        )
        .unwrap();
    match &out.responses[0] {
        TxnOpResponse::Range { kvs, .. } => kvs.iter().map(|(k, _)| k.clone()).collect(),
        other => panic!("expected Range response, got {other:?}"),
    }
}

/// `key` 是否落在某次访问的区间内（单键 = 精确相等）。
fn access_contains(access: &ScopeAccess, key: &[u8]) -> bool {
    if access.range_end.is_empty() {
        key == access.key.as_slice()
    } else if access.range_end == coord_core::kv_range::UNBOUNDED_RANGE_END {
        key >= access.key.as_slice()
    } else {
        key >= access.key.as_slice() && key < access.range_end.as_slice()
    }
}

/// 已知的 scope 外哨兵 Key：任何「通过鉴权的请求」都**不得**读到它们。
const OUT_OF_SCOPE: [&[u8]; 3] = [b"/app/a0x", b"/app/abc/config", b"/app/admin/root-token"];

fn seed(mvcc: &MvccStorage<RedbBackend>) {
    mvcc.put(b"/app/a", b"mine-parent", None).unwrap();
    mvcc.put(b"/app/a/1", b"mine", None).unwrap();
    mvcc.put(b"/app/a0x", b"SECRET-a0x", None).unwrap();
    mvcc.put(b"/app/abc/config", b"SECRET-abc", None).unwrap();
    mvcc.put(b"/app/admin/root-token", b"SECRET-root", None)
        .unwrap();
}

/// 【P0-1 主用例】`Txn{Range(key="/app/a", range_end="")}` 必须只返回 `/app/a`。
///
/// 修复前这里返回 5 个 Key（含 3 个 scope 外哨兵）。
#[test]
fn txn_empty_range_end_is_single_key_and_cannot_escape_scope() {
    let scope = "/app/a/";
    let (_dir, mvcc) = storage();
    seed(&mvcc);

    let accesses = extract_scope_access("/coord.txn.Txn/Txn", &txn_range_body(b"/app/a", b""))
        .expect("scope extraction must succeed");

    // (a) 鉴权层建模：对 `/app/a` 的单键访问，且被 scope 覆盖（所以请求**会**被放行）
    assert_eq!(accesses, vec![ScopeAccess::point(b"/app/a".to_vec())]);
    for a in &accesses {
        assert!(
            scope_covers_interval(scope, &a.key, &a.range_end),
            "该访问应当被 scope 覆盖（否则本用例不构成'放行后越权'的场景）"
        );
    }

    // (b) 服务端实际返回：必须完全落在建模区间内
    let returned = server_range_keys(&mvcc, b"/app/a", b"");
    assert_eq!(
        returned,
        vec![b"/app/a".to_vec()],
        "空 range_end 不得退化为前缀扫描"
    );
    for key in &returned {
        assert!(
            accesses.iter().any(|a| access_contains(a, key)),
            "服务端返回了鉴权层未建模的 Key: {:?}",
            String::from_utf8_lossy(key)
        );
    }
    for sentinel in OUT_OF_SCOPE {
        assert!(
            !returned.iter().any(|k| k == sentinel),
            "越权读到 scope 外 Key: {:?}",
            String::from_utf8_lossy(sentinel)
        );
    }
}

/// 【P0-1 变体】同样的 key、但显式给出跨出 scope 的区间上界：
/// 鉴权层必须**整体覆盖**判定为拒绝（A1 语义），服务端也不得被调用。
#[test]
fn txn_explicit_interval_beyond_scope_is_denied_by_auth() {
    let scope = "/app/a/";
    let (_dir, mvcc) = storage();
    seed(&mvcc);

    // range_end 取 scope 的后继（`/app/a0`）之外 → 区间跨出 scope
    let accesses =
        extract_scope_access("/coord.txn.Txn/Txn", &txn_range_body(b"/app/a/", b"/app/b"))
            .expect("scope extraction must succeed");
    assert!(
        !accesses
            .iter()
            .all(|a| scope_covers_interval(scope, &a.key, &a.range_end)),
        "跨出 scope 的区间必须被拒绝（fail-closed）"
    );
    // 对照组：区间恰好等于 scope 的前缀区间 → 必须放行
    let ok = extract_scope_access(
        "/coord.txn.Txn/Txn",
        &txn_range_body(b"/app/a/", b"/app/a0"),
    )
    .expect("scope extraction must succeed");
    assert!(
        ok.iter()
            .all(|a| scope_covers_interval(scope, &a.key, &a.range_end)),
        "仓库自有前缀扫描惯用法必须继续放行（A1 回归）"
    );
}

/// 【P0-1 变体】`range_end == key` 是单键语义（顶层 `KV/Range`/`KV/Delete` 均如此）：
/// 鉴权层与执行层必须一致，且该 Key 确实能被读到。
#[test]
fn txn_range_end_equal_key_is_single_key_on_both_layers() {
    let scope = "/app/a/";
    let (_dir, mvcc) = storage();
    seed(&mvcc);

    let accesses = extract_scope_access(
        "/coord.txn.Txn/Txn",
        &txn_range_body(b"/app/a/1", b"/app/a/1"),
    )
    .expect("scope extraction must succeed");
    assert_eq!(accesses, vec![ScopeAccess::point(b"/app/a/1".to_vec())]);
    assert!(accesses
        .iter()
        .all(|a| scope_covers_interval(scope, &a.key, &a.range_end)));

    let returned = server_range_keys(&mvcc, b"/app/a/1", b"/app/a/1");
    assert_eq!(
        returned,
        vec![b"/app/a/1".to_vec()],
        "range_end == key 必须与顶层 Range 一样被当作单键读取（此前 Txn 路径返回空）"
    );
}
