// wire 解码鲁棒性属性测试（coord-proto 生成类型）
//
// 口径：对**任意字节**调用 `prost::Message::decode` 不得 panic / 越界——
// 不要求语义正确（乱字节就该是 `Err`），只约束解析路径的健壮性。
// 覆盖 kv / txn / watch / lease 的代表类型（这些消息直接消费客户端字节）。
//
// 关于负控制：prost 生成代码没有可注入的边界分支，本套件是**回归护栏**——
// 若未来把 decode 路径换成手写解析，任意字节属性会约束其不得 panic。
// 手写解析路径（cache 值编解码、health 请求行）的负控制见对应单测文件。

use proptest::prelude::*;
use prost::Message;

proptest! {
    #[test]
    fn kv_messages_decode_arbitrary_bytes_without_panic(
        bytes in proptest::collection::vec(any::<u8>(), 0..4096),
    ) {
        let _ = coord_proto::kv::PutRequest::decode(bytes.as_slice());
        let _ = coord_proto::kv::RangeRequest::decode(bytes.as_slice());
        let _ = coord_proto::kv::DeleteRequest::decode(bytes.as_slice());
    }

    #[test]
    fn txn_watch_lease_messages_decode_arbitrary_bytes_without_panic(
        bytes in proptest::collection::vec(any::<u8>(), 0..4096),
    ) {
        let _ = coord_proto::txn::TxnRequest::decode(bytes.as_slice());
        let _ = coord_proto::watch::WatchRequest::decode(bytes.as_slice());
        let _ = coord_proto::lease::LeaseGrantRequest::decode(bytes.as_slice());
    }
}

/// 空输入是合法 wire 流（全默认消息）：decode 必须成功且字段全默认。
#[test]
fn empty_input_decodes_to_default_message() {
    let decoded = coord_proto::kv::PutRequest::decode(&[][..]).expect("empty decode");
    assert_eq!(decoded, coord_proto::kv::PutRequest::default());
}
