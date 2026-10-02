// 持久化值统一格式信封 —— 写路径与读路径收敛为唯一 V2（postcard）
// （退场计划与完成态见 docs/production/ops/dependencies.md；替代编码选型见 docs/adr/0005）
//
// 布局：MAGIC(4B) | VERSION_V2(1B) | postcard payload
//
// 为什么需要信封：raft 日志行 / PD 元数据行 / 对象 manifest 行的值直接是序列化
// 字节，且载体类型（openraft 实体类型、无版本字段的自定义结构）无法在不破坏旧
// 数据解码的前提下追加版本字段——换序列化格式前必须先能从字节上**辨识**一行
// 数据用了什么格式。
//
// 唯一格式与 fail-closed：只接受 V2（postcard）；V1（bincode）与无前缀历史行已
// 随退场计划显式退役——出现即显式报错（不猜测、不回落、不做试错解码）。解码
// 精确消费：尾随字节必须显式报错，不得被便捷解码函数静默忽略。

use serde::{Deserialize, Serialize};

/// 魔数：控制字节 `0x02` + `"CRD"`。
///
/// 首字节取 `0x02` 而非 ASCII 字母，避免与历史 bincode 行的首字节形态
/// （`Option` 标签只可能是 0/1）混淆；`"CRD"` 便于在 hexdump 中肉眼辨识。
pub const MAGIC: [u8; 4] = [0x02, b'C', b'R', b'D'];

/// 信封版本：postcard 载荷（ADR-0005；唯一受支持版本）。
pub const VERSION_V2: u8 = 2;

/// 前缀总长：魔数 4B + 版本 1B。
pub const PREFIX_LEN: usize = MAGIC.len() + 1;

/// 信封解码错误。
#[derive(Debug)]
pub enum DecodeError {
    /// 无信封魔数：历史无前缀行已退役（或数据被截断/损坏），显式拒绝。
    MissingEnvelope,
    /// 版本字节不受支持（未知/被篡改/已退役的 V1），显式拒绝。
    UnsupportedVersion(u8),
    /// payload postcard 解码失败。
    PayloadV2(postcard::Error),
    /// payload 精确消费后仍有剩余字节（解码器不得静默忽略尾随数据）。
    TrailingBytes(usize),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingEnvelope => write!(
                f,
                "missing format envelope magic (legacy rows are retired; expected MAGIC | \
                 {VERSION_V2} | postcard)"
            ),
            Self::UnsupportedVersion(v) => {
                write!(
                    f,
                    "unsupported envelope version: {v} (expected {VERSION_V2})"
                )
            }
            Self::PayloadV2(e) => write!(f, "postcard payload decode failed: {e}"),
            Self::TrailingBytes(n) => write!(f, "trailing bytes after envelope payload: {n}"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// 编码（写路径唯一入口）：`MAGIC | VERSION_V2 | postcard(value)`。
///
/// 所有持久化写路径必须使用本函数（白盒测试会断言原始行前缀）。写侧无运行期
/// 版本开关：同一二进制只有一种写格式。
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, postcard::Error> {
    let payload = postcard::to_allocvec(value)?;
    let mut out = Vec::with_capacity(PREFIX_LEN + payload.len());
    out.extend_from_slice(&MAGIC);
    out.push(VERSION_V2);
    out.extend_from_slice(&payload);
    Ok(out)
}

/// 剥离并校验信封前缀，返回 postcard payload。
///
/// - 魔数 + V2 ⇒ payload；
/// - 魔数成立但版本 ≠ V2 ⇒ [`DecodeError::UnsupportedVersion`]（含已退役 V1）；
/// - 无魔数（长度不足或首 4 字节不符）⇒ [`DecodeError::MissingEnvelope`]。
pub fn split_envelope(data: &[u8]) -> Result<&[u8], DecodeError> {
    if data.len() >= PREFIX_LEN && data[..MAGIC.len()] == MAGIC {
        match data[MAGIC.len()] {
            VERSION_V2 => Ok(&data[PREFIX_LEN..]),
            other => Err(DecodeError::UnsupportedVersion(other)),
        }
    } else {
        Err(DecodeError::MissingEnvelope)
    }
}

/// 解码：唯一格式 V2（postcard）；精确消费（尾随字节显式报错）。
pub fn decode<'a, T: Deserialize<'a>>(data: &'a [u8]) -> Result<T, DecodeError> {
    decode_postcard_exact(split_envelope(data)?)
}

/// postcard 精确消费：`take_from_bytes` 取 remainder 并断言为空
/// （`postcard::from_bytes` 等便捷函数会静默忽略尾随字节）。
fn decode_postcard_exact<'a, T: Deserialize<'a>>(data: &'a [u8]) -> Result<T, DecodeError> {
    let (value, remainder) =
        postcard::take_from_bytes::<T>(data).map_err(DecodeError::PayloadV2)?;
    if !remainder.is_empty() {
        return Err(DecodeError::TrailingBytes(remainder.len()));
    }
    Ok(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Serialize, Deserialize)]
    struct Sample {
        id: u64,
        name: String,
        flag: bool,
    }

    fn sample() -> Sample {
        Sample {
            id: 7,
            name: "coord".to_string(),
            flag: true,
        }
    }

    /// 写路径唯一入口：`MAGIC + VERSION_V2 + postcard`（正判据）。
    /// 负控制：写路径回退 V1 版本字节 ⇒ 本用例必红。
    #[test]
    fn test_encode_writes_v2_prefix() {
        let bytes = encode(&sample()).unwrap();
        assert!(bytes.starts_with(&MAGIC));
        assert_eq!(bytes[MAGIC.len()], VERSION_V2);
        // 前缀之外是标准 postcard payload（逐字节一致）
        let payload = postcard::to_allocvec(&sample()).unwrap();
        assert_eq!(&bytes[PREFIX_LEN..], payload.as_slice());
    }

    #[test]
    fn test_roundtrip() {
        let bytes = encode(&sample()).unwrap();
        let decoded: Sample = decode(&bytes).unwrap();
        assert_eq!(decoded, sample());
    }

    /// V1 行（已退役格式）⇒ 显式拒绝（UnsupportedVersion(1)），不得试解。
    /// 负控制：恢复 V1（bincode）读腿 ⇒ 本用例必红（P3 退役锚点）。
    #[test]
    fn test_retired_v1_rows_rejected_explicitly() {
        let mut v1 = Vec::new();
        v1.extend_from_slice(&MAGIC);
        v1.push(1);
        v1.extend_from_slice(&[0xAA, 0xBB, 0xCC]);
        match decode::<Sample>(&v1) {
            Err(DecodeError::UnsupportedVersion(1)) => {}
            other => panic!("V1 rows must be rejected explicitly, got {other:?}"),
        }
    }

    /// 无前缀历史行（已退役格式）⇒ 显式拒绝（MissingEnvelope），不得试解。
    /// 负控制：恢复无前缀回退 ⇒ 本用例必红（P3 退役锚点）。
    #[test]
    fn test_retired_legacy_unprefixed_rows_rejected() {
        let legacy = vec![0x05u8, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00];
        match decode::<Sample>(&legacy) {
            Err(DecodeError::MissingEnvelope) => {}
            other => panic!("unprefixed legacy rows must be rejected, got {other:?}"),
        }
    }

    /// 篡改版本字节 ⇒ 显式报错，不得静默解成垃圾（负判据）。
    #[test]
    fn test_tampered_version_fails_explicitly() {
        let mut bytes = encode(&sample()).unwrap();
        bytes[MAGIC.len()] = 0xFE;
        match decode::<Sample>(&bytes) {
            Err(DecodeError::UnsupportedVersion(0xFE)) => {}
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
    }

    /// 篡改魔数首字节 ⇒ 显式拒绝（MissingEnvelope），不得 Ok 出垃圾。
    #[test]
    fn test_tampered_magic_fails_explicitly() {
        let mut bytes = encode(&sample()).unwrap();
        bytes[0] = 0x03;
        assert!(matches!(
            decode::<Sample>(&bytes),
            Err(DecodeError::MissingEnvelope)
        ));
    }

    /// V2 行布局：MAGIC + VERSION_V2 + postcard payload（逐字节）。
    #[test]
    fn test_v2_layout() {
        let bytes = encode(&sample()).unwrap();
        assert!(bytes.starts_with(&MAGIC));
        assert_eq!(bytes[MAGIC.len()], VERSION_V2);
        let payload = postcard::to_allocvec(&sample()).unwrap();
        assert_eq!(&bytes[PREFIX_LEN..], payload.as_slice());
    }

    /// 对照测试：解码 → 再编码逐字节稳定。
    #[test]
    fn test_reencode_is_byte_stable() {
        let bytes = encode(&sample()).unwrap();
        let decoded: Sample = decode(&bytes).unwrap();
        assert_eq!(decoded, sample());
        assert_eq!(encode(&decoded).unwrap(), bytes);
    }

    /// 尾随字节 ⇒ 全部显式失败（精确消费）：
    /// V2 行 ⇒ TrailingBytes；V1/无前缀已退役行 ⇒ 版本/魔数检查先行拒绝。
    /// 负控制：去掉 V2 remainder 断言 ⇒ 本用例必红。
    #[test]
    fn test_trailing_bytes_rejected() {
        let mut v2 = encode(&sample()).unwrap();
        v2.extend_from_slice(&[0xDE, 0xAD]);
        assert!(matches!(
            decode::<Sample>(&v2),
            Err(DecodeError::TrailingBytes(2))
        ));

        let mut v1_trailing = Vec::new();
        v1_trailing.extend_from_slice(&MAGIC);
        v1_trailing.push(1);
        v1_trailing.extend_from_slice(&[0xDE, 0xAD]);
        assert!(matches!(
            decode::<Sample>(&v1_trailing),
            Err(DecodeError::UnsupportedVersion(1))
        ));
    }

    /// V2 行魔数逐字节破坏 ⇒ 全部显式失败（不得静默解出偏差值）。
    /// 负控制：去掉魔数比较（仅按版本字节分发）⇒ 本用例必红。
    #[test]
    fn test_v2_magic_corruption_fails_explicitly() {
        for i in 0..MAGIC.len() {
            let mut bytes = encode(&sample()).unwrap();
            bytes[i] = bytes[i].wrapping_add(1);
            assert!(
                decode::<Sample>(&bytes).is_err(),
                "V2 row with corrupted magic byte {i} must fail explicitly"
            );
        }
    }

    /// 已知版本之外的版本字节 ⇒ UnsupportedVersion。
    #[test]
    fn test_unknown_version_after_v2_is_rejected() {
        let mut bytes = encode(&sample()).unwrap();
        bytes[MAGIC.len()] = VERSION_V2 + 1;
        match decode::<Sample>(&bytes) {
            Err(DecodeError::UnsupportedVersion(v)) => assert_eq!(v, VERSION_V2 + 1),
            other => panic!("expected UnsupportedVersion, got {other:?}"),
        }
    }

    /// split_envelope：唯一分区 V2；V1 / 无前缀 / 未知版本显式拒绝。
    /// 负控制：恢复任一历史读腿 ⇒ 本用例必红。
    #[test]
    fn test_split_envelope_partitions() {
        let v2 = encode(&sample()).unwrap();
        assert!(split_envelope(&v2).is_ok());

        let mut v1 = Vec::new();
        v1.extend_from_slice(&MAGIC);
        v1.push(1);
        v1.extend_from_slice(&[0xAA]);
        assert!(matches!(
            split_envelope(&v1),
            Err(DecodeError::UnsupportedVersion(1))
        ));

        assert!(matches!(
            split_envelope(&[0x05, 0x00]),
            Err(DecodeError::MissingEnvelope)
        ));
        assert!(matches!(
            split_envelope(&MAGIC[..2]),
            Err(DecodeError::MissingEnvelope)
        ));
    }
}
