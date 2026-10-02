// 持久化值统一格式信封 —— bincode 退场 P0「先立格式可辨识」/ P1「读路径双解」
// （计划与判据见 docs/production/ops/dependencies.md；替代编码选型见 docs/adr/0005）
//
// 布局：MAGIC(4B) | VERSION(1B) | payload
//   VERSION=1 ⇒ bincode(payload)（只读兼容：历史写路径产物）
//   VERSION=2 ⇒ postcard(payload)（当前写路径）
//   无魔数    ⇒ 历史无前缀行，整行按 bincode 解码
//
// 为什么需要信封：raft 日志行 / PD 元数据行 / 对象 manifest 行的值直接是
// bincode 字节，且载体类型（openraft 实体类型、无版本字段的自定义结构）
// 无法在不破坏旧数据解码的前提下追加版本字段——换序列化格式前必须先能
// 从字节上**辨识**一行数据用了什么格式。
//
// 读兼容：三路（V1 / V2 / 无前缀）；三条路径均精确消费——尾随字节必须显式
// 报错，不得被便捷解码函数静默忽略。写路径：全部持久化面统一写 V2（P2b）；
// 快照/auth/SM 元数据/PD 队列四个直写面同样经本模块接入三路读（P2a；快照的
// bincode 迁移阶梯保留）。滚动升级必须先升级读路径（本模块随二进制发布），
// 再产生新格式写入。

use serde::{Deserialize, Serialize};

/// 魔数：控制字节 `0x02` + `"CRD"`。
///
/// 首字节取 `0x02` 而非 ASCII 字母：bincode 的 `Option` 标签只可能是 0/1，
/// 以 0x02 开头可保证「旧 Entry 行（以 Option 标签开头）」永远不会被误判成
/// 信封；`"CRD"` 便于在 hexdump 中肉眼辨识。
pub const MAGIC: [u8; 4] = [0x02, b'C', b'R', b'D'];

/// V1 信封版本（bincode 载荷）：**只读兼容**——历史写路径产物（P2b 起新写入
/// 不再产生）；读侧保留至 P3。
pub const VERSION: u8 = 1;

/// 当前写路径信封版本：postcard 载荷（ADR-0005）。
pub const VERSION_V2: u8 = 2;

/// 前缀总长：魔数 4B + 版本 1B。
pub const PREFIX_LEN: usize = MAGIC.len() + 1;

/// 信封/旧格式解码错误。
#[derive(Debug)]
pub enum DecodeError {
    /// 数据带信封魔数，但版本字节不是任何受支持的版本（不认识的新版本或被篡改）。
    UnsupportedVersion(u8),
    /// V1 行 payload（或旧格式整行）bincode 解码失败（含尾随字节）。
    Payload(bincode::Error),
    /// V2 行 payload postcard 解码失败。
    PayloadV2(postcard::Error),
    /// V2 行 payload 精确消费后仍有剩余字节（解码器不得静默忽略尾随数据）。
    TrailingBytes(usize),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedVersion(v) => {
                write!(
                    f,
                    "unsupported envelope version: {v} (expected {VERSION} or {VERSION_V2})"
                )
            }
            Self::Payload(e) => write!(f, "payload decode failed: {e}"),
            Self::PayloadV2(e) => write!(f, "postcard payload decode failed: {e}"),
            Self::TrailingBytes(n) => write!(f, "trailing bytes after envelope payload: {n}"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// 编码（写路径唯一入口）：`MAGIC | VERSION_V2 | postcard(value)`。
///
/// 所有持久化写路径必须使用本函数（白盒测试会断言原始行前缀）。写侧无运行期
/// 版本开关——同一二进制只有一种写格式，迁移期的「两种格式」只存在于读侧。
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, postcard::Error> {
    encode_v2(value)
}

/// V2 编码：`MAGIC | VERSION_V2 | postcard(value)`（读路径另有 V1/无前缀腿）。
pub fn encode_v2<T: Serialize>(value: &T) -> Result<Vec<u8>, postcard::Error> {
    let payload = postcard::to_allocvec(value)?;
    let mut out = Vec::with_capacity(PREFIX_LEN + payload.len());
    out.extend_from_slice(&MAGIC);
    out.push(VERSION_V2);
    out.extend_from_slice(&payload);
    Ok(out)
}

/// 行格式分区（[`classify`] 的判别结果）。
#[derive(Debug, Clone, Copy)]
pub enum Envelope<'a> {
    /// 无魔数：历史无前缀行，整段按 bincode 解码。
    Legacy(&'a [u8]),
    /// `MAGIC | VERSION | bincode(payload)`。
    V1(&'a [u8]),
    /// `MAGIC | VERSION_V2 | postcard(payload)`。
    V2(&'a [u8]),
}

/// 判别一行数据的格式分区（读路径按此分派，不做试错解码）。
///
/// - 魔数成立且版本受支持 ⇒ 对应分区；
/// - 魔数成立但版本字节未知（篡改/未来版本）⇒ [`DecodeError::UnsupportedVersion`]；
/// - 其余（长度不足或魔数不成立）⇒ [`Envelope::Legacy`]（交旧格式路径由结构
///   校验兜底拦截魔数损坏）。
pub fn classify(data: &[u8]) -> Result<Envelope<'_>, DecodeError> {
    if data.len() >= PREFIX_LEN && data[..MAGIC.len()] == MAGIC {
        let payload = &data[PREFIX_LEN..];
        match data[MAGIC.len()] {
            VERSION => Ok(Envelope::V1(payload)),
            VERSION_V2 => Ok(Envelope::V2(payload)),
            other => Err(DecodeError::UnsupportedVersion(other)),
        }
    } else {
        Ok(Envelope::Legacy(data))
    }
}

/// 解码（三路）：V1（bincode payload）/ V2（postcard payload）按版本分发；
/// 无前缀行（历史数据）按 bincode 整行解码。
///
/// 精确消费：三条路径均拒绝尾随字节（不得静默解出前缀合法但内容多余的垃圾）。
/// 篡改前缀 ⇒ 显式错误：
/// - 版本字节非受支持版本 ⇒ [`DecodeError::UnsupportedVersion`]；
/// - 魔数被破坏 ⇒ 走旧格式路径，由 bincode 的结构校验与调用方的行 key/内容
///   不变量（raft 日志条目 index、PD region_id）兜底拦截。
///
/// 已知边界：V1 行中既无结构约束又无内容不变量的类型（如 openraft `Vote`
/// 全字段均为任意数值）在魔数单字节损坏后存在静默解出偏差值的理论窗口；
/// V2（postcard）载荷按 bincode 结构几乎不可能成立（ADR-0005 实测逐字节
/// 破坏全部显式失败），V1 残余窗口在 bincode 退场完成后消失。
pub fn decode<'a, T: Deserialize<'a>>(data: &'a [u8]) -> Result<T, DecodeError> {
    match classify(data)? {
        Envelope::V2(payload) => decode_postcard_exact(payload),
        Envelope::V1(payload) | Envelope::Legacy(payload) => decode_bincode_exact(payload),
    }
}

/// bincode 精确消费：fixint + 小端（与历史行字节兼容），拒绝尾随字节。
///
/// 快照的 bincode 迁移阶梯（v5→v4→v3→v2）按版本逐级调用本函数。
pub(crate) fn decode_bincode_exact<'a, T: Deserialize<'a>>(
    data: &'a [u8],
) -> Result<T, DecodeError> {
    use bincode::Options;
    bincode::DefaultOptions::new()
        .with_fixint_encoding()
        .deserialize(data)
        .map_err(DecodeError::Payload)
}

/// postcard 精确消费：`take_from_bytes` 取 remainder 并断言为空
/// （`postcard::from_bytes` 等便捷函数会静默忽略尾随字节）。
pub(crate) fn decode_postcard_exact<'a, T: Deserialize<'a>>(
    data: &'a [u8],
) -> Result<T, DecodeError> {
    let (value, remainder) =
        postcard::take_from_bytes::<T>(data).map_err(DecodeError::PayloadV2)?;
    if !remainder.is_empty() {
        return Err(DecodeError::TrailingBytes(remainder.len()));
    }
    Ok(value)
}

/// 测试构造器：生成 V1（bincode 载荷）信封行——历史写格式，供读兼容用例与
/// 负控制构造「旧写路径产物」。
#[cfg(test)]
pub(crate) fn encode_v1<T: Serialize>(value: &T) -> Result<Vec<u8>, bincode::Error> {
    let payload = bincode::serialize(value)?;
    let mut out = Vec::with_capacity(PREFIX_LEN + payload.len());
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&payload);
    Ok(out)
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

    /// 新数据（写路径唯一入口 `encode`）必须带 `MAGIC + VERSION_V2` 前缀，
    /// payload 为 postcard（正判据）。
    /// 负控制：写路径回退 V1（bincode 载荷）⇒ 本用例必红。
    #[test]
    fn test_encode_writes_v2_prefix() {
        let bytes = encode(&sample()).unwrap();
        assert!(bytes.starts_with(&MAGIC));
        assert_eq!(bytes[MAGIC.len()], VERSION_V2);
        // 前缀之外是标准 postcard payload（逐字节一致）
        let payload = postcard::to_allocvec(&sample()).unwrap();
        assert_eq!(&bytes[PREFIX_LEN..], payload.as_slice());
        // 与 encode_v2 同一产物（写侧只有一种格式，无运行期开关）
        assert_eq!(bytes, encode_v2(&sample()).unwrap());
    }

    #[test]
    fn test_roundtrip() {
        let bytes = encode(&sample()).unwrap();
        let decoded: Sample = decode(&bytes).unwrap();
        assert_eq!(decoded, sample());
    }

    /// 旧数据（无前缀、纯 bincode）必须仍能解码（正判据）。
    #[test]
    fn test_legacy_unprefixed_payload_still_decodes() {
        let legacy = bincode::serialize(&sample()).unwrap();
        let decoded: Sample = decode(&legacy).unwrap();
        assert_eq!(decoded, sample());
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

    /// 篡改魔数首字节 ⇒ 只能走旧格式路径并显式失败（不得 Ok 出垃圾）。
    #[test]
    fn test_tampered_magic_fails_explicitly() {
        let mut bytes = encode(&sample()).unwrap();
        bytes[0] = 0x03;
        assert!(
            decode::<Sample>(&bytes).is_err(),
            "tampered magic must not decode into a value"
        );
    }

    // ──── V2（postcard）读路径：双解对照 + 精确消费 ────

    /// V2 行布局：MAGIC + VERSION_V2 + postcard payload（逐字节）。
    #[test]
    fn test_v2_layout() {
        let bytes = encode_v2(&sample()).unwrap();
        assert!(bytes.starts_with(&MAGIC));
        assert_eq!(bytes[MAGIC.len()], VERSION_V2);
        let payload = postcard::to_allocvec(&sample()).unwrap();
        assert_eq!(&bytes[PREFIX_LEN..], payload.as_slice());
    }

    /// 对照测试（V1/bincode 侧，只读兼容格式）：解码 → 再编码逐字节稳定。
    #[test]
    fn test_v1_reencode_is_byte_stable() {
        let bytes = encode_v1(&sample()).unwrap();
        let decoded: Sample = decode(&bytes).unwrap();
        assert_eq!(decoded, sample());
        assert_eq!(encode_v1(&decoded).unwrap(), bytes);
    }

    /// 对照测试（postcard 侧）：解码 → 再编码逐字节稳定。
    #[test]
    fn test_v2_reencode_is_byte_stable() {
        let bytes = encode_v2(&sample()).unwrap();
        let decoded: Sample = decode(&bytes).unwrap();
        assert_eq!(decoded, sample());
        assert_eq!(encode_v2(&decoded).unwrap(), bytes);
    }

    /// 三路尾随篡改 ⇒ 全部显式失败，不得静默忽略剩余字节（精确消费）。
    /// 负控制：bincode 侧改用 allow_trailing / 去掉 V2 remainder 断言 ⇒ 对应用例必红。
    #[test]
    fn test_trailing_bytes_rejected_on_all_paths() {
        let mut v1 = encode_v1(&sample()).unwrap();
        v1.extend_from_slice(&[0xDE, 0xAD]);
        assert!(matches!(
            decode::<Sample>(&v1),
            Err(DecodeError::Payload(_))
        ));

        let mut legacy = bincode::serialize(&sample()).unwrap();
        legacy.extend_from_slice(&[0xDE, 0xAD]);
        assert!(matches!(
            decode::<Sample>(&legacy),
            Err(DecodeError::Payload(_))
        ));

        let mut v2 = encode(&sample()).unwrap();
        v2.extend_from_slice(&[0xDE, 0xAD]);
        assert!(matches!(
            decode::<Sample>(&v2),
            Err(DecodeError::TrailingBytes(2))
        ));
    }

    /// V2 行魔数逐字节破坏 ⇒ 全部显式失败（ADR-0005 实验锚点：postcard 载荷
    /// 不可能按 bincode 结构成立，不得静默解出偏差值）。
    /// 负控制：去掉魔数比较（仅按版本字节分发）⇒ 本用例必红。
    #[test]
    fn test_v2_magic_corruption_fails_explicitly() {
        for i in 0..MAGIC.len() {
            let mut bytes = encode_v2(&sample()).unwrap();
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

    /// classify：四种分区判别（V1 / V2 / 无前缀 / 未知版本）。
    /// 负控制：删去版本判别（仅凭魔数当同一种）⇒ 本用例必红。
    #[test]
    fn test_classify_partitions() {
        let v1 = encode_v1(&sample()).unwrap();
        assert!(matches!(classify(&v1), Ok(Envelope::V1(_))));

        let v2 = encode_v2(&sample()).unwrap();
        assert!(matches!(classify(&v2), Ok(Envelope::V2(_))));

        let legacy = bincode::serialize(&sample()).unwrap();
        assert!(matches!(classify(&legacy), Ok(Envelope::Legacy(_))));

        // 魔数前缀但长度不足 ⇒ 按旧格式分区（交结构校验兜底）
        assert!(matches!(classify(&MAGIC[..2]), Ok(Envelope::Legacy(_))));

        let mut unknown = v1.clone();
        unknown[MAGIC.len()] = 9;
        assert!(matches!(
            classify(&unknown),
            Err(DecodeError::UnsupportedVersion(9))
        ));
    }
}
