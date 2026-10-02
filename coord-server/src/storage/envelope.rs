// 持久化值统一格式信封 —— bincode 退场 P0「先立格式可辨识」
// （计划与判据见 docs/production/ops/dependencies.md）
//
// 布局：MAGIC(4B) | VERSION(1B) | bincode(payload)
//
// 为什么需要信封：raft 日志行 / PD 元数据行 / 对象 manifest 行的值直接是
// bincode 字节，且载体类型（openraft 实体类型、无版本字段的自定义结构）
// 无法在不破坏旧数据解码的前提下追加版本字段——换序列化格式前必须先能
// 从字节上**辨识**一行数据用了什么格式。
//
// 读兼容：无前缀的行（本信封落地前写入的旧数据）按旧格式直接 bincode 解码。
// 写路径：一律写带前缀的新格式。滚动升级必须先升级读路径（本模块随二进制
// 发布），再产生新格式写入。

use serde::{Deserialize, Serialize};

/// 魔数：控制字节 `0x02` + `"CRD"`。
///
/// 首字节取 `0x02` 而非 ASCII 字母：bincode 的 `Option` 标签只可能是 0/1，
/// 以 0x02 开头可保证「旧 Entry 行（以 Option 标签开头）」永远不会被误判成
/// 信封；`"CRD"` 便于在 hexdump 中肉眼辨识。
pub const MAGIC: [u8; 4] = [0x02, b'C', b'R', b'D'];

/// 信封格式版本（魔数之后的 1 字节）。
pub const VERSION: u8 = 1;

/// 前缀总长：魔数 4B + 版本 1B。
pub const PREFIX_LEN: usize = MAGIC.len() + 1;

/// 信封/旧格式解码错误。
#[derive(Debug)]
pub enum DecodeError {
    /// 数据带信封魔数，但版本字节不是当前支持的值（不认识的新版本或被篡改）。
    UnsupportedVersion(u8),
    /// payload（或旧格式整行）bincode 解码失败。
    Payload(bincode::Error),
}

impl std::fmt::Display for DecodeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::UnsupportedVersion(v) => {
                write!(f, "unsupported envelope version: {v} (expected {VERSION})")
            }
            Self::Payload(e) => write!(f, "payload decode failed: {e}"),
        }
    }
}

impl std::error::Error for DecodeError {}

/// 编码：`MAGIC | VERSION | bincode(value)`。
///
/// 所有持久化写路径必须使用本函数（白盒测试会断言原始行前缀）。
pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, bincode::Error> {
    let payload = bincode::serialize(value)?;
    let mut out = Vec::with_capacity(PREFIX_LEN + payload.len());
    out.extend_from_slice(&MAGIC);
    out.push(VERSION);
    out.extend_from_slice(&payload);
    Ok(out)
}

/// 解码：带前缀行校验版本后解 payload；无前缀行（旧数据）按旧格式解码。
///
/// 篡改前缀 ⇒ 显式错误而非静默解出垃圾：
/// - 版本字节被篡改 ⇒ [`DecodeError::UnsupportedVersion`]；
/// - 魔数被破坏 ⇒ 只能走旧格式路径，由 bincode 的结构校验与调用方的行
///   key/内容不变量（raft 日志条目 index、PD region_id）兜底拦截。
///
/// 已知边界：对既无结构约束又无内容不变量的行（如 openraft `Vote` 全字段均为
/// 任意数值），魔数单字节损坏存在被宽松解码接受的理论窗口；该类残余窗口在 P1
/// 换格式（更强信封/校验）时消除。
pub fn decode<'a, T: Deserialize<'a>>(data: &'a [u8]) -> Result<T, DecodeError> {
    if data.len() >= PREFIX_LEN && data[..MAGIC.len()] == MAGIC {
        let version = data[MAGIC.len()];
        if version != VERSION {
            return Err(DecodeError::UnsupportedVersion(version));
        }
        bincode::deserialize(&data[PREFIX_LEN..]).map_err(DecodeError::Payload)
    } else {
        bincode::deserialize(data).map_err(DecodeError::Payload)
    }
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

    /// 新数据必须带 `MAGIC + VERSION` 前缀（正判据）。
    #[test]
    fn test_encode_prefixes_magic_and_version() {
        let bytes = encode(&sample()).unwrap();
        assert!(bytes.starts_with(&MAGIC));
        assert_eq!(bytes[MAGIC.len()], VERSION);
        // 前缀之外是标准 bincode payload（逐字节一致）
        let payload = bincode::serialize(&sample()).unwrap();
        assert_eq!(&bytes[PREFIX_LEN..], payload.as_slice());
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
}
