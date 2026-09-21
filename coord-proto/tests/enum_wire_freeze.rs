// 枚举 wire 冻结卡口（W1-7 / D-17 / C22）
//
// # 为什么需要它（而不是"靠 buf lint"）
//
// 枚举值的**改名**对 grpc-java 是**静默破坏**：数值映射不变，客户端代码照样编译、
// 照样发请求，直到线上出现"未知枚举值"才暴露。`apis/contracts/buf.yaml` 里
// `ENUM_VALUE_PREFIX` / `ENUM_ZERO_VALUE_SUFFIX` 的豁免是**刻意的 wire 兼容决定**
// （重命名枚举值本身就是 Breaking），因此"取消命名类 lint 豁免"在这套已冻结的契约上
// **不可执行** —— 那会要求把 wire 上已经在跑的枚举值改名。
//
// 那么在冻结面上**真正**可执行、且有意义的机械保护是什么？三条：
//
// 1. **冻结快照**：`coord-proto/wire-freeze/enums.txt` 逐行记录
//    `<enum 全名>.<值名>=<编号>`。任何新增 / 改名 / 删除 / 改号都会让快照失配 ⇒ 红。
//    改动必须显式更新快照，于是"枚举演进"从一个无人看的 lint 豁免变成一次
//    **有 diff 的评审**。
// 2. **零值口径白名单**：proto3 的枚举零值是"未设置"的占位。冻结面上若某枚举的零值
//    不是 `*_UNSPECIFIED`（例如 `watch.EventType.PUT = 0` 把"未设置"与"一次真实写入"
//    合并了），必须写进 `zero-value-allowlist.txt` 并写清理由；新增这类枚举 ⇒ 红。
//    这条把 C22 里"零值语义歧义"从注释变成机器判据，且**不要求改 wire**。
// 3. **快照非空判据**：卡口自身不得在"解析不到任何枚举"时静默放行。
//
// 更新快照（只有在确实要改 wire 时才做，且必须在 CHANGELOG 里说明）：
//
// ```text
// UPDATE_ENUM_WIRE_FREEZE=1 cargo test -p coord-proto --test enum_wire_freeze
// ```
//
// 生成后 **必须人工复核 diff**：新增值可以（Minor），改名/改号/删除都要走 Breaking
// 流程。快照文件是交付物，不得被 .gitignore 吃掉（见 `git check-ignore -v`）。

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use prost::Message;

/// 快照文件路径（相对 crate 根，cargo 保证 `CARGO_MANIFEST_DIR` 存在）。
fn freeze_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("wire-freeze")
        .join(name)
}

/// 逐行解析 `key=value` 形式的快照（`#` 起头为注释，空行忽略）。
fn parse_pairs(text: &str) -> BTreeMap<String, String> {
    text.lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .filter_map(|l| {
            l.split_once('=')
                .map(|(k, v)| (k.trim().to_string(), v.trim().to_string()))
        })
        .collect()
}

fn pairs_to_text(pairs: &BTreeMap<String, String>, header: &[&str]) -> String {
    let mut out = String::new();
    for line in header {
        out.push_str(line);
        out.push('\n');
    }
    for (k, v) in pairs {
        out.push_str(k);
        out.push('=');
        out.push_str(v);
        out.push('\n');
    }
    out
}

fn descriptor_set() -> prost_types::FileDescriptorSet {
    <prost_types::FileDescriptorSet as Message>::decode(coord_proto::FILE_DESCRIPTOR_SET)
        .expect("coord_descriptor.bin 应能反解为 FileDescriptorSet")
}

/// 递归收集某个 message 及其嵌套 message 里的枚举。
fn collect_message_enums(
    msg: &prost_types::DescriptorProto,
    prefix: &str,
    out: &mut Vec<(String, prost_types::EnumDescriptorProto)>,
) {
    let my_name = match msg.name.as_deref() {
        Some(n) => format!("{prefix}.{n}"),
        None => prefix.to_string(),
    };
    for e in &msg.enum_type {
        if let Some(name) = e.name.as_deref() {
            out.push((format!("{my_name}.{name}"), e.clone()));
        }
    }
    for nested in &msg.nested_type {
        collect_message_enums(nested, &my_name, out);
    }
}

/// 全部枚举：`(全名, 枚举描述符)`，按全名排序（deterministic）。
fn all_enums() -> Vec<(String, prost_types::EnumDescriptorProto)> {
    let mut out: Vec<(String, prost_types::EnumDescriptorProto)> = Vec::new();
    for file in &descriptor_set().file {
        let pkg = file.package.clone().unwrap_or_default();
        for e in &file.enum_type {
            if let Some(name) = e.name.as_deref() {
                out.push((format!("{pkg}.{name}"), e.clone()));
            }
        }
        for msg in &file.message_type {
            collect_message_enums(msg, &pkg, &mut out);
        }
    }
    out.sort_by(|a, b| a.0.cmp(&b.0));
    out
}

/// `<enum 全名>.<值名>` → 编号。
fn enum_value_pairs() -> BTreeMap<String, String> {
    let mut pairs = BTreeMap::new();
    for (enum_full, e) in all_enums() {
        for v in &e.value {
            if let Some(name) = v.name.as_deref() {
                pairs.insert(
                    format!("{enum_full}.{name}"),
                    v.number.unwrap_or_default().to_string(),
                );
            }
        }
    }
    pairs
}

/// `<enum 全名>` → 零值的值名（proto3 里编号 0 的那个值）。
fn zero_value_names() -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    for (enum_full, e) in all_enums() {
        let zero = e
            .value
            .iter()
            .find(|v| v.number.unwrap_or_default() == 0)
            .and_then(|v| v.name.clone())
            .unwrap_or_else(|| "<无零值>".to_string());
        out.insert(enum_full, zero);
    }
    out
}

/// 冻结卡口：枚举值集合必须与 `wire-freeze/enums.txt` **逐条相同**。
///
/// 失败信息分三类给出，因为处置方式完全不同：
/// * **新增** ⇒ 可以（Minor），更新快照即可；
/// * **删除 / 改名** ⇒ Breaking（改名的数值映射不变 ⇒ 对 grpc-java 是**静默**破坏）；
/// * **改号** ⇒ Breaking（线上旧客户端会解成另一个语义）。
#[test]
fn enum_wire_freeze_is_unchanged() {
    let path = freeze_path("enums.txt");
    let current = enum_value_pairs();

    // 卡口自身的有效性：解析不到枚举/值就说明判据失效（不许静默放行）
    //
    // 下限按**当前实际集合**标定（2026-09-21：9 个枚举 / 32 个值）—— 取得过低则
    // 「descriptor 解析失败」也能过，取得过高则会在合法新增场景误红。
    assert!(
        current.len() > 20,
        "从 descriptor 只解析出 {} 个枚举值，判据本身可能失效（期望 >20，当前实际 32）",
        current.len()
    );

    if std::env::var("UPDATE_ENUM_WIRE_FREEZE").is_ok() {
        let header = [
            "# coord 枚举 wire 冻结快照（W1-7 / D-17）。",
            "# 每行：<enum 全名>.<值名>=<编号>（按全名排序）。",
            "#",
            "# 更新方式：UPDATE_ENUM_WIRE_FREEZE=1 cargo test -p coord-proto --test enum_wire_freeze",
            "# 更新后必须人工复核 diff：",
            "#   * 新增值 ⇒ 允许（Minor），CHANGELOG 记录；",
            "#   * 删除/改名/改号 ⇒ Breaking：数值映射不变时为**静默**破坏（grpc-java 编译不报错），",
            "#     必须走契约 Major 流程（apis/contracts/WHITEPAPER.md §3.5）。",
        ];
        std::fs::write(&path, pairs_to_text(&current, &header)).expect("写入冻结快照");
        eprintln!("已更新 {}", path.display());
        return;
    }

    let frozen = parse_pairs(&std::fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "读取 {} 失败：{e}（首次生成见本文件顶部注释）",
            path.display()
        )
    }));

    let added: Vec<&String> = current
        .keys()
        .filter(|k| !frozen.contains_key(*k))
        .collect();
    let removed: Vec<&String> = frozen
        .keys()
        .filter(|k| !current.contains_key(*k))
        .collect();
    let renumbered: Vec<String> = current
        .iter()
        .filter_map(|(k, v)| match frozen.get(k) {
            Some(old) if old != v => Some(format!("{k}: {old} → {v}")),
            _ => None,
        })
        .collect();

    assert!(
        added.is_empty() && removed.is_empty() && renumbered.is_empty(),
        "枚举 wire 快照失配（这是**契约**变更，必须显式评审）：\n\
         * 新增 {} 条：{:?}\n\
         * 删除/改名 {} 条：{:?}\n\
         * 改号 {} 条：{:?}\n\
         改名的数值映射不变，对 grpc-java 是**静默**破坏（编译不报错）；\
         确认变更后按文件顶部注释更新快照，并在 CHANGELOG 记录。",
        added.len(),
        added,
        removed.len(),
        removed,
        renumbered.len(),
        renumbered
    );
}

/// 可接受的"未设置"零值名。
///
/// * `*_UNSPECIFIED` —— proto3 惯例（也是 `ENUM_ZERO_VALUE_SUFFIX` 要求的形态）；
/// * `*_UNKNOWN` —— 同义写法，存量 proto 里已在用；
/// * 裸 `UNKNOWN` —— gRPC 标准健康检查的取值名
///   （`grpc.health.v1.HealthCheckResponse.ServingStatus.UNKNOWN = 0`），
///   本仓库的 `coord.agent.HealthCheckResponse` 与之对齐，故接受。
fn is_not_set_name(name: &str) -> bool {
    name.ends_with("_UNSPECIFIED") || name.ends_with("_UNKNOWN") || name == "UNKNOWN"
}

/// 零值口径卡口：枚举零值必须是"未设置"名（见 [`is_not_set_name`]），否则必须进白名单
/// 并写明理由。
///
/// 背景（C22）：`watch.EventType.PUT = 0` 把"未设置"与"一次真实写入"合并 —— 这类零值
/// 歧义**不能**靠改名修（改名即 Breaking），只能显式登记 + 机械防止新增。
#[test]
fn zero_value_names_are_unspecified_or_justified() {
    let path = freeze_path("zero-value-allowlist.txt");
    let zeroes = zero_value_names();
    assert!(
        zeroes.len() >= 8,
        "只解析出 {} 个枚举，判据本身可能失效（当前实际 9）",
        zeroes.len()
    );

    let allowlist = parse_pairs(
        &std::fs::read_to_string(&path)
            .unwrap_or_else(|e| panic!("读取 {} 失败：{e}", path.display())),
    );

    let mut unjustified: Vec<String> = Vec::new();
    for (enum_full, zero_name) in &zeroes {
        if is_not_set_name(zero_name) {
            continue;
        }
        let justified = allowlist
            .get(&format!("{enum_full}.{zero_name}"))
            .is_some_and(|reason| reason.trim().len() >= 10);
        if !justified {
            unjustified.push(format!("{enum_full}.{zero_name}"));
        }
    }
    assert!(
        unjustified.is_empty(),
        "以下枚举的零值既不是 *_UNSPECIFIED，也没有在白名单里写明理由：{unjustified:?}\n\
         零值 = \"未设置\"的占位；若它同时表达一个**真实取值**（如 PUT=0），客户端就无法\
         区分\"没写\"与\"写了 PUT\"。修法二选一：①新增 `*_UNSPECIFIED = 0` 并把现有值顺延\
         （**Breaking**，需走 Major 流程）；②在 {} 登记 `<enum 全名>.<值名>=<理由>`\
         （≥10 字符），承认这个 wire 事实。",
        path.display()
    );

    // 反向：白名单不得有失效条目（枚举已改名/删除后，白名单会变成"看起来很在意"的摆设）
    let mut stale: Vec<&String> = Vec::new();
    for key in allowlist.keys() {
        let (enum_full, value_name) = key.rsplit_once('.').expect("白名单键格式为 <enum>.<值名>");
        match zeroes.get(enum_full) {
            Some(zero) if zero == value_name => {}
            _ => stale.push(key),
        }
    }
    assert!(
        stale.is_empty(),
        "白名单里以下条目已不是对应枚举的零值（枚举改名/删除后未同步，白名单成了摆设）：{stale:?}"
    );
}
