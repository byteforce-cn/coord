#!/usr/bin/env python3
# ============================================================
# descriptor_signature.py — 把 descriptor 文本归一成**可比较的 wire 签名**
#
# 用途：`check-wire-descriptor.sh` 的比对内核。输入是 `protoc
# --decode=google.protobuf.FileDescriptorSet` 的文本输出（两份：契约侧 / 实现侧），
# 输出是逐包的规范化签名并做比对。
#
# 为什么需要它（评审 D-e / V7 / G1）：`check-wire-sync.sh` 按**字段名全文匹配**，
# 不精确到 message 作用域 —— 它自己也在文件头承认了这一点。于是"契约已落地"这件事
# 当时只是一张**政策表述**：字段名恰好一致就过，同名不同 message、字段号互换、
# 类型改宽窄都可能不被发现。本脚本把判据升级为 **descriptor 级**：
# 以 protoc 产出的描述符为准，逐包比对
#   - service / method（名、入参、出参、流式标志）
#   - message / field（名、编号、label、类型、type_name、oneof_index、proto3_optional）
#   - enum / 值（名、编号）
#   - 嵌套类型与 map entry（天然递归展开）
#
# **刻意忽略**（不属 wire 语义，两侧本就允许不同）：
#   - 文件名与 import 列表（契约副本在 apis/contracts/proto，实现副本在
#     coord-proto/src/proto，路径必然不同 —— 比对键是 **package** 而非文件）
#   - options（java_package / go_package / optimize_for 等代码生成选项）
#   - source_code_info（注释 / 位置信息）
#   - json_name（由字段名机械派生）
# ============================================================
import sys
from collections import defaultdict

# ── 判据分层（**这是本卡口最需要小心的地方**）──────────────────────────────
#
# 两侧副本的**约定不同**，用一把尺子量会把合法约定判成漂移：
#
# 1) GA / COMMITTED 层（`coord.<domain>.v1`）—— **strict（相等）**：
#    contracts/v1.2.0 的迁移口径是"契约副本与实现副本逐字相同"（含服务名、
#    方法名、字段号、字段类型）。任何一侧多一项都意味着两份副本已经分叉，
#    而"包名逐字相同即对齐"（WHITEPAPER §4.1 通则）正建立在这个前提上。
#
# 2) STABLE 底座原语层（无 `.v1` 后缀，如 `coord.kv` / `coord.maintenance`）——
#    **subset（契约 ⊆ 实现）**：STABLE 契约可以是**承诺的下界**。
#    实例：`coord.maintenance` 的契约头部明确写着"v1 仅承诺 Status 探活接口；
#    Seal / Unseal / Snapshot / Compact / Member* / Join 不在对外承诺范围"，
#    并把 StatusResponse 的 2–5 号字段 reserved（线端仍返回，但不属承诺语义）。
#    ⇒ 这一层"实现更宽"是**有意的**，不该置红；但"契约声明了而实现没有"必须置红
#    （承诺没兑现），"同一项签名不同"也必须置红（承诺说错了）。
#
# 3) 随附的第三方标准 proto（`grpc.health.v1`）—— **跳过**：它由 lib 侧实现
#    （`tonic-health`），不来自本仓 .proto，本卡口无从比对。
VENDORED_PACKAGES = {"grpc.health.v1"}


def mode_for_package(pkg):
    """返回 "strict" 或 "subset"（见上方分层说明）。"""
    return "strict" if pkg.endswith(".v1") else "subset"



class Frame:
    __slots__ = ("indent", "kind", "name", "attrs", "parent")

    def __init__(self, indent, kind, parent):
        self.indent = indent
        self.kind = kind
        self.name = None
        self.attrs = {}
        self.parent = parent

    def scope_path(self):
        """package 之下所有具名 message / nested / enum / service 组成的路径。"""
        parts = []
        frame = self.parent
        while frame is not None:
            if frame.kind in ("message_type", "nested_type", "enum_type", "service") and frame.name:
                parts.append(frame.name)
            frame = frame.parent
        return list(reversed(parts))


def parse_descriptor_text(path):
    """解析 protoc --decode 文本 → {package: {signature_key: value}}。"""
    per_package = defaultdict(dict)

    with open(path, "r", encoding="utf-8") as handle:
        lines = handle.read().splitlines()

    package = None
    file_frame = None
    stack = []  # Frame 栈（不含 file/system 根）

    def current():
        return stack[-1] if stack else file_frame

    for raw in lines:
        if not raw.strip():
            continue
        indent = len(raw) - len(raw.lstrip(" "))
        text = raw.strip()

        if text == "}":
            while stack and stack[-1].indent >= indent:
                frame = stack.pop()
                emit(per_package, package, frame)
            continue

        if text.endswith("{"):
            kind = text[:-1].strip()
            if kind == "file":
                file_frame = Frame(indent, "file", None)
                package = None
                continue
            stack.append(Frame(indent, kind, current()))
            continue

        if ":" not in text:
            continue
        key, _, value = text.partition(":")
        key = key.strip()
        value = value.strip().strip('"')
        frame = current()
        if frame is None:
            continue
        if frame.kind == "file":
            if key == "package":
                package = value
            continue
        if frame.kind == "options":
            continue
        if key == "name" and frame.name is None:
            frame.name = value
        else:
            frame.attrs[key] = value

    return {pkg: sig for pkg, sig in per_package.items()}


def emit(per_package, package, frame):
    """帧关闭时把它的 wire 签名写入所属包。"""
    if package is None or frame.kind in ("file", "options"):
        return
    prefix = ".".join(frame.scope_path())

    if frame.kind in ("message_type", "nested_type", "enum_type", "service"):
        if frame.name:
            label = {
                "message_type": "message",
                "nested_type": "message",
                "enum_type": "enum",
                "service": "service",
            }[frame.kind]
            per_package[package].setdefault(f"{label}:{prefix}", {})
        return

    if frame.kind == "field":
        if frame.name and prefix:
            per_package[package][f"field:{prefix}.{frame.name}"] = (
                f"#{frame.attrs.get('number')} "
                f"label={frame.attrs.get('label')} "
                f"type={frame.attrs.get('type')} "
                f"type_name={frame.attrs.get('type_name', '-')} "
                f"oneof_index={frame.attrs.get('oneof_index', '-')} "
                f"proto3_optional={frame.attrs.get('proto3_optional', '-')}"
            )
        return

    if frame.kind == "method":
        if frame.name and prefix:
            per_package[package][f"method:{prefix}/{frame.name}"] = (
                f"in={frame.attrs.get('input_type')} "
                f"out={frame.attrs.get('output_type')} "
                f"client_streaming={frame.attrs.get('client_streaming', 'false')} "
                f"server_streaming={frame.attrs.get('server_streaming', 'false')}"
            )
        return

    if frame.kind in ("enum_value", "value"):
        if frame.name and prefix:
            per_package[package][f"enum_value:{prefix}.{frame.name}"] = str(
                frame.attrs.get("number")
            )
        return

    if frame.kind == "oneof_decl":
        if frame.name and prefix:
            per_package[package][f"oneof:{prefix}.{frame.name}"] = "decl"


def main():
    if len(sys.argv) != 3:
        print("usage: descriptor_signature.py <contract.txt> <internal.txt>", file=sys.stderr)
        return 2
    contract = parse_descriptor_text(sys.argv[1])
    internal = parse_descriptor_text(sys.argv[2])

    if not contract:
        print("FAIL: 契约侧描述符解析为空（脚本或路径有问题）", file=sys.stderr)
        return 1

    ok = True
    stats = {"equality": 0, "subset": 0, "vendored": 0}

    # ① 包集合：契约里出现的每个包都必须在实现里（承诺即交付义务）。
    for pkg in sorted(set(contract) - set(internal)):
        if pkg in VENDORED_PACKAGES:
            stats["vendored"] += 1
            print(f"NOTICE: {pkg} 是随附的第三方标准 proto（由 lib 实现，不在本仓描述符内）—— 跳过")
            continue
        ok = False
        print(f"FAIL: 契约包 {pkg} 在实现描述符中不存在（承诺未落地）", file=sys.stderr)

    # ② 逐包比对：按层取不同的判据（见模块头 VENDORED_PACKAGES / mode_for_package 说明）。
    for pkg in sorted(set(contract) & set(internal)):
        if pkg in VENDORED_PACKAGES:
            stats["vendored"] += 1
            print(f"NOTICE: {pkg} 是随附的第三方标准 proto —— 跳过结构性比对")
            continue

        c, i = contract[pkg], internal[pkg]
        mode = mode_for_package(pkg)
        if mode == "strict":
            missing = {k: v for k, v in c.items() if k not in i}
            extra = {k: v for k, v in i.items() if k not in c}
            differing = {k: (c[k], i[k]) for k in set(c) & set(i) if c[k] != i[k]}
        else:
            # 子集判据：契约是**承诺的下界**，实现可以更宽（STABLE 底座原语的实际约定，
            # 见 apis/contracts/proto/coord/maintenance/maintenance.proto 的头部说明）。
            missing = {k: v for k, v in c.items() if k not in i}
            extra = {}
            differing = {k: (c[k], i[k]) for k in set(c) & set(i) if c[k] != i[k]}

        if missing or extra or differing:
            ok = False
            print(
                f"FAIL: {pkg} 的 descriptor 级 wire 不一致（判据：{mode}）",
                file=sys.stderr,
            )
            for k, v in sorted(missing.items()):
                print(f"  - 契约声明但实现缺失: {k} = {v}", file=sys.stderr)
            for k, v in sorted(extra.items()):
                print(f"  - 实现多出（strict 层不允许）: {k} = {v}", file=sys.stderr)
            for k, (cv, iv) in sorted(differing.items()):
                print(f"  - 签名不一致: {k}\n      契约: {cv}\n      实现: {iv}", file=sys.stderr)
        else:
            stats["equality" if mode == "strict" else "subset"] += 1
            note = "" if mode == "strict" else f"（实现另有 {len(i) - len(c)} 项，不属承诺）"
            print(f"OK: {pkg} — {len(c)} 项 wire 签名逐条一致 [{mode}]{note}")

    if ok:
        print(
            "descriptor 级 wire 一致："
            f"严格比对 {stats['equality']} 包 + 子集比对 {stats['subset']} 包"
            f" + 随附 {stats['vendored']} 包（跳过）"
        )
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main())
