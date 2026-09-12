#!/usr/bin/env bash
# P0-F.4 CI 卡口：非测试代码的 panic 路径计数 = 0
#
# 统计生产代码（排除 tests/ 目录、build.rs、#[cfg(test)] 模块与
# 顶层 #[test]/#[tokio::test]/#[rstest] 测试函数体）中
# unwrap/expect/panic/unimplemented/todo 的数量；非零即失败。
#
# P0-F 收尾批次（2026-08-23）存量已清零：卡口硬性要求 0（无基线豁免）。
# 口径：clippy 以 warn 级别运行全部 5 个 lint，脚本解析 span 位置，
# 剔除测试代码后计数。与决策文档 P0-F"CI 卡口：非测试代码计数 = 0"一致。
set -euo pipefail

cd "$(dirname "$0")/.."

echo "::group::clippy panic-path scan"
# 输出到临时文件（JSON），避免与进度输出混流
# E5：去掉 `|| true` —— clippy 编译失败/未产出结果必须让卡口变红，而不是静默放过。
rm -f /tmp/coord-clippy.json
CARGO_TERM_COLOR=never cargo clippy --workspace --all-targets --message-format json \
    -- -W clippy::unwrap_used -W clippy::expect_used -W clippy::panic \
       -W clippy::unimplemented -W clippy::todo -W clippy::unreachable \
    > /tmp/coord-clippy.json 2>/tmp/coord-clippy.err
echo "::endgroup::"

python3 - /tmp/coord-clippy.json <<'PYEOF'
import json
import sys
from collections import defaultdict

data_path = sys.argv[1]
spans = []
try:
    with open(data_path) as f:
        for line in f:
            try:
                msg = json.loads(line)
            except Exception:
                continue
            if msg.get("reason") != "compiler-message":
                continue
            m = msg.get("message", {})
            code = (m.get("code") or {}).get("code") or ""
            if code not in (
                "clippy::unwrap_used",
                "clippy::expect_used",
                "clippy::panic",
                "clippy::unimplemented",
                "clippy::todo",
                "clippy::unreachable",
            ):
                continue
            for span in m.get("spans", []):
                f = span.get("file_name") or ""
                ln = span.get("line_start") or 0
                if f:
                    spans.append((f, int(ln), code))
except FileNotFoundError:
    # E5：clippy 未产出 JSON 说明卡口没有真正执行——必须失败，不得“treat as pass”。
    print("FAIL: clippy JSON output missing; panic-path gate did not run")
    sys.exit(1)

# 测试目录 / build.rs 直接剔除
non_test = [
    s for s in spans
    if "/tests/" not in s[0] and not s[0].endswith("/build.rs") and "/target/" not in s[0]
]

# 为每个源文件计算测试代码的行区间：
# - `#[cfg(test)]` 模块（括号匹配）；
# - 顶层 `#[test]`/`#[tokio::test]`/`#[rstest]` 测试函数体（P0-F 口径修正：
#   测试函数仅在测试构建编译，不属于生产 panic 路径）。
# 区间内的 span 视为测试代码，不计入卡口。
def test_regions(path):
    try:
        with open(path, encoding="utf-8", errors="ignore") as f:
            lines = f.readlines()
    except OSError:
        return []
    regions = []
    i = 0
    n = len(lines)

    # 行内括号计数（忽略字符串/字符字面量/注释中的括号；支持 r#""# 原始字符串）。
    # 状态跨行传递（原始字符串可跨多行）；返回 (delta, in_str, raw_delim, escape, in_block)。
    def brace_delta(line, in_str, raw_delim, escape, in_block):
        delta = 0
        k = 0
        ln = len(line)
        while k < ln:
            if in_block:
                if k + 1 < ln and line[k : k + 2] == "*/":
                    in_block = False
                    k += 2
                    continue
                k += 1
                continue
            ch = line[k]
            nxt = line[k + 1] if k + 1 < ln else ""
            if in_str is not None:
                if in_str == '"':
                    if escape:
                        escape = False
                    elif ch == "\\":
                        escape = True
                    elif ch == '"':
                        in_str = None
                else:  # 原始字符串：等待 '"' + raw_delim
                    if ch == '"' and line[k + 1 : k + 1 + len(raw_delim)] == raw_delim:
                        in_str = None
                        k += 1 + len(raw_delim)
                        continue
                k += 1
                continue
            if ch == "/" and nxt == "*":
                in_block = True
                k += 2
                continue
            if ch == "/" and nxt == "/":
                break  # 行注释至行尾
            if ch == '"':
                in_str = '"'
                k += 1
                continue
            if ch == "r" and (nxt == '"' or nxt == "#"):
                # r"..." / r#"..."# / r##"..."## 原始字符串
                d = ""
                j = k + 1
                while j < ln and line[j] == "#":
                    d += "#"
                    j += 1
                if j < ln and line[j] == '"':
                    in_str = "raw"
                    raw_delim = d
                    k = j + 1
                    continue
            if ch == "'":
                # 字符/字节字面量：'x'、'\n'、b'x'（后随闭合引号才视为字面量）；
                # 生命周期（'static、'a）无闭合引号，按普通代码计数。
                j = k + 1
                while j < ln and j <= k + 5 and line[j] != "'":
                    j += 1
                if j < ln and j <= k + 5 and line[j] == "'":
                    k = j + 1
                    continue
            if ch == "{":
                delta += 1
            elif ch == "}":
                delta -= 1
            k += 1
        return delta, in_str, raw_delim, escape, in_block

    def find_open(start):
        j = start
        while j < n and j <= start + 3:
            if "{" in lines[j]:
                return j
            j += 1
        return None

    def find_close(open_line):
        depth = 0
        k = open_line
        in_str = None
        raw_delim = ""
        escape = False
        in_block = False
        while k < n:
            delta, in_str, raw_delim, escape, in_block = brace_delta(
                lines[k], in_str, raw_delim, escape, in_block
            )
            depth += delta
            if depth <= 0 and k > open_line:
                return k
            k += 1
        return None

    while i < n:
        stripped = lines[i].strip()
        if stripped == "#[cfg(test)]":
            open_line = find_open(i)
            if open_line is None:
                i += 1
                continue
            closed = find_close(open_line)
            if closed is not None:
                regions.append((open_line + 1, closed + 1))
                i = closed + 1
                continue
        # 顶层测试函数：`#[test]` / `#[tokio::test(...)]` / `#[rstest]`
        if (
            stripped.startswith("#[test]")
            or stripped.startswith("#[tokio::test")
            or stripped.startswith("#[rstest]")
        ):
            j = i + 1
            # 跳过连续属性行（如 #[ignore]、#[should_panic]）
            while j < n and j <= i + 6 and lines[j].strip().startswith("#["):
                j += 1
            if j < n and "fn " in lines[j] and "{" in lines[j]:
                closed = find_close(j)
                if closed is not None:
                    regions.append((j + 1, closed + 1))
                    i = closed + 1
                    continue
        i += 1
    return regions

violations = []
regions_cache = {}
for f, ln, code in non_test:
    regions = regions_cache.get(f)
    if regions is None:
        regions = test_regions(f)
        regions_cache[f] = regions
    in_test = any(lo <= ln <= hi for lo, hi in regions)
    if not in_test:
        violations.append((f, ln, code))

if violations:
    # P0-F 收尾批次完成（2026-08-23）：存量全部清零，卡口硬性要求 0
    print("NEW panic-path violations in production code (non-test):")
    for f, ln, code in violations:
        print(f"  {f}:{ln}: {code}")
    print(f"TOTAL: {len(violations)}")
    print("Production code must not add unwrap/expect/panic/unimplemented/todo (P0-F).")
    sys.exit(1)
else:
    print("Panic-path check passed: 0 violations in production code.")
PYEOF
