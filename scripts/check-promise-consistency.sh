#!/usr/bin/env bash
# ============================================================
# check-promise-consistency.sh — P-Gate 9（文档与承诺一致）的**机械判据**卡口
#
# 为什么需要它：D-05/D-06/D-18 的共同形态是「对外文本互相矛盾」——
# README 说 EXPERIMENTAL、STATUS 说 COMMITTED、contracts README 的版本号停在旧版、
# 白皮书引用一个不存在的文件。这些**没人读错也发现不了**，只能靠机器逐条比。
#
# 判据（全部可机械判定；覆盖 P-Gate 9 的可机械化子集）：
#   1) 免责回归：README.md / README.zh-CN.md 不得再出现
#      `not intended for production use`（U-01 裁定取②），且两份都必须链接
#      生产化计划（承诺分级接续）。
#   2) 悬空引用：五份对外文本里的**仓库内相对链接**必须全部可达
#      （D-06 的形态：引用的文件不存在 = 对任何 clone 都是 404）。
#   3) 契约版本三方一致：CHANGELOG 最新条目 == WHITEPAPER 头部 == contracts README。
#   4) 承诺面一致：STATUS.md 的 COMMITTED 表（包名+期限）== contracts README 承诺
#      表（包名+期限）；且 COMMITTED ∪ STABLE 的包名集合 ↔ `proto/coord/`
#      实际目录集合（双向比对 —— 承诺了却没有 proto、或有 proto 却没进台账，都置红）。
#   5) 审计口径（U-14，2026-09-25）：五份文本不得把第三方审计当门槛或暗示"已审计"——
#      含「第三方(安全)审计 / 独立审计」的行必须带边界语境词（未经/不再/不采买/移出/已按/不得）。
#
# 用法：bash scripts/check-promise-consistency.sh
# 退出码：0 = 全过；1 = 有违规（**故意设计成会红**，见 W2-5 的负控制演练）
# ============================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

if ! command -v python3 >/dev/null 2>&1; then
  echo "FAIL: check-promise-consistency.sh 需要 python3" >&2
  exit 1
fi

python3 - "$REPO_ROOT" <<'PY'
import re
import sys
import pathlib

repo_root = pathlib.Path(sys.argv[1])

S = {
    "readme": repo_root / "README.md",
    "readme_zh": repo_root / "README.zh-CN.md",
    "contracts_readme": repo_root / "apis/contracts/README.md",
    "status": repo_root / "apis/contracts/STATUS.md",
    "whitepaper": repo_root / "apis/contracts/WHITEPAPER.md",
    "changelog": repo_root / "apis/contracts/CHANGELOG.md",
}

failures = []

for name, path in S.items():
    if not path.is_file():
        failures.append(f"缺少文件：{path.relative_to(repo_root)}（判据无法执行）")
if failures:
    for f in failures:
        print(f"FAIL: {f}", file=sys.stderr)
    sys.exit(1)

def read(key):
    return S[key].read_text(encoding="utf-8")

# ── 判据 1：免责声明不得回归 + 必须链接生产化计划 ────────────────────────
# 允许「裁定记录」里**引用**旧措辞（"replaces the former blanket …" /
# 「取代原先…的一揽子措辞」），但**裸声明**必须为零 —— 逐行判定：含旧措辞的行
# 必须同时出现语境词（replaces / 取代 / 原先 / former），否则置红。
PHRASES = {"readme": "not intended for production use", "readme_zh": "不可用于生产环境"}
ALLOW_QUOTE = re.compile(r"replaces|取代|原先|former")
for key, phrase in PHRASES.items():
    for lineno, line in enumerate(read(key).splitlines(), 1):
        if phrase in line and not ALLOW_QUOTE.search(line):
            failures.append(
                f"[P9/U-01] {S[key].name}:{lineno} 出现**裸**声明「{phrase}」"
                f" —— 与 U-01 裁定（取②「限定」）冲突"
            )
    if "production-readiness-plan-2026-09-21.md" not in read(key):
        failures.append(
            f"[P9/U-01] {S[key].name} 未链接生产化计划（承诺分级必须可接续）"
        )

# ── 判据 2：五份文本里的仓库内相对链接必须可达 ───────────────────────────
LINK_RE = re.compile(r"\]\(([^)]+)\)")
SKIP_PREFIXES = ("http://", "https://", "mailto:", "#")

def check_links(key):
    path = S[key]
    for m in LINK_RE.finditer(read(key)):
        raw = m.group(1).strip()
        # 去掉可选的 markdown title：`target "title"`
        target = re.split(r'\s+"', raw, maxsplit=1)[0].strip()
        target = target.strip("<>").strip()
        if not target or target.startswith(SKIP_PREFIXES) or target.startswith("/"):
            continue
        rel = target.split("#", 1)[0]
        if not rel:
            continue
        resolved = (path.parent / rel).resolve()
        if not resolved.exists():
            failures.append(
                f"[P9/D-06] {path.relative_to(repo_root)} 的链接悬空："
                f"`{target}` → {rel} 不存在"
            )

for key in ("readme", "readme_zh", "contracts_readme", "status", "whitepaper"):
    check_links(key)

# ── 判据 5：审计口径（U-14）—— 不得再把第三方审计当门槛/暗示已审计 ──────
AUDIT_PHRASE = re.compile(r"第三方(安全)?审计|独立审计")
AUDIT_CONTEXT = re.compile(r"未经|不再|不采买|移出|已按|不得|former")
for key in ("readme", "readme_zh", "contracts_readme", "status", "whitepaper"):
    for lineno, line in enumerate(read(key).splitlines(), 1):
        if AUDIT_PHRASE.search(line) and not AUDIT_CONTEXT.search(line):
            failures.append(
                f"[P9/U-14] {S[key].name}:{lineno} 出现未带边界语境的审计表述"
                f"（U-14：不得把第三方审计当门槛或暗示已审计）"
            )

# ── 判据 3：契约版本三方一致 ─────────────────────────────────────────────
def must_match(label, pattern, text, flags=re.M):
    m = re.search(pattern, text, flags)
    if not m:
        failures.append(f"[P9/版本] 在 {label} 中解析不到版本（模式：{pattern}）")
        return None
    return m.group(1)

v_changelog = must_match(
    "CHANGELOG.md 最新条目", r"^##\s*\[contracts/v(\d+\.\d+\.\d+)\]", read("changelog")
)
v_whitepaper = must_match(
    "WHITEPAPER.md 头部", r"^>\s*版本：contracts/v(\d+\.\d+\.\d+)", read("whitepaper")
)
v_wp_title = must_match(
    "WHITEPAPER.md 标题", r"^#\s*Coord 平台对外协议白皮书（v(\d+\.\d+\.\d+)）", read("whitepaper")
)
v_contracts_a = must_match(
    "apis/contracts/README.md（契约行）", r"（契约 v(\d+\.\d+\.\d+)", read("contracts_readme")
)
v_contracts_b = must_match(
    "apis/contracts/README.md（白皮书行）", r"协议白皮书 v(\d+\.\d+\.\d+)", read("contracts_readme")
)

versions = {
    "CHANGELOG.md 最新条目": v_changelog,
    "WHITEPAPER.md 头部": v_whitepaper,
    "WHITEPAPER.md 标题": v_wp_title,
    "contracts README（契约行）": v_contracts_a,
    "contracts README（白皮书行）": v_contracts_b,
}
present = {k: v for k, v in versions.items() if v is not None}
if len(set(present.values())) > 1:
    detail = " / ".join(f"{k}={v}" for k, v in present.items())
    failures.append(f"[P9/版本] 契约版本不一致：{detail}")

# ── 判据 4：承诺面一致（STATUS ↔ contracts README ↔ proto 目录） ─────────

def section_lines(text, heading):
    """返回 heading（如 `## 能力承诺面（COMMITTED）`）到下一个 `## ` 之间的行。"""
    m = re.search(rf"^{re.escape(heading)}\s*$", text, re.M)
    if not m:
        failures.append(f"[P9/台账] 在 STATUS.md 中找不到小节：{heading}")
        return []
    rest = text[m.end():]
    nxt = re.search(r"^## ", rest, re.M)
    body = rest[: nxt.start()] if nxt else rest
    return body.splitlines()

status_text = read("status")
committed = {}
for line in section_lines(status_text, "## 能力承诺面（COMMITTED）"):
    m = re.match(
        r"^\|\s*[^|]*\|\s*(coord\.[a-z0-9.]+)\s*\|\s*COMMITTED\s*\|\s*(\d{4}-\d{2}-\d{2})\s*\|",
        line,
    )
    if m:
        committed[m.group(1)] = m.group(2)
stable = set()
for line in section_lines(status_text, "## 底座原语（STABLE）"):
    m = re.match(r"^\|\s*[^|]*\|\s*(coord\.[a-z0-9.]+)\s*\|\s*STABLE\s*\|", line)
    if m:
        stable.add(m.group(1))

readme_text = read("contracts_readme")
readme_rows = {}
for line in readme_text.splitlines():
    m = re.match(
        r"^\|\s*[^|]*\|\s*`?(coord\.[a-z0-9.]+)`?\s*\|\s*(\d{4}-\d{2}-\d{2})\s*\|", line
    )
    if m:
        readme_rows[m.group(1)] = m.group(2)

if not committed:
    failures.append("[P9/台账] STATUS.md 的 COMMITTED 表解析为空（表结构变了？）")
if not readme_rows:
    failures.append("[P9/台账] contracts README 的承诺表解析为空（表结构变了？）")

# 4a) 包名集合一致
if set(committed) != set(readme_rows):
    only_status = sorted(set(committed) - set(readme_rows))
    only_readme = sorted(set(readme_rows) - set(committed))
    failures.append(
        f"[P9/台账] COMMITTED 包名集合不一致：仅 STATUS 有 {only_status}；"
        f"仅 contracts README 有 {only_readme}"
    )

# 4b) 期限逐包一致
for pkg in sorted(set(committed) & set(readme_rows)):
    if committed[pkg] != readme_rows[pkg]:
        failures.append(
            f"[P9/台账] {pkg} 的期限不一致：STATUS={committed[pkg]}，"
            f"contracts README={readme_rows[pkg]}"
        )

# 4c) 台账 ↔ proto 目录 双向一致
proto_root = repo_root / "apis/contracts/proto/coord"
actual_dirs = {p.name for p in proto_root.iterdir() if p.is_dir()}

def pkg_to_dir(pkg):
    return re.sub(r"\.v1$", "", pkg[len("coord."):])

ledger_dirs = {pkg_to_dir(p) for p in set(committed) | stable}
missing_proto = sorted(ledger_dirs - actual_dirs)
extra_proto = sorted(actual_dirs - ledger_dirs)
if missing_proto:
    failures.append(f"[P9/台账] 台账承诺了但没有 proto 目录：{missing_proto}")
if extra_proto:
    failures.append(f"[P9/台账] 有 proto 目录但不在台账（承诺漏登记？）：{extra_proto}")

# ── 汇总 ─────────────────────────────────────────────────────────────────
if failures:
    for f in failures:
        print(f"FAIL: {f}", file=sys.stderr)
    print(
        f"\nP-Gate 9 卡口未通过：{len(failures)} 处违规（对外文本/台账/proto 面互相矛盾）。",
        file=sys.stderr,
    )
    sys.exit(1)

print(
    "promise-consistency OK：免责口径（U-01）未回归；五份文本无悬空引用；"
    f"契约版本 {v_whitepaper or '-'} 三方一致；COMMITTED {len(committed)} 行 + "
    f"STABLE {len(stable)} 行 ↔ proto 目录双向一致；审计口径（U-14）无违规。"
)
PY
