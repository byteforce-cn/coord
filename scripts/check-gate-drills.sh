#!/usr/bin/env bash
# ============================================================
# check-gate-drills.sh — 门禁/告警的**自检**卡口（W2-5 / W5-3）
#
# 为什么需要它：本仓库反复出现的失效形态不是「门禁写错了」，而是
# 「门禁写了但不生效」或「文档承诺了但没人接线」（第四轮 §3.9.6 / §3.13）。
# 因此对**运维面**也立一道可机械判定的卡口：
#
#   判据 1（W5-3）每条 Prometheus 告警必须有非空 `runbook_url`。
#   判据 2（W5-3）每个 runbook_url 的锚点必须在目标文档里**真实存在**
#                 —— 否则告警会指到一个 404 的处置入口，比没有还坏
#                 （运维以为自己有 runbook）。
#   判据 3（W2-5）告警名与 runbook 里的处置小节必须一一对应（没有孤儿小节）。
#
# 用法：bash scripts/check-gate-drills.sh
# 退出码：0 = 全过；1 = 有违规（**故意设计成会红**，见 W2-5 的负控制演练）
# ============================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RULES="$REPO_ROOT/monitoring/prometheus-rules.yml"

if [[ ! -f "$RULES" ]]; then
  echo "FAIL: 找不到告警规则文件：$RULES" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "FAIL: check-gate-drills.sh 需要 python3" >&2
  exit 1
fi

python3 - "$REPO_ROOT" "$RULES" <<'PY'
import re
import sys
import pathlib

repo_root = pathlib.Path(sys.argv[1])
rules_path = pathlib.Path(sys.argv[2])

text = rules_path.read_text(encoding="utf-8")

# ── 解析告警块：`- alert: NAME` 到下一个 `- alert:` 或文件结束 ──
alerts = []
matches = list(re.finditer(r"^\s*-\s*alert:\s*(\S+)\s*$", text, re.M))
for i, m in enumerate(matches):
    name = m.group(1)
    end = matches[i + 1].start() if i + 1 < len(matches) else len(text)
    body = text[m.start():end]
    url_match = re.search(r"^\s*runbook_url:\s*[\"']?([^\"'\s]+)[\"']?\s*$", body, re.M)
    alerts.append((name, url_match.group(1) if url_match else None))

failures = []

# ── 判据 1：每条告警都有 runbook_url ──
if not alerts:
    failures.append("告警规则文件里没有解析到任何 `- alert:`（解析器或文件结构变了？）")
for name, url in alerts:
    if not url:
        failures.append(f"[W5-3] 告警 {name} 缺少 runbook_url")

# ── GitHub 风格锚点 slug 化（与 github.com 的渲染规则一致） ──
def slug(heading: str) -> str:
    s = heading.strip().lower()
    # 去掉 markdown 行内标记字符
    s = s.replace("`", "")
    # 保留：字母/数字/中日韩等 Unicode 字母/空格/连字符
    kept = []
    for ch in s:
        if ch == " " or ch == "-":
            kept.append(ch)
        elif ch.isalnum():
            kept.append(ch)
        # 其余（标点、—、≥、>、/ 等）丢弃
    s = "".join(kept)
    s = s.replace(" ", "-")
    return s

def headings_with_slug(path: pathlib.Path):
    """返回 {slug: 原标题}。"""
    out = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.startswith("#"):
            title = line.lstrip("#").strip()
            out[slug(title)] = title
    return out

# ── 判据 2：锚点必须真实存在 ──
BLOB = "/blob/main/"
anchors_used = {}
for name, url in alerts:
    if not url:
        continue
    if BLOB not in url:
        failures.append(f"[W5-3] 告警 {name} 的 runbook_url 不是 main 分支文档链接：{url}")
        continue
    rel, _, anchor = url.partition("#")
    rel = rel.split(BLOB, 1)[1]
    target = repo_root / rel
    if not target.is_file():
        failures.append(f"[W5-3] 告警 {name} 的 runbook_url 指向不存在的文件：{rel}")
        continue
    if not anchor:
        failures.append(f"[W5-3] 告警 {name} 的 runbook_url 没有锚点（指到文档顶部＝没指到处置步骤）")
        continue
    hs = headings_with_slug(target)
    if anchor not in hs:
        failures.append(
            f"[W5-3] 告警 {name} 的锚点 #{anchor} 在 {rel} 里不存在"
            f"（最接近的标题：{hs.get(anchor, '——')}）"
        )
    anchors_used.setdefault(rel, set()).add(anchor)

# ── 判据 3：runbook 的**告警处置段**里没有孤儿小节（每个小节都被至少一条告警引用） ──
# 只检查「§6 告警处置」之后的三级标题：§1–§5 是通用运维流程，本来就不对应告警。
for rel, used in anchors_used.items():
    target = repo_root / rel
    in_alert_section = False
    for line in target.read_text(encoding="utf-8").splitlines():
        if line.startswith("## "):
            in_alert_section = "告警处置" in line
            continue
        if not in_alert_section or not line.startswith("### "):
            continue
        s = slug(line.lstrip("#").strip())
        if s not in used:
            failures.append(
                f"[W2-5] {rel} 的告警处置小节“{line.lstrip('#').strip()}”没有任何告警指向它（孤儿 runbook）"
            )

print(f"check-gate-drills: {len(alerts)} 条告警，{len(anchors_used)} 个 runbook 文档")
if failures:
    print("VIOLATIONS:")
    for f in failures:
        print("  - " + f)
    sys.exit(1)
print("OK: 每条告警都有可达的 runbook 处置入口，且无孤儿小节")
PY
