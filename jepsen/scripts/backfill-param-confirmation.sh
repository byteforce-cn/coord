#!/usr/bin/env bash
#
# backfill-param-confirmation.sh —— 把 §5.4 参数确认的存档链接回填进每份
# MANIFEST，并重算该目录的 sha256sums.txt。
#
# 背景：dev.md §5.4 规定「所有验收级 run 必须使用经 coord 团队书面确认的取值，
# 链接进 MANIFEST」，否则证据只能作内部参考、不得用于引入评审。该链接在归档
# 时必然是空的（确认还没发生），所以需要一个**可审计、幂等**的回填动作 ——
# 手工改 26 份文件再手工重算 26 份校验和，迟早会漏。
#
# 用法：
#   scripts/backfill-param-confirmation.sh <存档链接或文本>
#   scripts/backfill-param-confirmation.sh --check     # 只统计，不修改
#
# 行为：
#   * 只替换占位符 `_(待填：issue/邮件存档链接)_`，已填过的目录原样跳过（幂等）；
#   * 只改 MANIFEST.md 的这一行，**不动任何 run 产物**；
#   * 改了哪个目录，就按 `collect-evidence.sh` 的同一顺序重算该目录的
#     sha256sums.txt（MANIFEST.md / summary.txt / run.log / results.edn /
#     history.edn / history.edn.gz / history.txt）。
#
# 退出码：0 = 成功（或 --check 时全部已填）；1 = --check 时仍有待填；2 = 用法错误。
set -uo pipefail

PLACEHOLDER='_(待填：issue/邮件存档链接)_'

REF="${1:-}"
MODE="write"
[[ "$REF" == "--check" || "$REF" == "-c" ]] && { MODE="check"; REF=""; }

if [[ "$MODE" == "write" ]]; then
    if [[ -z "$REF" ]]; then
        echo "usage: $0 <存档链接或文本> | --check" >&2
        exit 2
    fi
    # 拒绝会破坏 markdown 表格 / sed 表达式的字符
    case "$REF" in
        *'|'*) echo "拒绝：存档文本不能含 '|'（会破坏 MANIFEST 的表格）" >&2; exit 2 ;;
        *$'\n'*) echo "拒绝：存档文本不能含换行" >&2; exit 2 ;;
    esac
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="${EVIDENCE_REPO_ROOT:-$(cd "$SCRIPT_DIR/../.." && pwd)}"
cd -P "$REPO_ROOT" >/dev/null 2>&1 || true
if ! git -C "$REPO_ROOT" rev-parse --show-toplevel >/dev/null 2>&1; then
    echo "找不到仓库根（可用 EVIDENCE_REPO_ROOT 覆盖）：$REPO_ROOT" >&2
    exit 2
fi
REPO_ROOT="$(git -C "$REPO_ROOT" rev-parse --show-toplevel)"
EVID="$REPO_ROOT/docs/production/evidence"

shopt -s nullglob
dirs=("$EVID"/*/)
if [[ ${#dirs[@]} -eq 0 ]]; then
    echo "没有找到任何证据目录：$EVID" >&2
    exit 2
fi

esc_sed() { printf '%s' "$1" | sed -e 's/[&\\]/\\&/g'; }
REF_ESC="$(esc_sed "$REF")"

pending=(); nofield=(); filled=0; total=0

for d in "${dirs[@]}"; do
    m="${d}MANIFEST.md"
    [[ -f "$m" ]] || continue
    total=$((total + 1))
    if ! grep -qF "$PLACEHOLDER" "$m"; then
        # 区分「已回填」与「根本没有该字段的旧归档」（9-12 的两份由另一套采集器生成）
        if grep -qF '§5.4 参数确认记录链接' "$m"; then
            filled=$((filled + 1))
        else
            nofield+=("$(basename "$d")")
        fi
        continue
    fi
    pending+=("$(basename "$d")")
    [[ "$MODE" == "check" ]] && continue

    sed -i "s|${PLACEHOLDER}|${REF_ESC}|" "$m" || {
        echo "回填失败：$m" >&2; exit 2
    }
    # 与 collect-evidence.sh 完全相同的顺序与文件集合
    ( cd "$d" && { sha256sum MANIFEST.md summary.txt 2>/dev/null
                   for f in run.log results.edn history.edn history.edn.gz history.txt; do
                       [[ -f "$f" ]] && sha256sum "$f"
                   done; } > sha256sums.txt )
    echo "已回填：$(basename "$d")"
done

echo
if [[ "$MODE" == "check" ]]; then
    echo "共 $total 份归档：待填 ${#pending[@]} / 已回填 $filled / 无该字段 ${#nofield[@]}"
    if [[ ${#nofield[@]} -gt 0 ]]; then
        printf '  - （无 §5.4 字段，由旧采集器生成）%s\n' "${nofield[@]}"
    fi
    if [[ ${#pending[@]} -gt 0 ]]; then
        printf '  - %s\n' "${pending[@]}"
        echo "⇒ 这些归档目前只能作内部参考，不得用于引入评审（dev.md §5.4）。"
        exit 1
    fi
    echo "⇒ 带 §5.4 字段的归档已全部确认，可升为验收级证据。"
    exit 0
fi

echo "完成：本次回填 ${#pending[@]} 份，其余 $filled 份此前已填（共 $total 份）。"
echo "链接：$REF"
echo "提示：MANIFEST 的校验和已重算，run 产物未改动。"
