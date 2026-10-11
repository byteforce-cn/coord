#!/usr/bin/env bash
# ============================================================
# check-consumer-doc-labels.sh — 消费方文档状态标签一致性守卫（#44）
#
# 背景：契约状态存在两套口径的漂移史——`apis/contracts/STATUS.md`（**唯一事实源**）
# 已把 cache / mq / workflow / scheduler / storage 等提升为 COMMITTED，而消费方
# 文档（consumer-contract.md / SDK README）曾长期保留 EXPERIMENTAL 标注，
# 直接影响消费方选型与升级策略。
#
# 判据（两文件当前均应为零）：
#   1) docs/production/consumer-contract.md 不得出现 EXPERIMENTAL 标注；
#   2) coord-java-sdk/README.md 不得出现 EXPERIMENTAL 标注。
#
# 本门不表示「永远不许有实验面」：未来真要以实验包开放能力，须走 WHITEPAPER 的
# 实验包流程，并**显式更新本门**（出现标注即需显式处置，不得静默漂移）。
#
# 负控制（自检）：向任一受检文件注入一行 ``EXPERIMENTAL`` ⇒ 本脚本必须 exit 1。
# ============================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
fail=0

check_file() {
  local f="$1"
  if [[ ! -f "$f" ]]; then
    echo "FAIL: 受检文件不存在：$f" >&2
    fail=1
    return
  fi
  local hits
  hits="$(grep -n 'EXPERIMENTAL' "$f" || true)"
  if [[ -n "$hits" ]]; then
    echo "FAIL: $f 出现 EXPERIMENTAL 标注 —— 与 STATUS.md（唯一事实源）口径冲突：" >&2
    echo "$hits" >&2
    echo "      处置：与 STATUS.md 对齐标签；确为实验面的能力须走实验包流程。" >&2
    fail=1
  fi
}

check_file "$REPO_ROOT/docs/production/consumer-contract.md"
check_file "$REPO_ROOT/coord-java-sdk/README.md"

if (( fail == 0 )); then
  echo "OK: 消费方文档状态标签与 STATUS.md 口径一致（无 EXPERIMENTAL 残留）。"
fi
exit "$fail"
