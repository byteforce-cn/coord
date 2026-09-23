#!/usr/bin/env bash
# 包装一条重型门禁命令：输出**同时**透传到 step 日志并落盘；失败时自动调用注解器。
#
# 用法：bash scripts/ci-run-with-annotations.sh <logfile> <command> [args...]
# 退出码：**透传**被包装命令的退出码（成功 0 / 失败原样）。
#
# 与 `ci-annotate-test-failures.sh` 的关系：本脚本只负责"命令 → 日志文件 + 日志 → 注解"
# 的接线；抽取规则全在注解器里（便于单测/复用）。
set -uo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

LOG="${1:-}"
if [ -z "$LOG" ]; then
  echo "usage: ci-run-with-annotations.sh <logfile> <command> [args...]" >&2
  exit 2
fi
shift
if [ "$#" -eq 0 ]; then
  echo "usage: ci-run-with-annotations.sh <logfile> <command> [args...]" >&2
  exit 2
fi

mkdir -p "$(dirname "$LOG")"

"$@" 2>&1 | tee "$LOG"
status=${PIPESTATUS[0]}

if [ "$status" -ne 0 ]; then
  echo "::error::门禁命令失败（exit ${status}）：$*（完整输出：${LOG}）"
  bash "$SCRIPT_DIR/ci-annotate-test-failures.sh" "$LOG" || true
fi
exit "$status"
