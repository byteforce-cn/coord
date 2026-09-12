#!/usr/bin/env bash
# 清理**测试遗留**的 coord 进程（进程级测试在测试进程被 kill / 超时中断时，
# 其 spawn 的 `coord server` / `coord agent` 子进程会变成孤儿常驻）。
#
# 为什么需要它：本地反复跑全量套件会积累几十个孤儿 server（实测积累 30 个、
# 最久 11 小时），它们持续占用 CPU / 端口 / 临时目录，导致后续进程级套件出现
# "raft write timed out (no quorum?)" 这类**看起来像产品 bug 的假红**。
#
# 安全性：**只**清理数据目录位于临时目录下（`/tmp/.tmp*`、`$TMPDIR/.tmp*`）
# 的进程 —— 即测试用临时数据目录；人工启动的集群（真实 data-dir）不会被碰。
#
# 用法：
#   scripts/kill-stray-coord-procs.sh          # 清理
#   scripts/kill-stray-coord-procs.sh --dry-run
set -euo pipefail

DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

PATTERN='target/(debug|release)/coord (server|agent).*--data-dir (/tmp|'"${TMPDIR:-/tmp}"')/\.tmp'

mapfile -t PIDS < <(pgrep -f "$PATTERN" || true)
if [[ ${#PIDS[@]} -eq 0 ]]; then
    echo "no stray coord test processes found"
    exit 0
fi

echo "found ${#PIDS[@]} stray coord test process(es):"
ps -o pid,etimes,cmd -p "$(IFS=,; echo "${PIDS[*]}")" 2>/dev/null | head -40

if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "(dry-run: nothing killed)"
    exit 0
fi

kill "${PIDS[@]}" 2>/dev/null || true
for _ in $(seq 1 10); do
    sleep 0.3
    pgrep -f "$PATTERN" >/dev/null 2>&1 || break
done
# 仍然存活（卡在 IO / 忽略 SIGTERM）→ SIGKILL
mapfile -t REMAIN < <(pgrep -f "$PATTERN" || true)
if [[ ${#REMAIN[@]} -gt 0 ]]; then
    echo "escalating to SIGKILL for ${#REMAIN[@]} process(es)"
    kill -9 "${REMAIN[@]}" 2>/dev/null || true
fi
echo "stray coord test processes cleaned"
