#!/usr/bin/env bash
# 清理**测试遗留**的 coord 进程（进程级测试在测试进程被 kill / 超时中断时，
# 其 spawn 的 `coord server` / `coord agent` 子进程会变成孤儿常驻）。
#
# 为什么需要它：本地反复跑全量套件会积累几十个孤儿 server（实测积累 30 个、
# 最久 11 小时），它们持续占用 CPU / 端口 / 临时目录，导致后续进程级套件出现
# "raft write timed out (no quorum?)" 这类**看起来像产品 bug 的假红**。
#
# 安全性：**只**清理数据目录（或进程级测试的 `--agent-config` 配置路径）位于临时
# 目录下（`/tmp/.tmp*`、`$TMPDIR/.tmp*`）的进程 —— 即测试用临时数据目录；人工启动
# 的集群（真实 data-dir / 真实配置路径）不会被碰。
#
# 用法：
#   scripts/kill-stray-coord-procs.sh          # 清理
#   scripts/kill-stray-coord-procs.sh --dry-run
set -euo pipefail

DRY_RUN=0
[[ "${1:-}" == "--dry-run" ]] && DRY_RUN=1

# 第三轮复核 §5.5：此脚本此前在 macOS 上**直接失败**（`mapfile: command not found`，
# bash 4+ 内置命令，而 macOS 自带 bash 3.2）——set -e 下第一行就退出，什么都没清理。
# 实测其执行后仍残留 29 个测试孤儿进程（load average 5.79/12 核），正是厂商自述的
# "随机 no quorum 假红"的成因。CI（ubuntu）不受影响，但在 macOS 上开发的人拿不到
# 这层保护。现改为不使用 mapfile 的 POSIX 写法。
command -v pgrep >/dev/null 2>&1 || {
    echo "pgrep not found; cannot scan for stray coord processes" >&2
    exit 0
}

# 临时目录根：Linux 为 /tmp，macOS 为 $TMPDIR（/var/folders/...）。两者都覆盖。
TMP_ROOTS="/tmp"
if [[ -n "${TMPDIR:-}" && "${TMPDIR%/}" != "/tmp" ]]; then
    TMP_ROOTS="/tmp|${TMPDIR%/}"
fi

# 匹配两类测试进程：
#   1. `coord server|agent ... --data-dir <tmp>/.tmp*` —— 多数套件显式传 `--data-dir`；
#   2. `coord agent --agent-config <tmp>/.tmp*/agent.toml` —— 进程级插件套件（如
#      `plugin_real_process_test`）只给配置文件，data_dir 写在配置里，命令行上**没有**
#      `--data-dir`，旧 pattern 因此漏掉这类孤儿 agent。
PATTERN="target/(debug|release)/coord (server|agent).*(--data-dir|--agent-config) (${TMP_ROOTS})/\.tmp"

# 不带 mapfile 的 PID 收集（每行一个 PID）
collect_pids() {
    pgrep -f "$PATTERN" 2>/dev/null || true
}

PIDS="$(collect_pids)"
if [[ -z "${PIDS//[[:space:]]/}" ]]; then
    echo "no stray coord test processes found"
    exit 0
fi
# shellcheck disable=SC2086  # 此处按空白切分 PID 列表是刻意的
COUNT=$(printf '%s\n' $PIDS | wc -l | tr -d ' ')

echo "found ${COUNT} stray coord test process(es):"
# shellcheck disable=SC2086
ps -o pid,etime,command -p "$(printf '%s' "$PIDS" | tr '\n' ',')" 2>/dev/null | head -40 || true

if [[ "$DRY_RUN" -eq 1 ]]; then
    echo "(dry-run: nothing killed)"
    exit 0
fi

# shellcheck disable=SC2086
kill $PIDS 2>/dev/null || true
for _ in $(seq 1 10); do
    sleep 0.3
    pgrep -f "$PATTERN" >/dev/null 2>&1 || break
done
# 仍然存活（卡在 IO / 忽略 SIGTERM）→ SIGKILL
REMAIN="$(collect_pids)"
if [[ -n "${REMAIN//[[:space:]]/}" ]]; then
    # shellcheck disable=SC2086
    echo "escalating to SIGKILL for $(printf '%s\n' $REMAIN | wc -l | tr -d ' ') process(es)"
    # shellcheck disable=SC2086
    kill -9 $REMAIN 2>/dev/null || true
fi
echo "stray coord test processes cleaned"
