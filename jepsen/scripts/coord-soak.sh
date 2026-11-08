#!/usr/bin/env bash
#
# coord-soak.sh — long-run (72h) soak orchestrator for the coord Jepsen test.
#
# Runs on the CONTROL node as root, inside /root/coord-test. The test runs in
# a detached process group (setsid + nohup) so it survives SSH disconnects.
#
# Usage:
#   ./coord-soak.sh start  [--hours N] [--time-limit S] [--rate R] [--workload W]
#                         [--checker C] [--quiet S] [--disrupt S] [--nemesis N] [--regions N]
#                         [--seed S] [--concurrency C] [--mixture-ratio W,W,W]
#                         [--map-min-deletes N] [--extra "..."]
#                         [--map-min-deletes N]
#   ./coord-soak.sh status
#   ./coord-soak.sh wait   [interval-seconds]
#   ./coord-soak.sh stop
#   ./coord-soak.sh tail   [--lines N]
#   ./coord-soak.sh results
#   ./coord-soak.sh log    [--lines N]
#
# Defaults: hours=72, rate=0.5 ops/s, workload=register, checker=soak,
# quiet=1800s (30min), disrupt=600s (10min), concurrency=1n. --time-limit
# overrides --hours (e.g. --time-limit 120 for a short smoke run). Everything
# maps to the corresponding coord-test CLI options. --regions N (multi-raft
# mode) splits the keyspace into N regions; use with --workload
# multi-register. --seed makes the jittered schedule replayable;
# --mixture-ratio / --map-min-deletes apply to --workload mixture.
set -euo pipefail

PROJECT=/root/coord-test
SOAK_DIR="$PROJECT/soak"
# SOAK_NAME 让第二条浸泡（例如 #272 的 Jepsen v2 `kvv2`）有自己的日志/pid，
# 不会覆盖正在跑的 v1 长跑：SOAK_NAME=kvt -> soak-kvt.log / soak-kvt.pid。
SOAK_NAME="${SOAK_NAME:-}"
LOG="$SOAK_DIR/soak${SOAK_NAME:+-$SOAK_NAME}.log"
PIDFILE="$SOAK_DIR/soak${SOAK_NAME:+-$SOAK_NAME}.pid"
STORE="$PROJECT/store/coord"

mkdir -p "$SOAK_DIR"

# 离线运行：控制机/容器**没有出网**，plain `lein` 会先去解析
# SNAPSHOT 依赖、卡在网络上几分钟才继续（`make quick`/`make checkers` 早已按这条
# 改成默认 `-o`，本脚本此前漏了）。`ONLINE=1` 覆盖（确实需要联网时）。
LEIN_OFFLINE="${LEIN_OFFLINE:--o}"

LEIN_BASE=(LEIN_ROOT=true lein $LEIN_OFFLINE run test
           --nodes-file /root/nodes
           --username root
           --ssh-private-key /root/.ssh/id_ed25519)

usage() {
  sed -n '2,16p' "$0" | sed 's/^# \{0,1\}//'
  exit 1
}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

is_running() {
  [[ -s "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null
}

# check — exit 0 if a soak is running, 1 otherwise (used by the Makefile to
# refuse an `upload` that would clobber a running run).
cmd_check() {
  if is_running; then
    echo "soak is running (pid $(cat "$PIDFILE")) — stop it first: make soak-stop" >&2
    exit 1
  fi
  exit 0
}

# ---------------------------------------------------------------------------
# start
# ---------------------------------------------------------------------------

cmd_start() {
  local hours=72 time_limit="" rate=0.5 workload=register checker=soak quiet=1800 disrupt=600 regions="" nemesis=soak
  # 种子（回放）、并发度、mixture 混合比、map 面的 delete 样本门槛
  # --extra 是**通用透传**（原样拼进 lein 命令），用于新加的选项（例如
  # `--extra "--soak-mix map=40,txn=20,scan=5,watch=15,lease=10 --watch-min-events 200 --lease-min-grants 100 --lease-min-expiries 30"`）。
  # 为什么不一个一个加：每加一个选项就要改三处（usage / 解析 / 命令），漏一处
  # 就会「传了但没生效」——那正是长跑最怕的假绿（门槛没接上）。
  local seed="" concurrency=1n mixture_ratio="" map_min_deletes="" extra=""
  while [[ $# -gt 0 ]]; do
    case "$1" in
      --hours)      hours=$2; shift 2 ;;
      --time-limit) time_limit=$2; shift 2 ;;
      --rate)       rate=$2; shift 2 ;;
      --workload)   workload=$2; shift 2 ;;
      --checker)    checker=$2; shift 2 ;;
      --quiet)      quiet=$2; shift 2 ;;
      --disrupt)    disrupt=$2; shift 2 ;;
      --nemesis)    nemesis=$2; shift 2 ;;
      --regions)    regions=$2; shift 2 ;;
      --seed)       seed=$2; shift 2 ;;
      --concurrency) concurrency=$2; shift 2 ;;
      --mixture-ratio) mixture_ratio=$2; shift 2 ;;
      --map-min-deletes) map_min_deletes=$2; shift 2 ;;
      --extra)      extra="$2"; shift 2 ;;
      *) echo "unknown option: $1" >&2; usage ;;
    esac
  done

  if is_running; then
    echo "soak already running (pid $(cat "$PIDFILE")); stop it first (coord-soak.sh stop)" >&2
    exit 1
  fi

  local time_limit=${time_limit:-$((hours * 3600))}
  echo "== starting coord soak =="
  echo "   time-limit=${time_limit}s (${hours}h) rate=$rate workload=$workload checker=$checker nemesis=$nemesis regions=${regions:-(single-Raft)}"
  echo "   quiet=${quiet}s disrupt=${disrupt}s concurrency=$concurrency seed=${seed:-(random)}"
  echo "   extra: mixture-ratio=${mixture_ratio:-(default 4,2,2)} map-min-deletes=${map_min_deletes:-(off)} extra=${extra:-(none)}"
  echo "   log: $LOG"

  # 证据身份（#303）：72h 一轮最贵的失败是「跑的二进制不对」——Test version 只
  # 反映 jepsen 检出，不证明 server 二进制来自同一棵树。这里把控制机二进制哈希
  # 与源码树固定到日志头，节点侧哈希由 db.clj 的 setup! 写进 jepsen.log，
  # collect-evidence.sh 三者对不上就拒绝出 MANIFEST。
  local ctl_bin="$PROJECT/coord" ctl_real="" ctl_sha="-" commit="-" tree="-"
  local dirty="-"
  if [ -f "$ctl_bin" ]; then
    ctl_real="$(readlink -f "$ctl_bin")"
    ctl_sha="$(sha256sum "$ctl_real" | cut -d' ' -f1)"
  fi
  if git -C "$PROJECT" rev-parse --git-dir >/dev/null 2>&1; then
    commit="$(git -C "$PROJECT" rev-parse HEAD 2>/dev/null || echo -)"
    tree="$(git -C "$PROJECT" write-tree 2>/dev/null || echo -)"
    dirty="$(git -C "$PROJECT" status --porcelain 2>/dev/null | wc -l | tr -d ' ')"
  fi
  echo "   control-binary: ${ctl_sha} ${ctl_real:-$ctl_bin}"
  echo "   control-commit: ${commit}"
  echo "   control-tree:   ${tree}"
  echo "   control-dirty:  ${dirty}"

  # 取证 run 必须从**干净树 + 新鲜二进制**起跑（#174/#303 的教训：一整轮 72h
  # 因 make binary 抢在 merge 前构建而作废）。两个判据都是 mtime/porcelain 级，
  # 秒级可查；确需越界（开发/刻意复现）用 ALLOW_STALE_BINARY=1 / ALLOW_DIRTY_TREE=1。
  if [ "$dirty" != "-" ] && [ "$dirty" != "0" ] && [ "${ALLOW_DIRTY_TREE:-0}" != "1" ]; then
    echo "refusing to start: worktree has ${dirty} uncommitted path(s) — 取证 run 必须可复现" >&2
    echo "  → 先提交/收敛改动，或 ALLOW_DIRTY_TREE=1 显式覆盖（该轮不能当 GA 证据）" >&2
    exit 3
  fi
  if [ -n "$ctl_real" ] && [ "$commit" != "-" ]; then
    local src_ts bin_ts
    # 「二进制是否包含当前源码」看**最后一次改动 server 源码**的提交时间，而不是
    # HEAD：docs/jepsen-only 提交不改二进制（jepsen 代码在运行时从检出读取），用
    # HEAD 会误伤——每次改判据/文档都要重建一个逐字节相同的二进制才能起跑。
    # `:/<path>` 是「相对仓库根」的 pathspec：$PROJECT 是 jepsen/ 子目录，
    # 裸 `coord` 会被解释成 jepsen/coord（不存在）而静默退化成 HEAD 时间。
    src_ts="$(git -C "$PROJECT" log -1 --format=%ct -- \
        :/coord :/coord-server :/coord-core :/coord-proto :/coord-client \
        :/coord-agent :/Cargo.toml :/Cargo.lock 2>/dev/null || echo 0)"
    if [ -z "${src_ts:-}" ] || [ "$src_ts" = "0" ]; then
      src_ts="$(git -C "$PROJECT" log -1 --format=%ct 2>/dev/null || echo 0)"
    fi
    bin_ts="$(stat -c %Y "$ctl_real" 2>/dev/null || echo 0)"
    if [ "$bin_ts" -lt "$src_ts" ] && [ "${ALLOW_STALE_BINARY:-0}" != "1" ]; then
      echo "refusing to start: $ctl_real (mtime $bin_ts) 早于最后一次 server 源码改动 ($src_ts)" >&2
      echo "  → 先 make binary/upload，或 ALLOW_STALE_BINARY=1 显式覆盖（会废掉本轮取证）" >&2
      exit 3
    fi
  fi

  # 增量 op 日志（长跑 watchdog 的输入）：客户端把每条 op 的 invoke/完成追加到这里，watchdog 用
  # 它复判「跑到一半」的历史。每次 start 重建（同名 soak 的旧日志不能混进来）。
  local ops_log="$SOAK_DIR/soak${SOAK_NAME:+-$SOAK_NAME}-ops.edn"
  : > "$ops_log"
  echo "   op-log:  $ops_log"

  # Detach into its own session/process group so neither SSH nor SIGHUP can
  # kill it. The PID recorded is the session leader; `stop` kills the group.
  # The redirect is on `setsid` itself (NOT inside the bash -c string):
  # with `cd && lein ...` bash cannot exec-replace itself, so a redirect
  # inside the string would leave the wrapper holding the SSH channel open
  # and `vagrant ssh` would hang until lein exits.
  setsid bash -c "cd '$PROJECT' && ${LEIN_BASE[*]} --workload $workload --nemesis $nemesis --checker $checker --rate $rate --soak-quiet $quiet --soak-disrupt $disrupt ${regions:+--regions $regions} ${seed:+--seed $seed} ${mixture_ratio:+--mixture-ratio $mixture_ratio} ${map_min_deletes:+--map-min-deletes $map_min_deletes} $extra --op-log '$ops_log' --time-limit $time_limit --concurrency $concurrency" > "$LOG" 2>&1 < /dev/null &
  local pid=$!
  echo "$pid" > "$PIDFILE"
  sleep 5
  if kill -0 "$pid" 2>/dev/null; then
    echo "started (pid $pid)"
    echo "monitor:  ./coord-soak.sh status | tail"
    echo "cancel:   ./coord-soak.sh stop"

    # 运行中快速失败：SOAK_WATCHDOG=1 时用当前 checker 周期复判 op 日志，
    # 判据红就停跑并写 fail-fast.edn（省下剩余的 72h 机时）。
    local wd_pidfile="$SOAK_DIR/soak${SOAK_NAME:+-$SOAK_NAME}-watchdog.pid"
    local wd_log="$SOAK_DIR/soak${SOAK_NAME:+-$SOAK_NAME}-watchdog.log"
    rm -f "$wd_pidfile"
    if [ "${SOAK_WATCHDOG:-0}" = "1" ]; then
      setsid bash -c "cd '$PROJECT' && SOAK_NAME='$SOAK_NAME' ./scripts/soak-watchdog.sh '$ops_log' ${SOAK_WATCHDOG_INTERVAL:-300}" > "$wd_log" 2>&1 < /dev/null &
      echo "$!" > "$wd_pidfile"
      echo "watchdog: pid $! interval=${SOAK_WATCHDOG_INTERVAL:-300}s log=$wd_log"
    fi
  else
    echo "process exited immediately — check $LOG" >&2
    rm -f "$PIDFILE"
    tail -n 20 "$LOG" >&2 || true
    exit 1
  fi
}

# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------

cmd_status() {
  if is_running; then
    local pid; pid=$(cat "$PIDFILE")
    echo "== soak RUNNING (pid $pid) =="
    # Latest progress from the jepsen log
    grep -E "Jepsen test|soak:|elapsed|INFO|WARN|nemesis" "$LOG" 2>/dev/null | tail -n 5 || true
    echo
    echo "-- last 8 log lines --"
    tail -n 8 "$LOG" 2>/dev/null || echo "(log empty)"
    echo
    echo "-- latest store --"
    ls -1dt "$STORE"/*/ 2>/dev/null | head -n 1 | sed "s|$PROJECT|/root/coord-test|"
  else
    if [[ -s "$PIDFILE" ]]; then
      echo "== soak NOT running (stale pid $(cat "$PIDFILE")) =="
    else
      echo "== soak NOT running =="
    fi
    echo "latest result: $(ls -1dt "$STORE"/*/ 2>/dev/null | head -n 1 || echo none)"
    exit 1
  fi
}

# ---------------------------------------------------------------------------
# stop
# ---------------------------------------------------------------------------

cmd_stop() {
  if ! is_running; then
    echo "soak not running"
    rm -f "$PIDFILE"
    exit 0
  fi
  local pid; pid=$(cat "$PIDFILE")
  echo "stopping soak (session $pid)…"
  # TERM the whole process group, then SIGKILL the lein/java leftovers.
  kill -TERM -- "-$pid" 2>/dev/null || true
  sleep 3
  kill -KILL -- "-$pid" 2>/dev/null || true
  pkill -f 'jepsen.coord' 2>/dev/null || true
  # watchdog 是独立 session：停 soak 后它也应在下一拍自行退出；这里兜底收尸，
  # 避免下一条 soak（同名）被上一轮的 watchdog 误停。
  local wd_pidfile="$SOAK_DIR/soak${SOAK_NAME:+-$SOAK_NAME}-watchdog.pid"
  if [ -f "$wd_pidfile" ]; then
    local wd_pid
    wd_pid="$(cat "$wd_pidfile" 2>/dev/null || true)"
    [ -n "$wd_pid" ] && kill -TERM -- "-$wd_pid" 2>/dev/null || true
    rm -f "$wd_pidfile"
  fi
  rm -f "$PIDFILE"
  echo "stopped. results (if any): $STORE"
}

# ---------------------------------------------------------------------------
# tail / results
# ---------------------------------------------------------------------------

cmd_tail() {
  local lines=30
  [[ "${1:-}" == "--lines" ]] && lines=$2
  tail -n "$lines" "$LOG"
}

cmd_results() {
  echo "latest result: $(ls -1dt "$STORE"/*/ 2>/dev/null | head -n 1)"
  echo
  for d in $(ls -1dt "$STORE"/*/ 2>/dev/null | head -n 3); do
    echo "== $d =="
    if [[ -f "$d/results.edn" ]]; then
      grep -E ":valid\?|:ops|:failures|:final-converged|:max-committed" "$d/results.edn" | head -n 20 || true
    else
      echo "   (no results.edn yet)"
    fi
  done
}

cmd_log() {
  local lines=40
  [[ "${1:-}" == "--lines" ]] && lines=$2
  local d; d=$(ls -1dt "$STORE"/*/ 2>/dev/null | head -n 1)
  if [[ -z "$d" ]]; then
    echo "no results yet under $STORE"
    exit 1
  fi
  echo "== $d =="
  tail -n "$lines" "$d/jepsen.log" 2>/dev/null || echo "(no jepsen.log)"
}

# ---------------------------------------------------------------------------

# wait — block until the running soak finishes (exit 0 once idle). 夜间校验
# （make nightly）靠它把「起跑」与「取结果」串起来，而不用在外层主机上猜 pid。
cmd_wait() {
  local interval=${1:-300}
  while is_running; do
    echo "soak running (pid $(cat "$PIDFILE")) — waiting ${interval}s..."
    sleep "$interval"
  done
  echo "soak finished (no running process)"
  exit 0
}

case "${1:-}" in
  start)   shift; cmd_start "$@" ;;
  check)   cmd_check ;;
  wait)    shift; cmd_wait "$@" ;;
  status)  cmd_status ;;
  stop)    cmd_stop ;;
  tail)    shift; cmd_tail "$@" ;;
  results) cmd_results ;;
  log)     shift; cmd_log "$@" ;;
  *) usage ;;
esac
