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
# mode, T4.5) splits the keyspace into N regions; use with --workload
# multi-register. --seed (T0.5) makes the jittered schedule replayable;
# --mixture-ratio / --map-min-deletes apply to --workload mixture (T1.5).
set -euo pipefail

PROJECT=/root/coord-test
SOAK_DIR="$PROJECT/soak"
LOG="$SOAK_DIR/soak.log"
PIDFILE="$SOAK_DIR/soak.pid"
STORE="$PROJECT/store/coord"

mkdir -p "$SOAK_DIR"

# 离线运行（F-10/F-22）：控制机/容器**没有出网**，plain `lein` 会先去解析
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
  # T0.5/T1.5：种子（回放）、并发度、mixture 混合比、map 面的 §5.1 delete 样本门槛
  # T6.1：--extra 是**通用透传**（原样拼进 lein 命令），用于新加的选项（例如
  # `--extra "--soak-mix map=40,txn=20,scan=5,watch=15,lease=10 --watch-min-events 200 --lease-min-grants 100 --lease-min-expiries 30"`）。
  # 为什么不一个一个加：每加一个选项就要改三处（usage / 解析 / 命令），漏一处
  # 就会「传了但没生效」——那正是 T2.2 长跑最怕的假绿（门槛没接上）。
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

  # Detach into its own session/process group so neither SSH nor SIGHUP can
  # kill it. The PID recorded is the session leader; `stop` kills the group.
  # The redirect is on `setsid` itself (NOT inside the bash -c string):
  # with `cd && lein ...` bash cannot exec-replace itself, so a redirect
  # inside the string would leave the wrapper holding the SSH channel open
  # and `vagrant ssh` would hang until lein exits.
  setsid bash -c "cd '$PROJECT' && ${LEIN_BASE[*]} --workload $workload --nemesis $nemesis --checker $checker --rate $rate --soak-quiet $quiet --soak-disrupt $disrupt ${regions:+--regions $regions} ${seed:+--seed $seed} ${mixture_ratio:+--mixture-ratio $mixture_ratio} ${map_min_deletes:+--map-min-deletes $map_min_deletes} $extra --time-limit $time_limit --concurrency $concurrency" > "$LOG" 2>&1 < /dev/null &
  local pid=$!
  echo "$pid" > "$PIDFILE"
  sleep 5
  if kill -0 "$pid" 2>/dev/null; then
    echo "started (pid $pid)"
    echo "monitor:  ./coord-soak.sh status | tail"
    echo "cancel:   ./coord-soak.sh stop"
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

# wait — block until the running soak finishes (exit 0 once idle). 夜间门禁
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
