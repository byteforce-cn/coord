#!/usr/bin/env bash
#
# 长跑 watchdog（fast-fail 阶梯的运行中判据）：周期性用**当前** checker/gates 复判增量 op 日志，
# 判据一红就停跑，而不是等 72h 收尾 checker。
#
# 用法（控制机，coord-test 项目根目录）：
#   ./scripts/soak-watchdog.sh <op-log> [interval-seconds]
#
# 环境：
#   SOAK_NAME=kvt            停哪一条 soak（与 coord-soak.sh 的 SOAK_NAME 一致）
#   WORKLOAD=kvv2            传给 rejudge 的 workload
#   WATCHDOG_ONCE=1          只判一次（测试/调试）
#   WATCHDOG_FAIL_STREAK=2   连续多少次红才停（只用于可抖动的判据，见下）
#   WATCHDOG_MIN_OPS=200     op 少于这个数只记录、不启 JVM（日志还在长）
#   REJUDGE_EXTRA=""         额外的 rejudge 参数（如 --history-max-ratio 0，
#                            用于验证 watchdog 本身真的会停跑）
#
# 复判总带 `--partial`：增量日志还在长，gates 的「有样本却没判」在前段是正常
# 形态（样本没攒够），不能当违规；mvcc 的 insufficient-sample 也照报不判红。
#
# 判定策略（为什么不是「红就停」）：
#   * `:mvcc-violation` / `:history-unavailable-rate` / 判据空转（*-not-judged）
#     / `:duplicate-values` / RTO 类 —— **立即停**：这些不是扰动带来的抖动；
#   * `:op-liveness` / `:quiet-availability` —— 连续 N 次才停：分区窗口内单类
#     op 的瞬时下滑会把单次 checkpoint 误判成系统性故障；
#   * `insufficient-sample` —— 不算红（前段样本没到门槛）。
#
# 退出码：0 = soak 结束/被外部停止（watchdog 正常退出）；1 = 判据红、已停跑；
# 2 = 用法/前置错误；3 = 复判器连续失败（看不到判据 = fail-closed 停跑）。
set -uo pipefail

PROJECT=/root/coord-test
SOAK_DIR="$PROJECT/soak"
SOAK_NAME="${SOAK_NAME:-}"
PIDFILE="$SOAK_DIR/soak${SOAK_NAME:+-$SOAK_NAME}.pid"
OP_LOG="${1:-}"
INTERVAL="${2:-300}"
FAIL_FAST="$SOAK_DIR/fail-fast${SOAK_NAME:+-$SOAK_NAME}.edn"
WORKLOAD="${WORKLOAD:-kvv2}"
ONCE="${WATCHDOG_ONCE:-0}"
FAIL_STREAK="${WATCHDOG_FAIL_STREAK:-2}"
MIN_OPS="${WATCHDOG_MIN_OPS:-200}"
REJUDGE_EXTRA="${REJUDGE_EXTRA:-}"

say() { echo "[$(date -u +%FT%TZ)] $*"; }

if [[ -z "$OP_LOG" ]]; then
    echo "usage: $0 <op-log> [interval-seconds]" >&2
    exit 2
fi

soak_running() {
    [[ -f "$PIDFILE" ]] || return 1
    local pid
    pid="$(cat "$PIDFILE" 2>/dev/null || true)"
    [[ -n "$pid" ]] && kill -0 "$pid" 2>/dev/null
}

# 立即停跑 / 达到连续阈值后停跑：写证据 + 停 soak（不 kill 自己）。
abort_run() {
    local why="$1" verdict="$2" reasons="$3" ops="$4"
    say "FAIL-FAST: $why (verdict=$verdict reasons=$reasons ops=$ops)"
    {
        echo "{:time \"$(date -u +%FT%TZ)\""
        echo " :why \"$why\""
        echo " :verdict \"$verdict\""
        echo " :reasons \"$reasons\""
        echo " :ops $ops"
        echo " :op-log \"$OP_LOG\"}"
    } > "$FAIL_FAST"
    # 先摘掉自己的 pid：coord-soak.sh stop 会按 watchdog pid 收尸，不摘会把自己杀掉。
    rm -f "$SOAK_DIR/soak${SOAK_NAME:+-$SOAK_NAME}-watchdog.pid"
    ( cd "$PROJECT" && SOAK_NAME="$SOAK_NAME" ./scripts/coord-soak.sh stop ) || true
    exit 1
}

streak=0
judge_failures=0

while :; do
    if ! soak_running; then
        say "soak is not running — watchdog exits"
        exit 0
    fi

    lines="$(wc -l < "$OP_LOG" 2>/dev/null || echo 0)"
    if [[ "$lines" -lt "$MIN_OPS" ]]; then
        say "waiting: $lines/$MIN_OPS ops in $OP_LOG"
        if [[ "$ONCE" == "1" ]]; then exit 0; fi
        sleep "$INTERVAL"
        continue
    fi

    out="$( cd "$PROJECT" && SOAK_NAME="$SOAK_NAME" timeout 600 \
        env LEIN_ROOT=true lein -o run -m clojure.main scripts/rejudge.clj "$OP_LOG" \
            --workload "$WORKLOAD" --partial $REJUDGE_EXTRA 2>&1 )"
    verdict="$(printf '%s\n' "$out" | awk -F': ' '/^rejudge-verdict:/{print $2; exit}')"
    reasons="$(printf '%s\n' "$out" | awk -F': ' '/^rejudge-reason:/{print $2; exit}')"
    ops="$(printf '%s\n' "$out" | awk -F': ' '/^rejudge-ops:/{print $2; exit}')"

    if [[ -z "$verdict" ]]; then
        judge_failures=$((judge_failures + 1))
        say "rejudge produced no verdict ($judge_failures/3) — tail: $(printf '%s\n' "$out" | tail -1)"
        if [[ "$judge_failures" -ge 3 ]]; then
            abort_run "judge-unavailable" "unknown" "" "${ops:-$lines}"
        fi
        if [[ "$ONCE" == "1" ]]; then exit 3; fi
        sleep "$INTERVAL"
        continue
    fi
    judge_failures=0

    # 进度快照：逐 op 比率，红了也能直接看到是哪一类。
    say "checkpoint ops=$ops verdict=$verdict reasons=${reasons:--}"
    printf '%s\n' "$out" | grep -E '^rejudge-(op|history)-' | sed 's/^/    /' || true

    case "$verdict" in
        valid|insufficient-sample)
            streak=0
            ;;
        invalid)
            # 只有「可因扰动瞬时不稳」的两个判据走连续阈值；其余（含 *-not-judged、
            # mvcc、history-rate、RTO）立即停。混在一起时以任一非瞬态理由为准。
            all_transient=1
            for r in $reasons; do
                case "$r" in
                    :op-liveness|:quiet-availability) ;;
                    *) all_transient=0 ;;
                esac
            done
            if [[ "$all_transient" == "1" ]]; then
                streak=$((streak + 1))
                say "tolerated violation $streak/$FAIL_STREAK (transient-class: $reasons)"
                if [[ "$streak" -ge "$FAIL_STREAK" ]]; then
                    abort_run "violation-sustained" "$verdict" "$reasons" "$ops"
                fi
            else
                abort_run "violation" "$verdict" "$reasons" "$ops"
            fi
            ;;
        *)
            say "unknown verdict '$verdict' — treated as judge failure"
            judge_failures=$((judge_failures + 1))
            if [[ "$judge_failures" -ge 3 ]]; then
                abort_run "judge-unavailable" "$verdict" "$reasons" "$ops"
            fi
            ;;
    esac

    if [[ "$ONCE" == "1" ]]; then exit 0; fi
    sleep "$INTERVAL"
done
