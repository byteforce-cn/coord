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
# 退出码：0 = soak 结束/被外部停止（watchdog 正常退出）；1 = 判据红、已停跑
# （含判据面缺失 :why "judge-surface-missing"）；2 = 用法/前置错误；
# 3 = 复判器连续失败（看不到判据 = fail-closed 停跑）。
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

# ─── checkpoint 判据面（单一事实源）──────────────────────────────────────────
# 过滤面必须与实际输出的**冒号锚点**一致：rejudge 打印的是 `rejudge-op-<op>: …` /
# `rejudge-history: …` / `rejudge-linear: …` / `rejudge-gates: …` /
# `rejudge-mode: …`。旧的 `^rejudge-(op|history)-` 只命中前者（history 那行是
# 冒号），于是历史读率与 gates 的 judged 面整段从 checkpoint 日志里消失（#322）。
CHECKPOINT_PATTERN='^rejudge-(op-.+|history|linear|gates|mode):'

# stdin = rejudge 的完整输出；输出带缩进的判据面明细。
print_checkpoint() { grep -E "$CHECKPOINT_PATTERN" | sed 's/^/    /' || true; }

# 判据面可见性：没有它就无从知道"红的是哪一类"，按看不到判据 fail-closed。
judged_surface_visible() { grep -q '^rejudge-history:'; }

# 过滤面自检（负控制 + 正控制）：`WATCHDOG_FILTER_SELFTEST=1` 时只跑自检。
# 不放在 CI 里用另一份 pattern 重写一遍：两份 pattern 会各自漂移。
if [[ "${WATCHDOG_FILTER_SELFTEST:-0}" == "1" ]]; then
    sample="$(printf '%s\n' \
        'rejudge-ops: 12345' \
        'rejudge-op-v2-write: invoke=10 ok=10 info=0 fail=0' \
        'rejudge-history: historical-reads=5 unavailable=0 ratio=1.00%' \
        'rejudge-linear: valid=true errors=0' \
        'rejudge-gates: valid=true premise=writes=10')$"
    filtered="$(printf '%s\n' "$sample" | print_checkpoint)"
    for needle in \
        'rejudge-op-v2-write:' 'rejudge-history:' 'rejudge-linear:' 'rejudge-gates:'; do
        if ! printf '%s\n' "$filtered" | grep -q "^    $needle"; then
            echo "SELFTEST FAIL: checkpoint filter dropped $needle" >&2
            echo "got: $filtered" >&2
            exit 1
        fi
    done
    if printf '%s\n' "$filtered" | grep -q 'rejudge-ops:'; then
        echo "SELFTEST FAIL: op count line ('rejudge-ops:') must not be reported as an op class" >&2
        exit 1
    fi
    if ! printf '%s\n' "$sample" | judged_surface_visible; then
        echo "SELFTEST FAIL: judged surface present but reported missing" >&2
        exit 1
    fi
    if printf '%s\n' 'rejudge-op-v2-write: invoke=1 ok=1' | judged_surface_visible; then
        echo "SELFTEST FAIL: judged surface missing but reported visible" >&2
        exit 1
    fi
    echo "SELFTEST ok: checkpoint filter keeps history/linear/gates and the visibility check is not a no-op"
    exit 0
fi

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

    say "checkpoint ops=$ops verdict=$verdict reasons=${reasons:--}"
    if ! printf '%s\n' "$out" | judged_surface_visible; then
        # 判据面不可见 = 复判器输出契约变了（或只吐了半截）：按「看不到判据」
        # fail-closed 停跑，不让它在 72h 后被当成通过。
        abort_run "judge-surface-missing" "$verdict" "$reasons" "${ops:-$lines}"
    fi
    printf '%s\n' "$out" | print_checkpoint

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
