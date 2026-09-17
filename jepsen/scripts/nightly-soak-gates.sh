#!/usr/bin/env bash
#
# nightly-soak-gates.sh —— 「soak 常态化」的 lab 侧夜间门禁（T6.x / §7.2）。
#
# 为什么不在 CI 里跑：本仓的 jepsen 测试依赖 **jepsen 0.3.14-SNAPSHOT**（本地
# lein install 的产物，不在 Maven Central），而且 ≥2h 的 run 按 §7.2 明确
# 「禁止在 CI runner 起跑」。所以常态化的正确落点是 lab：夜间无人时段按
# 优先级 `T6 系列 > M5 冒烟 > M3/M5 长跑 > 日常短矩阵` 跑这一串门禁。
#
# 跑什么（顺序即优先级，前一步红就不进下一步）：
#   1. make checkers    —— 17 套 fixture（含负控制）必须全绿；红则拒绝起跑
#   2. make matrix-m1   —— T1.1/T1.2/T1.3/T1.5/T1.4 × none|kill
#   3. make matrix-m2   —— T2.1 watch / T2.2 lease × none|kill|pause|partition-halves
#   4. make soak WORKLOAD=soakfull —— 组合浸泡（默认 2h；72h 用 SOAK_SECONDS=259200）
#      + make soak-wait + make soak-results（退出码/摘要进日志）
#
# 用法（在 jepsen/lab 的上层，即仓库里）：
#   jepsen/scripts/nightly-soak-gates.sh
#   SOAK_SECONDS=259200 SEED=42 JEPSEN_PROVIDER=vagrant jepsen/scripts/nightly-soak-gates.sh
#
# 环境变量：
#   JEPSEN_PROVIDER  docker(默认) | vagrant
#   SOAK_SECONDS     第 4 步的时长（默认 7200 = 2h；72h = 259200）
#   SEED             固定种子（默认 42，T0.5 可回放）
#   SOAK_MIX         组合浸泡的面比例（默认 = T6.1 里已实现的部分）
#   SOAK_MIN_SAMPLE / SOAK_WATCH_MIN_EVENTS / SOAK_LEASE_MIN_GRANTS /
#   SOAK_LEASE_MIN_EXPIRIES —— §5.1 的样本门槛（默认取验收级取值）
#   LOG              日志路径（默认 /tmp/coord-jepsen-nightly-<utc>.log）
#   SKIP_LONG=1      只跑 1–3 步（PR 级；不跑长跑）
#
# 退出码：0 = 全部通过；非 0 = 第一个失败的门禁（日志里有 `NIGHTLY FAILED at ...`）。
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
LAB_DIR="$(cd "$HERE/../lab" && pwd)"

PROVIDER="${JEPSEN_PROVIDER:-docker}"
SOAK_SECONDS="${SOAK_SECONDS:-7200}"
SEED="${SEED:-42}"
SOAK_MIX="${SOAK_MIX:-map=40,txn=20,scan=5,watch=15,lease=10}"
SOAK_MIN_SAMPLE="${SOAK_MIN_SAMPLE:-10}"
SOAK_WATCH_MIN_EVENTS="${SOAK_WATCH_MIN_EVENTS:-200}"
SOAK_LEASE_MIN_GRANTS="${SOAK_LEASE_MIN_GRANTS:-100}"
SOAK_LEASE_MIN_EXPIRIES="${SOAK_LEASE_MIN_EXPIRIES:-30}"
LOG="${LOG:-/tmp/coord-jepsen-nightly-$(date -u +%Y%m%dT%H%M%SZ).log}"

say() { echo "[$(date -u +%Y-%m-%dT%H:%M:%SZ)] $*" | tee -a "$LOG"; }

run_gate() {
  local name="$1"; shift
  say "== nightly gate: $name"
  if "$@" 2>&1 | tee -a "$LOG"; then
    say "   OK: $name"
  else
    say "NIGHTLY FAILED at $name (see $LOG)"
    exit 1
  fi
}

mkdir -p "$(dirname "$LOG")"
say "nightly start: provider=$PROVIDER soak=${SOAK_SECONDS}s seed=$SEED mix=$SOAK_MIX"
say "log: $LOG"

cd "$LAB_DIR"

run_gate "checkers"   make checkers   JEPSEN_PROVIDER="$PROVIDER"
run_gate "matrix-m1"  make matrix-m1  JEPSEN_PROVIDER="$PROVIDER"
run_gate "matrix-m2"  make matrix-m2  JEPSEN_PROVIDER="$PROVIDER"

if [[ "${SKIP_LONG:-0}" == "1" ]]; then
  say "SKIP_LONG=1 —— 跳过组合浸泡（PR 级）"
  say "nightly done (short mode)"
  exit 0
fi

SOAK_EXTRA="--soak-mix ${SOAK_MIX} --min-op-sample ${SOAK_MIN_SAMPLE}"
SOAK_EXTRA+=" --watch-min-events ${SOAK_WATCH_MIN_EVENTS}"
SOAK_EXTRA+=" --lease-min-grants ${SOAK_LEASE_MIN_GRANTS}"
SOAK_EXTRA+=" --lease-min-expiries ${SOAK_LEASE_MIN_EXPIRIES}"

run_gate "soakfull(start)" make soak WORKLOAD=soakfull \
  SOAK_TIME_LIMIT="$SOAK_SECONDS" SEED="$SEED" SOAK_EXTRA="$SOAK_EXTRA" \
  JEPSEN_PROVIDER="$PROVIDER"
run_gate "soakfull(wait)"  make soak-wait  JEPSEN_PROVIDER="$PROVIDER"
run_gate "soakfull(results)" make soak-results JEPSEN_PROVIDER="$PROVIDER"

say "nightly done: ALL GATES PASSED"
