#!/usr/bin/env bash
# Spike 2026-10 · Spike 3 —— Outbox + 崩溃恢复的进程级 kill -9 复现脚本。
#
# 一条命令独立跑（不依赖人工操作）：
#
#   bash spike/2026-10/spike-3-recovery/scripts/repro.sh
#
# 对每个场景：起真实 `coord server`（单节点 bootstrap，固定 data-dir）→ 起 harness
# （真实引擎装配）→ 等实例进入**已持久化**的挂起态 → `kill -9` harness →
# 重启 harness（同一 server / 同一 data-dir / 同一 MQ 目录）→ 断言恢复后状态
# 一致、事件不丢不重。
#
# 产物：
#   evidence/kill9-<scenario>-<ts>.log    完整时间线（含 kill 时刻与 PID）
#   evidence/<scenario>-<ts>/{outbox,timers,events}.tsv + summary.json
#   evidence/verdict.tsv                  机器可读判定
#
# 临时文件一律在 /tmp（AGENTS.md 硬性规定）：默认 /tmp/coord-spike-spike3/run-<ts>/
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
EVIDENCE_DIR="$REPO_ROOT/spike/2026-10/spike-3-recovery/evidence"
TS="$(date +%Y%m%d-%H%M%S)"
RUN_DIR="${SPIKE3_RUN_DIR:-/tmp/coord-spike-spike3/run-$TS}"
TARGET_DIR="${CARGO_TARGET_DIR:-$REPO_ROOT/target}"
COORD_BIN="$TARGET_DIR/debug/coord"
HARNESS_BIN="$TARGET_DIR/debug/spike3-recovery-harness"
WAIT_SECS="${SPIKE3_WAIT_SECS:-5}"

mkdir -p "$RUN_DIR" "$EVIDENCE_DIR"
echo "run dir: $RUN_DIR"
echo "evidence dir: $EVIDENCE_DIR"

# ─── 构建（默认；SPIKE3_SKIP_BUILD=1 复用已有产物） ───
if [[ "${SPIKE3_SKIP_BUILD:-0}" != "1" ]]; then
  echo "[build] cargo build -p coord -p spike3-recovery-harness (CARGO_BUILD_JOBS=3)"
  ( cd "$REPO_ROOT" && CARGO_BUILD_JOBS=3 cargo build -p coord -p spike3-recovery-harness )
fi
[[ -x "$COORD_BIN" ]] || { echo "missing binary: $COORD_BIN" >&2; exit 1; }
[[ -x "$HARNESS_BIN" ]] || { echo "missing binary: $HARNESS_BIN" >&2; exit 1; }

# ─── 端口 ───
read -r GRPC_PORT RAFT_PORT < <(python3 - <<'PY'
import socket
def free():
    s = socket.socket(); s.bind(("127.0.0.1", 0)); p = s.getsockname()[1]; s.close(); return p
a, b = free(), free()
while a == b:
    b = free()
print(a, b)
PY
)
SERVER_ADDR="127.0.0.1:$GRPC_PORT"
DATA_DIR="$RUN_DIR/server"
TOPIC="com.china-workflow.spike3.events"
SERVER_LOG="$RUN_DIR/server.log"

echo "server: $SERVER_ADDR (raft 127.0.0.1:$RAFT_PORT) data-dir=$DATA_DIR"

SERVER_PID=""
HARNESS_PIDS=()
cleanup() {
  for p in "${HARNESS_PIDS[@]:-}"; do [[ -n "$p" ]] && kill -9 "$p" 2>/dev/null || true; done
  [[ -n "$SERVER_PID" ]] && kill "$SERVER_PID" 2>/dev/null || true
}
trap cleanup EXIT

mkdir -p "$DATA_DIR"
"$COORD_BIN" server \
  --id 1 --bootstrap --auth-enabled false \
  --addr "$SERVER_ADDR" --raft-addr "127.0.0.1:$RAFT_PORT" \
  --data-dir "$DATA_DIR" >"$SERVER_LOG" 2>&1 &
SERVER_PID=$!

wait_port() {
  local host=$1 port=$2 timeout=$3
  local end=$((SECONDS + timeout))
  while (( SECONDS < end )); do
    if (exec 3<>"/dev/tcp/$host/$port") 2>/dev/null; then exec 3>&- 3<&-; return 0; fi
    sleep 0.2
  done
  return 1
}
wait_port 127.0.0.1 "$GRPC_PORT" 120 || { echo "server did not listen" >&2; tail -50 "$SERVER_LOG" >&2; exit 1; }
sleep 1

wait_for_line() { # file pattern timeout_secs
  local file=$1 pat=$2 timeout=$3
  local end=$((SECONDS + timeout))
  while (( SECONDS < end )); do
    if [[ -f "$file" ]] && grep -q -- "$pat" "$file"; then return 0; fi
    sleep 0.2
  done
  return 1
}

verdict_lines=("case\tstatus\tfinal_status\toutbox_pending\tmissing_event_ids\tduplicate_event_ids\tlog")

# ─── 单个场景：start → kill -9 → recover → 断言 ───
run_case() { # scenario trigger_event expected_final
  local scenario=$1 with_trigger=$2 expected_final=$3
  local case_log="$EVIDENCE_DIR/kill9-$scenario-$TS.log"
  # 判定表只记仓库相对路径：绝对路径随工作目录漂移，重跑会污染 tracked 证据。
  local case_log_rel="${case_log#"$REPO_ROOT"/}"
  local case_report="$EVIDENCE_DIR/$scenario-$TS"
  # 每个场景独立 MQ 目录：证据自包含（同一 MQ 目录跨场景会让 events.tsv 混入别案事件）
  local MQ_DIR="$RUN_DIR/mq-$scenario"
  mkdir -p "$case_report" "$MQ_DIR"
  : > "$case_log"

  echo "=== case=$scenario ==="

  # 1) start（第一阶段 relay 关闭：事件停在 KV outbox，直接体现「杀进程前已持久」）
  "$HARNESS_BIN" --mode start --scenario "$scenario" --server "$SERVER_ADDR" \
    --mq-dir "$MQ_DIR" --topic "$TOPIC" --relay off --wait-secs "$WAIT_SECS" \
    >>"$case_log" 2>&1 &
  local start_pid=$!
  HARNESS_PIDS+=("$start_pid")

  if ! wait_for_line "$case_log" "READY" 60; then
    echo "case=$scenario FAILED: no READY line" >&2
    cat "$case_log" >&2
    verdict_lines+=("$scenario\tFAIL-no-ready\t-\t-\t-\t-\t$case_log_rel")
    return 1
  fi
  local instance
  instance=$(grep -m1 "READY" "$case_log" | sed -n 's/.*instance=\([^ ]*\).*/\1/p')
  echo "instance=$instance"

  # 2) kill -9（不经优雅停机）
  kill -9 "$start_pid"
  local rc=0; wait "$start_pid" 2>/dev/null || rc=$?
  echo "$(printf '%8d' 0) KILL9          pid=$start_pid signal=SIGKILL wait_status=$rc ts=$(date -u +%FT%T.%3NZ)" >>"$case_log"
  echo "killed harness pid=$start_pid (SIGKILL)"

  # 3) recover（同一 server / 同一 data-dir / 同一 MQ 目录）
  local trigger="$RUN_DIR/trigger-$scenario"
  rm -f "$trigger"
  local rec_log="$case_log"
  "$HARNESS_BIN" --mode recover --scenario "$scenario" --server "$SERVER_ADDR" \
    --mq-dir "$MQ_DIR" --topic "$TOPIC" --relay on \
    --instance "$instance" --report-dir "$case_report" \
    --trigger-file "$trigger" --deadline-secs 90 \
    >>"$rec_log" 2>&1 &
  local rec_pid=$!
  HARNESS_PIDS+=("$rec_pid")

  if [[ "$with_trigger" == "yes" ]]; then
    if wait_for_line "$rec_log" "SUBSCRIBED" 60; then
      echo "$(printf '%8d' 0) TRIGGER        event source fires (touch $trigger)" >>"$rec_log"
      touch "$trigger"
    else
      echo "case=$scenario FAILED: recover never resubscribed" >&2
      kill -9 "$rec_pid" 2>/dev/null || true
      verdict_lines+=("$scenario\tFAIL-no-subscribe\t-\t-\t-\t-\t$case_log_rel")
      return 1
    fi
  fi

  if ! wait "$rec_pid"; then
    echo "case=$scenario FAILED: recover exited non-zero" >&2
    tail -30 "$rec_log" >&2
    verdict_lines+=("$scenario\tFAIL-recover\t-\t-\t-\t-\t$case_log_rel")
    return 1
  fi

  # 4) 断言（从 summary.json 取事实，不解析人类可读文案）
  local summary="$case_report/summary.json"
  [[ -f "$summary" ]] || { echo "case=$scenario FAILED: no summary.json" >&2; verdict_lines+=("$scenario\tFAIL-no-summary\t-\t-\t-\t-\t$case_log_rel"); return 1; }
  local final pending missing duplicate
  final=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['final_status'])" "$summary")
  pending=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['outbox_pending'])" "$summary")
  missing=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['missing_event_ids'])" "$summary")
  duplicate=$(python3 -c "import json,sys;print(json.load(open(sys.argv[1]))['duplicate_event_ids'])" "$summary")

  local status="PASS"
  [[ "$final" == "$expected_final" ]] || status="FAIL-final:$final"
  [[ "$pending" == "0" ]] || status="FAIL-pending:$pending"
  [[ "$missing" == "0" ]] || status="FAIL-missing:$missing"
  [[ "$duplicate" == "0" ]] || status="FAIL-duplicate:$duplicate"

  echo "case=$scenario status=$status final=$final pending=$pending missing=$missing duplicate=$duplicate"
  verdict_lines+=("$scenario\t$status\t$final\t$pending\t$missing\t$duplicate\t$case_log_rel")
  [[ "$status" == "PASS" ]]
}

failures=0
run_case wait no completed || failures=$((failures + 1))
run_case listen yes completed || failures=$((failures + 1))
run_case retry no failed || failures=$((failures + 1))

{
  printf '%s\n' "${verdict_lines[@]}"
} > "$EVIDENCE_DIR/verdict-$TS.tsv"
cp "$EVIDENCE_DIR/verdict-$TS.tsv" "$EVIDENCE_DIR/verdict.tsv"

echo
echo "=== verdict ==="
column -t -s $'\t' "$EVIDENCE_DIR/verdict-$TS.tsv" 2>/dev/null || cat "$EVIDENCE_DIR/verdict-$TS.tsv"
echo
if (( failures > 0 )); then
  echo "SPIKE3 REPRO: FAILED ($failures case(s))" >&2
  exit 1
fi
echo "SPIKE3 REPRO: PASS (evidence under $EVIDENCE_DIR)"
