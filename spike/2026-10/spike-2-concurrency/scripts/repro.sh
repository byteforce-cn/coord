#!/usr/bin/env bash
#
# Spike 2（单写者并发演练）复现脚本。
#
# 一条命令跑出全部并发演练测试（真实单节点 Raft + 真实 KV/Txn/Lease gRPC），
# 把完整日志 tee 到 evidence/，并从日志抽取并发时间线为 jsonl。
#
# 用法：bash spike/2026-10/spike-2-concurrency/scripts/repro.sh
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$REPO_ROOT"

EVID="spike/2026-10/spike-2-concurrency/evidence"
mkdir -p "$EVID"
TS="$(date +%Y%m%d-%H%M%S)"
LOG="$EVID/test-run-$TS.log"
TL="$EVID/timeline-$TS.jsonl"

echo "== Spike 2 复现开始：$TS =="
echo "日志：$LOG"

# CARGO_BUILD_JOBS=3：与另外两个 Spike 并行编译时限制并发（勿删）。
CARGO_BUILD_JOBS="${CARGO_BUILD_JOBS:-3}" \
  cargo test -p coord --test spike2_concurrency -- --nocapture --test-threads=1 2>&1 | tee "$LOG"

# 抽取并发时间线（引擎级单效果/重复对照的真实派发顺序与时刻）。
grep -o 'TIMELINE {.*}' "$LOG" | sed 's/^TIMELINE //' > "$TL" || true

echo "== 完成 =="
echo "日志：    $LOG"
echo "时间线：  $TL"
