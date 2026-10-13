#!/usr/bin/env bash
# Spike 5 · 基准测试复现脚本
#
# 一条命令跑出全部基准，原始样本（逐条，非汇总）落到
#   spike/2026-10/spike-5-bench/evidence/raw/
# 运行日志（带时间戳）落到同目录 run-<ts>.log，机器规格落到 evidence/machine.txt。
#
# 时长（8C/31G 测试机，**非目标硬件**）：
#   txn + 单值大小 约 5s；KV 三档（各 2s 预热 + 10s 测量）约 45s；
#   Watch 1000 连接 + 20 轮 约 1-3min；存储 20k 写约 3min。总计约 6-8 分钟。
#
# 缩小范围（可选，均为测试读取的环境变量）：
#   SPIKE5_KV_LEVELS=10,50        KV 并发档（默认 100,500,1000）
#   SPIKE5_KV_WARMUP_MS=2000      KV 预热（ms）
#   SPIKE5_KV_MEASURE_MS=10000    KV 测量窗口（ms）
#   SPIKE5_WATCH_WATCHERS=1000    Watch 连接数
#   SPIKE5_WATCH_ROUNDS=20        扇出轮次
#   SPIKE5_STORAGE_WRITES=20000   存储增长写入次数
#   SPIKE5_STORAGE_SAMPLE_EVERY=1000
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../../.." && pwd)"
cd "$REPO_ROOT"
EVIDENCE="$REPO_ROOT/spike/2026-10/spike-5-bench/evidence"
mkdir -p "$EVIDENCE/raw"
TS="$(date +%Y%m%d-%H%M%S)"

# 机器规格（非目标硬件；报告须显著标注，禁止据此做达标判定）
{
  echo "# Spike 5 运行环境（非目标硬件，判定须等目标硬件清单）"
  echo "captured_at: $(date -Iseconds)"
  echo "nproc: $(nproc)"
  echo "loadavg: $(cat /proc/loadavg)"
  echo "kernel: $(uname -a)"
  echo "rustc: $(rustc --version)"
  echo "cargo: $(cargo --version)"
  echo "--- free -h ---"
  free -h
  echo "--- lscpu ---"
  lscpu 2>/dev/null | grep -E 'Model name|^CPU\(s\)|MHz|L3 cache' || true
  echo "--- disk ---"
  df -h "$REPO_ROOT" || true
} > "$EVIDENCE/machine.txt"

export SPIKE5_EVIDENCE_DIR="$EVIDENCE"

# --test-threads=1 与测试内的进程级串行闸双保险：并行基准互抢 CPU/fsync 会污染口径。
CARGO_BUILD_JOBS=3 cargo test -p coord --test spike5_bench -- \
  --ignored --nocapture --test-threads=1 \
  2>&1 | tee "$EVIDENCE/raw/run-$TS.log"
