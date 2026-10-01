#!/usr/bin/env bash
# Coord Release Mode Performance Benchmark
#
# 在 Release 模式下运行所有性能基准测试并输出 Markdown 报告。
# Release 模式 (opt-level=3 + LTO) 预期吞吐量比 debug 模式高 5-10x。
#
# 用法:
#   ./scripts/bench-release.sh              # 运行全部基准
#   ./scripts/bench-release.sh --perf-only   # 仅运行单操作基准
#   ./scripts/bench-release.sh --soak        # 运行长时间稳定性测试（默认 60s）
#   SOAK_DURATION_SECS=300 ./scripts/bench-release.sh --soak  # 5 分钟浸泡测试
#   ./scripts/bench-release.sh --all         # 全部测试（基准 + 浸泡）
#
# 输出: 终端 Markdown 表格 + bench-report-{timestamp}.md 文件

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
TIMESTAMP=$(date +%Y%m%d-%H%M%S)
REPORT_FILE="$PROJECT_DIR/bench-report-${TIMESTAMP}.md"

cd "$PROJECT_DIR"

echo "# Coord Release Mode 性能基准测试报告" | tee "$REPORT_FILE"
echo "" | tee -a "$REPORT_FILE"
echo "> 时间: $(date '+%Y-%m-%d %H:%M:%S')" | tee -a "$REPORT_FILE"
echo "> 模式: Release (opt-level=3, LTO)" | tee -a "$REPORT_FILE"
echo "> Rust: $(rustc --version)" | tee -a "$REPORT_FILE"
echo "" | tee -a "$REPORT_FILE"

MODE="${1:---perf-only}"

run_perf_bench() {
    echo "## 1. 性能基准测试" | tee -a "$REPORT_FILE"
    echo "" | tee -a "$REPORT_FILE"

    cargo test -p coord --test perf_bench --release -- --ignored --nocapture 2>&1 | tee -a "$REPORT_FILE"

    echo "" | tee -a "$REPORT_FILE"
    echo "---" | tee -a "$REPORT_FILE"
    echo "" | tee -a "$REPORT_FILE"
}

run_soak_test() {
    local duration="${SOAK_DURATION_SECS:-60}"
    echo "## 2. 长时间运行稳定性测试 (${duration}s)" | tee -a "$REPORT_FILE"
    echo "" | tee -a "$REPORT_FILE"

    SOAK_DURATION_SECS="$duration" \
        cargo test -p coord --test soak_test --release -- --ignored --nocapture 2>&1 | tee -a "$REPORT_FILE"

    echo "" | tee -a "$REPORT_FILE"
    echo "---" | tee -a "$REPORT_FILE"
    echo "" | tee -a "$REPORT_FILE"
}

case "$MODE" in
    --perf-only)
        run_perf_bench
        ;;
    --soak)
        run_soak_test
        ;;
    --all)
        run_perf_bench
        run_soak_test
        ;;
    *)
        echo "用法: $0 [--perf-only | --soak | --all]"
        exit 1
        ;;
esac

echo "✅ 基准测试完成。报告已保存到: $REPORT_FILE"
echo "   查看: cat $REPORT_FILE"
