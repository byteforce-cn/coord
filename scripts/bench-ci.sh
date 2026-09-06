#!/usr/bin/env bash
# P2-06：每周性能基准入仓管线
#
# 1. release 模式运行 perf_bench 全量基准（含 watch 扇出）；
# 2. 报告落盘 benchmark-results/report-<date>.md；
# 3. 解析关键指标与 benchmark-results/baseline.json 比较，
#    任一指标劣化 >20% 即失败（告警语义，决策文档 §6.4 P2-06）；
# 4. 有意变更后人工执行 `UPDATE_BASELINE=1 bash scripts/bench-ci.sh` 更新基线。
#
# 本地/CI 同参：bash scripts/bench-ci.sh
set -euo pipefail
cd "$(dirname "$0")/.."

OUT_DIR=benchmark-results
mkdir -p "$OUT_DIR"
TS=$(date -u +%Y-%m-%d)
REPORT="$OUT_DIR/report-$TS.md"

echo "==> running perf_bench (release) ..."
# T5.21：多 Region 吞吐 80% 阈值硬闸（PERF_GATE=1 → Benchmark 6 断言
# 「5/10/25 Region 均 ≥ 单 Region 基线 80%」，见 coord/tests/perf_bench.rs）
PERF_GATE=1 cargo test --release -p coord --test perf_bench -- --ignored --nocapture >"$REPORT" 2>&1
echo "==> report written to $REPORT"

python3 - "$REPORT" "$OUT_DIR/baseline.json" "${UPDATE_BASELINE:-0}" <<'PY'
import json, re, sys

report_path, baseline_path, update = sys.argv[1], sys.argv[2], sys.argv[3] == "1"

def parse_metrics(text):
    metrics = {}
    for line in text.splitlines():
        cells = [c.strip() for c in line.strip().strip("|").split("|")]
        if len(cells) < 2:
            continue
        # 吞吐行：... | {ops} ops/s |（含 MB/s 的行取 ops/s 格）
        for i, cell in enumerate(cells):
            m = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?) ops/s", cell)
            if m:
                metrics[f"{cells[0]}::ops_s"] = float(m.group(1))
        # 延迟行：| name | avg µs | p50 µs | p95 µs | p99 µs |
        if all("µs" in c for c in cells[1:]):
            try:
                avg = float(cells[1].split()[0])
                p99 = float(cells[4].split()[0])
                metrics[f"{cells[0]}::avg_us"] = avg
                metrics[f"{cells[0]}::p99_us"] = p99
            except (ValueError, IndexError):
                pass
        # watch 扇出行：| subs | events | elapsed | events/s | total/s |
        if len(cells) == 5 and cells[0].isdigit() and cells[1].isdigit():
            try:
                metrics[f"watch_fanout_{cells[0]}subs::total_s"] = float(cells[4])
            except ValueError:
                pass
    return metrics

text = open(report_path, encoding="utf-8").read()
current = parse_metrics(text)
if not current:
    print("FATAL: no benchmark metrics parsed from report")
    sys.exit(2)

try:
    baseline = json.load(open(baseline_path, encoding="utf-8"))
except FileNotFoundError:
    baseline = {}

if update or not baseline:
    json.dump(current, open(baseline_path, "w", encoding="utf-8"), indent=2)
    print(f"baseline updated: {len(current)} metrics -> {baseline_path}")
    sys.exit(0)

regressions = []
missing = []
for name, val in current.items():
    if name not in baseline:
        missing.append(name)
        continue
    base = baseline[name]
    if base <= 0:
        continue
    drop = (base - val) / base
    if drop > 0.20:
        regressions.append((name, base, val, drop * 100.0))

for name, base, val, pct in regressions:
    print(f"REGRESSION {name}: {base:.1f} -> {val:.1f} ({pct:.1f}% worse)")
if missing:
    print(f"WARNING: {len(missing)} metrics missing from baseline (new benchmarks?) — "
          f"run UPDATE_BASELINE=1 to refresh")

print(f"checked {len(current)} metrics, {len(regressions)} regressions (>20%)")
if regressions:
    print("PERF GATE FAILED: >20% regression detected (decision §6.4 P2-06)")
    sys.exit(1)
if missing:
    print("PERF GATE FAILED: baseline out of date, run UPDATE_BASELINE=1")
    sys.exit(1)
print("PERF GATE PASSED")
PY
