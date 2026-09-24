#!/usr/bin/env bash
# P2-06：每周性能基准入仓管线
#
# 1. release 模式运行 perf_bench 全量基准（含 watch 扇出）；
# 2. 报告落盘 benchmark-results/report-<date>.md；
# 3. 解析关键指标与 benchmark-results/baseline.json 比较，
#    任一指标劣化 >20% 即失败（告警语义，决策文档 §6.4 P2-06）；
#    ⚠️ 2026-09-23（W2 第六轮）：该**跨运行**比较只在**存在基线文件**时执行。
#    CI 上 `benchmark-results/` 是临时目录（基线未入库）⇒ 比较**未执行**，
#    脚本会发 `::warning::` 注解明说（旧版是静默 "baseline updated" + exit 0，
#    读起来像"门禁通过"——属自述式 no-op）。真门禁是下面的 within-run PERF_GATE。
# 4. 有意变更后人工执行 `UPDATE_BASELINE=1 bash scripts/bench-ci.sh` 更新基线。
#
# 失败自描述（2026-09-23）：输出 **tee** 到报告 + step 日志；失败时把
# 「断言数字/panic 位置」转成 check-run 注解（匿名可取的唯一通道，见
# docs/production/ops/ci-gate-forensics-2026-09-22.md）。
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
#
# `--test-threads=1` 是**测量方法**的一部分，不是提速开关（2026-09-22，W2-1 定位）：
#
# 该 job 此前 13/13 常驻红，本地 release 复现（`EXIT=101`）。根因不是产品劣化，而是
# **多个重型基准并发跑**：`--ignored` 会同时启动 `bench_all`、`bench_multi_region_*`、
# `bench_raw_redb_*`、`bench_mvcc_write_*`、`bench_value_size_impact` 等（默认
# test-threads = nproc），它们抢同一条 fsync 路径。同一次跑里的两组同代码测量：
#   · 单独实例（争用小）：1 Region 35 ops/s → 25 Region 48 ops/s（ratio **1.360** → 过）
#   · `bench_all` 实例（与其它基准重叠）：1 Region **219** ops/s → 25 Region 118 ops/s
#     （ratio **0.541** → **断言失败**，`perf_bench.rs:347`）
# 两次的 1-Region 基线本身差了 6 倍（35 vs 219）⇒ **噪声带宽远大于 0.80 判据**，
# 这才是红的判据。串行执行把测量的前提恢复为"同一时刻只有一个基准在写盘"。
#
# ✅ 2026-09-24（第七轮）：上面这段"样本量不对称"的脆弱性**已修** —— 单 Region 基线
# 改为与其它档同迭代数（5000）+ 同预热（100），见 `coord/tests/perf_bench.rs` 的注释。
# 修前本地 4 核（taskset 0-3）复跑**复现了 CI 失败形态**：bench_all 实例 ratio 0.667
# （1-Region 基线 222 ops/s / 0.9s 窗口），而同一次跑的 standalone 实例 1-Region 只有
# 128 ops/s ⇒ 不稳定的是**分母**（短窗口测到未进稳态的乐观基线）。
# 修后同一环境：**9 passed / 0 failed**（bench_all min ratio 0.940；standalone
# min ratio 1.025，四档 209/215/223/215 ops/s）。
# **阈值仍是 0.80 —— 只改测量方法，不改判据。**
# ⚠️ 2026-09-24（第七轮 W2）：管道必须在 `set +e` 下运行。本轮实测：
# `set -euo pipefail` 下 `false | tee x` 会**立刻终止脚本**（退出码取 pipefail
# 的非零值）⇒ 紧随其后的 `PERF_STATUS=${PIPESTATUS[0]}` 与失败分支（注解调用）
# 是**不可达代码**。后果实测二次：run 35810892940 / 35947874357 的 perf 红均为
# "exit 101 + 零注解"——注解器就位却永远不被调用（自述式 no-op 的第二形态：
# **错误处理路径也要有执行记录**）。
set +e
PERF_GATE=1 cargo test --release -p coord --test perf_bench -- --ignored --nocapture --test-threads=1 2>&1 | tee "$REPORT"
PERF_STATUS=${PIPESTATUS[0]}
set -e
if [ "$PERF_STATUS" -ne 0 ]; then
  # 2026-09-23：首次真跑（run 35810892940）红时报告被 `>"$REPORT"` 吞进文件、
  # step 日志几近为空、工件又因 skip 而不存在 ⇒ 一个数字都拿不到。现在：
  # 逐行透传到日志，并把断言/panic 行转成注解。
  echo "::error::perf_bench 失败（exit ${PERF_STATUS}）；报告：$REPORT"
  bash scripts/ci-annotate-test-failures.sh "$REPORT" || true
  exit "$PERF_STATUS"
fi
echo "==> report written to $REPORT"

# 同上的 set +e 理由：python 门禁失败（REGRESSION / FATAL）时，下面的注解分支
# 必须可达，否则这条路径同样只剩"红过"而没有任何数字。
set +e
python3 - "$REPORT" "$OUT_DIR/baseline.json" "${UPDATE_BASELINE:-0}" <<'PY' 2>&1 | tee /tmp/perf-gate.log
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
    if update:
        print(f"baseline updated (UPDATE_BASELINE=1): {len(current)} metrics -> {baseline_path}")
        sys.exit(0)
    # 2026-09-23（W2）：无提交基线时**不得**读成"门禁通过"。CI 上 benchmark-results/
    # 是临时目录 ⇒ 跨运行 20% 比较在 CI 上**不可能执行**（旧版静默写成
    # "baseline updated" + exit 0，属自述式 no-op）。此处明说"跳过"并发 warning 注解。
    # 注：跨机比较本身也不成立（共享 runner 硬件不固定）⇒ 若要把 20% 变成真门禁，
    # 需要固定 runner；见 docs/production/ops/ci-gate-forensics-2026-09-22.md。
    print(
        f"::warning::未找到基线 {baseline_path} ⇒ 跨运行 20% 比较**未执行**"
        f"（本次只有 within-run PERF_GATE 硬闸；{len(current)} 条指标已写入报告）"
    )
    print(f"baseline seeded (advisory only): {len(current)} metrics -> {baseline_path}")
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
GATE_STATUS=${PIPESTATUS[0]}
set -e
if [ "$GATE_STATUS" -ne 0 ]; then
  # 劣化/基线过期（或报告解析失败）也走注解通道：REGRESSION / FATAL 行会被带回。
  bash scripts/ci-annotate-test-failures.sh /tmp/perf-gate.log || true
  exit "$GATE_STATUS"
fi