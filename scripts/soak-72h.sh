#!/usr/bin/env bash
# 72h 生产浸泡
#
# 在隔离生产环境中执行真实 3 进程分布式浸泡。两套口径：
#   SOAK_SUITE=v1（默认）：单 Region v1 数据面，RegisterChecker 线性一致性
#     CHAOS_REAL=1 SOAK_DURATION_SECS=259200 \
#       cargo test -p coord --release --test chaos_real chaos_soak_distributed -- --ignored --nocapture
#   SOAK_SUITE=v2：KV-T v2 数据面（TSO/Txn/直写/Watch 语义 + 周期 kill -9）
#     KVT_SOAK_SECS=259200 \
#       cargo test -p coord --release --test kv_t_g6_soak_v2_test soak_v2_mixed_workload_stays_consistent -- --ignored --nocapture
#
# 用法：
#   ./scripts/soak-72h.sh                            # 默认 v1 72h（259200s）
#   SOAK_SUITE=v2 ./scripts/soak-72h.sh              # v2 72h（KV-T GA 证据口径）
#   SOAK_SUITE=v2 SOAK_DURATION_SECS=300 ./scripts/soak-72h.sh   # 冒烟
#
# 验收标准：全程无一致性违规、无 leader 空窗超时、无数据丢失；结束后汇总报告
# 落盘 ${COORD_EVIDENCE_DIR}/soak-report.md（运行产物不写入仓库）。
# 节点日志/数据目录留在 ${COORD_EVIDENCE_DIR}/v2-nodes（v2）或临时目录（v1）。
set -euo pipefail

cd "$(dirname "$0")/.."

SUITE="${SOAK_SUITE:-v1}"
DURATION="${SOAK_DURATION_SECS:-259200}"
START="$(date -u +%FT%TZ)"
EVIDENCE_DIR="${COORD_EVIDENCE_DIR:-}"
if [ -z "$EVIDENCE_DIR" ]; then
  echo "COORD_EVIDENCE_DIR 未设置：运行产物不写入仓库（请显式指定制品目录）。" >&2
  exit 2
fi
REPORT="${EVIDENCE_DIR}/soak-report.md"

case "$SUITE" in
  v1)
    SUITE_DESC='`chaos_real::chaos_soak_distributed`（真实 3 进程 + RegisterChecker 线性一致性）'
    ;;
  v2)
    SUITE_DESC='`kv_t_g6_soak_v2_test::soak_v2_mixed_workload_stays_consistent`（真实 3 进程 v2：TSO 单调/直写可见/跨区事务/周期 kill -9）'
    ;;
  *)
    echo "SOAK_SUITE 必须是 v1 或 v2（got: ${SUITE}）" >&2
    exit 2
    ;;
esac

echo "== coord production soak =="
echo "suite=${SUITE} duration=${DURATION}s start=${START}"
echo "binary: $(cargo build --release -p coord --bin coord 2>/dev/null && echo target/release/coord)"

mkdir -p "$EVIDENCE_DIR"

set +e
if [ "$SUITE" = v2 ]; then
  KVT_CHAOS_ARTIFACT_DIR="${EVIDENCE_DIR}/v2-nodes" \
    KVT_SOAK_SECS="${DURATION}" \
    cargo test -p coord --release --test kv_t_g6_soak_v2_test \
      soak_v2_mixed_workload_stays_consistent -- --ignored --nocapture --test-threads=1
else
  CHAOS_REAL=1 SOAK_DURATION_SECS="${DURATION}" \
    cargo test -p coord --release --test chaos_real chaos_soak_distributed -- --ignored --nocapture
fi
EXIT=$?
set -e
END="$(date -u +%FT%TZ)"

{
  echo "# 72h 生产浸泡报告"
  echo
  echo "- 开始: ${START}"
  echo "- 结束: ${END}"
  echo "- 时长: ${DURATION}s"
  echo "- 套件: ${SUITE_DESC}"
  if [ "${EXIT}" -eq 0 ]; then
    echo "- 结果: ✅ 通过（无一致性违规）"
  else
    echo "- 结果: ❌ 失败（exit=${EXIT}），详见上方测试输出"
  fi
} > "${REPORT}"

echo "soak finished: exit=${EXIT} at ${END}; report → ${REPORT}"
exit "${EXIT}"
