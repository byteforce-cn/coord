#!/usr/bin/env bash
# 72h 生产浸泡（G1 闸门 / M2 出口验收）
#
# 在隔离生产环境中执行真实 3 进程分布式浸泡：
#   CHAOS_REAL=1 SOAK_DURATION_SECS=259200 \
#     cargo test -p coord --release --test chaos_real chaos_soak_distributed -- --ignored --nocapture
#
# 用法：
#   ./scripts/soak-72h.sh                 # 默认 72h（259200s）
#   SOAK_DURATION_SECS=300 ./scripts/soak-72h.sh   # 冒烟（CI nightly 同款 120s）
#
# 验收标准（M2 出口）：全程线性一致性校验无违规、无 leader 空窗超时、
# 无数据丢失；结束后汇总报告落盘 docs/production/evidence/soak-report.md。
set -euo pipefail

cd "$(dirname "$0")/.."

DURATION="${SOAK_DURATION_SECS:-259200}"
START="$(date -u +%FT%TZ)"
REPORT="docs/production/evidence/soak-report.md"

echo "== coord production soak =="
echo "duration=${DURATION}s start=${START}"
echo "binary: $(cargo build --release -p coord --bin coord 2>/dev/null && echo target/release/coord)"

mkdir -p docs/production/evidence

set +e
CHAOS_REAL=1 SOAK_DURATION_SECS="${DURATION}" \
  cargo test -p coord --release --test chaos_real chaos_soak_distributed -- --ignored --nocapture
EXIT=$?
set -e
END="$(date -u +%FT%TZ)"

{
  echo "# 72h 生产浸泡报告"
  echo
  echo "- 开始: ${START}"
  echo "- 结束: ${END}"
  echo "- 时长: ${DURATION}s"
  echo "- 套件: \`chaos_real::chaos_soak_distributed\`（真实 3 进程 + RegisterChecker 线性一致性）"
  if [ "${EXIT}" -eq 0 ]; then
    echo "- 结果: ✅ 通过（无一致性违规）"
  else
    echo "- 结果: ❌ 失败（exit=${EXIT}），详见上方测试输出"
  fi
} > "${REPORT}"

echo "soak finished: exit=${EXIT} at ${END}; report → ${REPORT}"
exit "${EXIT}"
