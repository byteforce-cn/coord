#!/usr/bin/env bash
# E4 证据采集：把真实运行产物落盘到 docs/production/evidence/<UTC 时间戳>-<场景>/
#
# 用法：bash scripts/collect-evidence.sh <场景>
#   soak-smoke   120s 分布式浸泡（CHAOS_REAL）
#   chaos        kill9 / SIGSTOP / 分区注入 + 线性一致检查（CHAOS_REAL）
#   multi-raft   3 节点 × 3 Region 进程级套件（MULTI_RAFT_REAL）
#   plugin       插件引擎进程级 e2e（PLUGIN_REAL）
#   object       对象存储进程级 e2e（OBJECT_STORAGE_REAL）
#   jepsen       真实 Jepsen（需 lein + 可达集群）
#
# 产物：run.log（原始输出）、MANIFEST.md（环境/commit/命令/结论）、sha256sums.txt。
set -euo pipefail

cd "$(dirname "$0")/.."

SCENARIO="${1:-}"
if [[ -z "$SCENARIO" ]]; then
    echo "usage: $0 <soak-smoke|chaos|multi-raft|plugin|object|jepsen>" >&2
    exit 2
fi

case "$SCENARIO" in
    soak-smoke)
        export CHAOS_REAL=1 SOAK_DURATION_SECS=120
        CMD=(cargo test -p coord --test chaos_real chaos_soak_distributed -- --ignored --nocapture --test-threads=1)
        ;;
    chaos)
        export CHAOS_REAL=1
        CMD=(cargo test -p coord --test chaos_real -- --ignored --nocapture --test-threads=1)
        ;;
    multi-raft)
        export MULTI_RAFT_REAL=1
        CMD=(cargo test -p coord --test multi_raft_process_test -- --ignored --nocapture --test-threads=1)
        ;;
    plugin)
        export PLUGIN_REAL=1
        CMD=(cargo test -p coord --test plugin_real_process_test -- --ignored --nocapture --test-threads=1)
        ;;
    object)
        export OBJECT_STORAGE_REAL=1
        CMD=(cargo test -p coord --test object_storage_process_test -- --ignored --nocapture --test-threads=1)
        ;;
    jepsen)
        CMD=(bash scripts/jepsen-lab.sh)
        ;;
    *)
        echo "unknown scenario: $SCENARIO" >&2
        exit 2
        ;;
esac

TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="docs/production/evidence/${TS}-${SCENARIO}"
mkdir -p "$OUT"

COMMIT="$(git rev-parse HEAD 2>/dev/null || echo unknown)"
DIRTY="$(git status --porcelain 2>/dev/null | wc -l | tr -d ' ')"
RUSTC="$(rustc --version 2>/dev/null || echo unknown)"
UNAME="$(uname -a)"

echo "==> running: ${CMD[*]}"
set +e
"${CMD[@]}" 2>&1 | tee "$OUT/run.log"
RC="${PIPESTATUS[0]}"
set -e

{
    echo "# Evidence MANIFEST — ${SCENARIO}"
    echo
    echo "- **UTC**: ${TS}"
    echo "- **git commit**: \`${COMMIT}\` (dirty files: ${DIRTY})"
    echo "- **rustc**: ${UNAME%% } / ${RUSTC}"
    echo "- **uname**: \`${UNAME}\`"
    echo "- **command**: \`${CMD[*]}\`"
    echo "- **env**: CHAOS_REAL=${CHAOS_REAL:-} SOAK_DURATION_SECS=${SOAK_DURATION_SECS:-} MULTI_RAFT_REAL=${MULTI_RAFT_REAL:-} PLUGIN_REAL=${PLUGIN_REAL:-} OBJECT_STORAGE_REAL=${OBJECT_STORAGE_REAL:-}"
    echo "- **exit code**: ${RC}"
    echo
    echo "## 结论"
    echo
    if [[ "$RC" -eq 0 ]]; then
        echo "本次运行**通过**（见 \`run.log\` 原始输出）。"
    else
        echo "本次运行**失败**（exit=${RC}）——这是如实记录，不得改写为通过。"
    fi
} > "$OUT/MANIFEST.md"

( cd "$OUT" && sha256sum run.log MANIFEST.md > sha256sums.txt )

echo
echo "Evidence written to: $OUT (exit=${RC})"
[[ "$RC" -eq 0 ]]
