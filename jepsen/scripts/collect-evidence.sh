#!/usr/bin/env bash
#
# T0.1 —— 证据入库管道（MANIFEST + 原始产物 + 校验和）。
#
# 在**控制机**上运行（store 在这里）。把一次 Jepsen run 的全部产物落到
# `docs/production/evidence/<UTC 时间戳>-<场景>/`，并生成 MANIFEST.md：没有
# MANIFEST 的 run 不算证据（§0-4：可复现），引入评审时只能引用有 MANIFEST 的 run。
#
# 用法（控制机，/root/coord-test 下）：
#   scripts/collect-evidence.sh <场景> [STORE_DIR]
#
#   <场景>      自由文本标签，如 soak-72h / m1-map / t5.2-agent-smoke
#   STORE_DIR   默认 store/coord/latest
#
# 产物：
#   MANIFEST.md     环境 / commit / 命令行 / 参数 / 种子 / 版本 / 哈希 / 门槛结论
#   run.log         jepsen.log 原样
#   results.edn  history.edn  history.txt  （history.edn 超过阈值时 gzip）
#   summary.txt     summarize-results.clj 的输出（门槛机器可读摘要）
#   sha256sums.txt  上述文件的校验和
#
# 退出码：0 = 归档成功（**不代表 run 通过**——通过与否看 MANIFEST 的结论行）；
# 2 = 用法/前置错误。

set -uo pipefail

SCENARIO="${1:-}"
STORE="${2:-store/coord/latest}"
if [[ -z "$SCENARIO" ]]; then
    echo "usage: $0 <scenario-label> [STORE_DIR]" >&2
    exit 2
fi
if [[ ! -d "$STORE" ]]; then
    echo "store dir not found: $STORE" >&2
    exit 2
fi

# 证据必须落在**随 coord 版本化的 checkout**里（docs/production/evidence/），
# 否则「引用某次 run 的证据」在评审时根本找不到。
#
# 坑：bash 的 cd 默认是逻辑语义，`cd /root/coord-test/../..` 会解析到 `/root`
# 而不是仓库根 —— docker provider 下 `/root/coord-test` 是指向
# `/opt/coord/jepsen` 的软链，于是证据会被写进控制机的 `/root/docs/...`
# （容器内，随容器销毁）。所以优先用 git 找仓库根（物理路径），找不到才回退。
# 两种 provider 都可用 EVIDENCE_REPO_ROOT 显式覆盖。
REPO_ROOT="${EVIDENCE_REPO_ROOT:-}"
if [[ -z "$REPO_ROOT" ]]; then
    REPO_ROOT="$(cd -P "$(dirname "$0")/../.." && pwd)"
    if git -C "$REPO_ROOT" rev-parse --show-toplevel >/dev/null 2>&1; then
        REPO_ROOT="$(git -C "$REPO_ROOT" rev-parse --show-toplevel)"
    fi
fi
TS="$(date -u +%Y%m%dT%H%M%SZ)"
OUT="$REPO_ROOT/docs/production/evidence/${TS}-${SCENARIO}"
mkdir -p "$OUT"

# ---------------------------------------------------------------------------
# 采集
# ---------------------------------------------------------------------------

# 原始产物（大文件交给下面的 gzip 策略）
[[ -f "$STORE/jepsen.log"  ]] && cp "$STORE/jepsen.log"  "$OUT/run.log"
[[ -f "$STORE/results.edn" ]] && cp "$STORE/results.edn" "$OUT/results.edn"
[[ -f "$STORE/history.txt" ]] && cp "$STORE/history.txt" "$OUT/history.txt"

HIST_GZ="no"
if [[ -f "$STORE/history.edn" ]]; then
    SIZE=$(stat -c%s "$STORE/history.edn" 2>/dev/null || echo 0)
    if [[ "$SIZE" -gt 5242880 ]]; then      # >5MiB：压缩后入库（回放用 zcat）
        gzip -c "$STORE/history.edn" > "$OUT/history.edn.gz"
        HIST_GZ="yes (source ${SIZE} bytes)"
    else
        cp "$STORE/history.edn" "$OUT/history.edn"
    fi
fi

# 门槛摘要（有 lein/clojure 时用 reader 取嵌套的 checker 结果，失败则退化为提示）
TEST_DIR="$(cd "$(dirname "$0")/.." && pwd)"
run_summarizer() {
    # F-10 同族问题：lab 里没有外网，plain `lein` 会在依赖解析上挂到 300s 超时，
    # 于是每份 MANIFEST 的门槛结论都停在 "unknown"（看起来像"没结论"，实际是
    # "摘要器没跑成"）。默认走离线模式；ONLINE=1 可覆盖。
    local -a offline=(-o)
    [[ "${ONLINE:-}" == "1" ]] && offline=()
    if command -v lein >/dev/null 2>&1; then
        ( cd "$TEST_DIR" && LEIN_ROOT=true timeout 300 lein "${offline[@]}" run -m clojure.main \
            scripts/summarize-results.clj "$STORE" )
    elif command -v clojure >/dev/null 2>&1; then
        ( cd "$TEST_DIR" && timeout 300 clojure -M \
            scripts/summarize-results.clj "$STORE" )
    else
        return 1
    fi
}
run_summarizer > "$OUT/summary.txt" 2>/dev/null
if [[ ! -s "$OUT/summary.txt" ]]; then
    echo "summary unavailable (no lein/clojure, or summarizer failed) — inspect results.edn manually" \
        > "$OUT/summary.txt"
fi

# ---------------------------------------------------------------------------
# MANIFEST
# ---------------------------------------------------------------------------

coord_commit=$(cd "$REPO_ROOT" && git rev-parse HEAD 2>/dev/null || echo unknown)
tree_dirty=$(cd "$REPO_ROOT" && { git diff --quiet 2>/dev/null && echo clean || echo DIRTY; })
coord_tag=$(cd "$REPO_ROOT" && git describe --tags --always 2>/dev/null || echo -)
commit_date=$(cd "$REPO_ROOT" && git log -1 --format=%cI 2>/dev/null || echo -)

proto_hash=$(cd "$REPO_ROOT" && \
    { find apis/contracts/proto coord-proto/src -type f -name '*.proto' -print0 2>/dev/null \
      | sort -z | xargs -0 cat 2>/dev/null | sha256sum | cut -c1-16; } || echo -)
proto_hash=${proto_hash:-none}

# coord 的节点配置由 jepsen.coord.db 生成（没有静态 .toml），所以哈希配置**生成器**
config_hash=$(sha256sum "$REPO_ROOT/jepsen/src/jepsen/coord/db.clj" 2>/dev/null | cut -c1-16)
config_hash=${config_hash:-none}

jepsen_version=$(grep -oE '\[jepsen "[^"]+"\]' "$REPO_ROOT/jepsen/project.clj" 2>/dev/null \
                 | head -1 | sed 's/.*"\(.*\)".*/\1/')
jepsen_version=${jepsen_version:-unknown}
clj_version=$( { command -v clojure >/dev/null 2>&1 && clojure -Sdescribe 2>/dev/null \
                 | grep -oE '"version": "[^"]+"' | head -1 | cut -d'"' -f4; } || true )
clj_version=${clj_version:-unknown}
java_version=$(java -version 2>&1 | head -1 | sed 's/.*version "\(.*\)".*/\1/')
nodes=$(cat /root/nodes 2>/dev/null | tr '\n' ' ')
nodes=${nodes:-unknown}
image_id=$( { grep -rhoE 'image:.*jepsen-[a-z]+' "$REPO_ROOT/jepsen/lab/docker" 2>/dev/null \
              | sed 's/.*\(jepsen-[a-z]*\)/\1/' | sort -u | tr '\n' ' '; } )
image_id=${image_id:-n/a (vagrant)}

# 命令行：jepsen 把 test map 打进 jepsen.log，:time-limit / :workload 等都在里面
run_cmd=$(grep -m1 -E '^lein run test' "$OUT/run.log" 2>/dev/null || echo unknown)
run_args=$(grep -oE '^\s+?"?(--[a-z-]+)"?$' "$OUT/run.log" 2>/dev/null | tr -d ' "' | tr '\n' ' ')

seed=$(grep -oE ':seed [0-9]+' "$OUT/run.log" 2>/dev/null | head -1 | awk '{print $2}')
seed=${seed:-"(not recorded — pre-T0.5 run)"}

started=$(grep -m1 -oE '[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:,]+' "$OUT/run.log" 2>/dev/null)
finished=$(tail -50 "$OUT/run.log" 2>/dev/null | grep -oE '[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9:,]+' | tail -1)
started=${started:-unknown}; finished=${finished:-unknown}

gates_ok=$(grep -m1 '^gates-valid:' "$OUT/summary.txt" 2>/dev/null | cut -d' ' -f2-)
overall_ok=$(grep -m1 '^overall-valid:' "$OUT/summary.txt" 2>/dev/null | cut -d' ' -f2-)

cat > "$OUT/MANIFEST.md" <<EOF
# Jepsen evidence — ${SCENARIO}

| 字段 | 值 |
|:--|:--|
| 场景 | \`${SCENARIO}\` |
| UTC 时间戳 | \`${TS}\` |
| run 开始 / 结束 | ${started} / ${finished} |
| coord commit | \`${coord_commit}\` (${coord_tag}, ${commit_date}) |
| 工作树 | **${tree_dirty}** |
| coord-proto 哈希（16） | \`${proto_hash}\` |
| coord 配置生成器哈希（16） | \`${config_hash}\` (jepsen/coord/db.clj — 节点 TOML 由它生成) |
| Jepsen 版本 | ${jepsen_version} |
| Clojure 版本 | ${clj_version} |
| JVM | ${java_version} |
| 节点 | ${nodes} |
| lab 镜像 | ${image_id} |
| 随机种子 | **${seed}** |
| 命令行（jepsen.log 记录） | \`${run_cmd}\` |
| 选项 | \`${run_args}\` |
| history.edn | $( [[ "$HIST_GZ" == no ]] && echo "history.edn（未压缩）" || echo "history.edn.gz — ${HIST_GZ}" ) |
| §5.4 参数确认记录链接 | _(待填：issue/邮件存档链接)_ |

## 门槛结论

| 字段 | 值 |
|:--|:--|
| overall :valid? | ${overall_ok:-unknown} |
| gates :valid? (T0.2) | ${gates_ok:-unknown} |

完整门槛摘要见 \`summary.txt\`（rto-p95 / quiet-judged / premise-valid 等）。

> 本 MANIFEST 只证明"产物可追溯、可回放"，**不**代替引入评审结论。
> 未使用 §5.4 书面确认参数取值的 run 只能作内部参考，不得用于引入决策。

## 回放

\`\`\`bash
# 用记录下来的种子重建 jittered nemesis 排期（应与本次 run 一致）
lein run -m clojure.main scripts/replay.clj ${STORE} --seed ${seed}
# 重跑 checker（不需要集群）
lein run -m clojure.main scripts/validate-soak-checker.clj ${STORE}
\`\`\`
EOF

# ---------------------------------------------------------------------------
# 校验和
# ---------------------------------------------------------------------------

( cd "$OUT" && sha256sum MANIFEST.md summary.txt 2>/dev/null; \
  for f in run.log results.edn history.edn history.edn.gz history.txt; do
      [[ -f "$f" ]] && sha256sum "$f"
  done ) > "$OUT/sha256sums.txt"

echo "evidence archived -> $OUT"
echo "  overall-valid: ${overall_ok:-unknown}  gates-valid: ${gates_ok:-unknown}"
echo "  seed: ${seed}"
exit 0
