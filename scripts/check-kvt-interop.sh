#!/usr/bin/env bash
# ==============================================================================
# 跨语言互操作校验（KV-T G6 / #172 残余）：Rust `coord-client` ↔ Java SDK，
# 在**同一套真实 v2 集群**上对账。
#
# 判据（两个方向都做，任一侧不一致即失败）:
#   A) Rust 写：单键直写 + `coord.txn.v2` 提交两个 key + 从 `coord.watch.v2` 观察
#      到同一 `commit_ts` 的原子批 → 把**精确 commit_ts** 交给 Java；
#   B) Java 按该 `commit_ts` 作 `read_ts` 读：value / `mod_revision` /
#      `create_revision` 必须与 Rust 写的一致，且 Watch 批的 `commit_ts`
#      等于事务的 `commit_ts`；
#   C) 反向：Java 写（直写 + 事务）→ Rust 按 Java 给出的 `commit_ts` 读并断言。
#
# 为什么不能只跑单元测试：两侧各自的假服务端只能钉住自己的编解码；`revision ==
# commit_ts`（ADR-0015 D3/D12）是**同一套服务端**对两种语言客户端的行为，只有把
# 两条真实客户端接到同一套集群上才成立。
#
# 用法:
#   bash scripts/check-kvt-interop.sh                 # 自己起停 v2 dev 集群
#   COORD_KVT_INTEROP_ATTACH=1 bash scripts/check-kvt-interop.sh
#                                                    # 复用已在跑的 v2 集群
#
# 环境变量:
#   COORD_INTEROP_SERVER        Rust 客户端连的 server gRPC（默认 127.0.0.1:51051）
#   COORD_V2_AGENT_HOST/PORT    Java SDK 连的 agent（默认 127.0.0.1:19637）
#   COORD_INTEROP_EVIDENCE_DIR  证据目录（默认 /tmp/coord-kvt-interop-<UTC 时间戳>）
#   COORD_INTEROP_RUN_ID        两侧共用的 key 命名空间（默认按时间戳生成）
#   COORD_KVT_INTEROP_ATTACH    1 = 不启动/停止集群，只用已在跑的
#   MVN                         Maven 可执行文件（默认 PATH 上的 mvn）
#
# 退出码: 0 = 两个方向都对账通过；非 0 = 失败（缺集群/JDK/Maven/证据不一致）。
# 产物只写 ${COORD_INTEROP_EVIDENCE_DIR}（默认在 /tmp），不写仓库。
# ==============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT_DIR"

MAVEN="${MVN:-mvn}"
SERVER_ADDR="${COORD_INTEROP_SERVER:-127.0.0.1:51051}"
AGENT_HOST="${COORD_V2_AGENT_HOST:-127.0.0.1}"
AGENT_PORT="${COORD_V2_AGENT_PORT:-19637}"
STAMP="$(date -u +%Y%m%dT%H%M%SZ)"
RUN_ID="${COORD_INTEROP_RUN_ID:-${STAMP}-$((RANDOM))}"
EVIDENCE_DIR="${COORD_INTEROP_EVIDENCE_DIR:-${TMPDIR:-/tmp}/coord-kvt-interop-${STAMP}}"

log() { printf '[kvt-interop] %s\n' "$*"; }
die() { printf '[kvt-interop] ERROR: %s\n' "$*" >&2; exit 1; }

[[ "$RUN_ID" =~ ^[A-Za-z0-9-]+$ ]] || die "COORD_INTEROP_RUN_ID must be ASCII alphanumeric/dash: $RUN_ID"

# ── 前置：工具链必须齐全（缺了就是"没跑"，不能当成"通过"） ──
command -v cargo >/dev/null 2>&1 || die "cargo not found (Rust client phase)"
if [[ -n "${JAVA_HOME:-}" ]]; then
  [[ -x "$JAVA_HOME/bin/java" ]] || die "JAVA_HOME=$JAVA_HOME has no bin/java"
else
  command -v java >/dev/null 2>&1 || die "java not found (set JAVA_HOME or put java on PATH)"
fi
command -v "$MAVEN" >/dev/null 2>&1 || die "$MAVEN not found (set MVN or put Maven on PATH)"

mkdir -p "$EVIDENCE_DIR"
EVIDENCE_DIR="$(cd "$EVIDENCE_DIR" && pwd)"
RUN_LOG="$EVIDENCE_DIR/run.log"
: >"$RUN_LOG"
log "commit=$(git rev-parse --short HEAD) run=$RUN_ID"
log "server=$SERVER_ADDR agent=${AGENT_HOST}:${AGENT_PORT}"
log "evidence dir: $EVIDENCE_DIR"

{
  printf 'commit=%s\n' "$(git rev-parse HEAD)"
  printf 'date=%s\n' "$STAMP"
  printf 'run_id=%s\n' "$RUN_ID"
  printf 'server=%s\n' "$SERVER_ADDR"
  printf 'agent=%s:%s\n' "$AGENT_HOST" "$AGENT_PORT"
} >>"$RUN_LOG"

port_open() { (exec 3<>"/dev/tcp/$1/$2") 2>/dev/null; }

require_port() {
  local host="$1" port="$2" label="$3"
  port_open "$host" "$port" || die "$label is not reachable at ${host}:${port}"
}

server_host="${SERVER_ADDR%%:*}"
server_port="${SERVER_ADDR##*:}"

if [[ "${COORD_KVT_INTEROP_ATTACH:-0}" == "1" ]]; then
  log "attach mode: using the already running v2 cluster"
  require_port "$server_host" "$server_port" "coord server"
  require_port "$AGENT_HOST" "$AGENT_PORT" "coord agent"
else
  # 自己起一套：`ci-java-it-cluster.sh` 已做端口冲突 fail-closed、数据目录隔离。
  log "starting v2 dev cluster (coord server + coord agent)"
  bash scripts/ci-java-it-cluster.sh start v2 | tee -a "$RUN_LOG"
  trap 'bash scripts/ci-java-it-cluster.sh stop v2 >/dev/null 2>&1 || true' EXIT
fi

# ── 阶段 1：Rust 写 + 事务 + Watch 观察，交出精确 commit_ts ──
log "phase 1/3: Rust coord-client writes (direct put + txn commit) and watches the batch"
export COORD_INTEROP_SERVER="$SERVER_ADDR"
export COORD_INTEROP_EVIDENCE_DIR="$EVIDENCE_DIR"
export COORD_INTEROP_RUN_ID="$RUN_ID"
cargo test -p coord-client --test kvt_interop_peer rust_writes_for_java_to_read \
  -- --ignored --nocapture --test-threads=1 2>&1 | tee -a "$RUN_LOG"
[[ -f "$EVIDENCE_DIR/rust-to-java.properties" ]] \
  || die "Rust phase produced no evidence file (see $RUN_LOG)"

# ── 阶段 2：Java 按同一 commit_ts 读，并写出方向 C 的证据 ──
log "phase 2/3: Java SDK reads the Rust evidence at exact commit_ts and writes its own"
export COORD_V2_AGENT_HOST="$AGENT_HOST"
export COORD_V2_AGENT_PORT="$AGENT_PORT"
( cd coord-java-sdk && "$MAVEN" -B -Pinterop test ) 2>&1 | tee -a "$RUN_LOG"
[[ -f "$EVIDENCE_DIR/java-to-rust.properties" ]] \
  || die "Java phase produced no evidence file (see $RUN_LOG)"

# ── 阶段 3：反向 —— Rust 按 Java 给出的 commit_ts 读 ──
log "phase 3/3: Rust coord-client reads what Java wrote, at Java's exact commit_ts"
cargo test -p coord-client --test kvt_interop_peer rust_reads_what_java_wrote \
  -- --ignored --nocapture --test-threads=1 2>&1 | tee -a "$RUN_LOG"

log "PASS: Rust ↔ Java agree on value/mod_revision at the same commit_ts (both directions)"
log "evidence: $EVIDENCE_DIR"
printf 'result=PASS\n' >>"$RUN_LOG"
