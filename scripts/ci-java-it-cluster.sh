#!/usr/bin/env bash
# ==============================================================================
# 为 Java 集成测试启动一套**真实** dev 集群（coord server + coord agent）
#
# 真实进程集成测试需要一套可复现的 server+agent；CI 与本地共用本脚本，避免两边
# 各写一份必然漂移的命令。
#
# 用法:
#   scripts/ci-java-it-cluster.sh start [v1|v2]
#   scripts/ci-java-it-cluster.sh stop  [v1|v2]
#   scripts/ci-java-it-cluster.sh env   [v1|v2]
#   scripts/ci-java-it-cluster.sh wait  [v1|v2]
#
#   v1（默认）：单 Raft，v1 数据面可用（java-example / SDK v1 IT）。
#   v2：单节点 multi-raft + PD + object storage + `[kv_t].enabled=true`；
#       v1 数据面被冻结，只服务 `coord.*.v2`（SDK v2 IT）。
#
# 两套模式的端口与运行目录默认不同，可同时运行。启动前检查端口未被占用：
# 占用时 fail-closed —— 否则 `wait` 会把别的进程（例如机器上已有的 coord）
# 当成自己的集群，测试就会连到陌生服务上。
#
# 环境变量:
#   COORD_BIN         coord 二进制路径（默认 target/debug/coord，可由 cargo 构建）
#   COORD_RUN_DIR     运行目录（v1 默认 .coord-dev-cluster，v2 默认 /tmp/coord-dev-cluster-v2）
#   COORD_AGENT_PORT  agent gRPC 端口（v1 默认 19527，v2 默认 19637）
#   COORD_GRPC_PORT   server gRPC 端口（v1 默认 50051，v2 默认 51051）
#   COORD_RAFT_PORT   server raft 端口（v1 默认 50061，v2 默认 51061）
#   COORD_KEEP_DATA   1 = `start v2` 不清空数据目录（默认清空，夹具不继承状态）
#
# 退出码：0 = 集群就绪；非 0 = 启动失败（并打印日志尾部）
# ==============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MODE="${2:-v1}"

case "$MODE" in
  v1)
    DEFAULT_RUN_DIR="$ROOT_DIR/.coord-dev-cluster"
    DEFAULT_AGENT_PORT=19527
    DEFAULT_AGENT_HTTP_PORT=19528
    DEFAULT_GRPC_PORT=50051
    DEFAULT_RAFT_PORT=50061
    DEFAULT_HTTP_PORT=50071
    ;;
  v2)
    DEFAULT_RUN_DIR="${TMPDIR:-/tmp}/coord-dev-cluster-v2"
    DEFAULT_AGENT_PORT=19637
    DEFAULT_AGENT_HTTP_PORT=19638
    DEFAULT_GRPC_PORT=51051
    DEFAULT_RAFT_PORT=51061
    DEFAULT_HTTP_PORT=51071
    ;;
  *)
    printf '[dev-cluster] ERROR: unknown mode: %s (expected v1|v2)\n' "$MODE" >&2
    exit 1
    ;;
esac

RUN_DIR="${COORD_RUN_DIR:-$DEFAULT_RUN_DIR}"
COORD_BIN="${COORD_BIN:-$ROOT_DIR/target/debug/coord}"
AGENT_PORT="${COORD_AGENT_PORT:-$DEFAULT_AGENT_PORT}"
AGENT_HTTP_PORT="${COORD_AGENT_HTTP_PORT:-$DEFAULT_AGENT_HTTP_PORT}"
GRPC_PORT="${COORD_GRPC_PORT:-$DEFAULT_GRPC_PORT}"
RAFT_PORT="${COORD_RAFT_PORT:-$DEFAULT_RAFT_PORT}"
HTTP_PORT="${COORD_HTTP_PORT:-$DEFAULT_HTTP_PORT}"
READY_TIMEOUT_SECS="${COORD_READY_TIMEOUT_SECS:-60}"

log() { printf '[dev-cluster:%s] %s\n' "$MODE" "$*"; }
die() { printf '[dev-cluster:%s] ERROR: %s\n' "$MODE" "$*" >&2; exit 1; }

require_bin() {
  [[ -x "$COORD_BIN" ]] || die "coord binary not found/executable: $COORD_BIN (run: cargo build --workspace --bins)"
}

port_in_use() {
  (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null
}

require_free() {
  local port="$1" label="$2"
  if port_in_use "$port"; then
    die "$label port $port is already in use; stop that process or override the COORD_*_PORT variable"
  fi
}

write_configs() {
  mkdir -p "$RUN_DIR/server" "$RUN_DIR/agent"
  cat >"$RUN_DIR/server.toml" <<EOF
# dev 集群：单节点、明文、鉴权关闭（仅供集成测试）
[node]
id = 1
[network]
grpc_addr = "127.0.0.1:${GRPC_PORT}"
raft_addr = "127.0.0.1:${RAFT_PORT}"
http_addr = "127.0.0.1:${HTTP_PORT}"
[security]
auth_enabled = false
EOF

  if [[ "$MODE" == "v2" ]]; then
    cat >>"$RUN_DIR/server.toml" <<EOF

# v2：两个数据 Region 以 "m" 为界（跨区事务/范围读需要），
# [/obj/, /obj0) 由 object storage 在种子期另行切出。
[cluster]
cluster_name = "coord-dev-cluster-v2"
bootstrap = true
[[cluster.initial_nodes]]
id = 1
grpc = "127.0.0.1:${GRPC_PORT}"
raft = "127.0.0.1:${RAFT_PORT}"

[multi_raft]
enabled = true
[[multi_raft.initial_regions]]
id = 1
start_key = ""
end_key = "m"
[[multi_raft.initial_regions]]
id = 2
start_key = "m"
end_key = ""

[multi_raft.pd]
enabled = true
target_replicas = 1

[object_storage]
enabled = true

[kv_t]
enabled = true
EOF
  fi

  cat >"$RUN_DIR/agent.toml" <<EOF
agent_addr = "127.0.0.1:${AGENT_PORT}"
http_addr = "127.0.0.1:${AGENT_HTTP_PORT}"
data_dir = "${RUN_DIR}/agent"
discovery_mode = "static"
static_peers = ["127.0.0.1:${GRPC_PORT}"]

[auth]
enabled = false

[services]
# Java 集成套件只用核心面（KV/Txn/Lease/Watch/Maintenance + Registry），
# 但 agent 侧这些服务需要显式开启。
registry = true
config_center = true
lock = true
idgen = true
leader_election = true
EOF
}

wait_port() {
  local host="$1" port="$2" label="$3"
  local deadline=$(( $(date +%s) + READY_TIMEOUT_SECS ))
  while (( $(date +%s) < deadline )); do
    if port_in_use "$port"; then
      log "$label ready on ${host}:${port}"
      return 0
    fi
    sleep 0.3
  done
  return 1
}

start() {
  require_bin
  require_free "$GRPC_PORT" "server gRPC"
  require_free "$RAFT_PORT" "server raft"
  require_free "$HTTP_PORT" "server http"
  require_free "$AGENT_PORT" "agent gRPC"
  require_free "$AGENT_HTTP_PORT" "agent http"
  # 集成夹具不继承上一次运行的状态：v2 的 decision apply 允许在停机时仍有
  # pending 决策，复用目录会让下一次运行的直写越过它们。保留现场用
  # COORD_KEEP_DATA=1（调试恢复问题）。
  if [[ "$MODE" == "v2" && "${COORD_KEEP_DATA:-0}" != "1" ]]; then
    rm -rf "$RUN_DIR/server" "$RUN_DIR/agent"
    log "fresh data dirs under $RUN_DIR"
  fi
  write_configs

  log "starting coord server (grpc=${GRPC_PORT}, raft=${RAFT_PORT})"
  "$COORD_BIN" server \
    --id 1 \
    --addr "127.0.0.1:${GRPC_PORT}" \
    --raft-addr "127.0.0.1:${RAFT_PORT}" \
    --data-dir "$RUN_DIR/server" \
    --config "$RUN_DIR/server.toml" \
    --bootstrap \
    >"$RUN_DIR/server.log" 2>&1 &
  echo $! >"$RUN_DIR/server.pid"

  if ! wait_port 127.0.0.1 "$GRPC_PORT" "coord server"; then
    tail -30 "$RUN_DIR/server.log" >&2 || true
    stop
    die "coord server never became ready"
  fi

  log "starting coord agent (grpc=${AGENT_PORT})"
  "$COORD_BIN" agent \
    --agent-config "$RUN_DIR/agent.toml" \
    --data-dir "$RUN_DIR/agent" \
    >"$RUN_DIR/agent.log" 2>&1 &
  echo $! >"$RUN_DIR/agent.pid"

  if ! wait_port 127.0.0.1 "$AGENT_PORT" "coord agent"; then
    tail -30 "$RUN_DIR/agent.log" >&2 || true
    stop
    die "coord agent never became ready"
  fi

  log "cluster ready: agent=127.0.0.1:${AGENT_PORT} server=127.0.0.1:${GRPC_PORT}"
  log "run Java ITs with: $(show_env | tr '\n' ' ')"
}

stop() {
  for name in agent server; do
    local pid_file="$RUN_DIR/$name.pid"
    if [[ -f "$pid_file" ]]; then
      local pid
      pid="$(cat "$pid_file")"
      if kill -0 "$pid" 2>/dev/null; then
        kill "$pid" 2>/dev/null || true
        for _ in $(seq 1 20); do
          kill -0 "$pid" 2>/dev/null || break
          sleep 0.2
        done
        kill -9 "$pid" 2>/dev/null || true
      fi
      rm -f "$pid_file"
    fi
  done
  log "stopped"
}

show_env() {
  printf 'export COORD_AGENT_HOST=127.0.0.1\n'
  printf 'export COORD_AGENT_PORT=%s\n' "$AGENT_PORT"
  if [[ "$MODE" == "v2" ]]; then
    printf 'export COORD_V2_AGENT_HOST=127.0.0.1\n'
    printf 'export COORD_V2_AGENT_PORT=%s\n' "$AGENT_PORT"
  fi
  printf 'export COORD_SERVER_ADDR=127.0.0.1:%s\n' "$GRPC_PORT"
}

case "${1:-start}" in
  start) start ;;
  stop) stop ;;
  env) show_env ;;
  wait)
    wait_port 127.0.0.1 "$AGENT_PORT" "coord agent" || die "agent not ready on ${AGENT_PORT}"
    ;;
  *) die "unknown action: $1 (expected: start|stop|env|wait)" ;;
esac
