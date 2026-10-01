#!/usr/bin/env bash
# ==============================================================================
# 为 Java 集成测试启动一套**真实** dev 集群（coord server + coord agent）
#
# 背景（D3）：java-example 的 10 个集成测试此前把 `localhost:19527` 写死、且无任何
# CI 执行它们 —— "Java 接入可用" 这句话在仓库里没有任何可复现证据。本脚本把
# 「起集群」这一步固化下来，供 `.github/workflows/ci.yml` 的 `java-example-it`
# job 与本地开发者共用，避免两边各写一份必然会漂移的命令。
#
# 用法:
#   scripts/ci-java-it-cluster.sh start     # 启动（写 PID 文件，前台输出日志）
#   scripts/ci-java-it-cluster.sh stop      # 停止
#   scripts/ci-java-it-cluster.sh env       # 打印 COORD_AGENT_HOST/PORT 等变量
#   scripts/ci-java-it-cluster.sh wait      # 等待就绪（CI 里 start 后调用）
#
# 环境变量:
#   COORD_BIN         coord 二进制路径（默认 target/debug/coord，可由 cargo 构建）
#   COORD_RUN_DIR     运行目录（默认 .coord-dev-cluster，PID/日志/数据都在这）
#   COORD_AGENT_PORT  agent gRPC 端口（默认 19527，与 Java 测试默认值一致）
#   COORD_GRPC_PORT   server gRPC 端口（默认 50051）
#   COORD_RAFT_PORT   server raft 端口（默认 50061）
#
# 退出码：0 = 集群就绪；非 0 = 启动失败（并打印日志尾部）
# ==============================================================================
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUN_DIR="${COORD_RUN_DIR:-$ROOT_DIR/.coord-dev-cluster}"
COORD_BIN="${COORD_BIN:-$ROOT_DIR/target/debug/coord}"
AGENT_PORT="${COORD_AGENT_PORT:-19527}"
AGENT_HTTP_PORT="${COORD_AGENT_HTTP_PORT:-19528}"
GRPC_PORT="${COORD_GRPC_PORT:-50051}"
RAFT_PORT="${COORD_RAFT_PORT:-50061}"
READY_TIMEOUT_SECS="${COORD_READY_TIMEOUT_SECS:-60}"

log() { printf '[dev-cluster] %s\n' "$*"; }
die() { printf '[dev-cluster] ERROR: %s\n' "$*" >&2; exit 1; }

require_bin() {
  [[ -x "$COORD_BIN" ]] || die "coord binary not found/executable: $COORD_BIN (run: cargo build --workspace --bins)"
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
[security]
auth_enabled = false
EOF
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
    if (exec 3<>"/dev/tcp/${host}/${port}") 2>/dev/null; then
      exec 3>&- 2>/dev/null || true
      log "$label ready on ${host}:${port}"
      return 0
    fi
    sleep 0.3
  done
  return 1
}

start() {
  require_bin
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
  log "run Java ITs with: (cd java-example && COORD_AGENT_PORT=${AGENT_PORT} mvn -B verify -Pit)"
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
