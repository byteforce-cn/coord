#!/usr/bin/env bash
# D3 证据采集：真实 coord server + coord agent + java-example 全量集成套件。
#
# 被 `scripts/collect-evidence.sh java-it` 调用（也会把原始输出 tee 到 run.log）。
# 单独运行也安全：只在本地 .coord-dev-cluster/ 下起进程，退出时清理。
#
# 前置：JDK 21（或与 pom 的 <release>21</release> 兼容的 JDK）、maven、cargo。
# 退出码 = mvn 的退出码（0 = 全部集成测试通过）。
set -euo pipefail

cd "$(dirname "$0")/.."

echo "==> building coord binaries (server + agent)"
cargo build --workspace --bins

cleanup() {
    bash scripts/ci-java-it-cluster.sh stop >/dev/null 2>&1 || true
    echo "==> dev cluster log tails"
    tail -40 .coord-dev-cluster/server.log 2>/dev/null || true
    tail -40 .coord-dev-cluster/agent.log 2>/dev/null || true
}
trap cleanup EXIT

echo "==> starting dev cluster (real server + real agent)"
bash scripts/ci-java-it-cluster.sh start

echo "==> running java-example integration suites (mvn verify -Pit)"
set +e
( cd java-example && mvn -B verify -Pit )
RC=$?
set -e

echo "==> java integration suites exit code: ${RC}"
exit "$RC"
