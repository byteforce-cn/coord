#!/usr/bin/env bash
# ==============================================================================
# 运行 java-example 的 KV-T v2 示例（`KvTV2Example`）——需要一套**真实 v2 集群**。
#
# 为什么要有这个包装脚本：示例只用 proto stub（不引 SDK），因此必须先编译
# `java-example` 模块（含 protoc 生成契约 stub）再拼 classpath；把「编译 + 生成
# stub + 运行」写在一处，文档里的命令才不会和 pom 漂移。
#
# 用法:
#   bash java-example/scripts/run-v2-example.sh
#   COORD_V2_AGENT_HOST=127.0.0.1 COORD_V2_AGENT_PORT=19637 \
#     bash java-example/scripts/run-v2-example.sh
#
# 前置:
#   * v2 集群（multi-raft + PD + object storage + `[kv_t].enabled=true`）：
#       scripts/ci-java-it-cluster.sh start v2
#     集群不存在或不是 v2 时示例**硬失败**（退出码 2），不会降级到 v1 API。
#   * JDK 21+（`JAVA_HOME` 优先，否则用 PATH 上的 `java`）、Maven 3.9+（`MVN` 可覆盖）。
#
# 退出码: 0 = 全部断言通过；1 = 断言/RPC 失败；2 = 目标集群不是 v2。
# ==============================================================================
set -euo pipefail

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
MAVEN="${MVN:-mvn}"

die() { printf '[v2-example] ERROR: %s\n' "$*" >&2; exit 1; }

if [[ -n "${JAVA_HOME:-}" ]]; then
  JAVA_BIN="$JAVA_HOME/bin/java"
else
  JAVA_BIN="$(command -v java || true)"
fi
[[ -n "$JAVA_BIN" && -x "$JAVA_BIN" ]] || die "java not found (set JAVA_HOME or put java on PATH)"
command -v "$MAVEN" >/dev/null 2>&1 || die "$MAVEN not found (set MVN or put maven on PATH)"

CP_FILE="$(mktemp "${TMPDIR:-/tmp}/coord-kvt-v2-example-cp.XXXXXX")"
trap 'rm -f "$CP_FILE"' EXIT

# `-f` 定位 pom：protoc 的 protoSourceRoot 是相对 project.basedir 的，
# 因此不依赖调用者的当前目录。
"$MAVEN" -q -B -f "$PROJECT_DIR/pom.xml" -DskipTests compile
"$MAVEN" -q -B -f "$PROJECT_DIR/pom.xml" dependency:build-classpath \
  -DincludeScope=runtime -Dmdep.outputFile="$CP_FILE"

CP="$PROJECT_DIR/target/classes:$(cat "$CP_FILE")"
set +e
"$JAVA_BIN" -cp "$CP" cn.byteforce.coord.example.KvTV2Example "$@"
RC=$?
set -e
printf '[v2-example] exit code: %s\n' "$RC"
exit "$RC"
