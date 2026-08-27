#!/usr/bin/env bash
# Jepsen 等价验证（一致性正式验证收口）
#
# 在仓库内以真实 3 进程集群执行全故障注入矩阵 + 线性一致性校验
# （kill -9 / 重启 / SIGSTOP 暂停 / TCP 代理网络分区），即 chaos_real 套件：
#   - chaos_real_kill9_and_linearizability：kill9+暂停+分区+RegisterChecker
#
# 说明：本套件覆盖 Jepsen 的 nemesis（kill/pause/partition）与
# checker（linearizability）核心要素；独立 Clojure Jepsen 作为可选外部
# 验证（见 docs/production/evidence/m4.md 外部依赖清单）。
#
# 用法：./scripts/jepsen-check.sh   （约 2-3 分钟）
set -euo pipefail

cd "$(dirname "$0")/.."

echo "== coord jepsen-equivalent check (real 3-process cluster) =="
CHAOS_REAL=1 cargo test -p coord --release --test chaos_real chaos_real_kill9_and_linearizability \
  -- --ignored --nocapture --test-threads=1
