#!/bin/bash
# 跑 M5a 的四个 checker fixture 套件（离线，不需要集群）。
# 用法：从 jepsen/ 目录执行： bash scripts/run-m5a-fixtures.sh
set -u
cd "$(dirname "$0")/.." || exit 1
fail=0
for spec in "jepsen.coord.lockck scripts/lock-fixtures" \
            "jepsen.coord.electck scripts/elect-fixtures" \
            "jepsen.coord.idgenck scripts/idgen-fixtures" \
            "jepsen.coord.regck scripts/registry-fixtures"; do
  echo "=== $spec ==="
  LEIN_ROOT=true lein -o run -m clojure.main scripts/run-checker-tests.clj $spec \
    | grep -E "PASS|FAIL|fixtures passed|Syntax error|Exception" || true
done
