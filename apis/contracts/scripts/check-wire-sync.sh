#!/usr/bin/env bash
# ============================================================
# check-wire-sync.sh — 对外契约 ↔ 内部实现的 wire 一致性卡口
#
# 防止两类腐化：
#   1) 契约承诺了线端不存在的 rpc / 字段（空头承诺）；
#   2) 内部重构改了字段编号 / 类型，与契约漂移（静默 Breaking）。
#
# 检查规则（对 contracts/proto/coord/<svc>/<svc>.proto 逐个执行）：
#   - 契约中的每个 rpc 必须在内部 proto 中存在同名 rpc；
#   - 契约中的每个字段（按字段名匹配）在内部 proto 中的编号必须一致；
#   - Maintenance 为裁剪版：只校验契约中声明的子集（reserved 不校验）。
#
# 已知限制：按字段名全文匹配内部文件，不精确到 message 作用域。
# 字段名在各 proto 中基本唯一，对本仓库足够；若出现误报请人工复核。
# ============================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CONTRACT_DIR="$REPO_ROOT/apis/contracts/proto/coord"
INTERNAL_DIR="$REPO_ROOT/coord-proto/src/proto"

fail=0

for contract in "$CONTRACT_DIR"/*/*.proto; do
  svc="$(basename "$contract" .proto)"
  internal="$INTERNAL_DIR/$svc.proto"
  if [[ ! -f "$internal" ]]; then
    echo "FAIL: $svc — 内部 proto $internal 不存在" >&2
    fail=1
    continue
  fi

  # 1) rpc 存在性
  while read -r rpc_name; do
    if ! grep -qE "rpc ${rpc_name}\s*\(" "$internal"; then
      echo "FAIL: $svc — rpc ${rpc_name} 在内部 proto 中不存在" >&2
      fail=1
    fi
  done < <(grep -oE 'rpc [A-Za-z]+\s*\(' "$contract" | sed -E 's/rpc ([A-Za-z]+).*/\1/')

  # 2) 字段编号一致性（跳过 reserved / oneof 包装行 / option 行）
  while read -r field_name field_no; do
    [[ -z "$field_name" ]] && continue
    if ! grep -qE "\b${field_name}\s*=\s*${field_no}\s*;" "$internal"; then
      echo "FAIL: $svc — 字段 ${field_name}=${field_no} 与内部 proto 不一致" >&2
      fail=1
    fi
  done < <(grep -vE 'reserved|option|//' "$contract" \
           | grep -oE '[a-z_][a-z0-9_]* = [0-9]+;' \
           | sed -E 's/([a-z0-9_]+) = ([0-9]+);/\1 \2/')
done

if [[ "$fail" -ne 0 ]]; then
  echo "wire-sync 检查未通过：契约与内部实现已漂移。" >&2
  exit 1
fi
echo "wire-sync OK：契约承诺的 rpc/字段编号与 coord-proto 一致。"
