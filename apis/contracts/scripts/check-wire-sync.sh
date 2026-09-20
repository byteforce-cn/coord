#!/usr/bin/env bash
# ============================================================
# check-wire-sync.sh — 对外契约 ↔ 内部实现的 wire 一致性与承诺期限卡口
#
# 三层校验：
#   1) STABLE 层：proto/coord/<svc>/<svc>.proto ↔ coord-proto/<svc>.proto，
#      rpc 存在性 + 字段编号一致（防空头承诺 / 静默漂移）；
#   2) COMMITTED 层：proto/coord/<domain>/v1/<domain>.proto ↔
#      coord-proto/src/proto/<domain>.proto，包名 + rpc + 字段编号逐项一致
#      （contracts/v1.2.0 起每个 domain 一个内部副本；迁移前基线为 agent_api.proto）；
#   3) 期限卡口：解析 STATUS.md，COMMITTED 服务 GA 期限已到而
#      coord-proto 仍未落地契约包（package coord.<domain>.v1）→ 置红。
#      —— 承诺即交付义务：逾期 = 倒逼卡口红（WHITEPAPER §13）。
#
# 已知限制：按字段名全文匹配内部文件，不精确到 message 作用域。
#   ⇒ 结构性判据已由 `check-wire-descriptor.sh` 承担（protoc FileDescriptorSet
#     反解后逐包比对 service/method/message/field/enum 的 wire 签名，含字段号与
#     类型）。两者互补、都要跑：本脚本管**路径级缺失 + 承诺期限**，那个脚本管
#     **结构性漂移**。
# ============================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CONTRACT_DIR="$REPO_ROOT/apis/contracts/proto/coord"
INTERNAL_DIR="$REPO_ROOT/coord-proto/src/proto"
STATUS_FILE="$REPO_ROOT/apis/contracts/STATUS.md"
TODAY="$(date +%Y%m%d)"

fail=0

check_wire() {
  local contract="$1" internal="$2" label="$3"
  local rpc_name field_name field_no pkg

  if [[ ! -f "$internal" ]]; then
    echo "FAIL: $label — 内部 proto $internal 不存在" >&2
    fail=1
    return
  fi

  # 0) 包名一致：契约声明的 package 必须与实现副本的 package 逐字相同。
  #    这是"落地契约包"的机器判据（比字段名全文匹配精确）。
  pkg="$(grep -m1 -oE '^package [a-z0-9_.]+;' "$contract" | sed -E 's/^package (.*);/\1/')"
  if [[ -n "$pkg" ]] && ! grep -qFx "package ${pkg};" "$internal"; then
    echo "FAIL: $label — 内部 proto 缺少 \`package ${pkg};\`（契约包未落地）" >&2
    fail=1
  fi

  while read -r rpc_name; do
    if ! grep -qE "rpc ${rpc_name}[[:space:]]*\(" "$internal"; then
      echo "FAIL: $label — rpc ${rpc_name} 在内部 proto 中不存在" >&2
      fail=1
    fi
  done < <(grep -oE 'rpc [A-Za-z]+[[:space:]]*\(' "$contract" \
             | sed -E 's/rpc ([A-Za-z]+).*/\1/')

  while read -r field_name field_no; do
    [[ -z "$field_name" ]] && continue
    if ! grep -qE "\b${field_name}[[:space:]]*=[[:space:]]*${field_no}[[:space:]]*;" "$internal"; then
      echo "FAIL: $label — 字段 ${field_name}=${field_no} 与内部 proto 不一致" >&2
      fail=1
    fi
  done < <(sed -E 's|//.*||' "$contract" \
           | grep -vE 'reserved|option' \
           | grep -oE '[a-z_][a-z0-9_]* = [0-9]+;' \
           | sed -E 's/([a-z0-9_]+) = ([0-9]+);/\1 \2/')
}

# ── 1) STABLE 层：proto/coord/<svc>/<svc>.proto ↔ coord-proto/<svc>.proto
for contract in "$CONTRACT_DIR"/*/*.proto; do
  svc="$(basename "$contract" .proto)"
  check_wire "$contract" "$INTERNAL_DIR/$svc.proto" "STABLE/$svc"
done

# ── 2) COMMITTED 层：proto/coord/<domain>/v1/<domain>.proto ↔ coord-proto/<domain>.proto
#
# contracts/v1.2.0 迁移后，每个 domain 有独立的内部副本（`package coord.<domain>.v1`），
# 不再把全部契约拿到 agent_api.proto（`package coord.agent`）里做字段名全文搜索。
# 路径对应关系由 domain 名唯一确定，缺失即置红 —— 这是"契约包是否真的落地"的机器判据。
for contract in "$CONTRACT_DIR"/*/v1/*.proto; do
  [[ -e "$contract" ]] || continue
  domain="$(basename "$contract" .proto)"
  check_wire "$contract" "$INTERNAL_DIR/$domain.proto" "COMMITTED/$domain"
done

# ── 3) 期限卡口：STATUS.md 中 COMMITTED 服务逾期未迁移至契约包 → 红
if [[ ! -f "$STATUS_FILE" ]]; then
  echo "FAIL: STATUS.md 承诺台账不存在" >&2
  fail=1
else
  while read -r pkg deadline; do
    [[ -z "$pkg" ]] && continue
    d="${deadline//-/}"
    if [[ "$d" -le "$TODAY" ]]; then
      if ! grep -rFq "package ${pkg};" "$INTERNAL_DIR"; then
        echo "FAIL: ${pkg} — GA 期限 ${deadline} 已到，coord-proto 未落地契约包（承诺未兑现，倒逼卡口置红）" >&2
        fail=1
      else
        echo "OK: ${pkg} 已按期落地契约包（期限 ${deadline}）"
      fi
    else
      echo "PENDING: ${pkg} GA 期限 ${deadline}（未到期）"
    fi
  done < <(grep -oE '\| coord\.[a-z.0-9]+ \| COMMITTED \| [0-9]{4}-[0-9]{2}-[0-9]{2} \|' "$STATUS_FILE" \
           | sed -E 's/\| (coord\.[a-z.0-9]+) \| COMMITTED \| ([0-9-]+) \|.*/\1 \2/')

  while read -r pkg deadline; do
    [[ -z "$pkg" ]] && continue
    echo "NOTICE: ${pkg} 整改承诺期限 ${deadline}（EXPERIMENTAL，治理跟踪，机器不置红）"
  done < <(grep -oE '\| coord\.[a-z.0-9]+ \| EXPERIMENTAL \| [0-9]{4}-[0-9]{2}-[0-9]{2} \|' "$STATUS_FILE" \
           | sed -E 's/\| (coord\.[a-z.0-9]+) \| EXPERIMENTAL \| ([0-9-]+) \|.*/\1 \2/')
fi

if [[ "$fail" -ne 0 ]]; then
  echo "wire-sync/期限卡口未通过：契约与内部实现漂移，或承诺逾期未兑现。" >&2
  exit 1
fi
echo "wire-sync OK：STABLE/COMMITTED 契约与 coord-proto 一致，承诺期限无逾期。"
