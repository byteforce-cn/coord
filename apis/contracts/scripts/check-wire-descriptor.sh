#!/usr/bin/env bash
# ============================================================
# check-wire-descriptor.sh — **descriptor 级**契约 ↔ 实现 wire 一致性卡口
#
# 为什么需要这一层（评审 D-e / V7 / G1）：
#   `check-wire-sync.sh` 的判定方式是"把契约字段拿到内部文件里按**字段名全文
#   搜索**"，它自己在文件头就写明了这个限制（"不精确到 message 作用域"）。
#   于是一旦字段名恰好一致，脚本就绿 —— 而"契约包真的落地"这件事仍然只是一张
#   **政策表述**：同名不同 message、字段号互换、类型改宽窄、rpc 改名、服务改名
#   都可能不被发现。
#
# 本脚本把判据升级为 **descriptor 级**（与 `jepsen/scripts/check-agent-wire.clj`
# 已经采用的"解析原文件比对，而不是比对另一份手写副本"同一思路）：
#   1) 用 protoc 从**契约副本**（apis/contracts/proto）生成 FileDescriptorSet；
#   2) 用 protoc 从**实现副本**（coord-proto/src/proto）生成 FileDescriptorSet；
#   3) 反解成文本后归一为逐包的 wire 签名并比对：
#      service / method（名、入参、出参、流式）、message / field（名、编号、
#      label、类型、type_name、oneof、proto3_optional）、enum 值、嵌套与 map。
#
# 比对以 **package** 为键（而非文件路径）：两侧副本的文件位置本就不同
# （契约在 apis/contracts/proto/coord/<domain>/v1/，实现在 coord-proto/src/proto/）。
#
# 与 `check-wire-sync.sh` 的关系：两者互补，都要跑。
#   - `check-wire-sync.sh` 管**承诺期限**（STATUS.md ↔ 实现）与路径级缺失；
#   - 本脚本管**结构性漂移**（同一个包里逐项 wire 是否真的一致）。
# 且本脚本**包含**"契约包必须存在"的判定（包集合差），不会出现"两边都缺所以都过"。
#
# 用法：bash apis/contracts/scripts/check-wire-descriptor.sh
# ============================================================
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
CONTRACT_DIR="$REPO_ROOT/apis/contracts/proto"
INTERNAL_DIR="$REPO_ROOT/coord-proto/src/proto"
SCRIPT_DIR="$REPO_ROOT/apis/contracts/scripts"

if ! command -v protoc >/dev/null 2>&1; then
  echo "FAIL: check-wire-descriptor.sh 需要 protoc（CI 由 protobuf-compiler 提供）" >&2
  exit 1
fi
if ! command -v python3 >/dev/null 2>&1; then
  echo "FAIL: check-wire-descriptor.sh 需要 python3（用于 descriptor 归一比对）" >&2
  exit 1
fi

WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

# ── 收集两侧的 proto 文件（两侧各自全部 .proto；比对时按 package 取交集/差集）──
mapfile -t CONTRACT_PROTOS < <(find "$CONTRACT_DIR" -name '*.proto' | sort)
mapfile -t INTERNAL_PROTOS < <(find "$INTERNAL_DIR" -maxdepth 1 -name '*.proto' | sort)

if [[ ${#CONTRACT_PROTOS[@]} -eq 0 ]]; then
  echo "FAIL: 契约目录下没有 .proto 文件：$CONTRACT_DIR" >&2
  exit 1
fi
if [[ ${#INTERNAL_PROTOS[@]} -eq 0 ]]; then
  echo "FAIL: 实现目录下没有 .proto 文件：$INTERNAL_DIR" >&2
  exit 1
fi

echo "契约侧：${#CONTRACT_PROTOS[@]} 个 .proto；实现侧：${#INTERNAL_PROTOS[@]} 个 .proto"

protoc -I "$CONTRACT_DIR" --include_imports -o "$WORK/contract.desc" "${CONTRACT_PROTOS[@]}"
protoc -I "$INTERNAL_DIR" --include_imports -o "$WORK/internal.desc" "${INTERNAL_PROTOS[@]}"

# ── 反解成文本（google/protobuf/descriptor.proto 随 protoc 一起安装）──
protoc --decode=google.protobuf.FileDescriptorSet google/protobuf/descriptor.proto \
  < "$WORK/contract.desc" > "$WORK/contract.txt"
protoc --decode=google.protobuf.FileDescriptorSet google/protobuf/descriptor.proto \
  < "$WORK/internal.desc" > "$WORK/internal.txt"

python3 "$SCRIPT_DIR/descriptor_signature.py" "$WORK/contract.txt" "$WORK/internal.txt"
