#!/usr/bin/env bash
# 跨语言错误码契约一致性校验（第四轮 §3.14.2）。
#
# 背景：Java SDK 的 `ErrorMapper` 以 gRPC trailer `x-coord-error-code` 为首选判据，
# 对**未知**值返回 `INTERNAL`（静默降级为"不可重试"）。因此两侧的码表必须逐字一致：
# 若 Rust 侧新增一个码而 Java 侧没有，该错误在客户端会静默变成 INTERNAL——这正是
# 本仓库反复出现的"两份定义慢慢漂移"模式，必须由**机器**而不是人来保证。
#
# 校验内容（双向）：
#  ① Rust `CoordErrorCode::ALL` 的每个 `as_str()` 字面量，都必须在 Java `ErrorCode`
#     的 `protoName` 集合中出现 —— 否则该错误在客户端静默降级为 INTERNAL；
#  ② Java 中 Rust **从不发出**的码，必须在本脚本的 CLIENT_LOCAL_ALLOWED 白名单里
#     （这些是 SDK 本地产生的码：本地校验、channel 生命周期、SDK 侧分类）。
#     不在白名单 = 新加的码没决定归属，必须显式分类。
#
# 退出码：0 = 一致；1 = 不一致（缺码/未分类的额外码）。
set -euo pipefail

# SDK 本地码（服务端从不发出）。新增 Java 码时必须在此显式登记。
CLIENT_LOCAL_ALLOWED="
PROTOCOL_MISMATCH
AGENT_UNAVAILABLE
REGISTRY_SERVICE_NOT_FOUND
REGISTRY_INSTANCE_ALREADY_EXISTS
REGISTRY_LEASE_EXPIRED
CONFIG_KEY_NOT_FOUND
CONFIG_CAS_FAILED
WATCH_STREAM_ERROR
CONFIG_INVALID
"

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
RUST_SRC="${ROOT}/coord-core/src/error_code.rs"
JAVA_SRC="${ROOT}/coord-java-sdk/src/main/java/cn/byteforce/coord/sdk/ErrorCode.java"

for f in "$RUST_SRC" "$JAVA_SRC"; do
  if [[ ! -f "$f" ]]; then
    echo "ERROR: 找不到契约文件: $f" >&2
    exit 1
  fi
done

# Rust 侧：`Self::X => "NAME",` 中的 NAME
rust_codes() {
  sed -n '/pub fn as_str(self)/,/^    }/p' "$RUST_SRC" \
    | grep -oE '"[A-Z][A-Z_]*"' | tr -d '"' | sort -u
}

# Java 侧：`NAME("NAME"),` 中的 NAME
java_codes() {
  sed -n '/^public enum ErrorCode/,/^    private final String protoName;/p' "$JAVA_SRC" \
    | grep -oE '^[[:space:]]*[A-Z][A-Z_]*\("[A-Z][A-Z_]*"\)' \
    | grep -oE '"[A-Z][A-Z_]*"' | tr -d '"' | sort -u
}

rust_list="$(rust_codes)"
java_list="$(java_codes)"

if [[ -z "$rust_list" ]]; then
  echo "ERROR: 未能从 $RUST_SRC 提取任何错误码（解析逻辑或代码结构已变）" >&2
  exit 1
fi
if [[ -z "$java_list" ]]; then
  echo "ERROR: 未能从 $JAVA_SRC 提取任何错误码（解析逻辑或代码结构已变）" >&2
  exit 1
fi

missing_in_java="$(comm -23 <(echo "$rust_list") <(echo "$java_list"))"
java_only="$(comm -13 <(echo "$rust_list") <(echo "$java_list"))"

# ② Java 独有的码必须已在白名单登记（空白与注释行忽略）
unclassified="$(comm -23 \
  <(echo "$java_only" | sed '/^$/d' | sort -u) \
  <(echo "$CLIENT_LOCAL_ALLOWED" | sed 's/#.*//' | tr -d ' \t' | sed '/^$/d' | sort -u))"

rc=0
if [[ -n "$missing_in_java" ]]; then
  echo "FAIL: Rust 发出但 Java ErrorCode 不认识（客户端会静默降级为 INTERNAL）：" >&2
  echo "$missing_in_java" | sed 's/^/  - /' >&2
  rc=1
fi
if [[ -n "$unclassified" ]]; then
  echo "FAIL: Java 有、Rust 不发，且未登记为本 SDK 本地码（见 CLIENT_LOCAL_ALLOWED）：" >&2
  echo "$unclassified" | sed 's/^/  - /' >&2
  rc=1
fi

if [[ $rc -eq 0 ]]; then
  rust_count="$(echo "$rust_list" | wc -l | tr -d ' ')"
  java_local_count="$(echo "$java_only" | sed '/^$/d' | wc -l | tr -d ' ')"
  echo "OK: 错误码契约一致（Rust 发出 ${rust_count} 个；Java 本地 ${java_local_count} 个）"
fi
exit $rc
