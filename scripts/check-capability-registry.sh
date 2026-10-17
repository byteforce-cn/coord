#!/usr/bin/env bash
# 能力注册表 ↔ 执行面机检（issue #115，方案 A：单一事实源 + 机检）
#
# 判据在 coord-server 的单元测试里 —— 输入是 **proto 描述符集**（编译期
# FileDescriptorSet），不是第二份手写清单：
#   1) every_proto_method_is_mapped_or_capability_free
#      proto 的每个方法要么被 rpc_capability 映射，要么在显式豁免清单里
#      （新增 RPC 漏登记即红；豁免范围变化必须显式改常量）。
#   2) registry_live_set_equals_execution_face
#      registry 的 live 集合与 rpc_capability 的输出集合**双向恒等**。
#   3) deprecated_entries_name_a_live_replacement_or_reason
#      废弃条目必须点名 live 替代能力（或写明「无替代」），且不得仍在执行面。
#   4) builtin_ids_are_domain_service_action
#      内建 ID 必须能被解析成 domain:service:action（发现面字段的来源）。
#
# 本脚本不重复实现判据，它的职责是**拒绝把「未跑」伪装成「通过」**：
# 逐条断言期望的测试真的以 `... ok` 出现。测试被改名/被删除/被过滤掉都算失败。
set -uo pipefail
cd "$(dirname "$0")/.."

EXPECTED_TESTS=(
    every_proto_method_is_mapped_or_capability_free
    registry_live_set_equals_execution_face
    deprecated_entries_name_a_live_replacement_or_reason
    builtin_ids_are_domain_service_action
)

out="$(mktemp -t coord-capability-gate.XXXXXX)"
trap 'rm -f "$out"' EXIT

if ! cargo test -p coord-server --lib auth::capability::tests:: -- --test-threads=1 >"$out" 2>&1; then
    cat "$out"
    echo "::error::capability registry gate failed (test output above)"
    exit 1
fi

missing=0
for test_name in "${EXPECTED_TESTS[@]}"; do
    if ! grep -q "^test auth::capability::tests::${test_name} \.\.\. ok$" "$out"; then
        echo "::error::expected gate test did not run (renamed/filtered/deleted?): ${test_name}"
        missing=1
    fi
done
if [ "$missing" -ne 0 ]; then
    # 未跑 ≠ 通过：把实际跑到的测试列出来，便于定位。
    grep '^test ' "$out" || true
    exit 1
fi

echo "OK: capability registry gate (${#EXPECTED_TESTS[@]} checks) — registry live set == execution face"
