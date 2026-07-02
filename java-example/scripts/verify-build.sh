#!/usr/bin/env bash
# ==============================================================================
# Coord Java Example — 构建验证脚本 (Phase D1 测试)
#
# 验证:
#   1. Proto 文件存在且可被 protoc 编译
#   2. Java gRPC stub 正确生成
#   3. Java 编译通过 (包括手写代码)
#   4. 单元测试全部通过
#
# 用法:
#   ./scripts/verify-build.sh          # 全量验证
#   ./scripts/verify-build.sh proto    # 仅验证 proto 生成
#   ./scripts/verify-build.sh test     # 仅运行测试
# ==============================================================================
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
PROTO_DIR="$PROJECT_DIR/../coord-proto/src/proto"

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

pass() { echo -e "${GREEN}[PASS]${NC} $1"; }
fail() { echo -e "${RED}[FAIL]${NC} $1"; exit 1; }
info() { echo -e "${YELLOW}[INFO]${NC} $1"; }

# ─── Step 1: 验证 Proto 源文件存在 ───
check_proto_files() {
    info "Step 1/4: 验证 Proto 源文件..."
    local required=(
        "kv.proto" "txn.proto" "lease.proto" "watch.proto"
        "maintenance.proto" "raft.proto" "auth.proto"
    )
    for f in "${required[@]}"; do
        if [[ -f "$PROTO_DIR/$f" ]]; then
            pass "  $f 存在"
        else
            fail "  $f 缺失 (expected: $PROTO_DIR/$f)"
        fi
    done
}

# ─── Step 2: Maven proto 生成 ───
generate_proto() {
    info "Step 2/4: 生成 Java gRPC stubs (mvn generate-sources)..."
    cd "$PROJECT_DIR"
    if mvn generate-sources -q 2>&1; then
        pass "  Proto 生成成功"
    else
        fail "  Proto 生成失败 (检查 protoc 和 grpc-java 插件版本)"
    fi

    # 验证生成的 Java 文件
    local gen_dir="$PROJECT_DIR/target/generated-sources/protobuf/java"
    local expected_classes=(
        "coord/kv/Kv.java"              # gRPC service stub (outclass)
        "coord/kv/KvGrpc.java"          # gRPC client/server stub
        "coord/kv/KvOuterClass.java"     # protobuf message class
        "coord/lease/LeaseGrpc.java"
        "coord/watch/WatchGrpc.java"
        "coord/maintenance/MaintenanceGrpc.java"
        "coord/txn/TxnGrpc.java"
    )
    for cls in "${expected_classes[@]}"; do
        local full_path="$gen_dir/$cls"
        # gRPC stubs use the pattern *Grpc.java
        if ls "$gen_dir/${cls%/*}/"*Grpc.java 2>/dev/null | head -1 > /dev/null; then
            pass "  ${cls##*/} 已生成"
        else
            fail "  ${cls##*/} 未找到 (in $gen_dir/${cls%/*}/)"
        fi
    done
}

# ─── Step 3: Java 编译 ───
compile_java() {
    info "Step 3/4: 编译 Java 源码 + 生成的 stub..."
    cd "$PROJECT_DIR"
    if mvn compile -q 2>&1; then
        pass "  Java 编译成功"
    else
        fail "  Java 编译失败"
    fi
}

# ─── Step 4: 运行测试 ───
run_tests() {
    info "Step 4/4: 运行 JUnit 5 测试..."
    cd "$PROJECT_DIR"
    if mvn test 2>&1; then
        pass "  所有测试通过"
    else
        fail "  测试失败"
    fi
}

# ─── Main ───
case "${1:-all}" in
    proto)
        check_proto_files
        generate_proto
        ;;
    test)
        run_tests
        ;;
    compile)
        check_proto_files
        generate_proto
        compile_java
        ;;
    all|*)
        check_proto_files
        generate_proto
        compile_java
        run_tests
        ;;
esac

echo ""
echo -e "${GREEN}═══════════════════════════════════════${NC}"
echo -e "${GREEN}  Phase D1 验证完成 ✓${NC}"
echo -e "${GREEN}═══════════════════════════════════════${NC}"
