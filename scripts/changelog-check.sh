#!/usr/bin/env bash
# changelog-check.sh — 验证 CHANGELOG.md 中存在指定版本条目
#
# 用法：
#   ./scripts/changelog-check.sh <version>
#   例：./scripts/changelog-check.sh 0.2.0
#
# 退出码：
#   0 — 版本条目存在
#   1 — 未找到版本条目
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
NC='\033[0m'

if [[ $# -ne 1 ]]; then
    echo -e "${RED}用法: $0 <version>${NC}"
    exit 1
fi

VERSION="$1"
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
CHANGELOG="$REPO_ROOT/CHANGELOG.md"

if [[ ! -f "$CHANGELOG" ]]; then
    echo -e "${RED}[ERROR] 未找到 CHANGELOG.md${NC}"
    exit 1
fi

# 检查 ## [x.y.z] 条目是否存在
if grep -qE "^## \[$VERSION\]" "$CHANGELOG"; then
    echo -e "${GREEN}[OK] CHANGELOG.md 包含版本 [$VERSION] 条目${NC}"
    exit 0
fi

# 检查 [Unreleased] 段落是否有实质内容（非空 Unreleased 可作为待发版内容）
UNRELEASED_LINES=$(awk '/^## \[Unreleased\]/{found=1; next} found && /^## \[/{exit} found{print}' "$CHANGELOG" \
    | grep -v '^[[:space:]]*$' | wc -l | tr -d ' ') || UNRELEASED_LINES=0

if (( UNRELEASED_LINES > 0 )); then
    echo -e "${RED}[ERROR] CHANGELOG.md 中未找到版本 [$VERSION] 的条目。${NC}"
    echo "       [Unreleased] 段落有 $UNRELEASED_LINES 行内容，请先将其归档到 [$VERSION] 条目后再发版。"
    echo ""
    echo "  在 CHANGELOG.md 中添加如下格式的条目："
    echo "    ## [$VERSION] - $(date +%Y-%m-%d)"
    echo ""
    exit 1
fi

echo -e "${RED}[ERROR] CHANGELOG.md 中未找到版本 [$VERSION] 的条目，且 [Unreleased] 为空。${NC}"
echo "       请在 CHANGELOG.md 中记录本次发版变更后再重试。"
exit 1
