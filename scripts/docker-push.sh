#!/usr/bin/env bash
# docker-push.sh — 手工构建并推送 coord Docker 镜像
#
# 用法：
#   ./scripts/docker-push.sh                     # 用 Cargo.toml 中的版本
#   ./scripts/docker-push.sh 0.2.0               # 覆盖版本号
#
# 前置条件：
#   - CARGO_REGISTRIES_BYTEFORCE_TOKEN 已在环境中设置
#   - docker buildx 已就绪（docker buildx inspect 有效）
#   - 已登录目标 registry（nexus.byteforce.cn）
#
# 依赖：bash 3.2+, docker, cargo, grep, sed
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

RED='\033[0;31m'
GREEN='\033[0;32m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

# ── 读取版本 ──────────────────────────────────────────────────────────────────
if [[ $# -ge 1 ]]; then
    VERSION="$1"
else
    VERSION=$(grep -m1 '^version' "${REPO_ROOT}/Cargo.toml" | sed 's/.*"\(.*\)".*/\1/')
fi

if [[ -z "${VERSION}" ]]; then
    echo -e "${RED}无法从 Cargo.toml 读取版本，请手动传入版本号${NC}"
    exit 1
fi

IMAGE="nexus.byteforce.cn/image-private/coord"
TAG_VERSION="${IMAGE}:${VERSION}"
TAG_LATEST="${IMAGE}:latest"

# ── 检查 token ────────────────────────────────────────────────────────────────
if [[ -z "${CARGO_REGISTRIES_BYTEFORCE_TOKEN:-}" ]]; then
    echo -e "${RED}错误：CARGO_REGISTRIES_BYTEFORCE_TOKEN 未设置${NC}"
    echo "  export CARGO_REGISTRIES_BYTEFORCE_TOKEN=<your-nexus-cargo-token>"
    exit 1
fi

echo -e "${CYAN}${BOLD}构建版本：${VERSION}${NC}"
echo -e "  镜像：${TAG_VERSION}"
echo -e "  镜像：${TAG_LATEST}"
echo ""

# ── 构建并推送 ────────────────────────────────────────────────────────────────
cd "${REPO_ROOT}"

docker buildx build \
    --secret id=cargo_token,env=CARGO_REGISTRIES_BYTEFORCE_TOKEN \
    --platform linux/amd64 \
    -f docker/Dockerfile \
    -t "${TAG_VERSION}" \
    -t "${TAG_LATEST}" \
    --push \
    .

echo ""
echo -e "${GREEN}${BOLD}推送完成${NC}"
echo -e "  ${TAG_VERSION}"
echo -e "  ${TAG_LATEST}"
