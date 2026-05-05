#!/usr/bin/env bash
# release.sh — coord 发版脚本
#
# 用途：
#   在本地完成版本号更新、CHANGELOG 校验、编译验证、git tag 推送。
#   实际发布构建物由 GitHub Actions release.yml 在 tag push 触发后执行。
#
# 用法：
#   ./scripts/release.sh <new-version>
#   例：./scripts/release.sh 0.2.0
#
# 依赖：cargo, git, grep, sed
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

# ── 参数检查 ───────────────────────────────────────────────────────────────────
if [[ $# -ne 1 ]]; then
    echo -e "${RED}用法: $0 <new-version>${NC}"
    echo "  例: $0 0.2.0"
    exit 1
fi

NEW_VERSION="$1"

# 版本格式校验：x.y.z
if ! [[ "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo -e "${RED}[ERROR] 版本号格式错误，须为 x.y.z（如 0.2.0）${NC}"
    exit 1
fi

# ── 工作目录检查 ──────────────────────────────────────────────────────────────
REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ -n "$(git status --porcelain)" ]]; then
    echo -e "${RED}[ERROR] 工作区有未提交的变更，请先 commit 或 stash。${NC}"
    git status --short
    exit 1
fi

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$CURRENT_BRANCH" != "main" ]]; then
    echo -e "${YELLOW}[WARN] 当前分支为 '$CURRENT_BRANCH'，建议从 main 发版。继续？[y/N]${NC} "
    read -r answer
    [[ "$answer" =~ ^[Yy]$ ]] || exit 0
fi

# ── 当前版本 ──────────────────────────────────────────────────────────────────
CURRENT_VERSION="$(grep '^version' Cargo.toml | head -1 | sed 's/.*= *"\(.*\)"/\1/')"
echo "当前版本: $CURRENT_VERSION  →  新版本: $NEW_VERSION"

if [[ "$CURRENT_VERSION" == "$NEW_VERSION" ]]; then
    echo -e "${RED}[ERROR] 新版本与当前版本相同。${NC}"
    exit 1
fi

TAG="v$NEW_VERSION"
if git rev-parse "$TAG" &>/dev/null; then
    echo -e "${RED}[ERROR] tag $TAG 已存在。${NC}"
    exit 1
fi

# ── CHANGELOG 校验 ────────────────────────────────────────────────────────────
echo "检查 CHANGELOG.md..."
bash scripts/changelog-check.sh "$NEW_VERSION"

# ── 更新 Cargo.toml workspace.package.version ────────────────────────────────
echo "更新 Cargo.toml 版本号..."
sed -i.bak "s/^version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/" Cargo.toml
rm -f Cargo.toml.bak

# Cargo.lock 随 version 变化，重新生成
cargo update --workspace --precise "$NEW_VERSION" 2>/dev/null || true
# 触发 lockfile 更新（允许失败，私有 registry 在本地可能不可达）
cargo generate-lockfile 2>/dev/null || true

# ── 本地编译验证 ──────────────────────────────────────────────────────────────
echo "运行 fmt check..."
cargo fmt --check

echo "运行 clippy..."
cargo clippy --workspace --locked -- -D warnings

echo "运行 tests..."
cargo test --workspace --locked

# ── 提交 & 打 tag ─────────────────────────────────────────────────────────────
echo "提交版本变更..."
git add Cargo.toml Cargo.lock
git commit -m "chore: release $TAG"

echo "打 tag $TAG ..."
git tag -a "$TAG" -m "Release $TAG"

echo ""
echo -e "${GREEN}本地准备完成。执行以下命令推送：${NC}"
echo "  git push origin main && git push origin $TAG"
echo ""
echo "推送 tag 后，GitHub Actions release.yml 将自动构建并创建 GitHub Release。"
