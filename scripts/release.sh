#!/usr/bin/env bash
# release.sh — coord 发版脚本
#
# 用途：
#   无参数：显示当前最新 tag，列出待发布提交，并建议下一版本号。
#   有参数：更新版本号、自动生成 CHANGELOG、编译验证、提交并打 tag。
#           完成后给出 push 命令，由开发者手动执行推送。
#
# 用法：
#   ./scripts/release.sh                   # 查看版本状态与建议
#   ./scripts/release.sh <new-version>     # 执行发版，例：./scripts/release.sh 0.2.0
#
# 依赖：bash 3.2+, cargo, git, awk, sed
set -euo pipefail

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
NC='\033[0m'

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
cd "$REPO_ROOT"

CHANGELOG="$REPO_ROOT/CHANGELOG.md"

# ── 清理临时文件 ──────────────────────────────────────────────────────────────
_TMPDIR_CAT=""
_ENTRY_FILE=""
cleanup() {
    [[ -n "$_TMPDIR_CAT" && -d "$_TMPDIR_CAT" ]] && rm -rf "$_TMPDIR_CAT" || true
    [[ -n "$_ENTRY_FILE" && -f "$_ENTRY_FILE" ]] && rm -f "$_ENTRY_FILE" || true
}
trap cleanup EXIT

# ── 将条目文件插入 CHANGELOG 第一个版本节前 ───────────────────────────────────
insert_changelog_entry() {
    local changelog="$1"
    local entry_file="$2"
    awk '
        FNR == NR { entry = entry $0 "\n"; next }
        /^## \[/ && !inserted {
            printf "%s\n", entry
            inserted = 1
        }
        { print }
    ' "$entry_file" "$changelog" > "${changelog}.tmp" \
        && mv "${changelog}.tmp" "$changelog"
}

# ══════════════════════════════════════════════════════════════════════════════
# 无参数：版本概览 + 建议
# ══════════════════════════════════════════════════════════════════════════════
if [[ $# -eq 0 ]]; then
    LATEST_TAG=$(git tag --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -1) || LATEST_TAG=""

    if [[ -z "$LATEST_TAG" ]]; then
        echo -e "${YELLOW}当前仓库暂无版本 tag，建议从 0.1.0 开始：${NC}"
        echo "  $0 0.1.0"
        exit 0
    fi

    CURRENT_VER="${LATEST_TAG#v}"
    IFS='.' read -r MAJ MIN PAT <<< "$CURRENT_VER"
    NEXT_PATCH="$MAJ.$MIN.$((PAT + 1))"
    NEXT_MINOR="$MAJ.$((MIN + 1)).0"
    NEXT_MAJOR="$((MAJ + 1)).0.0"

    echo -e "${BOLD}${CYAN}当前最新 tag：${NC} ${GREEN}$LATEST_TAG${NC}"
    echo ""

    COMMIT_COUNT=$(git log "$LATEST_TAG..HEAD" --oneline --no-merges 2>/dev/null | wc -l | tr -d ' ') || COMMIT_COUNT=0
    if [[ "$COMMIT_COUNT" -gt 0 ]]; then
        echo -e "${CYAN}自 $LATEST_TAG 以来的 ${COMMIT_COUNT} 条提交：${NC}"
        git log "$LATEST_TAG..HEAD" --oneline --no-merges | head -20
        if (( COMMIT_COUNT > 20 )); then
            echo "  ... 还有 $((COMMIT_COUNT - 20)) 条（运行 git log ${LATEST_TAG}..HEAD --oneline 查看全部）"
        fi
        echo ""
    else
        echo -e "${YELLOW}自 $LATEST_TAG 以来暂无新提交。${NC}"
        echo ""
    fi

    echo -e "${BOLD}建议下一版本号：${NC}"
    echo -e "  ${YELLOW}补丁版本${NC} (bug 修复 / 微小改动)  ${GREEN}$NEXT_PATCH${NC}"
    echo    "    运行: $0 $NEXT_PATCH"
    echo ""
    echo -e "  ${YELLOW}次要版本${NC} (新功能 / 向后兼容)    ${GREEN}$NEXT_MINOR${NC}"
    echo    "    运行: $0 $NEXT_MINOR"
    echo ""
    echo -e "  ${YELLOW}主要版本${NC} (破坏性变更)           ${GREEN}$NEXT_MAJOR${NC}"
    echo    "    运行: $0 $NEXT_MAJOR"
    exit 0
fi

# ══════════════════════════════════════════════════════════════════════════════
# 有参数：执行发版流程
# ══════════════════════════════════════════════════════════════════════════════
if [[ $# -ne 1 ]]; then
    echo -e "${RED}用法: $0 [new-version]${NC}"
    echo "  不带参数：查看当前 tag 与版本建议"
    echo "  带版本号：执行发版，例：$0 0.2.0"
    exit 1
fi

NEW_VERSION="$1"

if ! [[ "$NEW_VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]]; then
    echo -e "${RED}[ERROR] 版本号格式错误，须为 x.y.z（如 0.2.0）${NC}"
    exit 1
fi

TAG="v$NEW_VERSION"

# ── 工作目录检查 ──────────────────────────────────────────────────────────────
if [[ -n "$(git status --porcelain)" ]]; then
    echo -e "${RED}[ERROR] 工作区有未提交的变更，请先 commit 或 stash。${NC}"
    git status --short
    exit 1
fi

CURRENT_BRANCH="$(git rev-parse --abbrev-ref HEAD)"
if [[ "$CURRENT_BRANCH" != "main" ]]; then
    echo -e "${YELLOW}[WARN] 当前分支为 '$CURRENT_BRANCH'，建议从 main 发版。继续？[y/N] ${NC}"
    read -r answer
    [[ "$answer" =~ ^[Yy]$ ]] || exit 0
fi

# ── 版本号确认 ────────────────────────────────────────────────────────────────
CURRENT_VERSION="$(grep '^version' Cargo.toml | head -1 | sed 's/.*= *"\(.*\)"/\1/')"
echo -e "${CYAN}版本更新：${NC} $CURRENT_VERSION  →  ${GREEN}$NEW_VERSION${NC}"

if [[ "$CURRENT_VERSION" == "$NEW_VERSION" ]]; then
    echo -e "${RED}[ERROR] 新版本与当前版本相同。${NC}"
    exit 1
fi

if git rev-parse "$TAG" &>/dev/null; then
    echo -e "${RED}[ERROR] tag $TAG 已存在。${NC}"
    exit 1
fi

# ── 获取上一个 tag（用于收集提交范围）────────────────────────────────────────
PREV_TAG=$(git tag --sort=-v:refname | grep -E '^v[0-9]+\.[0-9]+\.[0-9]+$' | head -1) || PREV_TAG=""

# ── CHANGELOG 处理 ─────────────────────────────────────────────────────────────
echo ""
if grep -qE "^## \[$NEW_VERSION\]" "$CHANGELOG" 2>/dev/null; then
    echo -e "${GREEN}[OK] CHANGELOG.md 已包含 [$NEW_VERSION] 条目，跳过自动生成。${NC}"
else
    echo -e "${CYAN}从 git 提交记录自动生成 CHANGELOG 条目...${NC}"

    # 收集提交 subject（跳过 merge commit 和发版 commit）
    if [[ -n "$PREV_TAG" ]]; then
        COMMITS=$(git log "${PREV_TAG}..HEAD" --no-merges --pretty=format:"%s" 2>/dev/null) || COMMITS=""
    else
        COMMITS=$(git log --no-merges --pretty=format:"%s" 2>/dev/null | head -100) || COMMITS=""
    fi

    # 按 Conventional Commits 分类写入临时文件
    _TMPDIR_CAT=$(mktemp -d)
    ADDED_F="$_TMPDIR_CAT/added"
    FIXED_F="$_TMPDIR_CAT/fixed"
    PERF_F="$_TMPDIR_CAT/perf"
    REFACTOR_F="$_TMPDIR_CAT/refactor"
    DOCS_F="$_TMPDIR_CAT/docs"
    CHANGED_F="$_TMPDIR_CAT/changed"

    while IFS= read -r line; do
        [[ -z "$line" ]] && continue
        # 跳过发版提交本身
        [[ "$line" =~ ^chore:\ release ]] && continue
        # 提取类型（去掉 scope 括号和 ! 标记）
        prefix="${line%%:*}"
        desc="${line#*: }"
        type="${prefix%%\(*}"
        type="${type%%\!*}"
        case "$type" in
            feat)     echo "- $desc" >> "$ADDED_F"    ;;
            fix)      echo "- $desc" >> "$FIXED_F"    ;;
            perf)     echo "- $desc" >> "$PERF_F"     ;;
            refactor) echo "- $desc" >> "$REFACTOR_F" ;;
            docs)     echo "- $desc" >> "$DOCS_F"     ;;
            *)        echo "- $line" >> "$CHANGED_F"  ;;
        esac
    done <<< "${COMMITS}"

    # 构建条目文件（Keep a Changelog 格式）
    TODAY=$(date +%Y-%m-%d)
    _ENTRY_FILE=$(mktemp)
    echo "## [$NEW_VERSION] - $TODAY" > "$_ENTRY_FILE"

    ENTRY_EMPTY=true
    for pair in \
        "${ADDED_F}:### 新增" \
        "${FIXED_F}:### 修复" \
        "${PERF_F}:### 性能" \
        "${REFACTOR_F}:### 重构" \
        "${DOCS_F}:### 文档" \
        "${CHANGED_F}:### 修改"
    do
        section_file="${pair%%:*}"
        section_head="${pair#*:}"
        [[ -f "$section_file" && -s "$section_file" ]] || continue
        ENTRY_EMPTY=false
        { echo ""; echo "$section_head"; cat "$section_file"; } >> "$_ENTRY_FILE"
    done

    if [[ "$ENTRY_EMPTY" == true ]]; then
        { echo ""; echo "### 修改"; echo "- 版本更新"; } >> "$_ENTRY_FILE"
    fi

    # 预览生成内容
    echo ""
    echo -e "${CYAN}────────────────── 自动生成的 CHANGELOG 条目 ──────────────────${NC}"
    cat "$_ENTRY_FILE"
    echo -e "${CYAN}──────────────────────────────────────────────────────────────${NC}"
    echo ""
    echo -e "${YELLOW}如何继续？${NC}"
    echo "  [y] 确认写入并继续发版"
    echo "  [e] 写入文件后手动编辑，完成后按 Enter 继续"
    echo "  [q] 退出"
    printf "请选择 [y/e/q]: "
    read -r choice

    case "$choice" in
        [Yy])
            insert_changelog_entry "$CHANGELOG" "$_ENTRY_FILE"
            echo -e "${GREEN}[OK] CHANGELOG.md 已更新。${NC}"
            ;;
        [Ee])
            insert_changelog_entry "$CHANGELOG" "$_ENTRY_FILE"
            echo ""
            echo -e "${YELLOW}已写入 CHANGELOG.md，请编辑后按 Enter 继续：${NC}"
            echo "  $CHANGELOG"
            read -r _
            if ! grep -qE "^## \[$NEW_VERSION\]" "$CHANGELOG"; then
                echo -e "${RED}[ERROR] CHANGELOG.md 中未找到 [$NEW_VERSION] 条目，请检查后重试。${NC}"
                exit 1
            fi
            echo -e "${GREEN}[OK] CHANGELOG.md 确认完毕。${NC}"
            ;;
        *)
            echo "已退出。"
            exit 0
            ;;
    esac
fi

# ── 更新 Cargo.toml workspace.package.version ────────────────────────────────
echo ""
echo "更新 Cargo.toml 版本号..."
sed -i.bak "s/^version = \"$CURRENT_VERSION\"/version = \"$NEW_VERSION\"/" Cargo.toml
rm -f Cargo.toml.bak

# 更新 Cargo.lock（允许失败，私有 registry 在本地可能不可达）
echo "更新 Cargo.lock..."
cargo generate-lockfile 2>/dev/null || true

# ── 本地编译验证 ──────────────────────────────────────────────────────────────
echo "运行 cargo fmt --check..."
cargo fmt --check

echo "运行 cargo clippy..."
cargo clippy --workspace --locked -- -D warnings

echo "运行 cargo test..."
cargo test --workspace --locked

# ── 提交 & 打 tag ─────────────────────────────────────────────────────────────
echo ""
echo "提交版本变更..."
git add Cargo.toml Cargo.lock CHANGELOG.md
git commit -m "chore: release $TAG"

echo "打 tag $TAG ..."
git tag -a "$TAG" -m "Release $TAG"

echo ""
echo -e "${BOLD}${GREEN}✓ 本地发版准备完成！${NC}"
echo ""
echo -e "${CYAN}执行以下命令推送到远端：${NC}"
echo ""
echo "  git push origin main && git push origin $TAG"
echo ""
echo -e "${YELLOW}推送后，GitHub Actions release.yml 将自动构建并创建 GitHub Release。${NC}"
