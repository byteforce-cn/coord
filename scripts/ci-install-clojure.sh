#!/usr/bin/env bash
# 安装 Clojure reader：只用于 `jepsen/**/*.clj` 的**语法预检**
# （`jepsen/scripts/check-clj-syntax.clj`），不装 jepsen 依赖。
#
# 为什么需要它：CI 跑不了 jepsen 的 `lein test`（jepsen 0.3.14-SNAPSHOT 不在 Maven
# Central），于是 `jepsen/src/**`、`jepsen/scripts/**` 的 reader 级语法错误在 CI 里
# 完全不可见 —— 测试全绿，`make soak-kv-t` 起来才炸（`1_000`：Clojure 没有数字
# 分隔符字面量）。语法检查只读 reader，不需要 jepsen 依赖。
#
# `clojure.core` 自身依赖 spec.alpha / core.specs.alpha，故三者都取 Maven Central
# 的定版 jar。Clojure 版本直接读 `jepsen/project.clj`（不另维护一份）；校验和写死：
# 下载内容不可信即失败。**版本升级必须同时更新下面的 pin**（脚本会点名拒绝），
# 不允许"下不到就当通过"。
#
# 用法：bash scripts/ci-install-clojure.sh
#   stdout 只输出一行 classpath；进度信息走 stderr。
#   CLJ_READER_DIR 可覆盖安装目录（默认 /tmp/coord-clojure-reader）。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
PROJECT_CLJ="$ROOT/jepsen/project.clj"
DEST="${CLJ_READER_DIR:-/tmp/coord-clojure-reader}"
BASE="https://repo1.maven.org/maven2/org/clojure"

log() { echo "$*" >&2; }
die() {
    echo "ci-install-clojure: $*" >&2
    exit 1
}

VERSION="$(sed -n 's/.*org\.clojure\/clojure "\([^"]*\)".*/\1/p' "$PROJECT_CLJ" | head -1)"
[[ -n "$VERSION" ]] || die "cannot read the org.clojure/clojure version from $PROJECT_CLJ"

if [[ "$VERSION" != "1.12.5" ]]; then
    die "jepsen/project.clj pins org.clojure/clojure $VERSION but this script's pins are for 1.12.5 — update the artifact checksums here (and the spec.alpha / core.specs.alpha versions if needed)"
fi

# name|relative-path|sha256
readonly ARTIFACTS=(
    "clojure.jar|clojure/1.12.5/clojure-1.12.5.jar|dfdee131460216a84fef22e537bb0d0579965f8f70338d748323dff26f8e5a8b"
    "spec.alpha.jar|spec.alpha/0.5.238/spec.alpha-0.5.238.jar|94cd99b6ea639641f37af4860a643b6ed399ee5a8be5d717cff0b663c8d75077"
    "core.specs.alpha.jar|core.specs.alpha/0.4.74/core.specs.alpha-0.4.74.jar|eb73ac08cf49ba840c88ba67beef11336ca554333d9408808d78946e0feb9ddb"
)

mkdir -p "$DEST"
for spec in "${ARTIFACTS[@]}"; do
    IFS='|' read -r name path sha <<<"$spec"
    target="$DEST/$name"
    if [[ -f "$target" ]] && echo "$sha  $target" | sha256sum -c --status -; then
        log "cached: $name"
        continue
    fi
    log "fetching: $BASE/$path"
    curl -fsSL --retry 3 --retry-delay 2 -o "$target.tmp" "$BASE/$path" \
        || die "download failed: $BASE/$path"
    echo "$sha  $target.tmp" | sha256sum -c --status - \
        || die "checksum mismatch for $BASE/$path (expected $sha)"
    mv "$target.tmp" "$target"
done

# classpath 的单一输出行（供 `java -cp "$(...)"` 使用）。
echo "$DEST/clojure.jar:$DEST/spec.alpha.jar:$DEST/core.specs.alpha.jar"
