#!/usr/bin/env bash
#
# 负控制：取证身份管道本身必须 fail-closed（#303 的采集器，别只「写过了」）。
#
# 覆盖四个方向：
#   1. 起跑头 + 节点哈希一致            => 出 MANIFEST（身份来源 = 日志）
#   2. 节点哈希与控制机不一致            => 退出码 3，且不产生 MANIFEST
#   3. 日志里没有身份头                  => 退出码 3（「没记录」不等于「通过」）
#   4. 历史 store + COORD_IDENTITY_OVERRIDE => 出 MANIFEST，标注 override(post-hoc)
#
# 第 3 条正是本轮踩到的坑：coord-soak.sh 只把身份头 echo 到终端，日志里没有，
# 采集器对着一轮干净的 72h 也只会拒绝——生产者与消费者必须一起被测。
#
# 用法：bash scripts/check-evidence-identity.sh
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
COLLECT="$REPO_ROOT/jepsen/scripts/collect-evidence.sh"
[[ -f "$COLLECT" ]] || { echo "missing $COLLECT" >&2; exit 2; }

work="$(mktemp -d "${TMPDIR:-/tmp}/coord-evidence-identity.XXXXXX")" || exit 2
trap 'rm -rf "$work"' EXIT

fails=0
fail() { echo "FAIL: $*" >&2; fails=$((fails + 1)); }
ok() { echo "PASS: $*"; }

sha="$(printf 'f%.0s' $(seq 1 64))"
other="$(printf 'a%.0s' $(seq 1 64))"
commit="$(git -C "$REPO_ROOT" rev-parse HEAD)"
tree="$(git -C "$REPO_ROOT" rev-parse HEAD^{tree})"

# 摘要器不在被测范围：把 lein/clojure 摘出 PATH，避免本地装了 lein 时白等
# 300s 的离线依赖解析超时（CI runner 上没有 lein，行为一致）。
run_collect() { # <out-dir> <store> <soak-log> [override-file]
    COORD_EVIDENCE_DIR="$1" SOAK_LOG="$3" COORD_IDENTITY_OVERRIDE="${4:-}" \
        PATH=/usr/bin:/bin bash "$COLLECT" identity-selfcheck "$2"
}

write_store() { # <store-dir> <node-sha>
    mkdir -p "$1"
    printf 'INFO node coord binary sha256 n1 %s\nINFO node coord binary sha256 n2 %s\n' \
        "$2" "$2" >"$1/jepsen.log"
    printf '{:valid? false}\n' >"$1/results.edn"
}

write_log() { # <log> [no-header]
    mkdir -p "$(dirname "$1")"
    if [[ "${2:-}" == "no-header" ]]; then
        printf 'WARNING: this log has no identity header\n' >"$1"
    else
        printf '== starting coord soak ==\n   control-binary: %s /opt/coord/jepsen/coord\n   control-commit: %s\n   control-tree:   %s\n   control-dirty:  0\n' \
            "$sha" "$commit" "$tree" >"$1"
    fi
}

manifest_of() { ls "$1"/*/MANIFEST.md 2>/dev/null | head -1; }
has_manifest() { [[ -n "$(manifest_of "$1")" ]]; }

# 1) 身份头 + 节点哈希一致 => MANIFEST
d="$work/green"
write_log "$d/soak.log"
write_store "$d/store" "$sha"
run_collect "$d/out" "$d/store" "$d/soak.log" >"$d/stdout" 2>&1
rc=$?
m="$(manifest_of "$d/out")"
if [[ $rc -eq 0 && -n "$m" ]] &&
    grep -qF "$sha" "$m" &&
    grep -qF '| 取证身份来源 | log |' "$m" &&
    grep -qE '\| coord 配置生成器哈希（16） \| `[0-9a-f]{16}`' "$m"; then
    ok "identity from the soak log yields a MANIFEST"
else
    fail "green case did not produce a log-sourced MANIFEST (rc=$rc): $(tail -3 "$d/stdout")"
fi

# 2) 节点哈希与控制机不一致 => 退出码 3，且不落半份归档
d="$work/mismatch"
write_log "$d/soak.log"
write_store "$d/store" "$other"
run_collect "$d/out" "$d/store" "$d/soak.log" >"$d/stdout" 2>&1
rc=$?
if [[ $rc -eq 3 ]] && ! has_manifest "$d/out"; then
    ok "a node/control hash mismatch is refused (exit 3, no MANIFEST)"
else
    fail "tampered node hash was not refused (rc=$rc, manifest=$(manifest_of "$d/out"))"
fi

# 3) 日志里没有身份头 => 退出码 3（采集器不能把「没记录」当通过）
d="$work/no-header"
write_log "$d/soak.log" no-header
write_store "$d/store" "$sha"
run_collect "$d/out" "$d/store" "$d/soak.log" >"$d/stdout" 2>&1
rc=$?
if [[ $rc -eq 3 ]] && ! has_manifest "$d/out"; then
    ok "a log without the identity header is refused (exit 3, no MANIFEST)"
else
    fail "a log without the identity header was accepted (rc=$rc, manifest=$(manifest_of "$d/out"))"
fi

# 4) 历史 store：显式 override 才允许，且 MANIFEST 必须标注来源
d="$work/override"
write_log "$d/soak.log" no-header
write_store "$d/store" "$sha"
printf 'commit: %s\ntree: %s\ncontrol-binary-sha256: %s\nnode-binary-sha256: %s\n' \
    "$commit" "$tree" "$sha" "$sha" >"$d/identity.txt"
run_collect "$d/out" "$d/store" "$d/soak.log" "$d/identity.txt" >"$d/stdout" 2>&1
rc=$?
m="$(manifest_of "$d/out")"
if [[ $rc -eq 0 && -n "$m" ]] &&
    grep -qF 'override(post-hoc)' "$m" &&
    grep -qF "$sha" "$m"; then
    ok "an explicit override yields a MANIFEST marked override(post-hoc)"
else
    fail "override case did not produce the marked MANIFEST (rc=$rc): $(tail -3 "$d/stdout")"
fi

if ((fails)); then
    echo "$fails evidence-identity check(s) FAILED" >&2
    exit 1
fi
echo "OK: evidence-identity pipeline fails closed in all directions"
