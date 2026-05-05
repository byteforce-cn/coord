#!/usr/bin/env bash
# Engineering guardrails — run in CI to prevent tech debt backflow.
#
# Usage:
#   ./scripts/ci-guardrails.sh
#
# Exit codes:
#   0 — all checks passed
#   1 — one or more guardrails tripped
set -euo pipefail

FAIL=0
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

# ── 1. Large file check ──────────────────────────────────────────────────────
# Flag files > 500 lines in high-risk source directories.
# Threshold is intentionally generous; tighten as codebase matures.

MAX_LINES=500
LARGE_FILES=()

while IFS= read -r f; do
    lines=$(wc -l < "$f")
    if (( lines > MAX_LINES )); then
        LARGE_FILES+=("$f ($lines lines)")
    fi
done < <(find crates/coord-server/src crates/coord-ctl/src -name '*.rs' -not -name '*.generated.*')

if (( ${#LARGE_FILES[@]} > 0 )); then
    echo -e "${YELLOW}[WARN] Large source files (>${MAX_LINES} lines):${NC}"
    for entry in "${LARGE_FILES[@]}"; do
        echo "  - $entry"
    done
    # Warning only — does not fail the build yet.
    # Uncomment the next line to make this a hard gate:
    # FAIL=1
fi

# ── 2. New #[deprecated] / #[allow(deprecated)] audit ────────────────────────
# Count deprecated annotations in high-risk directories.
# If the count exceeds the baseline, the build fails.

DEPRECATED_BASELINE=15  # Current known count; decrease over time.

DEPRECATED_COUNT=$(grep -r '#\[deprecated' crates/coord-server/src crates/coord-ctl/src 2>/dev/null | wc -l | tr -d ' ') || DEPRECATED_COUNT=0
ALLOW_DEPRECATED_COUNT=$(grep -r '#\[allow(deprecated)\]' crates/coord-server/src crates/coord-ctl/src 2>/dev/null | wc -l | tr -d ' ') || ALLOW_DEPRECATED_COUNT=0

TOTAL_DEPRECATED=$((DEPRECATED_COUNT + ALLOW_DEPRECATED_COUNT))

if (( TOTAL_DEPRECATED > DEPRECATED_BASELINE )); then
    echo -e "${RED}[FAIL] Deprecated annotation count ($TOTAL_DEPRECATED) exceeds baseline ($DEPRECATED_BASELINE).${NC}"
    echo "       Review new #[deprecated] / #[allow(deprecated)] additions."
    FAIL=1
else
    echo -e "${GREEN}[OK] Deprecated annotations: $TOTAL_DEPRECATED (baseline: $DEPRECATED_BASELINE)${NC}"
fi

# ── 3. Clippy deny gate ──────────────────────────────────────────────────────
# Ensure clippy passes with deny-level for core and server crates.

echo "Running clippy check..."
if cargo clippy -p coord-server -p coord-ctl --all-targets -- -D warnings 2>&1; then
    echo -e "${GREEN}[OK] Clippy clean${NC}"
else
    echo -e "${RED}[FAIL] Clippy found warnings/errors${NC}"
    FAIL=1
fi

# ── 4. Test coverage gate ─────────────────────────────────────────────────────
# Ensure key test files exist for modules that have business logic.

REQUIRED_TESTS=(
    "crates/coord-server/tests/backup_restore.rs"
    "crates/coord-server/tests/seal_unseal.rs"
    "crates/coord-server/tests/watch_keepalive.rs"
    "crates/coord-server/tests/error_contract.rs"
)

MISSING_TESTS=()
for test_file in "${REQUIRED_TESTS[@]}"; do
    if [[ ! -f "$test_file" ]]; then
        MISSING_TESTS+=("$test_file")
    fi
done

if (( ${#MISSING_TESTS[@]} > 0 )); then
    echo -e "${RED}[FAIL] Missing required test files:${NC}"
    for entry in "${MISSING_TESTS[@]}"; do
        echo "  - $entry"
    done
    FAIL=1
else
    echo -e "${GREEN}[OK] All required test files present${NC}"
fi

# ── 5. No new unwrap/expect in production code ───────────────────────────────
# The workspace lint config denies these, but double-check as a safety net.

UNWRAP_COUNT=$(grep -rn '\.unwrap()' crates/coord-server/src crates/coord-ctl/src \
    --include='*.rs' \
    2>/dev/null | grep -v '#\[cfg(test)\]' | grep -v 'mod tests' | grep -v '// SAFETY:' | wc -l | tr -d ' ') || UNWRAP_COUNT=0

if (( UNWRAP_COUNT > 0 )); then
    echo -e "${YELLOW}[WARN] Found $UNWRAP_COUNT .unwrap() calls in production code paths.${NC}"
    echo "       These should be caught by clippy deny rules."
fi

# ── Summary ───────────────────────────────────────────────────────────────────

if (( FAIL )); then
    echo -e "\n${RED}Guardrail checks FAILED${NC}"
    exit 1
else
    echo -e "\n${GREEN}All guardrail checks passed${NC}"
    exit 0
fi
