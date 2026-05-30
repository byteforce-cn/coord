# Coord — 本地开发 / CI 自动化
# =============================================================================
#
# 本地开发（默认含 [patch] 指向 private/rust-libs）：
#   make dev      — 确保使用本地 Cargo.toml（含 [patch]）
#   make check    — cargo check
#   make test     — cargo test
#   make build    — cargo build --release
#
# CI / 发布（不含 [patch]，纯 registry 依赖）：
#   make ci       — 切回 Cargo.toml.ci
#
# 原理：
#   Cargo.toml         含 [patch] 覆盖，本地改 private/rust-libs 即时生效
#   Cargo.toml.ci      纯 registry 版本，CI 构建用
# =============================================================================

.PHONY: dev ci check test build clean

# --- 本地开发（默认）---
dev:
	@if grep -q '^\[patch' Cargo.toml 2>/dev/null; then \
		echo "  ✓ Cargo.toml already has [patch] overrides (local dev mode)"; \
	else \
		echo "  ⚠ Cargo.toml is in CI mode. Run 'make ci' to keep it, or:"; \
		echo "    cp Cargo.toml.ci Cargo.toml  # to stay on registry deps"; \
	fi

# --- CI 模式：切回纯 registry 依赖 ---
ci:
	@cp Cargo.toml.ci Cargo.toml
	@echo "  ✓ Switched to CI mode (registry dependencies only)"

# --- 常用命令 ---
check:
	cargo check --workspace

test:
	cargo test --workspace

build:
	cargo build --release

clean:
	cargo clean
