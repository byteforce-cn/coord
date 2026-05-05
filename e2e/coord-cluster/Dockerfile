# ── Stage 1: Base image with toolchain + cargo-chef ──────────
FROM rust:1.88-slim AS chef

RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-chef --locked

WORKDIR /app

# ── Stage 2: Planner — generate recipe.json from manifests ───
# Only re-runs when Cargo.toml / Cargo.lock changes.
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: Builder — cook deps (cached), then compile ───────
FROM chef AS builder

# 1. Restore dependency layers from recipe (heavy, but CACHED)
COPY --from=planner /app/recipe.json recipe.json
RUN cargo chef cook --release --recipe-path recipe.json

# 2. Copy source and build (fast incremental after deps are cached)
COPY . .
RUN cargo build --release -p coord-server -p coord-ctl

# ── Stage 4: Runtime ──────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/coord-server /usr/local/bin/coord-server
COPY --from=builder /app/target/release/coord-ctl    /usr/local/bin/coord-ctl

EXPOSE 9090 8080

ENTRYPOINT ["coord-server", "serve"]
