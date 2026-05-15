# ── Stage 1: Builder ──────────────────────────────────────────
# cargo registry + incremental target/ are stored in BuildKit cache volumes.
# These survive --no-cache and layer invalidation; only `docker builder prune`
# clears them. Result: true incremental builds on every code change.
FROM local/gitea-runner-toolchain:ubuntu-22.04 AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

RUN mkdir -p /root/.cargo && printf '\
[registries.byteforce]\n\
index = "sparse+https://nexus.byteforce.cn/repository/cargo-repo/"\n\
credential-provider = ["cargo:token"]\n' >> /root/.cargo/config.toml

WORKDIR /app
COPY .cargo /app/.cargo
COPY . .

RUN --mount=type=cache,id=coord-cargo-registry,target=/root/.cargo/registry \
    --mount=type=cache,id=coord-cargo-git,target=/root/.cargo/git \
    --mount=type=cache,id=coord-cargo-target,target=/app/target \
    --mount=type=secret,id=cargo_credentials,dst=/root/.cargo/credentials.toml \
    cargo build --release -p coord && \
    cp /app/target/release/coord /coord

# ── Stage 2: Runtime ──────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /coord /usr/local/bin/coord

EXPOSE 9090 8080

ENTRYPOINT ["coord", "server"]
