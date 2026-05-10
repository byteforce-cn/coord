# syntax=docker/dockerfile:1.7
# ═══════════════════════════════════════════════════════════════════════════════
# coord — 多阶段构建镜像
#
# 构建命令（从仓库根目录执行，即 public/coord/）：
#
#   export CARGO_REGISTRIES_BYTEFORCE_TOKEN=<your-token>
#   docker build \
#     --secret id=cargo_token,env=CARGO_REGISTRIES_BYTEFORCE_TOKEN \
#     -f docker/Dockerfile \
#     -t registry.cn-hangzhou.aliyuncs.com/byteforce/coord:0.1.2 \
#     .
#
# 多平台构建（需 buildx）：
#   docker buildx build \
#     --secret id=cargo_token,env=CARGO_REGISTRIES_BYTEFORCE_TOKEN \
#     --platform linux/amd64,linux/arm64 \
#     -f docker/Dockerfile \
#     -t registry.cn-hangzhou.aliyuncs.com/byteforce/coord:0.1.2 \
#     --push .
# ═══════════════════════════════════════════════════════════════════════════════

# ── Stage 1: Base（Rust 1.93 + cargo-chef）────────────────────────────────────
FROM rust:1.93-slim AS chef

RUN apt-get update && apt-get install -y --no-install-recommends \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/* \
    && cargo install cargo-chef --locked

WORKDIR /app

# ── Stage 2: Planner — 仅解析 Cargo.toml / Cargo.lock，生成 recipe.json ────────
FROM chef AS planner
COPY . .
RUN cargo chef prepare --recipe-path recipe.json

# ── Stage 3: Builder — cook 依赖（重量级缓存层），再增量编译业务代码 ───────────
FROM chef AS builder

# 配置 cargo:token 凭据提供者（从 CARGO_REGISTRIES_BYTEFORCE_TOKEN 读取）
# 注意：~/.cargo/config.toml 与项目 .cargo/config.toml 合并生效
RUN mkdir -p ~/.cargo && \
    printf '[registry]\nglobal-credential-providers = ["cargo:token"]\n' \
    > ~/.cargo/config.toml

# 先 cook 依赖（Cargo.lock 不变则命中缓存，跳过所有依赖编译）
COPY --from=planner /app/recipe.json recipe.json
COPY .cargo/config.toml .cargo/config.toml
RUN --mount=type=secret,id=cargo_token \
    CARGO_REGISTRIES_BYTEFORCE_TOKEN=$(cat /run/secrets/cargo_token) \
    cargo chef cook --release --recipe-path recipe.json

# 再编译业务代码（依赖已缓存，仅重新编译源码）
COPY . .
RUN --mount=type=secret,id=cargo_token \
    CARGO_REGISTRIES_BYTEFORCE_TOKEN=$(cat /run/secrets/cargo_token) \
    cargo build --release --locked -p coord-server -p coord-ctl

# ── Stage 4: Runtime — 最小运行时镜像 ────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/coord-server /usr/local/bin/coord-server
COPY --from=builder /app/target/release/coord-ctl    /usr/local/bin/coord-ctl

EXPOSE 9090 8080

ENTRYPOINT ["coord-server"]
CMD ["serve"]
