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

# 项目级 cargo 配置（仅含 registry index，不含凭据）
COPY .cargo/config.toml .cargo/config.toml

# 先 cook 依赖（Cargo.lock 不变则命中缓存，跳过所有依赖编译）
# 凭据写入 credentials.toml 供 cargo:token provider 读取，
# 用完即删——BuildKit 单条 RUN 的最终文件系统才提交为 layer，不会留下 token。
COPY --from=planner /app/recipe.json recipe.json
RUN --mount=type=secret,id=cargo_token \
    mkdir -p ~/.cargo && \
    printf '[registries.byteforce]\ntoken = "%s"\n' "$(cat /run/secrets/cargo_token)" \
    > ~/.cargo/credentials.toml && \
    cargo chef cook --release --recipe-path recipe.json && \
    rm ~/.cargo/credentials.toml

# 再编译业务代码（依赖已缓存，仅重新编译源码）
COPY . .
RUN --mount=type=secret,id=cargo_token \
    mkdir -p ~/.cargo && \
    printf '[registries.byteforce]\ntoken = "%s"\n' "$(cat /run/secrets/cargo_token)" \
    > ~/.cargo/credentials.toml && \
    cargo build --release --locked -p coord && \
    rm ~/.cargo/credentials.toml

# ── Stage 4: Runtime — 最小运行时镜像 ────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/coord /usr/local/bin/coord

EXPOSE 9090 9091

ENTRYPOINT ["coord"]
CMD ["server"]
