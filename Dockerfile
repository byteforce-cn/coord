# ──── Stage 1: Build coord-ui frontend ────
FROM node:22-bookworm AS ui-builder

# 使用阿里云 Debian 镜像加速 apt
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources

WORKDIR /app/coord-ui

# 使用 npmmirror 中国镜像加速 pnpm install
RUN corepack enable pnpm && pnpm config set registry https://registry.npmmirror.com

COPY coord-ui/package.json coord-ui/pnpm-lock.yaml ./
RUN pnpm install --frozen-lockfile --ignore-scripts
COPY coord-ui/ ./
RUN pnpm build

# ──── Stage 2: Build coord Rust binary ────
FROM rust:1.98.1-bookworm AS builder

# 设置 Rust 工具链路径（跳过 rustup 网络同步）
ENV RUSTUP_TOOLCHAIN=1.98.1 \
    PATH=/usr/local/rustup/toolchains/1.98.1-x86_64-unknown-linux-gnu/bin:/usr/local/cargo/bin:$PATH

# 使用阿里云 Debian 镜像加速 apt
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources

# 安装编译依赖和 mold 链接器（mold 大幅缩减链接时间）
RUN apt-get update && apt-get install -y --no-install-recommends \
    protobuf-compiler \
    mold \
    && rm -rf /var/lib/apt/lists/*

# 使用 SJTU 镜像加速 crate 下载（冷启动必备）
ENV CARGO_NET_RETRY=5 \
    CARGO_HTTP_TIMEOUT=120
RUN mkdir -p /usr/local/cargo/config.d && \
    echo '[source.crates-io]\nreplace-with = "sjtu"\n\n[source.sjtu]\nregistry = "sparse+https://mirrors.sjtug.sjtu.edu.cn/crates.io-index/"' \
    > /usr/local/cargo/config.d/sjtu-mirror.toml

# 启用 mold 作为链接器
ENV RUSTFLAGS="-C link-arg=-fuse-ld=mold"

WORKDIR /app

# 复制全部源码和 UI 产物
COPY Cargo.toml Cargo.lock rust-toolchain.toml ./
COPY coord/ coord/
COPY coord-proto/ coord-proto/
COPY coord-core/ coord-core/
COPY coord-macros/ coord-macros/
COPY coord-server/ coord-server/
COPY coord-client/ coord-client/
COPY coord-agent/ coord-agent/
COPY coord-test/ coord-test/
COPY --from=ui-builder /app/coord-ui/dist/ coord-ui/dist/

# 单步编译（cache mount 跨构建缓存 registry 和 target）
# 编译后必须 cp 出 cache mount，否则 COPY --from 拿不到二进制
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/app/target \
    cargo build --release --package coord && \
    cp /app/target/release/coord /app/coord-bin

# ──── Stage 3: Runtime ────
FROM debian:bookworm-slim

# 使用阿里云 Debian 镜像加速 apt
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources && \
    apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Create non-root user
RUN useradd --system --uid 1000 --create-home coord

# Data directory
RUN mkdir -p /var/lib/coord && chown coord:coord /var/lib/coord

COPY --from=builder /app/coord-bin /usr/local/bin/coord

USER coord
WORKDIR /var/lib/coord

# ──── Ports ────
# 50051: gRPC API
# 50052: Raft internal
# 50061: HTTP health/BFF（server 模式：grpc 端口 + 10）
# 19527: Agent gRPC
# 19528: HTTP (health / BFF / UI)
EXPOSE 50051 50052 50061 19527 19528

VOLUME ["/var/lib/coord"]

# P0-G.1：默认启动 server 模式（鉴权默认开启）；不再使用 dev --fresh（会清空数据卷）。
# root 密码经 COORD_ROOT_PASSWORD 注入，否则随机生成并仅打印一次。
ENTRYPOINT ["/usr/local/bin/coord"]
CMD ["server", "--addr", "0.0.0.0:50051", "--raft-addr", "0.0.0.0:50052", "--bootstrap", "--data-dir", "/var/lib/coord"]

# 健康检查（P1-09 前以 HTTP /healthz 为准；server 模式 HTTP 端口 = grpc + 10）
HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:50061/healthz || exit 1