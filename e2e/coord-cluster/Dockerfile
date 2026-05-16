# syntax=docker/dockerfile:1.7
# ── coord-cluster/Dockerfile ─────────────────────────────────────────────────
# E2E 集群节点。与 docker/Dockerfile 对齐（rust:1.93-slim 基础镜像）。
# BuildKit cache volume 保留跨构建的 cargo registry/target，实现本地增量编译。
#
# 使用方式（build context = public/coord/）：
#   docker build \
#     --secret id=cargo_token,env=CARGO_REGISTRIES_BYTEFORCE_TOKEN \
#     -f e2e/coord-cluster/Dockerfile \
#     -t coord-e2e:local .
# ─────────────────────────────────────────────────────────────────────────────

# ── Stage 1: Builder ──────────────────────────────────────────────────────────
FROM docker.1ms.run/rust:1.93-slim AS builder

# 替换 Debian 源为国内镜像
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources \
    && sed -i 's/security.debian.org/mirrors.aliyun.com\/debian-security/g' /etc/apt/sources.list.d/debian.sources

RUN apt-get update && apt-get install -y --no-install-recommends \
    git \
        protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
# 项目级 cargo 配置（含私有 registry index，不含凭据）
COPY .cargo /app/.cargo
COPY . .

# cargo registry + incremental target/ 存入 BuildKit cache volume。
# --no-cache 和 layer 失效均不清除它们；只有 `docker builder prune` 才会清除。
# CARGO_REGISTRIES_BYTEFORCE_TOKEN 通过 secret 注入为 inline env var，
# 不写文件、不进 layer，RUN 结束后自动消失。
RUN --mount=type=cache,id=coord-cargo-registry,target=/root/.cargo/registry \
    --mount=type=cache,id=coord-cargo-git,target=/root/.cargo/git \
    --mount=type=cache,id=coord-cargo-target,target=/app/target \
    --mount=type=secret,id=cargo_token \
    CARGO_REGISTRIES_BYTEFORCE_TOKEN=$(cat /run/secrets/cargo_token) \
    cargo build --release --locked -p coord && \
    cp /app/target/release/coord /coord

# ── Stage 2: Runtime ──────────────────────────────────────────────────────────
FROM docker.1ms.run/debian:trixie-slim AS runtime

# 替换 Debian 源为国内镜像
RUN sed -i 's/deb.debian.org/mirrors.aliyun.com/g' /etc/apt/sources.list.d/debian.sources \
    && sed -i 's/security.debian.org/mirrors.aliyun.com\/debian-security/g' /etc/apt/sources.list.d/debian.sources

RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /coord /usr/local/bin/coord

EXPOSE 9090 8080

ENTRYPOINT ["coord"]
CMD ["server"]
