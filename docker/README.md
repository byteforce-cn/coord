# coord Docker 使用指南

本目录包含构建 coord 容器镜像及搭建运行环境所需的全部文件。

| 文件 | 说明 |
|------|------|
| `Dockerfile` | 多阶段构建文件，产出包含 `coord-server` 和 `coord-ctl` 的最小运行时镜像 |
| `docker-compose.dev.yml` | 单节点开发环境（dev 模式，调试日志） |
| `docker-compose.cluster.yml` | 三节点 Raft 集群环境 |

---

## 前提条件

- Docker 26+ 且 BuildKit 已启用（Docker 23+ 默认启用）
- 访问私有 Cargo registry 的凭据（`CARGO_REGISTRIES_BYTEFORCE_TOKEN`）

---

## 一、构建镜像

所有命令均在仓库根目录（`public/coord/`）下执行。

### 1.1 基本构建

```bash
export CARGO_REGISTRIES_BYTEFORCE_TOKEN=<your-token>

docker build \
  --secret id=cargo_token,env=CARGO_REGISTRIES_BYTEFORCE_TOKEN \
  -f docker/Dockerfile \
  -t registry.cn-hangzhou.aliyuncs.com/byteforce/coord:0.1.2 \
  .
```

同时打 `latest` 标签：

```bash
docker tag registry.cn-hangzhou.aliyuncs.com/byteforce/coord:0.1.2 \
           registry.cn-hangzhou.aliyuncs.com/byteforce/coord:latest
```

### 1.2 多平台构建并推送（需 buildx）

```bash
docker buildx build \
  --secret id=cargo_token,env=CARGO_REGISTRIES_BYTEFORCE_TOKEN \
  --platform linux/amd64,linux/arm64 \
  -f docker/Dockerfile \
  -t registry.cn-hangzhou.aliyuncs.com/byteforce/coord:0.1.2 \
  -t registry.cn-hangzhou.aliyuncs.com/byteforce/coord:latest \
  --push \
  .
```

> **缓存说明**：Dockerfile 使用 `cargo-chef` 分层缓存依赖。首次构建耗时较长（需要编译所有依赖），后续只要 `Cargo.lock` 不变，依赖层直接命中缓存，仅重新编译业务代码。

---

## 二、单节点开发环境

适合本地开发调试，启用 `dev` 模式（调试级日志）。

### 启动

```bash
docker compose -f docker/docker-compose.dev.yml up -d
```

指定镜像版本：

```bash
COORD_VERSION=0.1.2 docker compose -f docker/docker-compose.dev.yml up -d
```

### 端口

| 端口 | 协议 | 说明 |
|------|------|------|
| `9090` | gRPC | 客户端 / SDK 接入 |
| `8080` | HTTP | 健康检查（`/healthz`）、指标 |

### 验证

```bash
curl http://localhost:8080/healthz
```

### 停止并清理数据

```bash
docker compose -f docker/docker-compose.dev.yml down -v
```

---

## 三、三节点 Raft 集群环境

生产拓扑参考，coord-1 作为初始 bootstrap leader，coord-2 / coord-3 被自动加入集群。

### 启动

```bash
docker compose -f docker/docker-compose.cluster.yml up -d
```

指定镜像版本：

```bash
COORD_VERSION=0.1.2 docker compose -f docker/docker-compose.cluster.yml up -d
```

### 节点端口映射

| 节点 | gRPC（宿主机） | HTTP（宿主机） |
|------|---------------|---------------|
| coord-1 | `9090` | `8080` |
| coord-2 | `19090` | `18080` |
| coord-3 | `29090` | `28080` |

### 集群组建流程

1. **coord-1** 启动，以 `COORD_BOOTSTRAP=true` 自举为单节点 Raft leader。
2. **coord-2 / coord-3** 启动（`depends_on: coord-1: condition: service_healthy`）。
3. **coord-1** 探测 `coord-2:9090` 和 `coord-3:9090`，通过 auto-join 将它们加入集群。
4. 三节点 quorum 建立完成，集群进入正常服务状态。

### 验证集群健康

```bash
# 检查各节点健康状态
curl http://localhost:8080/healthz   # coord-1
curl http://localhost:18080/healthz  # coord-2
curl http://localhost:28080/healthz  # coord-3
```

### 停止并清理数据

```bash
docker compose -f docker/docker-compose.cluster.yml down -v
```

---

## 四、使用 coord-ctl

`coord-ctl` 已内置到镜像中，可通过 `docker exec` 调用：

```bash
# 单节点环境
docker exec coord-dev coord-ctl --help

# 集群环境（任意节点）
docker exec coord-1 coord-ctl --help
```

---

## 五、环境变量参考

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `COORD_GRPC_ADDR` | `0.0.0.0:9090` | gRPC 监听地址 |
| `COORD_HTTP_ADDR` | `0.0.0.0:8080` | HTTP 监听地址 |
| `COORD_DATA_DIR` | `/tmp/coord-dev` | 数据持久化目录 |
| `COORD_NODE_ID` | 自动生成 | 节点唯一标识 |
| `COORD_CLUSTER_PEERS` | 空 | 对等节点列表，格式：`host:port,...` |
| `COORD_BOOTSTRAP` | 空（无 peers 时自动 true） | 是否作为初始 leader 自举 |
| `COORD_TLS_CERT` | 无 | TLS 证书路径（PEM） |
| `COORD_TLS_KEY` | 无 | TLS 私钥路径（PEM） |
| `COORD_TLS_CLIENT_CA` | 无 | mTLS 客户端 CA 路径（PEM） |
| `COORD_OTLP_ENDPOINT` | 无 | OTLP 收集器地址 |
