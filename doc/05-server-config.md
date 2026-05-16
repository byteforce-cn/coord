# 服务端配置参考

`coord` 提供以下入口点：

| 子命令 | 用途 |
|--------|------|
| `server` | 生产模式（info 日志、严格安全约束） |
| `dev` | 开发 / 测试模式（调试日志、自动 init/unseal） |
| `client` | gossip 代理 / sidecar 模式（显式配置 UDP gossip 地址、seed、advertise 地址） |
| `all` | 单进程同时启动 `dev` 服务端 + 内嵌 gossip agent（开发 / 单机） |
| `ctl` | 管理 CLI，连接运行中的 coord 实例 |

---

## 一、命令行参数

所有参数既可通过 CLI 标志传入，也可通过对应环境变量设置。**环境变量优先级高于默认值，低于显式 CLI 标志**。

### Server / Dev / All 共享的服务端参数

#### 网络

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--grpc-addr` | `COORD_GRPC_ADDR` | `0.0.0.0:9090` | gRPC 监听地址 |
| `--http-addr` | `COORD_HTTP_ADDR` | `0.0.0.0:9091` | HTTP 控制面监听地址 |

#### 存储

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--data-dir` | `COORD_DATA_DIR` | `/tmp/coord-dev` | 数据目录（Raft log、snapshot、key store） |

#### 集群

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--node-id` | `COORD_NODE_ID` | 自动 UUID | 节点唯一标识，重启须保持一致 |
| `--peers` | `COORD_CLUSTER_PEERS` | `""` | 逗号分隔的对等节点地址，如 `coord-2:9090,coord-3:9090` |
| `--bootstrap` | `COORD_BOOTSTRAP` | `""` | 非空时以 bootstrap leader 模式启动 |

#### TLS

| 标志 | 环境变量 | 说明 |
|------|----------|------|
| `--tls-cert` | `COORD_TLS_CERT` | PEM 服务端证书路径 |
| `--tls-key` | `COORD_TLS_KEY` | PEM 服务端私钥路径 |
| `--tls-client-ca` | `COORD_TLS_CLIENT_CA` | PEM 客户端 CA（启用 mTLS） |

#### 可观测性

| 标志 | 环境变量 | 说明 |
|------|----------|------|
| `--otlp-endpoint` | `COORD_OTLP_ENDPOINT` | OTLP gRPC 收集器地址（如 `http://otel:4317`） |

### 运行时环境变量（非 CLI 标志）

| 环境变量 | 默认值 | 说明 |
|----------|--------|------|
| `RUST_LOG` | `server/client=info`；`dev/all=debug` | `tracing-subscriber` 全局日志过滤器；环境变量优先级高于内建默认值 |

#### Dev 模式专用

| 标志 | 环境变量 | 说明 |
|------|----------|------|
| `--dev-root-token` | `COORD_DEV_ROOT_TOKEN` | 启动时自动 init + unseal，使用固定 root token |
| `--auto-unseal-shares-file` | — | 从文件读取 unseal share（重启自动解封） |

### Client 模式（gossip 代理）

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--node-id` | `COORD_CLIENT_NODE_ID` | 自动 UUID | gossip client 节点标识 |
| `--gossip-addr` | `COORD_CLIENT_GOSSIP_ADDR` | `0.0.0.0:7947` | Gossip UDP 监听地址 |
| `--gossip-advertise-addr` | `COORD_CLIENT_GOSSIP_ADVERTISE_ADDR` | 与 `gossip-addr` 相同 | 对外广播的 Gossip 地址，NAT / 端口映射场景建议显式设置 |
| `--local-grpc-addr` | `COORD_CLIENT_GRPC_ADDR` | `127.0.0.1:9090` | 本地服务接入该 proxy 的 gRPC 地址 |
| `--local-http-addr` | `COORD_CLIENT_HTTP_ADDR` | `127.0.0.1:9091` | 本地 HTTP / metrics 地址 |
| `--gossip-seeds` | `COORD_CLIENT_GOSSIP_SEEDS` | `[]` | 逗号分隔的 seed UDP 地址，如 `10.0.0.11:7947,10.0.0.12:7947` |
| `--cluster-id` | `COORD_CLIENT_CLUSTER_ID` | `coord-cluster` | Gossip cluster ID，集群内必须一致 |
| `--server-endpoints` | `COORD_CLIENT_SERVER_ENDPOINTS` | `[]` | 逗号分隔的 coord-server gRPC 端点，用于 CP 透传 |
| `--cache-ttl-seconds` | `COORD_CLIENT_CACHE_TTL_SECONDS` | `30` | AP 发现缓存 TTL |
| `--health-interval-seconds` | `COORD_CLIENT_HEALTH_INTERVAL_SECONDS` | `10` | 健康检查间隔 |
| `--tls-ca` | `COORD_CLIENT_TLS_CA` | — | 验证 server TLS 证书的 CA（PEM） |
| `--tls-cert` | `COORD_CLIENT_TLS_CERT` | — | mTLS 客户端证书（PEM） |
| `--tls-key` | `COORD_CLIENT_TLS_KEY` | — | mTLS 客户端私钥（PEM） |

### All 模式专用（内嵌 gossip）

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--gossip-port` | `COORD_CLIENT_GOSSIP_PORT` | `7947` | 内嵌 gossip agent 的 UDP 监听端口 |
| `--cluster-id` | `COORD_CLIENT_CLUSTER_ID` | `coord-cluster` | 内嵌 gossip agent 的 cluster ID |
| `--cache-ttl-seconds` | `COORD_CLIENT_CACHE_TTL_SECONDS` | `30` | 服务发现缓存 TTL |

> `coord all` 的服务端参数完全复用上面的 Server / Dev / All 共享参数。
>
> 当前 `coord all` 会把 gossip 监听地址固定构造成 `0.0.0.0:<gossip-port>`，且不会单独接收 `gossip-advertise-addr` 或 `gossip-seeds`。它启动后只会加入自身，适合开发 / 单机；多机 gossip 组网请使用 `coord client`。

---

## 二、HTTP 端点

| 路径 | 说明 |
|------|------|
| `GET /healthz` | 存活探针，返回 `{"status":"ok"}` |
| `GET /readyz` | 就绪探针（节点 leader 或 follower 均已 ready） |
| `GET /metrics` | Prometheus 指标（text format） |

---

## 三、Gossip / All 模式最小示例

### `coord client`

```bash
coord client \
  --gossip-addr 0.0.0.0:7947 \
  --gossip-advertise-addr 10.0.0.12:7947 \
  --gossip-seeds 10.0.0.11:7947,10.0.0.13:7947 \
  --server-endpoints 10.0.0.21:9090,10.0.0.22:9090
```

### `coord all`

```bash
# 单机长期运行时，请显式设置持久化数据目录与日志级别
COORD_DATA_DIR=/var/lib/coord \
COORD_NODE_ID=coord-all-node-1 \
RUST_LOG=info \
coord all

# 修改内嵌 gossip agent 端口
COORD_CLIENT_GOSSIP_PORT=8947 coord all
```

> `coord all` 复用 `coord dev` 的服务端参数，因此默认数据目录仍是 `/tmp/coord-dev`。如果是容器单机部署，请把持久卷挂到例如 `/data`，并同时设置 `COORD_DATA_DIR=/data`；否则容器重建后状态会丢失。

Docker / Compose 使用 `coord all` 时，除 `9090` / `9091` 外还需映射 UDP 端口，例如：

```yaml
services:
  coord:
    command: ["all"]
    ports:
      - "9090:9090"
      - "9091:9091"
      - "7947:7947/udp"
    environment:
      COORD_DATA_DIR: "/data"
      RUST_LOG: "info"
      COORD_CLIENT_GOSSIP_PORT: "7947"
      COORD_CLIENT_CLUSTER_ID: "coord-cluster"
```

---

## 四、最小生产配置示例

```yaml
# docker-compose.yml 片段
environment:
  COORD_NODE_ID: "node-1"
  COORD_GRPC_ADDR: "0.0.0.0:9090"
  COORD_HTTP_ADDR: "0.0.0.0:9091"
  COORD_DATA_DIR: "/data"
  RUST_LOG: "info"
  COORD_CLUSTER_PEERS: "coord-2:9090,coord-3:9090"
  COORD_BOOTSTRAP: "true"
  COORD_TLS_CERT: "/certs/server.crt"
  COORD_TLS_KEY: "/certs/server.key"
  COORD_TLS_CLIENT_CA: "/certs/ca.crt"
  COORD_OTLP_ENDPOINT: "http://otel-collector:4317"
logging:
  driver: json-file
  options:
    max-size: "10m"
    max-file: "3"
```

---

## 五、日志级别

通过 `RUST_LOG` 环境变量控制：

- `server` / `client` 默认 `info`
- `dev` / `all` 默认 `debug`
- 单机长期运行 `coord all` 时，建议显式设置 `RUST_LOG=info`

```bash
# dev 模式默认
RUST_LOG=debug

# 生产推荐
RUST_LOG=info

# 针对特定模块调试
RUST_LOG=coord=debug,coord_core::raft=trace
```

> 周期性 `persisted runtime snapshot to redb` 日志为 `debug` 级别；生产推荐的 `RUST_LOG=info` 默认不会输出这类高频快照日志。
