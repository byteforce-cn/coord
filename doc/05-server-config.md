# 服务端配置参考

`coord` 提供两个服务端子命令：

| 子命令 | 用途 |
|--------|------|
| `dev` | 开发 / 测试模式（调试日志、自动 init/unseal） |
| `server` | 生产模式（info 日志、严格安全约束） |

---

## 一、命令行参数

所有参数既可通过 CLI 标志传入，也可通过对应环境变量设置。**环境变量优先级高于默认值，低于显式 CLI 标志**。

### 网络

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--grpc-addr` | `COORD_GRPC_ADDR` | `0.0.0.0:9090` | gRPC 监听地址 |
| `--http-addr` | `COORD_HTTP_ADDR` | `0.0.0.0:9091` | HTTP 控制面监听地址 |

### 存储

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--data-dir` | `COORD_DATA_DIR` | `/tmp/coord-dev` | 数据目录（Raft log、snapshot、key store） |

### 集群

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--node-id` | `COORD_NODE_ID` | 自动 UUID | 节点唯一标识，重启须保持一致 |
| `--peers` | `COORD_CLUSTER_PEERS` | `""` | 逗号分隔的对等节点地址，如 `coord-2:9090,coord-3:9090` |
| `--bootstrap` | `COORD_BOOTSTRAP` | `""` | 非空时以 bootstrap leader 模式启动 |

### TLS

| 标志 | 环境变量 | 说明 |
|------|----------|------|
| `--tls-cert` | `COORD_TLS_CERT` | PEM 服务端证书路径 |
| `--tls-key` | `COORD_TLS_KEY` | PEM 服务端私钥路径 |
| `--tls-client-ca` | `COORD_TLS_CLIENT_CA` | PEM 客户端 CA（启用 mTLS） |

### 可观测性

| 标志 | 环境变量 | 说明 |
|------|----------|------|
| `--otlp-endpoint` | `COORD_OTLP_ENDPOINT` | OTLP gRPC 收集器地址（如 `http://otel:4317`） |

### Dev 模式专用

| 标志 | 环境变量 | 说明 |
|------|----------|------|
| `--dev-root-token` | `COORD_DEV_ROOT_TOKEN` | 启动时自动 init + unseal，使用固定 root token |
| `--auto-unseal-shares-file` | — | 从文件读取 unseal share（重启自动解封） |

---

## 二、HTTP 端点

| 路径 | 说明 |
|------|------|
| `GET /healthz` | 存活探针，返回 `{"status":"ok"}` |
| `GET /readyz` | 就绪探针（节点 leader 或 follower 均已 ready） |
| `GET /metrics` | Prometheus 指标（text format） |

---

## 三、最小生产配置示例

```yaml
# docker-compose.yml 片段
environment:
  COORD_NODE_ID: "node-1"
  COORD_GRPC_ADDR: "0.0.0.0:9090"
  COORD_HTTP_ADDR: "0.0.0.0:9091"
  COORD_DATA_DIR: "/data"
  COORD_CLUSTER_PEERS: "coord-2:9090,coord-3:9090"
  COORD_BOOTSTRAP: "true"
  COORD_TLS_CERT: "/certs/server.crt"
  COORD_TLS_KEY: "/certs/server.key"
  COORD_TLS_CLIENT_CA: "/certs/ca.crt"
  COORD_OTLP_ENDPOINT: "http://otel-collector:4317"
```

---

## 四、日志级别

通过 `RUST_LOG` 环境变量控制：

```bash
# dev 模式默认
RUST_LOG=debug

# 生产推荐
RUST_LOG=info

# 针对特定模块调试
RUST_LOG=coord=debug,coord_core::raft=trace
```
