# coord-server 配置参考

`coord-server` 提供两个子命令：

| 子命令 | 用途 |
|--------|------|
| `dev` | 开发 / 测试模式（调试日志、自动 init/unseal） |
| `serve` | 生产模式（info 日志、严格安全约束） |

无子命令时默认等同于 `dev`。

---

## 一、命令行参数（`ServeArgs`）

所有参数既可通过 CLI 标志传入，也可通过对应环境变量设置，**环境变量优先级高于默认值，低于显式 CLI 标志**。

### 网络

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--grpc-addr` | `COORD_GRPC_ADDR` | `0.0.0.0:9090` | gRPC 监听地址（格式 `host:port`） |
| `--http-addr` | `COORD_HTTP_ADDR` | `0.0.0.0:9091` | HTTP 控制面监听地址 |

> **注意**：Docker 官方镜像中 HTTP 默认值改为 `0.0.0.0:8080`，通过 Compose 的 `environment` 传入。

### 存储

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--data-dir` | `COORD_DATA_DIR` | `/tmp/coord-dev` | 持久化数据目录（Raft 快照、node_id 文件、Redb 数据库） |

### 节点标识

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--node-id` | `COORD_NODE_ID` | 自动生成 UUID | 节点唯一标识；首次生成后持久化到 `<data_dir>/node_id` |

解析优先级：`--node-id` 标志 → `<data_dir>/node_id` 文件 → 生成新 UUID 并写入文件。

### 集群

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--peers` | `COORD_CLUSTER_PEERS` | `""` | 逗号分隔的对等节点地址（如 `coord-2:9090,coord-3:9090`） |
| `--bootstrap` | `COORD_BOOTSTRAP` | `""` | 是否作为初始 leader 自举；空值时：无 peers → `true`，有 peers → `false`；显式值：`true/1/yes/on` |

### 自动解封（Auto-Unseal）

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--auto-unseal-shares-file` | — | 无 | Shamir shares 文件路径，每行一个 share；服务器启动时自动提交 |

> **生产警告**：`serve` 模式下配置此选项时会打印 WARN 日志，提示操作者确认文件权限（建议 0400）。

### Dev 模式专用

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--dev-root-token` | `COORD_DEV_ROOT_TOKEN` | 无（随机生成） | dev 模式固定 root token；`serve` 模式下此参数被忽略 |

首次启动时自动执行：
1. 以 1-of-1 Shamir 初始化安全域，将指定 token 嵌入域中。
2. 将 unseal share 写入 `<data_dir>/dev-unseal.share`（权限 0600）。
3. 将 root token 写入 `<data_dir>/dev-root-token.txt`（权限 0600）。
4. 立即 unseal。

后续重启时读取 `dev-unseal.share` 自动 unseal，token 值不变。

### TLS

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--tls-cert` | `COORD_TLS_CERT` | 无 | PEM 服务端证书链 |
| `--tls-key` | `COORD_TLS_KEY` | 无 | PEM 服务端私钥 |
| `--tls-client-ca` | `COORD_TLS_CLIENT_CA` | 无 | PEM 客户端 CA（设置后强制 mTLS） |

`--tls-cert` 与 `--tls-key` 必须同时设置，任缺其一报错。TLS 同时作用于 gRPC 和 HTTP 控制面。

详细配置见 [TLS 指南](09-tls.md)。

### 可观测性

| 标志 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--otlp-endpoint` | `COORD_OTLP_ENDPOINT` | 无 | OTLP 收集器地址（如 `http://otel-collector:4317`）；W3C traceparent 传播始终有效 |

---

## 二、HTTP 控制面端点

所有端点均挂载在 `--http-addr` 上。

### 基础设施

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/healthz` | 存活探针，始终返回 `{"status":"ok"}` |
| GET | `/readyz` | 就绪探针，检查 Raft 状态和安全域 |
| GET | `/metrics` | Prometheus 指标 |
| GET | `/api/v1/role` | 当前节点 Raft 角色（leader / follower / candidate） |

### 集群

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/v1/overview` | 集群概览（节点、状态） |
| GET | `/api/v1/cluster/status` | Raft 集群详细状态 |
| POST | `/api/v1/cluster/member-add` | 添加成员 |
| POST | `/api/v1/cluster/member-remove` | 移除成员 |
| POST | `/api/v1/admin/backup/create` | 创建备份 |
| POST | `/api/v1/admin/backup/restore` | 恢复备份 |

### 业务 API（供前端/控制台调用）

| 路径前缀 | 说明 |
|---------|------|
| `/api/v1/services` | 服务注册 |
| `/api/v1/configs` | 配置中心 |
| `/api/v1/locks` | 分布式锁 |
| `/api/v1/transit/keys` | Transit 密钥 |
| `/api/v1/pki/*` | PKI 证书管理 |
| `/ui` | Web 控制台（内嵌静态资源） |

---

## 三、速率限制

HTTP 控制面对高风险端点（登录 / seal / backup / restore）应用令牌桶限流，
防止暴力破解。速率参数目前为内置常量，后续版本将暴露为可配置项。

gRPC 层同样有全局速率限制（基于 tower middleware），通过 `--grpc-addr` 的连接数量控制。

---

## 四、日志级别

通过标准 `RUST_LOG` 环境变量控制：

```bash
RUST_LOG=coord_server=debug,coord_core=info cargo run -- dev
```

dev 模式默认 `debug`，`serve` 模式默认 `info`。
