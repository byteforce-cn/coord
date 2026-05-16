# 单节点 Docker 部署

适用于本地开发、功能验证以及小规模内部服务场景。本文所有镜像示例均使用 `0.1.15`，与工作区 `Cargo.toml` 中的 `[workspace.package].version` 保持一致。

如果你只有一台服务器但希望容器重启后仍保留状态，推荐使用第四节的 `coord all` 示例，并显式挂载 volume + 设置 `COORD_DATA_DIR=/data`。`coord dev` 与 `coord all` 默认仍写入 `/tmp/coord-dev`，不适合依赖容器临时目录保存状态。

---

## 一、最简启动

```bash
docker run -d --name coord-dev \
  --restart unless-stopped \
  --log-opt max-size=10m \
  --log-opt max-file=3 \
  -p 9090:9090 \
  -p 9091:9091 \
  -v coord-data:/data \
  -e COORD_NODE_ID=coord-dev-node-1 \
  -e COORD_DATA_DIR=/data \
  nexus.byteforce.cn/image-private/coord:0.1.15 \
  dev
```

| 端口 | 协议 | 用途 |
|------|------|------|
| `9090` | gRPC | SDK / `coord ctl` 接入 |
| `9091` | HTTP | `/healthz` `/readyz` `/metrics` |

验证健康：

```bash
docker ps --filter name=coord-dev
curl http://localhost:9091/healthz
# → {"status":"ok"}
```

> 挂载了 `/data` volume，就应同时设置 `COORD_DATA_DIR=/data`；否则数据仍会写入容器内默认目录 `/tmp/coord-dev`，volume 无法承载实际状态。

---

## 二、使用 Docker Compose（推荐）

创建以下 `docker-compose.yml`：

```bash
cat > docker-compose.yml <<'YAML'
name: coord-single

services:
  coord:
    image: nexus.byteforce.cn/image-private/coord:${COORD_VERSION:-0.1.15}
    container_name: coord-dev
    command: ["dev"]
    restart: unless-stopped
    ports:
      - "9090:9090"
      - "9091:9091"
    environment:
      COORD_NODE_ID: "coord-dev-node-1"
      COORD_GRPC_ADDR: "0.0.0.0:9090"
      COORD_HTTP_ADDR: "0.0.0.0:9091"
      COORD_DATA_DIR: "/data"
      # 测试 / CI 需要固定 root token 时再取消注释
      # COORD_DEV_ROOT_TOKEN: "s.my-test-token"
    volumes:
      - coord-data:/data
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9091/healthz"]
      interval: 5s
      timeout: 5s
      retries: 12
      start_period: 10s

volumes:
  coord-data:
YAML

docker compose up -d
docker compose ps
```

上述示例同时启用了 Docker `json-file` 日志轮转，单容器默认上限为 `10m x 3`。

> 上面的 Compose 示例仅适用于 `command: ["dev"]` 的单节点模式，不包含 gossip UDP 端口。如果你要运行 `coord all`，不能只改 `command`，还需要同时补充 `7947/udp` 端口映射和 gossip 相关环境变量，见下一节的完整 Compose 示例。

指定镜像版本（默认已对齐 `0.1.15`）：

```bash
COORD_VERSION=0.1.15 docker compose up -d
```

停止容器但保留数据：

```bash
docker compose down
```

停止并销毁数据卷：

```bash
docker compose down -v
```

---

## 三、Dev Root Token（测试 / CI 推荐）<a id="dev-root-token"></a>

`dev` 模式支持通过环境变量在启动时自动完成安全域初始化 + 解封，并使用固定 root token：

```bash
docker run -d --name coord-dev \
  --restart unless-stopped \
  --log-opt max-size=10m \
  --log-opt max-file=3 \
  -p 9090:9090 \
  -p 9091:9091 \
  -v coord-data:/data \
  -e COORD_NODE_ID=coord-dev-node-1 \
  -e COORD_DATA_DIR=/data \
  -e COORD_DEV_ROOT_TOKEN=s.my-test-token \
  nexus.byteforce.cn/image-private/coord:0.1.15 \
  dev
```

> - **首次启动**：自动 init（1-of-1 Shamir）+ unseal，root token 嵌入域中。
> - **重启后**：读取 `/data/dev-unseal.share` 自动重新 unseal，token 保持不变。
> - 使用 Compose 时，直接取消上一节 `COORD_DEV_ROOT_TOKEN` 注释即可。
> - `server` 模式下此参数被忽略，不影响生产行为。

---

## 四、单进程 Server + Client（`coord all`，可选）

`coord all` 会在一个进程内同时启动：

- 与 `coord dev` 等价的 CP 服务端
- 一个内嵌 gossip agent

```bash
docker run -d --name coord-all \
  --restart unless-stopped \
  --log-opt max-size=10m \
  --log-opt max-file=3 \
  -p 9090:9090 \
  -p 9091:9091 \
  -p 7947:7947/udp \
  -v coord-data:/data \
  -e COORD_NODE_ID=coord-all-node-1 \
  -e COORD_DATA_DIR=/data \
  -e RUST_LOG=info \
  -e COORD_CLIENT_GOSSIP_PORT=7947 \
  -e COORD_CLIENT_CLUSTER_ID=coord-cluster \
  nexus.byteforce.cn/image-private/coord:0.1.15 \
  all
```

| 端口 | 协议 | 用途 |
|------|------|------|
| `9090` | gRPC | SDK / `coord ctl` 接入 |
| `9091` | HTTP | `/healthz` `/readyz` `/metrics` |
| `7947` | UDP | gossip 成员发现 / membership |

如需使用 Docker Compose 运行 `coord all`，建议直接使用以下完整示例：

```bash
cat > docker-compose.yml <<'YAML'
name: coord-single

services:
  coord:
    image: nexus.byteforce.cn/image-private/coord:${COORD_VERSION:-0.1.15}
    container_name: coord-all
    command: ["all"]
    restart: unless-stopped
    ports:
      - "9090:9090"
      - "9091:9091"
      - "7947:7947/udp"
    environment:
      COORD_NODE_ID: "coord-all-node-1"
      COORD_GRPC_ADDR: "0.0.0.0:9090"
      COORD_HTTP_ADDR: "0.0.0.0:9091"
      COORD_DATA_DIR: "/data"
      RUST_LOG: "info"
      COORD_CLIENT_GOSSIP_PORT: "7947"
      COORD_CLIENT_CLUSTER_ID: "coord-cluster"
      # 测试 / CI 需要固定 root token 时再取消注释
      # COORD_DEV_ROOT_TOKEN: "s.my-test-token"
    volumes:
      - coord-data:/data
    logging:
      driver: json-file
      options:
        max-size: "10m"
        max-file: "3"
    healthcheck:
      test: ["CMD", "curl", "-f", "http://localhost:9091/healthz"]
      interval: 5s
      timeout: 5s
      retries: 12
      start_period: 10s

volumes:
  coord-data:
YAML

docker compose up -d
docker compose ps
```

> - `coord all` 的内嵌 gossip agent 固定监听 `0.0.0.0:<COORD_CLIENT_GOSSIP_PORT>/udp`。
> - 默认 UDP 端口为 `7947`；同机运行多个实例时必须显式改成不同端口。
> - `coord all` 复用 `coord dev` 的服务端默认值，未显式设置 `COORD_DATA_DIR` 时仍会写入 `/tmp/coord-dev`；如果容器只依赖临时文件系统，重建后状态会丢失。
> - `coord all` 默认日志级别也是 `debug`；单机长期运行建议显式设置 `RUST_LOG=info`，避免把调试级别日志长期写入容器日志文件。
> - 当前 `all` 模式不支持单独配置 gossip seeds 或 advertise 地址，启动后只会加入自身，适合开发 / 单机。
> - 需要多机 gossip 组网、显式 seed 或 advertise 地址时，请使用 `coord client`。完整参数见 [服务端配置参考](05-server-config.md)。

---

## 五、常用环境变量

| 变量 | 二进制默认值 | 单节点部署推荐值 | 说明 |
|------|--------------|------------------|------|
| `COORD_GRPC_ADDR` | `0.0.0.0:9090` | `0.0.0.0:9090` | gRPC 监听地址 |
| `COORD_HTTP_ADDR` | `0.0.0.0:9091` | `0.0.0.0:9091` | HTTP 控制面监听地址 |
| `COORD_DATA_DIR` | `/tmp/coord-dev` | `/data` | 数据目录；挂载 volume 时应显式对齐 |
| `COORD_NODE_ID` | 自动生成 UUID | `coord-dev-node-1` | 节点唯一标识；重启与重建容器时应保持稳定 |
| `RUST_LOG` | `server/client=info`；`dev/all=debug` | `info` | 全局日志级别；长期运行的单机容器建议显式设置为 `info` |
| `COORD_DEV_ROOT_TOKEN` | — | 按需设置 | dev 模式固定 root token |
| `COORD_CLIENT_GOSSIP_PORT` | `7947` | `7947` | `coord all` 内嵌 gossip agent 的 UDP 监听端口 |
| `COORD_CLIENT_CLUSTER_ID` | `coord-cluster` | 按需设置 | `coord all` / `coord client` 的 gossip cluster id |
| `COORD_TLS_CERT` | — | 按需设置 | TLS 证书路径（PEM） |
| `COORD_TLS_KEY` | — | 按需设置 | TLS 私钥路径（PEM） |
| `COORD_TLS_CLIENT_CA` | — | 按需设置 | mTLS 客户端 CA（PEM） |
| `COORD_OTLP_ENDPOINT` | — | 按需设置 | OTLP 收集器地址（如 `http://otel:4317`） |

完整参数见 [服务端配置参考](05-server-config.md)。

---

## 六、手动安全域初始化

每次容器重新创建且未设置 `COORD_DEV_ROOT_TOKEN`，或主动清空数据卷后，都需执行一次初始化：

```bash
# 步骤 1：初始化（示例：3 share，threshold=2）
docker exec coord-dev coord ctl operator init --shares-total 3 --threshold 2
# 输出：
#   initialized: true
#   unseal_shares:
#   shamir:AAAA...
#   shamir:BBBB...
#   shamir:CCCC...
#   root_token: s.xxxxxxxxxxxxxxxx

# 步骤 2：提交 threshold 份额解封
docker exec coord-dev coord ctl operator unseal shamir:AAAA...
docker exec coord-dev coord ctl operator unseal shamir:BBBB...
# → sealed: false
```

> ⚠️ **请妥善保存 unseal shares 和 root_token**，丢失后无法恢复安全域内的加密数据。

---

## 七、查看日志与控制级别

```bash
docker logs -f coord-dev
```

通过 `RUST_LOG` 环境变量控制日志级别：

- `server` / `client` 默认是 `info`
- `dev` / `all` 默认是 `debug`
- 单机长期运行的 `coord all` 建议显式设置为 `RUST_LOG=info`

Docker `run` 示例：

```bash
docker run -d --name coord-all \
  --restart unless-stopped \
  -p 9090:9090 \
  -p 9091:9091 \
  -p 7947:7947/udp \
  -v coord-data:/data \
  -e COORD_DATA_DIR=/data \
  -e RUST_LOG=info \
  nexus.byteforce.cn/image-private/coord:0.1.15 \
  all
```

Compose 示例：

```yaml
environment:
  COORD_DATA_DIR: "/data"
  RUST_LOG: "info"
```

按模块临时提高日志级别：

```bash
RUST_LOG=info,coord=debug
```

> 周期性 `persisted runtime snapshot to redb` 日志为 `debug` 级别；默认 `info` 运行时不会持续刷屏。若需排查快照持久化细节，请临时提高 `RUST_LOG`。
