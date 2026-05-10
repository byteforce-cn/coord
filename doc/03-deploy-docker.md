# 单节点 Docker 部署

适用于本地开发、功能验证以及小规模内部服务场景。

---

## 一、最简启动

```bash
docker run -d --name coord-dev \
  -p 9090:9090 \
  -p 8080:8080 \
  -v coord-data:/data \
  nexus.byteforce.cn/image-private/coord:0.1.10 \
  dev
```

| 端口 | 协议 | 用途 |
|------|------|------|
| `9090` | gRPC | SDK / coord-ctl 接入 |
| `8080` | HTTP | `/healthz` `/readyz` `/metrics` `/ui` |

验证健康：

```bash
curl http://localhost:8080/healthz
# → {"status":"ok"}
```

---

## 二、使用 Docker Compose（推荐）

项目提供了现成的 Compose 文件：

```bash
# 从仓库根目录执行
docker compose -f docker/docker-compose.dev.yml up -d

# 指定版本
COORD_VERSION=0.1.9 docker compose -f docker/docker-compose.dev.yml up -d
```

停止并销毁数据：

```bash
docker compose -f docker/docker-compose.dev.yml down -v
```

---

## 三、Dev Root Token（测试 / CI 推荐）<a id="dev-root-token"></a>

**`dev` 模式**支持通过环境变量在启动时自动完成安全域初始化 + 解封，并使用固定 root token，
方便测试配置硬编码 token 值，不需要每次从日志中捞 token。

```bash
docker run -d --name coord-dev \
  -p 9090:9090 -p 8080:8080 \
  -v coord-data:/data \
  -e COORD_DEV_ROOT_TOKEN=s.my-test-token \
  nexus.byteforce.cn/image-private/coord:0.1.10 \
  dev
```

> - 首次启动：自动 init（1-of-1 Shamir） + unseal，root token 嵌入域中。
> - 重启后：读取 `/data/dev-unseal.share` 自动重新 unseal，token 保持不变。
> - `serve` 模式下此参数被忽略，不影响生产行为。

Compose 方式：

```yaml
# docker-compose.dev.yml 中添加
environment:
  COORD_DEV_ROOT_TOKEN: "s.my-test-token"
```

---

## 四、完整环境变量

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `COORD_GRPC_ADDR` | `0.0.0.0:9090` | gRPC 监听地址 |
| `COORD_HTTP_ADDR` | `0.0.0.0:8080` | HTTP 控制面监听地址 |
| `COORD_DATA_DIR` | `/tmp/coord-dev` | 数据目录（需挂载 volume 持久化） |
| `COORD_NODE_ID` | 自动生成 UUID | 节点唯一标识，重启需保持一致 |
| `COORD_DEV_ROOT_TOKEN` | — | dev 模式固定 root token（不影响生产） |
| `COORD_TLS_CERT` | — | TLS 证书路径（PEM） |
| `COORD_TLS_KEY` | — | TLS 私钥路径（PEM） |
| `COORD_TLS_CLIENT_CA` | — | mTLS 客户端 CA（PEM） |
| `COORD_OTLP_ENDPOINT` | — | OTLP 收集器地址（如 `http://otel:4317`） |

完整参数参考见 [服务端配置参考](05-server-config.md)。

---

## 五、手动安全域初始化（不使用 Dev Root Token 时）

每次容器重新创建（或清空数据卷）后需执行一次：

```bash
# 步骤 1：初始化（示例：3 share，threshold=2）
docker exec coord-dev coord-ctl operator init --shares-total 3 --threshold 2
# 输出：
#   initialized: true
#   unseal_shares:
#   shamir:AAAA...
#   shamir:BBBB...
#   shamir:CCCC...
#   root_token: s.xxxxxxxxxxxxxxxx

# 步骤 2：提交 threshold 份额解封
docker exec coord-dev coord-ctl operator unseal shamir:AAAA...
docker exec coord-dev coord-ctl operator unseal shamir:BBBB...
# → sealed: false
```

> ⚠️ **请将 unseal shares 和 root_token 妥善保存**，丢失后无法恢复安全域内的加密数据。

---

## 六、查看日志

```bash
docker logs -f coord-dev
```

---

## 七、访问控制台 UI

浏览器访问：<http://localhost:8080/ui>

首次访问需要 root_token（或有效 AppRole token）登录。
