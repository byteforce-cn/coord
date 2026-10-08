# coord docker-compose 三节点集群

> R-OBS-15 交付物。
> 本地开发的单节点 dev 组合见文末「单节点 dev（容器，本地开发）」一节。

## 快速启动

```bash
cd deploy/docker-compose

# 1. 填共享密钥（三节点一致）
cp .env.example .env
#   编辑 .env 设置 ROOT_PASSWORD / AUTH_ROOT_KEY / RAFT_SHARED_SECRET
#   并把 conf/coord-{1,2,3}.toml 中的 __FILL_AUTH_ROOT_KEY__ /
#   __FILL_RAFT_SHARED_SECRET__ 替换为与 .env 一致的取值
#   （一行 sed 即可：sed -i "s/__FILL_AUTH_ROOT_KEY__/<64位hex>/" conf/coord-*.toml）

# 2. 生成 TLS/mTLS 证书（gRPC + raft 默认 mTLS；缺失时集群拒绝启动）
./certs/gen-certs.sh   # 需要 openssl；产物：ca.crt/server.crt/server.key/agent.crt/agent.key

# 3. 构建镜像（仓库根目录 context）
docker compose build

# 4. 启动
docker compose up -d

# 5. 验证可读写（任一节点）
#   登录拿 CCT：/coord.auth.Auth/Authenticate（root / ROOT_PASSWORD）
#   HTTP：curl http://127.0.0.1:50061/metrics | head
docker compose ps
```

## 拓扑

| 节点 | 外部 gRPC | 外部 HTTP（/metrics /healthz） | 内部 raft |
|:--|:--|:--|:--|
| coord-1（bootstrap） | 50051 | 50061 | 50052 |
| coord-2 | 50052 | 50062 | 50052 |
| coord-3 | 50053 | 50063 | 50052 |

## 安全基线

- **传输层 mTLS 默认开启**（`tls_cert/tls_key/tls_ca`，gRPC + raft 同口径，R-SEC）：
  三节点共用同一 server 证书（SAN 覆盖 coord-1/2/3 + localhost + 127.0.0.1），
  客户端须携带同一 CA 签发的身份（`certs/agent.crt`）；
- 鉴权默认开启（`security.auth_enabled=true`）；
- raft 端口共享密钥 HMAC 认证保留为纵深防御（`security.raft_shared_secret`，R-SEC-03）；
- HTTP `/metrics` 默认 loopback 绑定（`network.http_addr` 可配内网地址，R-SEC-05）；
- 生产需补充：磁盘配额、`security.encryption_enabled` 灰度迁移。

## CLI 管理 mTLS 集群

```bash
# 对 TLS/mTLS 集群执行全部管理命令（auth/member/snapshot/reset/idgen）
coord auth status --addr 127.0.0.1:50051 \
  --tls-ca certs/ca.crt --tls-cert certs/agent.crt --tls-key certs/agent.key
coord member list --addr 127.0.0.1:50051 \
  --tls-ca certs/ca.crt --tls-cert certs/agent.crt --tls-key certs/agent.key
```

详见 `docs/production/ops/security.md`。

## 单节点 dev（容器，本地开发）

本地开发用单节点 dev 组合（与上文的集群组合相互独立，同一镜像）：

```bash
docker compose -f deploy/docker-compose/docker-compose.dev.yml up -d --build
# UI:         http://127.0.0.1:50061
# Agent gRPC: 127.0.0.1:19527（应用 / SDK 连接目标）
# Server:     127.0.0.1:50051（Direct 模式）
# 重置数据:   docker compose -f deploy/docker-compose/docker-compose.dev.yml down -v
```

要点：

- **仅限本机开发**：鉴权强制关闭（默认凭据 `root`/`root`）、明文传输；宿主端口只发布到
  `127.0.0.1`（不暴露局域网）；
- `coord dev --bind-addr 0.0.0.0 --allow-insecure` 在容器内可用：Raft 收敛 loopback
  （不对外暴露 raft 端口），Agent 非 loopback 明文绑定为 dev 显式放行（启动 WARN；见
  `docs/adr/0008-dev-container-mode.md`）；
- Agent 内建服务在 dev 下全开（`replication` 除外）；`transit` 回退 dev 专用默认 KEK
  （启动 WARN、数据无保密性；见 `docs/adr/0009-dev-mode-builtin-services.md`）；
- 数据存命名卷 `coord-dev-data`：重启保留，`down -v` 清空（默认不带 `--fresh`，
  避免每次重启清库）；
- 镜像与集群组合共用（`byteforce/coord:local`，同一 `Dockerfile`）：已构建过则无需重复构建。
