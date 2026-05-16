# 快速上手

5 分钟内让 coord 在本机跑起来。

`coord` 是单一二进制，包含服务端（`server` / `dev`）、gossip 代理（`client`）、单进程 server+client 模式（`all`）以及管理 CLI（`ctl`）五个入口点。

---

## 方式一：Docker（推荐）

```bash
# 以 dev 模式启动（gRPC :9090，HTTP/metrics :9091）
docker run -d --name coord-dev \
  --log-opt max-size=10m \
  --log-opt max-file=3 \
  -p 9090:9090 -p 9091:9091 \
  nexus.byteforce.cn/image-private/coord:0.1.15 dev
```

验证服务就绪：

```bash
curl http://localhost:9091/healthz
# → {"status":"ok"}
```

初始化安全域并解封（首次必须执行一次，重启后需重新解封）：

```bash
# 初始化（返回 unseal shares 和 root_token）
docker exec coord-dev coord ctl operator init --shares-total 1 --threshold 1

# 解封（将上一步输出的 share 粘贴到此处）
docker exec coord-dev coord ctl operator unseal <share>
```

> **快捷方式**：dev 模式支持 `COORD_DEV_ROOT_TOKEN` 环境变量，启动时自动 init + unseal，
> 无需手动操作。详见 [单节点 Docker 部署](03-deploy-docker.md#dev-root-token)。
>
> **日志说明**：上例已启用 Docker `json-file` 日志轮转；如需持久化数据、固定 node id 和更完整的 Compose 配置，参见 [03-deploy-docker.md](03-deploy-docker.md)。
>
> **持久化说明**：上面的 quick start 适合临时验证；如果你只有一台服务器并准备长期运行 `coord all`，请显式挂载数据卷并设置 `COORD_DATA_DIR=/data`，否则默认仍写入 `/tmp/coord-dev`，容器重建后状态会丢失。

---

## 方式二：源码构建

前提：Rust 1.93.0、protoc 3.x，以及 Byteforce 私有 registry 凭据。

```bash
git clone https://github.com/byteforce/coord.git
cd coord

# 配置私有 registry（见 doc/02-installation.md）
cargo build --release -p coord

# 以 dev 模式启动
./target/release/coord dev
```

---

## 方式三：单进程 Server + Client（`coord all`，可选）

当你需要在一台机器上同时启动 CP 服务端和 AP gossip 代理时，可使用 `coord all`：

```bash
# 默认 gossip UDP 端口为 7947
cargo run -p coord -- all

# 自定义 gossip UDP 端口
COORD_CLIENT_GOSSIP_PORT=8947 cargo run -p coord -- all
```

- `coord all` 的服务端部分与 `coord dev` 完全一致。
- 内嵌 gossip agent 默认监听 `0.0.0.0:7947/udp`，宿主机或容器部署时需放通 / 映射该 UDP 端口。
- `coord all` 复用 `coord dev` 的默认数据目录 `/tmp/coord-dev`；单机长期运行请显式设置 `COORD_DATA_DIR` 指向持久目录，容器部署时通常挂载到 `/data`。
- `coord all` 默认日志级别与 `dev` 一致为 `debug`；生产化单机部署建议显式设置 `RUST_LOG=info`。
- 当前 `all` 模式不支持单独配置 gossip seeds 或 advertise 地址，适合开发 / 单机场景；多机 gossip 组网请使用 `coord client`。
- 详细参数见 [服务端配置参考](05-server-config.md)。

---

## 接下来

| 目标 | 文档 |
|------|------|
| 生产 Docker 部署 | [03-deploy-docker.md](03-deploy-docker.md) |
| 三节点 Raft 集群 | [04-deploy-cluster.md](04-deploy-cluster.md) |
| 安全域 / Token / AppRole | [07-security.md](07-security.md) |
| Transit 加密与 PKI | [08-transit-pki.md](08-transit-pki.md) |
| 全量 CLI 参考 | [06-ctl.md](06-ctl.md) |
