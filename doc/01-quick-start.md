# 快速上手

5 分钟内让 coord 在本机跑起来。

`coord` 是单一二进制，包含服务端（`server` / `dev`）和管理 CLI（`ctl`）两个入口点。

---

## 方式一：Docker（推荐）

```bash
# 以 dev 模式启动（gRPC :9090，HTTP/metrics :9091）
docker run -d --name coord-dev \
  -p 9090:9090 -p 9091:9091 \
  nexus.byteforce.cn/image-private/coord:0.1.13 dev
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

## 接下来

| 目标 | 文档 |
|------|------|
| 生产 Docker 部署 | [03-deploy-docker.md](03-deploy-docker.md) |
| 三节点 Raft 集群 | [04-deploy-cluster.md](04-deploy-cluster.md) |
| 安全域 / Token / AppRole | [07-security.md](07-security.md) |
| Transit 加密与 PKI | [08-transit-pki.md](08-transit-pki.md) |
| 全量 CLI 参考 | [06-ctl.md](06-ctl.md) |
