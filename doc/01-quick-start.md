# 快速上手

5 分钟内让 coord 在本机跑起来。

---

## 方式一：Docker（推荐）

```bash
# 拉取并以 dev 模式启动（gRPC :9090，HTTP :8080）
docker run -d --name coord-dev \
  -p 9090:9090 -p 8080:8080 \
  nexus.byteforce.cn/image-private/coord:0.1.9 dev
```

验证服务就绪：

```bash
curl http://localhost:8080/healthz
# → {"status":"ok"}
```

初始化安全域并解封（首次必须执行一次，重启后需重新解封）：

```bash
# 初始化（返回 unseal shares 和 root_token）
docker exec coord-dev coord-ctl operator init --shares-total 1 --threshold 1

# 解封（将上一步输出的 share 粘贴到此处）
docker exec coord-dev coord-ctl operator unseal <share>
```

> **快捷方式**：dev 模式支持 `COORD_DEV_ROOT_TOKEN` 环境变量，启动时自动 init + unseal，
> 无需手动操作。详见 [单节点 Docker 部署](03-deploy-docker.md#dev-root-token)。

---

## 方式二：源码构建

前提：Rust 1.93、protoc 3.x，以及 Byteforce 私有 registry 凭据。

```bash
# 配置 registry（仅需一次）
cargo login --registry byteforce

# 编译
cd public/coord
cargo build --release -p coord-server -p coord-ctl

# 启动
./target/release/coord-server dev
```

---

## 第一个请求

```bash
# 通过 ctl 查询集群状态
coord-ctl cluster status

# 直接通过 HTTP 控制面查询概览
curl http://localhost:8080/api/v1/overview
```

---

## 下一步

| 主题 | 文档 |
|------|------|
| 源码构建详细步骤 | [安装指南](02-installation.md) |
| 单节点 Docker 生产部署 | [Docker 部署](03-deploy-docker.md) |
| 三节点集群 | [集群部署](04-deploy-cluster.md) |
| 所有服务端参数 | [服务端配置参考](05-server-config.md) |
| coord-ctl 全命令 | [ctl 参考](06-ctl.md) |
| 安全域（Seal / AppRole） | [安全控制面](07-security.md) |
| 单元测试接入 | [测试接入指南](10-testing.md) |
