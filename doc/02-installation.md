# 安装与构建

---

## 一、前提条件

| 工具 | 版本 | 用途 |
|------|------|------|
| Rust toolchain | 1.93.0 | 编译 coord |
| protoc | 3.x | 生成 protobuf 代码 |
| Docker | 27+ | 容器运行 / e2e 测试 |

---

## 二、配置 Byteforce 私有 Cargo registry

coord 依赖发布在 Byteforce 私有 Nexus 上的 crate（`coord-proto`、`coord-core` 等）。

在 `~/.cargo/config.toml` 中添加：

```toml
[registries.byteforce]
index = "sparse+https://nexus.byteforce.cn/repository/cargo-repo/"
credential-provider = ["cargo:token"]
```

在 `~/.cargo/credentials.toml` 中添加凭据：

```toml
[registries.byteforce]
token = "Bearer <your-nexus-token>"
```

---

## 三、源码构建

```bash
git clone https://github.com/byteforce/coord.git
cd coord

# 仅构建服务端二进制
cargo build --release -p coord

# 全量构建（含 SDK）
cargo build --release
```

产物路径：`target/release/coord`

---

## 四、使用 Docker 镜像（无需本地编译）

```bash
docker pull nexus.byteforce.cn/image-private/coord:0.1.15
```

镜像标签约定：

| 标签 | 说明 |
|------|------|
| `0.1.15` | 当前文档示例使用的稳定版本 |
| 其他显式语义化版本号 | 按目标发布版本精确指定，例如 `0.1.13` |

> 生产、测试与 CI 均建议使用明确版本号，不使用 `latest`。

---

## 五、二进制结构

`coord` 是单一自包含二进制：

```
coord server    # 生产服务端（Raft + gRPC + HTTP）
coord dev       # 开发模式（自动 init/unseal，调试日志）
coord client    # gossip 代理模式（Phase 4D，开发中）
coord all       # 单进程 server + gossip agent（开发 / 单机）
coord ctl       # 管理 CLI（连接运行中的 coord 实例）
```

- `coord client` 默认 gossip UDP 监听地址为 `0.0.0.0:7947`，通过 `COORD_CLIENT_GOSSIP_ADDR` 配置。
- `coord all` 默认内嵌 gossip UDP 端口为 `7947`，通过 `COORD_CLIENT_GOSSIP_PORT` 配置。
- `coord all` 当前仅适用于开发 / 单机场景；需要显式 gossip seed / advertise 地址时使用 `coord client`。

`coord ctl` 通过 gRPC 连接远程实例，不依赖本地运行的服务端进程。

---

## 六、验证安装

```bash
coord --version
# coord 0.1.15

coord ctl --help
```
