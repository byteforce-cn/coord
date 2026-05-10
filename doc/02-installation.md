# 安装指南

## 环境要求

| 工具 | 版本 | 备注 |
|------|------|------|
| Rust | 1.93.0 | `rust-toolchain.toml` 自动锁定；`rustup` 首次构建自动安装 |
| protoc | 3.x | 编译 proto 桩代码，`apt install protobuf-compiler` |
| Docker + Compose v2 | 26+ | 仅集成测试需要 |

---

## 一、配置私有 Cargo registry

`coord-core` 与 `coord-proto` 发布在 Byteforce 私有 registry。

### 1.1 本地开发

```bash
# 一次性写入凭据
cargo login --registry byteforce
# 提示 token: 填写 nexus 凭据（格式 Basic <base64(user:password)>）
```

或直接编辑 `~/.cargo/credentials.toml`：

```toml
[registries.byteforce]
token = "Basic <base64(user:password)>"
```

`~/.cargo/config.toml` 中已声明 registry 地址（项目级 `.cargo/config.toml` 同步）：

```toml
[registries.byteforce]
index = "https://nexus.byteforce.cn/repository/cargo-repo/"
```

### 1.2 CI / CD

```bash
export CARGO_REGISTRIES_BYTEFORCE_TOKEN="Basic <base64>"
```

---

## 二、编译

```bash
# 仅编译服务端和 ctl
cargo build --release -p coord-server -p coord-ctl

# 编译整个工作区（含 benchmark）
cargo build --release --workspace
```

输出二进制：

```
target/release/coord-server
target/release/coord-ctl
```

### 安装到系统路径

```bash
cargo install --path crates/coord-server
cargo install --path crates/coord-ctl
```

---

## 三、运行测试

```bash
# 所有单元测试
cargo test --workspace

# 仅 coord-server 的集成测试
cargo test -p coord-server
```

> **集成测试**（`tests/` 目录）会启动真实 Raft 节点，需要空闲端口。
> E2E 测试（`e2e/`）使用 Docker Compose，需要私有镜像。

---

## 四、运行时依赖

生产部署只需单一二进制 `coord-server`，无其他动态库依赖（静态链接 musl 构建）。
`coord-ctl` 同样单一二进制，可复制到任意机器使用。

使用 Docker 时直接拉取预构建镜像，跳过本地编译：

```bash
docker pull nexus.byteforce.cn/image-private/coord:0.1.10
```
