# coord — Coordination Service

[![CI](https://github.com/byteforce-cn/coord/actions/workflows/ci.yml/badge.svg)](https://github.com/byteforce-cn/coord/actions/workflows/ci.yml)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.93.0-orange.svg)](rust-toolchain.toml)

> **⚠️ MVP 预览版本 — 尚未达到生产就绪（Production-Ready）标准**
>
> 本仓库是 coord 的最小可行版本（MVP）。核心功能已实现并通过基础测试，但在稳定性、可观测性、安全加固及运维文档等方面仍存在已知缺口，**不建议在生产环境使用**。版本路线图参见 [CHANGELOG.md](CHANGELOG.md)。
> 注意:仓库在未声明稳定前可能重建

> **🤖 AI 辅助开发声明**
>
> 本项目的设计、代码和文档在开发过程中借助了 AI Agent（GitHub Copilot）进行辅助生成与评审。所有输出均经过人工审阅，但使用者应自行评估代码质量并进行充分测试。

---

coord 是一个基于 [OpenRaft](https://github.com/datafuselabs/openraft) 构建的轻量级分布式协调服务，使用 Rust 实现，通过单一二进制提供以下能力：

- **服务注册与发现**：租约心跳、健康检测
- **配置中心**：版本化 KV + 长连接 Watch 流
- **分布式锁**：FIFO 公平排队、TTL 自动释放
- **ID 生成**：Snowflake 算法，多节点独立分配
- **工作流引擎**：[CNCF Serverless Workflow DSL](https://serverlessworkflow.io/) 兼容引擎
- **Transit 加密**：AES-GCM 加解密、HMAC 签名、Key 轮转
- **PKI 服务**：内置 CA、证书签发/续期/吊销、CRL、OCSP、ACME
- **安全控制面**：Seal/Unseal、AppRole 认证、Shamir 密钥拆分、能力授权

所有写操作通过 Raft 一致性协议复制，支持多节点集群部署。

---

## 目录

- [coord — Coordination Service](#coord--coordination-service)
  - [目录](#目录)
  - [环境要求](#环境要求)
  - [快速启动](#快速启动)
  - [仓库结构](#仓库结构)
  - [SDKs](#sdks)
  - [运维控制台](#运维控制台)
  - [常用操作示例](#常用操作示例)
  - [基准测试](#基准测试)
  - [贡献](#贡献)
  - [许可证](#许可证)

> **注意**：Rust / Java / Go SDK 已移至 Byteforce 私有仓库维护，待可用性稳定后开放。

---

## 环境要求

| 工具 | 版本 |
|------|------|
| Rust | 1.93.0（见 `rust-toolchain.toml`，首次构建自动安装） |
| protoc | 3.x |
| Docker + Compose v2 | 集成测试需要 |

> **私有依赖说明**：`coord-core` 与 `coord-proto` 两个 crate 发布在 Byteforce 私有 Cargo registry（`byteforce`）。外部贡献者需联系维护者获取凭据，或跳过相关构建目标。
>
> 本地配置：`cargo login --registry byteforce`
> CI 注入：`CARGO_REGISTRIES_BYTEFORCE_TOKEN=Basic <base64(user:password)>`

---

## 快速启动

```bash
# 构建并运行测试
cargo test --workspace

# 启动单节点 dev 服务（gRPC :9090，HTTP 控制面 :9091）
cargo run -p coord -- dev

# 查看集群状态
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 cluster status

# 健康检查 & 指标
curl http://127.0.0.1:9091/healthz
curl http://127.0.0.1:9091/metrics
```

Dev 模式默认参数：

| 参数 | 默认值 |
|------|--------|
| gRPC 地址 | `0.0.0.0:9090` |
| HTTP 控制面 | `0.0.0.0:9091` |
| 数据目录 | `/tmp/coord-dev` |

---

## 仓库结构

```
crates/
  coord/            统一二进制：server / dev / client / ctl 四种模式
benchmark/          多场景压测工具 + 报告生成器
e2e/                集成测试套件（Cucumber/Docker Compose）
ui/console/         React + Tailwind 运维控制台
```

---

## SDKs

Rust、Java、Go SDK 已移至 Byteforce 私有仓库维护，不在本仓库发布。待功能稳定后开放
如确需接入，请联系 [byteforce@qq.com](mailto:byteforce@qq.com) 获取访问权限。

coord 服务通过标准 gRPC 对外暴露 API，可使用任意语言的 gRPC 工具自行生成客户端桩代码（proto 文件由私有仓库维护，可联系 [dev@byteforce.cn](mailto:dev@byteforce.cn) 获取）。

---

## 运维控制台

控制台作为静态资源内嵌于 `coord-server` HTTP 控制面（`/ui`）：

```bash
# 构建前端资源
cd ui/console && npm install && npm run build

# 启动服务端
cd ../.. && cargo run -p coord -- dev

# 浏览器访问
# http://127.0.0.1:9091/ui
```

当前控制台支持：集群状态、服务注册、配置管理、分布式锁、工作流、Transit 密钥、PKI 证书、安全控制面（只读 + 写操作）。

---

## 常用操作示例

<details>
<summary>安全控制面（Seal/Unseal / AppRole）</summary>

```bash
# 初始化安全域（返回 Shamir 拆分份额）
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 operator init --shares-total 3 --threshold 2

# 解封（提供 threshold 份额）
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 operator unseal <share-1>
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 operator unseal <share-2>

# 创建 AppRole 并获取 token
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 auth approle create svc-a --policy transit.encrypt
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 auth approle generate-secret-id <role-id>
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 auth approle login <role-id> <secret-id>
```

</details>

<details>
<summary>Transit 加密</summary>

```bash
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 transit create-key app-key
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 transit encrypt app-key "hello-coord"
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 transit decrypt app-key "<ciphertext>"
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 transit rotate-key app-key
```

</details>

<details>
<summary>PKI</summary>

```bash
# 签发证书
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 pki issue svc-a.internal \
  --san svc-a.internal --san 127.0.0.1 --ttl-seconds 86400

# 续期 / 吊销 / CA 链 / CRL / OCSP
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 pki renew <serial>
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 pki revoke <serial> --reason key-compromise
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 pki ca-chain
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 pki crl
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 pki ocsp <serial>
```

</details>

<details>
<summary>工作流引擎（CNCF Serverless Workflow DSL v2）</summary>

> **开发预览**：当前使用 `MemoryWorkflowStore`，重启后实例状态丢失，不可用于生产。

```bash
# 1) 部署工作流定义（YAML 文件，遵循 CNCF Serverless Workflow DSL v2 规范）
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 workflow deploy --file payment.yaml

# 2) 启动工作流实例
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 workflow start \
  --definition-id payment --namespace default --input-json '{"order_id":"123"}'

# 3) 查询实例状态
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 workflow get <instance-id>

# 4) 列出实例 / 定义
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 workflow list --namespace default
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 workflow definitions --namespace default
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 workflow definition <definition-id>
```

</details>

<details>
<summary>集群成员管理 & 备份</summary>

```bash
# 成员变更（通过 Raft 联合共识路径）
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 member add node-2 10.0.0.2:9090
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 member remove node-2

# 备份 & 恢复
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 backup create --file /tmp/coord-backup.json
cargo run -p coord -- ctl --endpoint http://127.0.0.1:9090 backup restore /tmp/coord-backup.json
```

</details>

---

## 基准测试

```bash
# 运行全部场景（release 模式）
./benchmark/run.sh \
  --endpoint http://127.0.0.1:9090 \
  --duration-seconds 30 \
  --concurrency 32 \
  --scenarios all

# 指定场景
./benchmark/run.sh \
  --endpoint http://127.0.0.1:9090 \
  --scenarios config_put_get,transit_encrypt_decrypt,idgen_generate

# 报告输出至 benchmark/reports/report_<timestamp>.{json,md}
```

---

## 贡献

欢迎提交 Issue 和 Pull Request，详见 [CONTRIBUTING.md](CONTRIBUTING.md)。参与即表示同意遵守 [行为准则](CODE_OF_CONDUCT.md)。

安全漏洞请参照 [SECURITY.md](SECURITY.md) 私下披露，**不要**通过公开 Issue 报告。

---

## 许可证

本项目基于 [Apache License 2.0](LICENSE) 授权。

Copyright 2026 Byteforce
