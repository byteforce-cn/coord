# Coord

<div align="center">

[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-1.93.0-orange.svg)](rust-toolchain.toml)
[![Java](https://img.shields.io/badge/java-21-red.svg)](coord-java-sdk/pom.xml)

[**English**](README.md) · **简体中文**

</div>

**Coord** 是一个面向微服务平台的分布式协调服务。一组基于 Raft 的 Server 集群提供强一致原语——KV 存储、原子事务、租约、变更监听、鉴权与静态加密；每台业务机器上运行的 `coord-agent` 守护进程再把这些原语转化为业务代码真正需要的高级协调能力。

> **Agent 优先架构**：业务应用永不直连 Coord Server 集群。每台机器运行一个 `coord-agent`，应用通过本机 gRPC（`127.0.0.1:19527`）接入。Agent 以同一契约代理 Server 核心原语，并在本机承载全部高级协调服务——缓存读请求、扇出 Watch，即使与集群链路暂时中断，本地协调能力也保持可用。

> **当前项目使用 Deepseek V4 辅助开发，用于学习和验证，不可用于生产环境。**

## 为什么选择 Coord？

如果你熟悉 etcd、Consul 或 ZooKeeper，可以把 Coord 看作两层：

- **共识与存储基座**——基于 Raft 提供线性一致的 KV / Txn / Watch / Lease，能力与 etcd 类似，并内置鉴权、TLS/mTLS 与静态加密；
- **每机 Agent 层**——每台宿主机的 `coord-agent` 以本地 gRPC 服务形式暴露注册发现、配置管理、分布式锁、ID 生成、Leader 选举、事件通知、缓存、消息队列、工作流、调度、限流、特性开关与 PKI 签发等能力，全部收敛在单一契约之下。业务代码只面对一个端点、一个 SDK，完全不需要感知集群拓扑。

一致性核心由仓库内 [Jepsen](https://github.com/jepsen-io/jepsen) 测试工程独立验证（见「验证与质量」）。

## 架构

```mermaid
graph TD
    subgraph "业务机器"
        APP[业务应用<br/>coord-client / coord-java-sdk]
        AGENT[coord-agent<br/>本机 :19527]
    end
    subgraph "Coord Server 集群 — 3 节点"
        S1[Server 1 · Raft Leader]
        S2[Server 2 · Raft Follower]
        S3[Server 3 · Raft Follower]
    end

    APP -->|gRPC · 本机| AGENT
    AGENT -->|gRPC :50051| S1
    AGENT -->|gRPC :50051| S2
    AGENT -->|gRPC :50051| S3
    S1 <-->|Raft :50052| S2
    S2 <-->|Raft :50052| S3
    S3 <-->|Raft :50052| S1
```

Server 的 `50051` / `50052` 端口仅对 Agent 可达，**对业务应用永不暴露**。

## 核心能力

**Server —— 共识与存储基座**

| 领域 | 说明 |
|:---|:---|
| KV / Txn / Watch / Lease | 线性一致；kill / pause / partition 全矩阵通过 Jepsen 验证 |
| Auth / RBAC | 用户 / 角色 / 权限，Ed25519 CCT 令牌，登录限流 |
| TLS / mTLS | gRPC 与 Raft 通道加密；缺 CA 拒绝启动（fail-closed） |
| 静态加密 | AES-256-GCM，外加 Shamir 分片的 Seal / Unseal |
| 运维 | 快照、MVCC 压缩、动态成员管理、Prometheus 指标 |
| Multi-Raft（opt-in） | 多 Raft 组 Region 分片 + 内嵌 PD 调度；`[multi_raft]` 显式开启（见 [`config.example.toml`](config.example.toml)） |

**Agent —— 业务应用真正打交道的协调层**

17 个可插拔 `coord.agent.*` gRPC 服务，通过 `[services]` 配置逐项开关：

- **发现与配置**：`Registry` · `ConfigCenter` · `Event`
- **协调原语**：`Lock` · `IdGen` · `LeaderElection`
- **数据与消息**：`Cache` · `Mq` · `Replica`（Cache/MQ 的 ISR 复制）
- **流程自动化**：`Workflow` · `Scheduler` · `Policy` · `FeatureFlags`
- **韧性与安全**：`CircuitBreaker` · `RateLimiter` · `Transit` · `Pki`

Agent 附加能力：与 Server 同契约的核心代理服务（`coord.kv` / `coord.txn` / `coord.lease` / `coord.watch` / `coord.maintenance`）、KV 读缓存、Watch 扇出，以及 `127.0.0.1:19528` 上的健康检查与 Prometheus 指标。

## 快速开始

**前置条件**：Rust 1.93.0（由 `rust-toolchain.toml` 固定）；Java 21 + Maven 3.9+ 与 Node 22 + pnpm 仅在需要使用 Java SDK 或 Web 界面时安装。

```bash
cargo build                                # 构建全部 crate
cargo test --workspace --no-fail-fast      # 运行全量测试
```

**单节点开发模式**（本机 Server + Agent）：

```bash
cargo run -p coord -- dev --fresh
```

Server gRPC 监听 `127.0.0.1:50051`，Agent 监听 `127.0.0.1:19527`。

**启动真实集群**：

```bash
# 节点 1 —— 引导
cargo run -p coord -- server --bootstrap

# 节点 2、3 —— 加入节点 1
cargo run -p coord -- server --id 2 --join <node1-grpc-addr>
```

生产部署请把 [`config.example.toml`](config.example.toml) 复制到每个节点：所有节点 `auth_root_key` 与 `raft_shared_secret` 一致，仅首节点 `bootstrap = true`，其余节点填 `join_addr`；`security` 段可选 mTLS 与静态加密。

**每台机器运行 Agent**：

```bash
cargo run -p coord -- agent --static-peers <server1>:50051,<server2>:50051
cargo run -p coord -- agent --agent-config agent.toml   # services / tls / auth / replication
```

**从业务应用接入**——连接本机 Agent（完整示例见 [`java-example/`](java-example/)）：

```java
CoordClient client = CoordClient.connectToLocalAgent();   // localhost:19527
client.put("/app/config", "value");
String val = client.get("/app/config");
```

运维子命令：`coord member | snapshot | security | auth | capability | idgen | reset`。

## 项目结构

```
coord/
├── coord/               # CLI 入口（server / agent / dev + 运维子命令）
├── coord-proto/         # Protobuf / gRPC 契约
├── coord-core/          # 公共 Trait 与类型
├── coord-server/        # Server：Raft / MVCC / Txn / Watch / Lease / Auth / TLS
├── coord-agent/         # Agent 守护进程——每机协调层
├── coord-client/        # Rust 客户端 SDK
├── coord-java-sdk/      # Java SDK（cn.byteforce:coord-java-sdk）
├── java-example/        # Java 接入示例
├── coord-ui/            # Web 管理界面（React 19 + Vite）
├── jepsen/              # 仓库内 Jepsen 测试工程与 lab
├── apis/contracts/      # 协议契约与能力承诺
├── deploy/              # docker-compose 三节点 + Kubernetes StatefulSet
└── monitoring/          # Grafana 面板 + Prometheus 规则
```

## 验证与质量

- **Jepsen**——仓库内 Clojure 工程（[`jepsen/`](jepsen/README.md)）在真实 3 节点集群上运行 knossos 线性一致性检查器：`register` / `cas-register` / `multi-register` 负载 × kill / pause / partition 故障注入，外加 72 小时浸泡；
- **快速本地收口**——`scripts/jepsen-check.sh` 无需 lab，约 2–3 分钟复现核心矩阵；
- **CI**——fmt + clippy（`-D warnings`）、非测试代码 panic 卡口、workspace 测试、proto 契约检查（buf breaking）、`cargo audit` + `cargo deny`、真实进程 chaos 运行。

## 部署

- **docker-compose**：三节点集群见 [`deploy/docker-compose/`](deploy/docker-compose/README.md)
- **Kubernetes**：含探针与 PDB 的 StatefulSet 见 [`deploy/k8s/statefulset.yaml`](deploy/k8s/statefulset.yaml)
- **监控**：Grafana 面板 + Prometheus 告警规则见 [`monitoring/`](monitoring/)
- **Web 界面**：见 [`coord-ui/README.md`](coord-ui/README.md)

## 状态

版本 `0.1.0`（pre-1.0），尚未发布任何 tag。Raft 引擎（`openraft`）为 alpha 依赖，Coord **暂不建议用于生产**；不过上述 Jepsen 矩阵已覆盖核心一致性与故障恢复语义。

## 文档

- 协议契约与能力承诺：[`apis/contracts/`](apis/contracts/README.md)
- Server 配置参考：[`config.example.toml`](config.example.toml)
- 漏洞报告：[`SECURITY.md`](SECURITY.md)

## 参与贡献

见 [`CONTRIBUTING.md`](CONTRIBUTING.md) 与 [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md)。

## 许可

[MIT](LICENSE) © Byteforce Team
