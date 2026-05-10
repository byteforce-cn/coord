# Changelog

本文件记录 coord 的所有重要变更，格式遵循 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。


## [0.1.9] - 2026-05-10

### 修改
- 移除 arm 发布

## [0.1.8] - 2026-05-10

### 修改
- 修复私有 nexus 仓库报错

## [0.1.7] - 2026-05-10

### 修改
- 修复私有 nexus 仓库报错

## [0.1.6] - 2026-05-10

### 修改
- 推送镜像到私有仓库

## [0.1.5] - 2026-05-10

### 修改
- 优化 release 脚本工具

## [0.1.4] - 2026-05-10

### 修改

- **修改**：优化容器构建

## [0.1.3] - 2026-05-10

### 修改

- **修改**：容器支持

## [0.1.2] - 2026-05-09

### 新增

- **修改**：更新 coord-core 到 0.0.1

## [0.1.1] - 2026-05-01

### 新增

- **增加版本管理辅助脚本**：实现 release 自动化。

## [Unreleased]

## [0.1.0] - 2026-05-01

### 新增

- **服务注册与租约心跳**：支持服务实例注册、TTL 租约与心跳续期。
- **配置中心与 Watch 流**：支持 key 级别配置写入、版本历史及长连接 Watch。
- **分布式锁（FIFO 语义）**：支持公平排队等待、TTL 自动释放、强制撤锁。
- **Snowflake ID 生成器**：毫秒级时钟序号，支持多节点独立分配。
- **轻量工作流调度器**：start / poll / complete / status / intervene 生命周期 API。
- **Transit 加密服务**：AES-GCM 加解密、HMAC 签名/验证、Key 轮转。
- **PKI 服务**：签发/续期/吊销证书、CA 链查询、CRL、OCSP 风格状态查询、ACME order/challenge/finalize。
- **安全控制面**：Seal/Unseal + AppRole + Token 鉴权 + 能力授权 + Shamir 拆分 + 加密安全域快照。
- **OpenRaft + Redb 存储**：多节点 Raft 一致性，支持 PreVote、联合共识成员变更、leader 步进。
- **backup v4**：一致性元数据 + Raft 提交索引 + 加密安全域，支持 v1/v2/v3 自动升级。
- **coord-server dev 模式**：单节点快速启动，gRPC `0.0.0.0:9090`，HTTP 控制面 `0.0.0.0:9091`（`/healthz` `/metrics` `/ui` `/api/v1/*`）。
- **coord-ctl 管理 CLI**：cluster / member / lock / operator / auth / workflow / transit / pki / backup 子命令。
- **Prometheus 指标**：全服务标准化指标暴露。
- **运维控制台 UI**：React + Tailwind，内嵌于 coord-server HTTP 控制面。
- **多语言 SDK**：Rust SDK、Java SDK（Maven）、Go SDK，均支持流重建与重试指标。

[Unreleased]: https://github.com/byteforce-cn/coord/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/byteforce-cn/coord/releases/tag/v0.1.0
