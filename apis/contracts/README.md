# Coord 对外协议契约（apis/contracts）

Coord 平台对上游业务方（Java/Go 微服务）的**服务协调能力契约**。

契约覆盖服务注册发现、分布式锁、Leader 选举、分布式 ID、事件通知，以及配置、
策略、熔断、限流、信封加密、PKI、缓存、消息队列、特性开关、工作流（Saga）、
调度、对象存储等协调能力；每项能力同时声明「承诺什么」与「不承诺什么」。
KV/Txn/Lease/Watch 是平台实现底座，保留稳定承诺供 SDK 与数据面对齐，
**不构成对业务方的编排建议**：业务方不需要、也不应该用原语自行拼装协调逻辑。

- 📜 承诺文本：[WHITEPAPER.md](./WHITEPAPER.md)（协议白皮书 v1.2.0）
- � 契约状态表：[STATUS.md](./STATUS.md)（三态分层 + 目标日期，机器解析）
- ⚠️ 契约承诺 ≠ 生产就绪声明：生产可用性由部署方按实际验证评估

## 能力承诺面（契约 v1.2.0，COMMITTED）

**16 个 `coord.<domain>.v1` 包 + `coord.storage`**（逐行状态与实现落点见
[`STATUS.md`](./STATUS.md)，一致性校验见 `scripts/check-wire-sync.sh`）：

| 能力 | 契约包 | 目标日期 |
|:---|:---|:---:|
| 服务注册发现 | `coord.registry.v1` | 2026-10-31 |
| 分布式 ID | `coord.idgen.v1` | 2026-10-31 |
| 分布式锁 | `coord.lock.v1` | 2026-11-30 |
| Leader 选举 | `coord.election.v1` | 2026-11-30 |
| 事件通知 | `coord.event.v1` | 2026-12-31 |
| 配置中心 | `coord.config.v1` | 2026-12-31 |
| 权限策略引擎 | `coord.policy.v1` | 2026-12-31 |
| 熔断器 | `coord.circuitbreaker.v1` | 2026-12-31 |
| 限流器 | `coord.ratelimiter.v1` | 2026-12-31 |
| 安全传输（信封加密） | `coord.transit.v1` | 2026-12-31 |
| PKI 证书签发 | `coord.pki.v1` | 2026-12-31 |
| 缓存 | `coord.cache.v1` | 2026-12-31 |
| 消息队列 | `coord.mq.v1` | 2026-12-31 |
| 特性开关 | `coord.featureflags.v1` | 2026-12-31 |
| 对象存储（Server） | `coord.storage` | 2026-12-31 |
| 工作流（Saga） | `coord.workflow.v1` | 2027-03-31 |
| 调度 | `coord.scheduler.v1` | 2027-03-31 |

> **`COMMITTED` 是接口承诺，不是生产就绪声明。** 目标日期到点 = 接口已冻结并挂载契约包；
> 生产可用性由部署方的实际运行验证评估。

语义契约全文在 proto 注释中（`proto/coord/<domain>/v1/`）；目标日期的逾期检查逻辑见
`scripts/check-wire-sync.sh`（约定详见 [WHITEPAPER.md](./WHITEPAPER.md)）。

## 底座原语（STABLE，实现底座）

KV / Txn / Lease / Watch / Maintenance.Status / Health：稳定承诺，受
`buf breaking` 与 wire-sync 校验保护，供 SDK 与数据面对齐。

## 实验承诺区（EXPERIMENTAL）

**本区自 `contracts/v1.2.0`（2026-09-19）起为空。** 原 4 个 `coord.experimental.*` 包
（cache / mq / workflow / scheduler）**从未有 proto 文件、从未有任何消费者**，故直接建为
稳定包 `coord.<domain>.v1` 并转入上方 COMMITTED 段；`coord.storage` 状态位提升但**包名不迁**
（改名即 Breaking）。**不以实验包形态对外开放任何能力**；各项能力的收敛记录见
[WHITEPAPER.md](./WHITEPAPER.md)。当前无 EXPERIMENTAL 行。

## 接入方式

- 业务应用唯一入口 = **本机 Coord Agent** `127.0.0.1:19527`（协调能力经 Agent 提供）。
- Server 端口（`:50051`/`:50052`）仅 Agent 可达，**不向业务网络开放**（见 WHITEPAPER.md 的红线约定）。
- 认证：Agent 非 loopback 强制 auth + TLS；`authorization: Bearer <token>` metadata。

## 目录结构

```
apis/contracts/
├── README.md                  # 本文件
├── WHITEPAPER.md              # 协议白皮书（承诺文本）
├── CHANGELOG.md               # 契约版本记录（独立于代码版本）
├── STATUS.md                  # 契约状态表（三态 + 目标日期，机器可读）
├── buf.yaml                   # buf 模块 / lint / breaking 配置
├── proto/
│   ├── coord/
│   │   ├── kv/ txn/ lease/ watch/ maintenance/     # 底座原语（STABLE，扁平包）
│   │   ├── registry/v1/registry.proto             # 能力承诺面（COMMITTED）
│   │   ├── idgen/v1/idgen.proto
│   │   ├── lock/v1/lock.proto
│   │   ├── election/v1/election.proto
│   │   ├── event/v1/event.proto
│   │   ├── config/v1/config.proto
│   │   ├── policy/v1/policy.proto
│   │   ├── circuitbreaker/v1/circuitbreaker.proto
│   │   ├── ratelimiter/v1/ratelimiter.proto
│   │   ├── transit/v1/transit.proto
│   │   ├── pki/v1/pki.proto
│   │   ├── cache/v1/cache.proto
│   │   ├── mq/v1/mq.proto
│   │   ├── featureflags/v1/featureflags.proto
│   │   ├── workflow/v1/workflow.proto
│   │   ├── scheduler/v1/scheduler.proto
│   │   └── storage/storage.proto                   # 对象存储（COMMITTED，扁平包）
│   └── grpc/health/v1/health.proto                 # 标准健康检查协议
└── scripts/
    ├── check-wire-sync.sh         # wire 一致性 + 目标日期校验
    ├── check-wire-descriptor.sh   # descriptor 级结构性漂移（+ descriptor_signature.py）
    └── check-sdk-sync.sh          # Rust/Java SDK 与契约面同步校验
```

## 消费方式

```bash
# 校验（CI 同款）
cd apis/contracts
buf lint
buf breaking --against '.git#branch=main,subdir=apis/contracts'
bash scripts/check-wire-sync.sh   # wire 一致性 + 目标日期校验

# 生成客户端（示例：Go / Java 可用各自 buf 插件）
buf generate proto --template '{"version":"v2","plugins":[{"local":"protoc-gen-go","out":"gen/go"}]}'
```

## 版本与变更

- 契约独立版本号：`contracts/v{MAJOR}.{MINOR}.{PATCH}`（git tag），与代码版本解耦。
- 兼容性铁律、废弃策略（≥12 个月）、错误码契约：见 [WHITEPAPER.md](./WHITEPAPER.md)。
- 目标日期调整 = 契约变更：走 [WHITEPAPER.md](./WHITEPAPER.md) 的变更流程并公示，不得静默顺延。
- 变更记录：CHANGELOG.md；CI 校验：`.github/workflows/contract-check.yml`。
