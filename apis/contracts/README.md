# Coord 对外协议契约（apis/contracts）

Coord 平台对上游业务方（100+ 微服务，Java/Go）的**服务协调能力承诺**。

契约的目的只有一个：**向业务消费者承诺协调能力**——服务注册发现、分布式锁、
Leader 选举、分布式 ID、事件通知——并以承诺**倒逼 coord 项目将这些能力做实**。
KV/Txn/Lease/Watch 是平台实现底座，保留稳定承诺供 SDK 与数据面对齐，
**不构成对业务方的编排建议**：业务方不需要、也不应该用原语自行拼装协调逻辑。

- 📜 承诺文本：[WHITEPAPER.md](./WHITEPAPER.md)（协议白皮书 v1.2.0）
- 🚦 生产就绪验收门：[../docs/production/production-readiness-plan-2026-09-21.md](../docs/production/production-readiness-plan-2026-09-21.md) §4（**契约期限 ≠ 生产就绪**）
- 🗂 承诺台账：[STATUS.md](./STATUS.md)（三态分层 + GA/整改期限，CI 解析，单一事实来源）

## 能力承诺面（契约 v1.2.0，COMMITTED — 期限为硬截止）

**16 个 `coord.<domain>.v1` 包 + `coord.storage`，共 17 行台账**（逐行状态与整改要点见
[`STATUS.md`](./STATUS.md)，机器解析见 `scripts/check-wire-sync.sh`）：

| 能力 | 契约包 | 期限 |
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

> **`COMMITTED` 是接口承诺，不是生产就绪声明。** 期限到点 = 接口已冻结并挂载契约包；
> 生产就绪是另一套更严的判据，见
> [`../docs/production/production-readiness-plan-2026-09-21.md`](../docs/production/production-readiness-plan-2026-09-21.md)
> §4 的 P-Gate 1–9。

语义契约全文在 proto 注释中（`proto/coord/<domain>/v1/`）；期限逾期 = CI 红牌
（倒逼机制，白皮书 §13）。

## 底座原语（STABLE，实现底座）

KV / Txn / Lease / Watch / Maintenance.Status / Health：稳定承诺，受
`buf breaking` + wire-sync 硬卡口保护，供 SDK 与数据面对齐。

## 承诺修复区（EXPERIMENTAL）

**本区自 `contracts/v1.2.0`（2026-09-19）起为空。** 原 4 个 `coord.experimental.*` 包
（cache / mq / workflow / scheduler）**从未有 proto 文件、从未有任何消费者**，故直接建为
稳定包 `coord.<domain>.v1` 并转入上方 COMMITTED 段；`coord.storage` 状态位提升但**包名不迁**
（改名即 Breaking）。**不以实验包形态对外开放任何能力**；历史整改台账保留在
白皮书 §9.1（不删行）。当前无 EXPERIMENTAL 行，故 `check-wire-sync.sh` 的整改期限
NOTICE 输出为空。

## 接入方式

- 业务应用唯一入口 = **本机 Coord Agent** `127.0.0.1:19527`（协调能力经 Agent 提供）。
- Server 端口（`:50051`/`:50052`）仅 Agent 可达，**不向业务网络开放**（白皮书 §9.3 R3）。
- 认证：Agent 非 loopback 强制 auth + TLS；`authorization: Bearer <token>` metadata。

## 目录结构

```
apis/contracts/
├── README.md                  # 本文件
├── WHITEPAPER.md              # 协议白皮书（承诺文本）
├── CHANGELOG.md               # 契约版本记录（独立于代码版本）
├── STATUS.md                  # 承诺台账（三态 + 期限，机器可读）
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
    ├── check-wire-sync.sh         # wire 一致性 + 承诺期限卡口
    ├── check-wire-descriptor.sh   # descriptor 级结构性漂移（+ descriptor_signature.py）
    └── check-sdk-sync.sh          # Rust/Java SDK 与契约面同步卡口
```

## 消费方式

```bash
# 校验（CI 同款）
cd apis/contracts
buf lint
buf breaking --against '.git#branch=main,subdir=apis/contracts'
bash scripts/check-wire-sync.sh   # wire 一致性 + STATUS 期限卡口

# 生成客户端（示例：Go / Java 可用各自 buf 插件）
buf generate proto --template '{"version":"v2","plugins":[{"local":"protoc-gen-go","out":"gen/go"}]}'
```

## 版本与变更

- 契约独立版本号：`contracts/v{MAJOR}.{MINOR}.{PATCH}`（git tag），与代码版本解耦。
- 兼容性铁律、废弃策略（≥12 个月）、错误码契约：白皮书 §4/§6。
- 期限调整 = 契约变更：走白皮书 §11 流程并公示，不得静默顺延。
- 变更记录：CHANGELOG.md；CI 门禁：`.github/workflows/contract-check.yml`。
