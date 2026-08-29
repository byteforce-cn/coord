# Coord 对外协议契约（apis/contracts）

Coord 平台对上游业务方（100+ 微服务，Java/Go）的**服务协调能力承诺**。

契约的目的只有一个：**向业务消费者承诺协调能力**——服务注册发现、分布式锁、
Leader 选举、分布式 ID、事件通知——并以承诺**倒逼 coord 项目将这些能力做实**。
KV/Txn/Lease/Watch 是平台实现底座，保留稳定承诺供 SDK 与数据面对齐，
**不构成对业务方的编排建议**：业务方不需要、也不应该用原语自行拼装协调逻辑。

- 📜 承诺文本：[WHITEPAPER.md](./WHITEPAPER.md)（协议白皮书 v1.1.0）
- 🗂 承诺台账：[STATUS.md](./STATUS.md)（三态分层 + GA/整改期限，CI 解析，单一事实来源）

## 能力承诺面（v1.1，COMMITTED — GA 期限为硬截止）

| 能力 | 契约包 | GA 期限 |
|:---|:---|:---:|
| 服务注册发现 | `coord.registry.v1` | 2026-10-31 |
| 分布式锁 | `coord.lock.v1` | 2026-11-30 |
| Leader 选举 | `coord.election.v1` | 2026-11-30 |
| 分布式 ID | `coord.idgen.v1` | 2026-10-31 |
| 事件通知 | `coord.event.v1` | 2026-12-31 |

语义契约全文在 proto 注释中（`proto/coord/<domain>/v1/`）；期限逾期 = CI 红牌
（倒逼机制，白皮书 §13）。

## 底座原语（STABLE，实现底座）

KV / Txn / Lease / Watch / Maintenance.Status / Health：稳定承诺，受
`buf breaking` + wire-sync 硬卡口保护，供 SDK 与数据面对齐。

## 承诺修复区（EXPERIMENTAL，整改承诺 + 期限）

Cache / MQ / Workflow / Scheduler：缺陷清单、整改承诺与期限见 STATUS.md 与
白皮书 §9；期限未兑现不得以任何形态对外开放。

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
│   │   ├── kv/ … txn/ lease/ watch/ maintenance/   # 底座原语（STABLE）
│   │   ├── registry/v1/registry.proto              # 能力承诺面（COMMITTED）
│   │   ├── lock/v1/lock.proto
│   │   ├── election/v1/election.proto
│   │   ├── idgen/v1/idgen.proto
│   │   └── event/v1/event.proto
│   └── grpc/health/v1/health.proto                 # 标准健康检查协议
└── scripts/
    └── check-wire-sync.sh     # wire 一致性 + 承诺期限卡口
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
