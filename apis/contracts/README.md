# Coord 对外协议契约（apis/contracts）

Coord 平台对上游业务方（100+ 微服务，Java/Go）的**稳定协议承诺**。
本目录与内部实现（`coord-proto/`、Raft、存储引擎）彻底解耦：

- **消费方只看本目录**：以这里的 proto 生成客户端，按《白皮书》编程。
- **内部重构不伤及承诺**：CI 门禁（buf breaking + wire-sync）保证
  `coord-proto` 的任何变更要么不越界，要么被阻断。

📜 **承诺文本：[WHITEPAPER.md](./WHITEPAPER.md)（协议兼容性白皮书，v1.0.0）**

## v1 承诺的服务

| 包 | 服务 | 说明 |
|:---|:---|:---|
| `coord.kv` | KV | Put / Range / Delete（线性一致读写、幂等 request_id） |
| `coord.txn` | Txn | 原子条件事务（Compare-And-Swap） |
| `coord.lease` | Lease | Grant / Revoke / KeepAlive（双向流） |
| `coord.watch` | Watch | 变更监听（双向流，at-least-once + 重同步协议） |
| `coord.maintenance` | Maintenance | **仅 Status**（探活；运维 RPC 不对外承诺） |
| `grpc.health.v1` | Health | 标准 gRPC 健康检查 |

红区（实验/残缺，**不在承诺范围**）：Multi-Raft/PD、Cache/MQ ISR、Seal/Unseal/静态加密、
Workflow、Agent 侧其余服务。完整矩阵与证据见白皮书 §2。

## 目录结构

```
apis/contracts/
├── README.md                  # 本文件
├── WHITEPAPER.md              # 协议兼容性白皮书（承诺文本）
├── CHANGELOG.md               # 契约版本记录（独立于代码版本）
├── buf.yaml                   # buf 模块 / lint / breaking 配置
├── proto/
│   ├── coord/
│   │   ├── kv/kv.proto
│   │   ├── txn/txn.proto
│   │   ├── lease/lease.proto
│   │   ├── watch/watch.proto
│   │   └── maintenance/maintenance.proto   # 裁剪版：仅 Status
│   └── grpc/health/v1/health.proto         # 标准健康检查协议
└── scripts/
    └── check-wire-sync.sh     # 契约 ↔ coord-proto wire 一致性卡口
```

## 消费方式

```bash
# 校验（CI 同款）
cd apis/contracts
buf lint
buf breaking --against '.git#branch=main,subdir=apis/contracts'
bash scripts/check-wire-sync.sh

# 生成客户端（示例：Go / Java 也可用各自 buf 插件）
buf generate proto --template '{"version":"v2","plugins":[{"local":"protoc-gen-go","out":"gen/go"}]}'
```

连接信息（默认，见 `config.example.toml`）：客户端 gRPC `:50051`。
认证：`authorization: Bearer <token>` metadata（Auth 启用集群）。
非 Leader 拒绝（`UNAVAILABLE`）时读取 `coord-leader-hint` metadata 重定向，详见白皮书 §5/§6。

## 版本与变更

- 契约独立版本号：`contracts/v{MAJOR}.{MINOR}.{PATCH}`（git tag），与代码版本解耦。
- 兼容性铁律、废弃策略（≥12 个月）、错误码契约：见白皮书 §3–§5。
- 变更流程（PR Checklist + 公示模板）：见白皮书 §10；
  CI 门禁：`.github/workflows/contract-check.yml`。
