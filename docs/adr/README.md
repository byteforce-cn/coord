# ADR — 架构决策记录

本目录以 ADR（MADR 风格）**单一定义**跨模块、长期有效的架构决策。
代码与文档引用这些决策时指向对应 ADR（如「见 ADR-0001」），
**不得**使用会漂移的章节号引用或一次性任务编号。

## 何时写 ADR

- 一个决策被多处引用，且长期约束实现（默认开关口径、边界声明、fail-closed 策略等）；
- 修正或推翻既有 ADR：新增 ADR 并注明「超越 ADR-xxxx」，旧 ADR 状态改为 superseded。

## 状态

`proposed` / `accepted` / `deprecated` / `superseded by ADR-xxxx`

## 落档格式

复制 `0000-template.md` 起新文件，编号四位递增。最小字段：
标题、状态、日期、背景、决定、后果、参考。

## 索引

| 编号 | 标题 | 状态 |
|:--|:--|:--|
| [ADR-0001](0001-default-off-until-production-verified.md) | 默认关闭：未经生产验收的能力面（显式启用即可用） | accepted |
| [ADR-0002](0002-cross-node-commit-atomicity-boundary.md) | Cache 跨节点提交原子性边界 | accepted |
| [ADR-0003](0003-read-path-convergence.md) | 读路径收敛：配置与发现必须走门面 API | accepted |
| [ADR-0004](0004-raft-log-reclamation-snapshot-anchored.md) | Raft 日志回收——快照锚定的保留窗口 | accepted |
| [ADR-0005](0005-bincode-replacement-codec-selection.md) | bincode 退场 P1：替代序列化格式选型（postcard） | accepted |
| [ADR-0006](0006-bincode-p2-write-path-migration.md) | bincode 退场 P2：写路径 V2 迁移与剩余直写面信封化 | accepted |
| [ADR-0007](0007-raft-rpc-payload-codec-marker.md) | bincode 退场 P2-RPC：Raft 载荷编码标记（单独评审） | accepted |
| [ADR-0008](0008-dev-container-mode.md) | 容器化 dev 模式：dev 非 loopback 绑定的显式放行 | accepted |
| [ADR-0009](0009-dev-mode-builtin-services.md) | dev 模式的 Agent 内建服务集与 dev 专用默认 KEK | accepted |
| [ADR-0010](0010-eis-consumer-gaps-full-landing.md) | 全量承接 EIS 消费方能力缺口（G-*）：方向、批次与治理 | accepted |
| [ADR-0011](0011-consumer-audit-batch-2.md) | 消费方审计第二批（#44–#53）：配置位点、缓存键空间、调度回读、开关管理面与 SDK 依赖治理 | accepted |
| [ADR-0012](0012-transit-hmac-purpose-bound-keys.md) | Transit HMAC 用途化密钥与轮换窗口（G-TR-3） | accepted |
| [ADR-0013](0013-workflow-approval-v2.md) | 工作流引擎 v2：中国式审批能力落地（状态模型/并发/事件/恢复/契约） | proposed |
| [ADR-0014](0014-approval-consume-closure.md) | 审批面消费闭环：动态席位来源 / 服务代终端用户 / 一体化启用 | accepted |
| [ADR-0015](0015-kv-t.md) | KV-T：全局时间戳、serializable 跨 Region 事务与严格透明 Watch | accepted |
| [ADR-0016](0016-mq-v2.md) | MQ v2：分区日志、消费组与工作流触发 | accepted |
