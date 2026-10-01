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
| [ADR-0004](0004-raft-log-reclamation-snapshot-anchored.md) | Raft 日志回收——快照锚定的保留窗口 | proposed |
