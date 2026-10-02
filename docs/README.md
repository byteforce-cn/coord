# docs — 文档索引

本目录收录长期维护的架构与生产运维文档。

## 架构决策记录（`adr/`）

跨模块决策的单一定义；撰写与编号规范见 [`adr/README.md`](adr/README.md)。

## 生产文档（`production/`）

| 文档 | 内容 |
|:--|:--|
| [`volume-object-storage.md`](production/volume-object-storage.md) | 对象存储数据面评估与边界 |
| [`ops/runbook.md`](production/ops/runbook.md) | 运维 Runbook |
| [`ops/upgrade.md`](production/ops/upgrade.md) | 升级与版本兼容 |
| [`ops/observability.md`](production/ops/observability.md) | 观测面与告警绑定 |
| [`ops/slo.md`](production/ops/slo.md) | SLO 定义 |
| [`ops/k8s-verification.md`](production/ops/k8s-verification.md) | Kubernetes 部署验证清单 |
| [`ops/dependencies.md`](production/ops/dependencies.md) | 依赖治理与 bincode 退场记录（已完成） |
| [`ops/boundaries.md`](production/ops/boundaries.md) | 范围与「不承诺」边界 |
| [`ops/security.md`](production/ops/security.md) | 安全模型与加固 |
