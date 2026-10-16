# 审批 v2 UI 影响评估

> Owner: maintainers ｜ Last verified: 2026-10-15

本页记录 v2 人工审批（ADR-0013 D7）对 `coord-ui` 的**影响评估**，不描述已实现的
UI 功能。审批语义（RPC、DSL、授权、幂等、事件、deadline）单一归属
[workflow-semantics.md §7](workflow-semantics.md) 与 `docs/adr/0013-workflow-approval-v2.md`；
本页只写 UI 侧现状缺口与后续里程碑所需面，不复述语义。

## 结论

**本期不实现 UI 审批面**（边界 B-WF-8）：审批面只承诺 gRPC，`coord-ui` 不改代码。
Web 接入需另立里程碑，先建 REST facade、再建页面。

## 现状缺口（可核对）

1. **悬空路由**：`coord-ui/src/components/layout/Sidebar.tsx` 有导航项
   `工作流 → /workflow`，但 `coord-ui/src/routes/` 下无对应路由文件
   （现有 `_app/registry`、`_app/config`）；该入口点击为死链。
2. **无 REST facade**：`coord-ui` 经 `coord-ui/src/api/client.ts` 的 `api`
   （`BASE_URL = "/api/v1"`，命名空间 `auth` / `registry` / `config`）访问后端。
   服务端 BFF（`coord-server/src/bff/`）只挂 config / registry 两组 REST API，
   **没有 workflow**，`api` 中也没有 `workflow` 命名空间。
3. **审批面只在 gRPC**：12 个 RPC 见
   `apis/contracts/proto/coord/workflow/v1/workflow.proto`；Rust / Java SDK 已包装，
   但浏览器 `fetch` 所需的 REST 通路不存在。

## 后续里程碑所需（建议面）

后端 BFF 需补 workflow REST API，逐一映射 12 个 RPC（能力名沿用
`coord:workflow:execute` / `coord:workflow:read`；防重放口径 = 命令体 `idempotencyKey`）：

| UI 能力 | 建议 REST（`/api/v1`） | 后端 RPC |
|:--|:--|:--|
| 我的待办 | `GET /workflow/pending-tasks?pageSize=&pageToken=` | `ListPendingTasks` |
| 审批详情 | `GET /workflow/instances/{id}/approval` | `GetInstanceDetail` |
| 同意 / 驳回 | `POST /workflow/instances/{id}/tasks/{taskId}/approve`、`/reject` | `Approve` / `Reject` |
| 确认 | `POST /workflow/instances/{id}/tasks/{taskId}/acknowledge` | `Acknowledge` |
| 加签 / 减签 | `POST /workflow/instances/{id}/branches/{branchId}/add-sign`、`/remove-sign` | `AddSign` / `RemoveSign` |
| 驳回至节点 | `POST /workflow/instances/{id}/branches/{branchId}/return-to-node` | `ReturnToNode` |
| 转办 / 委托 | `POST /workflow/instances/{id}/tasks/{taskId}/transfer`、`/delegate` | `Transfer` / `Delegate` |
| 撤回 / 终止 | `POST /workflow/instances/{id}/withdraw`、`/terminate` | `Withdraw` / `Terminate` |

前端路由（对齐 Sidebar 现有 `href`）：`_app/workflow/index.tsx`（待办列表）、
`_app/workflow/$instanceId.tsx`（详情与操作）；调用面新增 `api.workflow`。

## 边界

- 本评估**不改** `coord-ui` 代码、不建 facade、不改 `coord:workflow` 能力门禁与鉴权口径。
- Web 接入涉及 CCT/会话如何映射到 gRPC 身份、命令防重放、待办分页规模上界（B-WF-6）
  等，属该里程碑的设计输入；届时应按 [ops/boundaries.md](ops/boundaries.md) 的 B-WF-8
  另立决策，不在本里程碑内定案。
