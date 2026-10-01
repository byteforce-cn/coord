# AGENTS.md — 给 AI 助手的仓库规范

编辑本仓代码 / 文档时，请遵守以下约定（与 `CONTRIBUTING.md` 一致）：

## 构建与测试

- Rust：`cargo build` / `cargo test`；`cargo clippy --all-targets --all-features`；`cargo fmt --all -- --check`。
- Java（`coord-java-sdk` / `java-example`）与 UI（`coord-ui`）：见 `CONTRIBUTING.md`。

## 注释（.rs / .toml / .yml 等）

- **只写当前「为什么」**：不变量、陷阱、契约。
- **不写**：日期（真实期限/测试数据除外）、会漂移的章节号引用（如「第 3.4 节」）、一次性任务编号、
  「此前 / 修复前 / 旧版为 …」的变更史叙事。
- 跨模块决策写进 `docs/adr/`（单一定义），注释里引用「（见 ADR-00xx）」。
- 运行错误消息中的稳定诊断锚点（如 `R-SEC-04`）**逐字保留**——它们与 runbook / 测试断言联动。
- 不新增 `TODO` / `FIXME`。

## 文档（.md）

- 文档描述当前态（建议头注 Owner / Last verified）；现状文档放 `docs/production/`。
- 设计决策放 `docs/adr/`；同一事实只维护一处（单一归属），不复制粘贴其他文档内容。
- 新增/修改引用后跑 `bash scripts/check-doc-refs.sh`（悬空路径必须为 0）。

## 提交与检查

- 断言消息不得包含一次性任务编号；检查缺失必须 fail-closed（拒绝把「未跑」伪装成「通过」）。
