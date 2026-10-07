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

## 临时文件（强制）

- **所有临时文件、草稿脚本、日志、下载产物一律放系统 `/tmp` 下**（建议 `/tmp/coord-*` 前缀，
  如 `/tmp/coord-pr/`、`/tmp/coord-tmp/`）。
- **禁止**在仓库工作区内创建 `tmp/`、`scratch/` 等临时目录或在其中写文件——会污染源码树，
  干扰工作区视图与管理（即使已被 .gitignore 忽略也不允许）。
- 仓库 `tmp/` 下**既有**的历史脚本/产物保持原位，**不要迁移、删除或改名**（避免与既有流程/会话冲突）；
- 构建产物按各自约定入 `target/`（已 ignore）；其余大文件、备份、探测结果一律放 `/tmp`。

