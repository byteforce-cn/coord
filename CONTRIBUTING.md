# 贡献指南

感谢你对 Coord 的关注！本项目目前由 Byteforce Team 维护。

## 开发环境

- **Rust**: 1.98.1（见 `rust-toolchain.toml`）
- **Java**: 21+（仅 `coord-java-sdk` 与 `java-example` 模块需要）
- **Node.js**: 22.x（仅 `coord-ui` 模块需要）
- **构建工具**: Cargo / Maven / pnpm

> **没有 Spring Boot starter。** 本仓库**不提供任何 Spring 接入路径**：请自行 `@Bean`
> （参见 `java-example` 的手写装配），生命周期（`close()`、watch 订阅）需自行管理。

## 构建与测试

```bash
# 构建全部 Rust Crate
cargo build

# 运行全部测试
cargo test

# 代码检查
cargo clippy --all-targets --all-features

# 格式化
cargo fmt --all -- --check
```

## 项目结构

请参考 [README.md](./README.md) 中的项目结构说明。

## 注释规范

注释只写「当前为什么」——不变量、陷阱、契约。**不写**：

- 日期（真实期限与测试数据除外）；
- 会漂移的章节号引用（如「第 3.4 节」）与一次性任务编号——跨模块决策写进 `docs/adr/`，
  注释里引用「（见 ADR-00xx）」；
- 「此前 / 修复前 / 旧版为 X」的变更史（git 历史里已有；注释直述当前规则）。

运行错误消息中的稳定诊断锚点（如 `R-SEC-04`）**逐字保留**——它们与 runbook 和
测试断言联动，不得随意改文案。

## 文档规范

- 描述「系统现在是什么样」的文档放 `docs/production/`（建议头注 Owner / Last verified）；
- 设计决策放 `docs/adr/`；同一事实只在一个文档维护（单一归属），不复制粘贴其他文档内容；
- 新增/修改文档引用后跑 `bash scripts/check-doc-refs.sh`，悬空路径必须为 0。

## 提交规范

- 提交信息使用中文或英文均可
- 建议遵循 conventional commits 格式：`feat:`, `fix:`, `docs:`, `refactor:`, `test:` 等
- 每个提交应聚焦单一变更

## 开发流程

1. 确保 `cargo test` 全部通过后再提交
2. 新功能需包含对应测试

## 行为准则

本项目遵循 [贡献者公约](CODE_OF_CONDUCT.md)。

## 许可证

贡献的代码将采用 [MIT License](LICENSE)。
