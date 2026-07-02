# 贡献指南

感谢你对 Coord 的关注！本项目目前由 Byteforce Team 维护。

## 开发环境

- **Rust**: 1.93.0（见 `rust-toolchain.toml`）
- **Java**: 21+（仅 `coord-spring-boot-starter` 与 `java-example` 模块需要）
- **Node.js**: 22.x（仅 `coord-ui` 模块需要）
- **构建工具**: Cargo / Maven / pnpm

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
