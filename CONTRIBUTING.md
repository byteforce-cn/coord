# 贡献指南

感谢你有兴趣为 coord 做出贡献！在提交 issue 或 PR 之前，请先阅读以下内容。

## 行为准则

本项目遵循 [贡献者公约行为准则](CODE_OF_CONDUCT.md)，参与即表示你同意遵守该准则。

## 报告问题

在提交新 issue 之前，请先搜索现有 issue 确认问题尚未被记录。

提交 bug 报告时请包含：

- 操作系统及版本
- Rust 工具链版本（`rustc --version`）
- coord 版本或 commit hash
- 最小可复现步骤
- 预期行为与实际行为

## 提交 Pull Request

1. Fork 本仓库并基于 `main` 分支创建特性分支：
   ```bash
   git checkout -b feat/your-feature
   ```
2. 遵循现有代码风格；运行格式化和 lint 检查：
   ```bash
   cargo fmt --all
   cargo clippy --all-targets --all-features -- -D warnings
   ```
3. 确保所有测试通过：
   ```bash
   cargo test --workspace
   ```
4. 每个 commit 信息遵循 [Conventional Commits](https://www.conventionalcommits.org/) 规范：
   ```
   feat(pki): add OCSP stapling support
   fix(raft): correct election timeout jitter range
   ```
5. 提交 PR 时填写完整描述，关联相关 issue（`Closes #123`）。

## 开发环境要求

| 工具 | 最低版本 |
|------|---------|
| Rust | 1.93.0 |
| protoc | 3.x |
| Docker + Compose | v2 |

Rust 工具链版本固定在 `rust-toolchain.toml`，首次 `cargo build` 会自动安装。

部分 crate（`coord-core`、`coord-proto`）来自 Byteforce 私有 Cargo registry，外部贡献者无法直接拉取，相关测试在公开 CI 中会被跳过，维护者会在合并前验证。

## 代码组织

```
crates/coord-server   gRPC + HTTP 服务端
crates/coord-ctl      命令行管理工具
sdk/rust              Rust SDK
sdk/java              Java SDK（Maven）
sdk/go                Go SDK
proto/coord/v1        protobuf 定义
e2e/                  集成测试套件
ui/console/           React 运维控制台
```

## 许可证

提交 PR 即表示你同意将该贡献以 [Apache-2.0 许可证](LICENSE) 授权。
