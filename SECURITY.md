# 安全策略

## 报告安全漏洞

如果你发现 Coord 中存在安全漏洞，请**不要**在公开 Issue 中报告。请通过以下方式私下报告：

- 发送邮件至项目维护者

我们将尽快确认并响应你的报告。

## 安全特性

Coord 包含以下安全机制：

- **TLS/mTLS**：gRPC 客户端-服务器及 Raft 节点间通信加密
- **Storage Barrier**：AES-256-GCM 静止数据加密
- **Key Management**：三层密钥体系（Root Key → KEK → DEK），支持密钥轮换
- **Seal/Unseal**：Shamir Secret Sharing（默认 5 分片 / 3 门限）保护主密钥
- **Auth/RBAC**：基于角色的访问控制，令牌认证
- **Zeroize**：密钥材料使用后立即清零

## 支持的版本

| 版本 | 安全更新 |
|:---|:---|
| 0.1.x | ✅ 当前开发版本 |

## 依赖安全

本项目依赖由 Cargo 和 Maven 管理。建议定期运行：

```bash
# Rust 依赖审计
cargo audit

# Java 依赖审计
mvn dependency-check:check
```
