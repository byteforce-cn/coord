# 官方 Schema 来源与完整性

- 规范：CNCF Serverless Workflow DSL 1.0.3
- 来源：https://raw.githubusercontent.com/serverlessworkflow/specification/v1.0.3/schema/workflow.yaml
  （经 GitHub Contents API 原始内容通道获取；raw 直连不稳定）
- SHA256：`df3326da907e17e743e9d9e888839481d1557abdf91c592e74f27506ff523f0b`
- 行数/字节：1956 / 77416
- `$schema`：https://json-schema.org/draft/2020-12/schema；`$ref` 全部为内部 `#/$defs/*`（自包含）
- 用途：Spike 4 语料的权威合规判据；实现侧 `parse_and_validate_yaml` 不做此校验
- 许可：上游仓库 Apache-2.0，仅作本次 Spike 证据留档
