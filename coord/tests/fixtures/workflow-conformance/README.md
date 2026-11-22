# 工作流定义符合性语料

`coord/tests/workflow_conformance.rs` 的输入：24 份 CNCF Serverless Workflow DSL 1.0.3
JSON Schema 合法（或刻意残缺）的样例定义，用于锁定「接受 / 拒绝」边界——未实现的构造
必须在解析期显式拒绝并点名构造，禁止「接受但语义不同」。

- 规范：CNCF Serverless Workflow DSL 1.0.3
- 许可：语料按本仓 MIT 许可维护；上游规范仓库为 Apache-2.0
- 最后校验：对照上游 `schema/workflow.yaml`（v1.0.3，SHA256
  `df3326da907e17e743e9d9e888839481d1557abdf91c592e74f27506ff523f0b`）逐份体检，
  20-invalid-no-document 为刻意缺少 `document` 的负样例。
