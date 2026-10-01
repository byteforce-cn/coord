# ADR-0001: 默认关闭：未经生产验收的能力面（显式启用即可用）

- 状态：accepted
- 日期：2026-10-01（整理落档；决策自 2026-09-21 起生效）
- 决策者：维护团队

## 背景

`coord-agent` 的服务开关（`ServiceConfig`）曾存在两类缺口：

1. `cache` / `workflow` 代码默认开启，但两者的生产验收未闭合（cache 跨节点提交
   非原子，见 ADR-0002；workflow 持久化/补偿语义缺端到端验收）；
2. `registry` / `config_center` / `lock` / `idgen` / `policy` / `pki` 代码默认
   `true`，但字段是普通 `#[serde(default)]`（TOML 缺省 `false`）——同一个 agent
   「走不走 `--agent-config`」会得到**不同的服务集合**。

另：`transit` 在启用时必须注入 32 字节 KEK 材料（缺失即拒绝启动，fail-closed），
不再满足「启用即可用」。

## 决定

1. **未经生产验收的能力面不得默认开启**：凡验收未闭合、或缺端到端/soak 产物的
   能力面，`ServiceConfig::default()` 一律为 `false`。
2. **显式启用即可用**：默认关不等于不可用；显式开启后服务正常工作，除非启用前置
   条件缺失（如 `transit` 缺 KEK 材料），此时 fail-closed 拒绝启动并给出可诊断错误。
3. **两条路径一致**：每个字段的 `#[serde(default)]` 缺省 ≡ `ServiceConfig::default()`；
   `--agent-config` 的有无不得改变服务集合。
4. **改回默认开须新增 ADR**：能力面补齐验收产物后，若要调整默认值，须新增
   ADR 记录该决定。

## 后果

- `cache` / `workflow` / `transit` / `registry` / `config_center` / `lock` / `idgen` /
  `policy` / `pki` 全部默认 `false`。
- `test_service_config_defaults` 与
  `test_service_config_toml_missing_fields_match_code_defaults` 逐字段钉住不变量；
  改任一侧即红。
- 依赖旧默认值的用例必须显式启用相应服务。

## 参考

- 代码：`coord-agent/src/service.rs`（`ServiceConfig`、`impl Default` 与两条不变量测试）
- 边界：`docs/production/ops/boundaries.md`（B-SE-5）
