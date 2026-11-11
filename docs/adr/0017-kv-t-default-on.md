# ADR-0017: KV-T GA 后默认启用：`[kv_t].enabled` 改为三态

- 状态：accepted
- 日期：2026-11-11
- 决策者：维护团队（KV-T G6 签字见 `docs/production/ops/kv-t-ga-gate.md` §4）

## 背景

ADR-0001 第 4 条要求：能力面要改回默认开，必须新增 ADR。ADR-0015
（「与既有 ADR 的关系」）把 KV-T 钉在「GA 前必须显式启用，不得静默替换 v1」。

KV-T 已于 2026-11-11 签字 GA（G0–G6 全部有可复跑证据，含 72h Jepsen run）。
继续把它当实验开关默认关闭，会让新装的多 Region 集群默认落在**已冻结的** v1 数据面上
（B-KVT-2）——GA 的产品面反而要手工打开。

但 `enabled` 不能简单改成 `true`：KV-T 的硬前置是 `multi_raft.enabled = true` 且
`multi_raft.pd.enabled = true`，而多 Region 装配需要显式 `initial_regions`
（Region 表平铺整个 keyspace、本节点须在 `cluster.initial_nodes` 里），没有合理默认值。
字面改 `true` 会让所有未写 `[multi_raft]` 的配置（单 Raft 部署、`config.example.toml`、
大量测试夹具）在 `Config::validate` 处启动即失败——默认值本身不自洽。

又因为 v2 会冻结 v1 数据面，自动启用只能在 **greenfield** 上成立：已迁移的 v1 集群
（`multi_raft.legacy_migration` 跑过、Region 里已有数据）被隐式切成 v2 等于静默冻结
其数据面，违反 ADR-0001 的「不得静默替换 v1」。

## 决定

1. `[kv_t].enabled` 是三态：
   - **未写** = 自动：`multi_raft.enabled && multi_raft.pd.enabled` 时为 `true`，
     否则 `false`（单 Raft 集群行为字节级不变）；
   - **`true`** = 强制启用：前置缺失仍 fail-closed，错误点名配置项；
   - **`false`** = 强制关闭：v2 一律 `FAILED_PRECONDITION`（v1 不变）。
2. 解析只在 `Config::kv_t_enabled()` 一处；装配点不得各自解释 `Option`。
3. 自动启用只对 greenfield 成立：迁移标记已存在的集群拒绝隐式启用，启动失败并要求
   显式二选一。判据是既有的 raft 复制迁移标记，不新增持久化状态。
4. 数值默认值不变（仍逐字等于 ADR-0015 D9）；本 ADR 只改 `enabled` 的缺省语义。

## 后果

- 新装的多 Region + PD 集群开箱即 v2；单 Raft 部署默认仍是 v1。
- v1 多 Region 运维必须显式 `[kv_t].enabled = false`：`config.example.toml`、
  `coord/tests/multi_raft_process_test.rs`、Jepsen `--regions` 生成器都已按此钉住。
- 升级语义：单 Raft 集群零改动；已迁移的 v1 集群启动即被拒绝，错误给出两条可执行
  写法（`true` 转 v2 / `false` 留 v1），不会静默冻结数据面。
- 修订 ADR-0015「与既有 ADR 的关系」中「GA 前必须显式启用」的口径；KV-T 语义
  （D1–D13）本身不变。
- `coord_kv_t_enabled` 指标与所有装配分支使用有效值，自动启用可观测。
- 钉住的不变量：`config::tests::test_kv_t_default_is_auto_by_cluster_shape`（单 Raft
  自动关、多 Region + PD 自动开、缺 PD 自动关）、
  `config::tests::test_kv_t_enabled_requires_multi_raft_and_pd`（显式 `true` 仍校验前置）、
  `config::tests::test_kv_t_auto_prerequisite_error_names_the_opt_out`（错误点名退回 v1 的
  写法）、`tests::test_kv_t_implicit_activation_refused_on_migrated_v1_cluster`（迁移集群
  拒绝隐式启用）。

## 参考

- ADR-0001（默认关闭口径与「改回默认开须新增 ADR」）；ADR-0015（KV-T 语义与 D9 数值）。
- `docs/production/ops/kv-t-ga-gate.md`（GA 签字与证据）；`boundaries.md` B-KVT-2（v1 冻结）。
- `coord/src/config.rs`（`KvTConfig` / `Config::kv_t_enabled`）、`coord/src/main.rs`
  （装配分支与启动闸）、`config.example.toml`、`jepsen/src/jepsen/coord/db.clj`。
