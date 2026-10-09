# ADR-0009: dev 模式的 Agent 内建服务集与 dev 专用默认 KEK

- 状态：accepted
- 日期：2026-10-08
- 决策者：维护团队

## 背景

`coord dev` 提供单节点 Server + Agent 一键开发环境（loopback 优先、鉴权关闭；
容器场景见 ADR-0008）。但 agent 一直以裸 `AgentConfig::default()` 构造，
`ServiceConfig::default()` 按 ADR-0001 全关 ⇒ dev 里只有无条件注册的
`handshake`，registry / config_center / lock / idgen 等协调能力全部缺席：
UI 控制台、本地 SDK 示例与集成测试拿不到任何内建服务，「一键 dev」名不副实。

ADR-0001 的「默认关」约束的是**生产默认口径**（`ServiceConfig::default()`
与 TOML 缺省），并明确「显式启用即可用」；`coord dev` 是显式 opt-in 的本地
开发入口（鉴权关闭、无 TLS、启动 WARN），不是生产配置路径。因此可以在 dev
里采用一个显式的服务预设，而不触碰任何默认值。

`transit` 的既有口径是「缺 KEK 材料 ⇒ fail-closed 拒绝启动」（B-SE-2）。
若原样放进 dev 预设，默认的一键启动会直接失败；而 dev 本就是**已知不安全**
的入口（`root`/`root` 默认凭据、明文传输），为其提供一把固定、公开的
**dev 默认 KEK** 与既有姿态一致——只要保证该默认材料**仅** dev 可达。

`replication` 是唯一无法在 dev 工作的服务：跨 Agent ISR 复制，而 dev 是
单 Agent 拓扑，没有复制对端。

## 决定

1. `coord dev` 以显式预设 `ServiceConfig::dev_mode()` 启动 agent：除
   `replication` 外的全部内建服务开启（registry / config_center / lock /
   idgen / leader_election / event_notification / cache / mq / workflow /
   policy / scheduler / circuit_breaker / rate_limiter / feature_flags /
   transit / pki），另有无条件注册的 `handshake`。
2. **dev 专用默认 KEK**（`services/transit.rs` 的 `DEV_DEFAULT_KEK`）：
   「`COORD_TRANSIT_KEK` / `<data_dir>/transit-kek.bin` 均缺失」时回退到该
   固定材料并输出启动 WARN。材料固定 ⇒ 无保密性，只用于本地开发。
3. **回退仅经进程内 builder 开关**：
   `AgentServer::with_dev_default_transit_kek(bool)`（与 ADR-0008 的
   `with_dev_allow_insecure_non_loopback` 同模式；不进 `AgentConfig` serde
   面——`agent` 子命令 / `agent.toml` / 环境变量不可达）。生产路径缺材料仍
   fail-closed 拒绝启动，错误消息与既有判据逐字保留。
4. 预设是**代码面**（进程内构造）：生产默认口径 `ServiceConfig::default()` /
   TOML 缺省仍全关（ADR-0001 的两条不变量测试继续钉住）。
5. 需要在本地测试 `replication` 时，仍走 `coord agent --agent-config` 显式
   配置（自备对端列表）。
6. 不变量：预设与「全开 − {replication}」逐字段相等；端到端锚点 = dev 进程
   的 `coord.plugin.Plugin/List` 清单（含 `builtin` / `healthy` / `status`），
   且 Registry / Config / Lock / Transit（Encrypt+Decrypt 往返）各一次真实
   调用成功。

## 后果

- 一键 dev 的 agent 暴露完整协调能力面（与 README「17 内建服务」清单对齐，
  仅 `replication` 例外），UI 控制台 / SDK 示例 / 本地集成测试可直接使用。
- dev 下用默认 KEK 加密的数据**无保密性且不可移植**：换环境后旧密文解不开
  （fail-loud，不是静默降级）；启动 WARN 明示。生产 agent 行为零变化。
- dev 会多跑若干后台任务（registry / config / lock / …），仅限单节点本地；
  生产形态与默认口径零变化。
- 新增 `ServiceConfig` 字段时，`dev_mode()` 的穷举构造会触发编译错误，
  强制对「新服务的 dev 归属」做出显式决定（开 / 不开 + 理由）。
- 回退路径反转（删 builder / 关开关）⇒ dev 启动即 fail-closed 拒绝（负控制
  已实跑），这是特性而非缺陷。

## 参考

- 代码：`coord-agent/src/service.rs`（`ServiceConfig::dev_mode()`）、
  `coord-agent/src/services/transit.rs`（`DEV_DEFAULT_KEK`、
  `TransitKekMaterial::resolve`）、`coord-agent/src/lib.rs`
  （`with_dev_default_transit_kek` + transit 装配回退分支）、
  `coord/src/main.rs`（`run_dev`）
- 判据：`coord-agent/src/service.rs`
  （`test_dev_mode_preset_enables_all_but_replication`）、
  `coord-agent/tests/agent_dev_transit_kek_test.rs`（默认拒绝 / TOML 不可达 /
  开关正例 + gRPC 往返）、`coord/tests/dev_mode_services_test.rs`
  （Plugin.List 集合相等 + 真实调用）
- 相关：ADR-0001（默认关闭口径不受影响）、ADR-0008（dev 容器放行）、
  `docs/production/ops/boundaries.md` B-SE-2 / B-SE-8

