# ADR-0008: 容器化 dev 模式——dev 非 loopback 绑定的显式放行

- 状态：accepted
- 日期：2026-10-06
- 决策者：维护团队（2026-10-06 方案评审通过后实施）

## 背景

`coord dev`（对标 `consul agent -dev`）提供单节点 Server + Agent 一键开发环境，
但仅支持 loopback 绑定。`coord dev --help` 声称「容器化部署需设为 0.0.0.0，
此时必须显式传 `--allow-insecure`」，实测该路径**必然失败**：

- **Server**：`run_server` 的 R-SEC-03 判定（raft 非 loopback 且既无 raft mTLS
  又无 `security.raft_shared_secret` ⇒ 拒绝启动）不区分 dev 模式；dev 不注入
  任何密钥材料，服务端在端口就绪检查前退出（`Server did not become ready …
  within 30s`）。
- **Agent**：`coord-agent` 的非 loopback 绑定守卫（非 loopback 必须 auth+TLS
  同时开启，与 server 侧同口径）在 dev 下不可能满足——dev 强制关闭鉴权且无 TLS。

因此容器端口映射（要求进程绑定 `0.0.0.0`）无路径，「容器快速启动 dev」只能
变通为未文档化的 Linux `--network host`。

评审基线（决定必须同时满足）：

1. **生产语义不变**：`server` / `agent` 子命令与集群形态的 fail-closed 判定
   （R-SEC-03/04、agent 守卫默认拒绝）保持逐字不变；
2. **显式 opt-in**：新能力仅经既有 `--allow-insecure` 开关开启，启用必有启动 WARN；
3. **配置面最小**：`agent.toml` / 环境变量不得成为旁路入口（防生产误用）。

## 决定

### D1 dev 非 loopback 绑定时 Raft 收敛 loopback

`run_dev` 在 bind 非 loopback 时把 `raft_addr` 收敛为 `127.0.0.1:<grpc+1>`
（监听与通告同址）。dev 是单节点拓扑，Raft 端口无对外用途；收敛后 R-SEC-03
判定逐字不动、无需引入任何密钥材料，容器内也不暴露 raft 端口。

### D2 Agent 旁路 = 进程内 builder 开关，不进配置面

`AgentServer` 增加 `with_dev_allow_insecure_non_loopback(bool)`（与既有
`with_ready_flag` 等 builder 同风格）。守卫判定追加该开关；命中且非 loopback
时输出 WARN。该开关**不进入** `AgentConfig` 的 serde 反序列化面：

- `agent` 子命令路径不调用它 ⇒ 行为不变；
- 配置文件中的同名未知键被忽略 ⇒ 无法开启旁路。

### D3 容器交付与暴露面

`deploy/docker-compose/` 下新增单节点 dev 组合（`docker-compose.dev.yml`），
复用同一镜像与构建（`byteforce/coord:local`）：`command` 为
`dev --bind-addr 0.0.0.0 --allow-insecure`；宿主端口默认只发布到 `127.0.0.1`
（50051 gRPC / 19527 Agent / 50061 UI；19528 metrics 可选）；数据经命名卷持久化，
重置用 `down -v`；不默认 `--fresh`（避免每次重启清库）。

### D4 文档与边界

README（EN/zh-CN）快速开始补充容器 dev；`docs/production/ops/security.md`
补充 dev 放行语义；`docs/production/ops/boundaries.md` 新增边界条目，
记录 agent 守卫的 dev 唯一旁路与「配置文件不可达」事实。

## 后果

- 非 loopback / 容器场景获得一键 dev 环境；CLI 既有承诺变为真实行为。
- 生产不受影响：R-SEC-03/04 与 agent 守卫默认拒绝逐字保留；旁路仅经
  `run_dev` 调用链可达，且带 WARN。
- 新增显式例外面（boundaries B-SE 系列）：撤销任一机制（Raft 收敛或 builder
  接线）⇒ dev 容器正例必红（负控制实跑记录进 PR）。
- dev 的 Raft 端口不再对外；此前非 loopback 路径本就不可用，无既有行为回退。

## 参考

- `coord/src/main.rs`：`run_dev`（地址推导、agent 构造）、R-SEC-03 拒绝块、
  `is_non_loopback_bind`
- `coord-agent/src/lib.rs`：`AgentServer` 非 loopback 守卫、builder 面
- 判据：`coord/tests/dev_insecure_bind_test.rs`（正反用例）、
  `coord/tests/raft_sec03_failclosed_test.rs`（回归）、
  `coord-agent/tests/agent_non_loopback_guard_test.rs`（旁路正例 + 默认拒绝反例）
