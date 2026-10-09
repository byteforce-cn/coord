# coord 运维 Runbook

> Owner: maintainers ｜ Last verified: 2026-10-09

- **体例**：每条 = **触发 / 命令 / 预期输出 / 失败时怎么办**。
  「命令」一律是别人能重跑的东西；写不出的条目不入本表。
- **纪律**：凡引用代码行为处都带 `file:line`，**读到的与写下的必须一致**。

> **当前状态**：下表逐条的「演练」列标注是否已在真实环境演练过。**未演练**的条目
> 只能算「写成」，不能算「可运维」。演练记录落在 `docs/production/ops/drills/`。

| 小节 | 内容 | 已演练 |
|:--|:--|:--|
| 1 | 启动 / 停止 / 优雅下线 | ⏳ 待演练 |
| 2 | 缩扩容与成员变更 | ⏳ 待演练 |
| 3 | 备份 / 恢复 | ⏳ 待演练 |
| 4 | 密钥轮换（DEK）/ Seal-Unseal | ⏳ 待演练 |
| 5 | compaction | ⏳ 待演练 |
| 6 | 告警处置（逐告警 runbook） | ⏳ 待演练 |

---

## 0. 通用前置

- **二进制**：`coord`（`Cargo.toml` 的 `workspace.package.version` 为准，当前 `0.2.1`）。
  版本自检：`coord --version` 必须与 `Cargo.toml:15` 一致。
- **配置文件**：全局 `--config <path>`；CLI 参数覆盖配置文件同名字段。
- **数据目录**：`--data-dir`（缺省 `server` = `/var/lib/coord`，`agent` = `/var/lib/coord-agent`）。
- **鉴权**：开启了 `security.auth_enabled = true` 的集群，**所有管理命令**都需要管理员 CCT：

  ```bash
  # 取管理员 CCT 并落盘（后续命令自动携带、到期前自动续期）
  coord auth login root
  # 或一次性：导出到环境变量
  export COORD_TOKEN="$(coord auth login root --token-only)"
  ```

- **TLS**：集群启用了 TLS 时，所有 CLI 命令都要带 `--tls-ca <ca.pem>`
  （mTLS 再加 `--tls-cert/--tls-key`）。**fail-closed**：只给 `--tls-cert/--tls-key`
  而不给 `--tls-ca` 会直接报错退出（`coord/src/main.rs:126-152` 的 `build_cli_tls`）。
- **观测面**：
  - Server：`GET http://<http_addr>/healthz`（存活，永远 200）、`/ready`（就绪，未选主时 **503**）、
    `/metrics`（Prometheus）。
  - Agent：`GET http://<agent http_addr>/health`（见 `coord-agent/src/health.rs`）。

---

## 1. 启动 / 停止 / 优雅下线

### 1.1 首个节点（bootstrap）

```bash
coord server --config /etc/coord/coord.toml --bootstrap
```

`/etc/coord/coord.toml` 至少需要：`[node] id`、`[network] grpc_addr/raft_addr`、
`[cluster] bootstrap = true`、`[storage] data_dir`、`[security] auth_enabled/auth_root_key`。

**预期**：日志出现 `Raft inter-node TLS enabled`（配了 TLS 时）与
`User 'root' authenticated` 相关初始化；`GET /ready` 返回 `200 {"status":"READY"}`。

**失败时**：
- `/ready` 恒 503 → 未选出 leader。查 `raft_leader_id` 指标、节点间 raft 端口连通性、
  `security.raft_shared_secret`（配了 raft mTLS/共享密钥的集群，密钥不一致会互相拒绝）。
- 启动即退出且报 TLS 文件读取失败 → `--tls-*` 路径/权限问题（fail-closed，不会降级明文）。
- 启动即退出且报 `R-SEC-04`（`auth_enabled with non-loopback grpc bind …`）→ 鉴权开启 +
  gRPC 绑定非 loopback 但没配 `[security] tls_cert/tls_key`。配置证书（mTLS 另配
  `tls_ca`），或**仅限 dev/test** 显式 `allow_plaintext_remote = true`（默认 false，
  会 WARN；生产不得开启）。

### 1.2 后续节点（join）

```bash
coord server --config /etc/coord/coord.toml --join <leader-grpc-addr>
```

**预期**：`coord member list --addr <leader>` 里出现新节点，状态由 `Learner` 转 `Voter`
（`coord member promote`）。

### 1.3 优雅停止

对进程发 `SIGTERM`（**不要**先 `SIGKILL`）：

```bash
kill -TERM <pid>
```

**预期**：节点先**移交 leader** 再退出；退出码 0。K8s 侧由
`terminationGracePeriodSeconds: 60` 兜底（`deploy/k8s/statefulset.yaml`）。

**失败时**：若 60s 内未退出，检查是否有长事务/大快照在传；
强制 `SIGKILL` 后该节点重启会从快照 + 日志恢复，但**会丢失未 fsync 的尾部**（raft 语义）。

---

## 2. 缩扩容与成员变更

```bash
# 查看
coord member list --addr <any-node>
# 扩容（新节点进程先用 --join 起来，再 promote）
coord member add    --addr <leader> --id 4 --node-addr 10.0.0.4:50051
coord member promote --addr <leader> --id 4
# 缩容
coord member remove --addr <leader> --id 4
```

**预期**：`member list` 的成员数与 raft 配置一致；`remove` 后该 id 不再出现在列表。

**失败时**：
- `add` 报 not leader → 目标节点不是 leader，换一个节点重试（错误里带 leader hint）。
- `remove` 后 quorum 不足 → **先确认剩余成员数 ≥ 3 或为奇数**；2 节点集群缩到 1 节点会短暂无 quorum。

> ⚠️ **不得在成员变更进行中同时做 compaction / 快照恢复**。

---

## 3. 备份 / 恢复

### 3.1 在线拉取快照（推荐）

```bash
coord snapshot pull --addr <leader> --output /backup/coord-$(date -u +%FT%H%M%SZ).snap --region 0
```

**预期**：文件非空；`sha256sum` 可计算并归档。

### 3.2 本地导出 / 恢复（停机）

```bash
# 导出（直接读本地数据目录）
coord snapshot save --data-dir /var/lib/coord --output /backup/local.snap --region 0
# 恢复（**目标节点必须已停止**）
coord snapshot restore --snapshot /backup/local.snap --data-dir /var/lib/coord --region 0
```

### 3.3 已知边界（**必须告诉接入方**）

- 快照恢复会**落后于集群当前状态**；恢复单个节点后它靠 raft 日志/快照追赶。
- 对象存储面：快照恢复后落后节点的**本地 chunk 会被清空并 rebuild**，期间 `Get`
  返回 `UNAVAILABLE`（这是**已声明的边界**，不是缺陷）。见
  `docs/production/volume-object-storage.md` 与 `docs/production/ops/boundaries.md`。
- **全集群配置必须一致**（尤其 `security.*` 与 `[multi_raft]`）。

**演练要求**：至少演练一次「坏一个节点 → 用快照恢复 → `member list` 恢复 → 数据抽样读回」。

> **已做**：**进程内**备份/恢复演练 10/10 通过（覆盖快照流→恢复、导出→清空→导入、
> purge 守卫、重放幂等、kill -9 后 revision 不回退）。**它不替代本节的「演练要求」**：
> 坏节点恢复 + 对象存储 + 多节点仍是 ⏳ 未做。

---

## 4. 密钥轮换与 Seal / Unseal

### 4.1 首次初始化分片（bootstrap 之后立刻做）

```bash
coord security init-seal --n 5 --k 3 --output-dir /secure/shares
```

**预期**：输出 5 个分片文件；**分片离线保存**，不得与数据同机。

### 4.2 封存 / 解封

```bash
coord security seal   --addr <node>
coord security unseal --addr <node> --shares /secure/shares/1 /secure/shares/2 /secure/shares/3
```

**预期**：`seal_status` 指标由 1 变 2（sealed）；解封后回到 1，读写恢复。
告警 `CoordSealedNodeServing` 触发即表示「封存了但仍在提供写流量」——**配置错误**。

### 4.3 DEK 轮换

```bash
coord security rotate-keys --addr <node>
```

**预期**：轮换后旧数据仍可读（三层密钥体系 Root→KEK→DEK），新写入用新 DEK。

**失败时**：轮换是**幂等**的，可重试；失败不影响既有读写。

### 4.4 Agent 侧 `transit` 的 KEK 注入与轮换

> 与 4.1–4.3 的 **server** 侧三层密钥（Root→KEK→DEK，分片/Barrier）**不是同一套**。
> 这一节管的是 `coord-agent` 的 `coord.transit.v1` 信封加密。

**注入（启用 `transit` 的前置条件，缺则 agent 拒绝启动）**：

```bash
# 方式 A：环境变量（hex64 = 32 字节；多 agent 必须同一值）
COORD_TRANSIT_KEK=$(openssl rand -hex 32); export COORD_TRANSIT_KEK
# 方式 B：密钥文件（32 字节原始材料；每个 agent 的 data_dir 下，建议 0600）
head -c 32 /dev/urandom > /var/lib/coord-agent/transit-kek.bin
chmod 600 /var/lib/coord-agent/transit-kek.bin
```

**预期**：agent 正常启动（`services.transit = true` 时日志里不再出现
"KEK injection failed"）。

**失败时（必须如此）**：进程**非 0 退出**，stderr/日志里含
`transit service is enabled but no KEK material was injected: set COORD_TRANSIT_KEK …`
—— 这是**故意的 fail-closed**，不是故障。修法二选一：注入材料，或
`services.transit = false`。**不要**去改代码回落旧派生路径（该路径已删除）。

> 本节适用于生产 `agent` 子命令。`coord dev` 是唯一例外：未注入材料时回退到
> 内建 dev 默认 KEK（启动 WARN，数据无保密性；`agent` 子命令 / `agent.toml`
> 不可达该回退，见 ADR-0009）。

**轮换（G-TR-1 多材料流程，含回退路径）**：

KEK 更换分三步，**在完成迁移前不得移除旧材料**：

1. **并存窗口**：主材料与新注入方式不变（`COORD_TRANSIT_KEK`），把**旧材料**
   同时注入：
   - 环境变量 `COORD_TRANSIT_KEK_OLD`：`kek_id:hex64[,kek_id:hex64...]`；
   - 或 `<data_dir>/transit-kek-old.txt`（每行 `kek_id hex64`，`#` 注释）。
   此时新写入的 DEK 由主材料包裹；旧材料密文按记录中的材料标识自动选旧材料解密
   （旧记录无标识 ⇒ 按「主 → 历史」逐材料试解）。
2. **迁移存量**：对仍有效的存量 DEK 调 `Transit/Rewrap`（管理路径）：
   旧材料解出 DEK → **主材料重包**（新 nonce / 新 dek_id，材料标识更新为
   主材料）；返回 `new_dek_id` 与 `kek_id`。此后旧材料对该条已无依赖。
3. **下线旧材料**：确认无存量后，从注入集合移除旧材料并重启 agent。

**回退路径**：任一步出问题都可退回「新旧并存」——只要旧材料仍在注入集合，
旧密文（含未 rewrap 的）仍可解密。移除旧材料后旧密文解密报
`KEK material '<id>' is not injected`（fail-loud，不静默降级）。

**演练（预期 / 判据）**：

- 预期：并存窗口内旧材料密文可解密；`Rewrap` 后记录 `kek_id` 变为主材料且仅主
  材料可解；移除旧材料后未迁移的旧密文**必须**解密失败（负控制）。
- 判据（自动化）：`coord-agent` 单测
  `test_multi_material_decrypt_window_and_rewrap`（含负控制）、
  `test_legacy_record_without_material_id_falls_back_to_try_all`、
  `test_keyring_old_material_parsing_rules`。

### 4.5 PKI 证书轮换与泄露应急（G-PKI-3）

> 适用 `coord.pki.v1`（agent 内置 CA）。**没有** CRL/OCSP、也**没有** revoke
> RPC——泄露收口 = CN 轮换 + 消费侧剔除（口径与优先级见 `security.md` §3）。

**常规轮换（到期前处理 `CoordPkiCertExpiringSoon`，窗口默认 6h）**：

```bash
# 任何 gRPC 客户端均可，下例示意 RPC 面
# 1) 看摘要（不含私钥）
grpcurl -d '{"common_name":"orders-api"}' <agent>:<port> coord.pki.v1.PKI/ListCerts
# 2) 同 CN 换新材料（旧序列作废）——或换新 CN 重新 IssueCert
grpcurl -d '{"common_name":"orders-api","ttl_seconds":2592000}' <agent>:<port> coord.pki.v1.PKI/RotateCert
# 3) 验证新链
grpcurl -d '{"cert_pem":"<新证书 PEM>"}' <agent>:<port> coord.pki.v1.PKI/VerifyCert
```

**预期**：`RotateCert` 后 `GetCertByCN` 返回新 serial / 新密钥材料，
`VerifyCert` `valid=true`；`ListCerts` 可见 active（新）+ retired（旧）并存
（滚动验签窗口）。

**泄露应急（严格按此顺序）**：

1. **先剔除信任**：把受影响 CN 从**消费侧** mTLS 授权 / 校验白名单移除——
   即刻生效，不依赖 CA（CA 侧无法阻止不校验本 store 的消费方继续接受旧
   证书）；
2. **再作废材料**：`RotateCert`（同 CN）或换新 CN `IssueCert`；若需按序列号
   恢复签名能力用 `RenewCert(serial)`；
3. **负控制验证**：用旧证书做一次验证/连接——**必须失败**；
4. **归档**：CN / 旧 serial / 新 serial / 时间 / 执行人（演练记录按 §7）。

**判据（自动化）**：`coord-agent/src/pki.rs` 单测
`test_rotate_cert_replaces_expired_record`、
`test_concurrent_rotate_no_lost_update`、
`coord-agent/tests/agent_pki_test.rs::test_rotate_then_list_returns_active_and_retired`
（active/retired 并存）；到期观测：`test_cert_expiry_snapshot_counts_window`。

---

## 5. compaction

```bash
# 触发一次压缩（revision 必须 ≤ 当前 revision）
coord compact <revision> --addr <leader>
```

**预期**：`mvvccompact` 相关日志；磁盘的 changelog/tombstone 被物理删除。

**注意**：raft 日志回收是**快照锚定**的（ADR-0004）：缺省每 5000 条自动快照、并按
保留窗口 `raft.max_in_snapshot_log_to_keep`（默认 1000）回收已入快照的日志；
`raft.snapshot_logs_since_last = 0` = 手动快照模式 ⇒ **日志不回收**（启动 WARN，
`coord/src/main.rs` 的 `snapshot_logs_since_last == Some(0)` 分支）。可复现口径：
`coord-server/tests/raft_log_reclamation_test.rs`（有界实验 + 负控制）。见
`boundaries.md`。

---

## 6. 告警处置

> 与 `monitoring/prometheus-rules.yml` 的 `runbook_url` 一一对应。
> 每条告警**必须**能在本表里找到同名小节。

### `CoordLeaderChurn` — 3 分钟内 leader 变化 ≥ 2 次

1. 看 `/metrics` 的 `raft_leader_id` 序列与各节点日志的选举原因。
2. 常见原因：时钟漂移、raft 端口丢包、节点 CPU 饥饿（`pause` 类故障）。
3. 处置：先稳定网络/资源；**不要**在选举风暴中做成员变更（会放大）。

### `CoordFollowerApplyLag` — follower apply 落后 commit > 1000 且持续 5m

1. 看该 follower 的磁盘 IO 与 `coord_dead_background_tasks`。
2. 若是有界滞后（快照传输中）→ 等；若持续增长 → 该 follower 可能需要重建
   （停掉、清数据目录、`--join` 重新加入）。

### `CoordNoLeader` — 集群 60s 无 leader

1. 确认 quorum：`member list` 统计可达成员数。
2. 若 < 多数派 → 恢复节点/网络优先；**不要**重启所有节点（会丢 quorum 恢复的进度）。
3. 恢复后确认 `/ready` 回到 200。

### `CoordDiskWatermarkHigh` / `CoordDiskWatermarkCritical`

- 可用率 < 15% / < 5%。< 5% 时写请求返回 `RESOURCE_EXHAUSTED`（读仍可用）。
- 处置：清理数据目录外的日志、扩容卷、或触发 compaction。
- **切勿**手工删 `data_dir` 里的文件（绕过 raft 状态）。

### `CoordWatchBackpressure` — watch 丢弃率 > 10%

- 说明有订阅者消费不过来（缓冲区满 → **丢最旧 + 合成 `BufferOverflow`**）。
- 处置：找出慢消费者（客户端侧增大消费并发），或让该订阅者改用「KV 轮询 + revision」。

### `CoordAuthDeniedStorm` — 鉴权拒绝率异常

- 可能是扫描/爆破。登录限流会返回 `RESOURCE_EXHAUSTED`（不是 `UNAUTHENTICATED`）。
- 处置：查 `audit` 事件与来源 IP；必要时网络层封禁。

### `CoordSealedNodeServing` — sealed 仍在写

- **配置错误信号**：封存应停止服务，若仍有写流量说明装配顺序不对。
- 处置：立刻 `unseal`（安全前提下）或摘除该节点流量，再排查装配。

### `CoordBackgroundTaskDead` — 受监督后台任务死亡

> 本仓库的取舍是**不自动重启**（任务持有不可重建的本地状态），所以这是**终态**告警。

1. 取任务名：`GET /health?verbose=true` 的 `dead_background_tasks` 字段，或
   `coord_dead_background_task_info{task=...}`。
2. 判断该能力是否在关键路径上（时间轮/快照调度/对象 GC/PD 执行器/auth 维护）。
3. 处置：**人工重启进程**（重启前确认无正在进行的成员变更/恢复）。

### `CoordPluginTrap` / `CoordPluginLoadFailure` / `CoordPluginInvocationErrorRate`

- 插件沙箱触发资源上限（wasm fuel/epoch/memory 或 JS timeout）或被拒绝加载。
- 处置：查该插件的资源上限配置与调用入参；trap 持续说明插件逻辑有界性问题，
  应从 `workflows/plugins` 名单里摘除或修插件。

### `CoordAgentWorkflowWorkerFault` — 工作流后台 worker 死亡

> 「死亡」有两种形态，**判据不同**（不要用同一个数字覆盖两者）：
>
> | 形态 | 指标 | 正常行为 | 故障判据 |
> |:--|:--|:--|:--|
> | 循环型（`workflow_subflow_scanner`） | `coord_agent_workflow_loops_finished` | **永不结束** | `> 0` |
> | 一次性（每个实例的 `drive`） | `coord_agent_workflow_worker_faults_total` | 挂起/终态即返回 | 增量为正（= 结束了但**没跑到最后一行**） |
>
> 语义来源：`coord_core::workflow::runtime::WorkerLiveness`。**不要**把一次性任务的
> 正常结束当故障 —— 那会让这条告警长期噪声化，最后没人看。

**后果（为什么是 critical）**：该实例的 `drive` 死了 ⇒ 它**永久停在非终态且没有驱动者**。
任何后续 `Signal`/事件都不会再推进它，而客户端看到的是"实例还在 Running"。这与
「lease 过期 revoke 静默丢失」是同一种形态：**没有路径会告诉你**。所以本告警不设
auto-resolve 语义，处置是人工的。

**处置步骤**：

1. 取故障清单：agent 日志里 `workflow background worker died without completing`
   一条（含 `recent_faults`，形如 `workflow_drive[<instance_id>]`，最多保留 8 条）。
   指标侧：`coord_agent_workflow_workers_live` 会立刻下降（在飞数减少）。
2. 列出受影响的实例：`Workflow/ListInstances`，筛出长期处于 `Running`/`Waiting`
   且 `updated_at` 已停滞的实例。**注意**：正常等待外部事件（`listen`）的实例
   也会长期 Running —— 用 `updated_at` 与事件到达时间区分，不要一律当故障。
3. 处置（二选一）：
   * **恢复**：对这些实例走 `resume` 路径重新驱动（会重新 spawn 一个 `drive`）；
   * **终止**：若实例已无意义，`Cancel` 后清理。
4. 若 `recent_faults` 持续增长（同一 agent 反复死）：按缺陷立案，附
   agent 日志与实例 id；这是驱动循环里的**真缺陷**，不是运维问题。
5. 事后：确认 `coord_agent_workflow_loops_finished` 回落为 0（循环型死亡**不会**
   自愈：它是 `tokio::spawn` 出去的死句柄 ⇒ 必须重启 agent 进程）。

### `CoordMqConsumerLag` — MQ 消费组滞后

- 指标：`coord_agent_mq_consumer_lag{topic,group,partition}`（lag = next_offset −
  committed，下限 0）、`coord_agent_mq_consumer_offset`、`coord_agent_mq_next_offset`。
- 滞后持续增长 = 生产快于消费：扩容消费者（注意 `Ack` 必须发往分区 Leader，见
  MQ 契约页的消费路由小节）或降低生产速率。
- 滞后停滞但 > 0 = 消费者已停摆：检查消费者进程与错误率。消费位点只在 `Ack`
  成功后前进，**重复投递是 at-least-once 的正常形态**（不要按重复告警）。
- 分区 Leader 查询：`GetTopicLeader`；非 Leader 写路径返回
  `FAILED_PRECONDITION` + `coord-leader-hint` trailer。

### `CoordPkiCertExpiringSoon` — PKI 证书即将到期

- 口径：仍有效且剩余有效期 < `coord_agent_pki_expiry_warn_window_hours`（默认 6h，
  配置项 `expiry_warn_hours`）的 active 证书数；已过期证书**不**计入（到期即换新，
  由下一次 `IssueCert`/`RotateCert` 处理）。
- 处置：对窗口内的 CN 调 `RotateCert`（或按序列号 `RenewCert`）换新（操作步骤
  与泄露应急见 §4.5）；消费方按 `ListCerts` 的 serial/kid 在双密钥重叠窗口内
  滚动验签。
- 若计数长期不降：确认轮换确实执行（换新会改变 active serial）。

---

## 7. 演练纪律

- 每次演练产出一份记录：`docs/production/ops/drills/<UTC 时间戳>-<场景>.md`，
  含：谁跑的 / commit / 命令 / 预期 / 实际 / 结论 / 未通过时的后续项。
- 演练必须包含**负控制**（例如：故意用错的分片解封 ⇒ 必须失败）。
- 未演练的条目不得在对外材料里写成「已验证运维」。
