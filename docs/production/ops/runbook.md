# coord 运维 Runbook（W6-1）

- **日期**：2026-09-21
- **锚点**：`docs/production/production-readiness-plan-2026-09-21.md` §5 W6-1（P-Gate 7）
- **体例**：每条 = **触发 / 命令 / 预期输出 / 失败时怎么办**。
  「命令」一律是别人能重跑的东西；写不出的条目不入本表。
- **反例纪律**：本仓库既有的教训是「机制写好了、没人调用」与「文档说了、实现没做」
  （第四轮 §3.13）。因此本文件里凡引用代码行为处都带 `file:line`，**读到的与写下的必须一致**。

> **当前状态**：本文件是 W6-1 的**首个版本**。下表逐条的「演练」列标注是否已在真实环境
> 演练过。**未演练**的条目按 §7 的纪律只能算「写成」，不能算「可运维」（P-Gate 7 要求
> 演练归档）。演练记录落在 `docs/production/ops/drills/`。

| 小节 | 内容 | 已演练 |
|:--|:--|:--|
| §1 | 启动 / 停止 / 优雅下线 | ⏳ 待演练 |
| §2 | 缩扩容与成员变更 | ⏳ 待演练 |
| §3 | 备份 / 恢复 | ⏳ 待演练 |
| §4 | 密钥轮换（DEK）/ Seal-Unseal | ⏳ 待演练 |
| §5 | compaction | ⏳ 待演练 |
| §6 | 告警处置（逐告警 runbook） | ⏳ 待演练 |

---

## §0 通用前置

- **二进制**：`coord`（`Cargo.toml` 的 `workspace.package.version` 为准，当前 `0.2.0`）。
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

## §1 启动 / 停止 / 优雅下线

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

## §2 缩扩容与成员变更

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

## §3 备份 / 恢复

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

---

## §4 密钥轮换与 Seal / Unseal

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

---

## §5 compaction

```bash
# 触发一次压缩（revision 必须 ≤ 当前 revision）
coord compact <revision> --addr <leader>
```

**预期**：`mvvccompact` 相关日志；磁盘的 changelog/tombstone 被物理删除。

**注意**：`raft.snapshot_logs_since_last = 0` 时**自动快照被禁用 ⇒ raft 日志永不回收**
（`LogStore::purge` 依赖持久快照）。启动会对该配置打 WARN（`coord/src/main.rs` 的
`snapshot_logs_since_last == Some(0)` 分支）。见 `boundaries.md` §2。

---

## §6 告警处置（runbook 绑定）

> 与 `monitoring/prometheus-rules.yml` 的 `runbook_url` 一一对应（W5-3）。
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

> W5-4。「死亡」有两种形态，**判据不同**（不要用同一个数字覆盖两者）：
>
> | 形态 | 指标 | 正常行为 | 故障判据 |
> |:--|:--|:--|:--|
> | 循环型（`workflow_subflow_scanner`） | `coord_agent_workflow_loops_finished` | **永不结束** | `> 0` |
> | 一次性（每个实例的 `drive`） | `coord_agent_workflow_worker_faults_total` | 挂起/终态即返回 | 增量为正（= 结束了但**没跑到最后一行**） |
>
> 语义来源：`coord_core::workflow::runtime::WorkerLiveness`。**不要**把一次性任务的
> 正常结束当故障 —— 那会让这条告警长期噪声化，最后没人看。

**后果（为什么是 critical）**：该实例的 `drive` 死了 ⇒ 它**永久停在非终态且没有驱动者**。
任何后续 `Signal`/事件都不会再推进它，而客户端看到的是"实例还在 Running"。这与 F-27
（lease 过期 revoke 静默丢失）是同一种形态：**没有路径会告诉你**。所以本告警不设
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
4. 若 `recent_faults` 持续增长（同一 agent 反复死）：按 P1 缺陷立案，附
   agent 日志与实例 id；这是驱动循环里的**真缺陷**，不是运维问题。
5. 事后：确认 `coord_agent_workflow_loops_finished` 回落为 0（循环型死亡**不会**
   自愈：它是 `tokio::spawn` 出去的死句柄 ⇒ 必须重启 agent 进程）。

---

## §7 演练纪律

- 每次演练产出一份记录：`docs/production/ops/drills/<UTC 时间戳>-<场景>.md`，
  含：谁跑的 / commit / 命令 / 预期 / 实际 / 结论 / 未通过时的后续项。
- 演练必须包含**负控制**（例如：故意用错的分片解封 ⇒ 必须失败）。
- 未演练的条目不得在对外材料里写成「已验证运维」。
