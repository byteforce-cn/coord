# coord 运维 Runbook

> Owner: maintainers ｜ Last verified: 2026-10-23

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

- **二进制**：`coord`（`Cargo.toml` 的 `workspace.package.version` 为准，当前 `0.3.5`）。
  版本自检：`coord --version` 必须与 `Cargo.toml:18` 一致。
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

### 3.1 在线拉取单 Region 快照

> 这是**单 Region 的状态机快照**，不是整节点备份；v2 集群的备份口径见 §3.3。

```bash
coord snapshot pull --addr <leader> --output /backup/coord-$(date -u +%FT%H%M%SZ).snap --region 0
```

**预期**：文件非空；`sha256sum` 可计算并归档。

### 3.2 本地导出 / 恢复单 Region（停机）

> 同样只是单 Region 状态机快照（含 TSO/current revision，但不含 Raft log/成员关系）；整节点备份见 §3.3。

```bash
# 导出（直接读本地数据目录）
coord snapshot save --data-dir /var/lib/coord --output /backup/local.snap --region 0
# 恢复（**目标节点必须已停止**）
coord snapshot restore --snapshot /backup/local.snap --data-dir /var/lib/coord --region 0
```

### 3.3 v2 整节点离线备份 / 恢复（整节点备份口径）

v2 的备份范围是 region 0（TSO/元数据/校验索引/shard 表）+ 各数据 Region + Commit
Log shard + Raft log/snapshot。用 `coord backup` 在**所有节点停机后**原样复制一个
已追平节点的数据目录；服务器没停会因 region 0 的 redb 锁直接失败。

```bash
# 备份（--output 必须不存在；清单与数据同一目录，manifest 最后写）
coord backup save --data-dir /var/lib/coord --output /backup/coord-$(date -u +%FT%H%M%SZ)

# 恢复（目标节点停机；空目录直接恢复，覆盖非空目录加 --replace）
coord backup restore --input /backup/coord-<ts> --data-dir /var/lib/coord
```

**预期**：`<output>/coord-backup.json` 存在，`entries` 覆盖 `store.db`、各
`regions/region-*/store.db`、`raft-log/log.db`；恢复输出 TSO high water /
current revision；恢复后 TSO 继续从高水位发放（不复用），Commit Log 行可读
（Watch cursor 续订不出现洞）。

**失败时**：
- `open ... is the server still running?` → 还有节点进程持有数据目录；先停掉再备份。
- `manifest not found` / `size` / `checksum mismatch` → 备份不完整或被改动；重新备份，**不要**手工修清单。
- `target ... is not empty` → 目标数据目录非空；确认节点已停并加 `--replace`
  （旧目录保留为 `*.pre-restore-<ms>`，验证后可删）。
- `TSO high water ... lower than manifest` / `current revision ... lower than manifest` → 备份目录与清单不是同一次备份；停止恢复，重新取备份。

**边界**：
- 这是**停机整目录**备份，含 Raft log/成员关系/根密钥；不是 Raft snapshot。
  单 Region 快照（§3.1/§3.2）仍用于导出/追赶，不承担整节点备份。
- 恢复只支持**同一节点**（node_id 不变）；不承诺把 A 节点目录恢复到 B 节点，
  也不承诺在线（不停机）一致性备份。多节点时每个节点的数据目录分别归档。
- Commit Log 行随目录恢复，窗口内历史可续读；窗口外的旧事件仍按 B-KVT-4
  返回 `HISTORY_UNAVAILABLE`。

### 3.4 已知边界（**必须告诉接入方**）

- 快照恢复会**落后于集群当前状态**；恢复单个节点后它靠 raft 日志/快照追赶。
- 对象存储面：快照恢复后落后节点的**本地 chunk 会被清空并 rebuild**，期间 `Get`
  返回 `UNAVAILABLE`（这是**已声明的边界**，不是缺陷）。见
  `docs/production/volume-object-storage.md` 与 `docs/production/ops/boundaries.md`。
- **全集群配置必须一致**（尤其 `security.*` 与 `[multi_raft]`）。
- 整节点备份/恢复（§3.3）要求全部节点停机；在线一致性备份不在当前承诺内。

**演练要求**：至少演练一次「坏一个节点 → 用快照恢复 → `member list` 恢复 → 数据抽样读回」。

> **已做**：**进程内**备份/恢复演练 10/10 通过（覆盖快照流→恢复、导出→清空→导入、
> purge 守卫、重放幂等、kill -9 后 revision 不回退）；v2 整节点离线备份/恢复的 CLI 回归
> （`coord/tests/backup_cli_test.rs`：TSO 不复用、revision 不倒退、Commit Log 历史连续、
> 清单缺失/篡改与运行中目录 fail-closed）。**它不替代本节的「演练要求」**：
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
# 可选：主材料标识（默认 default-kek；轮换时必须随材料换代，见下方「轮换」）
# export COORD_TRANSIT_KEK_ID=gen-2026-10
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

1. **并存窗口**：轮换 = **新标识 + 新材料**——`COORD_TRANSIT_KEK_ID=<新标识>` +
   `COORD_TRANSIT_KEK=<新材料>`；把**旧材料按旧标识**同时注入（同一标识只能对应一份
   材料，标识不换代会让旧记录 / 旧签名落到新材料上）：
   - 环境变量 `COORD_TRANSIT_KEK_OLD`：`<旧标识>:hex64[,...]`；
   - 或 `<data_dir>/transit-kek-old.txt`（每行 `kek_id hex64`，`#` 注释）。
   此时新写入的 DEK 由主材料（新标识）包裹；旧材料密文按记录中的材料标识自动选旧
   材料解密（旧记录无标识 ⇒ 按「主 → 历史」逐材料试解）。
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

**HMAC 轮换（G-TR-3 / ADR-0012）**：

HMAC 与 DEK 共用材料集合，但**不参与 `Rewrap`**；迁移 = 业务按新主材料重散列并回写。

1. **写入侧**：`HmacSign` 必带 `purpose`；把响应的 `key_id` 与签名（散列）一起持久化——
   丢失 `key_id` 的存量在旧材料下线后既不可复算也不会 fail-loud。本改动前产出的（无 key_id / 旧派生公式）存量散列同样不可复算，须按新主材料重散列回写。
2. **并存窗口**：按本节 KEK 步骤 1 注入旧材料（**旧标识 + 旧材料**）；旧散列按 `key_id` 可验；防重按
   `HmacSign(purpose, data, key_id=旧)` 复算变体（DRV-005）。
3. **下线**：确认业务库无引用旧 `key_id` 的存量散列（或已在窗口内重建）后移除旧材料；
   此后按旧 `key_id` 验签/复算报
   `FAILED_PRECONDITION: KEK material '<id>' is not injected …`（fail-loud）。
4. **回退**：重新注入旧材料（同 id / 同材料）即恢复复算；未确认无依赖前不得下线。

**判据（自动化）**：`test_hmac_rotation_window_and_offline_fail_loud`（窗口可验 + 变体复算
+ 下线 fail-loud）、`test_hmac_cross_agent_consistency`、`test_hmac_purposes_are_mutually_unverifiable`、
`test_grpc_hmac_purpose_key_id_and_fail_loud`（错误码）。

### 4.5 PKI 证书轮换与泄露应急（G-PKI-3）

> 适用 `coord.pki.v1`。CA 材料存 server KV（多 agent 共享同一根），agent 只是签发面。
> **没有** CRL/OCSP、也**没有** revoke RPC——泄露收口 = CN 轮换 + 消费侧剔除
> （口径与优先级见 `security.md` §3）。

**常规轮换（到期前处理 `CoordPkiCertExpiringSoon`，窗口默认 6h）**：

```bash
# 1) 看摘要（不含私钥）；coord pki 直连 agent，不依赖 grpcurl
coord pki list --cn orders-api

# 2) 同 CN 换新材料（旧序列作废）——或换新 CN 重新 IssueCert
# 鉴权开启时带 CCT（dev 默认关闭鉴权，可省 -H）；服务名必须是 coord.pki.v1.Pki
grpcurl -plaintext -H "authorization: Bearer $COORD_TOKEN" \
  -d '{"common_name":"orders-api","ttl_seconds":2592000}' \
  <agent>:<port> coord.pki.v1.Pki/RotateCert

# 3) 验证新链
grpcurl -plaintext -H "authorization: Bearer $COORD_TOKEN" \
  -d '{"cert_pem":"<新证书 PEM>"}' \
  <agent>:<port> coord.pki.v1.Pki/VerifyCert
```

TLS/mTLS 集群把 `-plaintext` 换成 `-cacert <ca.pem>`（必要时 `-cert/-key`）；服务名
写错（如 `PKI`）会得到 `unknown service`，不要照抄旧示例。

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

**内部键空间与升级**：CA 与证书私钥落在 `/_pki/`（普通 KV 键）。鉴权开启时
`/_pki/` 与 `/_lease/`、`/_idgen/`、`/_pd/` 同属内部保留命名空间：仅 agent 自身
身份 / root 可经 KV 面访问，普通 `data:kv:read` 一律 `PERMISSION_DENIED`（`Watch`
同样拒绝）。升级到带 SAN 的版本后，旧 dev 数据目录中的无 SAN 记录会在下一次
`IssueCert` 时按「参数一致才幂等」自动替换；也可 `--fresh` 或换 CN 立即重来。

**备份注意**：快照与导出是**全量 KV 对**，包含 `/_pki/` 的 CA 与证书私钥；未开
静态加密时以明文导出。备份文件的保管等级等同信任根私钥。

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

### `CoordKvTTsoRefillFailures` — TSO 批次补发失败

- 指标：`coord_tso_refill_failures_total`、`coord_tso_batch_remaining`、
  `coord_tso_leader_changes_total`。
- 语义：本地 TSO 批次用尽后向 region 0 取新批失败（非 leader / 不可达）。
  v2 `GetTs` 与 `Txn.Begin(start_ts=0)` 走同一来源，写路径会拿不到 `commit_ts`。
- 处置：先确认 region 0 有 leader 且本节点可达（`raft_leader_id`）；等待选主
  完成后客户端自动补发。**不要**手工复用旧批次或回退到 v1 取号——`commit_ts`
  必须全局唯一、单调（ADR-0015 D11.1）。

### `CoordKvTTsoLeaderChurn` — TSO leader 频繁切换

- 指标：`coord_tso_leader_changes_total`（客户端观察到的领导权丢失次数）。
- 影响：换主会丢弃本地未用完批次（只产生空洞，不复用）；已发放但未提交的
  `commit_ts` 会被新 leader 的 safe-ts fence 以 `CommitRejected` 拒绝。
- 处置：与 `CoordLeaderChurn` 同一套排查（时钟/网络/CPU 饥饿）；选主未稳定前
  不要做成员变更，也不要重启整个 region 0 组。

### `CoordKvTSequencerValidationSlow` — Sequencer 单事务校验 > 50ms

- 指标：`coord_kvt_sequencer_validation_us_total / coord_kvt_sequencer_txns_total`
  （5 分钟 rate 的商，即单事务平均校验耗时）。
- 原因：Commit Sequencer 是**单 Raft 组**（ADR-0015 D4），校验含 OCC 读集/区间
  查询；大读集、大范围扫描、热 key 冲突都会抬高成本，直接压低全局提交吞吐。
- 处置：先看 `CoordKvTSequencerConflictStorm` 与事务读集上限
  （`[kv_t].max_read_set` / `max_range`）；让客户端缩小事务、避免全表范围读。
  这是协议串行化点，**不要**用扩 Region 或加副本解决。

### `CoordKvTSequencerConflictStorm` — OCC 冲突率 > 50%

- 指标：`coord_kvt_sequencer_conflicts_total / coord_kvt_sequencer_txns_total`。
- 语义：冲突是正常业务结果（`TXN_CONFLICT`，不写决策、不重试）；持续高冲突
  说明访问模式在争同一批 key，客户端重试会放大负载。
- 处置：定位热 key / write skew（应用侧拆分计数键、缩短事务）；确认客户端用
  **新 `start_ts`** 重跑整个事务，而不是原样重放。**不要**把冲突映射成服务端
  错误或关闭 OCC。

### `CoordKvTWatermarkStall` — Commit Watermark 停滞

- 指标：`coord_kv_t_commit_watermark`、`coord_kv_t_commit_in_flight`、
  `coord_kv_t_commit_watermark_stalls_total`、`coord_kv_t_commit_log_seal_lag_seconds`。
- 语义：`watermark = min(in-flight commit_ts) - 1`（D9）。只要有一个提交在
  in-flight，shard 就不能 seal，Watch 只能看到 watermark 之前的事件。
- 处置：
  1. 看 `commit_in_flight` 是否长期 > 0；定位卡住的 Region/事务（sequencer
     apply、租约清理、迁移 fence）。
  2. 事务 TTL（`[kv_t].txn_ttl_secs`，默认 60s）会回收会话，但**已提交决策**
     仍要 apply 到所有 Region 才会推进水位。
  3. **不要**手工 seal shard、推进 watermark 或删除 Commit Log 文件——那会让
     Watch 永久跳过未 apply 的事件。

### `CoordKvTShardSealLag` — Shard seal 超时

- 指标：`coord_kv_t_commit_log_seal_lag_seconds`、`coord_kv_t_commit_log_shards`、
  `coord_kv_t_commit_log_shards_sealed`、`coord_kv_t_commit_log_shards_gc_total`。
- 语义：未 seal 的 shard 不进入 GC，其 watermark 之后的数据也不会被 Watch 读到；
  超过 5 分钟（D9）会记一次 stall。
- 处置：按上一条检查 in-flight 提交与 Region apply 进度；确认 commit log shard
  组的 leader 存活。**不要**强制 GC 未 seal 的 shard。

### `CoordKvTSplitMergeStuck` — Split/Merge operator 卡住

- 指标：`coord_pd_queue_running`、`coord_pd_queue_pending`、
  `coord_pd_operator_requeued_total`。
- 语义：operator 在 `Running` 超过 15 分钟；常见原因是 drain 等活跃事务
  （会话 TTL 上限 60s）、目标 Region 组未选出 leader、或迁移 fence 未被清除。
- 处置：
  1. 看 `requeued_total` 是否持续增长（超时重认领会保留 intent，不会回滚已
     cutover 的表）。
  2. 查 Region 0 intent（`Prepared`）、目标 Region leader、以及 `DrainRange`
     扇出的不可达节点。
  3. **不要**手工把 operator 标成功/失败或删除 intent；恢复由 executor 在下一
     拍重试（ADR-0015 D6）。

### `CoordKvTWatchHistoryUnavailable` — Watch 历史窗口不足

- 指标：`coord_kv_t_watch_history_unavailable_total`、
  `coord_kv_t_watch_lag_commit_ts`、`coord_kv_t_commit_log_gc_before`。
- 语义：客户端游标早于 `watch_retention`（默认 24h，可配 1h–7d），事件已被 GC；
  这是客户端起点问题，**不是**服务端丢事件。
- 处置：让客户端用 `Range` 重建快照并从新 revision 重新订阅（§7）；
  **不要**调大保留窗口掩盖消费停滞——先修消费端的断线/积压。

### `CoordKvTCommitLogShardsHigh` — Commit Log shard 数量异常

- 指标：`coord_kv_t_commit_log_shards` / `_sealed` / `_gc_total`、
  `coord_kv_t_commit_watermark`、`coord_kv_t_commit_log_seal_lag_seconds`。
- 语义：shard 按 1M 条 / 1h 先到先 seal；24h 保留窗口下稳态约 24 个。长期远高于
  此值 = 老 shard 未被回收（GC 以整 shard 为单位，未 seal 或 watermark 未越过
  `end_ts` 的一律保留），会累积磁盘与内存开销。
- 处置：先修 `CoordKvTWatermarkStall` / `CoordKvTShardSealLag`；确认
  `watch_retention` 没有被误调大。**不要**手工删 Commit Log 文件或强制回收
  未 seal 的 shard——那会让 Watch 永久跳过这些事件。
- outbox 容量：回收水位以下的 staging 行由数据 Region 自己的 raft 自动裁剪
  （`coord_kv_t_commit_log_outbox_rows_trimmed_total`）。该计数不涨而磁盘持续
  增长时，查 `trim commit log outbox failed` 日志与数据 Region 的 leader；裁剪
  是尽力而为的容量优化，不影响 Watch 可读窗口。

## 7. KV-T v2 Watch 订阅排障

### 订阅无事件 / 背压 / 历史不足

- 指标：`coord_kv_t_watch_streams`（当前流数）、`coord_kv_t_watch_batches_total`、
  `coord_kv_t_watch_events_total`、`coord_kv_t_watch_lag_commit_ts`（最近一批的
  `watermark - 批末位置`）、`coord_kv_t_watch_history_unavailable_total`、
  `coord_kv_t_watch_backpressure_total`。
- `coord_kv_t_watch_streams` 持续增长但 `events_total` 不涨：检查消费端是否只建流
  不读（背压计数会同步上升）；服务端通道有界，**不会**为此丢事件。
- `history_unavailable_total` 上升 = 客户端起点早于 `watch_retention`：让客户端
  全量重同步（`Range` 重建快照 + 从新 revision 订阅）；不要调大窗口掩盖问题。
- `lag_commit_ts` 长期高：先看 `coord_kv_t_commit_watermark` 与
  `coord_kv_t_commit_log_*` 是否推进；本地 watermark 不走，watch 只能看到它之前的事件。
- 排障期间可用 `Range` 读当前值校对：“watch 没收到”与“写入没提交”是两类问题
  （后者看 `COMMIT_REJECTED` 与 §6 的 `CoordKvTSequencer*` 告警）。

---

## 8. 演练纪律

- 每次演练产出一份记录：`docs/production/ops/drills/<UTC 时间戳>-<场景>.md`，
  含：谁跑的 / commit / 命令 / 预期 / 实际 / 结论 / 未通过时的后续项。
- 演练必须包含**负控制**（例如：故意用错的分片解封 ⇒ 必须失败）。
- 未演练的条目不得在对外材料里写成「已验证运维」。
