# 整改第二轮（对第二轮复核结论的回应）— 2026-09-12

> 对应评审：`docs/coord-review-verification-and-remediation-2026-09-12.md`（第二轮复核，
> 代码基准 `e79f882`）。本轮针对该报告中「新引入回归（§4.4）」「表面/部分修复（§4.2）」
> 「未处理（§4.3）」逐条处置。
>
> 本文件**纳入版本控制**（`.gitignore` 逐层放行 `docs/production/design/`），
> 便于尽调时对照代码与证据。

## 1. 新引入回归（§4.4）——全部修复 + 回归测试固化

| # | 回归 | 处置 | 固化测试 |
|:--|:--|:--|:--|
| 1 | **A1 破坏标准前缀扫描**：`range_end.starts_with(prefix)` 与仓库自有惯用法 `prefix_end()`（末字节 +1，如 `…/ns/` → `…/ns0`）不兼容 → 合法区间扫描全部 403 | 改为**字节区间包含**判定 `key >= P && range_end <= succ(P)`；`scope_safe_byte_prefix()` 对末段字面量补足尾随 `/`（否则 `[P, succ(P))` 内含未授权 key）；遗留权限路径同步（空前缀 = 无上界，此前 `prefix_successor(&[])` 返回 `None` → 误拒） | `interceptor.rs::test_scope_covers_interval_accepts_repo_prefix_scan_idiom`、`test_legacy_permission_range_path_matches_scope_path` |
| 2 | **body 上限 1 MiB < 解码上限 4 MiB** → 1–4 MiB 合法 Put/Txn 被拒 | `MAX_GRPC_DECODING_BYTES = 4 MiB` + 64 KiB 帧开销余量；`coord/src/main.rs` 与服务端解码上限**共用同一常量**，避免两侧口径再次漂移 | `test_a2_scope_body_limit_covers_decoding_limit`、`test_a2_legal_max_body_is_buffered_intact`、`test_a2_oversize_body_is_rejected` |
| 3 | **Java CI 加了门禁但必然红**：`java-example` 10 个集成测试硬连 `localhost:19527`、无守卫，`mvn verify` 必败 | ① 端点改为可覆盖（`AgentEndpoint`，`COORD_AGENT_HOST/PORT`）；② `pom.xml` 默认排除 `*IntegrationTest`/`*AdvancedTest`，新增 `-Pit` profile 专跑真实集群套件；③ 新增 `scripts/ci-java-it-cluster.sh`（起真实 server+agent）与 CI job `java-example-it`；④ **修掉测试自身 8 处错误预期**（见下） | `docs/production/evidence/20260912T112707Z-java-it/`（48/48 通过，首次真实执行） |

### 修正的 Java 集成测试自身缺陷（此前从未执行，故从未暴露）

- **错误的"前缀扫描"惯用法**（16 处）：`range_end = prefix + "\0"` 在 etcd 语义下是
  `prefix` 的**最小后继**，区间 `[prefix, prefix+"\0")` 只含 `prefix` 本身 → 匹配 0 条。
  统一改为 `PrefixScan.end(prefix)`（末字节 +1）。
- **依赖"独占空命名空间"**（反复运行即失败）：新增 `Namespace.unique()`，单键/注册表
  测试改用每次运行唯一前缀。
- **`LeaseRevoke` 不存在租约**：实现是**幂等**的（raft apply 语义，重试安全），
  etcd 会返回 `NotFound` —— 测试改为断言真实语义并在注释中记录该差异。

## 2. 表面/部分修复（§4.2）——逐条收敛

| 项 | 残余问题 | 本轮处置 |
|:--|:--|:--|
| **A3（P0 未闭合）** | 只在 `signing_key_hex` **与** `verifying_key_hex` 都为空时拒绝；推荐配置（只设公钥）下 `hex::decode("")` → 空 HMAC 密钥仍可用，可自签 `roles:["root"]` | ① `coord-core::auth::cct` 新增 `MIN_HMAC_KEY_LEN = 32`，`sign/verify_hmac` 拒绝空/过短密钥，`decode_cct_any` 的 HMAC 分支**无可用密钥材料即报错**（不再"空 key 验签通过"）；② agent 启动校验强化：`signing_key_hex` 非空必须是合法 hex 且 ≥32B，公钥必须 32B，`bootstrap_token` 为空即拒绝启动（RoleCache 永不可能是"启动了但全 403"） | `cct.rs::test_cct_hmac_empty_key_is_refused_not_accepted`、`test_cct_encode_with_empty_key_is_refused`；`lib.rs::short_signing_key_is_rejected`、`enabled_auth_without_bootstrap_token_is_rejected` |
| **A3 连带** | role sync 的出站凭据句柄只被 `plugin-js/wasm` feature + `bootstrap_token` 双重门控填充 → 无插件 feature 的构建永远同步不到角色 | `ensure_plugin_identity_token` / `build_identity_client` 去掉 feature 门控；鉴权开启时显式做一次引导兑换（已兑换则复用缓存，不重复消费一次性令牌）后再 `spawn_role_sync` |
| **C1 TOCTOU** | ID 占用检查与写入之间夹 `timer.insert(..).await` → 并发 grant 同 ID 双成功、互相覆盖 | 检查与插入收进**同一写锁临界区**（临界区内无 await）；冲突时归还定时器 | `lease::tests::test_concurrent_grant_with_same_id_grants_exactly_one`（16 并发 → 恰好 1 成功） |
| **C3 双主窗口 / 误退不参选** | 时延 ≈ 3×(10s + TTL/2) ≈ 45s（TTL=30s）**超过 TTL**；固定 10s 轮询在 TTL<10s 时必然逾期；退位后不再参选 | ① 单次续期超时 `TTL/4`；② 距上次成功确认 ≥ `TTL/2` **或** 连续 2 次失败即退位（双主窗口 ≤ TTL/2 < TTL）；③ 轮询周期自适应 `clamp(TTL/4, 1s..10s)`；④ 退位后带退避**自动重新参选**（`MAX_REELECT_ATTEMPTS=3`，达上限明确告警） | 现有 `services::leader_election` 测试全绿；`campaign_shared` 与 `campaign` 共用实现 |
| **C4 后台续期"假丢锁"** | 只修了对外 `renew`；后台自动续期仍以 `keep_alive` 失败为判据 → 瞬时抖动即删记录、唤醒等待者 | 后台路径改为**以 Server 端锁 key 为判据**：`Ok(Some)` 保留、`Ok(None)` 才删除、`Err`（回查失败）保留并下一轮重试；判据抽成 `renew_action()` | `lock.rs::test_renew_action_*`（3 例：抖动保留 / 回查失败保留 / 明确不存在才删） |
| **B1 默认值自相矛盾** | `default_cache_kv_ttl_secs()` 已改 0，但 `impl Default for AgentConfig` 仍写死 `cache_kv_ttl_secs: 30` → 不带 `--agent-config` 时缓存仍开启 | 改为 `default_cache_kv_ttl_secs()`（0 = 默认关闭，配置路径与 Default 路径一致） |
| **E4 证据链** | `.gitignore` 的 `docs/*` 并未真正放行嵌套文件（Git：被忽略目录内部的文件无法单独放行），证据目录只有 README 占位 | 逐层放行 `docs/production/` → `docs/production/evidence/`、`docs/production/design/`（`git check-ignore -v` 实测：证据/设计文件不再命中，其他 `docs/` 仍被忽略）；**首次产出真实证据产物** `docs/production/evidence/20260912T112707Z-java-it/`（run.log + MANIFEST + sha256sums）；新增 `scripts/collect-evidence.sh java-it` 与 `scripts/evidence-java-it.sh` |

## 3. 未处理项（§4.3）——本轮处置

| 项 | 处置 |
|:--|:--|
| **P0-7 历史读压缩后静默返回错误值** | `get_at_revision` / `range_at_revision` 增加压缩水位守卫：`target_revision <= compacted_revision` → `RevisionCompacted`（与 Watch 回放同口径）；保留窗口内**未被写过**的 key 以"当前状态"补齐（此前被静默漏掉）；T 之后被改而 T 之前历史已被压缩的 key → **报错**而非返回猜测值。修复 `apply_compact` 语义理解错误（删除 `rev < effective`）。测试：3 个新用例（水位拒绝 / 补齐未变 key / 不可复原报错） |
| **README 夸大（3 处）** | ① "Jepsen-verified across kill/pause/partition" → 明确"工程已就位但**尚无产物入仓**，属设计意图而非已认证结论"；② "18 pluggable gRPC services" → **17 内建服务**（`register_native_service` 实测 17 处；`Plugin` 是管理面，单独说明）；③ "72-hour soak" → 改为"长时浸泡方案"并注明无产物。中英文同步；"快速本地收口"注明**不是** Jepsen 运行 |
| **Java SDK 默认明文** | `CoordConfig` 默认**拒绝**向非 loopback 主机建明文通道（fail-closed），loopback 开发场景不受影响；显式 `allowInsecurePlaintext(true)` 可声明接受风险。`ErrorCodeTest` 补齐 `CONFIG_INVALID`（D1 引入新错误码后该断言一直在失败——Java 测试此前从不执行） |
| **P0-5 跨 Region 事务** | **未处理**（已约定本轮保持单 Region），不在本文件声称范围内 |

## 4. 诚实声明：仍然存在的缺口

1. **C1 apply barrier 残余**：`id_taken` 仍读本地已 apply 视图（无 barrier），
   极端情况下可能把"刚被 apply 但本地视图未刷新"的 ID 判为空闲。本轮只消除
   了进程内 TOCTOU，未引入 apply barrier。
2. **C3 自动重新参选只做了静态验证**：单元测试覆盖决策逻辑，未做"分区→退位→
   恢复→重新当选"的进程级演练。若需要，应挂入 chaos 套件。
3. **Java SDK 的 Mockito 测试未在本机验证**：本机 JDK 25 上 Mockito inline mock
   无法 instrument（ByteBuddy `Could not modify java.lang.Object`），16 个用例在
   本机无法运行。CI `java-sdk` job 固定在 JDK 21 执行——这是它们**首次**真正执行，
   结果以 CI 为准，本文件不预称通过。
4. **Jepsen / soak / chaos 产物仍未入仓**：`docs/production/evidence/` 目前只有
   Java 集成产物。README 已按此收紧口径。
5. **`LeaseRevoke` 与 etcd 语义差异**：本实现对不存在租约返回 OK（幂等），etcd
   返回 `NotFound`。已记录在测试注释，未改实现（改实现需要把状态机的 apply 结果
   回传 RPC 层，属跨层改动）。

## 5. 全量测试暴露出的、**上一轮遗留**的问题（本轮一并修复）

`cargo test --workspace` 首轮执行 1909 passed / **5 failed**，逐条定位如下（说明这些
问题此前没有被任何一次全量运行覆盖过）：

| 失败 | 归因 | 处置 |
|:--|:--|:--|
| `plugin_credentials_process_test::agent_provisioner_grants_match_server_bootstrap_grants` | **上一轮遗漏**：把 `admin:auth:role_list` 加进 server 侧 `AGENT_BOOTSTRAP_CAPABILITY_GRANTS`，却没有同步 agent 侧的 `PROVISIONER_CAPABILITY_GRANTS` → 漂移断言（本来就是为此设的）直接失败 | agent 侧补齐该项（含注释说明为何需要只读的 `role_list`） |
| `agent_auth_integration_test::{test_dual_defense_agent_rejects_first, test_rate_limiter_integration_with_auth_flow, test_regression_cct_roundtrip_all_keys}` | 本轮 A3 引入：夹具 `TEST_KEY`/`ALT_KEY` 是 **31 字节**，低于新下限 32 → `encode_cct` 直接报错 | 夹具改为 32 字节（并在注释里点明这正是"长度校验落到实处才会爆"的坑） |
| `agent_non_loopback_guard_test::test_non_loopback_with_auth_and_tls_allowed` | 本轮 A3 引入：测试开启鉴权但未配 `bootstrap_token` → 启动期即拒绝 | 测试补上 `bootstrap_token` |

## 6. 撤销"失败重跑"后暴露出的测试夹具缺陷（本轮修复）

E6 撤掉了「首次失败降级为 warning 并重跑一次」，于是**夹具层面的竞态**第一次真正
浮出水面。三轮全量运行的失败集合每次都不同（同一提交），归因如下：

| 现象 | 真实原因 | 处置 |
|:--|:--|:--|
| `agent_grpc_test` / `agent_plugin_js_test` / `agent_plugin_grpc_test` / `agent_isr_test` 随机 `ConnectionRefused` | 夹具是「`sleep(200–500ms)` + 一次性 `connect()`」，而 `server.serve()` 是异步 spawn 的，**不保证** sleep 结束时已 bind；并发跑全量套件时几百毫秒根本不够 | 改为有界就绪探测（重试直至可连接，上限 15s，超时才失败）。实测每秒级等待被自动吸收，重复三次稳定通过 |
| `auth_enforcement_test` 随机 `raft write timed out (no quorum?)` | 机器上积压 **30 个**孤儿 `coord server`（进程级套件被中断后遗留，最久 11 小时），8 核 load 被推到 9 | 新增 `scripts/kill-stray-coord-procs.sh`（只清理 `/tmp/.tmp*` 数据目录的测试遗留进程）+ 挂入 CI `test` job 前置步骤 |
| `coord-server/tests/lease_raft_test.rs` 随机 `assertion failed: self.leader.is_none()`（openraft 引擎内部断言）或 `condition not met within timeout: node 2 sees key` | 该文件 3 个用例各自在**同一进程内**跑一整套真实时钟的 3 节点 raft，并行执行时相互抢 CPU → 选举/复制超时 | 文件内加 `RAFT_SERIAL` 串行闸（仅串行化本文件的用例，忽略 mutex 中毒以免连坐） |
| `coord/tests/dev_mode_test.rs` 随机 `NextId ... Unimplemented` | Agent 的插件服务是**异步**挂载的，"TCP 可连接"≠"服务已挂" | 对该 RPC 的 `UNIMPLEMENTED`/`UNAVAILABLE` 做有界重试（上限 15s） |

> 这两类问题**都不是产品缺陷**，但会在门禁上表现为"随机红"。这正是"去掉重跑"的
> 正确代价：把掩盖改成暴露，然后修夹具，而不是把重跑加回来。

## 7. 复验方式（可复现）

### 本轮实测结果（本机 8 核，2026-09-12）

| 门禁 | 命令 | 结果 |
|:--|:--|:--|
| 编译（含全部 target） | `cargo check --workspace --all-targets` | 0 error（本轮改动未引入新 warning） |
| 全量测试 | `cargo test --workspace --no-fail-fast` | **1914 passed / 0 failed（exit=0）** |
| panic 卡口 | `bash scripts/check-panics.sh` | 0 violations |
| Java SDK 单测（JDK21 语义；本机 JDK25 限 Mockito） | `mvn -B test`（`-Dtest=CoordConfigTest,ErrorCodeTest,...`） | 52 passed（Mockito 16 例受本机 JDK 限制，见缺口 §4） |
| Java 集成（真实集群） | `bash scripts/collect-evidence.sh java-it` | **48 passed / BUILD SUCCESS**（产物已入库） |

> 全量跑之前先执行 `bash scripts/kill-stray-coord-procs.sh`，否则残留进程会拖慢机器，
> 表现为随机的 `no quorum` / 连接超时（见 §6）。

> **跑全量套件前先清理孤儿进程**：进程级测试被中断（超时 / 手动 kill）时，
> 它 spawn 的 `coord server` 会变成孤儿常驻。本地实测积累了 **30 个**、最久的
> 已运行 11 小时，直接在 8 核机器上把 load 推到 9 → 后续套件出现
> `raft write timed out (no quorum?)` 这类**看起来像产品 bug 的假红**。
> 先执行 `bash scripts/kill-stray-coord-procs.sh`（只清理 `/tmp/.tmp*` 数据目录的
> 进程，不动人工集群；CI `test` job 已加为前置步骤）。```bash
# Rust：编译 + 全量测试 + panic 卡口
cargo check --workspace --all-targets
cargo test --workspace --no-fail-fast
bash scripts/check-panics.sh

# Java：SDK 单测（JDK 21）
(cd coord-java-sdk && mvn -B verify)

# Java：真实集群集成套件（48 用例）+ 证据落盘
bash scripts/collect-evidence.sh java-it
```
