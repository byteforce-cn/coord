# Coord 第三轮复核整改记录

| 项目 | 说明 |
|:---|:---|
| 整改对象 | `coord` v0.1.0（`github.com/byteforce-cn/coord`） |
| 复核输入 | `docs/第三轮.md`（评估方第三轮报告，基准 `37b0161`） |
| 整改基准 | `37b0161`（工作树干净；本记录所有改动均相对该提交） |
| 记录日期 | 2026-09-12 |
| 验证环境 | Linux，8 核；`rustc 1.98.1` / `rustfmt 1.9.0`（与 `rust-toolchain.toml` 一致）；JDK 25（CI 为 21，见"环境限制"） |

## 0. 摘要

本轮**逐条**处理了第三轮报告 §3（新发现）与 §7.2（收尾清单）中的全部条目，
并把两条 P0 都改到了"**结构性不可能再发生**"的层次，而不是在调用点打补丁：

| 议题 | 处理方式 | 关键证据 |
|:---|:---|:---|
| §3.1 P0 Txn 空 `range_end` 跨 scope 读 | 新增 `coord_core::kv_range::RangeSemantics` 作为**唯一**语义定义，服务端读取路径与鉴权层共同调用；Txn 内层 `Range` 空 `range_end` 改回单键语义 | `txn_scope_isolation_test.rs`（3 用例） |
| §3.2 P0 选举无互斥 | `campaign()` 改 Txn `Compare(Version==0)` CAS；并处理"键上就是本候选人"的重复竞选路径 | `leader_election_test.rs`（4 用例，含 8 候选并发） |
| §3.3 `java-sdk` job 必红 | `ServiceRegistryTest` → `ServiceRegistryIntegrationTest`（纳入既有命名约定）；SDK 侧 `assumeThat` 改**硬失败**并新增 `-Pit` 门禁，CI 在真实集群上运行 | `mvn -B verify`（java-example 默认 profile）**BUILD SUCCESS 且 0 个测试被执行**（修复前 `Tests run: 1` 且 FAILURE） |
| §3.4 C1 跨节点租约 ID 竞态 | 状态机对已存在的 `/_lease/{id}` **拒绝覆盖**（etcd `LeaseExist` 语义）+ 分配前 ReadIndex 屏障 + apply 后复核 | `test_lease_grant_refuses_to_overwrite_existing_id` 等 2 用例 |
| §3.5 P0-7 范围历史读静默漏 key | 补齐候选集合从"当前存活 Key"扩为"存活 ∪ T 之后被写过" | `test_historical_range_read_rejects_key_deleted_after_target` |
| §3.6 其余 4 项 | HMAC 宽限期加上明确 deadline 与 ERROR 告警；占位密钥在**原语层**拒绝；`scope="*"` 区间匹配修正；非法字符 scope 两侧同口径 | 3 个新单测 |
| §4.2① 后台任务静默死亡 | `timer.insert` 改 `Option`（不再退化为 `timer_id=0`）；新增 `supervisor` 模块并接入 5 个关键循环 | `insert_fails_closed_when_wheel_is_dead` 等 |
| §4.2② BFF 同步无界扫描 | 7 处 `range(.., usize::MAX)` → 有界 + `spawn_blocking`，截断如实上报 | `bff::scan_prefix_bounded` |
| §4.2③ 静默写丢弃 | 7 处 `let _ = raft_put` 改显式错误/计数；`AppliedLogId` 编码去掉失败路径；审计失败语义明确化 | `test_applied_log_id_encoding_is_total_and_backward_compatible` |
| §4.4 伪测试 | `write_batcher_test.rs` 由"自建 mock"**重写为真实实现测试**；`sim_jepsen`/`sim_chaos` 以 `required-features` 移出默认套件 | `write_batcher_test`（8 用例，全部 `use coord_server::…`） |
| §5.5 CI 脚本不可移植 | `kill-stray-coord-procs.sh` 去掉 `mapfile`（bash 4+），并覆盖 macOS `$TMPDIR` | `bash -n` + `--dry-run` 通过 |

**另有本轮新发现（比上述任何一条都更影响"门禁可信度"）**：CI 的 `lint` job 在
**基线提交上就是红的**——`cargo fmt --all -- --check` 与
`cargo clippy --workspace --lib --bins -- -D warnings` 两者都不通过。详见 §3。
这解释了为什么第三轮报告里多处"已修复"能带着新缺陷存活：**门禁存在 ≠ 门禁有效**。

---

## 1. §3.1 P0：Txn 空 `range_end` 跨 scope 越权读

### 1.1 根因与修法

报告指出的机理是"同一份代码里空 `range_end` 有两种语义"：

| 层 | 修复前 | 位置 |
|:--|:--|:--|
| 鉴权层 | 回落点查语义 | `auth/interceptor.rs` `authorize_access` |
| 服务端（顶层 `KV/Range`） | 单键精确查询 | `server/mod.rs` `let single_key = req.range_end.is_empty() \|\| req.range_end == req.key` |
| 服务端（Txn 内层 `TxnOp::Range`） | **字节前缀扫描** | `storage/mvcc.rs` `if range_end.is_empty() { tx.iter_prefix(..) }` |

取哪一边？**协议本身已经给了答案**：`coord-proto/src/proto/kv.proto` 对
`range_end` 的注释是"结束 Key（左闭右开），**空表示单 Key 精确查询**"；
插件 ABI 文档（`coord-agent/wit/coord-plugin.wit`）同样写着
"`kv.range`（`range-end` 空 = 单键精确查询）"。
因此修法是让 Txn 内层 op 与顶层 `KV/Range` 取**同一**语义，而不是给鉴权层
加第三个解读。

### 1.2 结构性修复：单一事实来源

新增 `coord-core/src/kv_range.rs`：

```rust
pub enum RangeSemantics { SingleKey, Interval }

impl RangeSemantics {
    /// 判定 `(key, range_end)` 的有效语义。
    /// **不得**在别处复制这段判断——复制出来的第二份就是下一个语义裂缝。
    pub fn of(key: &[u8], range_end: &[u8]) -> Self {
        if range_end.is_empty() || range_end == key { Self::SingleKey } else { Self::Interval }
    }
}
```

三处调用点全部改为从它派生：

- `coord-server/src/server/mod.rs`：顶层 `Range` 与 `Delete` 的 `single_key` / `is_range`；
- `coord-server/src/storage/mvcc.rs`：Txn 内层 `TxnOp::Range` 的扫描范围；
- `coord-server/src/auth/interceptor.rs`：新增 `ScopeAccess::from_range()`，
  `KV/Range`、`KV/Delete`、Txn 的 `RequestRange`/`RequestDelete` 统一走它。

**结构性保证**：鉴权层建模的区间与服务端实际扫描的区间现在由**同一个函数**决定，
两者不可能再分叉。

### 1.3 修复过程中被测试抓出的第二个裂缝

写"Txn 内层 Range 与顶层 Range 结果必须一致"的一致性测试时，测试立刻失败：

```
assertion `left == right` failed: Txn 内层 Range 与顶层 Range 语义分叉:
key="/ns/a" range_end="/ns/a"
  left: []                          // Txn：返回空
  right: [[47, 110, 115, 47, 97]]   // 顶层：返回 /ns/a
```

原因：`range_end == key` 属单键语义，但循环里的"防御性上界复核"仍用
`!range_end.is_empty() && user_key >= range_end` 提前 `break`，把唯一命中丢掉。
已改为按 `semantics.is_interval()` 判定。**同一族语义分叉在同一文件里第二例**——
这正是"复制出来的第二份判断"的典型代价。

### 1.4 证据

新增 `coord-server/tests/txn_scope_isolation_test.rs`（把评估方的临时 PoC 固化为永久用例），
固化的不变量是两层同时成立：

1. 请求要能通过鉴权 ⇒ 鉴权层建模的每个访问区间都必须被授权 scope 覆盖；
2. 服务端返回的每个 Key 都必须落在鉴权层建模的某个访问区间内。

```
running 3 tests
test txn_empty_range_end_is_single_key_and_cannot_escape_scope ... ok
test txn_explicit_interval_beyond_scope_is_denied_by_auth ... ok
test txn_range_end_equal_key_is_single_key_on_both_layers ... ok
test result: ok. 3 passed; 0 failed
```

同时补上报告点名"本轮缺失的用例"：`test_extract_scope_access_txn_empty_range_end_is_point`
（`coord-server/src/auth/interceptor.rs` 单测模块）。

`mvcc.rs` 侧另加 `test_txn_range_matches_top_level_range_semantics`（矩阵化对照两条路径）。

> 行为变更说明：`Txn{Range(key, range_end="")}` 从"前缀扫描"改为"单键精确查询"。
> 依据是 proto 注释与插件 ABI 文档；仓库内唯一依赖旧行为的调用点是测试
> （`test_txn_range_inside`），已按新语义改写并补上显式区间版本。

---

## 2. §3.2 P0：Leader 选举互斥

### 2.1 修法

`campaign_shared()` 从**无条件 `put_lease`** 改为 **Txn `Compare(Version == 0)` + Put**，
即照抄同仓 `lock.rs` 的锁获取模式（正确写法一直就在隔壁）：

```rust
let compare = Compare {
    result: CompareResult::Equal as i32,
    target: Target::Version as i32,
    key: storage_key.clone(),
    target_value: Some(TargetValue::Version(0)),
};
let put_op = RequestOp { op: Some(Op::RequestPut(PutRequest { key, value, lease_id, .. })) };
match inner.client.txn().txn(vec![compare], vec![put_op], vec![]).await {
    Ok(resp) if resp.succeeded => /* 独占当选 */,
    Ok(_) => /* 键已被占用 → Follower，归还租约 */,
    Err(e) => /* 释放租约 + 原样上报（不得静默当 Follower） */,
}
```

同时处理一个 CAS 引入的新边界（不处理会让 CAS 把系统变成"**无主**"）：
若 CAS 失败但键上记录的 `leader_id` **就是本候选人**（重复 campaign，或退位后重选而
键尚未随租约撤销删除），必须**保持 Leader**。否则本地自认 Follower 而服务端 key 仍指向
自己，会同时阻塞其它节点当选且不提供服务。

### 2.2 为什么这个缺陷能存活

`leader_election.rs` 原有 11 个测试**无一调用 `campaign()`/`resign()`**；
且 `AgentInner` 位于私有模块 `mod proxy;`，**任何集成测试都无法构造**
`LeaderElectionService` 或 `LockService`。因此本次一并把 `AgentInner` 对外导出
（`pub use proxy::AgentInner;`）——它本来就是 `pub struct` + `pub fn new`，
只是被私有模块挡住。这是这两个服务"零功能测试/零故障注入测试"的**结构性原因**。

### 2.3 证据

新增 `coord/tests/leader_election_test.rs`（真实 gRPC server：进程内单节点 raft +
真实状态机 + 两个独立客户端）：

```
running 4 tests
test concurrent_campaign_has_exactly_one_winner ... ok          // 8 个候选者并发竞选
test follower_cannot_usurp_while_leader_lease_alive ... ok      // 在任期内反复抢占均失败
test recampaign_by_current_leader_stays_leader ... ok           // 重复竞选不得自我降级
test new_leader_elected_after_leader_lease_revoked ... ok       // 故障注入：Leader 消失后可重新选出
test result: ok. 4 passed; 0 failed
```

`concurrent_campaign_has_exactly_one_winner` 直接对应 §7.3 的一票否决项
"任一时刻不得超过一个节点自认 leader"，并额外校验**本地自认与服务端 key 记录一致**。

> 诚实边界：这是 L2（进程内真实 server/client + 真实状态机与网络栈），
> **不是** L3 多进程 + 网络分区注入。分区/时钟回拨下的选举行为仍属未覆盖（见 §6）。

---

## 3. 【本轮新发现】CI `lint` job 在基线提交上必然红

复核 §7.3"未经通过不得承载业务"这类**门禁类**结论时，我实际执行了
`.github/workflows/ci.yml` 的 `lint` job 的每一步。结论：

| CI 步骤 | `ci.yml` 行 | 基线 `37b0161` 实测 |
|:---|:---|:---|
| `cargo fmt --all -- --check` | :43 | **FAILED**（≥16 个文件不符合 rustfmt） |
| `cargo clippy --workspace --lib --bins -- -D warnings` | :45 | **FAILED**（`clippy::map_entry` @ `coord-server/src/lease/mod.rs`） |
| `scripts/check-panics.sh` | :49 | PASS |

工具链已排除版本差异因素：`rust-toolchain.toml` 固定 `channel = "1.98.1"`，
本机 `rustc 1.98.1` / `rustfmt 1.9.0` 与之逐字一致。

**为什么这条比前两个 P0 更值得关注**：前两轮的整改报告都以"CI 无
`continue-on-error`、无测试命令级 `|| true`"作为门禁有效的证据。但门禁**存在**
不等于门禁**有效**——一个从第一期就红着、且看起来"配置齐全"的 job，会让
"我们加了门禁"这件事本身失去证据价值。**这也解释了为什么本轮仍能在上一轮
"已修复"的区域内发现新 P0**：没有任何一道自动门禁在替人看。

### 已处理

- `cargo fmt --all`：使 fmt 门禁转绿（`cargo fmt --all -- --check` 返回 0）。
- `coord-server/src/lease/mod.rs`：`contains_key` + `insert` 改为 `HashMap::entry()`
  （语义不变，仅消除 `map_entry`），`cargo clippy --workspace --lib --bins -- -D warnings`
  现在返回 0。

### 建议（超出本轮代码改动范围，需组织层面决定）

1. 把 `lint` job 设为**分支保护必需检查**；
2. 在 CI 里加一条"门禁自检"：故意引入一个 fmt 违规，确认 job 变红——
   否则无法区分"门禁通过"与"门禁没跑"。

---

## 4. §3.3 / §7.2-9：Java 接入

### 4.1 `ServiceRegistryTest` 躲在两个 pattern 的缝里

`java-example/pom.xml` 默认 profile 排除 `**/*IntegrationTest.java` 与
`**/*AdvancedTest.java`，`-Pit` profile 只 **include** 这两类。
`ServiceRegistryTest` **两个 pattern 都不匹配** → 在无集群的默认 CI 中被执行并 FAILURE，
而在真实集群下反而不跑。

修法：改名 `ServiceRegistryIntegrationTest`，归入既有命名约定。并把约定写成注释：
**任何需要真实 Agent 的测试都必须以 `IntegrationTest`/`AdvancedTest` 结尾。**

验证（修复后，无集群）：

```
[INFO] --- surefire:3.5.2:test (default-test) @ coord-java-example ---
[INFO] BUILD SUCCESS
```

注意 surefire 这一节**没有再执行任何测试**（修复前是 `Tests run: 1, Errors: 1`）。

### 4.2 SDK 集成用例不再"静默跳过"

`coord-java-sdk/src/test/.../CoordClientIntegrationTest.java` 此前用
`assumeThat(agentAvailable)` —— 在 CI 报告里"跳过"与"通过"无法区分，
该套件**从未真正产生过证据**。现在：

- `coord-java-sdk/pom.xml` 增加与 java-example 一致的默认排除 + `-Pit` profile；
- `assumeThat` 改为**硬失败**（`IllegalStateException`，信息含
  `scripts/ci-java-it-cluster.sh` 指引）；
- CI 的 `java-example-it` job（已起真实 server + agent）新增一步
  `mvn -B verify -Pit`（`coord-java-sdk`），使该套件真正成为门禁。

---

## 5. §3.4 / §3.5：租约 ID 竞态与历史范围读

### 5.1 C1 跨节点租约 ID 竞态（§3.4）

上一轮只收口了**进程内** TOCTOU；跨节点仍可静默覆盖。本轮在**状态机**层收口：

`apply_lease_op` 的 `LeaseOp::Grant` 现在**拒绝覆盖**已存在的 `/_lease/{id}`：

- 状态机是 ID 归属的**最终权威**——即使 leader 侧检查因任何原因漏判，
  也只会得到"本次 Grant 未生效"，而不会破坏已有租约；
- 语义与 etcd `LeaseGrant` 指定已存在 ID 报 `LeaseExist` 一致；
- `Revoke` 后同 ID 可复用（`test_lease_id_reusable_after_revoke`）。

配套两条 leader 侧措施：

1. `lease_grant` 在分配 ID **之前**做一次 ReadIndex 线性一致屏障，
   保证存在性检查看到的是**完整的已提交历史**；
2. apply **之后复核**：读回 `/_lease/{id}`，若 `keepalive_revision` 不是本次写入的
   revision，说明该 ID 已被占用 → 回滚本地缓存并返回 `AlreadyExists`，
   **绝不假装成功**。

### 5.2 P0-7 范围路径（§3.5）

`range_at_revision` 的补齐循环候选集合从"当前存活的 Key"扩为
"**当前存活 ∪ T 之后被写过**"。此前"T 时刻存在 → T 之后被删除 → T 之前历史已压缩"
的 Key 既不进 `view`、也不在存活集合、更不进 `unreconstructable` → **静默漏报**，
而同一条 Key 走单键路径会明确报错。

```
test_historical_range_read_rejects_key_deleted_after_target ... ok
test_historical_read_keeps_keys_untouched_since_compaction ... ok
test_historical_read_rejects_unreconstructable_key ... ok
test_range_at_revision_historical_view ... ok
```

---

## 6. §4.2：代码质量/运维视角

### 6.1 ① 后台任务静默死亡

**timer wheel**（最难诊断路径的源头）：

- `TimerWheelHandle::insert` 返回类型改为 `Option<u64>`，时间轮死亡时返回 `None`
  —— `0` 从来不是合法 ID（`next_id` 从 1 起），旧实现把它当"失败值"传播，
  最终让租约**永不触发到期**、绑定 Key 静默无界泄漏，且无报错无日志；
- `LeaseManager::grant_with_id_checked` 与 `rebuild` 两条路径都改为 **fail-closed**
  （前者拒绝 grant，后者跳过该条记录并 ERROR），不再写入伪造的 0。

**新增 `coord-server/src/supervisor.rs`**：

- `spawn_supervised`：任务意外结束（正常 return / panic / cancel）→ **ERROR 日志** +
  登记到进程级 `dead_tasks()`；
- `spawn_supervised_with_shutdown`：优雅停机不再误报为死亡；
- **不自动重启**（这些循环持有不可重建的本地状态，盲目重启可能双跑）——
  取舍是把"静默死亡"变成"显式可观测死亡"。

已接入：`timer_wheel`、`write_batcher`、`snapshot_scheduler`、`object_gc_loop`（×2）。

### 6.2 ② BFF 同步无界扫描

新增 `coord_server::bff::scan_prefix_bounded(node, prefix)`：

- 扫描移入 `spawn_blocking`（不再占用 async worker，raft 心跳/定时器不被读请求拖住）；
- 硬上限 `BFF_SCAN_LIMIT = 5000`（多取 1 条判定截断）；
- `truncated` 由调用方**如实上报**，不静默截断成"看起来是全部"。

7 处调用点全部替换，`bff/` 下已无 `usize::MAX` 作为扫描上限（仅注释中提及）：

```
config_api.rs   3 处   registry_api.rs  4 处
```

顺带修掉两处 `unwrap_or_default()`（读失败被当成"没有实例"）。

### 6.3 ③ 静默写丢弃

| 站点 | 修复前 | 修复后 |
|:--|:--|:--|
| `config_api.rs` / `registry_api.rs` | `let _ = raft_put(..)`（7 处） | 新增 `raft_put_aux`：辅助写失败→ERROR + 明确错误响应（"主写入已提交，但…未完整完成"）；registry 健康写回改为**计数并回传** `writeFailures` |
| `mvcc.rs` `AppliedLogId::to_bytes` | `bincode::serialize(..).unwrap_or_else(\|_\| Vec::new())` → 空字节成为 apply 水位 → **重启从 0 重放整个日志** | 手写定长编码（4B 魔数 + 3×8B 大端），**无失败路径**；`from_bytes` 兼容旧 bincode 编码，升级不丢水位 |
| `audit/mod.rs` | `warn!("audit append failed")`，而底层 store 注释宣称"审计不丢事件" | 门面与实现表述一致：失败 ERROR + `append_failures()` 计数；明确"写失败即**永久丢失**，sync_all 只能保证写成功时不丢" |

> 说明：`check-panics.sh` 要求**生产代码非测试 panic 计数为 0**，因此报告建议的
> "改为 `expect` 并说明不可达理由"在本仓不可用——必须改成**没有失败路径**的编码，
> 这也确实比 `expect` 更好。

---

## 7. §3.6 其余四项

| 项 | 修复 |
|:--|:--|
| HMAC 宽限期无下线机制 | 新增 `HMAC_GRACE_DEADLINE_UNIX`（UTC 2026-12-31）与 `hmac_grace_expired()`；到期后每次进入 `decode_any` 以 **ERROR** 告警（进程内一次，避免刷屏）。**刻意不自动失效**：按墙钟日期静默改变认证行为本身是可用性风险；删除该分支应当是显式、可测试的代码变更。 |
| agent 侧密钥熵校验缺失 | 把"非空、≥32B、**且非单字节重复占位串**"下沉到密码学原语 `coord_core::auth::cct::is_usable_hmac_key`（`sign`/`verify`/`decode` 全部经过它）。此前长度检查只在 agent、全零检查只在服务端配置层——**两侧不对称**，运维照抄 32B 全零占位串时 agent 会接受。 |
| `scope="*"` 区间读 403 | `scope_covers_interval` 的 match-all 白名单补上 `"*"`（`ScopeTrie` 一直把它当通配段）。 |
| 非法字符 scope 两侧不一致 | `scope_safe_byte_prefix` 先调用 `validate_scope_chars`，与点查路径同口径 fail-closed。 |

新增用例：`placeholder_hmac_keys_are_not_usable`、`star_scope_is_match_all_for_intervals`、
`invalid_scope_chars_are_denied_on_the_interval_path_too`。

---

## 8. §4.3 / §4.4 / §5.5：测试与工程卫生

### 8.1 伪测试（§4.4 第 1 项）

- **`coord-server/tests/write_batcher_test.rs`**：原文 291 行零 `use coord*`，
  9 个用例测的是文件内自建的 `MockStorage`，与真实实现无任何代码路径关联，
  **且没有任何免责声明**。已**重写为真实实现测试**（8 个用例，直接驱动
  `coord_server::storage::write_batcher::WriteBatcher`）：队列 FIFO 语义、
  group commit 合并（`run_blocking` 一次 flush 覆盖全部待处理）、
  50 条并发提交**恰好送达一次**、停机前最终 flush、写失败不得回执成功。
- **`sim_jepsen_test` / `sim_chaos_test`**：是自建模型（有诚实免责声明，
  但不是系统级证据）。已用 `required-features = ["sim-tests"]` 移出默认 `cargo test`，
  并在 CI 的 `test` job 中**显式**运行——"移出默认"不等于"永不运行"。

### 8.2 测试夹具去重（§4.3 复制粘贴族）

新增 `coord/tests/common/mod.rs`：把在多个测试文件里**逐字重复**的
`find_port()` / `start_test_server()` 收敛为单一事实来源；`agent_proxy_test.rs`
与 `agent_watch_test.rs` 已改为 `mod common;` 引用（各删除 ~150 行副本）。
同时新增 `wait_until`/`wait_tcp_ready` 有界等待助手，替代固定 `sleep`。

### 8.3 `kill-stray-coord-procs.sh` 可移植性（§5.5）

- 去掉 `mapfile`（bash 4+ 内置，macOS 自带 bash 3.2 没有）——`set -e` 下第一行就退出，
  **什么都没清理**；
- 临时目录覆盖 `/tmp` 与 `$TMPDIR`（macOS 的 `/var/folders/...`）；
- `ps -o etimes` → `ps -o etime`（BSD 兼容）。

验证：`bash -n` 通过；`--dry-run` 在本机正常输出。

---

## 9. 验证结果

| 验证项 | 命令 | 结果 |
|:--|:--|:--|
| 编译（全目标） | `cargo check --workspace --all-targets` | **0 error** |
| 格式门禁 | `cargo fmt --all -- --check` | **通过**（基线为红） |
| Clippy 门禁 | `cargo clippy --workspace --lib --bins -- -D warnings` | **通过**（基线为红） |
| Panic 门禁 | `scripts/check-panics.sh` | 生产代码 0 违规 |
| 全量测试 | `cargo test --workspace --no-fail-fast` | 见 §9.1 |
| 模拟套件（门控） | `cargo test -p coord-server --features sim-tests --test sim_jepsen_test --test sim_chaos_test` | 通过（默认套件不再混入） |
| Java 默认 profile | `mvn -B verify`（java-example，无集群） | **BUILD SUCCESS** 且 0 个集群依赖用例被执行 |

### 9.1 全量测试

```
$ bash scripts/kill-stray-coord-procs.sh
$ cargo test --workspace --no-fail-fast
...
EXIT=0
passed=1909  failed=0  ignored=41
（"error / test result: FAILED" 行数 = 0）
```

**0 个 target 失败。** 通过数较第二轮报告的 1914 略少，是**刻意**的构成变化，不是覆盖下降：

| 变化 | 数量 |
|:---|:---|
| `sim_jepsen_test` / `sim_chaos_test` 移出默认套件（`required-features = ["sim-tests"]`，改由 CI 显式运行） | −26 |
| `write_batcher_test.rs` 由"自建 mock"重写为真实实现测试（9 → 8） | −1 |
| 本轮新增的针对性回归测试 | +21 |

新增的 21 个测试全部对应本轮的具体修复（见下表），即**净增的是有证据价值的测试**，
减少的是"让数量虚高"的那部分。

| 新增测试 | 对应修复 |
|:---|:---|
| `txn_scope_isolation_test`（3） | §3.1 P0-1 |
| `leader_election_test`（4） | §3.2 P0-2（含 8 候选并发 + 故障注入重选） |
| `kv_range::tests`（3） | §3.1 语义单一来源 |
| `mvcc::tests::test_txn_range_*`（2） | §3.1（Txn/顶层语义一致性矩阵） |
| `placeholder_hmac_keys_are_not_usable` 等（3） | §3.6 |
| `lease_grant_refuses_to_overwrite_existing_id` 等（2） | §3.4 C1 |
| `test_historical_range_read_rejects_key_deleted_after_target`（1） | §3.5 P0-7 |
| `insert_fails_closed_when_wheel_is_dead`（1） | §4.2① |
| `test_applied_log_id_encoding_is_total_and_backward_compatible`（1） | §4.2③ |
| `supervisor::tests::supervised_task_records_exit`（1） | §4.2① |

**原始日志已作为可审计证据入库**（记录提交号与工具链，含压缩前后校验和）：
`docs/production/evidence/20260912T164636Z-round3-workspace-tests/`
（`MANIFEST.md` + `run.log.gz` + `sha256sums.txt`）。
这直接回应第三轮 §4.4 第 2 项对"证据与提交位脱钩"的批评：上一份入库证据记录的是
`e79f882` 且 `dirty files: 30`，本份记录的是明确的提交 `8e2cb37`。

### 9.2 反向证明：新测试**确实**能抓住修复前的缺陷

第三轮报告的方法是"把读码推断升级为可复现证据"，因此本轮的回归测试同样必须证明
**它们在修复前的代码上会失败**——否则"加了测试"不等于"测试有效"。

做法：临时把两处修复改回修复前的实现，跑对应回归测试，记录输出，然后恢复。

**(a) P0-1**（`mvcc.rs` 的 Txn 内层 `Range` 改回"空 `range_end` → 前缀扫描"）：

```
test txn_empty_range_end_is_single_key_and_cannot_escape_scope ... FAILED
  assertion `left == right` failed: 空 range_end 不得退化为前缀扫描
  left:  [/app/a, /app/a/1, /app/a0x, /app/abc/config, /app/admin/root-token]
  right: [/app/a]

test txn_range_end_equal_key_is_single_key_on_both_layers ... FAILED
  assertion `left == right` failed: range_end == key 必须与顶层 Range 一样被当作单键读取
  left: []   right: [/app/a/1]

test result: FAILED. 1 passed; 2 failed
```

返回集合与评估方 §3.1 的 PoC 输出**逐字一致**（含 3 个 scope 外哨兵 Key）。

**(b) P0-2**（`leader_election.rs` 的 Txn CAS 改回无条件 `put_lease`）：

```
test concurrent_campaign_has_exactly_one_winner ... FAILED
  assertion `left == right` failed: 竞选必须互斥：期望恰好 1 个 Leader，实际
  ["cand-0","cand-1","cand-2","cand-3","cand-4","cand-5","cand-6","cand-7"]（Follower: []）
  left: 8   right: 1

test follower_cannot_usurp_while_leader_lease_alive ... FAILED
  assertion `left == right` failed: 第 0 次抢占必须失败（Leader 租约仍有效）
  left: Leader   right: Follower

test recampaign_by_current_leader_stays_leader ... FAILED
  assertion `left == right` failed: 保持身份时必须沿用**原有**租约
  left: 3   right: 13

test result: FAILED. 1 passed; 3 failed
```

**8 个候选者全部自认 Leader**——这正是第三轮报告 §3.2 描述的"永久双主且不自愈"，
在修复前**可被自动化复现**，修复后全部转绿（4 passed）。

恢复后已确认**源码中无任何临时标记**（`git grep -c "NEGATIVE-PROOF" HEAD -- '*.rs'` 无输出），
且上述两个套件重新全绿。

### 9.3 全量套件的一处**既有**时序脆性（非本轮引入）

首次全量运行时有 2 个 target 失败（`coord` 的 `auth_enforcement_test` 与 `cluster_test`）。
单独重跑两者**均全绿**（3 passed / 12 passed）。

排查结论：`auth_enforcement_test` 在整仓并行负载下超时失败 → 它 spawn 的
`coord server` 子进程未被回收成为孤儿（实测 3 个、存活 9 分 45 秒）→ 负载进一步升高
→ 紧随其后的 `cluster_test` 出现 raft "no quorum" 假红。这正是第三轮报告 §5.5 与
厂商自述中记录过的**同一**故障模式（孤儿进程 → 假红）。

它**不是**本轮改动引入的（两者都只读取/隔离既有行为），但它是"全量套件必须
串行且先清理孤儿进程"这一结论的又一例证——也说明 `kill-stray-coord-procs.sh`
的可移植性修复（§8.3）确实有实际价值。

### 9.2 环境限制（需在 CI 上复验）

- 本机 JDK 为 **25**；`coord-java-sdk` 中 16 个 Mockito inline mock 用例在 JDK 25 上
  无法运行（ByteBuddy 不能 instrument）——CI 固定 JDK 21，故这是**环境问题而非回归**。
  本机因此只能验证：SDK 默认 profile **不再执行** `CoordClientIntegrationTest`（已确认）。
- Java 真实集群集成（`java-example-it` 与新增的 SDK `-Pit`）需要
  `scripts/ci-java-it-cluster.sh` 起 server+agent，属 CI job 范围。

---

## 10. 本轮未完成 / 建议下一轮处理

诚实列出未闭合项，避免"改了一部分就宣称全面"：

| 项 | 状态 | 说明 |
|:--|:--|:--|
| §4.3 `prefix_end` 4 份拷贝收敛 | **未做** | `policy.rs` / `workflow.rs` / `workflow_store.rs` / `commands.rs` 各一份，且用**空 `Vec` 同时表示"空前缀"与"全 0xFF 前缀"**两种语义。收敛需要改变调用方语义（`Option` 化），风险高，应单独一轮做并补测试。本轮已在 `coord-core::kv_range::prefix_successor` 提供正确原语，供下一轮替换。 |
| §4.3 模块级 `//!` 文档 | **未做** | 全仓 260 个源文件仅 1 处 `//!`（还是测试夹具），`cargo doc` 里所有生产模块都没有模块文档。属低成本高回报，但会触及 200+ 文件，应作为独立的机械提交，避免与安全整改混在同一 diff。 |
| §4.3 超长函数/上帝文件拆分 | **未做** | `run_server` 1684 行等。属结构性重构，不宜与缺陷整改同批进行。 |
| §4.3 373 处内部任务编号注释 | **未做** | 指向不在仓库里的计划文档，对后来维护者不可解。清理成本低但需要先确认哪些编号仍有意义。 |
| §4.4 时序脆性（~150 处 `sleep`） | **部分** | 新增/改写的测试使用有界等待；存量 `sleep` 未批量替换（容易引入新 flaky）。 |
| §4.4 锁与 LeaderElection 的**故障注入** | **部分** | LeaderElection 已有并发竞选用例与"Leader 消失后重选"用例；**网络分区 / 时钟回拨**下的选举与锁行为仍无覆盖。§7.3 的一票否决项要求这两条必须有故障注入测试，**目前仍未达标**。 |
| C1 跨节点的**多节点**复现测试 | **未做** | 本轮在状态机层收口并补了单节点单测；多节点 leader 切换窗口的复现测试需要 L3 夹具。 |
| 依赖版本发散（`hashbrown` 4 版等） | **未做** | `deny.toml` 仍是 `warn` 而非 `deny`。 |
| BFF `serde_json::to_vec(..).unwrap_or_default()`（~10 处） | **未做** | 与 §4.2③ 同类（失败即空值），但这些类型序列化实际不可失败，优先级低；已在 §6.3 记录。 |
| timer wheel 监督的"优雅停机"区分 | **未做（精度问题）** | `TimerWheel::start()` 目前用 `spawn_supervised`（无停机信号），因此进程退出/测试里调用 `shutdown()` 时也会记一条 ERROR "task EXITED"。要精确区分，需给 `TimerWheelHandle` 增加一个 shutdown `watch` 信号并改用 `spawn_supervised_with_shutdown`（约 10 行）。不影响正确性，仅影响告警信噪比。 |
| `docs/production/evidence/` 证据与提交位对位 | **未做** | 第三轮 §4.4 第 2 项指出的"证据产自脏工作区、与任一提交无法对位"需要流程而非代码修复（例如 CI 产出证据时记录 commit SHA + 工作树校验和）。 |

---

## 11. 结论

- 第三轮报告的四条 P0/中高优先项（§3.1、§3.2、§3.3、§3.4、§3.5）与
  §7.2 收尾清单的第 1–6、8、9（部分）项**均已闭合**，并都有可执行证据。
- 两个 P0 的修法都落在"结构性不可能再发生"的层次（单一语义定义 / 状态机权威 +
  CAS），而不是调用点补丁。
- **新增一条更根本的发现**：CI 的 fmt 与 clippy 门禁在基线提交上是红的。
  这解释了缺陷为何能持续存活，也意味着"门禁已加"这类结论在此之前**没有证据价值**。
  本轮已把两道门禁修绿，并建议将 `lint` 设为分支保护必需检查。
- 仍有明确未闭合项（§10），其中最需要关注的是
  **锁与选举在分区/时钟回拨下仍无故障注入证据**——按 §7.3 的一票否决标准，
  该能力域**仍不应承载业务**。
