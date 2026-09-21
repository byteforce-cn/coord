# 备份恢复演练证据（W6-4）

| 字段 | 值 |
|:--|:--|
| 场景 | `w6-4-backup-restore-drill`（**进程内**备份/恢复演练） |
| UTC 时间戳 | `20260921T161653Z` |
| coord commit | `19cb350ec32627d1c04cd3b3f2e18fd126de2559` |
| 工作树 | **CLEAN**（`git status --porcelain` 仅本证据目录自身 ⇒ 代码树与该 commit 逐字一致） |
| 宿主 | Linux 6.8.0-139-generic x86_64，rustc 1.98.1 |
| 跑的人 | 本次会话（AI 助理），**未人工复核** |
| 结论 | ✅ 四套用例 **10/10 通过**（详见下） |

## 命令

```bash
D=docs/production/evidence/20260921T161653Z-w6-4-backup-restore-drill
cargo test -p coord-server --test snapshot_rpc_test       >> $D/run.log 2>&1   # 2 passed
cargo test -p coord-server --test snapshot_transfer_test  >> $D/run.log 2>&1   # 1 passed
cargo test -p coord-server --test restart_recovery_test   >> $D/run.log 2>&1   # 4 passed
cargo test -p coord     --test m0_recovery_suite          >> $D/run.log 2>&1   # 3 passed
```

## 覆盖了什么（逐条对应 runbook §3）

| 用例 | 覆盖的运维动作 | 结果 |
|:--|:--|:--|
| `snapshot_rpc_test::test_snapshot_rpc_stream_and_restore` | **在线拉快照**（Maintenance/Snapshot 流式）→ 恢复 | 通过 |
| `snapshot_rpc_test::test_snapshot_rpc_matches_local_export` | 在线快照与本地导出的**一致性** | 通过 |
| `snapshot_transfer_test::test_snapshot_export_wipe_import_roundtrip` | 导出 → **清空** → 导入 → 数据回读（真正的"恢复"路径） | 通过 |
| `restart_recovery_test::test_snapshot_persisted_and_loaded` | 快照落盘后重启加载 | 通过 |
| `restart_recovery_test::test_purge_guard_refuses_without_durable_snapshot` | **purge 守卫**：无持久快照时拒绝回收 raft 日志（与 W1-4(b) 的 `snapshot_logs_since_last=0` WARN 同一族风险） | 通过 |
| `restart_recovery_test::test_applied_persisted_across_restart` | `applied` 水位跨重启持久化 | 通过 |
| `restart_recovery_test::test_replay_is_idempotent` | 重启后**重放幂等**（恢复过程不得产生第二份效果） | 通过 |
| `m0_recovery_suite::m0_snapshot_purge_then_restart` | 崩溃恢复全流程（purge → 重启 → 可读） | 通过 |
| `m0_recovery_suite::m0_purged_log_restart_guard_allows_valid_snapshot` | 有合法快照时重启守卫放行 | 通过 |
| `m0_recovery_suite::m0_kill9_restart_revision_stable` | **kill -9** 后重启 revision 不回退（崩溃一致性） | 通过 |

（共 10 条；`grep -E "^test [a-zA-Z0-9_]+ \.\.\. ok" run.log` 可复核。）

> ⚠️ 不要把这 10 条读成"备份恢复已经验收"。它们证明的是**这条代码路径在当前实现下可用**，
> 不证明"运维能在真实故障下按时恢复" —— 后者需要带真实进程 / 多节点 / 对象存储的演练（见下）。

## 本证据**没有**覆盖的（边界，必须一起引用）

1. **对象存储（`coord.storage`）**：本次完全没碰。
   已知边界（runbook §3.3、`adopter-playbook.md` §3）：快照恢复时**落后节点的本地 chunk
   会被清空并 rebuild**，期间 `Get` 可能返回 `UNAVAILABLE`；全集群配置须一致。
   ⇒ 含对象存储的恢复演练 **⏳ 未做**。
2. **多节点**：全部是进程内单节点用例。"逐节点停/换/起"的**停机升级**路径
   （`ops/upgrade.md` §2）未演练 ⇒ 该文 §2 的四条验收判据仍标 ⏳。
3. **`--features sim-tests`**：本次没有启用（CI 的 workspace tests job 单独跑它）。
4. **参数确认（jepsen §5.4-③）**：与本演练无关 —— 本演练不是 jepsen run，
   不产生 lab 证据，也不声明 quiet 可用率。
5. **人工复核**：跑的人是本次会话（AI），按 `governance.md` 的纪律，
   "AI 跑通"≠"运维演练完毕"。本文件只声明"可复现 + 与 run.log 一致"。

## 复现

```bash
git checkout 19cb350
D=/tmp/w6-4-drill && mkdir -p $D
for t in "coord-server snapshot_rpc_test" "coord-server snapshot_transfer_test" \
         "coord-server restart_recovery_test" "coord m0_recovery_suite"; do
  set -- $t; cargo test -p $1 --test $2 >> $D/run.log 2>&1
done
grep -c 'test result: ok' $D/run.log   # 期望 4
```

## 产物

| 文件 | 说明 |
|:--|:--|
| `run.log` | 完整原始输出（含编译 warning，未截断） |
| `MANIFEST.md` | 本文件 |
| `sha256sums.txt` | 产物校验和 |
