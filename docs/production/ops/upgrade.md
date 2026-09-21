# 升级与版本兼容（W6-3 / P-Gate 7）

- **日期**：2026-09-21
- **锚点**：`docs/production/production-readiness-plan-2026-09-21.md` §5 W6-3
  （「滚动升级 + 版本兼容矩阵（N-1）」：**要么做、要么写进"不支持"清单**）
- **体例**：每条结论后跟**可核验的证据**（`file:line` / 命令 / 测试）。没有证据的
  一律标 ⏳ **未验证**，不得当作已支持。

---

## §1 裁定（一句话）

**v0.2.0 不支持滚动升级（rolling upgrade），也不声明 N-1 版本兼容。**
升级路径是**停机升级**（每节点 stop → 换二进制 → start），且**必须整集群同版本**。

理由不是"没时间做"，而是**三个前提在当前仓库里都不成立**：

| 前提 | 现状 | 证据 |
|:--|:--|:--|
| 新旧节点间的**协议协商** | ❌ agent↔server 之间**没有**版本协商 | `SUPPORTED_PROTOCOL_VERSIONS` 只存在于 agent↔SDK 面（`coord-agent/src/services/handshake.rs:29`）；server 侧无对应常量（`grep -rn protocol_version coord-server/src` 无输出） |
| 新旧节点间的**数据格式兼容** | ❌ 未验证（见 §3） | 见 §3 的「诚实的坏消息」 |
| 混合版本集群的**行为** | ❌ 未验证 | 无任何 lab 产物或测试覆盖"N-1 节点 + N 节点"混合拓扑 |

> ⚠️ **不要**把本节读成"我们不能升级"。停机升级是可执行的（§2），只是它**不是**滚动升级，
> 且要求整个集群一起动。

---

## §2 支持的升级路径：停机升级（可执行）

**适用**：v0.2.0 → v0.2.x（同一契约 Major 内）、以及未来的 Minor 升级。

```bash
# 1) 先确认集群健康且无进行中的成员变更 / 恢复
curl -s localhost:2379/health?verbose=true

# 2) 拉快照（回滚点）—— 见 runbook §3.1
coord-cli maintenance snapshot --output /backup/pre-upgrade-$(date -u +%Y%m%dT%H%M%SZ).snap

# 3) 逐节点：停 → 替换二进制 → 起（顺序任意，但**同一时间只动一个节点**）
#    每个节点起来后必须确认它重新加入并追平，再动下一个
systemctl stop coord && cp /opt/coord/bin/coord /opt/coord/bin/coord.bak \
  && install -m 0755 ./coord /opt/coord/bin/coord && systemctl start coord
curl -s localhost:2379/health?ready=true   # 必须 200

# 4) 全部节点完成后核对：
#    - MemberList 版本一致（Version 字段）
#    - 集群 60s 无 leader 变化（否则触发 CoordLeaderChurn）
```

**验收判据（每节点）**：

1. 停止期间该节点的 gRPC 端口关闭、`/health` 不可达 —— **不要**让负载均衡器继续打它；
2. 起来后 `/health?ready=true` 返回 200（未选主/未追平时返回 503 —— 这是
   `deploy/k8s/statefulset.yaml` 就绪探针用 `/ready` 而不是 `/healthz` 的原因，
   见 `docs/production/ops/k8s-verification.md`）；
3. `MemberList` 里该节点的 `version` 与其他节点一致；
4. 客户端在升级窗口内的失败必须是**可重试的 `UNAVAILABLE`**（不是数据错误）。

**⏳ 未验证（诚实标注）**：上面 4 条判据**没有**任何归档证据 —— 本次没有跑过真实
停机升级演练。首次演练须产出 `docs/production/ops/drills/<ts>-upgrade.md`（见 §5）。

---

## §3 三条兼容性的**实际**状态（逐条带证据）

### 3.1 wire（proto）—— ✅ 有机械保护

- 契约面卡口：`bash apis/contracts/scripts/check-wire-sync.sh`、`check-wire-descriptor.sh`；
- CI 的 `buf breaking`（`against main,subdir=apis/contracts`）阻断删字段/改编号/删 RPC；
- **枚举值改名/改号**的补充卡口：`coord-proto/tests/enum_wire_freeze.rs`
  （快照 `coord-proto/wire-freeze/enums.txt`，见 W1-7 的说明）。

⇒ **结论**：同一契约 Major 内的 wire 兼容是**被机器守着**的。

### 3.2 持久化格式（快照 / raft 日志 / `/_sys/auth/` / PD 元数据 / 对象存储 manifest）—— ❌ **未验证**

> **诚实的坏消息**：仓库里唯一名为"快照格式版本兼容"的测试
> （`coord-server/tests/sim_chaos_test.rs:530 test_snapshot_format_version_compatibility`）
> **不覆盖真实快照路径** —— 它在测试内**重新定义**了一个
> `struct SnapshotHeader { version, created_at, region_count }`（还有 V2 变体）做
> bincode roundtrip。也就是说：它只证明了"`#[serde(default)]` 能让新增字段反序列化成功"，
> 而**真实**快照的格式版本字段、以及"v0.2.0 的快照能否被 v0.3.0 读出"**完全没有被验证**。

真实格式由 bincode 编码的 Rust 结构决定，而 bincode **不做字段名/顺序的自描述**：
给某个结构**新增字段**对 bincode 就是**不兼容**（除非 `#[serde(default)]` 且顺序一致）。
因此：

- **同一 Minor 内的补丁升级**（只改实现、不改持久化结构）：可以直接停机升级；
- **改了任何持久化结构**：必须由升级前的一次快照 + 升级后的读取来验证，**当前没有这条路径**。

⇒ **结论**：本仓库**不声明**跨版本快照可读。runbook §3.3 的边界声明（"全集群配置须一致"、
"落后节点本地 chunk 清空 rebuild"）在新的口径下依然有效，但**不再隐含**"快照可跨版本恢复"。

### 3.3 agent↔SDK 协议 —— ✅ 有协商（且拒绝是**明确**的）

- 单一事实来源：`coord-agent/src/services/handshake.rs:29`
  `SUPPORTED_PROTOCOL_VERSIONS = ["coord-agent-api-v2"]`；
- 语义：无论客户端报什么版本都返回**全部**支持版本，由客户端判定自己是否在内
  ⇒ "不支持"在客户端侧是**可诊断**的，而不是一个含糊的 gRPC 错误；
- 对表卡口：`coord/tests/agent_handshake_test.rs`（Rust 常量 ↔ Java 侧
  `ProtocolNegotiator.SDK_PROTOCOL_VERSION`，防止再次漂移成空头承诺）；
- **不在列表内**的旧协议（`coord-agent-api-v1`）拿到的是"不支持"，不是静默降级。

⇒ **结论**：SDK 与 agent 的版本不一致是**被检测**的（这正是 D2 一次性改名留下的可诊断性缺口，已补）。

### 3.4 agent↔server —— ❌ 无协商、无验证

agent 把请求代理给 server，两侧**都没有**协议版本常量，也没有"最低 server 版本"检查。
混合版本部署（新 agent + 旧 server）的失败形态**未定义**：可能表现为
`unknown RPC method`（fail-closed 拒绝）或字段缺失导致的语义错误。

⇒ **结论**：**必须整集群 + 全部 agent 同版本**。这写进 §4 的"不支持"清单。

---

## §4 「不支持」清单（升级相关；接入方必须知道）

| # | 不承诺 | 后果 | 若要变成承诺需要做什么 |
|:--|:--|:--|:--|
| U-1 | **滚动升级**（不停机、逐节点换版本） | 升级窗口内需要停机 / 或有客户端失败 | 实现成员级版本协商 + 双向兼容的持久化格式；跑一次真实滚动升级演练并归档 |
| U-2 | **N-1 版本兼容矩阵** | 新旧节点/agent 混跑的行为未定义 | 同上 + 一份逐版本矩阵（每格一个 e2e 演练） |
| U-3 | **跨版本快照恢复** | 不得用旧版本快照恢复新集群（反之亦然） | 在快照头写显式 `format_version` + 提供迁移/拒绝路径 + 逐版本 roundtrip 证据 |
| U-4 | **降级（downgrade）** | 升上去就下不来（除按 U-3 的备份恢复） | 同 U-3 |
| U-5 | agent 与 server 版本不一致 | 可能 `unknown RPC` / 语义错误 | 两侧加协议版本协商（与 §3.3 同构） |

> 处置纪律沿用 `docs/production/ops/boundaries.md` 的规则 6：**不承诺**与**承诺**同等重要，
> 接入方必须逐条知道（`WHITEPAPER.md:42` §10 规则 4）。

---

## §5 待办（进入下一轮，不掩盖）

1. **停机升级演练**：单机 docker 三节点，跑 §2 的完整流程 + 客户端可重试失败验证
   ⇒ 产出 `docs/production/ops/drills/<ts>-upgrade.md`。**这是 W6-3 唯一还没做的部分。**
   > 相关的**备份恢复**演练已做了**进程内**版本（10/10，见
   > `docs/production/evidence/20260921T161653Z-w6-4-backup-restore-drill/`）——
   > 即 §2 第 2 步（拉回滚点快照）的代码路径已验证；但"逐节点停/换/起"本身仍未演练。
2. **快照格式版本字段**：给真实快照头加显式 `format_version`（不是测试里的影子结构），
   并在读取时对不支持的版本**显式拒绝**而不是尽力解析。
3. **agent↔server 版本协商**：与 §3.3 同构（两侧共用一个版本常量 + 对表测试），
   这是让"整集群同版本"从纪律变成**机制**的最小改动。
