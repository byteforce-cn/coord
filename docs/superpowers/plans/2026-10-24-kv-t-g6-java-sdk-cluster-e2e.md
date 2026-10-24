# KV-T G6 (#172) Java SDK v2 Real-Cluster E2E Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Java SDK 的六个 v2 客户端（TSO/KV/Txn/Watch/Lease/Storage）在**真实 v2 集群**上通过 Agent 跑通，并把这条证据接进 CI；不再只靠 fake Netty server。

**Architecture:** 给 `scripts/ci-java-it-cluster.sh` 增加 `v2` 模式（单节点 multi-raft + PD + object storage + `[kv_t].enabled=true`，独立端口/运行目录，显式 `http_addr` 避免与 raft 端口相撞）；SDK 增加 `KvTV2ClusterIntegrationTest`（`-Pit` 下执行，硬失败而非 skip），按真实语义验证六个数据面、OCC 冲突返回值与错误码。CI 在同一 job 里并行起 v1/v2 两套集群。

**Tech Stack:** Rust `coord server`/`coord agent`、Java 21 / JUnit 5 / AssertJ / Maven、bash。

**Spec:** issue #172（Java SDK v2 GA 的「示例可跑 / 跨语言 e2e」残余）、ADR-0015 D3/D5/D7；上一刀 `docs/superpowers/plans/2026-10-23-kv-t-g6-client-watch-epoch.md` 与 #243。

## Global Constraints

- `[kv_t].enabled=false` 的 v1 集群行为与现有 Java IT 不变；v2 模式只新增一套端口/目录。
- v2 集群必须走真实 `coord server` 进程（不是 in-process fake），Agent 必须是真实 `coord agent`。
- 测试数据唯一（时间戳前缀），不清理也不断言全局键空间为空——dev 集群可复用。
- 缺少 v2 集群时 `KvTV2ClusterIntegrationTest` **明确失败**并给出启动命令；禁止 `assumeTrue` 跳过。
- 不新增 wire 字段、错误码或 SDK 公共 API；只验证既有面。
- 注释只写当前「为什么」；不写日期/变更史；不新增 `TODO`/`FIXME`。
- 临时文件只放 `/tmp/coord-*`；提交前跑 AGENTS.md 门禁（fmt / clippy / check-panics / audit / deny / check-doc-refs）。

## Rulings

1. **测试放 `coord-java-sdk`，不是 `java-example`。** #172 的验收对象是 SDK 的「示例可跑」；`java-example` 是 raw-stub 演示，放进它无法证明 SDK 面可用。SDK 已有 `-Pit` profile 与 CI 入口，复用即可。
2. **六个服务全跑。** v2 GA 的能力矩阵包含 Storage；只在集群配置里打开 `[object_storage]`，不给测试加跳过分支。
3. **OCC 冲突是结果，不是异常。** 断言 `CommitResult.committed=false` 且 `commitTs=0`；若 SDK 抛异常，说明语义漂移，测试该红。
4. **Watch 用「未来 revision」起订。** `start_revision = base+1`（base 在本事务 commit 前从 TSO 取），流建立晚于 commit 也不会漏事件，避免用 sleep 赌时序。
5. **显式 `http_addr`。** 服务端默认 `http = grpc+10`；dev 集群的 `raft_addr` 恰好占这个端口，raft RPC 监听会失败（单节点仍能选主，但夹具本身是坏的）。v1/v2 两种模式都写死独立 http 端口。
6. **CI 里 v1 与 v2 集群并存。** 两套 Agent 端口不同；v1 的现有套件不受影响。job 结束（含失败）都要 stop 两套，避免端口泄漏。
7. **启动前 fail-closed，v2 每次 start 清空自己的数据目录。** 机器上可能已有别的 `coord` 占着默认端口；`wait` 只探端口，会把陌生进程当成自己的集群。v2 夹具不继承上一次运行的未 apply 决策（`COORD_KEEP_DATA=1` 才保留现场）。

## Review Focus

- v2 集群里 v1 RPC 被冻结（#173）——测试绝不能退化成 v1 调用；SDK 的 v2 方法是否真的打到 `coord.*.v2`？
- 跨 Region 事务是否仍是一个 Watch 原子批，而不是两条事件？
- `read_ts=0` 与显式 `readTs=commitTs` 的可见性差异：提交后的读是否等到 apply 完成，而不是偶发空读？
- CI 是否在没有 v2 集群时也会红（fail-closed），而不是静默跳过？
- 同一 job 里两套集群的端口/数据目录是否互相覆盖？

## File Structure

- Modify: `scripts/ci-java-it-cluster.sh` — `v1|v2` 模式、v2 配置生成、独立 RUN_DIR/端口、显式 `http_addr`。
- Create: `coord-java-sdk/src/test/java/cn/byteforce/coord/sdk/KvTV2ClusterIntegrationTest.java` — 真实集群 e2e。
- Modify: `.github/workflows/ci.yml` — `java-example-it` job 启动 v2 集群并跑 SDK `-Pit`。
- Modify: `coord-java-sdk/README.md` — 本地跑 v2 e2e 的两条命令。
- Modify: `docs/adr/0015-kv-t.md` — 验收锚点补 Java 真实集群用例（一句话）。

## Tasks

### Task 1: v2 dev cluster mode

**Files:**
- Modify: `scripts/ci-java-it-cluster.sh`

**Interfaces:**
- Produces: `scripts/ci-java-it-cluster.sh start v2` / `stop v2` / `env v2`；v2 Agent 默认 `127.0.0.1:19537`。

- [ ] **Step 1: 参数化模式**

`start|stop|env|wait` 接受第二个位置参数 `v1|v2`（缺省 `v1`）。`RUN_DIR` 默认 `.coord-dev-cluster`（v1）或 `.coord-dev-cluster-v2`（v2）；端口默认 v1 `19527/50051/50061`、v2 `19537/50151/50161`。

- [ ] **Step 2: 写 v2 配置**

v2 server.toml 追加（v1 不变）：

```toml
[cluster]
cluster_name = "coord-dev-cluster-v2"
bootstrap = true
[[cluster.initial_nodes]]
id = 1
grpc = "127.0.0.1:50151"
raft = "127.0.0.1:50161"
[multi_raft]
enabled = true
# 两个数据 Region："a/" 与 "z/" 落在不同 Region，供跨区事务/范围读
[[multi_raft.initial_regions]]
id = 1
start_key = ""
end_key = "m"
[[multi_raft.initial_regions]]
id = 2
start_key = "m"
end_key = ""
[multi_raft.pd]
enabled = true
target_replicas = 1
[object_storage]
enabled = true
[kv_t]
enabled = true
```

两种模式都写 `[network] http_addr = "127.0.0.1:<grpc+20>"`。
启动前逐个检查端口未被占用（占用即 die）。`start v2` 默认清空 `$RUN_DIR/{server,agent}`；`COORD_KEEP_DATA=1` 保留。

- [ ] **Step 3: 启动并验证**

```bash
cargo build --workspace --bins
bash scripts/ci-java-it-cluster.sh start v2
bash scripts/ci-java-it-cluster.sh wait v2
```

预期：v2 Agent/Server 就绪，server.log 无 `Raft RPC server error`。

### Task 2: Java SDK real-cluster e2e

**Files:**
- Create: `coord-java-sdk/src/test/java/cn/byteforce/coord/sdk/KvTV2ClusterIntegrationTest.java`

**Interfaces:**
- Consumes: v2 Agent（`COORD_V2_AGENT_HOST/PORT`，默认 `localhost:19537`）、`CoordClient.create`、`tsoV2()/kvV2()/txnV2()/watchV2()/leaseV2()/storageV2()`。

- [ ] **Step 1: 写第一个失败用例（TSO+KV）**

`@BeforeAll` 建 client 并做一次 `getTimestamp()` 探活；不可达时抛 `IllegalStateException`（含 `scripts/ci-java-it-cluster.sh start v2`）。用例：TSO 单调、批量左闭右开、Put 的 `commit_ts` 成为 `mod_revision`、`prevKv` 生效。

- [ ] **Step 2: 运行，确认无 v2 集群时红**

```bash
cd coord-java-sdk
JAVA_HOME=/opt/jdk-21.0.12.1+1 \
  /opt/apache-maven-3.9.16/bin/mvn -B verify -Pit \
  -Dtest=KvTV2ClusterIntegrationTest
```

预期：`coord-agent is not reachable at ...`（先停掉 v2 集群）。

- [ ] **Step 3: 补齐六个数据面**

有序用例（唯一 key 前缀）：

1. TSO + KV Put/Range/Delete/Put(prevKv)；
2. 跨 Region Txn（`a/...` + `z/...`）→ 显式 `readTs=commitTs` 读到两键；同事务在 Watch 中是一个原子 `Event`；
3. OCC 冲突：会话读后另事务写同键 → `CommitResult.committed=false`、`commitTs=0`；
   写方提交后先按 `readTs=commitTs` 等可见性，再做后续直写（避免直写越过 pending 决策，#244）；
4. Lease：grant → `putWithLease` → TimeToLive 列出 key → revoke 后 `readTs=revokeCommitTs` 读不到；
5. Storage：put/get/stat/delete roundtrip，重复 put 返回 `ALREADY_EXISTS` 原码；
6. 跨 Region Range 扇出：范围读同时命中两个 Region 的 key。

- [ ] **Step 4: 跑绿**

启动 v2 集群后重复 Step 2 命令；`mvn verify -Pit` 全绿（含原有 v1 IT，v1 集群仍在跑）。

### Task 3: CI 与文档

**Files:**
- Modify: `.github/workflows/ci.yml`（`java-example-it` job）
- Modify: `coord-java-sdk/README.md`
- Modify: `docs/adr/0015-kv-t.md`

- [ ] **Step 1: CI 起两套集群**

在现有 v1 `start` 之后加 `scripts/ci-java-it-cluster.sh start v2`；SDK `-Pit` 步骤注入 `COORD_V2_AGENT_PORT=19537`；job 末尾（`if: always()`）停止两套。

- [ ] **Step 2: README**

写明本地两条命令：`start v1` + `start v2`，以及 `COORD_V2_AGENT_*` 覆盖。

- [ ] **Step 3: ADR 锚点**

在 ADR-0015 验收锚点表补 `KvTV2ClusterIntegrationTest`（Java 真实集群六服务）。

- [ ] **Step 4: 门禁**

`cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-panics.sh`、`cargo audit`、`cargo deny check`、`bash scripts/check-doc-refs.sh`；Java `mvn -B verify`（默认 profile）与 `-Pit`。

- [ ] **Step 5: 交付**

PR（引用 #172）→ CI 全绿 → merge（`--merge`）→ 删分支；#172 回填进展。

## Verification Anchors

| 行为 | 证据 |
|:--|:--|
| 真实 v2 集群可重复启动 | `scripts/ci-java-it-cluster.sh start v2` + `wait` |
| TSO 单调 / KV revision | `KvTV2ClusterIntegrationTest#tsoAndKv...` |
| 跨 Region 事务 + Watch 原子批 | `#crossRegionTransactionIsOneWatchBatch` |
| OCC 冲突不抛异常 | `#occConflictIsReturnedNotThrown` |
| Lease 撤销删键 | `#leaseRevokeDeletesBoundKeys` |
| Storage + 错误码 | `#storageRoundTripAndDuplicatePutIsRejected` |
| 跨 Region Range 扇出 | `#rangeSpansRegions` |
| CI fail-closed | 无 v2 集群时同命令红（Step 2 记录） |
| 端口占用 fail-closed | 已有进程占用 51051 时 `start v2` 直接失败 |
| 重复运行稳定 | fresh 集群上同一套件连续 3 次 6/6，server 无 apply 停止日志 |

## Residual（本切片不修）

- **#244**：直写（`PutV2`/`DeleteV2`）可以在 pending CommitDecision 之前推进
  `current_revision`，导致决策永远 apply 不了、`safe_ts` 卡死。真实集群 e2e 在
  stop/start 后踩到过（`decision apply stopped` / `passed unapplied decision`）。
  本切片的测试用「事务提交后先等可见性」避开窗口，并由 `start v2` 的 fresh 目录
  避免继承上一轮状态；server 侧修复与回归测试归 #244（GA 前必须收口，#177）。
- Rust client 的后台 KeepAlive / 流式 `open_put` 仍随 #170 的生命周期语义，未在本切片。
