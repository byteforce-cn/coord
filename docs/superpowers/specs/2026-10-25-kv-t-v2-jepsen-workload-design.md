# KV-T v2 的 Jepsen 工作负载（并发线性一致性证据）

**Owner:** 测试/一致性 · **Last verified:** 2026-10-29 · **状态:** 已落地（PR #292）· **Issue:** #272

## 1. 意图与为什么

KV-T（v2 数据面：TSO/MVCC/跨 Region 事务）的 GA 证据里**没有并发线性一致性判据**：

- 仓内 Jepsen 只能讲 v1。v1 数据面在 v2 集群被策略拒绝（`KvTState::v1_gate`，
  `coord-server/src/kvt/mod.rs:156`，ADR-0015 D7/D13），所以 `make soak` 打 v2 集群
  是 100% `FAILED_PRECONDITION`，零信号。
- 当前 72h 证据来自 `scripts/soak-72h.sh` → `kv_t_g6_soak_v2_test`：单线程、单客户端、
  每轮「写后读回」，`wait_read` 读不到就重试到超时
  （`coord/tests/kv_t_g6_soak_v2_test.rs:134`）。它证的是**长稳/活性/TSO 单调/重启收敛**，
  不证任意时刻的读一致性——瞬时不合法陈旧读会被重试吞掉，而那正是 Jepsen soak checker
  在 v1 上抓到过的 bug 类。

本设计补上这个缺口：给 Jepsen 加 v2 客户端与工作负载，得到一个**能判陈旧读的
v2 判据**，可在 kill/pause/分区下跑短矩阵，也能跑 72h 浸泡。

## 2. 目标 / 非目标 / 反目标

**目标**

1. Jepsen（Clojure）能用 v2 契约直接驱动 `coord.tso.v2` / `coord.kv.v2` / `coord.txn.v2`。
2. 一个新 checker 判 v2 快照读与事务的**精确**期望（`commit_ts` 让期望可计算，不需要
   搜索），负控制 fixture 先行。
3. 短矩阵（none/kill/partition-halves）进 lab 矩阵与 nightly；72h 用同一 workload。
4. 证据可复跑、可回放（seed + MANIFEST），与现有 `collect-evidence.sh` 一致。

**非目标（本轮不做，且要在文档里写明）**

- `watch.v2` / `lease.v2` / `storage.v2` 的 Jepsen 面（各自有独立判据，后续单独设计）。
- split/merge 期间的进程级故障注入（ADR-0015 §3.3 的 D10 裁定：不做）。
- 用 knossos 判 v2（见 §5.3：v2 的期望是确定的，knossos 反而更弱且更贵）。
- 发送 `coord-routing-epoch` 头：服务端不带该头就跳过 epoch 校验
  （`kvt/services.rs:170`），而本轮不做路由表缓存，带一个自己不会刷新的版本号
  只会制造假 `EPOCH_STALE`。客户端靠「逐节点重试 + NOT_LEADER 轮换」取路，与 v1 同。
  （`EPOCH_STALE` 仍留在分类表里：被 split 围栏拒绝的直写也会返回它。）

**反目标（技术上跑通但等于失败）**

- 客户端内部静默重试，让「读到了旧值」在 history 里看不见。
- 用 `read_ts=0` 的读去判陈旧——safe_ts 语义下那只能判活性。
- 门槛不足（样本太少）却判绿。
- 改坏 v1：任何 v1 配置/矩阵的结果必须字节级不变。

## 3. 约束

- **离线 lab**：控制机/容器不出网，`lein -o`；不能引入构建期下载（protoc 插件不可行）。
- **无 protoc**：v1 的做法是手写 `DescriptorProtos` + `DynamicMessage`
  （`jepsen/src/java/jepsen/coord/CoordRpc.java` 1735 行）。v2 沿用，字段号必须与
  `apis/contracts/proto/coord/*/v2/*.proto` 逐字一致。
- **v2 配置有前置**：`[kv_t].enabled=true` 要求 `multi_raft.enabled` + `multi_raft.pd.enabled`，
  且与 `legacy_migration` 互斥（`config.example.toml:158`）。现有 v1 配置必须保持默认。
- **失败必须显形**：`FAILED_PRECONDITION`（v1 门禁 / 能力未交付）、`ABORTED+EPOCH_STALE` 等
  都要按类别进 history，不能折叠成一种错误。

## 4. 做法（已选 A）

| 做法 | 取舍 | 结论 |
|:--|:--|:--|
| **A 手写 v2 descriptors**（沿用 v1） | 零新依赖、离线可用、与其他面同构；代价 ~500–700 行机械代码 + 漂移风险 | **选它**，用 wire 漂移检查兜住 |
| B 依赖 `coord-java-sdk` | 复用已测 v2 客户端；但 jar 要进离线 m2，且 SDK 用 protobuf 4.28 vs jepsen 3.25.5，需一次运行时升级验证 | 否 |
| C Rust driver 子进程 | 复用 `coord-client` 的 `txn_v2`/`watch_v2`，最快；但每 op 一次 IPC，且错误分类要跨进程回传才不掩盖故障 | 否 |

选 A 的理由：证据链里最不该有的就是「客户端自己掩盖了失败」，而 A 与 v1 完全同构、
没有跨进程/跨版本的语义损耗；慢出来的两天换的是长跑时的可信度。

## 5. 设计

### 5.1 线路层：`CoordRpc.java` 增三个 file descriptor

新增 `tsoV2FileProto()` / `kvV2FileProto()` / `txnV2FileProto()`（package `coord.tso.v2` 等），
以及方法描述符常量：`TSO_V2_ALLOCATE`、`TSO_V2_GET_TS`、`KV_V2_PUT`、`KV_V2_RANGE`、
`KV_V2_DELETE`、`TXN_V2_BEGIN`、`TXN_V2_READ`、`TXN_V2_COMMIT`、`TXN_V2_ABORT`。
消息体逐字段照抄契约（`kv/v2/kv.proto:50-105`、`txn/v2/txn.proto:40-97`、`tso/v2/tso.proto:24-45`），
嵌套消息 `coord.kv.v2.KeyValue` 用 `repMsg` 引用 `.coord.kv.v2.KeyValue`。

**错误分类必须读 trailer**：服务端把结构化错误码放在 trailer `x-coord-error-code`
（`coord-core/src/error_code.rs:31`、`attach`），而 gRPC 码本身不足以分类
（`SAFE_TS_LAG` 与「连接失败」都是 `UNAVAILABLE`，`TXN_CONFLICT` 与 `EPOCH_STALE`
都是 `ABORTED`，见 `error_code.rs:190-194`）。`CoordRpc` 增 `errorCode(StatusRuntimeException)`：
从 `getTrailers()` 读该键，缺失时返回 nil（老路径不受影响）。分类顺序：trailer 优先、
gRPC 码兜底。

**漂移检查（硬门禁）**：新增 `check-kv-t-wire.clj`（`jepsen/scripts/` 下）解析
`apis/contracts/proto/coord/{kv,txn,tso}/v2/*.proto` 的 `message/rpc/字段(名,号,类型)` 文本，
与 JPype 反射出的 `FileDescriptor` 逐项比对；不一致即非零退出并列出差异。
挂在 `make checkers` 最前面（红则拒跑）。

### 5.2 配置层：`db.clj` 的 `--kv-t` 模式

- 新 CLI：`--kv-t`（布尔，默认 false）。
- `--kv-t` 时 `config-str` 追加：

  ```toml
  [multi_raft]
  enabled = true
  [[multi_raft.initial_regions]]   # id 1: "" -> "m"
  [[multi_raft.initial_regions]]   # id 2: "m" -> ""
  [multi_raft.pd]
  enabled = true
  target_replicas = 3
  scheduler_paused = true
  [kv_t]
  enabled = true
  ```

- **固定 2 Region 布局**（与 `tests/common/v2_cluster.rs` 一致），不用 `--regions`；
  `--kv-t` 与 `--regions` 同时给出 → CLI 报错（两种布局不能混）。
- `scheduler_paused = true` 是刻意的：自动 split 会把旧 revision 变成设计内的
  `HISTORY_UNAVAILABLE`，混进数据面判据就是假红；split/merge 由 g5 套件负责。
- 不传 `--kv-t` 时 `config-str` 输出**字节级不变**：扩展现有
  `jepsen/scripts/test-multi-raft-config.clj` 断言这一点。
- 鉴权保持 `auth_enabled = true`（lab 现状）。P0 冒烟先证明 root CCT 能调
  `tso.v2/kv.v2/txn.v2`（Rust 夹具用 `auth_enabled = false`，这条路径在 Jepsen 是新的）。

### 5.3 Checker：新增 `mvcc.clj`（`jepsen/src/jepsen/coord/` 下）

v2 的判据可以做成**确定性**的，因为每次写都带回 `commit_ts`、每次读都回显实际
`read_ts`。对每个 key，`:ok` 写按 `commit_ts` 构成全序；于是：

1. **快照读的期望值**：对 `:ok` 读（`read_ts = R`，显式或回显的 safe_ts），返回值必须等于
   「该 key 所有 `:ok` 写中 `commit_ts <= R` 的最后一条」的值；
   tombstone（空 value）算一个值。`mod_revision <= R`、`create_revision <= mod_revision`。
2. **可能生效的写（`:info`）**：响应丢失的写按 `windex` 的既有口径进「可能集」——
   它的**候选位置就是本次 op 尝试过的每个 ts**（`COMMIT_REJECTED` 换号重试会留下多个）。
   若候选集合跨过读的 `R`（有尝试 `<= R`、也有 `> R`），该读看到旧值与新值都合法；
   但可能集绝不作为「必须可见」的依据。这是避免假红的唯一来源，必须显式建模。
3. **读不到未来**：`mod_revision > read_ts` ⇒ 立即 invalid（`read-from-future`）。
4. **safe_ts 单调（只对 `read_ts=0` 的读）**：按 (client, node) 分组，`read_ts=0` 的读
   回显的 `read_ts` 不得回退；基线在 nemesis `:start` 与节点重启后重置。历史读
   （显式 `read_ts>0`）**不受此约束**，它只断言「回显 == 请求的 ts」。
5. **TSO**：每个客户端的取号严格递增；所有客户端的 `:ok` 写 `commit_ts` 互不相同
   （前提判据——重复即 history 不成立，记 invalid 而不是绿）。
6. **事务原子性（`txnv2`）**：同一 `commit_ts` 的写集对任意 `R >= commit_ts` 的读必须
   **全有**，对 `R < commit_ts` **全无**；`TXN_CONFLICT` 是合法 `:fail`，其写集不进必须可见集。
   响应丢失（`:info`）时 `commit_ts` **未知**（由 sequencer 分配）：客户端先尝试用同一
   `request_id` 再 `Commit` 一次把 ts **问出来**（幂等，服务端返回首次结果）；问不出来
   （会话已随节点重启/TTL 消失）时，该事务按「位置未知的原子块」建模：checker 不再断言它
   相对某个 `R` 的先后（那是猜），但**仍然断言全有或全无**（按写集的值身份两两比对）。
   原子性不需要 ts，只有「相对某次读的先后」才需要。
7. **样本门槛（fail-closed）**：`--v2-min-writes` / `--v2-min-reads` / `--v2-min-ts`
   （默认各 200），低于门槛一律 `{:valid? false :reason :insufficient-sample}`。
   矩阵格（45s）用 `--rate 20`（≈900 op）远超门槛；72h 浸泡用 0.5 ops/s（≈13 万 op）。
8. **收尾收敛**：结尾的全量读（`read_ts=0`）必须看到每个 key 的最新 `:ok` 写；
   `safe_ts` 永久滞后本身算失败（这正是 `SAFE_TS_LAG` 的活性面）。

`--checker linear` 对这两个 workload **在参数解析期拒绝**（knossos 无法表达多版本期望）。

### 5.4 工作负载：`kvv2` 与 `txnv2`

两个独立 workload 名（而不是给现有 workload 加开关）：v1 矩阵与 `soakfull` 的 mix
不受影响，且 `soakfull` 的「未实现的面构造期失败」规则不会被绕过。

- **`kvv2`**：`tso GetTs` 取**一个**号 → `kv.v2 Put(commit_ts)` / `Delete(commit_ts)` /
  `Range(read_ts=0)` 与 `Range(read_ts=<历史 ts>)` 混合；key 分布跨两个 Region
  （`/jepsen/v2/k/*` 落 region 1，`zz-jepsen-v2-*` 落 region 2）。
  `--v2-historical-read-ratio` 控制历史读比例，历史读只取最近
  `--v2-history-window-secs`（默认 600）内的 ts；`HISTORY_UNAVAILABLE` 合法但计入
  一个失败率上限（>1% 即采样前提不成立 ⇒ invalid）。
  并发度 `1n`（lab 默认，3 客户端）：并发直写会让 `commit_ts <= current_revision` 的
  写被 `COMMIT_REJECTED` 拒，这是**常态而非故障**（见 §5.5），其比例进 summary。
- **`txnv2`**：`Begin` → `Read`（读集含两个 Region 的 key）→ `Commit`（写集跨 Region）。
  生成器用**银行账本**：固定 key 集合、值域小、总和守恒，于是原子性违规既可能被
  checker 第 6 条抓到，也会破坏总和（更强的信号）。冲突重试就是重开一次会话（新 op）。

### 5.5 客户端：报告而非掩盖

`client.clj` 新增 `invoke-kv2-*` / `invoke-txn2-*`。分类按 `x-coord-error-code` trailer
优先、gRPC 码兜底（§5.1）：

| 情形（trailer / gRPC 码） | 记录 |
|:--|:--|
| `COMMIT_REJECTED` / `FAILED_PRECONDITION` | 该 `commit_ts` 已被本地 revision 越过，**未生效**；换新号、同 `request_id` 重试（服务端幂等去重回放首次 `commit_ts`，所以最终 ts 是确定的）；有界（8 次）后用尽 ⇒ `:info :commit-rejected` |
| `SAFE_TS_LAG` / `UNAVAILABLE` | `:info :safe-ts-lag`；**不轮换节点、不换 `read_ts`**（换 ts 重读等于掩盖） |
| `EPOCH_STALE` / `ABORTED` | 被围栏拒绝 ⇒ 轮换节点重试（本轮不发 epoch 头，只会来自 split 围栏）；用尽 ⇒ `:info` |
| `TXN_CONFLICT` / `ABORTED` | `:fail :txn-conflict`（合法结果，checker 白名单） |
| `NOT_LEADER`（trailer）/ 无码 `UNAVAILABLE` / 连接失败 | 轮换节点；用尽 ⇒ `:info :unavailable-maybe-applied`（与 v1 同） |
| 会话相关（Begin 与 Commit 不同节点、`TXN_NOT_FOUND`、TTL 过期） | 中止旧会话，`recur` 开新会话；op 记 `:retried` |

事务会话绑定节点，任何节点切换都以新会话重开；history 里那是一条新 op，不会被
合并成「一次成功的事务」。

`synchronize` 的 `setup!` 探针也要分叉：现有探针打 **v1** `kv-range`
（`client.clj:2262`），在 `[kv_t]` 集群上必然 `FAILED_PRECONDITION` 而 60 次后抛
「did not become ready」。`--kv-t` 时改用 `tso.v2 GetTs` + `kv.v2 Range(read_ts=0)`
（两者都成功才算 ready），失败消息仍带最后一次的错误码与端点（保持既有的可诊断性）。

### 5.6 Nemesis / 门禁 / 证据

- Nemesis 直接复用：`kill`、`pause`、`partition-halves`、`soak`。v2 的 TSO/sequencer
  在 region 0 leader 上，而 3 个节点都是 region 0 成员，所以 `kill` 随机节点天然覆盖
  「决策面 leader 丢失」。
- `jepsen.coord.gates`（可用率/RTO）形状不变，需验证它对 v2 op 的 `:type/:time` 仍成立。
- summary 额外报三个数：`COMMIT_REJECTED` 重试率、`SAFE_TS_LAG` 次数、历史读失败率
  （都不是失败，但都要可见，否则「判据靠重试活下来」看不出来）。
- 新增 Make 目标：`matrix-m3`（`kvv2`/`txnv2` × none|kill|partition-halves，45s）、
  `soak-kv-t`（默认 2h，`SOAK_TIME_LIMIT=259200` 即 72h）。
- 证据：`scripts/collect-evidence.sh soak-kv-t`，MANIFEST 含 commit/seed/checker 判据。

### 5.7 文档（单一归属）

- `jepsen/README.md`：怎么跑、checker 判什么、**不判什么**（watch/lease/storage v2、
  split/merge、knossos）。
- `docs/production/ops/kv-t-ga-gate.md`：§2 的 #174/#175 证据行指向新 oracle；
  旧脚本 soak 降为交叉验证，不再作为唯一长稳证据；未达标项按实际结论增删。

## 6. 实施顺序（每阶段自带 fail-closed 验收）

| 阶段 | 内容 | 验收 |
|:--|:--|:--|
| P0 | db `--kv-t` 配置 + wire descriptor + trailer 分类 + `check-kv-t-wire.clj`；一个 `tso` 只取号的 20s 冒烟 | docker lab 起 v2 集群、root CCT 取号成功、`kv.v2 Range` 成功；v1 配置字节级不变测试绿；漂移检查绿 |
| P1 | `kvv2` + `mvcc` checker 的**负控制 fixture 先行**（`jepsen/scripts/` 下的 `mvcc-fixtures/`） | 12 个 fixture（含 stale / read-from-future / fabricated / tombstone-resurrection / shared-commit-ts / txn-half-visible）全按预期；`make checkers` 绿 |
| P2 | `kvv2` 端到端：none → kill → partition-halves（`--rate 20`） | 3 轮绿且样本门槛达标；红的时候能给出反例 |
| P3 | `txnv2`（银行账本）+ 原子性判据 | 同上；冲突率与总和守恒出现在 summary |
| P4 | 矩阵/nightly/证据接线 + 文档 | `nightly` 绿；`collect-evidence.sh soak-kv-t` 产出 MANIFEST |
| P5 | 2h 浸泡，然后 72h（在 GA 最终提交上） | 报告通过；gate 文档更新 |

## 7. 风险与待验证

- **v2 + 鉴权**：Jepsen lab 用 `auth_enabled = true`，Rust 夹具用 false。P0 先证 root CCT
  能调三个 v2 服务；若权限不足，要在服务端授予（不是关鉴权）。
- **`COMMIT_REJECTED` 是常态**：3 客户端并发直写时，「取号早于别人已应用的 revision」
  会周期性发生（`mvcc.rs:1498`）。它的比例进 summary 而不是判据，但**必须**在 P2 的
  第一轮里确认它按预期被重试消化（不消化就是假红）。
- **幂等去重的范围**：只依赖「同一 `request_id` 的重试拿回的 `commit_ts` 对本次 op 是
  确定的」。若该缓存是单节点进程内的，同一 `request_id` 可能在两个节点各落一次
  （同一个值、两个 ts）——对「值层面」的判据无害，但 checker 不得假设「一个值只有一个 ts」。
- **history 规模**：0.5 ops/s × 72h ≈ 13 万 op，checker 是 O(n log n)；
  history.edn 按现有规则超过 5MiB 压 gzip。
- **protobuf 3.25.5 与契约**：v2 proto 全是 plain proto3，无 `optional`/map，风险低；
  wire 检查覆盖类型，不靠人眼。
- **`target_replicas = 3` 与 3 节点**：Jepsen 固定 3 节点集群（`db.clj:45`），一致。
- **历史读的保留窗口**：`watch_retention` 默认 24h，72h 长跑必须让历史读只指向最近
  窗口（`--v2-history-window-secs`），否则会把设计内的 GC 当成失败。

## 8. 留给实现者的

- fixture 的具体 history 构造与命名细节。
- `kvv2` 的历史读比例与 key 分布的默认值（在 lab 上按吞吐调，语义不变）。
- `txnv2` 账本的 key 数与冲突率目标（先按 v1 `txn` 的实测口径起步）。
