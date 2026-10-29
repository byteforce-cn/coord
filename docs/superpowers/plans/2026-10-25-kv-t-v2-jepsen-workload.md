# KV-T v2 的 Jepsen 工作负载 — 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 让仓内 Jepsen 直接驱动 `coord.{tso,kv,txn}.v2`，并用一个确定性 MVCC checker 判并发下的快照读与事务原子性，产出可复跑证据。

**Architecture:** 手写 `DescriptorProtos`（沿用 v1 的 `CoordRpc` 模式）→ `proto.clj` 请求/响应 helper → `client.clj` 的 v2 invoke（按 `x-coord-error-code` trailer 分类，重试留在同一 op 内并留痕）→ 两个 workload（`kvv2`/`txnv2`）→ 一个 O(n log n) checker（`mvcc.clj`），期望由 `commit_ts`/`read_ts` 直接算出，不需要搜索。

**Tech Stack:** Clojure 1.12 + jepsen 0.3.14-SNAPSHOT + grpc-java 1.68 + protobuf-java 3.25.5；Clojure 侧不新增依赖，不引入 protoc。

**Spec:** `docs/superpowers/specs/2026-10-25-kv-t-v2-jepsen-workload-design.md`

**Issue:** #272（GA gate 行：#174 / #175）

> 路径约定：`Files:` 里的 `{name}` 表示**本任务要新建**的文件；命令一律在控制机的 `/root/coord-test`
> 里执行（`./scripts/...` 是相对该目录的写法）。

## Global Constraints

- 3 节点集群（`db/server-count`），`auth_enabled = true`，**不得**为 v2 关鉴权。
- v2 配置逐字：`[multi_raft] enabled = true` + 两个 `[[multi_raft.initial_regions]]`（`""→"m"`、`"m"→""`）+ `[multi_raft.pd] enabled = true / target_replicas = 3 / scheduler_paused = true` + `[kv_t] enabled = true`。
- `--kv-t` 与 `--regions` 互斥；不传 `--kv-t` 时 `config-str` 输出**字节级不变**。
- 客户端**不发** `coord-routing-epoch` 头。
- 错误分类以 trailer `x-coord-error-code` 优先、gRPC 码兜底；重试上限 8 次，重试痕迹进 op 的 `:retried`。
- 直写每次用 `tso.v2.GetTs` 取**一个**号（不用批分配）。
- 样本门槛默认 `200/200/200`（writes/reads/ts）；矩阵格 `--rate 20`、`TIME_LIMIT 45`；浸泡 `--rate 0.5`。
- 历史读只指向最近 `600s` 内产生的 ts；`HISTORY_UNAVAILABLE` 合法但失败率 >1% ⇒ invalid。
- 门槛缺失/样本不足一律 fail-closed（`{:valid? false :reason :insufficient-sample}`），不得默认绿。
- 所有临时文件落 `/tmp`；改动走 分支 → PR → CI 绿 → merge。

## Review Focus

| 会咬人的输入/条件 | 期望行为 | 由谁的测试钉住 |
|:--|:--|:--|
| 写响应丢失后重试成功（`:info` 的候选 ts 跨过某次读的 `R`） | 读到旧值或新值都合法，不能判红 | Task 3 `expect-valid-info-write-window.edn` |
| 换主后 `read_ts=0` 回显的 safe_ts 低于本节点之前的值 | 在 nemesis `:start` 后重置基线，不判红 | Task 3 `expect-valid-safe-ts-after-restart.edn` |
| 历史读命中 tombstone（空 value） | 返回空 value + `mod_revision`，不得当成「不存在」 | Task 3 `expect-valid-tombstone-read.edn` / `expect-invalid-tombstone-resurrection.edn` |
| 事务响应丢失、`commit_ts` 未知，读在 `R < ts` 时返回旧值 | 不得判红；但写集半可见必须判红 | Task 3 `expect-valid-txn-unknown-ts.edn` / `expect-invalid-txn-half-visible.edn` |
| `COMMIT_REJECTED` 用尽重试预算 | 记 `:info`（可能生效），不是 `:fail` | Task 3 `expect-valid-commit-rejected-info.edn` + Task 4 的分类表 |

---

### Task 1: v2 集群配置模式（`--kv-t`）与字节级不变守卫

**Files:**
- Modify: `jepsen/src/jepsen/coord/db.clj:98-146`（`config-str`）、`jepsen/src/jepsen/coord/db.clj:301-331`（`coord` 构造器）
- Modify: `jepsen/src/jepsen/coord.clj:1463-1720`（`cli-opts`）、`jepsen/src/jepsen/coord.clj:1363-1400`（`coord-test`）
- Create: `jepsen/scripts/{test-kv-t-config.clj}`
- Modify: `jepsen/lab/Makefile:466`（`checkers` 目标）
- Test: 新脚本自带的断言（无单测框架，沿用 `test-multi-raft-config.clj` 的 `passes`/`failures` 计数风格）

**Interfaces:**
- Produces: `(db/kv-t-toml) -> string`（v2 的三个 TOML 段，逐字见 Global Constraints）；db 记录新键 `:kv-t?`（boolean）；CLI 新旗标 `--kv-t`。
- Consumes: 现有 `regions/multi-raft-toml`、`db/coord`、`db/server-count`。

- [ ] **Step 1: 写失败的配置断言**

`test-kv-t-config.clj` 的断言（每条一个 `(check name expr)`）：
1. `(str/includes? (db/config-str legacy-db test node1) "[multi_raft]")` 为 false；
2. 同一 legacy 输入**逐字节不变**：脚本内嵌 `legacy-golden`（从当前 `config-str` 输出原样粘贴），断言 `(= legacy-golden (db/config-str legacy-db test node1))`；
3. `(db/config-str v2-db test node1)` 含 `"[kv_t]\nenabled = true\n"`、`"scheduler_paused = true"`、两段 `initial_regions`（id 1 `""`→`"m"`、id 2 `"m"`→`""`）、`target_replicas = 3`；
4. 三个节点的 v2 配置除 `[node] id/name` 与 `root_password` 外**逐行相同**；
5. 不含 `legacy_migration` 与 `allow_unmigrated`。

- [ ] **Step 2: 跑它，确认红的**

Run: `make stage && vagrant ssh control -c 'cd /root/coord-test && LEIN_ROOT=true lein -o run -m clojure.main ./scripts/test-kv-t-config.clj'`
Expected: FAIL（`db/kv-t-toml` 不存在 / `:kv-t?` 未接）。

- [ ] **Step 3: 实现 `kv-t-toml` 并接进 `config-str`**

`db.clj` 新增 `(defn kv-t-toml "…" [])`，返回 Global Constraints 里那三段（region 表**写死**在本函数，不复用 `regions/multi-raft-toml`——v2 布局与 `--regions` 无关）。`config-str` 在 `(when multi-raft multi-raft)` **之前**追加 `(when (:kv-t? db) (kv-t-toml))`；`coord` 构造器加 `:kv-t? (:kv-t opts)`。

- [ ] **Step 4: 跑它，确认绿的**

Run: 同 Step 2。Expected: 5 条断言全 PASS，退出码 0。

- [ ] **Step 5: 加 CLI 旗标与互斥校验**

`cli-opts`（布尔旗标必须用 `:assoc-fn`，见 `--no-jitter` 的注释）：
```clojure
[nil "--kv-t" "KV-T v2 集群（[kv_t].enabled=true）：只服务 v2 数据面；固定 2 Region 布局（--regions 是 v1，互斥）"
 :default false
 :assoc-fn (fn [m _ _] (assoc m :kv-t true))]
```
`coord-test` 里（在 `db/coord` 之前）：
```clojure
(when (and (:kv-t opts) (:regions opts))
  (throw (ex-info "--kv-t 与 --regions 互斥：v2 用固定 2 Region 布局" {:kv-t true :regions (:regions opts)})))
```

- [ ] **Step 6: 配置断言接进 `make checkers`（红则拒跑）**

`jepsen/lab/Makefile` 的 `checkers` 目标末尾加 `$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main ./scripts/test-kv-t-config.clj)`。

- [ ] **Step 7: 跑 `make checkers`，确认含新断言**

Run: `make checkers`。Expected: 新脚本 PASS 计数出现，退出码 0。

- [ ] **Step 8: Commit**

```bash
git add ./jepsen/src/jepsen/coord/db.clj ./jepsen/src/jepsen/coord.clj ./jepsen/scripts/test-kv-t-config.clj ./jepsen/lab/Makefile
git commit -m "test(jepsen): add the KV-T v2 cluster config mode (--kv-t)"
```

---

### Task 2: 线路层（descriptors + trailer 分类 + P0 冒烟）

**Files:**
- Modify: `jepsen/src/java/jepsen/coord/CoordRpc.java`（三段 file descriptor、方法常量、`errorCode`）
- Modify: `jepsen/src/jepsen/coord/proto.clj`（v2 请求/响应 helper）
- Modify: `jepsen/src/jepsen/coord/client.clj:2262-2296`（`setup!` 探针分叉）、`client.clj:2321-2347`（`coord-client` 记录新字段）
- Create: `jepsen/scripts/{check-kv-t-wire.clj}`、`jepsen/scripts/{smoke-kv-t.clj}`
- Modify: `jepsen/lab/Makefile`（`stage` ship protos、`checkers` 跑漂移检查、`KV_T` 变量）
- Test: `check-kv-t-wire.clj --selftest` + `smoke-kv-t.clj`

**Interfaces:**
- Produces（Java）：`CoordRpc.TSO_V2_ALLOCATE`、`TSO_V2_GET_TS`、`KV_V2_PUT`、`KV_V2_RANGE`、`KV_V2_DELETE`、`TXN_V2_BEGIN`、`TXN_V2_READ`、`TXN_V2_COMMIT`、`TXN_V2_ABORT`（皆 `MethodDescriptor<DynamicMessage,DynamicMessage>`）；`CoordRpc.errorCode(StatusRuntimeException) -> String|null`。
- Produces（Clojure, `proto.clj`）：`(tso-get-ts-req)`、`(tso-get-ts [resp]) -> long`、`(kv2-put-req* {:key :value :commit-ts :request-id})`、`(kv2-range-req* {:key :read-ts})`、`(kv2-delete-req* {:key :commit-ts :request-id})`、`(kv2-range-resp [resp]) -> {:kvs [{:key :value :create-revision :mod-revision :version}] :read-ts long}`、`(kv2-put-resp [resp]) -> {:commit-ts long}`、`(kv2-delete-resp [resp]) -> {:deleted int :commit-ts long}`、`(txn2-begin-req* {:start-ts :ttl-ms})`、`(txn2-begin-resp [resp]) -> {:txn-id bytes :start-ts long}`、`(txn2-read-req* {:txn-id :key})`、`(txn2-commit-req* {:txn-id :request-id :mutations [{:kind :key :value}]})`、`(txn2-commit-resp [resp]) -> {:commit-ts long}`、`(txn2-abort-req* {:txn-id})`。
- Consumes: Task 1 的 `--kv-t`。

- [ ] **Step 1: 写漂移检查脚本（先让它红）**

`check-kv-t-wire.clj`：从 `apis/contracts/proto/coord/{kv,txn,tso}/v2/*.proto` 文本抽出 `(package, message, field-name → number/type)` 与 `(service, method)`，与 `CoordRpc` 的 `FileDescriptor`/`ServiceDescriptor` 逐项比对，输出 `missing`/`extra`/`number-mismatch`/`type-mismatch`，有差异退出 1。
proto 目录解析顺序（**都不存在即 fail-closed 报错，不允许跳过**）：`$COORD_PROTO_DIR`、`protos/coord`、`../apis/contracts/proto/coord`、`/opt/coord/apis/contracts/proto/coord`。
`--selftest`：把期望表里 `kv.v2.PutRequest.value` 的号改成 `99` 再比较，**必须**报出 `number-mismatch` 且退出 0（证明比较器不是空转）。

Run: `... -m clojure.main ./scripts/check-kv-t-wire.clj --selftest`
Expected: FAIL（`TSO_V2_*` 等常量不存在）。

- [ ] **Step 2: 实现三个 file descriptor 与方法常量**

照着 `kvFileProto()`/`txnFileProto()` 的写法加三段，字段逐字对齐契约：
- `coord/tso/v2`：`AllocateRequest{batch_size=1 uint32}`、`AllocateResponse{start_ts=1,end_ts=2 int64}`、`GetTsRequest{}`、`GetTsResponse{ts=1 int64}`、`service TSO{Allocate,GetTs}`；
- `coord/kv/v2`：`KeyValue{key=1,value=2,create_revision=3,mod_revision=4,version=5,lease_id=6}`、`PutRequest{key=1,value=2,lease_id=3,prev_kv=4,request_id=5,commit_ts=6}`、`PutResponse{prev_kv=1,commit_ts=2}`、`DeleteRequest{key=1,range_end=2,prev_kv=3,request_id=4,commit_ts=5}`、`DeleteResponse{deleted=1,prev_kvs=2,commit_ts=3}`、`RangeRequest{key=1,range_end=2,limit=3,read_ts=4,keys_only=5,count_only=6}`、`RangeResponse{kvs=1,count=2,read_ts=3}`、`service KV{Put,Range,Delete}`；
- `coord/txn/v2`：`BeginRequest{start_ts=1,ttl_ms=2}`、`BeginResponse{txn_id=1,start_ts=2}`、`ReadRequest{txn_id=1,key=2,range_end=3,limit=4,keys_only=5}`、`ReadResponse{kvs=1,read_ts=2}`、`Mutation{kind=1(enum),key=2,range_end=3,value=4}`、`CommitRequest{txn_id=1,mutations=2,request_id=3}`、`CommitResponse{commit_ts=1}`、`AbortRequest{txn_id=1}`、`AbortResponse{}`、`service Txn{Begin,Read,Commit,Abort}`。

`TXN_V2_FILE` 的 deps 传 `KV_V2_FILE`（`ReadResponse.kvs` 引用 `.coord.kv.v2.KeyValue`）。
再加 `public static String errorCode(StatusRuntimeException e)`：读 `e.getTrailers()` 的 `x-coord-error-code`，缺失返回 `null`。

- [ ] **Step 3: 跑漂移检查的真比对与自检**

Run: `./scripts/check-kv-t-wire.clj` → Expected: PASS（0 diff）；`--selftest` → Expected: PASS（报出 1 条 `number-mismatch`）。
若 `Mutation.kind` 的枚举名/值域与契约不符，此步会红——按契约文本改正，**不得**改期望表。

- [ ] **Step 4: 写冒烟脚本（先红）**

`smoke-kv-t.clj` 在控制机执行：
1. 用 `db/default-root-password` 鉴权；
2. 逐节点试 `tso.v2 GetTs`，`(assert (pos? ts))`；
3. 连续 20 次 `GetTs`，`(assert (apply < ts-seq))`；
4. `kv.v2 Range(read_ts=0)` 一个全新 key，`(assert (empty? kvs))` 且 `(assert (>= (:read-ts resp) 0))`；
5. 任一失败打印 `errorCode` 与端点，退出 1。

Run: `./scripts/smoke-kv-t.clj`
Expected: FAIL（`proto.clj` 的 v2 helper 不存在 / `setup!` 仍打 v1）。

- [ ] **Step 5: 实现 `proto.clj` helper**

按 Interfaces 的签名实现；`DynamicMessage` 的 repeated message 字段用 `(.getField d "kvs")`（`java.util.List<DynamicMessage>`）逐项读。空 value 必须原样返回长度 0 的 `byte[]`，**不得**转成 nil（tombstone 判定靠它）。

- [ ] **Step 6: `setup!` 探针分叉**

`coord-client` 记录末尾加 `(:kv-t? opts)`；`setup!` 开头 `(if (:kv-t? this) …)`：v2 分支循环 60 次、每轮 `GetTs` + `kv.v2 Range(read_ts=0)`（key 用 `/jepsen/v2/ready`），任一成功即 ready；失败抛的 ex-info 带 `errorCode` 与端点（保留现有可诊断性）。

- [ ] **Step 7: 跑冒烟（docker lab）**

Run:
```bash
cd jepsen/lab && make docker-up docker-upload
make docker-test WORKLOAD=register KV_T=1 TIME_LIMIT=20 NEMESIS=none
```
Expected: 日志出现 `coord cluster is ready`；op 全 `:fail`（v1 门禁 `FAILED_PRECONDITION`，这是**正确**行为）；不因 setup 超时而退出。

- [ ] **Step 8: 漂移检查接进 `make checkers`，并 ship protos + `KV_T`**

`jepsen/lab/Makefile`：
- `stage` 加 `@cp -r $(REPO_ROOT)/apis/contracts/proto $(UPLOAD_STAGE)/coord-test/protos`；
- `checkers` 目标在 fixture 之前加两条：`--selftest` 与真比对（都必须在）；
- `KV_T :=` 变量 + `test`/`soak`/`soakfull` 目标拼 `$(if $(KV_T),--kv-t)`。

- [ ] **Step 9: Commit**

```bash
git add ./jepsen/src/java/jepsen/coord/CoordRpc.java ./jepsen/src/jepsen/coord/proto.clj ./jepsen/src/jepsen/coord/client.clj ./jepsen/scripts/check-kv-t-wire.clj ./jepsen/scripts/smoke-kv-t.clj ./jepsen/lab/Makefile
git commit -m "feat(jepsen): speak coord.tso/kv/txn v2 over dynamic descriptors"
```

---

### Task 3: `mvcc` checker 与 13 个负控制 fixture（fixture 先行）

**Files:**
- Create: `jepsen/src/jepsen/coord/{mvcc.clj}`
- Create: `jepsen/scripts/{mvcc-fixtures}/expect-*.edn`（13 个）
- Modify: `jepsen/lab/Makefile:466`（`checkers` 目标）
- Test: 既有 `jepsen/scripts/run-checker-tests.clj`（负控制先行：文件名即断言）

**Interfaces:**
- Produces: `(mvcc/checker {:min-writes n :min-reads n :min-ts n}) -> jepsen.checker/Checker`；返回 map 至少含 `:valid?`、`:reason`（invalid 时）、`:writes`/`:reads`/`:ts`、`:errors`（前 20 条）、`:errors-count`、`:stats {:commit-rejected n :safe-ts-lag n :history-unavailable n}`。
- Consumes: Task 4/5 的 op 形状（本任务先按它写 fixture）。

op 形状（fixture 与客户端共用，写死在 `mvcc.clj` 的 docstring 里）：

```clojure
{:type :invoke|:ok|:info|:fail, :f :v2-write|:v2-read|:v2-delete|:v2-txn|:v2-ts,
 :time <monotonic ns>, :process p, :node n,
 ;; write/delete: :key :value :commit-ts :request-id :retried [code …]
 ;; read:         :key :read-ts :value :mod-revision :create-revision
 ;; txn:          :txn-id :start-ts :commit-ts(可缺) :writes {k v} :reads {k v}
 ;; :info 的候选位置: :attempted-ts [t1 t2 …]}
```

- [ ] **Step 1: 写 13 个 fixture 与判定文件名**

`mvcc-fixtures/`：
`expect-valid-clean-register.edn`、`expect-valid-info-write-window.edn`、`expect-valid-tombstone-read.edn`、`expect-valid-safe-ts-after-restart.edn`、`expect-valid-txn-unknown-ts.edn`、`expect-valid-commit-rejected-info.edn`、
`expect-invalid-stale-superseded.edn`、`expect-invalid-read-from-future.edn`、`expect-invalid-fabricated-value.edn`、`expect-invalid-tombstone-resurrection.edn`、`expect-invalid-shared-commit-ts.edn`、`expect-invalid-txn-half-visible.edn`、`expect-invalid-safe-ts-regression.edn`。
关键构造：
- `stale-superseded`：写 v1@100 `:ok`、写 v2@120 `:ok`，读 `read_ts=130` 仍返回 v1；
- `info-write-window`：写 v2 `:info` 且 `:attempted-ts [120 140]`，读 `read_ts=130` 分别出现 v1 与 v2；
- `txn-half-visible`：`commit_ts=200` 的两 key 写，读 `read_ts=210` 只看到一个；
- `shared-commit-ts`：两条 `:ok` 写同 `commit_ts`；
- `safe-ts-after-restart`：`(:type :invoke :f :nemesis :nemesis :start)` 之后 `read_ts=0` 回显变小。

- [ ] **Step 2: 跑 fixture 套件，确认全红（namespace 不存在）**

Run: `... -m clojure.main ./scripts/run-checker-tests.clj jepsen.coord.mvcc scripts/mvcc-fixtures '{:min-writes 0 :min-reads 0 :min-ts 0}'`
Expected: `No such namespace` / 全红。

- [ ] **Step 3: 实现 `mvcc.clj`**

按 spec §5.3 的 8 条规则；结构 `(defn- index-writes [history])`、`(defn- check-read …)`、`(defn- check-txn-atomicity …)`、`(defn- check-premise …)`。四条纪律：
1. `:fail` 的写（含 `:txn-conflict`）**不进**任何可见集；`:info` 只进候选集；
2. 「该 key 从未写过」必须显式建模为「读到不存在」，不靠缺省；
3. 错误列表 `(take 20)` 截断前先把总数写进 `:errors-count`；
4. 不得假设「一个值只有一个 ts」：同一 `request_id` 的重试可能在两个节点各落一次（同值、两个 ts），判据按值身份聚合。

- [ ] **Step 4: 跑 fixture 套件，逐个修到绿**

Run: 同 Step 2。Expected: 13 个 fixture 全按文件名预期；`make checkers` 退出码 0。

- [ ] **Step 5: 负控制的负控制（确认套件不是空转）**

把 `expect-invalid-stale-superseded.edn` 临时复制成 `expect-valid-stale-superseded.edn` 再跑，**必须**红；删掉副本恢复绿。

- [ ] **Step 6: 接进 `make checkers`**

```make
$(call runctl, cd $(CTRL_PROJECT) && LEIN_ROOT=true lein $(LEIN_OFFLINE)run -m clojure.main \
    ./scripts/run-checker-tests.clj jepsen.coord.mvcc scripts/mvcc-fixtures \
    '{:min-writes 0 :min-reads 0 :min-ts 0}')
```

- [ ] **Step 7: Commit**

```bash
git add ./jepsen/src/jepsen/coord/mvcc.clj ./jepsen/scripts/mvcc-fixtures ./jepsen/lab/Makefile
git commit -m "feat(jepsen): add the deterministic MVCC checker with negative controls"
```

---

### Task 4: `kvv2` 工作负载（直写 + 快照读 + TSO）

**Files:**
- Modify: `jepsen/src/jepsen/coord/client.clj`（`invoke!` 分派 + 四个 invoke + `v2-error-class`）
- Modify: `jepsen/src/jepsen/coord.clj`（`client-gen`、`checker`、`coord-test` 默认值、CLI opts、`--workload` 枚举）
- Modify: `jepsen/lab/Makefile`（`MATRIX_M3`、`matrix-m3`）
- Create: `jepsen/scripts/{test-v2-error-class.clj}`
- Test: Task 3 的 checker 端到端（红/绿都靠它判）

**Interfaces:**
- Consumes: Task 2 的 `proto.clj` helper 与 `CoordRpc.errorCode`；Task 3 的 `mvcc/checker`。
- Produces: CLI `--v2-min-writes/--v2-min-reads/--v2-min-ts`（默认 200）、`--v2-historical-read-ratio`（默认 0.2）、`--v2-history-window-secs`（默认 600）；workload `:kvv2`。
- Produces: `(v2-error-class [^StatusRuntimeException e]) -> :commit-rejected|:safe-ts-lag|:epoch-stale|:txn-conflict|:not-leader|:unauthenticated|:unavailable|:other`（`client.clj` 私有）。

- [ ] **Step 1: 写分类的失败断言**

`test-v2-error-class.clj`：用 `StatusRuntimeException` + `Metadata` 构造 8 个样例（每个 trailer 一个），断言 `(v2-error-class e)` 返回预期关键字；无 trailer 的 `UNAVAILABLE` 返回 `:unavailable`；`COMMIT_REJECTED` 返回 `:commit-rejected`（**不是** `:other`）。
Run: `./scripts/test-v2-error-class.clj`。Expected: FAIL（函数不存在）。

- [ ] **Step 2: 实现 `v2-error-class` 与 `invoke-v2-*`**

规则（spec §5.5）：`COMMIT_REJECTED` → 新号 + 同 `request_id` 重试（≤8，`request_id` 只在首次生成）；`SAFE_TS_LAG` → `:info`，**不换节点不换 ts**；`EPOCH_STALE`/`NOT_LEADER`/无码 `UNAVAILABLE` → 轮换节点重试（≤8）；`TXN_CONFLICT` → `:fail`；`HISTORY_UNAVAILABLE`（range 的 `OUT_OF_RANGE`）→ `:info`。完成 op 记 `:retried`（每次的 code+node）。
Run: Step 1 的命令。Expected: 8 条全 PASS。

- [ ] **Step 3: 实现 `kvv2-gen`**

`coord.clj` 的 `(defn- kvv2-gen [opts])`，用 `gen/mix`：
- 55% 写（`/jepsen/v2/k/<n>` → region 1、`zz-jepsen-v2-<n>` → region 2；值 `"w-<n>-<rand>"`）；
- 30% `read_ts=0` 读（两个 Region 各半）；
- 15% 历史读：从**客户端本地**最近 600s 见过的 `commit_ts` 里随机取（无可用 ts 就不发这类 op，不发必然失败的读）；
- 每 20 op 一条 `:f :v2-ts`；
- 写阶段结束后，对每个写过的 key 各发一次 `read_ts=0` 读（`:final true`）——`mvcc` 第 8 条的收敛断言靠它。
`client-gen` 加 `:kvv2 [(kvv2-gen opts)]`；`--workload` 枚举加 `:kvv2`。

- [ ] **Step 4: 接 checker 与默认值**

`coord-test`（在 checker 默认值之前）：
```clojure
(when (and (contains? #{:kvv2 :txnv2} (:workload opts)) (= :linear (:checker opts)))
  (throw (ex-info "v2 工作负载不能用 --checker linear（多版本期望无法用单寄存器模型表达）" {:workload (:workload opts)})))
```
这两个 workload 的 `:checker` 默认成 `:soak`（长跑口径，实际走 mvcc）。`checker` 的 `cond` 最前面加：
```clojure
(contains? #{:kvv2 :txnv2} workload)
(mvcc/checker {:min-writes (or (:v2-min-writes opts) 200)
               :min-reads  (or (:v2-min-reads opts) 200)
               :min-ts     (or (:v2-min-ts opts) 200)})
```

- [ ] **Step 5: 跑 60s 直连（应绿）**

Run: `make test WORKLOAD=kvv2 KV_T=1 NEMESIS=none TIME_LIMIT=60 RATE=20 CONCURRENCY=2n`
Expected: `:valid? true`；`writes`/`reads` ≥ 门槛；`stats.commit-rejected` > 0（并发直写的常态）。

- [ ] **Step 6: 跑 kill 与 partition-halves**

Run: `make test WORKLOAD=kvv2 KV_T=1 NEMESIS=kill TIME_LIMIT=45 RATE=20 CONCURRENCY=2n`，再 `NEMESIS=partition-halves`。
Expected: 两轮 `:valid? true`；`commit-rejected`/`safe-ts-lag` 出现且被重试消化。红时先看 `:errors` 的 `:kind`——只允许修客户端/checker，**不得**放宽判据。

- [ ] **Step 7: 加 `matrix-m3`**

`jepsen/lab/Makefile`：`MATRIX_M3 := kvv2:none kvv2:kill txnv2:none txnv2:kill`；`matrix-m3` 照 `matrix-m1` 的写法（`RATE=20 CONCURRENCY=2n KV_T=1`）。

- [ ] **Step 8: Commit**

```bash
git add ./jepsen/src/jepsen/coord/client.clj ./jepsen/src/jepsen/coord.clj ./jepsen/scripts/test-v2-error-class.clj ./jepsen/lab/Makefile
git commit -m "feat(jepsen): add the kvv2 workload (direct writes, snapshot reads, TSO)"
```

---

### Task 5: `txnv2` 银行账本（跨 Region 原子事务）

**Files:**
- Modify: `jepsen/src/jepsen/coord/client.clj`（`invoke-v2-txn`）
- Modify: `jepsen/src/jepsen/coord/{mvcc.clj}`（原子性与「位置未知的原子块」）
- Modify: `jepsen/src/jepsen/coord.clj`（`txnv2-gen`、`--workload` 枚举）
- Test: Task 3 的 fixture + 两个新账本 fixture + 端到端

**Interfaces:**
- Consumes: Task 2 的 `txn2-*` helper、Task 4 的 `v2-error-class`。
- Produces: workload `:txnv2`；op 形状沿用 Task 3（`:f :v2-txn`），`:info` 时附 `:attempted-ts` 或 `:ts-unknown true`。

- [ ] **Step 1: 写账本不变量的失败断言**

新增 `expect-invalid-ledger-sum.edn`（两 key 转账只写了一个 ⇒ 总和变了）与 `expect-valid-ledger-sum.edn`（完整转账）。先跑，确认前者此刻是**绿的**（判据未实现）：
Run: `... run-checker-tests.clj jepsen.coord.mvcc scripts/mvcc-fixtures '{:min-writes 0 :min-reads 0 :min-ts 0}'`
Expected: `expect-invalid-ledger-sum.edn` FAIL（红测试确实红）。

- [ ] **Step 2: 实现原子性与未知 ts 规则**

spec §5.3 第 6 条：按 `:txn-id` 聚合写集；`R >= commit_ts` 必须全有、`R < commit_ts` 必须全无；`commit_ts` 未知时只断言**全有或全无**（按值身份比对），不断言与 `R` 的先后。账本 fixture 另加总和守恒（`min-writes` 统计 txn 数）。

- [ ] **Step 3: 跑 fixture，修到绿**

Run: 同 Step 1。Expected: 15 个 fixture 全按预期。

- [ ] **Step 4: 实现 `invoke-v2-txn` 与 `txnv2-gen`**

`invoke-v2-txn`：`Begin(0)` → 逐 key `Read` → `Commit`；会话必须落在**同一节点**（记 `:node`），换节点就 `Abort` 后开新会话并 `recur`（`retried` 留痕）。`Commit` 响应丢失时先同会话同 `request_id` 再 `Commit` 一次把 `commit_ts` 问出来；会话已消失则 `:info` + `:ts-unknown true`。
`txnv2-gen`：4 个 key（region 1：`/jepsen/v2/acct/a|b`；region 2：`zz-jepsen-v2/acct/c|d`），每次转 1 个单位（值域 0–50，源不足则反向），写集跨 Region 且**两 key 同 `commit_ts`**。

- [ ] **Step 5: 端到端（none → kill → partition-halves）**

Run: `make test WORKLOAD=txnv2 KV_T=1 NEMESIS=none TIME_LIMIT=45 RATE=20 CONCURRENCY=2n`，再 `kill`、`partition-halves`。
Expected: 三轮 `:valid? true`；summary 报冲突率与总和守恒；`TXN_CONFLICT` 是 `:fail` 且不进可见集。

- [ ] **Step 6: Commit**

```bash
git add ./jepsen/src/jepsen/coord/client.clj ./jepsen/src/jepsen/coord/mvcc.clj ./jepsen/src/jepsen/coord.clj ./jepsen/scripts/mvcc-fixtures
git commit -m "feat(jepsen): add the txnv2 cross-region ledger workload"
```

---

### Task 6: 矩阵 / nightly / 证据 / 文档接线

**Files:**
- Modify: `jepsen/lab/Makefile`（`soak-kv-t` 目标、`nightly` 追加矩阵）
- Modify: `jepsen/scripts/nightly-soak-gates.sh`（第 5 步）
- Modify: `jepsen/README.md`（v2 章节）
- Modify: `docs/production/ops/kv-t-ga-gate.md`（§2 的 #174/#175 证据行）
- Modify: `docs/superpowers/specs/2026-10-25-kv-t-v2-jepsen-workload-design.md`（状态 → 已落地）

**Interfaces:**
- Consumes: Task 4/5 的 `matrix-m3` 与 `--kv-t`。

- [ ] **Step 1: 加 `soak-kv-t` 目标**

照 `soakfull` 的写法：`make soak-kv-t SOAK_TIME_LIMIT=7200`（默认 2h）→ `--workload kvv2 --kv-t --nemesis soak --rate 0.5 --checker soak`；`SOAK_TIME_LIMIT=259200` 即 72h。日志/pid 与现有 soak 隔离（`soak-kvt.log`/`soak-kvt.pid`），**不得**覆盖正在跑的 v1 soak。

- [ ] **Step 2: nightly 追加矩阵**

`nightly-soak-gates.sh` 加第 5 步 `matrix-m3`；`SKIP_LONG=1` 时跳过（与第 4 步一致）。

- [ ] **Step 3: 跑接线（不是长跑）**

Run: `SKIP_LONG=1 jepsen/scripts/nightly-soak-gates.sh`
Expected: checkers（含 mvcc + wire + config）→ matrix-m1 → matrix-m2 → matrix-m3 全绿。

- [ ] **Step 4: 文档**

`jepsen/README.md` 新增「KV-T v2 工作负载」：`--kv-t` 用法、判**什么**（快照读期望、TSO 唯一性、事务原子性、safe_ts 活性）、**不判什么**（watch/lease/storage v2、split/merge、knosss 模型）。
`kv-t-ga-gate.md`：#174 行改为「72h v2 soak（Jepsen `kvv2` + `mvcc` checker）」并给复跑命令；#175 行补「v2 并发判据见 Jepsen `matrix-m3`」；旧脚本 soak 降为交叉验证一句。§3 的未达标项按实际状态增删（split/merge 与 D10 不动）。

- [ ] **Step 5: 文档引用检查**

Run: `bash scripts/check-doc-refs.sh`（仓库根）。Expected: `check-doc-refs: OK`。

- [ ] **Step 6: Commit**

```bash
git add jepsen/lab/Makefile jepsen/scripts/nightly-soak-gates.sh jepsen/README.md docs/production/ops/kv-t-ga-gate.md docs/superpowers/specs/2026-10-25-kv-t-v2-jepsen-workload-design.md
git commit -m "docs(jepsen): wire the v2 matrix/nightly/soak and record what the oracle proves"
```

---

### Task 7: 2h 浸泡收口（证据）

**Files:**
- 产物只落 `${COORD_EVIDENCE_DIR}`（`/tmp/...`），不入库。

- [ ] **Step 1: 起 2h 浸泡**

Run: `make soak-kv-t SOAK_TIME_LIMIT=7200 && make soak-kv-t-wait && make soak-kv-t-results`
Expected: `:valid? true`；`writes`/`reads` ≥ 门槛；`stats` 里 `commit-rejected` 与 `safe-ts-lag` 都有计数。

- [ ] **Step 2: 收证据**

Run: `./scripts/collect-evidence.sh soak-kv-t store/coord/latest`
Expected: `${COORD_EVIDENCE_DIR}/<UTC>-soak-kv-t/` 下有 `MANIFEST.md`、`results.edn`、`history.edn`（>5MiB 已 gzip）、`summary.txt`、`sha256sums.txt`。

- [ ] **Step 3: 结论写进 gate 文档（不写成 72h）**

`kv-t-ga-gate.md` §3 把「72h v2 soak 运行中」改为「2h Jepsen v2 浸泡已通过（commit + 证据目录）；72h 待在 GA 最终提交上重跑」。**不得**把 2h 写成 72h。

- [ ] **Step 4: Commit**

```bash
git add docs/production/ops/kv-t-ga-gate.md
git commit -m "docs(kvt): record the 2h Jepsen v2 soak as the pre-72h evidence"
```
