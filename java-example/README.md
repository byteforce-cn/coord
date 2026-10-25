# java-example

Java 应用接入 Coord 的示例模块（KV / Lease / Watch / Txn / Registry 的 v1 面，
以及 KV-T v2 数据面）。**刻意不依赖 `coord-java-sdk`**：示例只用 Maven 生成的
gRPC + proto stub（契约包 `cn.byteforce.coord.contracts.*`），用来证明"普通
gRPC 客户端可以直连 coord-agent"，而不是证明"SDK 自己能用"。

本文件是这些示例**命令的唯一归属**：`coord-java-sdk/README.md` 只留指针，
不复制命令（同一事实只维护一处）。

## 前置

- JDK 21+（`JAVA_HOME` 优先，否则用 PATH 上的 `java`）。
- Maven 3.9+（`MVN` 环境变量可覆盖可执行文件）。
- 需要真实集群的用例：`cargo build --workspace --bins` 产出 `target/debug/coord`。

## 目录

| 路径 | 内容 |
|:--|:--|
| `src/main/java/cn/byteforce/coord/example/CoordClient.java` | v1 面的客户端封装（KV/Lease/Watch/Txn/Maintenance） |
| `src/main/java/cn/byteforce/coord/example/KvTV2Example.java` | **v2 数据面示例**：`coord.tso.v2` / `coord.kv.v2` / `coord.txn.v2` / `coord.watch.v2` |
| `src/test/java/com/byteforce/coord/example/*IntegrationTest.java` | 连真实 v1 集群（`scripts/ci-java-it-cluster.sh start v1`）的集成套件 |
| `scripts/verify-build.sh` | proto 生成 + 编译 + 单测的本地校验 |

默认 `mvn verify` 排除 `*IntegrationTest` / `*AdvancedTest`（没有集群时会必然红）；
`-Pit` 才纳入，此时集群缺失是**硬失败**，不会静默跳过。

## 构建

```bash
bash java-example/scripts/verify-build.sh        # proto + 编译 + 单测
(cd java-example && mvn -B -q verify -DskipTests)  # CI 的编译口径
```

## KV-T v2 示例（`KvTV2Example`）

需要一套真实 v2 集群（multi-raft + PD + object storage + `[kv_t].enabled=true`）：

```bash
cargo build --workspace --bins
bash scripts/ci-java-it-cluster.sh start v2          # agent 127.0.0.1:19637
bash java-example/scripts/run-v2-example.sh          # 退出码 0 = 通过
bash scripts/ci-java-it-cluster.sh stop v2
```

端点可用 `COORD_V2_AGENT_HOST` / `COORD_V2_AGENT_PORT` 覆盖（默认
`localhost:19637`）。示例依次验证：

1. `coord.tso.v2.GetTs` 单调递增；
2. `coord.kv.v2`：显式 `commit_ts` 写、`read_ts=commit_ts` 读（`mod_revision ==
   commit_ts`）、覆盖写的 `prev_kv`、`read_ts=0`（服务端 `safe_ts`）读；
3. `coord.txn.v2`：`Begin` → `Read`（读 `start_ts` 快照）→ `Commit`（两个 key 共享
   一个 `commit_ts`）；
4. `coord.watch.v2`：先 create、再提交事务，断言同一个事务只回来**一个**原子批
   （`commit_ts` 与事务一致），最后 cancel。

退出码：`0` 通过；`1` 断言/RPC 失败；**`2` 目标集群不是 v2**（`FAILED_PRECONDITION`
= `[kv_t]` 未开启、或 `UNIMPLEMENTED` = agent 不转发 v2 服务）。没有"降级到 v1 API"
的分支：v2 没验过就是失败。

## 跨语言互操作（Rust `coord-client` ↔ Java SDK）

在**同一套** v2 集群上双向对账：Rust 写（直写 + 事务 + Watch 观察）→ Java 按精确
`commit_ts` 读；再 Java 写 → Rust 按精确 `commit_ts` 读。

```bash
# 自己起停 v2 dev 集群（默认；缺 JDK/Maven/cargo/集群一律 fail-closed）
bash scripts/check-kvt-interop.sh

# 复用已在跑的 v2 集群
COORD_KVT_INTEROP_ATTACH=1 bash scripts/check-kvt-interop.sh
```

- 证据目录：`COORD_INTEROP_EVIDENCE_DIR`（默认 `/tmp/coord-kvt-interop-<UTC 时间戳>`），
  内含 `rust-to-java.properties`、`java-to-rust.properties`、`run.log`；**不写仓库**。
- 端点/命名空间：`COORD_INTEROP_SERVER`（Rust 直连 server，默认 `127.0.0.1:51051`）、
  `COORD_V2_AGENT_HOST`/`COORD_V2_AGENT_PORT`（Java 经 agent，默认 `127.0.0.1:19637`）、
  `COORD_INTEROP_RUN_ID`（两侧共用的 key 命名空间）。
- 脚本内部阶段：`cargo test -p coord-client --test kvt_interop_peer rust_writes_for_java_to_read`
  → `mvn -Pinterop test`（`KvTInteropIntegrationTest`）→
  `cargo test -p coord-client --test kvt_interop_peer rust_reads_what_java_wrote`。
  单独的 `-Pinterop` 会因为缺少证据文件而失败——那是刻意的 fail-closed。
