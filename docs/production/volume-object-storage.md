# Volume 分布式对象存储能力评估与落地决策

> 文档类型：能力评估 + 立项决策 + 落地进度（Decision Record）
> 日期：2026-09-06（评估）／2026-09-06（决策拍板，§8）
> 适用版本：main（Multi-Raft 落地完成；对象存储 Phase A 数据面闭环 + Phase B
> 深度接入 + chunk DEK 化 + SDK/Agent 代理已落地，2026-09-06 批次）
> 状态：**已拍板立项（§8 D1–D5）；Phase A 数据面闭环 + 进程级 e2e + 混沌矩阵
> （kill/pause/partition）已收口；Phase B 深度接入已落地；Phase C 项 chunk DEK
> 化已落地（§10 批次日志）**
> 关联：`apis/contracts/STATUS.md`（EXPERIMENTAL 治理行 `coord.storage` 2026-12-31）、
> `apis/contracts/proto/coord/storage/storage.proto`、`config.example.toml` `[object_storage]`

---

## 1. 背景与目标

Coord Server 当前定位是「共识与存储基座」：3 节点单 Raft 组（或 Multi-Raft 多 Region）承载
KV/Txn/Watch/Lease 等协调原语，高级能力全部由 Agent 落地。Server 节点的本地磁盘
（volume）在协调负载下大量闲置，尤其是 follower 与空闲 region。

本评估回答一个问题：**在已完成 Multi-Raft 落地的现状下，能否在 Server 上提供可开关的
分布式对象存储能力，把各节点 volume 作为对象存储的数据面复用，且不破坏协调基座与
既有磁盘布局承诺？**

评估结论前置（详见 §6）：可行，但必须满足 4 个硬前提——chunk 与 MVCC/快照隔离、
配额 + 磁盘水位防护、流式传输绕过 4MiB RPC 上限、容量按副本数折算。当前 Multi-Raft
已提供约九成的「元数据面」（路由/调度/迁移/快照/对账），缺的是「数据面」（chunk
落地与对象生命周期），工程量集中在 Phase A 数据面闭环。

---

## 2. 现状盘点（评估依据）

### 2.1 Multi-Raft 基座（已落地）

| 能力 | 现状 | 代码/配置落点 |
|:---|:---|:---|
| 多 Region 装配 | `[multi_raft].enabled` 开关；静态 Region 表；key range 平铺 keyspace | `config.example.toml` |
| 存储隔离 | 目录级隔离：region 0 = 根目录（legacy 布局字节级不变）；region ≥1 = `<data_dir>/regions/region-{id:016x}/` | `coord-server/src/raft/region_runtime.rs::region_data_dir` |
| Region 路由 | `RegionManager` BTreeMap，按 start_key 二分路由 O(log N) | `coord-server/src/raft/region.rs` |
| Legacy 迁移 | 一次性 `legacy_migration`，fail-closed 闸 + `allow_unmigrated` 救援；只读源、可回滚 | `config.example.toml`；`coord-server/src/migration.rs` |
| Watch / Lease | per-Region 路由（跨 Region 显式拒绝）；lease 全局唯一（元数据存 region 0 全局租约表） | `coord-server/src/raft/`（T5.6/T5.7） |
| 快照 / 压缩 | per-Region 导出/恢复（`coord snapshot --region`）；各 Region 独立 compact 水位 | `coord-server/src/storage/snapshot*.rs`、`compaction.rs` |
| 内嵌 PD | meta store 落盘 `<data_dir>/pd/pd-meta.db`；节点/Region 心跳（leader/size/keys）；operator 全局队列经 region 0 system raft，Running 超时重认领（failover）；调度暂停开关 + 审计 | `coord-server/src/pd/embedded.rs` |
| Operator 种类 | AddPeer / RemovePeer / TransferLeader **可用**；SplitRegion / MergeRegion 操作符已定义但执行器返回 unsupported（v1 无在线 Split/Merge） | `coord-server/src/pd/operator.rs`、`executor.rs` |
| Region 指标 | `coord_region_size_bytes{region_id=…}` gauge + Region 面板 | `coord-server/src/metrics.rs`、`coord-ui` |

### 2.2 存储基座（直接约束对象存储形态）

| 事实 | 数值/行为 | 落点 |
|:---|:---|:---|
| 引擎模型 | redb 单写者；每 Region 独立 DB；Multi-Raft 日志 append 走 WriteBatcher 组提交（仅日志路径，apply 无等价物） | `coord-server/src/storage/redb_backend.rs`、`write_batcher.rs` |
| MVCC | 多版本 + 历史 revision 读，版本保留至 compact | `coord-server/src/storage/mvcc.rs` |
| gRPC 消息上限 | 服务端 4MiB（`MAX_DECODING_MSG = 4 * 1024 * 1024`），Raft RPC 另有独立上限 | `coord/src/main.rs` |
| 快照 | 全量单事务导出 + 2MiB 分块流式 + token bucket 限速（默认 50MiB/s） | `coord-server/src/storage/snapshot.rs`、`snapshot_limiter.rs` |
| 磁盘水位 | Warn <15% / ReadOnly <5%（写返回 `RESOURCE_EXHAUSTED`，读仍可用） | `coord-server/src/storage/disk_watermark.rs` |
| 静态加密 | 仅覆盖 `/kv/` 用户数据前缀，默认关闭 | `config.example.toml` `[security]` |
| 副本 | 全量 3 副本（`target_replicas` ≤ `initial_nodes` 成员数），可用容量 ≈ 磁盘/3 | `config.example.toml` `[multi_raft.pd]` |
| PD 阈值（预留） | `region_split_size_mb=256` / `region_split_keys=1,000,000` / `region_merge_size_mb=16` | `config.example.toml` |

---

## 3. 能力评估：现状能做什么、不能做什么

### 3.1 现状已具备（零改动可用）

- **小对象直存 KV**：≤4MiB 的字节流可直接经 KV/Txn 写入任意 key，获得线性一致性
  （Jepsen 全矩阵已验证）、Watch、租约绑定与鉴权，适合「配置文件/小文件」场景。
- **容量与隔离**：Multi-Raft 已提供 region 级目录隔离、per-Region 快照/压缩、
  region 大小指标（`coord_region_size_bytes`）——对象数据落在哪个 region 就能被
  观测、备份与调度。
- **迁移与退化**：Legacy→Multi-Raft 迁移闸与字节级退化承诺，为新增存储特性提供了
  现成的「可开关 + 可回滚」模式样板。

### 3.2 硬约束（现状直接做对象存储会遇到什么）

1. **4MiB RPC 上限**：单次 Put ≤4MiB，大对象必须分片 + 流式协议，现有 KV 契约无法
   表达「对象」语义（无 Stat/Delete 幂等/大小原子可见性）。
2. **写放大链**：对象数据若进 Raft 日志 + MVCC，成本 = 3× 副本 × 版本留存系数，
   且每字节都会进入 per-Region 全量快照与 compact 扫描。估算：256MiB 对象直走 KV
   需 64 次 4MiB 写入，落盘量随版本留存可达数百 MiB 至 GiB 级，且快照体积随对象
   总量线性膨胀——不可接受。
3. **快照语义冲突**：现有快照是「单事务全量导出」，对象数据入快照会使恢复时间与
   备份体积不可控。
4. **无在线 Split**：v1 静态 Region 表，executor 对 Split/Merge 返回 unsupported。
   单 region 写热时无法自动拆分，只能靠运维扩 region 表。
5. **生命周期缺失**：无 bucket/配额/过期/GC 语义；删除若只是 KV Delete，旧版本仍
   滞留 MVCC 至 compact，磁盘释放不可预期。
6. **加密边界**：静态加密只覆盖 `/kv/`，对象 chunk 文件若明文落盘，与已有加密承诺
   不一致。
7. **容量效率**：3 副本全量复制，可用容量 ≈ 磁盘/3；「充分利用本地资源」必须按此
   口径向使用者披露，否则容量预期失真。

---

## 4. 候选方案对比

| 维度 | A：大 value 直走 KV | B（推荐）：chunk + manifest | C：raft 仅元数据，chunk 独立复制 |
|:---|:---|:---|:---|
| 一致性 | 强（线性一致，已验证） | manifest 强一致（ReadIndex）+ chunk 随 raft 日志复制（日志==状态机） | 元数据强一致；chunk 最终一致需自研修复 |
| 单对象上限 | ≤4MiB（RPC 上限） | 256MiB v1（64×4MiB chunk） | 不受 raft 日志限制 |
| 写放大 | 极高（MVCC 版本 + 快照全量） | 低（chunk 不进 MVCC/快照） | 低，且可纠删码 |
| 快照 | 全量携带对象 | 仅 manifest + chunk 索引 | 仅元数据 |
| 复用现有代码 | 全部 | Region 路由/PD/迁移/水位均复用 | 复用最少 |
| 工程量 | 无（但能力不成立） | 中（数据面 + 生命周期） | 大（复制/修复/一致性重造） |
| 结论 | 不适合 | **v1 采用** | 不承诺（远期可纠删码时再评估） |

**方案 B 形态**：对象 key 落独立前缀 `/obj/{bucket}/{object_id}`，经 `RegionManager`
自动路由进所属 Region（initial_regions 平铺 keyspace，天然覆盖）；manifest 存该
Region 的 KV（raft 强一致），chunk payload 落该 Region 目录下 append-only chunk
文件（`<region data_dir>/objects/...`），不进 MVCC、不入快照；删除 = manifest
tombstone + 后台 GC。

---

## 5. 推荐落地路径（可开关、分阶段）

### Phase A：数据面闭环（单 Region 亦可用，不依赖 Multi-Raft）

- 新增 `[object_storage]` 段，`enabled = false` 默认关闭，与 `[multi_raft]` 正交；
  关闭时磁盘布局字节级不变（对齐既有退化承诺）。
- 新增 proto 服务（建议 `coord.experimental.storage.v1`）：Put/Get/Delete/Stat，
  client/server streaming（禁止 unary 传大对象）；chunk 4MiB、单对象 256MiB
  （对齐 RPC 上限与 PD split 阈值语义）。
- manifest（对象元数据 + chunk 清单/哈希 + tombstone）走 raft KV；chunk 文件落
  `<data_dir>/objects/`（multi_raft 开启时随 region 目录）。
- 资源防护三件套：接入 `disk_watermark`（ReadOnly 时写返回 `RESOURCE_EXHAUSTED`）、
  配额（`max_total_storage_bytes` / `max_object_size` / 按 agent 配额）、删除后
  异步 chunk GC。
- 验收：Jepsen blob 工作负载（kill/pause/partition 全矩阵）、磁盘填满测试验证水位
  与配额生效、4MiB 边界测试、快照不含 chunk 的断言。

### Phase B：Multi-Raft 深度接入

- 对象 key 进入 Region 路由（BTreeMap 已具备）；chunk 目录随 Region 迁移。
- Region 心跳增加「存储字节」维度；PD split 阈值纳入存储字节（字段已预留）。
- 存储重 region 默认不参与自动 leader/副本均衡，或复用快照限速 token bucket
  控制 chunk 搬运速率，防平衡风暴。
- 验收：多 Region 对象路由一致性、region 迁移后对象可读、Legacy 迁移不触碰
  chunk 目录。

### Phase C：演进（不承诺）

- 在线 Split/Merge 启用后，按存储字节自动拆热点 region；
- 复制因子可选 / 纠删码，缓解容量效率 1/3 的硬约束。

---

## 6. 合理性结论

**可行且合理，前置条件明确。** Multi-Raft 已交付路由、调度、迁移、快照、对账的
元数据面，Phase A 只需补数据面（chunk 落地 + 生命周期），即可让 server volume
提供强一致的分布式对象存储。四个硬前提缺一不可：

1. **chunk 与 MVCC/快照物理隔离**——否则写放大与快照风暴会反噬协调基座；
2. **配额 + 磁盘水位**——否则「充分利用本地资源」会退化为「写穿磁盘」；
3. **流式协议绕过 4MiB 上限**——否则单对象 ≤4MiB，能力不成立；
4. **容量按副本数折算披露**——3 副本下可用容量 ≈ 磁盘/3，指标与文档同步口径。

---

## 7. 风险与对策

| 风险 | 对策 |
|:---|:---|
| 写放大（3× 副本 + MVCC 版本） | chunk 旁路 MVCC/快照，manifest-only 快照 |
| 4MiB RPC 上限 | streaming + 4MiB chunk 分片 |
| 磁盘写穿 | disk_watermark 接入 + 配额 + 后台 GC |
| 快照/压缩风暴 | chunk 不入快照，manifest + 索引入快照 |
| 大对象挤占协调原语 quorum 提交 | 存储写独立限流/批量降级，与 KV 写路径分流 |
| region 迁移搬运 chunk 成本高 | Phase B 禁自动均衡或限速 |
| 静态加密边界（仅 `/kv/`） | chunk 文件独立加密（§8 D2 已落地：DEK 化信封 + 自动轮换） |
| 无在线 Split，热点无法自动拆 | v1 文档化约束；Phase C 依赖在线 Split 落地 |

---

## 8. 决策记录（2026-09-06 拍板，落地即生效）

> §8 原「待决策」5 项已全部拍板；决策依据 = 仓库 wire-sync/配置/加密/GC 现状与
> 本节工程取舍。契约台账行：`coord.storage` EXPERIMENTAL，期限 2026-12-31。

| # | 决策项 | 拍板结论 | 说明/落点 |
|:---|:---|:---|:---|
| D1 | 契约口径 | **`coord.storage`（扁平包）**，STATUS.md 记 EXPERIMENTAL 治理行；不用 `v1/` 子目录 | 服务端契约走 layer-1 扁平包自动 wire-sync（`proto/coord/storage/storage.proto` ↔ `coord-proto/src/proto/storage.proto`，rpc+字段号一致）；`v1/` 会被 wire-sync 错配到 agent_api。EXPERIMENTAL 仅治理，不影响 wire 校验 |
| D2 | chunk 加密 | **独立开关 + 独立根密钥**：`[object_storage].encryption_enabled`（默认 false）+ `encryption_root_key`（hex64）或环境变量 `COORD_OBJECT_STORAGE_ENCRYPTION_ROOT_KEY` | 不复用 `/kv/` `encryption_enabled`（chunk 文件与 MVCC 无关）。**DEK 化信封（已落地）**：根密钥经 HKDF-SHA256(info="coord-obj-kek-v1") 派生 KEK（仅内存），KEK 包裹随机 DEK（key_id 版本化；密文落盘 `<data_dir>/objects/keys/dek-{key_id:08x}.bin`）；新 chunk 文件头 `magic("COBJ2") || key_id(4BE) || nonce(12)`；v1（"COBJ1" 根密钥直作 DEK）文件兼容读取；按 `encryption_rotation_days`（默认 90）自动轮换，仅影响新写入。配置校验：encryption_enabled 缺 key / 有 key 未开 → 拒绝 |
| D3 | 上限数值 | 采纳 **单对象 256MiB / chunk 4MiB**（对齐 RPC MAX_DECODING_MSG 与 PD split 阈值语义） | `chunk_size_bytes`（≤4MiB 可配）/ `max_object_size_bytes` 默认如上；流式单消息 ≤ chunk_size |
| D4 | 命名空间 | **共享 keyspace，`/obj/` 前缀**；manifest 用户 key = `/obj/m/{bucket}/{object_id}` 自动路由 | Multi-Raft 复用 `RegionManager`（initial_regions 平铺天然覆盖）；manifest 作 `/kv/` 用户行自动获得加密/快照/压缩/MVCC（仅 manifest 小行入快照，chunk 文件不入）；对象启用时 `/obj/` 对用户 KV/Txn/Watch 保留（写拒绝、Range 结果过滤、Watch 拒绝） |
| D5 | 删除语义 | **tombstone + apply 同步删 chunk 文件 + 后台 GC**（孤儿扫描兜底） | Delete 经 raft 提交 KV tombstone（manifest 消失）并在 apply 同步删除对象目录；Creating（中断上传）按 `upload_timeout_secs` 由 leader GC 回收；崩溃残留 = 孤儿文件按 live-manifest 哈希集清扫 |

**体系结构决策（落地设计，随代码固化）**：chunk 数据**随 raft 日志复制**
（日志==状态机，不产生独立复制面；每 chunk 一条 ≤4MiB 日志条目），apply 时
由 SM 写 append-only chunk 文件（不进 MVCC、不入快照）；快照仅携带 manifest
（`/kv/` 用户行自动包含）；快照安装（落后节点）后本地 chunk 目录清空置 rebuild，
缺失 chunk 的 Get 返回 UNAVAILABLE（v1 已知边界，见 STATUS 整改要点）。

**已知边界（v1，文档化）**：
- 全集群 `[object_storage]` 配置必须一致（同 multi_raft）；启用后不可热关；
  `enabled` 与 `multi_raft.legacy_migration` 互斥（校验拒绝）；
- 并发对同一对象上传未定义（Begin 冲突 → ALREADY_EXISTS；后到者失败）；
- 上传中 leader 变更 → 客户端中止整体重试；残留 Creating 由 GC 回收；
- 用户 KV Range 对 `/obj/` 不可见（manifest 元数据不外泄）；全 keyspace Watch
  在对象启用时会观察到对象事件（manifest 字节，无对象数据）——建议对象存储
  开在专用/新建 keyspace；
- 配额 = 尽力而为 admission（非硬限制）；磁盘水位与 KV 写共用同一只读闸。

---

## 9. 参考落点

- `config.example.toml`：`[multi_raft]` / `[multi_raft.pd]` / `[storage]` / `[limits]` / `[security]` / `[object_storage]`
- `coord-server/src/raft/region_runtime.rs`：`region_data_dir` 目录约定
- `coord-server/src/pd/`：embedded / operator / executor（split unsupported 证据）
- `coord-server/src/storage/`：mvcc / redb_backend / snapshot / compaction / write_batcher / disk_watermark / **object_store**
- `coord-server/src/raft/type_config.rs`：`Command::ObjectStore` / `ObjectStoreOp`（末尾追加）
- `coord-server/src/raft/state_machine.rs`：apply 臂 + `object_chunk_store` + 快照安装清 chunk
- `coord-server/src/server/object_storage.rs`：`coord.storage.Storage` gRPC 实现 + `object_gc_loop`
- `coord/src/main.rs`：`MAX_DECODING_MSG = 4MiB`；服务注册/健康/GC 接线
- `apis/contracts/STATUS.md`：承诺台账（机器可解析，勿改列结构）
- `apis/contracts/proto/coord/storage/storage.proto` ↔ `coord-proto/src/proto/storage.proto`（wire-sync 镜像）

---

## 10. 落地进度（批次日志，docs/ 不入 git）

### Phase A —— 数据面闭环（已收口 2026-09-06）

| 项 | 状态 | 说明 |
|:---|:---|:---|
| 决策 D1–D5 | ✅ | §8 拍板并随代码固化 |
| 契约 `coord.storage`（Put 流/Get 流/Stat/Delete）+ codegen + wire-sync | ✅ | 镜像一致，check-wire-sync.sh 绿 |
| `[object_storage]` 配置 + 校验（上限/加密/互斥/轮换） | ✅ | `coord/src/config.rs`；config.example.toml（新增 `encryption_rotation_days`） |
| `ChunkStore`（明文/加密 chunk 文件，惰性建目录） | ✅ | `storage/object_store.rs`；单测 14/14 |
| `Command::ObjectStore` + apply（manifest 走 `/kv/` 用户行） | ✅ | Begin/Chunk/Commit/Delete；无状态变更分支持久化 applied 水位 |
| 流式服务 Put/Get/Stat/Delete | ✅ | `server/object_storage.rs`；ReadIndex 屏障 + 水位/配额闸 |
| KV/Txn/Watch `/obj/` 保留守卫 + Range 过滤 | ✅ | 写拒绝 / 读过滤 / Watch 拒绝 |
| 后台 GC（stale Creating + 孤儿文件，leader-only） | ✅ | root + 每 Region 一个；`upload_timeout_secs` 墙钟随 op 提议 |
| 快照语义 | ✅ | manifest 随 `/kv/` 快照自动携带；install_snapshot 清空本地 chunk（rebuild 边界） |
| 真实 raft 集成测试 | ✅ | `object_store_raft_test.rs` 2/2（legacy root 明文 + Region 加密） |
| gRPC 层进程级 e2e（流式分帧/4MiB 边界/配额/加密/GC） | ✅ | `coord/tests/object_storage_process_test.rs` 4/4：roundtrip+boundary+GC / quota RESOURCE_EXHAUSTED / 加密落盘 / chaos |
| 混沌矩阵（kill/SIGSTOP pause/网络 partition + leader failover） | ✅ | 同文件 `object_storage_real_chaos_kill_pause_partition`：9 轮注入 + 收敛 + 杀 leader 后全量对象读回（Rust 进程级，对齐 chaos_real；外部 Clojure Jepsen lab 的 blob 工作负载为 lab 侧后续项，本环境无 Clojure 工具链） |
| Rust SDK 对象方法（put/get/stat/delete_full） | ✅ | `coord-client` `Client::storage()` → `StorageClient`；进程级 SDK 回环测试 ✅（含跨 4MiB 边界 + AlreadyExists/NotFound 语义） |
| e2e 暴露缺陷修复 | ✅ | ① 服务端鉴权 fail-closed：`coord.storage.Storage/*` 未注册到 `infer_capability`（auth 关闭也拒绝）→ 补映射 + 注册 `data:storage:read/write` 能力（server/agent 双侧）；② gRPC 解码上限：4MiB chunk 的 protobuf 编码消息 >4MiB → StorageServer 上限 = chunk_size+64KiB、客户端 8MiB；③ `usage_bytes()` 统计的是目录大小而非 chunk 文件字节（配额闸形同虚设）→ 递归求和文件字节（keys/ 目录除外） |

### Phase B —— Multi-Raft 深度接入（已落地）

| 项 | 状态 | 说明 |
|:---|:---|:---|
| `/obj/` key 进 Region 路由；chunk 目录随 Region（每 Region 自己 data_dir/objects） | ✅ 已具备 | `object_target_for` + `RegionRuntime.chunk_store`（Phase A 已按 Region 挂载） |
| Legacy 迁移不触碰 chunk 目录 | ✅ | 组合被配置校验拒绝；迁移只导 `/kv/` 用户行 |
| Region 心跳增加「存储字节」维度 + PD split 阈值纳入 | ✅ | 心跳每拍上报 chunk 用量（`rt.chunk_store.usage_bytes()`）→ `PlacementDriver::handle_region_heartbeat(.., storage_bytes, ..)` → `PdMetaStore` 内存视图（同 size/keys，不落盘）；`ScheduleContext.region_storage_bytes` 供调度器；SplitChecker 以「存储字节 + redb 文件大小」判定（不入 RegionMeta 持久 schema） |
| 存储重 Region 不参与自动均衡 / 限速搬运 | ✅ | Balance/Leader 调度器 `with_storage_gate`（阈值 = region_split_size_mb）：存储重 Region 不作 AddPeer/TransferLeader 候选；调度器单测覆盖 |

### Phase C —— 演进（剩余项不承诺）

| 项 | 状态 |
|:---|:---|
| 在线 Split/Merge 启用后按存储字节自动拆热点 region | ⬜ 前置 = v1 无在线 split（executor unsupported） |
| 复制因子可选 / 纠删码（缓解容量 1/3） | ⬜ 远期 |
| chunk 加密根密钥轮换 / DEK 化（对齐 /kv/ key_management） | ✅ 已落地（2026-09-06）：KEK(HKDF) 包裹随机 DEK、key_id 版本化、`encryption_rotation_days` 自动轮换、v1 兼容读；单测含轮换/重启恢复/到期自动轮换 |

### SDK 与接入（2026-09-06）

| 项 | 状态 | 说明 |
|:---|:---|:---|
| Rust SDK（coord-client） | ✅ | `Client::storage()` + `StorageClient`（put/put_chunked/get/stat/delete/delete_full），流式实现 + leader 自动路由/重试；进程级 SDK 回环测试 ✅ |
| Agent 存储代理 | ✅ | `coord-agent` `StorageProxy`（经 coord-client SDK 转发；Put 缓冲整对象后上传、Get 按 ≤4MiB 回放、Stat/Delete unary），注册到 agent gRPC 路由 + 双侧鉴权映射 |
| Java SDK（coord-java-sdk） | ✅ 代码落地（需 mvn 构建验证） | `CoordClient.objectStore()` → `ObjectStoreClient`（put 异步流 / get/stat/delete blocking），经 agent 代理访问；本环境无 Java 工具链，未编译验证 |
