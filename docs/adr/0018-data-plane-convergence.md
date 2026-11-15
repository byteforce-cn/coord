# ADR-0018: 数据面收敛——单一数据面（v2）与 v1 退场

- 状态：accepted
- 日期：2026-11-15
- 决策者：维护团队
- 修订：ADR-0017（`[kv_t]` 三态默认）中「单 Raft 部署默认仍是 v1」对 `coord dev` 的口径；
  ADR-0015 D13 的迁移口径不变

## 背景

coord 曾经有两个数据面：v1（`coord.kv` / `coord.txn` / `coord.watch` / `coord.lease` /
`coord.storage`，revision = raft log index）与 v2（`coord.{tso,kv,txn,watch,lease,storage}.v2`，
`commit_ts` + OCC + TSO）。ADR-0015/0016/0017 之后，两者的边界是：

- v2 集群**冻结** v1 数据面（B-KVT-2），v1 集群不服务 v2（B-KVT-1）；
- `coord dev` 是单 Raft 拓扑 ⇒ 落在 v1 数据面；
- `coord-client`（Rust）内部有 `v2_compat` 兼容层，v1 形状的调用在 v2 集群上落到 v2 wire，
  内部服务不需要知道平面；
- **Java SDK 没有这一层**：`CoordClient` 直接暴露 `kvV2()` / `txnV2()` / `tsoV2()` / `leaseV2()` /
  `watchV2()` / `storageV2()`，v1 数据面没有对应入口；`HandshakeResponse` 只回
  `supported_versions`，**不告诉调用方集群跑哪个数据面**。

结果是缝泄漏到调用方：同一个适配器要写两套路径、按部署知识切方法名，缓存/重试/错误码实现两遍，
新能力要在两个平面上各做一次；而"跑在哪个平面"这件事只有部署方知道，调用方无法自探测。
这与「coord 是基础能力平台」的定位冲突：平台应该只承诺一个语义面，平面是实现细节。

## 决定

1. **D1 单一承诺数据面 = v2**。新能力只上 v2；v1 数据面不再新增任何特性。
2. **D2 `coord dev`（单机一体化拓扑）= v2**：单节点 `[multi_raft]` + `[multi_raft.pd]`
   （`target_replicas = 1`）+ `[kv_t].enabled = true`，Agent `data_plane = "v2"`。
   dev 不再提供 v1 数据面拓扑（需要 v1 的调试场景用 `coord server` + `coord agent` 显式配置）。
3. **D3 v1 数据面移出承诺面**：`coord.kv` / `coord.txn` / `coord.lease` / `coord.watch` /
   `coord.storage`(v1) 从 `STATUS.md` 删除——不再承诺、不再演进、不再要求 SDK 覆盖。
   既有 v1 集群继续按原来的**冻结语义**运行（B-KVT-2），实现与 wire 均保留；
   **wire 级删除需另立 ADR**（涉及既有 v1 集群与 Jepsen v1 夹具的退役节奏）。
4. **D4 客户端入口按语义命名、不带平面后缀**：唯一数据面后，SDK 的入口为
   `client.kv()` / `tx()` / `tso()` / `lease()` / `watch()` / `storage()`，客户端类型落在
   `cn.byteforce.coord.sdk.{kv,txn,tso,lease,watch,storage}`；带平面后缀的旧入口
   （`kvV2()` …）**已删除**（0.x 内的一次性改名，不保留别名——别名会让"两套面"重新长回来）。
   **禁止**再新增带平面后缀的客户端入口。
5. **D5 不做数据面探测**：D2 之后不存在"这台集群跑哪个平面"的问题，`Handshake` 不新增
   `data_plane` 字段——部署细节不进协议。
6. **D6 dev 数据目录不就地迁移**：检测到 v1 布局（`store.db` / `raft-log/`）时 fail-closed，
   提示 `--fresh` 或换 `--data-dir`；绝不静默把旧目录当空库启动。
7. **D7 生产 v1 → v2 口径不变**：仍按 ADR-0015 D13（greenfield / `legacy_migration`），
   本 ADR 只改 dev 拓扑与客户端面。

## 后果

- **收益**：调用方只有一个语义面（不再按部署切方法名）；`coord dev` 与生产同构，
  TSO/亚秒租约/OCC/MQ v2 前置在 dev 上直接可用；SDK 的 `v2_compat` 等价物不再需要写在每个消费者里。
- **代价**：`coord dev` 首次启动变为多 Region 装配（Region 表种子 + 内嵌 PD），启动路径更长；
  v1 dev 数据目录不兼容（D6 显式拒绝）；dev 相关集成测试必须改用 v2 数据面。
- **不受影响**：既有 v1 集群（D3 只降状态、不改语义）、v2 集群的 v1 冻结行为（B-KVT-2）、
  内部 `coord-client` 的 `v2_compat`（D1 之后它是唯一实现路径，不再需要 v1 分支）。

## 验收判据

1. `coord dev --fresh` 起来后：`coord.tso.v2.GetTs` 单调可用；`coord.txn.v2` 的
   Begin/Read/Commit CAS 成功；v1 `coord.kv` 在该拓扑上返回 `FAILED_PRECONDITION`
   （冻结语义与 B-KVT-2 一致，不静默成功）。
2. 既有 v1 dev 数据目录启动 ⇒ 明确报错并点名 `--fresh`（不静默重建、不半启动）。
3. `coord dev` 既有集成测试（内建服务面、PKI CLI、非 loopback 绑定等）在 v2 拓扑下全绿。
4. 既有 v1 集群回归测试全绿（D3 不改变 v1 行为）。

## 参考

- ADR-0015（KV-T 全局 TSO 与严格 watch）、ADR-0016（MQ v2）、ADR-0017（`[kv_t]` 三态默认）
- `docs/production/ops/boundaries.md` B-KVT-1 / B-KVT-2
- `coord-client/src/v2_compat.rs`（v1 形状 → v2 wire 的既有实现）
