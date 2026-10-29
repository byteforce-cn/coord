# KV-T G6 — Region 心跳统计/采样上报（#278）

**Goal:** 让 PD 调度面拿到真实 Region 数据（`size`/`keys`/对象存储字节/leader/采样 Key），
使 `region_split_keys`/`region_split_size_mb` 与 size 驱动的 merge 真正生效，并让
`perf_split_write_pause` 产出 split 写停顿数字（#174 的剩余项）。

**Spec:** `docs/adr/0015-kv-t.md` D6/D9/D10；issue #278；`docs/production/ops/kv-t-ga-gate.md` §3.5-6。

## 现状（缺口）

- `PlacementDriver::handle_region_heartbeat` 已在每节点的 `heartbeat_loop` 调用，但**只喂本节点
  的 PD**：operator 生成收敛在 region 0 leader 的 PD，其他节点上报的 Region 统计到不了那里。
- 心跳循环遍历启动时的静态 `region_ids`，split/merge 产生的新 Region 不进入上报。
- `region_sample_keys` 只有测试注入；`RegionHandle::update_stats` 无调用方。
- `coord_region_split_total` / `coord_region_merge_total` / `coord_regions_total` 无生产写入点
  （`perf_split_write_pause` 的判据因此恒 0）。

## 设计决定

1. **传输选节点间 RPC，不落 region 0 raft。** 统计是派生瞬态数据（`PdMetaStore` 已明确
   内存视图、不写穿），走 raft 会为每拍心跳引入一次共识 + fsync，与既有取舍冲突。
   新增 `coord.raft.Raft/ReportRegionStats`（与 `SubmitPdOp`/`SubmitRegionCommand` 同一条
   认证 + 连接池路径），只由 Region leader 发往 **region 0 leader 节点**的 PD；
   丢失/换届只损失一拍，下一拍重报。
2. **上报方 = 本节点确认为 leader 的 Region。** follower 的 `size/keys` 会滞后；上报
   `leader_node_id` 与 raft leader 判定一致（0 = 未知，接收方清除陈旧视图）。
3. **采样 Key = 有界字节空间分位探测。** `MvccStore::sample_keys_in` 在 Region 区间内按
   中点递归生成探测点，每个探测点有界扫描 1 行；无样本时调度器仍回退 `mid_key`。
4. **周期覆盖动态 Region。** 心跳循环遍历 `RegionManager::list_runtimes()`；Region 注销后
   上报以 `not found` 失败，PD 顺手清掉该 Region 的 leader/样本视图。
5. **可观测性补齐。** 心跳循环刷新 `coord_regions_total`/`coord_nodes_online`；operator
   success 时按类型累加 split/merge 计数。
6. **PD 元数据周期对账。** 实现中发现：Region 表由 region 0 raft 复制到每个节点，但
   split/merge 执行器只更新执行节点的 `PdMetaStore`，其他节点的调度视图停在旧表。
   心跳循环每拍把本地已 apply 的权威表对账到本机 PD 元数据（执行器收尾复用同一实现）。

## Tasks

- [x] Task 1: `MvccStore::sample_keys_in`（分位探测、有界扫描、单元测试）
- [x] Task 2: PD 样本视图 + 上报入口（`RegionHeartbeatReport`/`RegionHeartbeatSink`、
      未知 Region 清理、`ScheduleContext` 填充、单元测试）
- [x] Task 3: raft RPC 接线（proto + 出站 client + 入站 handler + 能力豁免）
- [x] Task 4: 心跳循环（list_runtimes、leader 过滤、采样、本地/转发、gauges）与 main 装配
- [x] Task 5: 进程级验收：3 节点 v2 集群跨节点写 → PD 统计非零 → 自动 split 落地；
      `perf_split_write_pause` 去掉 debug-only 调度钩子依赖并产出数字
- [ ] Task 6: 文档（ADR 决定、GA gate §3.5-6 关闭、performance §6.6 数字）+ 门禁 + PR

## 验收

- 3 进程 v2 集群上，Region leader 与 region 0 leader 不同节点时，写超阈值后
  `coord_region_split_total` 增长（跨节点统计送达）。
- `[kv_t].enabled=false` / `multi_raft.pd.enabled=false` 路径行为不变。
- `PERF_GATE=1`（release）`perf_split_write_pause` 输出 `split_write_pause` 数字并失败即红。
