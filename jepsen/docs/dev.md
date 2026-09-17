# coord Jepsen 开发计划（v3）

> 配套文档：[`coverage-gaps-and-implementation-plan.md`](coverage-gaps-and-implementation-plan.md)
> （缺口证据基线，缺口编号 A1–G5）。
>
> **v3 修订说明**（2026-09-16，第二轮评审）：① 统一工时/机器时间算术，明确
> 计划内/缓冲/预留边界；② 短矩阵验收按 workload 类型细化；③ 排期拆分为
> W1–W8 并设收口缓冲；④ 关键路径显式标注 `T1.4 ⇒ T5.2` 硬前置；⑤ 锁时钟
> 容差/RTO 分档/split 短跑等参数列入"待 coord 团队确认"表（§5.4）；⑥ 补充
> lab 预约/CI 队列规则（§7.2）；⑦ P0 书面放弃改为双签 + 有效期流程（§6）；
> ⑧ 延期项回补改为角色责任并纳入 90 天跟踪（§8）。
>
> **v3.1 进度说明**（2026-09-16，M0 落地）：M0 六个任务的**代码**已全部落地，
> 其中 T0.1/T0.2/T0.3/T0.5 已本地验证（含用真实 store 回归），T0.4/T0.6 的
> **lab 实跑判定**待做。静态审计产出 8 条 coord 侧发现，其中 4 条直接回答了
> §9 的 ①②⑤⑧，并**修正了本计划 3 处**（§5.4-⑥ watch 语义、T3.4 故障设计、
> T2.1 默认 checker）。逐步进度与证据见 [`PROGRESS.md`](PROGRESS.md)；
> 缺陷单与倒逼结论见 [`coord-findings.md`](coord-findings.md)。
>
> **v3.2 进度说明**（2026-09-16，M0 收口 + M1 起跑）：
> ① **M0 全部收口**：docker lab 上实跑 `make checkers`（4 套 / 29 fixture 全绿）、
> `make env-reset STRICT_CLOCK=1`（control + n1..n5 全 clean）、
> `make quick SEED=42`（绿，`:seed 42` 入 results 与 MANIFEST）、
> `make test NEMESIS=kill SEED=42`（节拍实测 15 个 gap / 13 个不同取值）。
> ② **T1.4 幂等专项落地并跑出红证据**：`--workload idempotency` +
> `jepsen.coord.idem` checker + 10 个负控制 fixture，**零故障注入**即复现
> F-01（Delete 无幂等，含范围删重放删掉新写入）与 F-02（Put 命中丢 `prev_kv`），
> 47/47 与 26/26 全中；F-01/F-02 因此升级为 `confirmed-by-run`。
> ③ **实跑暴露并修掉 4 条测试自身缺陷**（F-09…F-12），全部属
> 「离线看着是绿的」那一类；本计划据此**修正 2 处判据/实现约定**（见 §5.5）。
> ④ 为 T1.1/T1.2/T1.3 备齐了协议层 builder（`proto.clj`：4×3 种 compare、
> range 变体、delete 的 `range_end`/`prev_kv`、txn 的 delete/range RequestOp）。
>
> **v3.3 进度说明**（2026-09-16，M1 数据面主体落地 + 首轮倒逼落地）：
> ① **T1.1 / T1.2 / T1.3 落地并在 docker lab 实跑通过**（`map` / `txn` /
> `scan` 三个 workload + 专用 checker + 30 个新负控制 fixture）：map 60s
> `--nemesis kill` 绿（170 ops / 27 delete = 15.9%）；txn 60s kill 绿
> （176 txn：成功 141 / 失败 31，**两条分支都真跑到**，无副作用/原子可见/半
> 应用可见 0 违反）；scan 60s kill 绿（93 scan / 44 read-at / 404 个值观察，
> 0 违反）。
> ② **倒逼落地（coord 侧）**：F-01（Delete 无幂等）与 F-02（Put 命中丢
> `prev_kv`）**已修**（`IdempotentEntry` 存完整响应 + delete 入口/出口补去重），
> `cargo check -p coord-server` 通过；回归 run 见 `PROGRESS.md` §2。
> ③ **真跑又暴露 5 条测试自身缺陷（F-13…F-17）**，其中 **F-13 最严重**：
> `txn-req` 用了不存在的 `DynamicMessage$Builder.addAllField`，于是
> **cas / exists / 全部 txn-\* 从未真的发过请求**，而 knossos 对全 `:info` 的
> 历史判 valid —— 整个 Txn 面在报告里是绿的。为此新增 **G6「op 级活性」门槛**
> （单类 op 的 `:ok` 率下界，F-13 的第二形态与 F-14/F-15 都是它抓到的）。
> ④ §9-⑨ 已答（version 起始值 / 不存在表示，见 `coord-findings.md` F-18）。
> ⑤ **回归与收口**：修完 F-01/F-02 后同一 T1.4 workload **154 组 / 352 次重放
> 0 违反**（F-01/F-02 `closed`）；F-03 用 `--nemesis kill --idem-replay-delay-ms
> 4000` 跑出 13 组 `:revision-advanced`（`confirmed-by-run`），并把
> `kv.proto`/`txn.proto` 的 `request_id` 注释**限缩到实际作用域**（单节点 /
> 进程内 / 不复制不持久 / 60s / 4096 FIFO）。checker fixture 现为 **8 套 64 个**，
> 全部进 `make checkers` 前置门。
> ⑥ **机器时间**：本轮 docker lab 实跑约 40 次短跑（20–150s）≈ 1.2h，仍在
> §7.2「短矩阵不计入主表」的额度内；vagrant 的 8h/24h/72h 额度未被占用。
>
> **v3.4 进度说明**（2026-09-17，T1.5 落地 + M1 收口）：
> ① **T1.5 `mixture` workload 落地**：`--workload mixture` 把 map / txn / scan 三个面
> 混在同一条历史里跑，每份 op 带 `:sub` 标记（生成器打、客户端 completion 原样保留），
> 由新 checker `jepsen.coord.mixck` 按 `:sub` 切片后分别喂给三个面的专属 checker
> （`mapck` / `txnck` / `scanck`）—— **不新造判据**，只做路由与组合。
> `--mixture-ratio W,W,W`（默认 `4,2,2`）是**目标 op 份额**。
> ② **新增「组合层活性」门槛**（F-13 的组合形态）：`mixck` 的 `routing-checker` 断言
> 「每个面的完成数 ≥ `--min-op-sample`」且「不存在没被任何 checker 路由的客户端 op」。
> 理由是一条真会发生的失败：生成器忘了打 `:sub`（或切片条件写错）时，三个子 checker
> 各自按 `:sub` 过滤，谁也看不到那条 op，**组合结果是绿的**——这正是 F-13 那一类假绿。
> 每个 run 的服务侧报告里还带**实测 op 份额** `:routing {:share ...}`，让混合比偏差可见。
> ③ **口径实证**（写进 `coord.clj` 注释，避免后人再走一遍）：`gen/mix` 是「均匀抽一个
> 子生成器、从它取 **1 个** op」，所以面份额 = **实例数占比**，与子生成器内部槽位数无关。
> 实测 4/2/2 实例的一轮 120s run（1198 个 client op）份额 0.517/0.255/0.228
> （目标 .50/.25/.25，±0.015）；「按内部槽位 8/5/5 折算」的模型预测 .615/.192/.192，
> 被实测否掉。**这条修正推翻了两处中间实现**（先按槽位折算，再改回按实例数）。
> ④ **T1.5 workload 质量改进（CAS 命中率）**：`cas-register` 的 `old` 原来从
> `client/seen`（200 元集合）均匀抽样 ⇒ 45s 只有 13 个 `:ok` / 108 个 cas（其余全
> `cas-miss`，有效线性化样本少一个量级，G6 的 `:ok` 率 0.12 也贴着 0.1 下界）。
> 改为 `client/last-seen`（**最近一次观察到的值**）= 教科书 read-then-CAS。正确性不受
> 影响：CAS 成功 ⇒ 该时刻寄存器确实是 `old` ⇒ 线性化点必落在自己的区间内。
> ⑤ **fixture 门槛扩到 14 套 86 个**（`scripts/mixture-fixtures/` 8 +
> `mixture-fixtures-routing/` 3 + `watch-fixtures/` 7 + `watch-fixtures-sample/` 2 +
> `watch-fixtures-lossless/` 1 + `watch-fixtures-coalescing/` 1）：mixture 那套证明
> **任一面的违反都能穿透组合**（只违反 map / 只违反 txn / 只违反 scan 各一条）+
> 未路由 op 判红 + F-17 的 `:info` 写可被观察，routing 那套把「每个面必须真的
> 跑到」单独钉住（`:min-sample 3`）；watch 那套四态判据 + 三个语义档各一条 +
> F-21 的空会话守门员。
> ⑥ **matrix-m1 增加 mixture 两档**（none / kill），与 `make checkers` 一起构成 M1 门禁。
> ⑦ soak 编排补参数：`coord-soak.sh` 与 `make soak` 现在能把 `--seed` / `--concurrency` /
> `--mixture-ratio` / `--map-min-deletes` 透传给 coord（此前只有 hours/rate/quiet/disrupt/
> regions），T1.5 的 2h 浸泡因此可以带固定种子与 delete 样本门槛。
> ⑧ **本轮实跑结果**（详见 `PROGRESS.md` §1.4/§1.5；全部为 docker lab 真跑）：
> `make checkers` 10 套 75 个 fixture 全绿（T2.x 后 **14 套 86 个**）；
> `make matrix-m1` 9 组合全绿（map/txn/scan/mixture × none|kill + idempotency）；
> mixture 60s kill 绿；F3 值大小扫描 4KB / 64KB 各 60s kill 绿（值长度实测
> 4096 / 65536）；**T2.0/T2.1 watch：none 45s 绿（380 事件）、kill 120s 绿
> （1070 事件 / 78 次流重开）**；T1.5 2h mixture 浸泡（rate 2 · seed 42 ·
> `--checker soak`）结果见 PROGRESS。
> ⑨ **T2.0 流式接线落地**：`CoordRpc.java` 补上 `coord/watch/watch.proto` 的
> FileDescriptor + `Watch` 的 **BIDI_STREAMING** MethodDescriptor + `Watcher`
> 类（后台读线程 → 有界队列 → Clojure 侧 `.poll`/`.tryPoll`/`.close`，`close()`
> 即契约里的「关闭流 = 取消」；读线程每收一条**主动 `call.request(1)`**，否则
> 只会收到一条就停住）；`proto.clj` 补 `watch-create-req` / `watch-req` /
> `event-type`（enum 标量**没有 presence**，必须走 `EnumValueDescriptor`，
> 用 `hasField` 会抛）/ `event->edn` / `response-events` / `open-watch`。
> ⑩ **T2.1 watch workload 落地**：`--workload watch`（一个 watch key + 写产生
> 事件）+ `jepsen.coord.watchck`。**一次「会话」= 一个 op**：开流 → 窗口内收事件
> → 关流；流中断时按契约**在同一个 op 内**重开到 `last-revision + 1`
> （`--watch-resumes`）。两种会话：`start-revision 0`（从最新）与 `:last`
> （从本客户端上次观察到的最大 revision = 契约的续传对照路径）。
> checker 四条判据 + 一条门槛：①流内 revision 严格递增（+类型/kvs 结构）
> ②`start_revision = R > 0` 的会话不得收到 revision < R 的事件
> ③事件值必须真的写过（fabricated）
> ④**静默丢事件**（P0）：确认写的 revision 落在会话有效区间 `(lower, upper]`
> 内却没出现，且缺口之后没有 `BUFFER_OVERFLOW`/`HISTORY_UNAVAILABLE` 标记
> ⑤事件数 < `--watch-min-events`（默认 200，§5.1）⇒ invalid（不是绿）。
> **可判定性设计**：`start_revision = 0` 时服务器不告诉客户端当时的 revision，
> 因此取**首个事件的 revision** 当保守下界（只可能漏报、不会误报 —— 与 soak
> checker 的 P0-4 修正同一取向）；`>0` 时下界就是它，判据②因此可严格判。
> **语义档** `--watch-semantics overflow-marker|lossless|coalescing`：默认按
> F-06 的源码口径（丢最旧 + 显式 BufferOverflow + 按 revision 去重）；
> lossless 与 coalescing 用**同一条有缺口的历史**做期望值相反的 fixture，
> 证明参数真的接上了（否则「参数化」只是文档里的说法）。
>
> **v3.5 进度说明**（2026-09-17，T2.2 落地 + T6.1 组合浸泡入口 + soak 结项）：
> ① **T2.2 lease workload 落地**：`--workload lease` 三个场景（`:lease-ttl` 到期 /
> `:lease-keepalive` 续期 / `:lease-revoke` 撤销级联）+ 新 checker
> `jepsen.coord.leaseck`（6 条判据 + §5.1 样本门槛）。**判定基准是客户端单调量**
> （`:at-ms` = 相对 op 起点的毫秒），不跨机比较时钟 —— 与 F-08 的单调时钟口径一致，
> 因此不受节点时钟差与墙钟跳变影响。接线：`CoordRpc` 补 `coord/lease/lease.proto`
> 的 FileDescriptor + `LeaseGrant`/`LeaseRevoke`（unary）+ **`LeaseKeepAlive`
> （BIDI_STREAMING）** 与 `KeepAliver` 类；`proto.clj` 补 `put-req*`（含
> `lease_id` 绑定字段）、`lease-grant-req` / `lease-revoke-req` / `open-keepalive` /
> `keepalive->edn`。
> ② **T6.1 组合浸泡入口落地**：新增 `--workload soakfull` + `--soak-mix`
> （面集合与权重，默认 = T6.1 比例里已实现的部分 `map=40,txn=20,scan=5,watch=15,
> lease=10`）。组合 checker 复用 `mixck`（泛化出 `:surfaces` 入口：每个面仍然由
> **它自己的** checker 判，组合层只加「每面最小样本 + 禁止未路由 op」两道路由门槛）。
> **拒绝静默丢弃**：`--soak-mix` 里出现 lock/election/registry（需 M5 的 agent
> 插件面）时**构造期抛异常** —— 一个「少跑 3 个面但全绿」的 72h soak 比不跑更糟。
> ③ **soak 常态化（夜间门禁）**：新增 `jepsen/scripts/nightly-soak-gates.sh` 与
> `make nightly` / `make soakfull` / `make soak-wait`：`checkers`（17 套 98 个
> fixture）→ `matrix-m1`（9 组合）→ `matrix-m2`（**8 组合**，本轮加入 lease 四档）
> → `soakfull`（默认 2h，可 `SOAK_SECONDS=259200` 跑 72h）→ 等待 → 取结果。
> `coord-soak.sh` 新增通用 `--extra` 透传（新选项不必再改三处）与 `wait` 子命令。
> ④ **soak 结项报告**：[`soak-closure-report.md`](soak-closure-report.md) —— 量化
> 「soak 逼出了什么」：22 条缺陷单里 coord 侧 8 条（已修 2 / 契约限缩 2 /
> 行为判定 3 / 未闭环 1）、测试自身 14 条（其中 7 条属「报告全绿而覆盖面为零」
> 的假绿自查），并诚实列出未兑现部分（M3–M6、lock/election/registry、72h）。
> ⑤ **本轮新增 3 条测试自身缺陷（F-23/F-24/F-25）**，全部由「改完先加载一次」
> 这条纪律抓到：docstring 里的 ASCII 引号让 ns 编译不过（F-23）、`:keys [.. surfaces ..]`
> 遮蔽本 ns 默认值导致默认三面失效（F-24）、编辑 `cli-opts` 少一个 `[` 让整个文件
> 读不进去而报错位置在文件末尾（F-25）。三条都已修 + 回归。
> ⑥ **fixture 门槛扩到 17 套 98 个**（新增 `lease-fixtures` 8 + `lease-fixtures-sample`
> 2 + `soakfull-fixtures` 2），全部进 `make checkers` 前置门。
>
> 总量：**计划内 31.5 人日 + 缓冲 3 人日 = 34.5（对外承诺区间 30–40）**；
> 机器时间：**计划内 122h + 重跑/补采预留 30–80h = 上限 200h**。
> 本计划不含被逼出的 coord 缺陷的修复工时。

## 0. 执行纪律（全程适用）

1. **负控制先行**：每个新 checker 必须配 `expect-invalid-*.edn` 坏历史 fixture，
   经统一运行器（T0.3）验证能抓红。没有负控制的 checker 不算完成。
2. **不得弱化断言**：跑出 coord 缺陷时测试保持红 + 按 §6 缺陷 SLA 立项，禁止
   改断言迁就实现。
3. **Phase 门径**：每 Phase 结束回归全绿（标准见 §5.1）才进入下一 Phase；
   coord 缺陷导致的红按 §6 分级处理。
4. **可复现**：所有 run 记录随机种子（T0.5），失败历史自动归档可回放；每次
   长跑产物按 T0.1 规范入库。
5. **环境幂等**：任何 nemesis 结束后环境必须经统一清理验证（T0.4）才可进入
   下一 run。
6. **参数确认**：凡 §5.4 表列参数，验收级 run 必须使用经 coord 团队确认的
   取值；默认值仅限开发迭代。

---

## 1. 覆盖矩阵（缺口 × 任务 × 验收证据）

| 缺口 | 任务 | 证据产物 |
|:--|:--|:--|
| A1 Delete 未测 | T1.1 | **已产出**：`--workload map`（多 key + delete/tombstone + 存在性）+ `jepsen.coord.mapck`（knossos 短矩阵 / O(n) 索引长跑两路）+ `scripts/map-fixtures/` 12 个 fixture；实跑 60s kill 绿（170 ops / 27 delete = 15.9% ≥ 10%） |
| A2 Range 单形态 | T1.3 | **已产出**：`--workload scan`（range_end / limit / keys_only / count_only / 历史 revision 读）+ `jepsen.coord.scanck` + `scripts/scan-fixtures/` 10 个 fixture；实跑 60s kill 绿（93 scan / 44 read-at / 404 值观察） |
| A3 Txn ~5% 形态 | T1.2 | **已产出**：`--workload txn`（create-if-absent / 多 key 写集+区间回读 / 值 CAS / 双分支 CAS-delete / txn 内读）+ `jepsen.coord.txnck` + `scripts/txn-fixtures/` 8 个 fixture；实跑 60s kill 绿（176 txn / 成功 141 / 失败 31） |
| A4 request_id 幂等 | T1.4 | **已产出**：红证据 `evidence/20260916T132103Z-t1.4-idempotency/`（零故障注入即红：Delete 47/47、Put 命中 26/26）；修复后回归 `evidence/20260916T150557Z-t1.4-regression-after-f01-f02-fix/`（154 组 / 352 次重放 **0 违反**）；跨节点重放 `evidence/20260916T150552Z-t1.4-cross-node-f03/`（13 组 `:revision-advanced`，`:confirmed-by-run`）+ 契约措辞已限缩 |
| A5 Watch 未测 | T2.0+T2.1 | **已产出**：watch 矩阵 + watch-checker fixtures；lab 实跑 none 45s（380 事件）/ kill 120s（1070 事件 / 78 次流重开）均绿 |
| A6 Lease 未测 | T2.2 | **已产出**：`--workload lease`（到期 / 续期 / Revoke 三场景）+ `jepsen.coord.leaseck`（6 条判据）+ `scripts/lease-fixtures/` 9 个与 `lease-fixtures-sample/` 2 个；lab 实跑 60s 绿（grants 207 / expiries 95 / keepalive 58 / revoke 54，六类违反 0）：`evidence/20260917T144810Z-t2.2-lease-60s/` |
| B1 成员变更 | T3.1 | membership 矩阵 results |
| B2 Snapshot/Compact | T3.2 | compact nemesis + read-at 断言 |
| B3 Seal/Unseal | **延期**（§8，角色责任） | — |
| B4 时钟故障 | T3.4 | clock×lease 组合 results |
| B5 网络单形态 | T3.3 | netem/端口分区矩阵 results |
| B6 磁盘故障 | T3.5 | disk-full 行为记录（预期立项） |
| B7 滚动升级 | **延期**（§8，角色责任） | — |
| C1 PD 动态调度 | T4.1 | split/merge 中 per-key 线性证据 |
| C2 跨 region 拒绝 | T4.2 | 拒绝断言 + 无副作用检查 |
| C3 Region 快照/压缩 | T4.3 | region_id 变体 results |
| D1 agent 直连缺口 | T5.1+T5.2 | --via-agent soak（含 2h 提前冒烟） |
| D2 Lock/Election/Registry | T5.3/T5.4/T5.5 | 三 checker fixtures + 矩阵 |
| E1 非 root RBAC | T5.7 | 权限不放大断言 results |
| E2 TLS | **延期**（§8，角色责任） | — |
| E3 CCT 过期 | 已覆盖（现状） | 现有 soak re-auth 日志 |
| E4 登录限流 | T5.2 顺带观察 + §9 倒逼清单 | soak.log 限流计数 |
| F1 单 key | T1.1/T4.1 | 同上 |
| F2 soak 压力过低 | T6.0 高压长跑 | 12h×200ops/s results |
| F3 值形态单一 | T1.1（值大小扫描选项）+ T4.1 | value-size sweep results |
| F4 cas 无 soak | T1.2（txn-checker 即 O(n log n)，可直接长跑） | txn 2h+ soak results |
| G1 checker 前提保护 | T0.2 | 值唯一性自检 + fixtures |
| G2 可用性门槛 | T0.2 | quiet-availability 门槛结果 |
| G3 nemesis 节拍固定 | T0.6 | 抖动化后的矩阵 results |
| G4 无 RTO 断言 | T0.2 | RTO P95/max 进 summary |
| G5 产物零入库 | T0.1 | docs/production/evidence/ 目录 |

延期项（B3/B7/E2）不阻塞引入决策，回补责任为 §8 的角色责任，纳入 90 天
跟踪；对应生产动作（启用加密 / 首次升级 / 跨机暴露 gRPC）发生前必须闭环。

---

## 2. 任务依赖图

```
T0.1 evidence ─┐
T0.2 gates   ──┤ M0（全部前置）
T0.3 fixture ──┤
T0.4 cleanup ──┤
T0.5 seeds   ──┘
T0.6 jitter  ───────────────┐
                            ▼
T1.1 map/delete ──► T1.3 scan/rev ──► T3.2 compact ──► T4.3 region-compact
     │                  │
     │                  └──► T1.2 txn ──► T1.5 M1收口(2h soak)
     │                              │
T1.4 幂等 ──（与 T1.2/T1.3 并行）───┘
     │
     ║  ★ 硬前置：T1.4 幂等结论未经评审，T5.2 不得起跑
     ║  （agent 重试路径依赖 request_id 去重语义）
     ▼
T2.0 流式接线 ──► T2.1 watch ──► T2.3 M2收口(2h soak)
               └─► T2.2 lease ──►（与 T3.4 clock 联调）
                                    │
T3.1 membership ─► T3.6 M3收口(8h soak，叠加 T3.2/3.3)
T4.1 PD split ──► T4.2 跨region ──► T4.3
                                    │
T5.1 agent 部署 ──► T5.2 冒烟(2h) ──► T5.3 lock ─► T5.4 election
      ▲                │          └─► T5.5 registry └─► T5.7 RBAC
      ║ T1.4 ══════════╝            ▼
      ║ 硬前置                 T5.6 M5收口(24h soak)
      ║                            ▼
      ╚══ T6.0 高压 12h ──► T6.1 72h soak-full ──► T6.2 证据 ──► T6.3 缺陷账
```

硬依赖（不可并行）：T1.3→T3.2→T4.3；T2.0→T2.1/T2.2；T5.1→T5.2→T5.6；
T6.0→T6.1；**T1.4 ⇒ T5.2（跨里程碑硬前置）**。
可并行：M3 与 M5（两人时，资源规则见 §7.2）；T1.4 与 T1.2/T1.3；
T3.3/T3.4/T3.5 彼此。
**关键路径**：`M0 → T1.1 → T2.0 → T2.1 → T5.1 → T5.2 → T5.6 → T6.0 → T6.1`。
注：T5.2 另受 `T1.4 ⇒ T5.2` 约束——**M1 若延误，M5 冒烟直接顺延**，
排期不得假设两者解耦。

---

## 3. 里程碑与工时（v3 统一算术）

### 3.1 人日

| 里程碑 | 任务 | 人日 |
|:--|:--|:--|
| M0 基座 | T0.1 evidence+MANIFEST(0.5) · T0.2 checker 门槛(1.5) · T0.3 fixture 运行器(0.5) · T0.4 环境清理(0.5) · T0.5 种子回放(0.5) · T0.6 nemesis 抖动(0.5) | 4 |
| M1 数据面 | T1.1 map/delete(2) · T1.2 txn 全形态+checker(2.5) · T1.3 scan/rev(1) · T1.4 幂等(0.5) · T1.5 收口(0.5) | 6.5 |
| M2 Watch+Lease | T2.0 流式接线(1) · T2.1 watch(2) · T2.2 lease(1.5) · T2.3 收口(0.5) | 5 |
| M3 运维面 | T3.1 membership(2) · T3.2 compact(1) · T3.3 网络增强(0.5) · T3.4 clock(0.5) · T3.5 磁盘满(0.5) · T3.6 收口(0.5) | 5 |
| M4 Multi-Raft | T4.1 PD split(1.5) · T4.2 跨region(0.5) · T4.3 region compact(0.5) | 2.5 |
| M5 Agent 层 | T5.1 agent 部署(1.5) · T5.2 冒烟+分析(1.5) · T5.3 lock(1.5) · T5.4 election(1) · T5.5 registry(0.5) · T5.7 RBAC(0.5) | 6.5 |
| M6 收口 | T6.0 高压 12h(0.5) · T6.1 72h soak-full(0.5) · T6.2 证据+文档(0.5) · T6.3 缺陷账(0.5) | 2 |
| **计划内小计** | | **31.5** |
| 缓冲 | W3/W4/W5 各 1d（缺陷复现/最小化/重跑机动） | 3 |
| **合计（对内）** | | **34.5** |
| **对外承诺区间** | | **30–40** |

### 3.2 机器时间

| 类别 | 明细 | 时长 |
|:--|:--|:--|
| 计划内长跑 | M1 2h · M2 2h · M3 8h · M5 冒烟 2h · M5 24h · T6.0 12h · T6.1 72h | **122h** |
| 短矩阵/冒烟/fixture | 不计入主表，计入 §7.2 lab 占用日历 | — |
| 重跑/补采预留 | 失败复现、争议段重跑、证据补采 | +30–80h |
| **上限** | | **200h** |

边界口径：**计划内** = 一次通过的净成本；**缓冲（人日）** = 排期内的机动，
不用则提前交付；**预留（机器）** = 不排进日历但保留资源额度，超限需升级
评审。

---

## 4. 任务详单（v3）

### M0 — 基座

**T0.1 证据入库管道（0.5d）**——MANIFEST 字段：coord commit、tree dirty、
确切命令行、节点拓扑、workload/nemesis/参数、随机种子（T0.5）、Jepsen/JVM/
Clojure 版本、coord-proto 哈希、lab 镜像 ID、config 文件哈希、**§5.4 参数
确认记录链接**。附 `make soak-status` 轮询 + 退出码告警脚本（邮件/IM）。

**T0.2 soak checker 门槛增强（1.5d）**——三项断言（参数见 §5.4-②③）：
1. quiet-window 可用性：quiet 窗口内 write `:ok` 率；**窗口样本 < 100 不评估**
   （记入 summary 不参与判定）。
2. RTO：stop 完成→首个 `:ok` write；按 nemesis 分档（§5.4-②），summary 输出
   P95/max；`--soak-max-rto-seconds` 可全局覆盖。
3. 值唯一性自检：同一 value 两次 write invoke → invalid。
fixtures 4 个：quiet 不可用 / RTO 超时 / 重复值 / 样本不足不误判。

**T0.3 统一 checker fixture 运行器（0.5d）**——按 namespace 参数化
（`scripts/run-checker-tests.clj <checker-ns> <fixture-dir>`）；接入
`make quick` 前置，fixture 红拒绝起跑。

**T0.4 环境清理与 preflight（0.5d）**——`scripts/env-reset.sh`：清
iptables/tc、强制时钟同步并验证偏移 < 100ms、删 fallocate 填充、pkill
coord、清数据目录；每个 run 的 setup/teardown 强制执行并输出校验结果；
各 nemesis `:stop` 路径内置幂等清理。

**T0.5 随机种子与回放（0.5d）**——`--seed`（缺省生成并打印）；results 记录
种子；`scripts/replay.clj` 重建同种子历史（generator 层确定性）；失败
history.edn 自动随 evidence 归档。

**T0.6 nemesis 抖动（0.5d）**——G3：5s 固定节拍改 3–8s 均匀抖动（种子驱动）；
soak quiet/disrupt 加 ±20% 抖动。

### M1 — 数据面核心

**T1.1 map workload（2d）**——delete 语义三层处理：
- knossos 短跑：delete 建模为 `write nil`；
- soak checker：delete 以显式 tombstone 哨兵入 write-index，与 nil 区分；
- 存在性由 txn 的 `Compare{VERSION, EQUAL, 0}` 断言（version 起始值/不存在
  表示先读源码确认，§9-⑨）。
`--value-size`（默认 16B；矩阵补 4KB/64KB 两档，F3）。

> **已落地**（§9-⑨ 已答）：`--workload map`（`--map-keys` 默认 8；`--value-size`
> 默认 16B）、`jepsen.coord.mapck` 双路 checker（`:linear` = 逐 key knossos +
> delete→`write nil` 改写；`:index` = O(n log n) 写索引，长跑用）、
> `--map-min-deletes` 样本门槛（§5.1 的 delete ≥ 10%）。
> delete 响应本身也被断言：`deleted ∈ {0,1}`、`count(prev_kvs) == deleted`，
> 且 `prev_kvs` 里的值是**一次读**（必须满足 fabricated/future/stale 三条）。

**T1.2 txn 全形态（2.5d）**——checker 语义形式化：
- **无副作用**：`succeeded=false` 时未被执行分支的写不得出现在任何后续
  `:ok` 读；
- **原子可见**：对 txn T（写集 W），首个观察到 T 任一写入的 `:ok` 读是全局
  分界点；完成时间更晚的任何 `:ok` 读若读 W 中 key，不得看到 T 之前的值。
- compare 见证池（保证 §5.1 的命中率门槛）；txn-checker O(n log n)，兼解 F4。
fixtures：failure 泄漏 / 半应用可见 / 存在性误判。

> **已落地**：`--workload txn` 五个形态（`:txn-create` / `:txn-write-set` /
> `:txn-cas` / `:txn-cas-delete` / `:txn-read`）；`:txn-cas*` 每 3 组有 1 组
> 故意传错的 compare 值 ⇒ **失败分支被真实执行**（否则「无副作用」永远只测
> 一半；首次实跑就是 `:branch-taken :success 159 / :failure 0`，已修）。
> checker 断言：失败分支泄漏（P0）/ 成功必可见 / 半应用可见（P0）/ 失败分支
> 未执行 / create-if-absent 在全新 key 上必须成立 / 逐 key 的 fabricated・
> future・stale（含 txn 写集在每个 key 上的投影）。

**T1.3 scan + revision 读（1d）**——前置：读 MVCC 源码确认 compacted 读的
错误码/行为。

> **已落地**（前置已答：`Error::RevisionCompacted` → `OUT_OF_RANGE`，map_err
> 见 `server/mod.rs:1178`）：`--workload scan` + `jepsen.coord.scanck`：结构断言
> （字典序/无重复/区间内/limit/count 自洽/keys_only/count_only）+ **精确**历史
> revision 读断言（写响应 revision = r ⇒ 读 r 必须拿回那次写的值，且与同一
> 次操作里先做的最新点读一致）+ 每个返回 kv 的值层判据（fabricated/stale）。

**T1.4 request_id 幂等（0.5d）**——同一 rid 重放 ≤3 次记入 history；断言
version 至多前进 1。**产出（去重是否实现、窗口多大）是 T5.2 的硬前置评审
项**，同时进 §9-①。

> **前置结论已交付**（F-01/F-02/F-03，静态审计，2026-09-16）：去重**已实现**但
> 作用域是「单节点进程内 + TTL 60s + 4096 FIFO，不随 raft 复制、不持久化」；
> `Delete` **没有**去重（契约 `kv.proto:81` 却明确承诺）；`Put` 幂等**命中时**
> `prev_kv` 恒为 `None`。因此 T1.4 的 run 必须包含三个分支：同节点重放、
> **换节点重放**（先制造 leader 变更）、Delete 重放（含范围删 + `prev_kv`）。

**T1.5 M1 收口（0.5d + 2h）**——`mixture` workload；产物入库。

### M2 — Watch + Lease

**T2.0 流式接线（1d）**——docker lab loopback 冒烟先行（watch 单 key 收
事件、keepalive 往返），通过后再写 workload。

**T2.1 watch workload（2d）**——前置：读源码确认事件是否允许合并/压缩
（§9-⑧）。**已判**（F-06，`watch/mod.rs:228/267/90`）：丢最旧 + 显式
`BufferOverflow` + 按 revision 去重 → checker 参数化
`--watch-semantics coalescing|lossless|overflow-marker`，**默认第三态**。
零违反断言相应改为「事件序列要么完整有序，要么缺口之前出现过
`BufferOverflow`」。断言、resume、分区愈合补齐、4 个负控制 fixture 同 v2。

**T2.2 lease workload（1.5d）**——安全/活性断言；grace 参数（§5.4-⑤）；
与 T3.4 联调；**计时基准已判**（F-08，`lease/mod.rs:43`）：单调时钟
（`tokio::time::Instant`），契约成立——因此真正的证伪点是「长冻结期内单调钟
照走导致租约到期」与「重启丢 deadline 需由 raft 重建」，见 T3.4。

> **已落地**（第五轮）：`--workload lease` 三场景（`:lease-ttl` / `:lease-keepalive` /
> `:lease-revoke`）+ `jepsen.coord.leaseck`（6 条判据 + §5.1 样本门槛）。
> **判定基准全是客户端单调量**（`:at-ms` = 相对 op 起点的毫秒），**不跨机比较时钟**
> ⇒ 不受节点时钟差与墙钟跳变影响；TTL 一律用响应里**实际授予**的值（服务端可调整）。
> 活性锚点（停续期时刻 / Revoke 时刻）缺失时**不判**并计入 `:liveness-unjudged`
> （F-26：第一版在这里抛 NPE，jepsen 把整个 checker 降成 `:valid? :unknown`）。
> 接线：`CoordRpc` 的 `lease.proto` 描述符 + `LeaseKeepAlive`（BIDI_STREAMING）
> 与 `KeepAliver`；`proto.clj` 的 `put-req*`（`lease_id` 绑定字段）。
> 实跑：60s / 2n 绿（grants 207 / expiries 95 / 六类违反 0），证据
> `docs/production/evidence/20260917T144810Z-t2.2-lease-60s/`。

### M3 — 运维面

**T3.1 membership（2d）**——含 `membership-partition` 叠加 nemesis。
**T3.2 compact/snapshot（1d）**——前置：固化 `snapshot_logs_since_last`
语义（§9-③）。
**T3.3 网络增强（0.5d）** / **T3.4 时钟域故障（0.5d）** / **T3.5 磁盘写满（0.5d）**
——均接 T0.4 清理验证。

**T3.4 设计已修正**（F-08）：Lease 判定用单调时钟，**墙钟跳变对租约无效**
（照跑只会得到"时钟故障下租约完全正常"的假绿）。改为两类能真正证伪的故障：
`pause`（长冻结：单调钟在冻结期照走 → 租约到期）与 `kill`（重启丢进程内
`deadline` → 必须由 raft 重建）。

**T3.5 行为已判**（F-07）：可用空间 <5% → 写 `RESOURCE_EXHAUSTED`（6 个写
入口均经 `ensure_writable()`）、**读仍可用**、恢复空间后自动恢复 → 可写确定性
断言，不再只是"行为记录"。
**T3.6 M3 收口（0.5d + 8h）**。

### M4 — Multi-Raft 动态化

**T4.1 PD split（1.5d）**——停线范围：任何 per-key 线性违反或写丢失。长跑
前先在 docker lab 短跑（参数 §5.4-④）。**必须在 T3.2 之后**（依赖 compact
配置经验），故与 M5 并行而非 M3。
**T4.2（0.5d）** / **T4.3（0.5d，依赖 T3.2）**。

### M5 — Agent 层

**T5.1 agent 部署（1.5d）**——认证对齐 `coord/tests/agent_auth_process_test.rs`；
kill pattern 拆 `:kill-server / :kill-agent / :kill-both`。

**T5.2 经 agent 冒烟 + 分析（1.5d）**——T5.1 完成立即 2h `--via-agent`
冒烟，当天评审；agent 缓存 stale 即 P0。**起跑闸口：T1.4 结论已评审
（§2 硬前置）**。

**T5.3 Lock（1.5d）**——op 时间戳为控制机单一时钟（无跨机偏移）；服务器
时钟域差异以重叠容差吸收（参数 §5.4-①）；fencing/活性/pause 特化同 v2。

**T5.4 Election（1d）** / **T5.5 Registry（0.5d）** / **T5.7 RBAC（0.5d）**
——同 v2。
**T5.6 M5 收口（24h）**。

### M6 — 收口

**T6.0 高压长跑（0.5d + 12h）**——F2：mixture @ 200 ops/s，quiet 600s /
disrupt 300s。
**T6.1 72h soak-full（0.5d + 72h）**——比例：map 40 / txn 20 / watch 15 /
lease 10 / lock 10 / election 3 / registry 2（%）；`--via-agent`；
quiet 1800±20% / disrupt 600±20%。

> **入口已落地**（第五轮）：`--workload soakfull` + `--soak-mix`（面集合与权重）+ 组合
> checker（`mixck` 的 `:surfaces` 入口：每面仍由它自己的 checker 判 + 路由门槛）。
> **未实现的面在构造期硬失败**（`soakfull-mix`）：`lock` / `election` / `registry` 需 M5
> 的 agent 插件面，现阶段 `--soak-mix` 里一出现就报错——一个「少跑 3 个面但全绿」的
> 72h soak 比不跑更糟。默认 mix = 已实现部分 `map=40,txn=20,scan=5,watch=15,lease=10`。
> 实跑 90s / kill（五面全跑到、`:unrouted 0`）：
> `evidence/20260917T144816Z-t6.1-soakfull-90s-kill/`。
> 启动：`make soakfull SOAK_TIME_LIMIT=7200`，或夜间门禁 `make nightly`
> （`checkers → matrix-m1 → matrix-m2 → soakfull + soak-wait + soak-results`）。
**T6.2 证据+文档（0.5d）** / **T6.3 缺陷账（0.5d）**——通过标准按 §5.2/§6。

---

## 5. 量化验收标准

### 5.1 短矩阵通过标准——通用 + 按 workload 细化

**通用**：≥2 个不同种子；knossos/专用 checker `valid? = true`；`:info`
比例 ≤ 30%（超出需人工评审）；`:fail` 仅限白名单（`cas-miss`、
`not-leader`、`compacted`、`permission-denied`、`lock-held`）；fixture
运行器全绿；变异校验通过（§5.3）。

| workload | 最小样本 | 专属零违反断言 |
|:--|:--|:--|
| map | history ≥ 5,000 ops 且 delete ≥ 10%（≥500 次）；实现上由 `--map-min-deletes` 守住 | per-key 线性；tombstone 后读旧值 = 0（`:stale-after-tombstone`）；delete 响应自洽；存在性与写索引一致 |
| txn | history ≥ 3,000 ops；`succeeded=true` 占比 ≥ 20%（验证见证池有效）；多 op txn ≥ 30% | 无副作用 / 原子可见违反 = 0；**两条分支都必须真跑到**（`:branch-taken :failure > 0`，否则该 run 的「无副作用」只测了一半） |
| idempotency | 重放尝试 ≥ 50 次 | 同 rid version 多跳 = 0；最终值一致 |
| scan | scan ≥ 90 / read-at ≥ 40（60s 量级）；值层观察 ≥ 400 | 结构断言（序/区间/limit/count/keys_only/count_only）= 0；历史读错答 = 0 |
| watch | 每 watcher 事件 ≥ 200；分区 resume ≥ 1 次 | 丢序/重复/伪造 = 0；静默丢流 = 0 |
| lease | grant ≥ 100；到期场景 ≥ 30 | 安全（TTL 内消失）= 0；活性（ttl+grace 未消失）= 0 |
| lock | acquire 成功 ≥ 200 | 重叠（>容差，§5.4-①）= 0；fencing 误删 = 0 |
| election | campaign 轮次 ≥ 50 | 同时双 leader = 0；幽灵 leader = 0 |
| registry | 注册/到期循环 ≥ 30 | 幽灵实例 = 0；TTL 消失超时 = 0 |
| membership | 变更轮次 ≥ 3（含 1 次叠加分区） | 变更期线性违反 = 0；终态 3 voters |
| region/PD | split ≥ 2 次 | split 中 per-key 线性违反 = 0 |

样本不足的 cell 视为**未执行**，不计绿也不计红，必须补跑。

### 5.2 长跑（8h/24h/72h）通过标准

| 指标 | 门槛 |
|:--|:--|
| 线性违反（stale/future/fabricated/txn 原子性/锁重叠/watch 丢序/lease 安全） | **0** |
| quiet-window 写可用率（样本 ≥100 的窗口） | ≥ 0.95（§5.4-③） |
| RTO P95 | ≤ 分档上界（§5.4-②） |
| 最终收敛 | 每 key/region finale 读 `:ok` 且值 == 最新已确认 |
| watch 活性 | 最后一个已确认写 120s 内被观察到（或收到明确 OVERFLOW/UNAVAILABLE） |
| lease 活性 | 停止续租的 key 在 ttl+grace 内消失率 100% |

**T6 三级结论**：**通过** = 全表满足且无未闭环 P0/P1；**有条件通过** =
指标全满足但有 P1 未修（须按 §6 双签接受，P0 不允许有条件）；**不通过**
= 任一指标违反或存在未闭环 P0。

### 5.3 checker 质量门槛

每 checker ≥ 2 个负控制 fixture 且运行器全绿；每里程碑一次变异校验（真实
绿历史人工注入一个该 checker 负责的缺陷，必须抓红）；误报案例必须沉淀为
新 fixture + 修正语义文档，不允许口头豁免。

### 5.5 判据补充（v3.2，来自实跑）

1. **抖动判据必须看离散度，不能只看区间**（F-11）。原判据"相邻 nemesis op
   间隔 ∈ [3,8]s"被**恒定 6.42s** 的节拍完全满足 —— 而那个恒定节拍正是
   `clojure.core/cycle` 预求值导致的（抖动根本没生效）。判据补充为：
   **周期数 ≥ 6 时，相邻间隔的「不同取值个数」必须 ≥ 3**。
   `scripts/nemesis-timeline.clj` 已内置该自检并在不满足时告警。
2. **checker 门槛参数一律用 `or` 解析，禁止用 destructuring 的 `:or`**（F-09）。
   `:or` 只在键**缺失**时生效；本仓的 checker 组合总是显式传入这些键
   （未给 CLI 选项时值为 `nil`），于是门槛会被解析成 `nil`。后果是
   「静默失效」或 `(>= x nil)` NPE 两档，都属于假绿/假红。
   `scripts/gates-fixtures-defaults/` 固定住这条：一个只能用**生产默认门槛**
   （min-sample 100 / ratio 0.95）判出 invalid 的 quiet 窗口，每次
   `make checkers` 都跑。
3. **lab 一律按离线运行**（F-10）：`make` 目标与证据采集器的 `lein` 调用默认加
   `-o`（`ONLINE=1` 覆盖）。时钟校验用控制机 NTP 式夹逼采样，**不要求**节点
   安装 chrony/ntpq；判定规则为「只在可证超过上界时 FAIL，精度不足时 WARN
   并打印不确定度」（高 RTT 链路上无法审计 100ms 时钟，当 FAIL 只会产出假红）。
4. **evidence 的 MANIFEST 必须带真实门槛值**（F-12）：`results.edn` 里内嵌
   knossos tagged literal，摘要器必须用 `:default` reader；否则 `summary.txt`
   会退化成 `summary unavailable`，而 MANIFEST 的门槛结论恒为 `unknown` ——
   看起来像"没结论"，实际是"摘要器没跑成"。**`unknown` 不得当作通过。**
5. **必须能回答「这个 workload 真的跑了吗」（F-13，新增 G6 门槛）**：
   按 `:f` 分组的**每类**客户端 op，完成数 ≥ `--min-op-sample`（默认 10）时
   `:ok` 率必须 ≥ `--min-op-ok-ratio`（默认 0.1）。理由是一条实测：
   `txn-req` 用了 `DynamicMessage$Builder` 上不存在的 `addAllField`，
   整个 Txn 面（caS / 存在性 compare / create-if-absent / 多 op 写集）的
   completion 全变成 `:info`，而 knossos 对一份没有可判 op 的历史
   **判 valid** —— 报告全绿，覆盖面为零。门槛上线后又立刻抓到两个同形缺陷
   （F-14 值解析、F-15 无缓存 revision）。定义上它只看 `:ok` 率，
   不看绝对量：重扰动下 20% `:ok` 依然是合法历史（有守门员 fixture）。
6. **写索引里的 `:info` 写必须算「可能已生效」**（F-17）：响应丢失的写
   可能已经生效，它的值被后续读/扫看到是**合法**的。只把 `:ok` 写喂进索引
   会直接产出假红 `:fabricated`；正确做法是喂 `:ok`+`:info`，由索引内部用
   `:confirmed` 区分「已生效」（强制序/陈旧读的依据）与「可能生效」
   （合法观察的依据）。
7. **nil 判据的探针只能用「确认的非 nil 写」**（写 T1.1 负控制时发现）：
   tombstone 也是确认写，如果把它算进「被强制夹在本读之前的写」，就会拿它
   自己的完成时间去比自己的 invoke ⇒ **必然假红**（已写入
   `windex/write-index` 的 `:confirmed-non-nil`，并由
   `expect-valid-clean` / `expect-valid-exists-after-tombstone` 两条 fixture
   固定）。
8. **历史读（`read-at`）不适用陈旧读/fabricated 判据**：它返回的就是「那个
   revision 当时的值」，用「是否已被后续写覆盖」去判必然假红（实测 60s run
   即红）。历史读只归「精确 revision 对应」断言管（F-15）。
9. **新接一个面的第一跑：先看计数，再看 `:valid?`**（F-19）。
   `:valid? true` 在「一条 op 都没成功」时也成立（knossos 对空历史判 valid），
   所以判据是：先看 **事件数 / `:by-op` 完成数**（是否 > 0、是否达到样本门槛），
   再看 `:valid?`。F-19 就是这条规则的反面教材：watch 的 oneof 字段被塞了普通
   map（构造期错误），69 个会话全是 `:info`、事件 0 —— 而这次没出假绿，是因为
   G6 的 op 级活性门槛 + watch 的样本门槛 + 顶层 combine 三张网同时报警。
   **推论**：任何新 workload 都必须自带「这个面真的跑了吗」的判据，且该判据要
   进 `:valid?`（不能只进报告）。
10. **op 级汇报字段的粒度必须与事实一致**（F-20）：一次 op 内部做了多次尝试
   （watch 会话的多次重开）时，op 级字段只能是**整次 op 的不变量**（这里是
   **初始**起点），每次尝试的量必须放数组（`:sessions`）。把「当前尝试」的变量
   当成 op 级字段汇报，会造出**看起来像被测系统违约**的假红（实测 6 条
   `:watch-event-before-start`，且带具体 revision 与 UNAVAILABLE 记录，比假绿
   更难自查）。判据：凡「一个 op 内部有循环/重试」的 workload，先问一句
   「completion 里的每个字段，是整个 op 的、还是最后一次尝试的？」

### 5.4 待 coord 团队确认参数表

规则：**默认值可用于开发迭代；所有验收级 run（进 evidence 的）必须使用经
coord 团队书面确认的取值**（issue/邮件存档，链接进 MANIFEST）。未确认即
跑出的证据只能作为内部参考，不得用于引入评审。

| # | 参数 | 计划默认值 | 影响面 | 确认内容 |
|:--|:--|:--|:--|:--|
| ① | 锁重叠时钟容差 | 500ms | T5.3 假红/漏红边界 | lease 计时精度与服务器时钟域误差 |
| ② | RTO 分档 | kill/pause 120s · partition 120s · membership 300s · netem/disk 600s | T0.2/§5.2 | 选举超时、快照安装耗时的设计上限 |
| ③ | quiet 可用率门槛 + 最小样本 | 0.95 / 100 ops | T0.2/§5.2 | 引入方 SLO 期望（引入方团队确认） |
| ④ | PD split 短跑规模 | keys=32 × 3 轮 × 5min | T4.1 | split 阈值在短跑内可达（阈值下限） |
| ⑤ | lease grace | 2×ttl | T2.2 活性判定 | 到期清理节拍（check_expired 周期） |
| ⑥ | watch 语义 | **overflow-marker**（三态：coalescing / lossless / overflow-marker，默认第三态） | T2.1 checker 模式 | 以源码为准，双方理解一致。**已判**（F-06）：实现是「缓冲区满丢最旧 + 合成 `BufferOverflow` + 订阅者按 revision 去重」，所以 coalescing 与 lossless **都不是**正确模型 |
| ⑦ | version 起始值/不存在表示 | 源码为准 | T1.1 存在性 compare | 同上 |

> **⑦ 已判**（F-18）：新建 Key `version = 1`、删（`mark_deleted`）也 +1、
> 软删除后的 Put 继续递增（**version ≠ 写次数**）；软删除被所有读路径与
> compare 过滤 ⇒ 「不存在」= version 0 ⇒ `Compare{VERSION, EQUAL, 0}`
> 就是精确的存在性判定。
| ⑧ | 高压长跑速率 | 200 ops/s | T6.0 | lab 节点承载力（不压垮即失真） |

---

## 6. 缺陷分级与 SLA

| 级 | 定义 | 处理规则 |
|:--|:--|:--|
| P0 | 数据丢失/错误、线性违反、锁双持、越权放大、写错 raft 组 | 阻塞 T6；48h 内出最小化复现；修复或按下方双签流程豁免前不得收口 |
| P1 | 可用性/RTO 不达标、行为未定义但无数据错误、agent 缓存 stale 但有界可绕过 | 不阻塞里程碑推进；T6 前必须修复或书面接受 |
| P2 | 报错不友好、指标缺失、文档不符 | 记录即可，不阻塞 |

立项格式：复现命令（含种子）、history 片段、最小化用例（
`scripts/shrink-history.clj` delta-debugging，M1 顺带完成）、影响面、分级、
阻塞关系。

**P0 豁免双签流程**（P0 不允许单方放弃）：
1. 主张豁免方提交书面材料：缺陷描述、影响面、不可修复/不宜立即修复的理由、
   缓解措施；
2. **双签人**：coord 技术负责人（角色）+ 引入方团队负责人（角色），双方
   书面签署（issue/邮件存档，链接进缺陷条目）；缺一签 = 豁免不成立，T6
   不通过；
3. 登记：写入 `docs/production/remaining-known-gaps.md`，含签署日期、
   **有效期（≤90 天或下一 release，先到为准）**、回补条件；
4. 超期未回补的豁免自动升级为阻塞项，暂停一切依赖该能力的上线动作。

---

## 7. 排期与资源

### 7.1 排期表（单人串行 8 周；双人并行约 6.5 周）

> **实际对齐**（2026-09-16，第三轮）：W1（M0）与 W2/W3 的 M1 数据面部分
> （T1.1/T1.2/T1.3/T1.4）**已提前完成**，且比原计划多花了约 2 人日用于
> 「真跑暴露的测试自身缺陷」（F-09…F-17，11 条）—— 这笔开销在 W3/W4/W5 的
> 缓冲里可吸收。下一动作是 T1.5（M1 收口：`mixture` + 2h soak），随后进入
> W3 后半段的 M2（T2.0 流式接线 → T2.1 watch）。

| 周 | 内容 | 机器时间（夜间/后台） |
|:--|:--|:--|
| W1 | M0 全部（4d）→ M1 T1.1 起 | — |
| W2 | M1 T1.1–T1.3（余 2d）→ T1.2（2.5d） | — |
| W3 | M1 T1.4–T1.5（1d）→ M2 T2.0–T2.1（3d）· **缓冲 1d** | 2h map soak |
| W4 | M2 T2.2–T2.3（2d）→ M3 T3.1–T3.2（3d）· **缓冲 1d** | 2h mixture soak |
| W5 | M3 T3.3–T3.6（1.5d）→ M4 全部（2.5d）→ M5 T5.1 起 · **缓冲 1d** | 8h soak |
| W6 | M5 T5.1–T5.2（含 2h 冒烟+评审，闸口：T1.4 已闭环）→ T5.3–T5.5/T5.7 | 2h agent 冒烟 |
| W7 | M5 T5.6（24h）→ T6.0（12h）→ **T6.1 72h 起跑**（周末前） | 24h + 12h + 72h（跨周） |
| W8 | 72h 完成 → T6.2 证据+文档（0.5d）→ T6.3 缺陷账（0.5d）→ **收口缓冲 2d**（争议段重跑/证据补采/评审材料） | 预留 30h |

双人方案：A 自 W3 走 M3→M4，B 走 M2→M5，W6 末汇合于 T5.6。72h 期间两人
均可推进 §8/§9 的文字工作，不占关键路径。

### 7.2 lab 预约与 CI 队列规则

- **vagrant lab（单锁资源）**：共享日历预约，粒度半天；优先级
  `T6 系列 > M5 冒烟 > M3/M5 长跑 > 日常短矩阵`；起跑前 `make soak-status`
  确认无在跑任务（Makefile 已有上传保护，沿用）；长跑（≥8h）一律夜间/周末
  起跑。
- **docker lab（无锁）**：短矩阵/fixture/开发冒烟随用随起；与 vagrant 长跑
  并行允许。双人并行期：A（M3/M4）以 vagrant 为主，B（M2/M5）开发期以
  docker 为主，需 vagrant 时按预约表错半日。
- **CI 队列**：CI 只跑 ≤5min 的冒烟矩阵（PR gate）；≥2h 的 run **禁止在
  CI runner 起跑**，一律 lab 手动触发并登记预约表；nightly 的既有 chaos
  套件与本计划长跑不共享窗口（nightly 优先，长跑顺延）。
  具体门禁（均已入 Makefile）：`make checkers`（64 个 checker fixture，
  ≥2min）→ **`make matrix-m1`**（T1.1/T1.2/T1.3/T1.4 × 2 nemesis，45s/组合，
  约 12min —— 超出 5min，属「lab 手动触发」档）→ **`make matrix`**
  （register × 6 + cas-register × 6，45s/组合，约 25min，周一/发版前跑）。
- **控制机磁盘**：history.edn 全量保留至 T6 收口后 30 天；日志压缩归档；
  磁盘占用 >80% 时告警（T0.1 脚本顺带）。

---

## 8. 延期项登记（角色责任 + 90 天跟踪）

| 缺口 | 回补触发条件 | 责任角色（非个人） | 预估 |
|:--|:--|:--|:--|
| B3 Seal/Unseal | 生产启用 encryption-at-rest 之前 | coord 验证负责人 | 1d |
| B7 滚动升级 | 首个生产升级窗口之前 | coord 发布负责人 | 1.5d |
| E2 TLS 矩阵 | 跨机/跨环境暴露 gRPC 之前 | coord 安全负责人 | 1.5d |

跟踪机制：三项作为 tracked 条目写入 `docs/production/remaining-known-gaps.md`；
**引入后 90 天内每 30 天评审一次状态**（评审会或异步 issue 打卡），触发
条件先到先补；90 天届满未触发的条目转年度复审，不得无声删除。

---

## 9. 与 coord 侧的接口（倒逼清单）

1. `request_id` 去重语义：是否实现？窗口多大？（T1.4 判决，**T5.2 硬前置**）
   —— **已答 + 已复现**（F-01/F-02/F-03）：已实现，但限「单节点 + 60s + 4096 FIFO」；
   `Delete` 无去重（47/47 分组复现），范围删重放会删掉首次执行之后写入的数据
   （39/39 复现），`Put` 命中丢 `prev_kv`（26/26 复现）。同 leader/60s 窗口内
   去重确实生效（32 个分组 `revision` 完全一致）。
2. 磁盘写满时的行为定义（T3.5）—— **已答**（F-07）：<5% ReadOnly，写
   `RESOURCE_EXHAUSTED`、读可用。
3. `snapshot_logs_since_last = 0` 语义与文档（T3.2）—— 待 T3.2 前置源码确认
4. agent 读缓存在分区/重连窗口的一致性保证（T5.2，预期最重要发现）
5. Lease 计时基准：wall clock 还是 raft 逻辑时钟？clock bump 下行为（T2.2）
   —— **已答**（F-08）：单调时钟；墙钟跳变无效，`pause`/重启才是有效故障
6. Lock Release 的 fencing 校验（lease_id 不匹配是否拒绝）（T5.3）
7. 跨 Region Txn/Range 拒绝的错误码与无副作用保证（T4.2）
8. Watch 事件是否允许合并/压缩（T2.1 前置，决定 checker 语义）
   —— **已答**（F-06）：丢最旧 + 显式 `BufferOverflow`，两者皆非
9. key 的 version 起始值与"不存在"的表示（T1.1 前置，决定存在性 compare）
   —— **已答**（F-18）：新建为 1、软删除后 `deleted=true` 被全路径过滤 ⇒
   「不存在」= version 0，`Compare{VERSION, EQUAL, 0}` 即精确存在性判定。