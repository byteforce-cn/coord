# §5.4 参数确认单（验收级 run 的前置条件）

> 规则来源：`jepsen/docs/dev.md` §5.4 ——「默认值可用于开发迭代；**所有验收级
> run（进 evidence 的）必须使用经 coord 团队书面确认的取值**（issue/邮件存档，
> 链接进 MANIFEST）。未确认即跑出的证据只能作为内部参考，**不得用于引入评审**。」

## 当前状态

**未签回。** 因此 `docs/production/evidence/` 下的 24 份带该字段的 Jepsen 归档
（含本轮新增的 lease / soakfull / watch 三份）一律是**内部参考等级**，
MANIFEST 里 `§5.4 参数确认记录链接` 全部为「待填」；另 2 份（2026-09-12 的
java-it / round3-workspace-tests）由另一套采集器生成，**没有该字段**，需要时
按同一规则补签。

用 `jepsen/scripts/backfill-param-confirmation.sh --check` 可随时查看台账，
当前输出 `共 26 份归档：待填 24 / 已回填 0 / 无该字段 2`。

本文件的作用就是把「缺哪一步」写死，避免它被当成一句口号：

| # | 参数 | 计划默认值 | 影响面 | 需要谁确认什么 |
|:--|:--|:--|:--|:--|
| ① | 锁重叠时钟容差 | 500ms | T5.3 假红/漏红边界 | lease 计时精度与服务器时钟域误差 |
| ② | RTO 分档 | kill/pause 120s · partition 120s · membership 300s · netem/disk 600s | T0.2 / §5.2 | 选举超时、快照安装耗时的设计上限 |
| ③ | quiet 可用率门槛 + 最小样本 | 0.95 / 100 ops | T0.2 / §5.2 | 引入方 SLO 期望（**引入方团队**确认） |
| ④ | PD split 短跑规模 | keys=32 × 3 轮 × 5min | T4.1 | split 阈值在短跑内可达（阈值下限） |
| ⑤ | lease grace | 2×ttl | T2.2 活性判定 | 到期清理节拍（`check_expired` 周期） |
| ⑥ | watch 语义 | `overflow-marker`（三态：coalescing / lossless / overflow-marker） | T2.1 checker 模式 | 以源码为准，**双方理解一致**。已判（F-06）：实现是「缓冲区满丢最旧 + 合成 `BufferOverflow` + 订阅者按 revision 去重」 |
| ⑦ | version 起始值 / 不存在表示 | 源码为准 | T1.1 存在性 compare | 同上。已判（F-18）：新建 `version=1`、删除也 +1、软删除被全读路径过滤 ⇒ 「不存在」= version 0 |
| ⑧ | 高压长跑速率 | 200 ops/s | T6.0 | lab 节点承载力（不压垮即失真） |

⑥⑦ 已在源码侧判定（F-06 / F-18），本条仍需要的是「双方对结论的书面确认」，
而不是新的分析。

## 签回方式

1. coord 技术负责人在 issue/邮件里回复上表（逐条「确认」或给出替代取值）；
2. 把该存档的链接写进下表；
3. 执行回填（脚本会更新每份 MANIFEST 并**重算 `sha256sums.txt`**）：

```bash
jepsen/scripts/backfill-param-confirmation.sh <存档链接>
jepsen/scripts/backfill-param-confirmation.sh --check   # 只查看还有几份待填
```

| 字段 | 值 |
|:--|:--|
| 确认存档链接 | _(待填)_ |
| 确认人（coord 技术负责人） | _(待填)_ |
| 确认人（引入方团队负责人） | _(待填)_ |
| 确认日期 | _(待填)_ |
| 回填范围 | `docs/production/evidence/*/MANIFEST.md` |

> 回填只改 MANIFEST 的这一行并重算校验和，**不改动任何 run 产物**
> （`run.log` / `results.edn` / `history.*` 均保持原样、原校验值）。
> 回填完成后这些归档才从「内部参考」升为「可用于引入评审」。

## 尚未覆盖的部分（签回也不等于验收通过）

参数确认只解决「证据效力」，不解决「覆盖面」。以下三条独立存在，
详见 `jepsen/docs/soak-closure-report.md` §0：

1. M2 收口（T2.3）与 M3–M6 未执行；**T6.1 的 72h 全比例 soak 因 lock /
   election / registry 属 M5 未实现，当前构造期即硬失败**；
2. F-05（登录限流）未闭环，是「短矩阵干净绿」的阻塞点；
3. 本轮 26 份归档的 run 全部发生在提交之前 ⇒ MANIFEST 的「工作树」字段为
   `DIRTY`；后续 run 应先提交再起跑。
