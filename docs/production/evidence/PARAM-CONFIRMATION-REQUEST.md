# §5.4 参数确认请求（issue / 邮件正文）

> 用途：把下面「正文」部分**原样复制**到 issue 或邮件里发给确认人；对方的回复（或
> 在该 issue 里的逐条答复）就是 `dev.md` §5.4 要求的**书面确认存档**。
> 拿到存档链接后执行 `jepsen/scripts/backfill-param-confirmation.sh <存档链接>` 回填
> 24 份 MANIFEST（脚本会同时重算每份 `sha256sums.txt`，不改任何 run 产物）。
>
> 规则出处：`jepsen/docs/dev.md` §5.4 ——「默认值可用于开发迭代；**所有验收级 run
> （进 evidence 的）必须使用经 coord 团队书面确认的取值**（issue/邮件存档，链接进
> MANIFEST）。未确认即跑出的证据只能作为内部参考，**不得用于引入评审**。」
>
> 台账（随时可查）：`jepsen/scripts/backfill-param-confirmation.sh --check`
> ⇒ **发信时**（2026-09-18）`共 26 份归档：待填 24 / 已回填 0 / 无该字段 2`；
> 同日签回后已变为 `待填 0 / 已回填 24 / 无该字段 2`（见 `PARAM-CONFIRMATION.md`）。

---

## 正文（复制以下内容）

**标题**：coord soak 验收参数确认请求（jepsen 验收级 run 前置条件，dev.md §5.4）

各位好，

按 `jepsen/docs/dev.md` §5.4 的规定，凡是要进 `docs/production/evidence/` 的
**验收级 run**，其参数取值必须经过**书面确认**并把存档链接填进 MANIFEST；未确认
之前这些证据只能作内部参考、不得用于引入评审。

目前 24 份带该字段的归档全部为「待填」，因此需要请各位对下表**逐条给出书面答复
（「确认」或给出替代取值）**。表中 ⑥⑦ 两项已在 coord 源码侧判定完毕，本条需要的
只是「双方对结论理解一致」的书面认可，不需要新的分析。

| # | 参数 | 现用（计划）默认值 | 影响面 | 需要确认的技术点 |
|:--|:--|:--|:--|:--|
| ① | 锁重叠时钟容差 | 500ms | T5.3 假红/漏红边界 | lease 计时精度与服务器时钟域误差 |
| ② | RTO 分档 | kill/pause 120s · partition 120s · membership 300s · netem/disk 600s | T0.2 / §5.2 | 选举超时、快照安装耗时的设计上限 |
| ③ | quiet 可用率门槛 + 最小样本 | 0.95 / 100 ops | T0.2 / §5.2 | 引入方 SLO 期望（**引入方团队**确认） |
| ④ | PD split 短跑规模 | keys=32 × 3 轮 × 5min | T4.1 | split 阈值在短跑内可达（阈值下限） |
| ⑤ | lease grace | 2×ttl | T2.2 活性判定 | 到期清理节拍（`check_expired` 周期 200ms） |
| ⑥ | watch 语义 | `overflow-marker`（三态：coalescing / lossless / overflow-marker） | T2.1 checker 模式 | 双方理解一致：实现是「缓冲区满丢最旧 + 合成 `BufferOverflow` + 订阅者按 revision 去重」（源码侧已判，见 F-06） |
| ⑦ | version 起始值 / 「不存在」表示 | 源码为准 | T1.1 存在性 compare | 双方理解一致：新建 `version=1`、删除也 +1、软删除被全读路径过滤 ⇒ 「不存在」= version 0（源码侧已判，见 F-18） |
| ⑧ | 高压长跑速率 | 200 ops/s | T6.0 | lab 节点承载力（不压垮即失真） |

**回复方式**：在本 issue 下逐条回复「① 确认 / ② 确认 / …⑧ 用 100 ops/s」这样的
形式即可；邮件回复请保留本邮件正文以便归档。

**回复之后我们会做**：把本次答复的存档链接填进 24 份 MANIFEST（
`jepsen/scripts/backfill-param-confirmation.sh <存档链接>`，只改 MANIFEST 的那一行
并重算 `sha256sums.txt`，不动任何 run 产物），这些归档才从「内部参考」升为
「可用于引入评审」。

**另外请一并知悉（参数确认不等于验收通过）**：参数确认只解决「证据效力」，不解决
「覆盖面」。以下三条独立存在，详见 `jepsen/docs/soak-closure-report.md` §0：

1. M2 收口（T2.3）与 M3–M6 未执行；**T6.1 的 72h 全比例 soak 在当前代码上跑不起来**
   —— 声明比例含 lock 10 / election 3 / registry 2，这三个面属 M5（agent 插件面），
   `--soak-mix` 里一出现即构造期硬失败；
2. F-05（登录限流 × 「刚重启」窗口）未闭环；
3. 本轮 26 份归档的 run 全部发生在提交之前 ⇒ MANIFEST 的「工作树」字段为 `DIRTY`；
   后续正式 run 先提交再起跑。

---

## 附：确认人回填（收到答复后由测试方填写）

`docs/production/evidence/PARAM-CONFIRMATION.md` 的表格需要填写下列四项，本文件
只做提醒，实际值以收到的存档为准：

| 字段 | 值 |
|:--|:--|
| 确认存档链接 | （收到的 issue/邮件链接） |
| 确认人（coord 技术负责人） | （姓名 / 角色 / 日期） |
| 确认人（引入方团队负责人） | （姓名 / 角色 / 日期；若 ③ 由同一方确认需注明） |
| 确认日期 | （YYYY-MM-DD） |

回填命令与自查：

```bash
jepsen/scripts/backfill-param-confirmation.sh <存档链接>   # 写模式：回填 + 重算校验和
jepsen/scripts/backfill-param-confirmation.sh --check      # 只统计，期望输出「待填 0」
```
