# Jepsen evidence — w1-2-f05-kill-all-60s

| 字段 | 值 |
|:--|:--|
| 场景 | `w1-2-f05-kill-all-60s` |
| UTC 时间戳 | `20260921T163732Z` |
| run 开始 / 结束 | 2026-09-21 16:35:32,241 / unknown |
| coord commit | `d8da8a5ab84d1bbf3085d7e112fed12330fb94da` (soak-params-2026-09-18-14-gd8da8a5, 2026-09-21T16:28:55Z) |
| 工作树 | **clean** |
| coord-proto 哈希（16） | `42c0aa2ac00601cf` |
| coord 配置生成器哈希（16） | `2689c032193355da` (jepsen/coord/db.clj — 节点 TOML 由它生成) |
| Jepsen 版本 | 0.3.14-SNAPSHOT |
| Clojure 版本 | unknown |
| JVM | 25.0.4.1 |
| 节点 | n1 n2 n3 n4 n5  |
| lab 镜像 | jepsen-control jepsen-node jepsen-setup  |
| 随机种子 | **992196396** |
| 命令行（jepsen.log 记录） | `lein run test --nodes-file /root/nodes --username root --ssh-private-key /root/.ssh/id_ed25519 --workload register --nemesis kill-all --time-limit 60 --concurrency 1n` |
| 选项 | `--nodes-file --username --ssh-private-key --workload --nemesis --time-limit --concurrency ` |
| history.edn | history.edn（未压缩） |
| §5.4 参数确认记录链接 | _(待填：issue/邮件存档链接)_ |

## 门槛结论

| 字段 | 值 |
|:--|:--|
| overall :valid? | true |
| gates :valid? (T0.2) | true |

完整门槛摘要见 `summary.txt`（rto-p95 / quiet-judged / premise-valid 等）。

> 本 MANIFEST 只证明"产物可追溯、可回放"，**不**代替引入评审结论。
> 未使用 §5.4 书面确认参数取值的 run 只能作内部参考，不得用于引入决策。

## 本轮备注（W1-2 / F-05 复跑；*人工补充，非采集器生成*）

**目的**：`jepsen/docs/coord-findings.md` §F-05 的复现路径 —— `NEMESIS=kill-all
TIME_LIMIT=60 CONCURRENCY=1n`，统计 `:fail :write [:no-client Failed to
authenticate to coord]` 出现率。

**调用**（`JEPSEN_PROVIDER=docker`；`SKIP_CHECKERS=1` 跳过 fixture 门禁）：

```bash
make -C jepsen/lab upload JEPSEN_PROVIDER=docker
make -C jepsen/lab test WORKLOAD=register NEMESIS=kill-all TIME_LIMIT=60 \
     CONCURRENCY=1n SKIP_CHECKERS=1 JEPSEN_PROVIDER=docker
```

binary sha256 前 8 位 `24cd089d`（宿主 `target/release/coord`，提交 `d8da8a5` 后重建）。

**结果**：

| 项 | 值 |
|:--|:--|
| 判决 | ✅ `:valid? true`、`:gates {:valid? true}`、make 退出码 **0** |
| `:fail` 计数 | **0**（`:failures []`） |
| **F-05 形态（`:no-client Failed to authenticate`）** | **0 次 —— 本 run 未复现** |
| nemesis 实际动作 | 5 轮 `kill-all`（每轮同时 kill n1/n2/n3） |
| linear | `:valid? true`（knossos `:wgl`，register 模型） |
| RTO | 5 个可测样本，p95 **2.31s** / max 2.53s，预算 120s，`unrecovered 0` |
| premise | `:valid? true`，`writes 71`，重复值 0 |

**这次的诚实边界（必须与结论一起引用）**：

1. **只有 1 次 run**（原复现路径要求 ×3 以算出现率）⇒ 只能得出"**本 run 未复现**"，
   **不能**得出"F-05 已消失"。
2. 形态不完全同源：原证据里出错的多是**经 agent** 的 run（M5b MQ，09-19）与
   `partition-ring`；本 run 是 `kill-all` + `workload=register` + 默认 `AGENTS=2`。
3. **可用率门槛未被判定**：`quiet-judged 0 / skipped-small-sample 5` —— 60s 窗口的
   每窗 op 数不足 100，**因此本 run 不构成对 §5.4-③ 的 0.95 可用率的任何证据**
   （该参数至今未签回）。
4. **工作树 `clean`** —— 这是 jepsen 归档里**第一份**非 DIRTY 的记录（跑在已提交的
   `d8da8a5` 上），消掉了 D-10 的复现折扣的一半。

**判据的最终形态**（W1-2 修正后，见计划书 §5 W1-2）：登录路径的两类失败必须可区分
（密码错 = `UNAUTHENTICATED`；无 quorum = `UNAVAILABLE`/`DEADLINE_EXCEEDED`），
已由 `coord-server` 的
`auth::service::cct_tests::session_persist_failure_propagates_retryable_code`
（含负控制）钉住；本 run 是它的**端到端对照**。

## 回放

```bash
# 用记录下来的种子重建 jittered nemesis 排期（应与本次 run 一致）
lein run -m clojure.main scripts/replay.clj store/coord/2026-09-21T16:35:32.215037076Z --seed 992196396
# 重跑 checker（不需要集群）
lein run -m clojure.main scripts/validate-soak-checker.clj store/coord/2026-09-21T16:35:32.215037076Z
```
