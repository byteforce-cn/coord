# Jepsen evidence — t2.1-watch-120s-kill

| 字段 | 值 |
|:--|:--|
| 场景 | `t2.1-watch-120s-kill` |
| UTC 时间戳 | `20260917T153058Z` |
| run 开始 / 结束 | 2026-09-17 11:29:46,953 / unknown |
| coord commit | `3a22f44bb71208408f2453e7ea61267a824ded9a` (v0.1.0-3-g3a22f44, 2026-09-17T15:30:54+00:00) |
| 工作树 | **DIRTY** |
| coord-proto 哈希（16） | `9be93f0024d639c1` |
| coord 配置生成器哈希（16） | `c2c69027ffbcb2a0` (jepsen/coord/db.clj — 节点 TOML 由它生成) |
| Jepsen 版本 | 0.3.14-SNAPSHOT |
| Clojure 版本 | unknown |
| JVM | 25.0.2 |
| 节点 | unknown |
| lab 镜像 | jepsen-control jepsen-node jepsen-setup  |
| 随机种子 | **960196454** |
| 命令行（jepsen.log 记录） | `lein run test --nodes-file /root/nodes --username root --ssh-private-key /root/.ssh/id_ed25519 --workload watch --nemesis kill --time-limit 120 --concurrency 1n --rate 5` |
| 选项 | `--nodes-file --username --ssh-private-key --workload --nemesis --time-limit --concurrency --rate ` |
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

## 回放

```bash
# 用记录下来的种子重建 jittered nemesis 排期（应与本次 run 一致）
lein run -m clojure.main scripts/replay.clj /data/jepsen/coord/jepsen/store/coord/2026-09-17T11:29:46.926064013Z --seed 960196454
# 重跑 checker（不需要集群）
lein run -m clojure.main scripts/validate-soak-checker.clj /data/jepsen/coord/jepsen/store/coord/2026-09-17T11:29:46.926064013Z
```
