# Jepsen evidence — w1.2-f05-kill-all-60s

| 字段 | 值 |
|:--|:--|
| 场景 | `w1.2-f05-kill-all-60s` |
| UTC 时间戳 | `20260926T045524Z` |
| run 开始 / 结束 | 2026-09-26 04:53:27,192 / unknown |
| coord commit | `bb35fac0dd628b04ca10f300a95afd6710891b8b` (soak-params-2026-09-18-46-gbb35fac, 2026-09-26T04:36:49Z) |
| 工作树 | **clean** |
| coord-proto 哈希（16） | `42c0aa2ac00601cf` |
| coord 配置生成器哈希（16） | `2689c032193355da` (jepsen/coord/db.clj — 节点 TOML 由它生成) |
| Jepsen 版本 | 0.3.14-SNAPSHOT |
| Clojure 版本 | unknown |
| JVM | 25.0.4.1 |
| 节点 | n1 n2 n3 n4 n5  |
| lab 镜像 | jepsen-control jepsen-node jepsen-setup  |
| 随机种子 | **2056544631** |
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

## 回放

```bash
# 用记录下来的种子重建 jittered nemesis 排期（应与本次 run 一致）
lein run -m clojure.main scripts/replay.clj store/coord/2026-09-26T04:53:27.169864342Z --seed 2056544631
# 重跑 checker（不需要集群）
lein run -m clojure.main scripts/validate-soak-checker.clj store/coord/2026-09-26T04:53:27.169864342Z
```
