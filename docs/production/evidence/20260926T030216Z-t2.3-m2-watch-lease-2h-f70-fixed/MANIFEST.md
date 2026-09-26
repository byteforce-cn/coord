# Jepsen evidence — t2.3-m2-watch-lease-2h-f70-fixed

| 字段 | 值 |
|:--|:--|
| 场景 | `t2.3-m2-watch-lease-2h-f70-fixed` |
| UTC 时间戳 | `20260926T030216Z` |
| run 开始 / 结束 | 2026-09-26 00:55:38,029 / unknown |
| coord commit | `109b462b94467d9be28d3c23656395ca9b70bad9` (soak-params-2026-09-18-42-g109b462, 2026-09-26T00:46:41Z) |
| 工作树 | **clean** |
| coord-proto 哈希（16） | `42c0aa2ac00601cf` |
| coord 配置生成器哈希（16） | `2689c032193355da` (jepsen/coord/db.clj — 节点 TOML 由它生成) |
| Jepsen 版本 | 0.3.14-SNAPSHOT |
| Clojure 版本 | unknown |
| JVM | 25.0.4.1 |
| 节点 | n1 n2 n3 n4 n5  |
| lab 镜像 | jepsen-control jepsen-node jepsen-setup  |
| 随机种子 | **42** |
| 命令行（jepsen.log 记录） | `lein run test --nodes-file /root/nodes --username root --ssh-private-key /root/.ssh/id_ed25519 --workload soakfull --nemesis soak --checker soak --rate 2 --soak-quiet 1800 --soak-disrupt 600 --seed 42 --watch-min-events 200 --lease-min-grants 100 --lease-min-expiries 30 --soak-mix map=20,watch=40,lease=40 --time-limit 7200 --concurrency 2n` |
| 选项 | `--nodes-file --username --ssh-private-key --workload --nemesis --checker --rate --soak-quiet --soak-disrupt --seed --watch-min-events --lease-min-grants --lease-min-expiries --soak-mix --time-limit --concurrency ` |
| history.edn | history.edn.gz — yes (source 15015684 bytes) |
| §5.4 参数确认记录链接 | https://github.com/byteforce-cn/coord/blob/soak-params-2026-09-18/docs/production/evidence/PARAM-CONFIRMATION.md |

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
lein run -m clojure.main scripts/replay.clj store/coord/latest --seed 42
# 重跑 checker（不需要集群）
lein run -m clojure.main scripts/validate-soak-checker.clj store/coord/latest
```
