# Jepsen evidence — baseline-partition-ring-60s

| 字段 | 值 |
|:--|:--|
| 场景 | `baseline-partition-ring-60s` |
| UTC 时间戳 | `20260916T122844Z` |
| run 开始 / 结束 | 2026-09-06 03:06:01,040 / 2026-09-06 03:08:40,454 |
| coord commit | `ad22337b09f7aae407b39e9d639de867e52bccc7` (v0.1.0, 2026-09-13T13:31:39+00:00) |
| 工作树 | **DIRTY** |
| coord-proto 哈希（16） | `ac4be7073b7b5b12` |
| jepsen/files config 哈希（16） | `none` |
| Jepsen 版本 | 0.3.14-SNAPSHOT |
| Clojure 版本 | unknown |
| JVM | 25.0.2 |
| 节点 | unknown |
| lab 镜像 | image: ${JEPSEN_REGISTRY:-ghcr.io/nurturenature/jepsen-docker/}jepsen-control image: ${JEPSEN_REGISTRY:-ghcr.io/nurturenature/jepsen-docker/}jepsen-node image: ${JEPSEN_REGISTRY:-ghcr.io/nurturenature/jepsen-docker/}jepsen-setup  |
| 随机种子 | **(not recorded — pre-T0.5 run)** |
| 命令行（jepsen.log 记录） | `lein run test --nodes-file /root/nodes --username root --ssh-private-key /root/.ssh/id_ed25519 --workload multi-register --nemesis partition-ring --time-limit 60 --concurrency 1n --regions 3` |
| 选项 | `--nodes-file --username --ssh-private-key --workload --nemesis --time-limit --concurrency --regions ` |
| history.edn | history.edn（未压缩） |
| §5.4 参数确认记录链接 | https://github.com/byteforce-cn/coord/blob/soak-params-2026-09-18/docs/production/evidence/PARAM-CONFIRMATION.md |

## 门槛结论

| 字段 | 值 |
|:--|:--|
| overall :valid? | true |
| gates :valid? (T0.2) | - |

完整门槛摘要见 `summary.txt`（rto-p95 / quiet-judged / premise-valid 等）。

> 本 MANIFEST 只证明"产物可追溯、可回放"，**不**代替引入评审结论。
> 未使用 §5.4 书面确认参数取值的 run 只能作内部参考，不得用于引入决策。

## 回放

```bash
# 用记录下来的种子重建 jittered nemesis 排期（应与本次 run 一致）
lein run -m clojure.main scripts/replay.clj store/coord/latest --seed (not recorded — pre-T0.5 run)
# 重跑 checker（不需要集群）
lein run -m clojure.main scripts/validate-soak-checker.clj store/coord/latest
```
