# T1.2（txn 全形态）checker fixture 套件

```
LEIN_ROOT=true lein -o run -m clojure.main scripts/run-checker-tests.clj \
  jepsen.coord.txnck scripts/txn-fixtures
```

checker 无参数（`jepsen.coord.txnck/checker`）。

| fixture | 断言（dev.md §5.1 / §6 分级） |
|:--|:--|
| `expect-valid-clean` | 四种形态全绿（create / 多 key 写集 / cas-delete 失败分支 / txn 内读） |
| `expect-invalid-failure-branch-leak` | P0：失败 txn 的**成功分支**写值被读到 |
| `expect-invalid-txn-lost-write` | P0：`succeeded=true` 而写集全不可见 |
| `expect-invalid-txn-partial-visibility` | P0：写集只被看到**非空真子集**（半应用可见） |
| `expect-invalid-create-absent-failed` | 全新 key 上 `Compare{VERSION, EQUAL, 0}` 不成立 |
| `expect-invalid-failure-branch-not-executed` | `succeeded=false` 但失败分支（delete）没执行 |
| `expect-invalid-stale-vs-txn-write` | 成功 txn 的写被强制排在读之前，读却返回旧值 |
| `expect-valid-info-txn-may-apply` | **健全性**：`:info` txn 的写值可被观察到（不判 fabricated） |

两条 `expect-valid-*` 是**假红守门员**：它们必须一直绿，否则判据过严。
