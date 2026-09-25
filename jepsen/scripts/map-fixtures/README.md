# T1.1（map/delete）checker fixture 套件

运行（T0.3 统一运行器）：

```
make checkers      # 或：
LEIN_ROOT=true lein -o run -m clojure.main scripts/run-checker-tests.clj \
  jepsen.coord.mapck scripts/map-fixtures '{:mode :index}'
```

`{:mode :index}` 让 fixture 走 **O(n log n) 写索引** 路径（长跑用的那条）；
短矩阵的 knossos 路径（`delete` 建模为 `write nil`）在 lab 实跑里覆盖，
它的语义在这里由 `expect-invalid-stale-after-tombstone` /
`expect-invalid-nil-after-write` 两条历史以同一模型固定住。

| fixture | 断言 |
|:--|:--|
| `expect-valid-clean` | 写→读→删→读 nil→exists=false 全绿（含「删后读 nil 合法」） |
| `expect-invalid-stale-after-tombstone` | §5.1「tombstone 后读旧值 = 0」 |
| `expect-invalid-nil-after-write` | 有确认写、无 tombstone，却读到 nil |
| `expect-invalid-fabricated` | 读到从来没人写过的值 |
| `expect-invalid-future` | 读完成早于其值的写的 invoke |
| `expect-valid-nil-with-pending-delete` | **健全性**：未完成的 tombstone 让 nil 合法（防假红） |
| `expect-valid-delete-fail-not-shape-violation` | **失败 ≠ 违反**：`:fail` 的 delete（无响应字段）不得被判成 DeleteResponse 形状违反（F-69 浸泡中 256 条 `:fail` 曾被误判；配对的 `expect-invalid-delete-response-inconsistent` 必须仍红） |
| `expect-invalid-delete-prev-kv-fabricated` | delete 汇报的 prev_kv 不是真实值 |
| `expect-invalid-delete-response-inconsistent` | `deleted` ∉ {0,1} / 与 `prev_kvs` 条数不符 |
| `expect-valid-delete-prev-kv-ok` | prev_kv 是当时的真实值 → 合法 |
| `expect-invalid-exists-fabricated` | `exists?=true` 但从没有非 nil 写被 invoke |
| `expect-invalid-exists-stale` | `exists?=false` 却已有确认写（nil 判据的镜像） |
| `expect-valid-exists-after-tombstone` | tombstone 后 `exists?=false` 正确 |

样本门槛（`--map-min-deletes`，§5.1 的 delete ≥ 10%）另有独立 fixture 目录
`scripts/map-fixtures-min-deletes/`，用 `{:mode :index :min-deletes 5}` 运行。
