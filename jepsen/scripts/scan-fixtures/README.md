# T1.3（scan / revision 读）checker fixture 套件

```
LEIN_ROOT=true lein -o run -m clojure.main scripts/run-checker-tests.clj \
  jepsen.coord.scanck scripts/scan-fixtures
```

checker 无参数（`jepsen.coord.scanck/checker`）。

| fixture | 断言 |
|:--|:--|
| `expect-valid-clean` | 区间/keys_only/count_only/完整 count/历史读 全绿 |
| `expect-invalid-scan-range-violation` | 返回区间外的 key |
| `expect-invalid-scan-order-violation` | 非字典序（乱序） |
| `expect-invalid-scan-limit-violation` | 返回条数超过 `limit` |
| `expect-invalid-scan-count-inconsistent` | 已扫完但 `count` ≠ 返回条数 |
| `expect-invalid-scan-keys-only-violation` | `keys_only=true` 却返回 value |
| `expect-invalid-scan-values-only-violation` | `count_only=true` 却返回 kvs |
| `expect-invalid-read-at-mismatch` | 历史 revision 读返回了更新的值（错答） |
| `expect-invalid-scan-stale-value` | 扫描返回被确认写覆盖的旧值 |
| `expect-valid-read-at-compacted` | 压缩区间读返回 `OUT_OF_RANGE` → 合法（假红守门员） |

「扫漏了 key」不做断言：并发写下不可判定（见 `scanck` 的漏检边界）。
