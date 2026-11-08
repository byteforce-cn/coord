# gates checker —— v2 (kvv2) 形状的负控制

`jepsen.coord.gates` 的三条判据（premise / quiet 可用率 / RTO）过去只认 v1 的
op 名（`:write` / `:read`），于是 kvv2 run 上全部空转，报告仍然 `:valid? true`
（#308）。这组 fixture 用 **v2 op 形状** 把新行为钉住：

- `expect-valid-v2-gates.edn`：v2 写/删/读/TS + kill 扰动，写完成跨 quiet 窗口
  ⇒ availability `judged > 0`、RTO `n > 0`、liveness 判到 4 类 op。把
  `write-ops` 改回只认 `:write`，这条会变成「没判」并被 `:require-samples?`
  判红 —— 这就是负控制；
- `expect-invalid-v2-availability-not-judged.edn`：有写完成，但每个 quiet
  窗口的样本都低于 `:quiet-min-sample` ⇒ `:quiet-availability-not-judged`；
- `expect-invalid-v2-rto-not-measured.edn`：有扰动，但 stop 落在写阶段之后
  （`measurable?` 全 false）⇒ `:rto-not-measured`；
- `expect-valid-v2-safe-ts-lag.edn`：窗口内 30 ok + 2 个 `:info :safe-ts-lag`
  ⇒ 可用率 100%（背压类 info 从分母剔除并进 `:excluded-by-error`，设计 §5.4）；
- `expect-invalid-v2-unavailable-counted.edn`：同形状但换成 `:info :unavailable`
  ⇒ 93.75% < 0.95 ⇒ `:quiet-availability`（真不可用**不能**被豁免）。

跑法（`make checkers` 已包含）：

```bash
LEIN_ROOT=true lein run -m clojure.main scripts/run-checker-tests.clj \
    jepsen.coord.gates scripts/gates-v2-fixtures \
    '{:quiet-min-sample 2 :nemesis :kill :min-op-sample 2
      :check-premise? false :require-samples? true}'
```
