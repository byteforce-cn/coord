# gates checker fixtures (T0.2)

Negative controls for `jepsen.coord.gates`. Run them with the unified runner
(T0.3), overriding the production thresholds down to fixture scale:

```bash
LEIN_ROOT=true lein run -m clojure.main scripts/run-checker-tests.clj \
    jepsen.coord.gates scripts/gates-fixtures \
    '{:quiet-min-sample 10 :nemesis :kill}'
```

These fixtures pin the **semantics** of the three gates — not the production
thresholds. The production defaults (0.95 availability, ≥100 samples, the
per-nemesis RTO table) live in `src/jepsen/coord/gates.clj` and are the values
`s5.4-②③` require the coord team to confirm in writing; the fixtures use
`:quiet-min-sample 10` so each window can be expressed with a handful of ops.

| fixture | what it pins |
|:--|:--|
| `expect-invalid-quiet-unavailable.edn` | a quiet window whose write `:ok` ratio is below the threshold is `:valid? false` |
| `expect-invalid-rto-timeout.edn` | a disruption whose first post-stop `:ok` write arrives after the RTO budget is `:valid? false` |
| `expect-invalid-duplicate-value.edn` | two write invocations with the same value break the checker's premise (`:valid? false`) |
| `expect-valid-small-sample.edn` | a window with fewer than `:quiet-min-sample` samples is **recorded but not judged** — a low ratio there must not produce a false red |

Times are the same clock the real histories use: epoch **milliseconds** on the
control node. Bulk writes are represented completion-only (`:type :ok` /
`:info`, no matching `:invoke`) except where the premise check needs the
invokes — `gates/checker` deliberately keys availability/RTO off completions
and the uniqueness premise off invokes, so both shapes are legal.
