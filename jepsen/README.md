# coord Jepsen test

Jepsen tests for [coord](../README.md) — a strongly-consistent (linearizable)
KV store with gRPC KV + Txn (CAS) + Maintenance APIs.

> 本目录是 **coord 仓库的一部分**（`coord/jepsen/`）：Jepsen 测试源码与跑批脚本随
> coord 一起版本化，checkout 任意 coord commit 即得到与之配套的测试与脚本。coord
> release 可执行文件（`coord`，约 28MB）**不入库**——由部署方从当前 checkout 构建后
> 随上传部署到控制机（见下文 Run）。旧位置 `jepsen-custom/examples/coord-test/`
> 已于 2026-09-05 废弃迁移，请勿继续引用。

配套文档（都在 `jepsen/docs/`）：

* `dev.md` —— 开发计划（v3.5）：覆盖矩阵、依赖图、验收标准、缺陷 SLA。
* `PROGRESS.md` —— 逐步进度与证据（**每个任务收尾当天更新**）。
* `coord-findings.md` —— 倒逼 coord 的缺陷单：契约锚点 + 源码/产物证据 + 分级。
* `soak-closure-report.md` —— **soak 结项报告**：量化这套测试体系到底逼出了
  什么（coord 侧缺陷账 / 测试自身假绿自查 / 未兑现部分），引入评审的入口。
* `coverage-gaps-and-implementation-plan.md` —— 缺口基线与 checker 设计。
* `coord-agent-coverage-plan.md` —— **coord-agent（第二被测系统）覆盖方案（建议稿）**：
  差分运行、多 agent 拓扑、idgen/event、安全面（断连降级 / CCT 回退 / 网关拒绝）、
  待 coord-agent 团队书面确认的 10 条语义。

Implements the design recorded in the (not-committed) local design note
`docs/coord-agent-plugin-engine-plan-2026-09-09.md` and `docs/production/`; a
standalone `docs/coord.md` never existed — the earlier reference to it was a
dangling link (fourth review §3.15(6)).

* **DB** (`jepsen.coord.db`) — uploads the `coord` binary, writes a 3-node
  TOML config (auth enabled, shared `auth_root_key` / `raft_shared_secret`),
  starts each node via `start-stop-daemon` (pidfile-backed so nemesis can
  target single nodes), and tears everything down between tests.
* **Client** (`jepsen.coord.client`) — plaintext gRPC over HTTP/2 via
  `grpc-netty-shaded` + `DynamicMessage` (no protoc needed). Authenticates as
  root, attaches `authorization: Bearer <cct>`, and implements leader
  discovery: `UNAVAILABLE` → rotate to next node, `DEADLINE_EXCEEDED` →
  `:info` (may or may not have applied), CAS miss → `:fail`. T2.1 adds a
  **bidi streaming** watch session (see below): `CoordRpc.Watcher` reads
  responses on a daemon thread into a bounded queue so a session can observe
  stream termination and resume at `last-revision + 1`.
* **Proto layer** (`jepsen.coord.proto` + `src/java/jepsen/coord/CoordRpc.java`)
  — wire-compatible descriptors built programmatically (field numbers match
  the coord contract exactly), so no generated stubs are required.
* **Nemeses** — kill/restart (via the DB `Kill` protocol), SIGSTOP pause,
  iptables partitions (random node / random halves / majorities ring), a
  combined `:all`, and a slow-rotating `:soak` cycle (see below).
* **Checkers** — knossos `linearizable` over `model/register` or
  `model/cas-register` (WGL), plus `perf`; and a dedicated O(n log n) **soak
  checker** for long runs (`jepsen.coord.soak`). The T1.1–T1.5 data-plane
  workloads have their own checkers (`mapck` / `txnck` / `scanck`), the
  `mixture` workload composes all three (`mixck`), and T2.1's watch workload
  has `watchck` (four assertions + a sample gate).

## Run

测试源码与脚本在本目录（coord 仓库 `jepsen/`）维护；实际执行在 **仓库内 lab**
（`jepsen/lab/`，随 coord 版本化）上——clone coord 即可自建 lab。lab 提供两种
provider（`JEPSEN_PROVIDER=vagrant|docker`，默认 vagrant）：

* **vagrant**（VirtualBox/libvirt）：控制机 + n1..nN VM；全矩阵与 72h soak 首选。
* **docker**（compose，仅需 Docker）：`jepsen-control` 官方镜像已内置
  `lein install` 好的 `jepsen 0.3.14-SNAPSHOT`（与本目录 `project.clj` 依赖一致），
  日常开发/冒烟/CI 首选。

lab 会把**当前 coord checkout 的 `jepsen/`** 连同新构建的 coord release 二进制部署
到控制机 `/root/coord-test`（docker 走 bind-mount；staging 见 `lab/Makefile` 的
`stage` 目标）。详见 `jepsen/lab/README.md`。

在 lab 目录执行：

```bash
cd jepsen/lab
make up               # 首次：自动生成 keys/ + 开机并 provision
make setup            # one-time: lein install of the local jepsen lib
make upload           # 部署本目录(jepsen/) + 构建/复用 coord -> 控制机
make quick            # 20s sanity run (no nemesis)
make test             # NEMESIS=partition-halves TIME_LIMIT=60 (defaults)
make test NEMESIS=kill TIME_LIMIT=120
```

Docker 变体（同一套操作，前缀 `docker-` 或 `JEPSEN_PROVIDER=docker`）：

```bash
make docker-up docker-upload docker-quick docker-test
```

> `make upload` 部署前会先检查控制机上是否有 soak 在跑并拒绝覆盖；本地迁移不影响
> 已在控制机运行的 72h soak。

Manual equivalent（vagrant，源 = 本目录 + 同 checkout 构建的 release 二进制，
`jepsen/coord -> ../../target/release/coord`，已 git-ignore）：

```bash
# from the host, in the lab dir (jepsen/lab)
rm -rf /tmp/coord-upload/coord-test && mkdir -p /tmp/coord-upload/coord-test
cp -r ../. /tmp/coord-upload/coord-test/          # or rsync with --exclude store/
rm -f /tmp/coord-upload/coord-test/coord
cp ../../target/release/coord /tmp/coord-upload/coord-test/coord
cp /tmp/coord-upload/coord-test/scripts/coord-soak.sh \
   /tmp/coord-upload/coord-test/coord-soak.sh     # 顶层兼容入口（soak Makefile 依赖）
vagrant upload /tmp/coord-upload/coord-test /tmp/coord-test control
vagrant ssh control -- sudo mv /tmp/coord-test /root/coord-test
```

Then, on the control node as root:

```bash
cd /root/coord-test
lein deps          # first time: pull grpc/protobuf deps
lein run test --nodes-file /root/nodes --username root \
  --ssh-private-key /root/.ssh/id_ed25519 \
  --workload register --nemesis none --time-limit 60 --concurrency 1n
```

Workloads: `register`, `cas-register`, `idempotency` (T1.4 `request_id` 幂等
专项: same-rid replay groups + a dedicated checker — see
`docs/coord-findings.md` F-01/F-02/F-03), `multi-register`, `map` (T1.1),
`txn` (T1.2), `scan` (T1.3), `mixture` (T1.5: map+txn+scan interleaved,
`--mixture-ratio 4,2,2` by default), `watch` (T2.1: watch sessions on one
key while writes drive events), `lease` (T2.2: TTL expiry / KeepAlive
renewal / Revoke cascade-delete), and `soakfull` (T6.1: the long-run
acceptance entry point — several surfaces interleaved at declared weights,
`--soak-mix`).
Nemeses: `none`, `kill`, `kill-all`, `pause`, `partition`,
`partition-halves`, `partition-ring`, `all`, `soak`.

Results land in `store/coord/<date>/`.

## Options

| Option            | Meaning                                                        |
|-------------------|----------------------------------------------------------------|
| `--workload`      | `register` (default), `cas-register`, `idempotency` (T1.4), `map` (T1.1: delete/tombstone + 存在性), `txn` (T1.2: 全形态), `scan` (T1.3: range/revision), `mixture` (T1.5: map+txn+scan 混合), `watch` (T2.1), `multi-register` |
| `--nemesis`       | `none` … `all`, or `soak` (slow rotating fault cycle)          |
| `--time-limit`    | Test duration in seconds                                       |
| `--concurrency`   | Clients, e.g. `1n` (one per node)                              |
| `--rate`          | Fixed *global* ops/sec via `gen/delay` (soak default `0.5`)    |
| `--checker`       | `linear` (knossos, default) or `soak` (O(n), for long runs)    |
| `--soak-quiet`    | Soak nemesis: quiet seconds between disruptions (default 1800) |
| `--soak-disrupt`  | Soak nemesis: seconds each disruption lasts (default 600)      |
| `--seed`          | T0.5: seed for the nemesis jitter schedule. Generated + printed when absent; recorded in the test map / `results.edn` so a failing history can be replayed |
| `--no-jitter`     | T0.6: disable nemesis jitter (fixed 5s beat short runs, exact soak quiet/disrupt) to reproduce pre-T0.6 results |
| `--soak-max-rto-seconds` | T0.2: single global RTO bound, overriding the per-nemesis table |
| `--quiet-availability-min` | T0.2: quiet-window write `:ok` ratio threshold (default 0.95) |
| `--quiet-min-sample` | T0.2: minimum ops in a quiet window before it is judged (default 100) |
| `--idem-replay-delay-ms` | T1.4 (`--workload idempotency`): sleep before each replay of the same `request_id`. Non-zero pushes the replay past a kill/restart so it lands on a node with no cached entry (F-03); `0` (default) is the same-leader control group |
| `--idem-min-replay-attempts` | T1.4: replay-attempt sample gate (default 50, plan §5.1). Below it the idempotency checker is **invalid** (`:reason :insufficient-sample`) rather than green |
| `--idem-replay-node-offset` | T1.4: start each *replay* attempt N nodes after the cached leader. **Measured: this alone does not produce cross-node replay** (a write can only be served by the leader, so a non-leader attempt rotates back) — use `--nemesis kill --idem-replay-delay-ms 4000` instead |
| `--map-keys` / `--value-size` / `--map-min-deletes` | T1.1: distinct keys (8), written value bytes (16; F3 sweeps 4KB/64KB), and the §5.1 delete sample gate |
| `--mixture-ratio` | **T1.5** (`--workload mixture`): relative weights of `map,txn,scan` as three positive integers (default `4,2,2`, i.e. target op share 50%/25%/25%). `gen/mix` picks a sub-generator uniformly and takes **one** op from it, so the share equals the weight ratio — and the *achieved* share is reported per run under `:routing {:share ...}` |
| `--watch-semantics` | **T2.1** (`--workload watch`): which event-stream semantics the checker assumes — `overflow-marker` (default; the source drops the oldest events and emits an explicit `BufferOverflow`), `lossless`, or `coalescing`. Each of the three modes has a fixture proving the parameter is wired |
| `--watch-window-ms` / `--watch-max-events` | **T2.1**: how long one watch session stays open (default 2000 ms) and its event cap (default 10000) |
| `--watch-resumes` | **T2.1**: maximum stream re-opens **inside** one session, each resuming at `last-observed-revision + 1` (the contract's at-least-once resumption; default 2) |
| `--watch-min-events` | **T2.1**: §5.1 sample gate for the watch checker — below this many observed events the run is **invalid** (`:reason :insufficient-sample`), not green (default 200) |
| `--lease-ttl-seconds` / `--lease-grace-ms` / `--lease-keepalive-ms` / `--lease-revoke-ttl-seconds` | **T2.2** (`--workload lease`): requested lease TTL (2), the grace beyond `ttl` within which an expired/revoked key must be gone (4000), how long the keepalive scenario renews (2×ttl), and the TTL used by the revoke scenario (30, deliberately long so disappearance cannot be natural expiry). Assertions always use the **granted** ttl from the response (the server may clamp) |
| `--lease-min-grants` / `--lease-min-expiries` | **T2.2**: §5.1 sample gates (default 0 = do not gate; the plan asks for ≥100 grants / ≥30 expiry scenarios in acceptance runs). Below them the lease checker is **invalid**, not green |
| `--lease-tolerance-ms` | **T2.2**: slack on the safety side (default 500). Only "the key vanished early" is relaxed by this — better to under-report than to emit a false red from client/server timing skew |
| `--soak-mix` | **T6.1** (`--workload soakfull`): surfaces and relative weights, e.g. `map=40,txn=20,scan=5,watch=15,lease=10`. A surface that is not implemented yet (lock/election/registry need the M5 agent plugin surface) makes the test **fail at build time** — a soak must never look green while quietly skipping a surface |
| `--min-op-ok-ratio` / `--min-op-sample` | **G6 (F-13)**: per-op-type `:ok` ratio floor (0.1) and sample floor (10). Catches "this workload never actually ran" (e.g. a client-side construction bug turning every op into `:info` while knossos reports valid). Whitelisted legit failures (`cas-miss` …) are excluded from the denominator |

## 72h soak test

`--nemesis soak` drives the composed kill/pause/partition nemesis on a slow,
rotating schedule: 30 min quiet → 10 min of one disruption → recovery →
repeat. In soak mode the client runs at a fixed low rate (default 0.5 ops/s)
and writes **unique, monotonic values**, so the history stays checkable.

For long runs the `--checker soak` mode replaces the knossos search with an
exact O(n log n) single-register linearizability check (see
`jepsen.coord.soak`): it flags fabricated, future, and **stale** reads — the
stale-read class is exactly the bug the lab caught on `partition-halves` /
`partition-ring`. It also reports a summary (op counts, disruptions, max
committed value, whether the final verification reads converged).

```bash
# on the control node (root), 72 hours = 259200 seconds
cd /root/coord-test
LEIN_ROOT=true lein run test --nodes-file /root/nodes --username root \
  --ssh-private-key /root/.ssh/id_ed25519 \
  --workload register --nemesis soak --time-limit 259200 \
  --concurrency 1n --rate 0.5 --checker soak
```

Or from the host, backgrounded and monitored:

```bash
make soak              # 72h (SOAK_HOURS=72), 0.5 ops/s, checker soak
make soak-status       # progress + latest log lines
make soak-tail         # tail the soak log
make soak-results      # summarize the latest results.edn
make soak-stop         # cancel
```

T6.1 组合浸泡（多个面按声明比例混跑，进入验收长跑的入口）：

```bash
# 短跑验证（10min）；不传 SOAK_TIME_LIMIT 则为 72h
make soakfull SOAK_TIME_LIMIT=600 SEED=42 JEPSEN_PROVIDER=docker
make soak-wait && make soak-results
# 面比例可覆盖（未实现的面会让测试**构造期失败**，不会静默少跑）
make soakfull SOAK_MIX='map=40,txn=20,scan=5,watch=15,lease=10'
```

门禁（PR 级用前两个，夜间/发版前用 `nightly`）：

```bash
make checkers     # 17 套 98 个 checker fixture（含负控制与门槛档）
make matrix-m1    # 数据面 9 组合（map/txn/scan/mixture × none|kill + idempotency）
make matrix-m2    # Watch+Lease 8 组合（none|kill|pause|partition-halves）
make nightly      # checkers -> matrix-m1 -> matrix-m2 -> soakfull(2h) -> 结果
SKIP_LONG=1 jepsen/scripts/nightly-soak-gates.sh   # 只要 1-3 步（PR）
```

The soak runs detached on the control node (log at
`/root/coord-test/soak/soak.log`, pid at `soak/soak.pid`), so SSH
disconnects do not stop it. `--checker linear` can be used instead for a
short soak smoke run (e.g. `--time-limit 120`).

T1.5/T2.x 追加的透传参数（`make soak` 与 `scripts/coord-soak.sh start` 都支持）：

| 变量 | 含义 |
|:--|:--|
| `SOAK_TIME_LIMIT` | 秒数（覆盖 `SOAK_HOURS`）；M1 的 2h 收口即 `SOAK_TIME_LIMIT=7200` |
| `SEED` | 固定种子（T0.5）：让抖动过的故障排期可回放 |
| `CONCURRENCY` | 并发度（默认 `1n`；mixture 浸泡用 `2n` 提高混合度） |
| `MIXTURE_RATIO` | T1.5：`--workload mixture` 的目标 op 份额（默认 `4,2,2`） |
| `MAP_MIN_DELETES` | T1.5：map 面的 §5.1 delete 样本门槛（2h 档用 200） |
| `SOAK_MIX` | T6.1：`--workload soakfull` 的面比例（默认 = T6.1 比例里已实现的部分） |
| `SOAK_EXTRA` | 通用透传：原样拼进 soak 的 lein 命令（新选项不必再改脚本三处） |
| `SOAK_SECONDS`（nightly） | `nightly-soak-gates.sh` 第 4 步的时长（默认 7200 = 2h；72h = 259200） |

> 离线：`coord-soak.sh` 的 `lein` 调用默认带 `-o`（F-10/F-22：控制机/容器没有
> 出网，plain `lein` 会先卡在依赖解析上几分钟）；`ONLINE=1` 或
> `LEIN_OFFLINE=""` 可覆盖。

### Soak checker internals

`src/jepsen/coord/soak.clj` implements the O(n log n) checker. It indexes
`:ok` write completions, builds sorted prefixes over them, and for every `:ok`
read verifies (a) the value was actually written, (b) it was not read "from the
future" (its write had completed), and (c) it is not **stale** — no confirmed
write is *forced* between the write that produced the read's value and the read
itself (invoked only after that write completed, yet completed before the read
began). Property (c) is the stale-read bug class this lab caught.

> The older, stronger "(c) = the value is older than the newest confirmed write
> that completed before the read began" rule was **unsound** and produced a
> false positive in the 2026-09-05 72h soak: soak values are unique and
> increasing in *invocation* order, but not in *commit* order (a client that
> rotates/ re-issues after UNAVAILABLE can re-append a lower-valued write
> later). The region-3 leader's own `raft_wm ... read_served` watermark proved
> the read in question was served from its own applied state at the index
> carrying the lower value. `scripts/soak-checker-fixtures/` +
> `scripts/run-soak-checker-tests.clj` pin both this legal case
> (`expect-valid-reordered-commit.edn`) and the genuine one the rule must still
> catch (`expect-invalid-stale-superseded.edn`).

`scripts/validate-soak-checker.clj` re-runs the checker over an existing
`history.edn` (useful for debugging a failure):

```bash
# on the control node (root), from /root/coord-test
LEIN_ROOT=true lein run -m clojure.main \
  /tmp/validate.clj <path-to>/history.edn
```

### Checker gates (T0.2) — `src/jepsen/coord/gates.clj`

Composed into every run alongside the linearizability checker, and **gated**
(its failures reach `:valid?`, not just a report). Three independent gates:

1. **premise** — the soak staleness rule treats a write's value as its global
   sequence number, so two write *invocations* carrying the same value make the
   whole verdict unsound: recorded as invalid rather than a green lie.
2. **quiet-window availability** — write `:ok` ratio inside each quiet window
   (previous nemesis stop → next start). Windows with fewer than
   `--quiet-min-sample` ops are **recorded but not judged** (a 6-op window says
   nothing about availability, and judging it is a false red).
3. **RTO** — disruption stop → first `:ok` write, gated on the P95 against the
   per-nemesis budget, with P95/max in the summary. A disruption is only judged
   when its stop precedes the last write *invoke*: past the write phase there is
   nothing left to recover for, and the soak generator stops the nemesis after
   the write phase by construction. Cluster recovery at the end is the soak
   checker's convergence gate, not RTO's.

Times in `history.edn` are a **monotonic nanosecond** clock on the control node
(a `--time-limit 60` run spans ~1.2e11), *not* epoch milliseconds — RTO is
`(t2 - t1) / 1e9`. `scripts/nemesis-timeline.clj` used to mislabel this; it now
prints seconds relative to the first op.

### Checker fixtures (T0.3) — negative controls first

```bash
# on the control node (root), from /root/coord-test
LEIN_ROOT=true lein run -m clojure.main scripts/run-checker-tests.clj \
    jepsen.coord.soak  scripts/soak-checker-fixtures
LEIN_ROOT=true lein run -m clojure.main scripts/run-checker-tests.clj \
    jepsen.coord.gates scripts/gates-fixtures '{:quiet-min-sample 10 :nemesis :kill}'
```

`expect-valid-*.edn` must come out valid and `expect-invalid-*.edn` invalid; a
fixture whose name matches neither is an error (renaming a fixture must not
silently disable it). Both shapes load: a stream of op maps (what jepsen writes)
and a single vector literal. From the host: `make checkers` (a red fixture
refuses to start `make quick` / `make test`).

### Environment preflight (T0.4)

```bash
make env-reset STRICT_CLOCK=1     # all nodes: iptables/tc/clock/processes/data
```

`scripts/env-reset.sh` clears iptables DROP/REJECT rules, netem qdiscs, stray
`coord` processes and disk-fill files, force-syncs the clock and **verifies** the
offset, and exits non-zero when the environment is not clean. Two modes matter:

* `--wipe-data /var/lib/coord` — run boundaries only (`db.setup!`, `make env-reset`).
* `--network-only` — what every nemesis `:stop` path calls. It deliberately does
  **not** pkill `coord`: it runs inside `:stop`, right after kill's `stop!`
  restarted the node and pause's `stop!` sent SIGCONT, so a pkill there would turn
  "disruption → recovery" into "disruption → permanent outage" and poison the
  whole history.

`scripts/env-reset-all.sh` runs it on the control node and every node in
`/root/nodes` (the worker nodes have no `/root/coord-test`, so `db.setup!` also
uploads `env-reset.sh` to each node).

### Seeds, replay and evidence (T0.5 / T0.1)

```bash
# rebuild the jittered nemesis schedule from a recorded seed
lein run -m clojure.main scripts/replay.clj store/coord/latest --seed 42
# archive a run with a MANIFEST (environment/commit/params/seed/gate verdicts)
scripts/collect-evidence.sh soak-72h store/coord/latest
# key=value gate summary of a results.edn
lein run -m clojure.main scripts/summarize-results.clj store/coord/latest
```

`collect-evidence.sh` writes to `docs/production/evidence/<UTC>-<scenario>/`
(`MANIFEST.md`, `run.log`, `results.edn`, `history.edn` (gzipped above 5MiB),
`summary.txt`, `sha256sums.txt`). A run without a MANIFEST is not evidence: it
cannot be cited in an adoption review.
