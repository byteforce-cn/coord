# coord Jepsen test

Jepsen tests for [coord](../README.md) — a strongly-consistent (linearizable)
KV store with gRPC KV + Txn (CAS) + Maintenance APIs.

> 本目录是 **coord 仓库的一部分**（`coord/jepsen/`）：Jepsen 测试源码与跑批脚本随
> coord 一起版本化，checkout 任意 coord commit 即得到与之配套的测试与脚本。coord
> release 可执行文件（`coord`，约 28MB）**不入库**——由部署方从当前 checkout 构建后
> 随上传部署到控制机（见下文 Run）。旧位置 `jepsen-custom/examples/coord-test/`
> 已于 2026-09-05 废弃迁移，请勿继续引用。

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
  `:info` (may or may not have applied), CAS miss → `:fail`.
* **Proto layer** (`jepsen.coord.proto` + `src/java/jepsen/coord/CoordRpc.java`)
  — wire-compatible descriptors built programmatically (field numbers match
  the coord contract exactly), so no generated stubs are required.
* **Nemeses** — kill/restart (via the DB `Kill` protocol), SIGSTOP pause,
  iptables partitions (random node / random halves / majorities ring), a
  combined `:all`, and a slow-rotating `:soak` cycle (see below).
* **Checkers** — knossos `linearizable` over `model/register` or
  `model/cas-register` (WGL), plus `perf`; and a dedicated O(n log n) **soak
  checker** for long runs (`jepsen.coord.soak`).

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

Workloads: `register`, `cas-register`.
Nemeses: `none`, `kill`, `kill-all`, `pause`, `partition`,
`partition-halves`, `partition-ring`, `all`, `soak`.

Results land in `store/coord/<date>/`.

## Options

| Option            | Meaning                                                        |
|-------------------|----------------------------------------------------------------|
| `--workload`      | `register` (default) or `cas-register`                         |
| `--nemesis`       | `none` … `all`, or `soak` (slow rotating fault cycle)          |
| `--time-limit`    | Test duration in seconds                                       |
| `--concurrency`   | Clients, e.g. `1n` (one per node)                              |
| `--rate`          | Fixed *global* ops/sec via `gen/delay` (soak default `0.5`)    |
| `--checker`       | `linear` (knossos, default) or `soak` (O(n), for long runs)    |
| `--soak-quiet`    | Soak nemesis: quiet seconds between disruptions (default 1800) |
| `--soak-disrupt`  | Soak nemesis: seconds each disruption lasts (default 600)      |

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

The soak runs detached on the control node (log at
`/root/coord-test/soak/soak.log`, pid at `soak/soak.pid`), so SSH
disconnects do not stop it. `--checker linear` can be used instead for a
short soak smoke run (e.g. `--time-limit 120`).

### Soak checker internals

`src/jepsen/coord/soak.clj` implements the O(n log n) checker. It indexes
`:ok` write completions, builds a sorted prefix-max over them, and for every
`:ok` read verifies (a) the value was actually written, (b) it was not read
"from the future" (its write had completed), and (c) it is not **stale** —
older than the newest confirmed write that completed before the read began.
The last property is the stale-read bug class this lab caught. `scripts/
validate-soak-checker.clj` re-runs the checker over an existing
`history.edn` (useful for debugging a failure):

```bash
# on the control node (root), from /root/coord-test
LEIN_ROOT=true lein run -m clojure.main \
  /tmp/validate.clj <path-to>/history.edn
```
