# Remaining known gaps (round 4 remediation, 2026-09-13)

This file is the **honest counterpart** to the round-4 remediation work. It lists what was
*deliberately not fixed* in this round, why, what the impact is, and what the evidence is.

The fourth external review (`docs/第四轮.md`, base commit `e2ab961`) produced a 22-item
closing list (§6.2, groups A/B/C). This round fixed every A-item, every B-item, and the
client-side half of the Java items. The list below is what remains.

**Rule for this file:** every entry must name a *verifiable* fact (file/line, command, or
test), not an intention. An entry that cannot be verified does not belong here.

---

## A/B items — all closed

> **⚠️ Read this first — a P0 regression was found while validating this round.**
>
> Adding `/coord.watch.Watch/Watch` to the body-buffering scope set (`needs_scope_extraction`)
> **broke watch entirely**: the auth layer is mounted **regardless of `auth.enabled`**, and for
> that method it ran `buffer_request_body` — but Watch is a **streaming** RPC whose body never
> ends before the client half-closes, so the request was never forwarded. The client received
> neither events nor an error (Rust `message().await` hung; the Java suites timed out).
>
> Fixed in this round: the buffering set is now **unary-only**, `is_streaming_rpc()` names the
> invariant, and two regression tests pin it (`coord-core` `streaming_rpcs_must_not_be_body_buffered`,
> `coord-agent` `scope_bearing_rpcs_are_unary_only`). Verified: `java-example`
> `WatchAdvancedTest` + `WatchIntegrationTest` **6/6 pass** against a real cluster.
>
> **Residual — corrected: it is a functional limitation, NOT a security hole.**
>
> (This entry originally claimed a "known security gap … a role with a non-empty scope
> restriction can subscribe to a prefix outside that scope". That claim was **wrong** and was
> caught by testing instead of reasoning. It is left visible here because an unverified
> security claim in either direction is a defect.)
>
> What actually happens: the agent's capability table maps `/coord.watch.Watch/Watch` →
> `data:watch:subscribe` **without a scope extractor** (`scope_extractor: Option<ScopeExtractor>`
> takes `&http::HeaderMap`, and Watch's prefix lives in the *streaming body*). So the tower
> layer takes the **synchronous** path and calls
> `validate_request_accesses(rpc, header, &[])` — with **zero** extracted accesses. That path is
> **fail-closed**: `scope_allows` requires an *unrestricted* (empty-scope) grant, otherwise it
> denies. Verified by `coord-agent` test `watch_scope_is_fail_closed_not_bypassed`, which pins
> both directions: a scoped `data:watch:subscribe` grant → **Deny**; an unrestricted one →
> Allow. (The generic mechanism was already tested by
> `test_interceptor_scope_restricted_capability_fails_closed_without_resource_key`.)
>
> So a scope-restricted role cannot subscribe outside its scope — it cannot subscribe **at
> all**. The real cost is lost functionality for those roles, and the real fix is to evaluate
> the **first decoded `WatchCreateRequest`** in the handler (`WatchProxy::watch` already decodes
> it; what it lacks is the caller's per-capability grant scopes, which the interceptor resolves
> and currently discards). `coord_core::grpc_auth::extract_scope_access` models Watch requests
> for exactly that purpose, but the handler-side call is not wired.
>
> *Deliberately not done in this round:* it means re-touching the same auth path that just
> produced the P0 above, and it **grants** access that is currently denied — i.e. it can only
> widen, never narrow. Doing it immediately before a release would trade a known-safe
> over-restriction for an unknown availability risk. It is the first post-`0.1.0` item.

| # | Item | Status | Evidence |
|:--|:---|:---|:---|
| A1 | Agent forwards caller credential | closed | `coord-client/src/credential.rs` (`REQUEST_TOKEN` task-local), `coord-agent/src/proxy.rs` |
| A2 | Agent scope fail-closed + production extractor | closed | `coord-agent/src/auth/interceptor.rs` (`validate_request_accesses`, `scope_allows`) |
| A3 | Single capability table | closed | `coord-core/src/grpc_auth.rs` (`rpc_capability`) |
| A4 | `agent + auth_enabled=true` process test | closed | `coord/tests/agent_auth_process_test.rs` (`AGENT_AUTH_REAL=1`) |
| B5 | CI wiring (8 breaks) | closed except ⑧ | see "CI" below |
| B6 | Election exception-branch judge (**P0**) | closed | `coord-agent/src/services/leader_election.rs` (`election_key_is_mine`); `coord/tests/leader_election_test.rs::two_instances_with_same_candidate_id_cannot_both_be_leader` — **negative-controlled**: with the old judge this test reports `left: Leader, right: Follower` |
| B7 | `metrics.method_metrics` bound | closed | `coord-server/src/metrics.rs` (`MAX_METHOD_METRICS`) |
| B8 | `expire_tx` has no consumer | closed (deleted) | `coord-server/src/timer/mod.rs` — channel removed; expiry is driven by `LeaseManager::check_expired()` |
| B9 | `_prune()` / `cleanup_expired()` wired | closed | `AuthService::start_maintenance_worker` + `AuthOp::ConsumeSessions`; `coord/src/main.rs` |
| B10 | Watch subscriber reclaim | closed | `coord-server/src/server/mod.rs` `tokio::select! { _ = tx.closed() => break, .. }` |
| B11 | Watch range semantics | closed (unified, wire behaviour kept) | `coord_core::kv_range::watch_match_interval` used by **both** `watch/mod.rs::key_matches` and `grpc_auth::extract_scope_access`; equivalence test vs the legacy predicate |

### B5 ⑧ — branch protection is a repository setting

`.github/workflows/ci.yml` cannot enforce "`lint` + `test` are required checks, no direct
push to `main`". This is a GitHub repo setting and must be applied by whoever owns the
repository, not by a commit. **Until it is applied, the 45-all-red-CI history can repeat.**

---

## C items — what is closed, what is not

### Closed in this round

| # | Item | Evidence |
|:--|:---|:---|
| C12② | `x-coord-error-code` trailer emitted by Rust | `coord-core/src/error_code.rs`; applied in `coord-agent/src/proxy.rs::map_core_error` (every `CoreError` arm), `coord-server/src/auth/interceptor.rs::classify_denial`, agent `deny_response`, `CoordNode::map_client_write_error` (`NOT_LEADER`). Java: 10 new `ErrorCode` constants, `ErrorMapper` fallback de-lossified, `RetryTemplate` retries `NOT_LEADER`/`UNAVAILABLE`. Tests: `coord-core error_code` 6, `coord-server auth::interceptor` (incl. *trailer survives HTTP encoding*), `coord-java-sdk` `ErrorCodeTest` 5 + `ErrorMapperTest` 12 |
| C12③(part) | Documentation no longer claims a Spring integration exists | `CONTRIBUTING.md`, `.gitignore` |
| C15 | `CacheService::max_size_bytes` no longer silently dropped | `coord-agent/src/services/cache.rs` — value stored, startup warning, `Debug` reports `max_size_enforced: false` |
| C16 | `wasm_engine` command queue bounded | `coord-agent/src/plugin/wasm_engine.rs` — `sync_channel(MAX_PLUGIN_QUEUE_DEPTH)`, `try_send` backpressure (never blocks the async executor) |
| C21 | `dead_tasks()` has a consumer + an alert | `coord_dead_background_tasks` / `coord_dead_background_task_info` in `coord-server/src/metrics.rs`; `/health?verbose=true` returns `dead_background_tasks`; alert `CoordBackgroundTaskDead` in `monitoring/prometheus-rules.yml` |
| C22(part) | Enum-rename hazard documented | `coord-proto/buf.yaml` (see below) |
| C12③ | **Java SDK watch reconnects and resumes** | `internal/watch/GrpcWatchStream.java` (cancellable stream), `WatchManager` (bounded exponential backoff, resume from `lastRevision + 1`, terminal notification, unregister on termination); `autoRestoreWatches` is now read. Regressions: `WatchManagerTest` (6 tests, incl. reconnect-from-revision + cancel-wakes-a-silent-stream) |
| C12④ | **`close()` can no longer leave a hung future** | `WorkflowWatchHandler` runs on the managed executor, registers in a live set, and is completed exceptionally by `CoordClient.close()`; bounded poll failures. Regressions: `WorkflowWatchHandlerTest.clientShutdownMustNotLeaveTheFutureHanging` |
| C12④(part) | MQ subscriptions are tracked and cancelled by `close()`; heartbeat threads are daemon | `MqClientImpl` (`cancelAllSubscriptions`), `ThreadPoolManager` |
| C13 | **`java-example`'s three real bugs are fixed** | `CoordClient.scan()` now uses the shared `PrefixScan.end` (was `prefix + "\0"` ⇒ matched **zero** keys, so `getAll()`/`discover()` always returned empty); `keepAlive()` really keeps sending (was one frame); `ServiceRegistry.register()` renews via keep-alive instead of re-`put` (which never extended the TTL). `PrefixScan` moved to `src/main` so main and tests share **one** implementation |
| §3.14.5(part) | `ConfigClient.getObject` no longer loses data silently; `list()` no longer turns failures into "no config" | `getObject` throws `INVALID_ARGUMENT`/`CONFIG_INVALID`; `list` only maps explicit NOT_FOUND to an empty map |
| **Spring** | **Product decision: no Spring Boot starter will be restored.** The SDK is the supported integration surface; adopters wire `@Bean`s and own the lifecycle | recorded for adopters in `README.md` (the “Spring Boot adopters” block in the Java integration section, with the `@Bean(destroyMethod = "close")` recipe) and for contributors in `CONTRIBUTING.md` |
| §3.14.8 / §3.15 | README (EN + zh-CN) no longer overstates TLS, Jepsen, CI, `jepsen-check.sh`, the SDK coordinates or the experimental capabilities | see the diff; `docs/*` at the repo root is git-ignored, so all new docs live under `docs/production/` |

### NOT closed — with impact

#### 1. Spring Boot starter — **an explicit product decision, not a gap**

16 files / −993 lines were deleted in `2b55809` **together with their tests and with no
replacement**. The decision taken in this round is **not to restore it**: `coord-java-sdk` is
the supported integration surface and is verified (149 unit tests + a real-cluster
integration suite); adopters hand-write `@Bean` wiring and own the lifecycle.

*Consequence to state plainly:* the review's §6.3 condition is satisfied only through its
"…or the organisation explicitly accepts hand-written `@Bean` + self-managed lifecycle"
branch. There is **no** auto-configuration, no `@Bean` beans for `CoordClient`/clients, and
no Spring lifecycle (`destroyMethod`) integration.

#### 2. Java SDK watch: **fixed in this round** (was §3.14.3) — retained here for audit

Reconnects with bounded exponential backoff and resumes from `lastRevision + 1`; a terminal
subscription notifies the listener via `ConfigListener#onTerminated` / `RegistryListener#onTerminated`
and is removed from the registry. `autoRestoreWatches` (previously declared, documented as
default `true`, and **never read**) now genuinely controls reconnection.

*Residual:* each watch consumes one virtual thread while blocked, and the stream queue is
bounded at 1024 — overflow ends the stream and triggers a replay from the last revision,
which requires the server to still hold that history (compaction can legitimately make it
unavailable; the server reports `HISTORY_UNAVAILABLE` in that case).

#### 3. `CoordClient.close()` — **fixed in this round** (was §3.14.4)

Pending workflow watches are completed exceptionally, MQ subscriptions are cancelled, and
heartbeat threads are daemon. *Residual:* there is still no `addShutdownHook`, so a process
that never calls `close()` leaks rather than exits cleanly — but it can now exit.

#### 4. `java-example` — **bugs fixed in this round** (was §3.14.6)

The three defects are fixed (`range_end`, keep-alive renewal, single-frame keep-alive).
*Residual:* `java-example` still does **not** depend on the SDK (`grep -rn
"cn.byteforce.coord.sdk" java-example/src` → 0 hits), so it remains a raw-gRPC demonstration
and its 51 integration tests still do not exercise the SDK. Making the CI job prove SDK
usability requires that migration; it is not done here.

#### 5. CI step name and evidence artifacts (C14, §3.16)

* `ci.yml`'s Java-example step runs **0 tests** (the pom excludes all 51 with the default
  profile); only `-Pit` runs them. The step name still implies otherwise.
* `docs/production/evidence/20260912T112707Z-java-it/` records commit `e79f882` with
  `dirty files: 30`, and its `command` field references a script that does not exist at that
  commit; there is **no committed `-Pit` run of `CoordClientIntegrationTest`** at all, so the
  SDK's only real integration evidence is not in the repo.

*Why not fixed here:* regenerating evidence requires a clean checkout at a *tagged* commit.
That is the natural first act **after** tagging, not before it.

#### 6. `proto` enum evolution is still not mechanically protected (C22)

`buf.yaml` still exempts `ENUM_VALUE_PREFIX` and `ENUM_ZERO_VALUE_SUFFIX`; `reserved` is
unused; `watch.proto`'s `EventType.PUT = 0` merges "unset" with "a real write". Renaming an
enum value is a **silent** break for grpc-java (the numeric mapping is unchanged, so
nothing fails to compile).

*Why not fixed here:* un-exempting those rules forces renaming enum values, which is
source-breaking for the Java SDK. Doing it in the same commit as everything else would
make the diff unreviewable. It is a **wire-contract migration**, and the first tagged
release is the deadline for it — schedule it as the first post-0.1.0 work item, while there
are still no external consumers.

#### 7. In-process agent watch probe (`coord/tests/agent_watch_test.rs`) — `#[ignore]`d

`test_agent_watch_single_subscriber` **never actually verified delivery**: its
timeout/error branches only logged a warning and returned, so "zero events delivered" also
counted as a pass (the baseline run took 33s ≈ setup + an 8s empty wait). It was that
tolerance — not a passing assertion — that let the streaming-body regression above slip
through. The tolerance is removed (the test now asserts), and the test is explicitly
`#[ignore]`d with the reason rather than being silently green or deleted.

*Known state:* in the **in-process** agent setup the client receives no events. The
`WatchProxy` is confirmed to subscribe upstream (`inner = Some`, correct prefix,
`start_revision = 0`), so the gap is between `coord_client`'s upstream receiver and the
in-process test wiring — not the Java path, which is covered with real assertions by
`java-example`'s watch suites (6/6, in the `java-example-it` CI job).

#### 8. Unbounded growth that was not touched
Ranked by the review's §3.10; all of these are self-consistent with the "declared but not
enforced" theme of C15:

* `MemoryWorkflowStore` + a 5-second full scan (the heaviest agent-side growth).
* Dynamic region cleanup is not wired; adding/removing regions is effectively unsupported.
* `snapshot_logs_since_last = 0` semantics (following the documentation breaks it).
* Connection-count limits and concurrency protection.

#### 9. Doc references that cannot be committed — **FIXED**

`config.example.toml` (in the `[object_storage]` block) and `apis/contracts/STATUS.md` used
to point at `docs/volume-object-storage.md`, and `jepsen/README.md` at a `docs/coord.md` that
never existed. Files placed directly under `docs/` **cannot be committed** — `.gitignore`
allows only `docs/production/`, `docs/production/evidence/` and `docs/production/design/` —
so those targets could be created locally but never tracked, i.e. dead links for anyone who
clones the repository.

Fixed: the design record moved to `docs/production/volume-object-storage.md`, `.gitignore`
now allows top-level `docs/production/*.md`, and both references (plus `jepsen/README.md`)
were corrected. The review reports themselves (`第三轮.md` / `第四轮.md`) are intentionally
**not** tracked — they are local working documents.

#### 10. `cargo deny`: exactly one advisory is exempted by policy (bincode)

`deny.toml` now states `unmaintained = "all"` / `unsound = "all"` explicitly (instead of
inheriting whatever the tool's default happens to be) and exempts exactly one advisory:
`RUSTSEC-2025-0141` — bincode 1.3.3 is **unmaintained, not vulnerable**; the advisory itself
says `No safe upgrade is available!`.

This was the actual cause of the `cargo audit + deny` CI failure, reproduced locally with the
exact version CI uses (the `EmbarkStudios/cargo-deny-action@v2` Docker image pins
`deny_version=0.20.2`; the same 0.20.2 binary reproduces `error[unmaintained]` on bincode and
then `advisories ok` once exempted). With informational advisories disabled the check reports
`advisories ok` — i.e. **there are no vulnerabilities in the graph today**.

The job had a **second, independent** failure — the `cargo audit` step, and it was not about
dependencies at all. `rustsec/audit-check@v2.0.0` calls `cargo audit --json` with
`ignoreReturnCode: true` (so the audit exit code is irrelevant), and when there is anything to
report it POSTs a **check run**; with no `checks: write` permission it calls
`core.setFailed("Resource not accessible by integration")` and the step goes red. Its own output
in that run was `"vulnerabilities":{"found":false,"count":0}` — the only trigger was the
informational bincode warning. `ci.yml` now declares `permissions: { contents: read,
checks: write }` **on that job only**, so informational advisories are reported as a check run
(visible, non-blocking) while real vulnerabilities still block.

The general lesson is worth stating: a security gate that fails because it cannot *report* is
worse than no gate, because it trains people to ignore it.

bincode is not a convenience dependency here — it is the persistence format: snapshots
(including V1/V2/V3 backward-compatible decoding), Raft log entries, `/_sys/auth/` records
(where the enum variant index *is* the variant number), PD region metadata, object-store
manifests, and `AppliedLogId`'s legacy-encoding fallback. Replacing it is a cross-version data
migration with a compatibility window, not a dependency bump — doing it inside a release
commit would be the risky choice.

*Rule for this exemption:* the list may only grow one advisory at a time, each with a written
reason and a closing path. Removing bincode is real debt and stays open.

#### 11. `coord-ui` coverage: the global threshold is a ratchet, not a target

The `frontend lint (coord-ui)` job failed in CI, and **not** at lint or build — `pnpm lint`
(0 errors) and `pnpm build` both pass. It failed at the coverage thresholds added in round 4:
the global 80/70 bar was never achievable. Only three unit-test files exist (`auth` API,
`authStore`, `client`), giving ~15 % global statements, because `src/routes/**` and
`src/components/**` have **no vitest tests at all** — their verification happens in the
Playwright e2e run, which does not count toward vitest coverage. A threshold that is red on
every commit is not a strict gate; it is a gate people learn to ignore.

Now the global numbers are a **ratchet** pinned at the achieved level (so coverage cannot
regress silently) and the modules that do have tests (`src/api/**`) are held to 90/85/90/90
(currently ~94/90/95/100). The gate was negative-controlled: raising the `src/api/**`
statement bar to 99 % turns the job red (`does not meet "src/api/**" threshold (99%)`).

*Open:* raising the global bar to 80/70 requires writing route/component tests. That is not
done, and the config does not pretend otherwise.

Related honesty fix: `src/hooks/__tests__/useAuth.test.ts` never touches the `useAuth` hook
(it asserts the auth HTTP API through msw, as its own header comment says). The coverage
report shows `src/hooks/useAuth.ts` at 0 %. The file name overstates what is covered.

#### 12. MQ subscribe has an unavoidable registration window

`MqSubscribeRequest` carries only `topic` + `consumerGroup` — there is **no start offset**,
and the server registers the subscription inside its handler. So messages published after
`subscribe()` returns on the client but before the server handler runs are delivered neither
live nor in replay (replay happens once, at subscribe time). The client has no way to ask for
"from offset N", so it cannot close that window itself.

This is a protocol limitation, not a client bug, but it must be stated for adopters:
**subscribe, then verify you are receiving, then rely on it.** It surfaced as a flaky CI test —
`MqClientTest.shouldSubscribeReplayAndPush` failed once on `f3e33de` with
`[订阅应收到回放+实时共 2 条] expecting value to be true but was false` although no Java code
changed in that commit. The test was racing the window; it now waits for server-side
registration (`FakeMqService.awaitSubscribers`) and passes 8/8 locally. The assertion was not
weakened — it still requires both messages.

*Closing path:* add an explicit `start_offset` to `MqSubscribeRequest` (a wire change) and let
the client resume deterministically.

**The same window exists for Watch — and there the protocol *does* offer an anchor.**
`WatchCreateRequest.start_revision` replays history, while `start_revision = 0` means "from
latest" and therefore replays nothing. `java-example`'s `WatchIntegrationTest` was written as
"watch, then put" with `start_revision = 0` — i.e. it raced the window and had **no** way to
notice. It failed once on `78a92b9` with
`WatchIntegrationTest.testWatchSingleKey:92 [Watch event received]` (1 of 51) on a commit that
changed no Java code, one run after being 51/51. It now anchors on the protocol's own signal:
`put` → watch **from that revision** → await the replay (which simultaneously proves the
subscription is registered) → `put` again → await the live event. Both replay and live-push
coverage are retained; locally the class passes 6/6 repeated runs and the full suite 51/51.
MQ cannot do the equivalent yet — that is why §12 remains open.

#### 13. Plugin identity: bounded retry added; the chaos red was **inter-suite contamination**

`PluginIdentityManager::ensure_with_retry` (max 5 attempts, ~3 s worst case) now wraps
`ensure`, and both engines (`js_engine`, `component_engine`) use it and log the degradation at
**ERROR** with its consequence. Before this, a single transient failure during startup
permanently degraded the plugin to the shared **unauthenticated** client, after which every
outbound call is fail-closed by the server (`unauthenticated: missing CCT token`) and
**never recovers** until a SIGHUP or agent restart. That is a real defect regardless of cause:
disable-then-never-retry is the wrong answer to a transient error on an auth path.

**Root cause of the CI red (confirmed by local A/B, not by reasoning):** the chaos job's suites
run serially but **contaminate each other**. `chaos_real` (kill9), the soak, multi-raft and the
three plugin_auth suites leave orphan `coord server`/`coord agent` processes behind, which keep
competing for CPU and ports — so *later* suites fail in ways that look like product bugs. The
job only ran `scripts/kill-stray-coord-procs.sh` **once at the start**, not between steps.

Local reproduction, same commit, same command:

| condition | `plugin_real_process_test` |
| --- | --- |
| run the preceding suites, then this one (orphans present) | **red** — `bootstrap-role must succeed: RoleAdd("agent-bootstrap") failed: … raft auth write timed out (no quorum?)` |
| `scripts/kill-stray-coord-procs.sh`, then the identical sequence | **green** (4.03 s) |

CI's own manifestation was the `missing CCT token` variant: under contention the agent's
`Authenticate` for the plugin account fails, `ensure` fails, and the plugin degrades to the
unauthenticated client. `ci.yml` now pre-cleans before **every** process-heavy step in that job
(7 steps). `scripts/kill-stray-coord-procs.sh` only touches processes whose data directory — or,
for the suites that configure the agent through `--agent-config` alone, whose *config path* — is
under `/tmp/.tmp*`, so real clusters are untouched (verified: a `--agent-config /tmp/.tmpFAKE/…`
agent is killed while the local dev cluster on `.coord-dev-cluster` keeps running).

*Still open:* after the bounded retries fail there is **no on-demand retry** — the plugin stays
degraded until it is reloaded. Closing this means resolving the client lazily per call (or
re-running `ensure` when an outbound call is denied). The real-process test now dumps the agent
log tail into the assertion message, because the client-side error alone (`missing CCT token`)
cannot distinguish "identity never authenticated" from "token expired".

#### 14. `--data-dir` silently overrode the configured `data_dir` — **fixed** (this was the last chaos red)

This is the defect behind the `plugin_real_agent_process_e2e` red that §13 first attributed to
inter-suite contamination. Contamination was real, but it was not the whole story.

`coord/src/main.rs` declared the global flag as `data_dir: PathBuf` with clap
`default_value = "/var/lib/coord"`. A clap default is **indistinguishable from an explicitly
passed argument**, so the flag was always populated:

* `Commands::Agent` did `agent_config.data_dir = cli.data_dir…` **unconditionally** — the
  `data_dir` in `agent.toml` never took effect. The agent used `/var/lib/coord`, i.e. the
  *server's* default, not the agent's documented `/var/lib/coord-agent`.
* `Commands::Server` passed `Some(&cli.data_dir)` to `apply_cli_overrides`, with the same effect
  on `coord.toml` (`apply_cli_overrides` only skips a field when it receives `None`).
* `run_dev` compensated with a string sentinel: `if data_dir.to_string_lossy() == "/var/lib/coord"`,
  which silently rewrote an explicitly-passed `/var/lib/coord` to `./coord-dev-data`.

**Reproduced exactly, in CI's condition** (uid 1000, `/var/lib/coord` absent — as on a fresh
runner), running CI's step-11 command:

```
thread 'plugin_real_agent_process_e2e' panicked at coord/tests/plugin_real_process_test.rs:647:13:
plugin KV write must still succeed after agent restart (persisted account):
  code: 'Internal error', message: "plugin 'counter' invoke 'put' failed: … unauthenticated: missing CCT token"
ERROR plugin 'cache' start failed: Permission denied (os error 13); isolating
ERROR 2 builtin service(s) failed to start: [("cache", "start failed: Permission denied (os error 13)"), …]
```

That is the same failure, on the same assertion, as the CI log. Mechanism: the agent could not
create `/var/lib/coord/cache.redb` nor persist `plugin-accounts/`, so the plugin account password
was never durable; after the test's `agent.restart()` step the agent generated a *new* random
password, `Authenticate` failed (`unauthenticated: invalid credentials`), the plugin fell back to
the shared unauthenticated client, and every server-side check was fail-closed
(`missing CCT token`).

**Why it stayed hidden locally — two independent reasons:**

1. The suites were run as **root**, for whom `/var/lib/coord` is writable; the configured
   temp directory was silently ignored and nothing complained.
2. Once `/var/lib/coord/plugin-accounts/*.enc` exists (written by any earlier root run), it is
   world-readable, so a non-root run can still `load` the secret even though it cannot `store`
   it — which is enough to make the restart step pass. This is why the failure only appears when
   the directory does **not** pre-exist, as on CI.

Fixed: `--data-dir` is now `Option<PathBuf>` with no clap default, so "not passed" is
representable; the agent overrides `data_dir` only when the flag is explicit; the server passes
`as_ref()`; `run_dev` takes `Option<&Path>` and uses `./coord-dev-data` on `None`, with the
sentinel removed.

Evidence:

* `coord/tests/cli_agent_test.rs::test_agent_uses_config_data_dir_unless_flag_is_explicit` pins
  **both** directions (config alone ⇒ the configured directory is used; explicit flag ⇒ the flag
  wins and the configured directory is not created).
* Negative-controlled: against the pre-fix `main.rs` the new test **fails** (it waits out its
  30 s deadline because the configured directory is never created). It fails **as root too**, so
  this class of bug can no longer hide behind "local is green, CI is red".
* A/B on CI's own step-11 command, uid 1000, `/var/lib/coord` absent:
  pre-fix **red** with the log above; post-fix **green** (3.37 s) and `/var/lib/coord` is
  **never created**.
* A direct probe of `coord agent --agent-config …` as uid 1000 confirms the unit-level change:
  pre-fix the configured directory is not created and `Permission denied` appears twice;
  post-fix it is created (`cache.redb`) with zero `Permission denied`.

*Side effect worth knowing:* this also fixes real-process isolation — the suites no longer share
`/var/lib/coord`, so cross-suite state leakage through the default data directory is gone.

*Also:* chaos steps 12 (`OBJECT_STORAGE_REAL`) and 13 (`AGENT_AUTH_REAL`) had **never executed**
in CI, because step 11's failure skipped them. Both pass locally as uid 1000 against the fixed
binary (3.40 s / 2.74 s) — but that local pass turned out **not** to be sufficient evidence: step
13 failed the first time it actually ran in CI. See §15.

#### 15. Agent served the auth path with an empty `RoleCache` — **fixed** (exposed by the never-run chaos step 13)

With §14 fixed, step 11 turned green and the job continued past it for the first time. Step 12
passed; step 13 failed:

```
in-scope read through agent must be allowed, got: Some(Status { code: Unauthenticated,
  message: "role(s) [\"app-reader\"] do not have capability 'data:kv:read'" })
```

That message is emitted by the **agent** (`coord-agent/src/auth/interceptor.rs:438`), not by the
server: the interceptor denied because `RoleCache::scopes_for_capability` returned an empty list.

Root cause: `spawn_role_sync` (`coord-agent/src/auth/sync.rs`) `tokio::spawn`ed
`RoleSyncTask::run()` **without awaiting the first full sync**, and `lib.rs` then enabled the
interceptor and started serving. For that window the cache was empty ⇒ every role-gated RPC was
fail-closed denied — including **legitimate, in-scope** ones. `RoleCache::is_initialized()`
existed but had **no production caller**; that is precisely this ordering seam left unwired. The
test was written on the assumption that "agent 启动时做一次全量同步" makes the role available
before traffic — the implementation made that sync asynchronous, so the assumption only *usually*
held (which is why it passed locally and failed on a loaded runner).

Fixed: `spawn_role_sync` now performs one full sync **before returning**, bounded by
`INITIAL_ROLE_SYNC_TIMEOUT` (10 s). Failure/timeout still does **not** block startup — the
documented "server unreachable ⇒ degrade, don't stop" behaviour is preserved, and the agent stays
fail-closed while the background loop retries with backoff. `run()` no longer issues a second
sync immediately afterwards.

*Test:* `agent_auth_process_test` (CI chaos step 13) is the pin — it is the test that caught it.
No new test was written: a mock `ListRoles` service would be needed to unit-test the ordering,
and the real-process test already exercises exactly this path.

#### 16. `auth_enforcement_test` first-RPC readiness race — **hardened** (still open)

`workspace tests` failed on `test_auth_users_survive_restart` at the first `Authenticate`,
immediately after `wait_ready` returned:

```
called `Result::unwrap()` on an `Err` value: Status { code: Unknown, message: "transport error",
  source: Some(tonic::transport::Error(Transport, hyper::Error(Io, Kind(ConnectionReset)))) }
```

`wait_ready` only probes the BFF's `/healthz` (port `grpc + 10`); the gRPC listener is a
*different* listener, so "HTTP ready" is not "gRPC ready". This is a harness race, not a product
defect, and it is unrelated to §14/§15 — these tests pass `--data-dir` explicitly, so
`apply_cli_overrides` receives an identical `Some(tmpdir)` before and after that change.

*Observed rate locally:* 1 failure in 30 consecutive runs — and that failure was a different
symptom of the same readiness/liveness family, `DeadlineExceeded: raft auth write timed out (no
quorum?)` under CPU contention (the "假红" class §13 already describes).

Hardened: the readiness-sensitive first `Authenticate` in each of the three tests now goes
through `authenticate_ready()`, which treats "the first RPC succeeds" as the real readiness
condition. It retries only **transient** errors (transport / `UNAVAILABLE` / `DEADLINE_EXCEEDED`)
within a 30 s budget, and on failure prints the **server log tail**, so the next occurrence can be
told apart from "the server started and then exited". No assertion was weakened: a persistent
failure still fails the test.

*Still open:* `find_free_port()` is a TOCTOU helper (bind `:0` → read port → close) and
`grpc + 10` is never checked for availability, so two concurrent tests can still be handed
overlapping ports. A deterministic fix would hold the listener until the child binds, or retry the
whole spawn when a bind fails. Not done here.

---

## CI

`.github/workflows/ci.yml` now installs `protoc`, pins the toolchain to `1.98.1`, runs
`buf format -w` before `buf breaking`, pins `pnpm`, fixes the `concurrency` group, has a
gate self-check (inject a fmt violation, assert the job goes red), and runs the
cross-language error-code contract check (`scripts/check-error-code-contract.sh`).

**The first full CI runs have now happened** (`e2ab961` baseline, then `45e7c09`, `5097072`,
`f3e33de`), so the review's rule *"在成功跑过至少一次全量之前，不接受任何『门禁已通过』的表述"*
is now satisfied for the jobs listed below — and the runs immediately did their job by
exposing real defects:

| job | baseline `e2ab961` | now |
| --- | --- | --- |
| `fmt + clippy -D warnings` | red | green |
| `proto contract (buf lint + breaking)` | red | green |
| `workspace tests` | red | green |
| `plugin engine feature matrix` | red | green |
| `java example integration (real server + agent)` | red | green (`f3e33de`) |
| `gate self-check` | — | green |
| `frontend lint (coord-ui)` | red (never passed) | fixed: see §11 |
| `cargo audit + deny` | red | fixed: see §10 |
| `java sdk (maven verify)` | green | red once via a flaky test: see §12 |
| `real-process chaos (nightly + PR gate)` | never ran (its first step failed on an empty `toolchain` input) | first real run at `5097072`; `plugin_real_agent_process_e2e` failed on the `--data-dir` defect (§14, fixed); once step 11 passed, steps 12–13 ran for the first time and step 13 exposed the empty-`RoleCache` window (§15, fixed) |
| `weekly perf baseline` | skipped on PRs | skipped on PRs (by design) |

Notable: the baseline's `real-process chaos` job failed at `dtolnay/rust-toolchain@master`
with `'toolchain' is a required input`, i.e. **the chaos suites had never executed** before
`5097072`. Its first real execution is what produced §13.

`f3e33de` also fixed a **P0 availability regression introduced by round 4**: adding
`/coord.watch.Watch/Watch` to the agent's body-buffering set made watch (a *streaming* RPC)
hang forever with no error to the client. See §7 for the probe, and the top banner for the
follow-on finding that Watch's scope check is fail-closed (a functional limitation for
scope-restricted roles, not a bypass).

---

## How to use this file

* If you are evaluating Coord for adoption: every "NOT closed" entry above is a reason to
  keep your own acceptance test, per §7 of the review. The review's §5.1 checklist remains a
  suitable skeleton.
* If you are working on Coord: pick an entry, replace it with a verifiable fact, delete the
  entry. Do not soften an entry to make it read better.
