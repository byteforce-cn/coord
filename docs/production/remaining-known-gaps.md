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
| **Spring** | **Product decision: no Spring Boot starter will be restored.** The SDK is the supported integration surface; adopters wire `@Bean`s and own the lifecycle | recorded in `CONTRIBUTING.md`; `README` capability table |
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

#### 7. Unbounded growth that was not touched

Ranked by the review's §3.10; all of these are self-consistent with the "declared but not
enforced" theme of C15:

* `MemoryWorkflowStore` + a 5-second full scan (the heaviest agent-side growth).
* Dynamic region cleanup is not wired; adding/removing regions is effectively unsupported.
* `snapshot_logs_since_last = 0` semantics (following the documentation breaks it).
* Connection-count limits and concurrency protection.

#### 8. Doc references that cannot be committed — **FIXED**

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

---

## CI

`.github/workflows/ci.yml` now installs `protoc`, pins the toolchain to `1.98.1`, runs
`buf format -w` before `buf breaking`, pins `pnpm`, fixes the `concurrency` group, has a
gate self-check (inject a fmt violation, assert the job goes red), and runs the
cross-language error-code contract check (`scripts/check-error-code-contract.sh`).

**No CI run of the round-4 code has been observed yet.** The review's rule stands: *"在成功
跑过至少一次全量之前，不接受任何『门禁已通过』的表述。"* The first green full run — and
only that — is what closes this.

---

## How to use this file

* If you are evaluating Coord for adoption: every "NOT closed" entry above is a reason to
  keep your own acceptance test, per §7 of the review. The review's §5.1 checklist remains a
  suitable skeleton.
* If you are working on Coord: pick an entry, replace it with a verifiable fact, delete the
  entry. Do not soften an entry to make it read better.
