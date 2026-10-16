# coord-java-sdk

Java SDK for the Coord platform — the agent-side service surfaces (registry,
config, lock, election, idgen, event, cache, MQ, scheduler, workflow, policy,
PKI, transit, circuit breaker, rate limiter, feature flags, object store).
Maven coordinates: `cn.byteforce:coord-java-sdk` (Java 21+).

> This README is a starting point only. The **normative consumer semantics**
> (per-service consistency / routing / failure rules, capabilities, accepted
> boundaries, canonical patterns) live in
> [`docs/production/consumer-contract.md`](../docs/production/consumer-contract.md);
> wire facts live in `apis/contracts/proto/`. When in doubt, those win.

## Requirements

- Java 21+ (the SDK is compiled with `--release 21`).
- Maven 3.9+.
- A reachable **agent** endpoint (`agentHost` / `agentPort` in `CoordConfig`;
  the local agent default is `127.0.0.1:19527`).

## Dependency compatibility

The SDK manages its gRPC / protobuf transitive dependencies via
`grpc-bom` / `protobuf-bom` (dependency management — no scattered pins).
The tested compatibility range:

| Dependency | Built & tested with | Also tested (consumer-platform combo) |
|:--|:--|:--|
| `io.grpc:*` | **1.68.0** (repo default) | **1.83.1** |
| `com.google.protobuf:protobuf-java` | **4.28.0** (repo default) | **4.35.1** |

The CI runs `mvn verify` twice — once with the repo pins, once with the
tested-upper combo — so "compiles against a newer platform BOM" is a machine
fact, not an assumption. Consumers who need a specific combination can align
via their own BOM imports; within the range above both compile and pass the
SDK test suite. **Outside the range is not covered**: if you hit behaviour
suspicious of a gRPC/protobuf mismatch, pin back to a tested combo first.

**Shading / relocation** (evaluated, not adopted): the SDK's **public API does
not expose gRPC or protobuf types** (all public signatures use SDK types), so
consumers can freely align versions through BOMs without a shaded artifact;
shading would only add relocation complexity. Re-evaluate if a future API ever
leaks wire types into the public surface.

## Install

Available on **Maven Central** — repository version `0.3.1`
(latest published release: `0.3.1`; alternatively use the from-source path below):

```xml
<dependency>
    <groupId>cn.byteforce</groupId>
    <artifactId>coord-java-sdk</artifactId>
    <version>0.3.1</version>
</dependency>
```

Or build & install from source (repository root):

```bash
mvn -pl coord-java-sdk install
```

## Quick start

```java
import cn.byteforce.coord.sdk.CoordClient;
import cn.byteforce.coord.sdk.CoordConfig;

CoordConfig config = CoordConfig.builder()
        .agentHost("127.0.0.1")
        .agentPort(19527)
        // Production: plaintext to a non-loopback agent is refused (fail-closed);
        // .useTls(true).tlsCaCertPath("/etc/coord/ca.pem")
        // CCT credentials are read per call, so refresh needs no channel rebuild
        // .authTokenSupplier(() -> credentialStore.currentCct())
        .build();

try (CoordClient client = CoordClient.create(config)) {
    // Config center
    client.configClient().put("/app/config", "value");
    String val = client.configClient().getString("/app/config").orElse(null);

    // MQ — correctness path is poll + ack (at-least-once)
    client.mq().createTopic("orders", 4);
    client.mq().publish("orders", 0, null, "payload".getBytes());
    for (var m : client.mq().poll("orders", 0, "orders-svc", 0, 100)) {
        // process(m) — keep it idempotent; duplicates are normal
        client.mq().ack("orders", "orders-svc", m.partition(), m.offset());
    }
}
```

## Client surfaces

The SDK covers the **agent-side service surfaces**; the server KV data plane is
served by the Rust `coord-client`.

| Surface | Entry point | Notes |
|:--|:--|:--|
| Registry / Config | `client.registry()`, `client.configClient()` | service discovery / config center; config resync = `listWithRevision` + `watchFrom` (snapshot position closes the delete window) |
| Lock / LeaderElection / IdGen | `client.lock()`, `client.election()`, `client.idgen()` | lease-based lock; global ids |
| Event | `client.events()` | live by default; pass a persisted `seq` cursor to replay within the retention window |
| Cache | `client.cache()` | capacity is per-agent, converged periodically; `setIfAbsent` (SETNX), `keys`, `clear` |
| MQ | `client.mq()` | `poll+ack` reliability path; leader routing via `getTopicLeader` |
| Scheduler / Workflow | `client.scheduler()`, `client.workflow()` | claim-based jobs; completion results persist (first result wins) and read back via `getJob`; workflow retention via `deleteInstance` / `deleteDefinition` |
| Policy | `client.policy()` | `checkPermission` is agent-local RBAC; production path is OPA bundle + `evaluate` |
| PKI | `client.pki()` | `getCertByCN` returns the **private key** — treat "read" as private-key read |
| Transit | `client.transit()` | envelope encryption + purpose-bound HMAC (`hmacSignWithPurpose` / `hmacVerifyWithPurpose`; persist the returned `keyId`); `rewrap` migrates DEKs during KEK rotation |
| CircuitBreaker / RateLimiter / FeatureFlags | `client.circuitBreaker()`, `client.rateLimiter()`, `client.featureFlags()` | breaker/limiter are local (per-agent); feature flags live in the shared KV — `putFlag` / `putPercentageFlag` / `deleteFlag` / `listFlags` (management plane; takes effect immediately) |
| ObjectStore | `client.objectStore()` | see `docs/production/volume-object-storage.md` |
| Health | `client.healthCheck()` | agent health check |

## Errors

All failures surface as `CoordException` carrying a structured `ErrorCode`
(read from the `x-coord-error-code` trailer — never parse message text):

```java
try {
    client.mq().publish("orders", 0, null, bytes);
} catch (CoordException e) {
    switch (e.getErrorCode()) {
        case NOT_LEADER      -> routeTo(e.getLeaderHint());   // re-route, don't parse text
        case UNAVAILABLE,
             DEADLINE_EXCEEDED -> retryBackoff();
        case RESOURCE_EXHAUSTED -> backpressure();            // do NOT retry
        default -> throw e;
    }
}
```

Routing errors carry a `coord-leader-hint` trailer, exposed as
`CoordException#getLeaderHint()`. Retry discipline per error code is tabulated
in the consumer contract (`§0.2`).

## Capabilities

The server enforces per-RPC capabilities (`coord:mq:consume`,
`coord:policy:manage`, …) from the client's CCT. The authoritative mapping is
`coord-core/src/grpc_auth.rs::rpc_capability`; the consumer contract lists the
consumer-facing view per service. A missing capability fails with
`PERMISSION_DENIED` — that is an authorization change, not a retry.

## Tests

- Unit tests: `mvn test`.
- Integration suites (real server + agent): `mvn -Pit` — see
  `.github/workflows/ci.yml` for the authoritative invocation.
- Note: local runs require a JDK the test stack supports (the repo CI uses
  Java 21; Mockito-based suites may not run on much newer JDKs).
