package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.kv.v2.KvClient;
import cn.byteforce.coord.sdk.lease.v2.LeaseClient;
import cn.byteforce.coord.sdk.storage.v2.StorageClient;
import cn.byteforce.coord.sdk.tso.v2.TsoClient;
import cn.byteforce.coord.sdk.txn.v2.TxnClient;
import cn.byteforce.coord.sdk.watch.v2.WatchClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowableOfType;

/**
 * KV-T v2 SDK against a real {@code coord server} + {@code coord agent}.
 *
 * <p>The other v2 suites ({@code KvTV2ClientTest}, {@code TxnWatchStorageV2ClientTest})
 * run against a fake Netty server: they pin the wire/trailer contract but say
 * nothing about the real data plane (multi-raft regions, sequencer, watch
 * coordinator, object manifests). This suite closes that gap through the public
 * SDK surface and the agent proxy.
 *
 * <p>The cluster is started by {@code scripts/ci-java-it-cluster.sh start v2};
 * the endpoint is {@code COORD_V2_AGENT_HOST:COORD_V2_AGENT_PORT}. A missing v2
 * cluster is a hard failure — {@code Assumptions} would make "not run"
 * indistinguishable from "passed".
 *
 * <p>Only under {@code -Pit}: the default Surefire profile excludes
 * {@code *IntegrationTest}.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@DisplayName("KV-T v2 SDK — real cluster through the agent")
class KvTV2ClusterIntegrationTest {

    /** Unique per JVM run: the dev cluster is reused across CI/local runs. */
    private static final String RUN = "it/" + System.currentTimeMillis() + "/";

    private static CoordClient client;

    @BeforeAll
    static void setUp() {
        String host = env("COORD_V2_AGENT_HOST", "localhost");
        int port = Integer.parseInt(env("COORD_V2_AGENT_PORT", "19637"));
        client = CoordClient.create(CoordConfig.builder()
                .agentHost(host)
                .agentPort(port)
                .requestTimeout(Duration.ofSeconds(10))
                .build());
        // Probe before the first assertion so "cluster missing" reports the
        // startup command instead of a bare UNAVAILABLE from an arbitrary RPC.
        try {
            assertThat(client.tsoV2().getTimestamp()).isPositive();
        } catch (RuntimeException e) {
            client.close(Duration.ZERO);
            throw new IllegalStateException(
                    "coord-agent v2 cluster is not usable at " + host + ":" + port
                            + ". Start one with "
                            + "`bash scripts/ci-java-it-cluster.sh start v2` (or set "
                            + "COORD_V2_AGENT_HOST/COORD_V2_AGENT_PORT).", e);
        }
    }

    @AfterAll
    static void tearDown() {
        if (client != null) {
            client.close(Duration.ofSeconds(5));
        }
    }

    @Test
    @Order(1)
    @DisplayName("TSO is monotonic; commit_ts becomes mod_revision; prev_kv/delete")
    void tsoAndKvRoundTrip() {
        long first = client.tsoV2().getTimestamp();
        long second = client.tsoV2().getTimestamp();
        assertThat(second)
                .as("GetTs must strictly increase")
                .isGreaterThan(first);

        TsoClient.TimestampBatch batch = client.tsoV2().allocate(8);
        assertThat(batch.endTs()).isEqualTo(batch.startTs() + 8);
        // Explicit allocations reserve disjoint ranges; the local GetTs cache is
        // an independent (older) batch, so only batch-to-batch monotonicity is
        // guaranteed here.
        TsoClient.TimestampBatch next = client.tsoV2().allocate(8);
        assertThat(next.startTs())
                .as("explicit timestamp batches must not overlap or go backwards")
                .isGreaterThanOrEqualTo(batch.endTs());

        byte[] key = runKey("tso-kv");
        KvClient.PutResult put = client.kvV2().put(key, utf8("v1"), first);
        assertThat(put.commitTs()).isEqualTo(first);

        KvClient.RangeResult atCommit = client.kvV2()
                .range(key, new byte[0], 0, first, false, false);
        assertThat(atCommit.readTs()).isEqualTo(first);
        assertThat(atCommit.kvs()).hasSize(1);
        KvClient.KeyValue stored = atCommit.kvs().get(0);
        assertThat(text(stored.value())).isEqualTo("v1");
        assertThat(stored.modRevision()).isEqualTo(first);
        assertThat(stored.version()).isEqualTo(1);

        long overwriteTs = client.tsoV2().getTimestamp();
        KvClient.PutResult overwrite = client.kvV2()
                .put(key, utf8("v2"), overwriteTs, true, new byte[0]);
        assertThat(overwrite.prevKv()).isNotNull();
        assertThat(text(overwrite.prevKv().value())).isEqualTo("v1");

        long deleteTs = client.tsoV2().getTimestamp();
        KvClient.DeleteResult deleted = client.kvV2()
                .delete(key, new byte[0], deleteTs, false, new byte[0]);
        assertThat(deleted.deleted()).isEqualTo(1);
        assertThat(client.kvV2().range(key, new byte[0], 0, deleteTs, false, false).kvs())
                .isEmpty();
    }

    @Test
    @Order(2)
    @DisplayName("a cross-region transaction is one atomic Watch event")
    void crossRegionTransactionIsOneWatchBatch() throws InterruptedException {
        // Start the subscription at a revision that does not exist yet: the
        // stream may be established after the commit and still replays it,
        // so the test never races the watch coordinator with a sleep.
        long base = client.tsoV2().getTimestamp();
        LinkedBlockingQueue<WatchClient.Batch> batches = new LinkedBlockingQueue<>();
        AtomicReference<CoordException> watchError = new AtomicReference<>();
        WatchClient.Subscription subscription = client.watchV2().watch(
                WatchClient.Options.key(utf8("a/"))
                        .range(utf8("z0"))
                        .fromRevision(base + 1)
                        .prevKv(true),
                new WatchClient.Listener() {
                    @Override
                    public void onBatch(WatchClient.Batch batch) {
                        batches.add(batch);
                    }

                    @Override
                    public void onError(CoordException error) {
                        watchError.set(error);
                    }
                });
        try {
            byte[] left = runKey("a/cross");
            byte[] right = runKey("z/cross");
            TxnClient.Session txn = client.txnV2().begin();
            TxnClient.CommitResult committed = txn.commit(List.of(
                    TxnClient.Mutation.put(left, utf8("left")),
                    TxnClient.Mutation.put(right, utf8("right"))));
            assertThat(committed.committed()).isTrue();
            long commitTs = committed.commitTs();
            assertThat(commitTs).isGreaterThan(base);

            WatchClient.Event event = awaitEvent(batches, watchError, commitTs, 15);
            assertThat(event.commitTs()).isEqualTo(commitTs);
            assertThat(keys(event)).containsExactlyInAnyOrder(text(left), text(right));

            // read_ts = commit_ts waits for safe_ts, so both regions are visible
            // under the same snapshot even though apply is asynchronous.
            KvClient.RangeResult snapshot = client.kvV2()
                    .range(utf8("a/"), utf8("z0"), 0, commitTs, false, false);
            assertThat(keys(snapshot.kvs())).contains(text(left), text(right));
        } finally {
            subscription.close();
        }
    }

    @Test
    @Order(3)
    @DisplayName("OCC conflict is a CommitResult, not an exception")
    void occConflictIsReturnedNotThrown() {
        byte[] key = runKey("a/conflict");
        TxnClient.Session reader = client.txnV2().begin();
        reader.read(key);

        TxnClient.Session writer = client.txnV2().begin();
        TxnClient.CommitResult written = writer.commit(
                List.of(TxnClient.Mutation.put(key, utf8("new"))));
        assertThat(written.committed()).isTrue();
        // Await visibility before any later direct v2 write: a direct write whose
        // commit_ts is above this pending decision would advance the Region
        // revision past it (see the direct-write/pending-decision issue).
        KvClient.RangeResult visible = client.kvV2()
                .range(key, new byte[0], 0, written.commitTs(), false, false);
        assertThat(visible.kvs()).hasSize(1);
        assertThat(text(visible.kvs().get(0).value())).isEqualTo("new");

        TxnClient.CommitResult conflict = reader.commit(
                List.of(TxnClient.Mutation.put(key, utf8("mine"))));
        assertThat(conflict.committed()).isFalse();
        assertThat(conflict.commitTs()).isZero();

        reader.abort();
    }

    @Test
    @Order(4)
    @DisplayName("lease revoke atomically deletes bound keys")
    void leaseRevokeDeletesBoundKeys() {
        LeaseClient.Grant grant = client.leaseV2().grant(60);
        assertThat(grant.leaseId()).isPositive();

        byte[] key = runKey("z/leased");
        long putTs = client.tsoV2().getTimestamp();
        client.kvV2().putWithLease(key, utf8("leased"), grant.leaseId(), putTs,
                false, new byte[0]);

        LeaseClient.State state = client.leaseV2().timeToLive(grant.leaseId(), true);
        assertThat(state.ttlSeconds()).isPositive();
        assertThat(state.keys()).anyMatch(bound -> Arrays.equals(bound, key));

        long revokeTs = client.leaseV2().revoke(grant.leaseId(), new byte[0]);
        assertThat(revokeTs)
                .as("a granted lease with bound keys must produce a deletion decision")
                .isPositive();
        assertThat(client.kvV2().range(key, new byte[0], 0, revokeTs, false, false).kvs())
                .isEmpty();
    }

    @Test
    @Order(5)
    @DisplayName("storage round trip through the v2 object manifest")
    void storageRoundTripAndDuplicatePutIsRejected() {
        String bucket = "it-bucket";
        byte[] objectId = runKey("storage-object");
        byte[] payload = utf8("hello kv-t v2 storage");

        StorageClient.PutResult put = client.storageV2().put(bucket, objectId, payload);
        assertThat(put.commitTs()).isPositive();
        assertThat(put.size()).isEqualTo(payload.length);

        StorageClient.ObjectData fetched = client.storageV2().get(bucket, objectId);
        assertThat(fetched.data()).isEqualTo(payload);
        assertThat(fetched.stat().exists()).isTrue();
        assertThat(fetched.stat().commitTs()).isEqualTo(put.commitTs());

        CoordException duplicate = catchThrowableOfType(CoordException.class,
                () -> client.storageV2().put(bucket, objectId, payload));
        assertThat(duplicate).isNotNull();
        assertThat(duplicate.getErrorCode())
                .as("v2 does not support overwrite: the original code must survive the agent")
                .isEqualTo(ErrorCode.ALREADY_EXISTS);

        StorageClient.DeleteResult deleted = client.storageV2()
                .delete(bucket, objectId, new byte[0]);
        assertThat(deleted.deleted()).isTrue();
        assertThat(client.storageV2().stat(bucket, objectId).exists()).isFalse();
    }

    @Test
    @Order(6)
    @DisplayName("a range read fans out across regions")
    void rangeSpansRegions() {
        byte[] left = runKey("a/range");
        byte[] right = runKey("z/range");
        long leftTs = client.tsoV2().getTimestamp();
        client.kvV2().put(left, utf8("l"), leftTs);
        long rightTs = client.tsoV2().getTimestamp();
        client.kvV2().put(right, utf8("r"), rightTs);

        KvClient.RangeResult ranged = client.kvV2()
                .range(utf8("a/"), utf8("z0"), 0, rightTs, false, false);
        assertThat(keys(ranged.kvs())).contains(text(left), text(right));
    }

    private static WatchClient.Event awaitEvent(
            LinkedBlockingQueue<WatchClient.Batch> batches,
            AtomicReference<CoordException> error,
            long commitTs,
            int timeoutSeconds) throws InterruptedException {
        long deadline = System.nanoTime() + Duration.ofSeconds(timeoutSeconds).toNanos();
        while (System.nanoTime() < deadline) {
            CoordException failure = error.get();
            if (failure != null) {
                throw new AssertionError("watch ended before commit_ts " + commitTs, failure);
            }
            WatchClient.Batch batch = batches.poll(500, TimeUnit.MILLISECONDS);
            if (batch == null) {
                continue;
            }
            for (WatchClient.Event event : batch.events()) {
                if (event.commitTs() == commitTs) {
                    return event;
                }
            }
        }
        throw new AssertionError("no watch event for commit_ts " + commitTs);
    }

    private static List<String> keys(WatchClient.Event event) {
        List<String> keys = new ArrayList<>();
        for (WatchClient.Change change : event.changes()) {
            keys.add(text(change.key()));
        }
        return keys;
    }

    private static List<String> keys(List<KvClient.KeyValue> values) {
        List<String> keys = new ArrayList<>();
        for (KvClient.KeyValue value : values) {
            keys.add(text(value.key()));
        }
        return keys;
    }

    /** Data-plane key under the per-run namespace (no cross-run leftovers). */
    private static byte[] runKey(String suffix) {
        return utf8(RUN + suffix);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }

    private static String env(String key, String fallback) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? fallback : value.trim();
    }
}
