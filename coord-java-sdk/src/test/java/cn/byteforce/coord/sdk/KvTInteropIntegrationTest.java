package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.kv.v2.KvClient;
import cn.byteforce.coord.sdk.txn.v2.TxnClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Cross-language interop with the Rust {@code coord-client} on one real v2
 * cluster (#172 residual).
 *
 * <p>The two languages never share memory: each direction hands over the exact
 * {@code commit_ts} it used, and the other side reads at that {@code read_ts}.
 * Values and {@code mod_revision} must match, otherwise one of the two clients
 * is not speaking {@code revision == commit_ts} (ADR-0015 D3/D12).
 *
 * <p>Driven by {@code scripts/check-kvt-interop.sh}, which runs the Rust phase,
 * then this suite, then the reverse Rust phase. Not part of {@code -Pit}: the
 * evidence files come from that script, so including it in the CI integration
 * job would only report "missing evidence" as a red build. Required
 * environment:
 * <ul>
 *   <li>{@code COORD_INTEROP_EVIDENCE_DIR} — directory the Rust phase wrote to
 *       (and this suite writes the reverse direction into);</li>
 *   <li>{@code COORD_INTEROP_RUN_ID} — key namespace shared with the Rust
 *       phase;</li>
 *   <li>{@code COORD_V2_AGENT_HOST} / {@code COORD_V2_AGENT_PORT} — the v2
 *       dev cluster agent (default {@code localhost:19637}).</li>
 * </ul>
 */
@DisplayName("KV-T v2 — Rust ↔ Java interop on one cluster")
class KvTInteropIntegrationTest {

    private static final String RUST_EVIDENCE = "rust-to-java.properties";
    private static final String JAVA_EVIDENCE = "java-to-rust.properties";

    private static Path evidenceDir;
    private static String runId;
    private static CoordClient client;

    @BeforeAll
    static void setUp() {
        evidenceDir = requireEvidenceDir();
        runId = requireEnv("COORD_INTEROP_RUN_ID");
        String host = env("COORD_V2_AGENT_HOST", "localhost");
        int port = Integer.parseInt(env("COORD_V2_AGENT_PORT", "19637"));
        client = CoordClient.create(CoordConfig.builder()
                .agentHost(host)
                .agentPort(port)
                .requestTimeout(Duration.ofSeconds(10))
                .build());
        try {
            assertThat(client.tsoV2().getTimestamp()).isPositive();
        } catch (RuntimeException e) {
            client.close(Duration.ZERO);
            throw new IllegalStateException(
                    "coord-agent v2 cluster is not usable at " + host + ":" + port
                            + "; start one with `bash scripts/ci-java-it-cluster.sh start v2`", e);
        }
    }

    @AfterAll
    static void tearDown() {
        if (client != null) {
            client.close(Duration.ofSeconds(5));
        }
    }

    @Test
    @DisplayName("Java reads the Rust transaction at its exact commit_ts")
    void readsWhatRustWrote() throws IOException {
        Properties rust = load(evidenceDir.resolve(RUST_EVIDENCE));
        String prefix = required(rust, "prefix.txn");
        long txnCommitTs = Long.parseLong(required(rust, "txn.commit_ts"));
        long directCommitTs = Long.parseLong(required(rust, "direct.commit_ts"));

        // One key of the Rust transaction: value and revision come from the same
        // commit_ts on both sides.
        for (String suffix : List.of("a", "b")) {
            String key = required(rust, "txn.key." + suffix);
            String expected = required(rust, "txn.value." + suffix);
            KvClient.RangeResult atCommit = client.kvV2()
                    .range(utf8(key), new byte[0], 0, txnCommitTs, false, false);
            assertThat(atCommit.readTs()).isEqualTo(txnCommitTs);
            assertThat(atCommit.kvs()).as("key %s visible at read_ts=%s", key, txnCommitTs)
                    .hasSize(1);
            KvClient.KeyValue stored = atCommit.kvs().get(0);
            assertThat(text(stored.value())).isEqualTo(expected);
            assertThat(stored.modRevision()).isEqualTo(txnCommitTs);
            assertThat(stored.createRevision()).isEqualTo(txnCommitTs);
            assertThat(stored.version()).isEqualTo(1);
        }

        // The prefix read at the same read_ts is exactly the Rust write set:
        // a transaction writes all of its keys at one commit_ts.
        KvClient.RangeResult range = client.kvV2()
                .range(utf8(prefix), prefixEnd(prefix), 0, txnCommitTs, false, false);
        assertThat(range.kvs()).as("prefix %s at read_ts=%s", prefix, txnCommitTs).hasSize(2);
        assertThat(range.kvs())
                .allSatisfy(stored -> assertThat(stored.modRevision()).isEqualTo(txnCommitTs));

        // A direct put (no transaction) carries its own commit_ts.
        KvClient.RangeResult direct = client.kvV2().range(
                utf8(required(rust, "direct.key")), new byte[0], 0, directCommitTs, false, false);
        assertThat(direct.kvs()).hasSize(1);
        assertThat(text(direct.kvs().get(0).value())).isEqualTo(required(rust, "direct.value"));
        assertThat(direct.kvs().get(0).modRevision()).isEqualTo(directCommitTs);

        // Rust observed its own transaction on coord.watch.v2 as one atomic batch
        // at the same commit_ts the Java SDK just read.
        assertThat(Long.parseLong(required(rust, "watch.commit_ts")))
                .as("Watch batch commit_ts == the transaction commit_ts both sides see")
                .isEqualTo(txnCommitTs);
    }

    @Test
    @DisplayName("Java writes a transaction + a direct put for Rust to read")
    void writesForRustToRead() throws IOException {
        String root = "/interop/" + runId + "/java/";
        String txnPrefix = root + "txn/";
        String keyA = txnPrefix + "a";
        String keyB = txnPrefix + "b";
        String directKey = root + "direct/k";

        long directCommitTs = client.tsoV2().getTimestamp();
        KvClient.PutResult direct = client.kvV2()
                .put(utf8(directKey), utf8("java-direct"), directCommitTs);
        assertThat(direct.commitTs()).isEqualTo(directCommitTs);

        TxnClient.Session txn = client.txnV2().begin();
        assertThat(txn.read(utf8(directKey)).readTs()).isEqualTo(txn.startTs());
        TxnClient.CommitResult commit = txn.commit(List.of(
                TxnClient.Mutation.put(utf8(keyA), utf8("java-txn-a")),
                TxnClient.Mutation.put(utf8(keyB), utf8("java-txn-b"))));
        assertThat(commit.committed())
                .as("a fresh transaction must not conflict")
                .isTrue();
        long txnCommitTs = commit.commitTs();
        assertThat(txnCommitTs).isGreaterThan(txn.startTs());

        // Assert locally before handing the evidence over: a Rust failure must
        // mean "the other client disagrees", not "this side wrote nothing".
        KvClient.RangeResult atCommit = client.kvV2()
                .range(utf8(txnPrefix), prefixEnd(txnPrefix), 0, txnCommitTs, false, false);
        assertThat(atCommit.kvs()).hasSize(2);
        assertThat(atCommit.kvs())
                .allSatisfy(stored -> assertThat(stored.modRevision()).isEqualTo(txnCommitTs));

        Properties evidence = new Properties();
        evidence.setProperty("run", runId);
        evidence.setProperty("prefix.txn", txnPrefix);
        evidence.setProperty("txn.key.a", keyA);
        evidence.setProperty("txn.value.a", "java-txn-a");
        evidence.setProperty("txn.key.b", keyB);
        evidence.setProperty("txn.value.b", "java-txn-b");
        evidence.setProperty("txn.commit_ts", Long.toString(txnCommitTs));
        evidence.setProperty("direct.key", directKey);
        evidence.setProperty("direct.value", "java-direct");
        evidence.setProperty("direct.commit_ts", Long.toString(directCommitTs));
        try (var writer = Files.newBufferedWriter(evidenceDir.resolve(JAVA_EVIDENCE),
                StandardCharsets.UTF_8)) {
            evidence.store(writer, "direction B: Java wrote, coord-client (Rust) reads");
        }
    }

    /** {@code prefix} 的区间上界（末字节 +1）；全 0xFF 时返回空 = 无上界。 */
    private static byte[] prefixEnd(String prefix) {
        byte[] bytes = prefix.getBytes(StandardCharsets.UTF_8);
        for (int i = bytes.length - 1; i >= 0; i--) {
            if ((bytes[i] & 0xFF) < 0xFF) {
                byte[] end = new byte[i + 1];
                System.arraycopy(bytes, 0, end, 0, i + 1);
                end[i] = (byte) (end[i] + 1);
                return end;
            }
        }
        return new byte[0];
    }

    private static Properties load(Path path) throws IOException {
        if (!Files.isRegularFile(path)) {
            throw new IllegalStateException("missing interop evidence " + path
                    + "; run `bash scripts/check-kvt-interop.sh` end-to-end");
        }
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
            properties.load(reader);
        }
        return properties;
    }

    private static String required(Properties properties, String name) {
        String value = properties.getProperty(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException("interop evidence lacks " + name);
        }
        return value;
    }

    private static Path requireEvidenceDir() {
        String raw = System.getenv("COORD_INTEROP_EVIDENCE_DIR");
        if (raw == null || raw.isBlank()) {
            throw new IllegalStateException(
                    "COORD_INTEROP_EVIDENCE_DIR is required: the Rust phase hands its commit_ts over "
                            + "through this directory (run `bash scripts/check-kvt-interop.sh`)");
        }
        return Path.of(raw);
    }

    private static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            throw new IllegalStateException(name + " is required; run "
                    + "`bash scripts/check-kvt-interop.sh` instead of invoking this suite directly");
        }
        return value.trim();
    }

    private static String env(String name, String fallback) {
        String raw = System.getenv(name);
        return (raw == null || raw.isBlank()) ? fallback : raw.trim();
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static String text(byte[] value) {
        return new String(value, StandardCharsets.UTF_8);
    }
}
