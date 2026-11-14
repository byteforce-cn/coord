package cn.byteforce.coord.sdk.mq.v2;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Contract-shape rules for the {@code sdk.mq.v2} model:
 * the wire carries {@code bytes} (never assume UTF-8) and caller-owned arrays /
 * collections must not become aliases into the request the SDK built.
 */
class MqV2ModelTest {

    private static byte[] bytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    @Test
    void topicConfigDefaultsAreServerDefaults() {
        MqV2TopicConfig config = MqV2TopicConfig.defaults();
        assertThat(config.retentionSecs()).isZero();
        assertThat(config.maxBytes()).isZero();
        assertThat(config.maxMessageBytes()).isZero();
        assertThat(config.segmentBytes()).isZero();
        assertThat(config.dlqRetentionSecs()).isZero();
    }

    @Test
    void topicDescriptionCopiesThePartitionList() {
        List<MqV2PartitionInfo> partitions = new ArrayList<>();
        partitions.add(new MqV2PartitionInfo(0, 1, "127.0.0.1:50051", 0, 10, 4096));

        MqV2TopicDescription description = new MqV2TopicDescription(
                "orders", 7, 2, 1, MqV2TopicConfig.defaults(), 1700000000000L, partitions);
        partitions.clear();

        assertThat(description.partitions()).hasSize(1);
        assertThat(description.partitions().get(0).leaderAddr()).isEqualTo("127.0.0.1:50051");
        // null list is normalised, not an NPE on every access
        assertThat(new MqV2GroupDescription("g", 1, MqV2GroupState.EMPTY, null, null)
                .members()).isEmpty();
    }

    @Test
    void publishCopiesKeyPayloadAndHeaders() {
        byte[] key = bytes("k");
        byte[] payload = bytes("payload");
        Map<String, String> headers = new HashMap<>();
        headers.put("ce-id", "1");

        MqV2Publish publish = new MqV2Publish("orders", 0, 3, key, payload, "idem", headers, "p");
        key[0] = 'X';
        payload[0] = 'X';
        headers.put("ce-id", "2");

        assertThat(publish.key()).isEqualTo(bytes("k"));
        assertThat(publish.payload()).isEqualTo(bytes("payload"));
        assertThat(publish.headers()).containsEntry("ce-id", "1");
        assertThat(publish.headers().getClass().getName()).doesNotContain("HashMap");
    }

    @Test
    void messageToStringDoesNotLeakThePayload() {
        MqV2Message message = new MqV2Message(
                "orders", 1, 42, 1700000000000L, new byte[0], bytes("secret-payload"),
                Map.of("ce-id", "1"), "producer", "", "");

        String rendered = message.toString();
        assertThat(rendered).doesNotContain("secret-payload");
        assertThat(rendered).contains("orders").contains("offset=42").contains("payloadBytes=14");
    }

    @Test
    void pollKeepsAnExplicitlyUnsetStartOffsetNull() {
        MqV2Poll poll = new MqV2Poll("orders", 0, 5, null, 10, 1024, 0);
        assertThat(poll.startOffset()).isNull();

        MqV2Poll explicit = new MqV2Poll("orders", 0, 5, 0L, 0, 0, 0);
        assertThat(explicit.startOffset()).isZero();
    }

    @Test
    void joinKeepsTheAssignmentViewImmutable() {
        List<MqV2Assignment> assignments = new ArrayList<>();
        assignments.add(new MqV2Assignment("orders", 0, 3));
        MqV2Joined joined = new MqV2Joined("m-1", 9, assignments, 3000, 30000);
        assignments.clear();

        assertThat(joined.assignments()).hasSize(1);
        assertThat(joined.assignments().get(0).committedOffset()).isEqualTo(3);
    }

    @Test
    void dlqMessageKeepsTheOriginalOffsetAndReason() {
        MqV2Message original = new MqV2Message("orders", 0, 11, 1L, new byte[0], new byte[0],
                Map.of(), "", "poison", "decode failed");
        MqV2DlqMessage dlq = new MqV2DlqMessage(original, "poison", "decode failed");

        assertThat(dlq.message().offset()).isEqualTo(11);
        assertThat(dlq.reason()).isEqualTo("poison");
        assertThat(dlq.detail()).isEqualTo("decode failed");
    }

    @Test
    void groupAndDeletionEnumsCarryTheContractStates() {
        assertThat(MqV2GroupState.values()).containsExactly(
                MqV2GroupState.UNSPECIFIED, MqV2GroupState.EMPTY, MqV2GroupState.STABLE,
                MqV2GroupState.PREPARING_REBALANCE, MqV2GroupState.DEAD);
        assertThat(MqV2DeletionState.values()).containsExactly(
                MqV2DeletionState.UNSPECIFIED, MqV2DeletionState.PENDING,
                MqV2DeletionState.DONE, MqV2DeletionState.FAILED);
    }
}
