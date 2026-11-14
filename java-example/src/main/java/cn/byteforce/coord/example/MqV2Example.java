package cn.byteforce.coord.example;

import cn.byteforce.coord.contracts.mq.v2.AckEntry;
import cn.byteforce.coord.contracts.mq.v2.AckRequest;
import cn.byteforce.coord.contracts.mq.v2.CreateTopicRequest;
import cn.byteforce.coord.contracts.mq.v2.CreateTopicResponse;
import cn.byteforce.coord.contracts.mq.v2.DeleteTopicRequest;
import cn.byteforce.coord.contracts.mq.v2.DescribeTopicRequest;
import cn.byteforce.coord.contracts.mq.v2.GetTopicDeletionRequest;
import cn.byteforce.coord.contracts.mq.v2.GetTopicDeletionResponse;
import cn.byteforce.coord.contracts.mq.v2.JoinGroupRequest;
import cn.byteforce.coord.contracts.mq.v2.JoinGroupResponse;
import cn.byteforce.coord.contracts.mq.v2.LeaveGroupRequest;
import cn.byteforce.coord.contracts.mq.v2.MQGrpc;
import cn.byteforce.coord.contracts.mq.v2.MoveToDlqRequest;
import cn.byteforce.coord.contracts.mq.v2.PollDlqRequest;
import cn.byteforce.coord.contracts.mq.v2.PollDlqResponse;
import cn.byteforce.coord.contracts.mq.v2.PollRequest;
import cn.byteforce.coord.contracts.mq.v2.PollResponse;
import cn.byteforce.coord.contracts.mq.v2.PublishRequest;
import cn.byteforce.coord.contracts.mq.v2.PublishResponse;
import cn.byteforce.coord.contracts.mq.v2.TopicConfig;
import cn.byteforce.coord.contracts.mq.v2.TopicDeletionState;
import cn.byteforce.coord.contracts.mq.v2.UpdateTopicRequest;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;

/**
 * MQ v2 示例（{@code coord.mq.v2}）：建 topic → 发布（含幂等重放）→ 组消费并
 * Ack → 移入 DLQ 并读回 → 更新 → 删除并等回收完成。
 *
 * <p>只用 gRPC + proto stub，与 {@link KvTV2Example} 同口径：经 **Agent** 的
 * {@code MqV2Proxy} 转发（分片 Leader 路由由 Agent 承担，示例不缓存服务端地址）。
 *
 * <p><b>为什么必须连真实 v2 集群</b>：{@code [mq_v2].enabled=false}（默认）时
 * 服务端对每个 RPC 返回 {@code FAILED_PRECONDITION}；v1 agent 不代理
 * {@code coord.mq.v2} 时返回 {@code UNIMPLEMENTED}。两种都按退出码 2 硬失败——
 * 不提供"降级到 MQ v1"的分支，静默降级会把"v2 未验证"伪装成"v2 通过"。
 *
 * <p>运行方式见 {@code java-example/README.md}
 * （{@code bash java-example/scripts/run-v2-example.sh mq}）。退出码：
 * <ul>
 *   <li>0 = 全链路通过；</li>
 *   <li>1 = 断言失败或 RPC 报错；</li>
 *   <li>2 = 目标集群未启用 MQ v2（或 agent 未代理 v2）。</li>
 * </ul>
 */
public final class MqV2Example {

    private static final int EXIT_NOT_V2 = 2;
    private static final int EXIT_FAILED = 1;
    private static final String TOPIC = "example-mq-v2-" + System.currentTimeMillis();
    private static final String GROUP = "example-group-" + System.currentTimeMillis();

    private MqV2Example() {
    }

    public static void main(String[] args) {
        String host = env("COORD_V2_AGENT_HOST", "localhost");
        int port = Integer.parseInt(env("COORD_V2_AGENT_PORT", "19637"));
        int rc;
        try {
            rc = run(host, port);
        } catch (NotV2ClusterException e) {
            System.err.println("[mq-v2-example] FAIL (MQ v2 is not usable here): " + e.getMessage());
            System.err.println("[mq-v2-example] start one with "
                    + "`bash scripts/ci-java-it-cluster.sh start v2` and retry");
            rc = EXIT_NOT_V2;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            System.err.println("[mq-v2-example] FAIL (interrupted): " + e);
            rc = EXIT_FAILED;
        } catch (RuntimeException e) {
            System.err.println("[mq-v2-example] FAIL: " + e);
            rc = EXIT_FAILED;
        }
        System.exit(rc);
    }

    private static int run(String host, int port) throws InterruptedException {
        ManagedChannel channel = ManagedChannelBuilder.forAddress(host, port)
                .usePlaintext()
                .build();
        try {
            MQGrpc.MQBlockingStub mq = MQGrpc.newBlockingStub(channel)
                    .withDeadlineAfter(10, TimeUnit.SECONDS);

            // 1. 建 topic（request_id 幂等：重放返回同一身份）。
            CreateTopicRequest create = CreateTopicRequest.newBuilder()
                    .setTopic(TOPIC)
                    .setPartitions(1)
                    .setRequestId("example-" + TOPIC)
                    .setConfig(TopicConfig.newBuilder().setRetentionSecs(3600))
                    .build();
            CreateTopicResponse created = call(() -> mq.createTopic(create));
            assertEquals(TOPIC, created.getTopic(), "created topic");
            long epoch = created.getTopicEpoch();
            System.out.println("[mq-v2-example] topic " + TOPIC
                    + " id=" + created.getTopicId() + " epoch=" + epoch);

            // 2. 发布：幂等键重放返回首次 offset 且 deduplicated=true。
            PublishRequest publish = PublishRequest.newBuilder()
                    .setTopic(TOPIC).setPartition(0).setTopicEpoch(epoch)
                    .setKey(utf8("k"))
                    .setPayload(utf8("hello mq v2"))
                    .setIdempotencyKey("example-1")
                    .build();
            PublishResponse first = call(() -> mq.publish(publish));
            PublishResponse replay = call(() -> mq.publish(publish));
            assertEquals(first.getOffset(), replay.getOffset(), "idempotent replay offset");
            assertEquals(true, replay.getDeduplicated(), "replay deduplicated");
            System.out.println("[mq-v2-example] published offset=" + first.getOffset());

            // 3. 组消费：Join → Poll → Ack（至少一次；重复投递是正常形态）。
            JoinGroupResponse joined = call(() -> mq.joinGroup(JoinGroupRequest.newBuilder()
                    .setGroup(GROUP)
                    .addTopics(TOPIC)
                    .setSessionTimeoutMs(30000)
                    .build()));
            PollResponse polled = call(() -> mq.poll(PollRequest.newBuilder()
                    .setTopic(TOPIC).setPartition(0).setTopicEpoch(epoch)
                    .setStartOffset(0).setMaxMessages(10)
                    .build()));
            assertEquals(1, polled.getMessagesCount(), "polled messages");
            assertEquals("hello mq v2",
                    polled.getMessages(0).getPayload().toStringUtf8(), "payload");
            long acked = call(() -> mq.ack(AckRequest.newBuilder()
                    .setTopic(TOPIC).setPartition(0).setTopicEpoch(epoch)
                    .setGroup(GROUP).setMemberId(joined.getMemberId())
                    .setGeneration(joined.getGeneration())
                    .setOffset(polled.getMessages(0).getOffset())
                    .build())).getCommittedOffset();
            assertEquals(first.getOffset() + 1, acked, "committed offset");
            System.out.println("[mq-v2-example] consumed and acked committed=" + acked);

            // 4. 死信：移入 DLQ（幂等）后按原 offset + reason 读回。
            call(() -> mq.moveToDlq(MoveToDlqRequest.newBuilder()
                    .setTopic(TOPIC).setPartition(0).setTopicEpoch(epoch)
                    .setOffset(first.getOffset())
                    .setReason("example").setDetail("moved by MqV2Example")
                    .build()));
            PollDlqResponse dlq = call(() -> mq.pollDlq(PollDlqRequest.newBuilder()
                    .setTopic(TOPIC).setPartition(0).setTopicEpoch(epoch)
                    .setStartOffset(0).setMaxMessages(10)
                    .build()));
            assertEquals(1, dlq.getMessagesCount(), "dlq messages");
            assertEquals("example", dlq.getMessages(0).getDlqReason(), "dlq reason");
            assertEquals(first.getOffset(), dlq.getMessages(0).getOffset(), "original offset");
            System.out.println("[mq-v2-example] dlq entries=" + dlq.getMessagesCount());

            call(() -> mq.ackBatch(cn.byteforce.coord.contracts.mq.v2.AckBatchRequest.newBuilder()
                    .setGroup(GROUP).setMemberId(joined.getMemberId())
                    .setGeneration(joined.getGeneration())
                    .addAcks(AckEntry.newBuilder()
                            .setTopic(TOPIC).setPartition(0).setTopicEpoch(epoch)
                            .setOffset(first.getOffset()))
                    .build()));
            call(() -> mq.leaveGroup(LeaveGroupRequest.newBuilder()
                    .setGroup(GROUP).setMemberId(joined.getMemberId())
                    .setGeneration(joined.getGeneration())
                    .build()));

            // 5. 更新（epoch CAS）→ 删除 → 等回收完成。
            long updatedEpoch = call(() -> mq.updateTopic(UpdateTopicRequest.newBuilder()
                    .setTopic(TOPIC)
                    .setExpectedTopicEpoch(epoch)
                    .setConfig(TopicConfig.newBuilder().setRetentionSecs(7200))
                    .build())).getTopicEpoch();
            String deleteId = call(() -> mq.deleteTopic(DeleteTopicRequest.newBuilder()
                    .setTopic(TOPIC).setExpectedTopicEpoch(updatedEpoch)
                    .build())).getDeleteId();
            GetTopicDeletionResponse progress = null;
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
            while (System.nanoTime() < deadline) {
                progress = call(() -> mq.getTopicDeletion(GetTopicDeletionRequest.newBuilder()
                        .setDeleteId(deleteId).build()));
                if (progress.getState() == TopicDeletionState.TOPIC_DELETION_STATE_DONE) {
                    break;
                }
                Thread.sleep(200);
            }
            assertEquals(TopicDeletionState.TOPIC_DELETION_STATE_DONE,
                    progress == null ? null : progress.getState(), "deletion state");
            System.out.println("[mq-v2-example] topic reclaimed: messages="
                    + progress.getMessagesRemoved() + " bytes=" + progress.getBytesReclaimed());

            // describe 必须已经返回 TOPIC_NOT_FOUND（删除立即生效，不等回收）。
            try {
                call(() -> mq.describeTopic(DescribeTopicRequest.newBuilder()
                        .setTopic(TOPIC).build()));
                throw new IllegalStateException("describe after delete must fail");
            } catch (StatusRuntimeException e) {
                Status.Code code = e.getStatus().getCode();
                if (code != Status.Code.NOT_FOUND) {
                    throw e;
                }
            }
            System.out.println("[mq-v2-example] OK");
            return 0;
        } finally {
            channel.shutdownNow();
        }
    }

    @FunctionalInterface
    private interface Rpc<T> {
        T call();
    }

    /** Maps "service not enabled / not proxied" to exit code 2, everything else rethrows. */
    private static <T> T call(Rpc<T> rpc) {
        try {
            return rpc.call();
        } catch (StatusRuntimeException e) {
            Status.Code code = e.getStatus().getCode();
            if (code == Status.Code.FAILED_PRECONDITION || code == Status.Code.UNIMPLEMENTED) {
                throw new NotV2ClusterException(e.getStatus().getDescription());
            }
            throw e;
        }
    }

    private static void assertEquals(Object expected, Object actual, String what) {
        if (expected == null ? actual != null : !expected.equals(actual)) {
            throw new IllegalStateException(
                    what + ": expected " + expected + " but was " + actual);
        }
    }

    private static ByteString utf8(String value) {
        return ByteString.copyFrom(value, StandardCharsets.UTF_8);
    }

    private static String env(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isEmpty() ? fallback : value;
    }

    private static final class NotV2ClusterException extends RuntimeException {
        private NotV2ClusterException(String message) {
            super(message);
        }
    }
}
