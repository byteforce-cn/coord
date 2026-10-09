package cn.byteforce.coord.sdk.mq;

/**
 * Reclamation report returned by {@link MqClient#deleteTopic(String)} (G-MQ-4).
 * <p>
 * Counts are the actual removed entries observed by the agent that executed the
 * deletion; ISR followers apply the same deletion idempotently. In single-agent
 * deployments the numbers are exactly the local state that was reclaimed.
 */
public final class MqDeleteTopicResult {

    private final long messagesRemoved;
    private final long dlqRemoved;
    private final long offsetsRemoved;
    private final long idempotencyRemoved;
    private final long bytesReclaimed;

    public MqDeleteTopicResult(long messagesRemoved, long dlqRemoved, long offsetsRemoved,
                               long idempotencyRemoved, long bytesReclaimed) {
        this.messagesRemoved = messagesRemoved;
        this.dlqRemoved = dlqRemoved;
        this.offsetsRemoved = offsetsRemoved;
        this.idempotencyRemoved = idempotencyRemoved;
        this.bytesReclaimed = bytesReclaimed;
    }

    /** Main-log messages removed. */
    public long messagesRemoved() {
        return messagesRemoved;
    }

    /** Dead-letter queue entries removed. */
    public long dlqRemoved() {
        return dlqRemoved;
    }

    /** Consumer-offset rows removed. */
    public long offsetsRemoved() {
        return offsetsRemoved;
    }

    /** Idempotency-index rows removed. */
    public long idempotencyRemoved() {
        return idempotencyRemoved;
    }

    /** Accounted bytes reclaimed (messages + DLQ physical bytes). */
    public long bytesReclaimed() {
        return bytesReclaimed;
    }

    @Override
    public String toString() {
        return "MqDeleteTopicResult{messages=" + messagesRemoved + ", dlq=" + dlqRemoved
                + ", offsets=" + offsetsRemoved + ", idempotency=" + idempotencyRemoved
                + ", bytes=" + bytesReclaimed + "}";
    }
}
