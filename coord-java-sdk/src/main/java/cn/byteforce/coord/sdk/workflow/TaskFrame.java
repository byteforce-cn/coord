package cn.byteforce.coord.sdk.workflow;

import java.util.Arrays;
import java.util.Objects;

/**
 * A single frame in the workflow task execution stack.
 * <p>
 * Corresponds to the proto {@code TaskFrame} message, representing
 * the execution state of one workflow task step.
 */
public final class TaskFrame {

    private final String taskName;
    private final String taskType;
    private final String status;
    private final byte[] input;
    private final byte[] output;
    private final long startedAt;
    private final long endedAt;
    private final int retryCount;

    public TaskFrame(String taskName, String taskType, String status,
                     byte[] input, byte[] output,
                     long startedAt, long endedAt, int retryCount) {
        this.taskName = taskName;
        this.taskType = taskType;
        this.status = status;
        this.input = input;
        this.output = output;
        this.startedAt = startedAt;
        this.endedAt = endedAt;
        this.retryCount = retryCount;
    }

    public String taskName() { return taskName; }
    public String taskType() { return taskType; }
    public String status() { return status; }
    public byte[] input() { return input; }
    public byte[] output() { return output; }
    public long startedAt() { return startedAt; }
    public long endedAt() { return endedAt; }
    public int retryCount() { return retryCount; }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof TaskFrame that)) return false;
        return startedAt == that.startedAt && endedAt == that.endedAt
                && retryCount == that.retryCount
                && Objects.equals(taskName, that.taskName)
                && Objects.equals(taskType, that.taskType)
                && Objects.equals(status, that.status)
                && Arrays.equals(input, that.input)
                && Arrays.equals(output, that.output);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(taskName, taskType, status, startedAt, endedAt, retryCount);
        result = 31 * result + Arrays.hashCode(input);
        result = 31 * result + Arrays.hashCode(output);
        return result;
    }

    @Override
    public String toString() {
        return "TaskFrame{taskName='" + taskName + "', taskType='" + taskType
                + "', status='" + status + "', retryCount=" + retryCount + "}";
    }
}
