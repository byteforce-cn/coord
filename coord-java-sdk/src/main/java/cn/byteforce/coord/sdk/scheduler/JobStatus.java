package cn.byteforce.coord.sdk.scheduler;

import java.nio.charset.StandardCharsets;

/**
 * Job detail / result snapshot returned by {@link SchedulerClient#getJob(String)}.
 */
public final class JobStatus {
    private final String jobId;
    private final String state;
    private final byte[] result;
    private final long completedAtMillis;

    public JobStatus(String jobId, String state, byte[] result, long completedAtMillis) {
        this.jobId = jobId;
        this.state = state;
        this.result = result == null ? new byte[0] : result.clone();
        this.completedAtMillis = completedAtMillis;
    }

    /** Job id (the registration name). */
    public String getJobId() {
        return jobId;
    }

    /** One of {@code Pending} / {@code Running} / {@code Completed} / {@code Failed}. */
    public String getState() {
        return state;
    }

    /** Convenience: {@code state} equals {@code Completed}. */
    public boolean isCompleted() {
        return "Completed".equals(state);
    }

    /**
     * The result persisted at completion (first completion wins; later completions
     * never overwrite it). Empty when the job is not completed yet. Defensive copy.
     */
    public byte[] getResult() {
        return result.clone();
    }

    /** Result as UTF-8 text. */
    public String getResultAsString() {
        return new String(result, StandardCharsets.UTF_8);
    }

    /** Completion wall-clock millis since UNIX epoch; {@code 0} when not completed. */
    public long getCompletedAtMillis() {
        return completedAtMillis;
    }

    @Override
    public String toString() {
        return "JobStatus{jobId=" + jobId + ", state=" + state
                + ", resultBytes=" + result.length + ", completedAtMillis=" + completedAtMillis + "}";
    }
}
