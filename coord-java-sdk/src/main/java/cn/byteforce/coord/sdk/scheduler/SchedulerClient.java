package cn.byteforce.coord.sdk.scheduler;

import cn.byteforce.coord.sdk.CoordException;

import java.util.Optional;

/**
 * Distributed scheduling API ({@code coord.scheduler.v1}).
 *
 * <p>Jobs are registered by <b>name</b>, and claiming is a <b>cross-node atomic
 * CAS</b> on the Coord server's shared state — so the same job is never handed to two
 * workers at once, even if they talk to different agents. The claim is <b>leased</b>:
 * if you neither {@link #heartbeat(String) renew} nor
 * {@link #completeJob(String, byte[]) complete} it, the claim expires and the job
 * becomes claimable again.
 *
 * <p><b>The claim handle is the job id.</b> The wire carries no worker identity
 * ({@code SchedulerHeartbeatRequest} / {@code SchedulerCompleteJobRequest} only have
 * {@code job_id}), so <b>possession of the returned job id is the credential</b>.
 * Do not log it or share it with another worker.
 *
 * <pre>{@code
 * try (CoordClient client = CoordClient.create(config)) {
 *     SchedulerClient scheduler = client.scheduler();
 *     scheduler.registerJob("nightly-report", "0 0 3 * * ?", payload);
 *
 *     Optional<ClaimedJob> claimed = scheduler.claimJob("nightly-report");
 *     if (claimed.isPresent()) {
 *         ClaimedJob job = claimed.get();
 *         try {
 *             runReport(job.getPayload());
 *             scheduler.completeJob(job.getJobId(), "ok".getBytes());
 *         } catch (Exception e) {
 *             // 不 complete ⇒ 租约到期后任务可被重新认领（至少一次）
 *         }
 *     }
 * }
 * }</pre>
 *
 * <p><b>Boundaries (explicit, per {@code WHITEPAPER.md} boundary rules):</b>
 * <ul>
 *   <li>The contract's {@code cron_expression} has no documented dialect; pass the
 *       6-field form the coordinator accepts.</li>
 *   <li>{@code result} on completion <b>is persisted</b> with the job record
 *       (KV-backed, survives restarts; the <b>first</b> completion's result wins and
 *       later completions never overwrite it) — read it back with
 *       {@link #getJob(String)}.</li>
 * </ul>
 */
public interface SchedulerClient {

    /**
     * Register (or overwrite) a job.
     *
     * @param name            job name; this is the key {@link #claimJob(String)} uses
     * @param cronExpression  schedule expression
     * @param payload         opaque payload handed back to whoever claims the job
     * @return the job id (currently the same as {@code name})
     * @throws CoordException on communication failure
     */
    String registerJob(String name, String cronExpression, byte[] payload);

    /** Register a job with a UTF-8 string payload. */
    default String registerJob(String name, String cronExpression, String payload) {
        return registerJob(name, cronExpression,
                payload == null ? new byte[0]
                        : payload.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /**
     * Try to claim a job.
     *
     * <p>Returns empty when the job is not registered, is already claimed by someone
     * else, or is not yet due. Claiming is atomic across nodes: at most one caller
     * ever gets a given claim.
     *
     * @param name job name
     * @return the claim (with its job id and payload), or empty if not claimable now
     * @throws CoordException on communication failure or CAS contention
     */
    Optional<ClaimedJob> claimJob(String name);

    /**
     * Renew a claim's lease.
     *
     * @param jobId the job id returned by {@link #claimJob(String)}
     * @throws CoordException if the claim is no longer valid (lease expired or the job
     *                        was completed) or on communication failure
     */
    void heartbeat(String jobId);

    /**
     * Mark a job completed, releasing the claim.
     *
     * <p>The {@code result} is <b>persisted with the job record</b> (G-SC-1: KV-backed,
     * survives restarts). The <b>first</b> completion's result wins; duplicate
     * completions are idempotent and never overwrite it. Read it back with
     * {@link #getJob(String)}.
     *
     * @param jobId  the job id returned by {@link #claimJob(String)}
     * @param result bytes persisted at completion (first completion wins)
     * @throws CoordException on communication failure
     */
    void completeJob(String jobId, byte[] result);

    /**
     * Read job detail, including the persisted completion result.
     *
     * <p>Read-only and idempotent; an unknown job returns {@link Optional#empty()}
     * (not an error). The job state is {@code Pending} / {@code Running} /
     * {@code Completed} / {@code Failed}; the result is readable after completion,
     * across restarts and from any agent (state lives in the shared KV).
     *
     * @param jobId the job id returned by {@link #claimJob(String)} (or the registration name)
     * @return the job snapshot, or empty when the job was never registered
     * @throws CoordException on communication failure
     */
    Optional<JobStatus> getJob(String jobId);

    /** Complete a job with a UTF-8 string result. */
    default void completeJob(String jobId, String result) {
        completeJob(jobId, result == null ? new byte[0]
                : result.getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }

    /** An acquired claim. The {@code jobId} doubles as the renewal/completion credential. */
    final class ClaimedJob {
        private final String jobId;
        private final byte[] payload;

        public ClaimedJob(String jobId, byte[] payload) {
            this.jobId = jobId;
            this.payload = payload == null ? new byte[0] : payload.clone();
        }

        /** Job id — also the claim credential for heartbeat/complete. */
        public String getJobId() {
            return jobId;
        }

        /** The payload supplied at registration time (defensive copy). */
        public byte[] getPayload() {
            return payload.clone();
        }

        /** Payload as UTF-8 text. */
        public String getPayloadAsString() {
            return new String(payload, java.nio.charset.StandardCharsets.UTF_8);
        }

        @Override
        public String toString() {
            return "ClaimedJob{jobId=" + jobId + ", payloadBytes=" + payload.length + "}";
        }
    }
}
