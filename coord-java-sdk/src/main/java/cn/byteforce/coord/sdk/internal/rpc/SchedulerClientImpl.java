package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.contracts.scheduler.v1.SchedulerClaimJobRequest;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerClaimJobResponse;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerCompleteJobRequest;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerCompleteJobResponse;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerGrpc;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerHeartbeatRequest;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerHeartbeatResponse;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerRegisterJobRequest;
import cn.byteforce.coord.contracts.scheduler.v1.SchedulerRegisterJobResponse;
import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.scheduler.SchedulerClient;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link SchedulerClient} backed by gRPC to the Coord Agent.
 *
 * <p>字段映射逐项对齐契约（{@code scheduler.proto}）：
 * {@code RegisterJob} 的 {@code name}/{@code cron_expression}/{@code payload}、
 * {@code ClaimJob} 的 {@code name} → {@code job_id}/{@code payload}/{@code found}、
 * {@code Heartbeat} 与 {@code CompleteJob} 的 {@code job_id}（认领句柄）。
 */
public final class SchedulerClientImpl extends AgentRpcClient implements SchedulerClient {

    private static final Logger log = LoggerFactory.getLogger(SchedulerClientImpl.class);

    private final CoordConfig config;

    public SchedulerClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                               RetryTemplate retryTemplate, ObservabilityProvider observability,
                               CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    @Override
    public String registerJob(String name, String cronExpression, byte[] payload) {
        SchedulerRegisterJobRequest request = SchedulerRegisterJobRequest.newBuilder()
                .setName(nullToEmpty(name))
                .setCronExpression(nullToEmpty(cronExpression))
                .setPayload(ByteString.copyFrom(payload == null ? new byte[0] : payload))
                .build();

        SchedulerRegisterJobResponse response = callWithRetry(
                (ch, req) -> SchedulerGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .registerJob((SchedulerRegisterJobRequest) req),
                request, "scheduler.registerJob");

        log.debug("Scheduler job registered: name={} jobId={}", name, response.getJobId());
        return response.getJobId();
    }

    @Override
    public Optional<ClaimedJob> claimJob(String name) {
        SchedulerClaimJobRequest request = SchedulerClaimJobRequest.newBuilder()
                .setName(nullToEmpty(name))
                .build();

        SchedulerClaimJobResponse response = callWithRetry(
                (ch, req) -> SchedulerGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .claimJob((SchedulerClaimJobRequest) req),
                request, "scheduler.claimJob");

        // `found=false` 是**正常返回**，不是错误：它同时覆盖"任务未注册"、
        // "已被别的 worker 持有"、"尚未到期"三种情形。契约刻意不区分它们
        // （区分会让调用方把竞态当成故障）。
        if (!response.getFound()) {
            return Optional.empty();
        }
        return Optional.of(new ClaimedJob(
                response.getJobId(),
                response.getPayload().toByteArray()));
    }

    @Override
    public void heartbeat(String jobId) {
        SchedulerHeartbeatRequest request = SchedulerHeartbeatRequest.newBuilder()
                .setJobId(nullToEmpty(jobId))
                .build();

        // 续期失败必须**可见**：服务端在 claim 已失效时返回 FAILED_PRECONDITION
        // （历史实现把 renew 的失败结果直接丢掉，调用方以为续期成功 —— 与
        // `Lock.RenewResponse.new_ttl` 成功也回 0 同型的"静默失效"）。
        // 这里不做任何吞错处理：映射后的 CoordException 必须上抛。
        SchedulerHeartbeatResponse ignored = callWithRetry(
                (ch, req) -> SchedulerGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .heartbeat((SchedulerHeartbeatRequest) req),
                request, "scheduler.heartbeat");
        log.debug("Scheduler claim renewed: jobId={} (ack={})", jobId, ignored != null);
    }

    @Override
    public void completeJob(String jobId, byte[] result) {
        SchedulerCompleteJobRequest request = SchedulerCompleteJobRequest.newBuilder()
                .setJobId(nullToEmpty(jobId))
                .setResult(ByteString.copyFrom(result == null ? new byte[0] : result))
                .build();

        SchedulerCompleteJobResponse ignored = callWithRetry(
                (ch, req) -> SchedulerGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .completeJob((SchedulerCompleteJobRequest) req),
                request, "scheduler.completeJob");
        log.debug("Scheduler job completed: jobId={} (ack={})", jobId, ignored != null);
    }

    private static String nullToEmpty(String s) {
        return s == null ? "" : s;
    }
}
