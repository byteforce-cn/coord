package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.circuitbreaker.CircuitBreakerClient;
import cn.byteforce.coord.sdk.election.LeaderElectionClient;
import cn.byteforce.coord.sdk.featureflags.FeatureFlagClient;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.ratelimiter.RateLimiterClient;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.contracts.circuitbreaker.v1.CircuitBreakerGetStateRequest;
import cn.byteforce.coord.contracts.circuitbreaker.v1.CircuitBreakerGetStateResponse;
import cn.byteforce.coord.contracts.circuitbreaker.v1.CircuitBreakerGrpc;
import cn.byteforce.coord.contracts.circuitbreaker.v1.CircuitBreakerReportFailureRequest;
import cn.byteforce.coord.contracts.circuitbreaker.v1.CircuitBreakerReportFailureResponse;
import cn.byteforce.coord.contracts.circuitbreaker.v1.CircuitBreakerReportSuccessRequest;
import cn.byteforce.coord.contracts.circuitbreaker.v1.CircuitBreakerReportSuccessResponse;
import cn.byteforce.coord.contracts.circuitbreaker.v1.CircuitBreakerResetRequest;
import cn.byteforce.coord.contracts.circuitbreaker.v1.CircuitBreakerResetResponse;
import cn.byteforce.coord.contracts.election.v1.LeaderCampaignRequest;
import cn.byteforce.coord.contracts.election.v1.LeaderCampaignResponse;
import cn.byteforce.coord.contracts.election.v1.LeaderElectionGrpc;
import cn.byteforce.coord.contracts.election.v1.LeaderGetLeaderRequest;
import cn.byteforce.coord.contracts.election.v1.LeaderGetLeaderResponse;
import cn.byteforce.coord.contracts.election.v1.LeaderResignRequest;
import cn.byteforce.coord.contracts.election.v1.LeaderResignResponse;
import cn.byteforce.coord.contracts.featureflags.v1.FeatureFlagEvaluateRequest;
import cn.byteforce.coord.contracts.featureflags.v1.FeatureFlagEvaluateResponse;
import cn.byteforce.coord.contracts.featureflags.v1.FeatureFlagIsEnabledRequest;
import cn.byteforce.coord.contracts.featureflags.v1.FeatureFlagIsEnabledResponse;
import cn.byteforce.coord.contracts.featureflags.v1.FeatureFlagsGrpc;
import cn.byteforce.coord.contracts.ratelimiter.v1.RateLimiterAllowRequest;
import cn.byteforce.coord.contracts.ratelimiter.v1.RateLimiterAllowResponse;
import cn.byteforce.coord.contracts.ratelimiter.v1.RateLimiterGrpc;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * D3 / D4 / D5：四个新增 SDK 面的契约测试（election / circuitBreaker / rateLimiter /
 * featureFlags）。
 * <p>
 * 每个用例都断言**真实 RPC 字段**（请求侧服务端收到什么、响应侧 SDK 映射成什么），
 * 而不是只断言"方法存在" —— 后者在字段号/类型漂移时仍然会绿。
 */
class NewClientFacesTest {

    private Server server;
    private ManagedChannel channel;
    private final FakeElection election = new FakeElection();
    private final FakeBreaker breaker = new FakeBreaker();
    private final FakeRateLimiter limiter = new FakeRateLimiter();
    private final FakeFlags flags = new FakeFlags();

    private LeaderElectionClient electionClient;
    private CircuitBreakerClient breakerClient;
    private RateLimiterClient rateLimiterClient;
    private FeatureFlagClient flagClient;

    @BeforeEach
    void setUp() throws Exception {
        server = ServerBuilder.forPort(0)
                .addService(election)
                .addService(breaker)
                .addService(limiter)
                .addService(flags)
                .build()
                .start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort())
                .usePlaintext()
                .build();

        AgentChannelManager channelManager = mock(AgentChannelManager.class);
        when(channelManager.getChannel()).thenReturn(channel);
        CoordConfig config = CoordConfig.builder().agentHost("localhost").build();
        ObservabilityProvider obs = new ObservabilityProvider() {
        };

        electionClient = new LeaderElectionClientImpl(channelManager, new ErrorMapper(),
                new RetryTemplate(), obs, config);
        breakerClient = new CircuitBreakerClientImpl(channelManager, new ErrorMapper(),
                new RetryTemplate(), obs, config);
        rateLimiterClient = new RateLimiterClientImpl(channelManager, new ErrorMapper(),
                new RetryTemplate(), obs, config);
        flagClient = new FeatureFlagClientImpl(channelManager, new ErrorMapper(),
                new RetryTemplate(), obs, config);
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    // ── election ────────────────────────────────────────────────────────────

    @Test
    void electionCampaignSendsCredentialsAndMapsOutcome() {
        election.campaignOutcome = true;

        LeaderElectionClient.LeaderLease lease =
                electionClient.campaign("daily-report", "node-1", 30);

        assertThat(election.lastCampaign.getGroupName()).isEqualTo("daily-report");
        assertThat(election.lastCampaign.getCandidateId()).isEqualTo("node-1");
        assertThat(election.lastCampaign.getTtlSeconds()).isEqualTo(30);
        assertThat(lease.isElected()).isTrue();
        assertThat(lease.getLeaseId()).isEqualTo(77);
        assertThat(lease.getLeaderId()).isEqualTo("node-1");
    }

    @Test
    void electionCampaignNotElectedIsNormalNotError() {
        election.campaignOutcome = false;

        LeaderElectionClient.LeaderLease lease =
                electionClient.campaign("daily-report", "node-2", 30);

        assertThat(lease.isElected()).isFalse();
    }

    @Test
    void electionResignAndCurrentLeaderRoundTrip() {
        assertThat(electionClient.resign("g", "node-1", 77)).isTrue();
        assertThat(election.lastResign.getLeaseId()).isEqualTo(77);

        LeaderElectionClient.Leader leader = electionClient.currentLeader("g");
        assertThat(leader).isNotNull();
        assertThat(leader.getLeaderId()).isEqualTo("node-1");
        assertThat(leader.getElectedAt()).isEqualTo(1_700_000_000L);

        // 无 leader（exists=false）→ null，而不是一个"空 leader"对象
        election.hasLeader = false;
        assertThat(electionClient.currentLeader("g")).isNull();
    }

    // ── circuit breaker ─────────────────────────────────────────────────────

    @Test
    void breakerStateMapsContractStrings() {
        breaker.nextState = "OPEN";
        assertThat(breakerClient.getState("payment-gw").getState())
                .isEqualTo(CircuitBreakerClient.CircuitState.OPEN);

        breaker.nextState = "HALF_OPEN";
        assertThat(breakerClient.getState("payment-gw").getState())
                .isEqualTo(CircuitBreakerClient.CircuitState.HALF_OPEN);

        breaker.nextState = "CLOSED";
        assertThat(breakerClient.getState("payment-gw").getState())
                .isEqualTo(CircuitBreakerClient.CircuitState.CLOSED);
    }

    /**
     * 未知状态**不得**静默当作 CLOSED（那会把"看不懂"伪装成"正常放行"）。
     * 契约的取值是 CLOSED/OPEN/HALF_OPEN；其它一律按探测态处理。
     */
    @Test
    void breakerUnknownStateIsNotTreatedAsHealthy() {
        breaker.nextState = "SOMETHING_NEW";
        assertThat(breakerClient.getState("x").getState())
                .isEqualTo(CircuitBreakerClient.CircuitState.HALF_OPEN);
    }

    @Test
    void breakerReportAndResetReachTheServer() {
        breakerClient.reportSuccess("payment-gw");
        breakerClient.reportFailure("payment-gw");
        breakerClient.reset("payment-gw");

        assertThat(breaker.lastSuccessName).isEqualTo("payment-gw");
        assertThat(breaker.lastFailureName).isEqualTo("payment-gw");
        assertThat(breaker.lastResetName).isEqualTo("payment-gw");
    }

    // ── rate limiter ────────────────────────────────────────────────────────

    @Test
    void rateLimiterSendsPermitsAndMapsDecision() {
        limiter.allowed = false;
        limiter.remaining = 0;
        limiter.resetTime = 1_700_000_123_456L;

        RateLimiterClient.RateLimitDecision d = rateLimiterClient.allow("orders-api", 3);

        assertThat(limiter.lastKey).isEqualTo("orders-api");
        assertThat(limiter.lastPermits).isEqualTo(3);
        assertThat(d.isAllowed()).isFalse();
        assertThat(d.getRemaining()).isEqualTo(0);
        assertThat(d.getResetTime()).isEqualTo(1_700_000_123_456L);
    }

    // ── feature flags ───────────────────────────────────────────────────────

    @Test
    void featureFlagIsEnabledSendsJsonContextAsBytes() {
        flags.enabled = true;
        flags.variant = "treatment";

        FeatureFlagClient.FlagDecision d =
                flagClient.isEnabled("new-checkout", "{\"userId\":\"u-1\"}");

        assertThat(flags.lastEnabledName).isEqualTo("new-checkout");
        assertThat(flags.lastEnabledContext.toStringUtf8())
                .isEqualTo("{\"userId\":\"u-1\"}");
        assertThat(d.isEnabled()).isTrue();
        assertThat(d.getVariant()).isEqualTo("treatment");
    }

    @Test
    void featureFlagNullContextIsEmptyAndEmptyVariantBecomesNull() {
        flags.enabled = false;
        flags.variant = "";

        FeatureFlagClient.FlagDecision d = flagClient.isEnabled("f", null);

        assertThat(flags.lastEnabledContext.size()).isEqualTo(0);
        assertThat(d.isEnabled()).isFalse();
        // 契约里 variant 是 string；空串按"没有 variant"映射为 null
        assertThat(d.getVariant()).isNull();
    }

    @Test
    void featureFlagEvaluateReturnsJsonResult() {
        flags.result = "{\"bucket\":7}";

        String result = flagClient.evaluate("f", "{}");

        assertThat(flags.lastEvaluateName).isEqualTo("f");
        assertThat(result).isEqualTo("{\"bucket\":7}");
    }

    // ── fakes ───────────────────────────────────────────────────────────────

    private static final class FakeElection extends LeaderElectionGrpc.LeaderElectionImplBase {
        boolean campaignOutcome = true;
        boolean hasLeader = true;
        LeaderCampaignRequest lastCampaign;
        LeaderResignRequest lastResign;

        @Override
        public void campaign(LeaderCampaignRequest req, StreamObserver<LeaderCampaignResponse> o) {
            lastCampaign = req;
            o.onNext(LeaderCampaignResponse.newBuilder()
                    .setElected(campaignOutcome)
                    .setLeaseId(77)
                    .setLeaderId(req.getCandidateId())
                    .build());
            o.onCompleted();
        }

        @Override
        public void resign(LeaderResignRequest req, StreamObserver<LeaderResignResponse> o) {
            lastResign = req;
            o.onNext(LeaderResignResponse.newBuilder().setResigned(true).build());
            o.onCompleted();
        }

        @Override
        public void getLeader(LeaderGetLeaderRequest req,
                              StreamObserver<LeaderGetLeaderResponse> o) {
            o.onNext(LeaderGetLeaderResponse.newBuilder()
                    .setExists(hasLeader)
                    .setLeaderId("node-1")
                    .setLeaseId(77)
                    .setElectedAt(1_700_000_000L)
                    .build());
            o.onCompleted();
        }
    }

    private static final class FakeBreaker extends CircuitBreakerGrpc.CircuitBreakerImplBase {
        String nextState = "CLOSED";
        String lastSuccessName;
        String lastFailureName;
        String lastResetName;

        @Override
        public void getState(CircuitBreakerGetStateRequest req,
                             StreamObserver<CircuitBreakerGetStateResponse> o) {
            o.onNext(CircuitBreakerGetStateResponse.newBuilder()
                    .setState(nextState)
                    .setLastFailureTime(1_700_000_000_000L)
                    .build());
            o.onCompleted();
        }

        @Override
        public void reportSuccess(CircuitBreakerReportSuccessRequest req,
                                  StreamObserver<CircuitBreakerReportSuccessResponse> o) {
            lastSuccessName = req.getName();
            o.onNext(CircuitBreakerReportSuccessResponse.getDefaultInstance());
            o.onCompleted();
        }

        @Override
        public void reportFailure(CircuitBreakerReportFailureRequest req,
                                  StreamObserver<CircuitBreakerReportFailureResponse> o) {
            lastFailureName = req.getName();
            o.onNext(CircuitBreakerReportFailureResponse.getDefaultInstance());
            o.onCompleted();
        }

        @Override
        public void reset(CircuitBreakerResetRequest req,
                          StreamObserver<CircuitBreakerResetResponse> o) {
            lastResetName = req.getName();
            o.onNext(CircuitBreakerResetResponse.getDefaultInstance());
            o.onCompleted();
        }
    }

    private static final class FakeRateLimiter extends RateLimiterGrpc.RateLimiterImplBase {
        boolean allowed = true;
        long remaining = 9;
        long resetTime = 1_700_000_000_000L;
        String lastKey;
        int lastPermits;

        @Override
        public void allow(RateLimiterAllowRequest req,
                          StreamObserver<RateLimiterAllowResponse> o) {
            lastKey = req.getKey();
            lastPermits = req.getPermits();
            o.onNext(RateLimiterAllowResponse.newBuilder()
                    .setAllowed(allowed)
                    .setRemaining(remaining)
                    .setResetTime(resetTime)
                    .build());
            o.onCompleted();
        }
    }

    private static final class FakeFlags extends FeatureFlagsGrpc.FeatureFlagsImplBase {
        boolean enabled = true;
        String variant = "";
        String result = "";
        String lastEnabledName;
        ByteString lastEnabledContext = ByteString.EMPTY;
        String lastEvaluateName;

        @Override
        public void isEnabled(FeatureFlagIsEnabledRequest req,
                              StreamObserver<FeatureFlagIsEnabledResponse> o) {
            lastEnabledName = req.getFlagName();
            lastEnabledContext = req.getContext();
            o.onNext(FeatureFlagIsEnabledResponse.newBuilder()
                    .setEnabled(enabled)
                    .setVariant(variant)
                    .build());
            o.onCompleted();
        }

        @Override
        public void evaluate(FeatureFlagEvaluateRequest req,
                             StreamObserver<FeatureFlagEvaluateResponse> o) {
            lastEvaluateName = req.getFlagName();
            o.onNext(FeatureFlagEvaluateResponse.newBuilder()
                    .setResult(ByteString.copyFrom(result, StandardCharsets.UTF_8))
                    .build());
            o.onCompleted();
        }
    }
}
