package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.sdk.transit.TransitClient;
import cn.byteforce.coord.sdk.transit.TransitHmacSignature;
import cn.byteforce.coord.contracts.transit.v1.TransitGrpc;
import cn.byteforce.coord.contracts.transit.v1.TransitHmacSignRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitHmacSignResponse;
import cn.byteforce.coord.contracts.transit.v1.TransitHmacVerifyRequest;
import cn.byteforce.coord.contracts.transit.v1.TransitHmacVerifyResponse;
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
 * Transit HMAC 用途化 API 的契约测试（G-TR-3 / ADR-0012）。
 * <p>
 * 每个用例都断言**真实 RPC 字段**：purpose/key_id 是否真的送达服务端、
 * 响应侧 key_id 是否映射进 {@link TransitHmacSignature}——只断言"方法存在"
 * 在字段号/类型漂移时仍然会绿。
 */
class TransitHmacContractTest {

    private Server server;
    private ManagedChannel channel;
    private final FakeTransit transit = new FakeTransit();
    private TransitClient client;

    @BeforeEach
    void setUp() throws Exception {
        server = ServerBuilder.forPort(0).addService(transit).build().start();
        channel = ManagedChannelBuilder.forAddress("localhost", server.getPort()).usePlaintext().build();
        AgentChannelManager channelManager = mock(AgentChannelManager.class);
        when(channelManager.getChannel()).thenReturn(channel);
        client = new TransitClientImpl(channelManager, new ErrorMapper(), new RetryTemplate(),
                new ObservabilityProvider() {
                }, CoordConfig.builder().agentHost("localhost").build());
    }

    @AfterEach
    void tearDown() {
        channel.shutdownNow();
        server.shutdownNow();
    }

    @Test
    void hmacSignWithPurposeSendsPurposeAndMapsKeyId() {
        transit.signKeyId = "k-primary";
        TransitHmacSignature result =
                client.hmacSignWithPurpose("data".getBytes(StandardCharsets.UTF_8), "eis:hr:id:v1");
        assertThat(transit.lastSign.getPurpose()).isEqualTo("eis:hr:id:v1");
        assertThat(transit.lastSign.getKeyId()).isEmpty(); // 新写入不指定材料
        assertThat(transit.lastSign.getAlgorithm()).isEqualTo("HMAC-SHA256");
        assertThat(result.keyId()).isEqualTo("k-primary");
        assertThat(result.signature()).hasSize(32);
    }

    @Test
    void hmacSignWithPurposeAndKeyIdSendsHistoricalMaterial() {
        client.hmacSignWithPurpose("data".getBytes(StandardCharsets.UTF_8),
                "eis:hr:id:v1", "HMAC-SHA256", "k-old");
        assertThat(transit.lastSign.getKeyId()).isEqualTo("k-old");
    }

    @Test
    void hmacVerifyWithPurposeSendsPurposeAndKeyId() {
        transit.valid = true;
        boolean valid = client.hmacVerifyWithPurpose("data".getBytes(StandardCharsets.UTF_8),
                new byte[32], "eis:hr:id:v1", "k-old");
        assertThat(valid).isTrue();
        assertThat(transit.lastVerify.getPurpose()).isEqualTo("eis:hr:id:v1");
        assertThat(transit.lastVerify.getKeyId()).isEqualTo("k-old");
    }

    private static final class FakeTransit extends TransitGrpc.TransitImplBase {
        volatile TransitHmacSignRequest lastSign;
        volatile TransitHmacVerifyRequest lastVerify;
        volatile String signKeyId = "";
        volatile boolean valid;

        @Override
        public void hmacSign(TransitHmacSignRequest request,
                             StreamObserver<TransitHmacSignResponse> observer) {
            lastSign = request;
            observer.onNext(TransitHmacSignResponse.newBuilder()
                    .setSignature(ByteString.copyFrom(new byte[32]))
                    .setAlgorithm(request.getAlgorithm())
                    .setKeyId(signKeyId)
                    .build());
            observer.onCompleted();
        }

        @Override
        public void hmacVerify(TransitHmacVerifyRequest request,
                               StreamObserver<TransitHmacVerifyResponse> observer) {
            lastVerify = request;
            observer.onNext(TransitHmacVerifyResponse.newBuilder().setValid(valid).build());
            observer.onCompleted();
        }
    }
}
