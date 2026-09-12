package cn.byteforce.coord.sdk.internal.channel;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * D2 证伪测试：CCT 必须以 {@code authorization: Bearer <cct>} 注入每个调用，
 * 且 token 刷新后**下一次调用**立即生效（无需重建 channel）。
 */
class AuthTokenInterceptorTest {

    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private static final MethodDescriptor.Marshaller<String> STRING_MARSHALLER =
            new MethodDescriptor.Marshaller<>() {
                @Override
                public InputStream stream(String value) {
                    return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
                }

                @Override
                public String parse(InputStream stream) {
                    try {
                        return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
                    } catch (java.io.IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            };

    private static final MethodDescriptor<String, String> METHOD =
            MethodDescriptor.<String, String>newBuilder()
                    .setType(MethodDescriptor.MethodType.UNARY)
                    .setFullMethodName("coord.test/Probe")
                    .setRequestMarshaller(STRING_MARSHALLER)
                    .setResponseMarshaller(STRING_MARSHALLER)
                    .build();

    /** 记录 {@code start()} 收到的 metadata。 */
    private static final class CapturingCall extends ClientCall<String, String> {
        final Metadata started = new Metadata();

        @Override
        public void start(Listener<String> responseListener, Metadata headers) {
            started.merge(headers);
        }

        @Override
        public void request(int numMessages) {
        }

        @Override
        public void cancel(String message, Throwable cause) {
        }

        @Override
        public void halfClose() {
        }

        @Override
        public void sendMessage(String message) {
        }
    }

    @SuppressWarnings("unchecked")
    private static Channel channelReturning(CapturingCall call) {
        return new Channel() {
            @Override
            public <ReqT, RespT> ClientCall<ReqT, RespT> newCall(
                    MethodDescriptor<ReqT, RespT> method, CallOptions callOptions) {
                return (ClientCall<ReqT, RespT>) call;
            }

            @Override
            public String authority() {
                return "test";
            }
        };
    }

    @Test
    void injectsBearerToken() {
        AuthTokenInterceptor interceptor = new AuthTokenInterceptor(() -> "cct-abc");
        CapturingCall call = new CapturingCall();

        interceptor.interceptCall(METHOD, CallOptions.DEFAULT, channelReturning(call))
                .start(new ClientCall.Listener<>() {
                }, new Metadata());

        assertThat(call.started.get(AUTHORIZATION)).isEqualTo("Bearer cct-abc");
    }

    @Test
    void readsCurrentTokenOnEveryCall() {
        AtomicReference<String> token = new AtomicReference<>("cct-old");
        AuthTokenInterceptor interceptor = new AuthTokenInterceptor(token::get);

        CapturingCall first = new CapturingCall();
        interceptor.interceptCall(METHOD, CallOptions.DEFAULT, channelReturning(first))
                .start(new ClientCall.Listener<>() {
                }, new Metadata());
        assertThat(first.started.get(AUTHORIZATION)).isEqualTo("Bearer cct-old");

        // token 刷新（续期/重新登录）→ 下一次调用立即携带新值
        token.set("cct-new");
        CapturingCall second = new CapturingCall();
        interceptor.interceptCall(METHOD, CallOptions.DEFAULT, channelReturning(second))
                .start(new ClientCall.Listener<>() {
                }, new Metadata());
        assertThat(second.started.get(AUTHORIZATION)).isEqualTo("Bearer cct-new");
    }

    @Test
    void doesNotAttachHeaderWhenTokenMissing() {
        AuthTokenInterceptor interceptor = new AuthTokenInterceptor(() -> "  ");
        CapturingCall call = new CapturingCall();

        interceptor.interceptCall(METHOD, CallOptions.DEFAULT, channelReturning(call))
                .start(new ClientCall.Listener<>() {
                }, new Metadata());

        // 不注入空凭据：服务端随后按 fail-closed 拒绝
        assertThat(call.started.get(AUTHORIZATION)).isNull();
    }

    @Test
    void passesThroughAlreadyPrefixedToken() {
        AuthTokenInterceptor interceptor = new AuthTokenInterceptor(() -> "Bearer cct-x");
        CapturingCall call = new CapturingCall();

        interceptor.interceptCall(METHOD, CallOptions.DEFAULT, channelReturning(call))
                .start(new ClientCall.Listener<>() {
                }, new Metadata());

        assertThat(call.started.get(AUTHORIZATION)).isEqualTo("Bearer cct-x");
    }
}
