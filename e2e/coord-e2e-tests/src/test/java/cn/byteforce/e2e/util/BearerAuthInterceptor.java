package cn.byteforce.e2e.util;

import io.grpc.*;

import java.util.concurrent.atomic.AtomicReference;

/**
 * gRPC ClientInterceptor that injects a Bearer token when available.
 * The token reference is mutable so it can be set after security domain initialization.
 */
public class BearerAuthInterceptor implements ClientInterceptor {

    private static final Metadata.Key<String> AUTH_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private final AtomicReference<String> tokenRef;

    public BearerAuthInterceptor(AtomicReference<String> tokenRef) {
        this.tokenRef = tokenRef;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                String token = tokenRef.get();
                if (token != null && !token.isEmpty()) {
                    headers.put(AUTH_KEY, "Bearer " + token);
                }
                super.start(responseListener, headers);
            }
        };
    }
}
