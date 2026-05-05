package cn.byteforce.mock.pay.config;

import io.grpc.*;

public class BearerAuthInterceptor implements ClientInterceptor {

    private static final Metadata.Key<String> AUTH_KEY =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private final CoordAuthTokenHolder tokenHolder;

    public BearerAuthInterceptor(CoordAuthTokenHolder tokenHolder) {
        this.tokenHolder = tokenHolder;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                String token = tokenHolder.getToken();
                if (token != null && !token.isEmpty()) {
                    headers.put(AUTH_KEY, "Bearer " + token);
                }
                super.start(responseListener, headers);
            }
        };
    }
}
