package cn.byteforce.coord.sdk.internal.channel;

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;

import java.util.function.Supplier;

/**
 * D2：把 CCT 作为 gRPC metadata {@code authorization: Bearer <cct>} 注入**每一个**调用。
 *
 * <p>为何用 {@link Supplier} 而不是构建时快照：token 会在运行中刷新（续期 / 重新登录）。
 * 若在 channel 构建时固定 token，刷新后必须重建 channel 才能生效，且会与在途调用产生
 * 竞态。这里每次调用都读取"当前" token，因此：
 * <ul>
 *   <li>刷新后立刻对后续调用生效，无需重建 channel；</li>
 *   <li>不存在"读旧 token / 写新 token"交错导致的半状态。</li>
 * </ul>
 *
 * <p>token 为 {@code null}/空白时**不注入** header —— 服务端随后按 fail-closed 拒绝该调用，
 * 而不是伪造一个空凭据。
 */
public final class AuthTokenInterceptor implements ClientInterceptor {

    private static final Metadata.Key<String> AUTHORIZATION =
            Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);

    private final Supplier<String> tokenSupplier;

    public AuthTokenInterceptor(Supplier<String> tokenSupplier) {
        this.tokenSupplier = tokenSupplier;
    }

    @Override
    public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
            MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel next) {
        return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
            @Override
            public void start(Listener<RespT> responseListener, Metadata headers) {
                String token = tokenSupplier == null ? null : tokenSupplier.get();
                if (token != null && !token.isBlank()) {
                    headers.put(AUTHORIZATION,
                            token.startsWith("Bearer ") ? token : "Bearer " + token);
                }
                super.start(responseListener, headers);
            }
        };
    }
}
