package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;

/**
 * Abstract base for all RPC client implementations.
 * <p>
 * Provides the {@link #callWithRetry} template method that handles retry logic,
 * error mapping, and observability recording uniformly for all API calls.
 */
public abstract class AgentRpcClient {

    protected final AgentChannelManager channelManager;
    protected final ErrorMapper errorMapper;
    protected final RetryTemplate retryTemplate;
    protected final ObservabilityProvider observability;

    protected AgentRpcClient(AgentChannelManager channelManager, ErrorMapper errorMapper,
                              RetryTemplate retryTemplate, ObservabilityProvider observability) {
        this.channelManager = channelManager;
        this.errorMapper = errorMapper;
        this.retryTemplate = retryTemplate;
        this.observability = observability;
    }

    /**
     * Execute an RPC call with automatic retry, error mapping, and observability.
     *
     * @param rpcCall       the actual RPC invocation
     * @param request       the request object (for logging context)
     * @param operationName a human-readable operation name for observability
     * @param <Req>         request type
     * @param <Resp>        response type
     * @return the response
     * @throws CoordException on failure after retries exhausted or non-retryable error
     */
    protected <Req, Resp> Resp callWithRetry(
            RpcCall<Req, Resp> rpcCall,
            Req request,
            String operationName) throws CoordException {

        long start = System.nanoTime();
        boolean success = false;
        try {
            Resp result = retryTemplate.execute(ctx -> {
                ManagedChannel ch = channelManager.getChannel();
                try {
                    return rpcCall.call(ch, request);
                } catch (StatusRuntimeException e) {
                    throw errorMapper.map(e);
                }
            });
            success = true;
            return result;
        } finally {
            observability.recordRpcCall(operationName, System.nanoTime() - start, success);
        }
    }

    /**
     * Functional interface for an RPC call.
     */
    @FunctionalInterface
    public interface RpcCall<Req, Resp> {
        Resp call(ManagedChannel channel, Req request) throws StatusRuntimeException;
    }
}
