package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.contracts.kv.v2.KVGrpc;
import cn.byteforce.coord.contracts.kv.v2.Kv;
import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.kv.v2.KvClient;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.sdk.tso.v2.TsoClient;

import java.util.concurrent.TimeUnit;

/**
 * {@link KvClient} backed by {@code coord.kv.v2} calls to the Coord Agent.
 * <p>
 * Writes require an explicit TSO {@code commitTs}; the convenience overloads
 * fetch one fresh timestamp. There is no client-side KV cache: v2 reads are
 * snapshot reads, and a cache could not prove the revision it returns.
 */
public final class KvV2ClientImpl extends AgentRpcClient implements KvClient {

    private final CoordConfig config;
    private final TsoClient tsoClient;

    public KvV2ClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                          RetryTemplate retryTemplate, ObservabilityProvider observability,
                          CoordConfig config, TsoClient tsoClient) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
        this.tsoClient = tsoClient;
    }

    private long deadlineMs() {
        return config.getRequestTimeout().toMillis();
    }

    @Override
    public long getCommitTs() {
        return tsoClient.getTimestamp();
    }

    @Override
    public PutResult put(byte[] key, byte[] value, long commitTs, boolean prevKv,
                         byte[] requestId) {
        return putInternal(key, value, 0, commitTs, prevKv, requestId);
    }

    @Override
    public PutResult putWithLease(byte[] key, byte[] value, long leaseId, long commitTs,
                                  boolean prevKv, byte[] requestId) {
        return putInternal(key, value, leaseId, commitTs, prevKv, requestId);
    }

    private PutResult putInternal(byte[] key, byte[] value, long leaseId, long commitTs,
                                  boolean prevKv, byte[] requestId) {
        Kv.PutRequest request = Kv.PutRequest.newBuilder()
                .setKey(KvProto.bytes(key))
                .setValue(KvProto.bytes(value))
                .setLeaseId(leaseId)
                .setCommitTs(commitTs)
                .setPrevKv(prevKv)
                .setRequestId(KvProto.bytes(requestId))
                .build();
        Kv.PutResponse response = callWithRetry(
                (ch, req) -> KVGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                        .put((Kv.PutRequest) req),
                request,
                "kvV2.put");
        return new PutResult(
                response.hasPrevKv() ? KvProto.toSdk(response.getPrevKv()) : null,
                response.getCommitTs());
    }

    @Override
    public RangeResult range(byte[] key, byte[] rangeEnd, long limit, long readTs,
                             boolean keysOnly, boolean countOnly) {
        Kv.RangeRequest request = Kv.RangeRequest.newBuilder()
                .setKey(KvProto.bytes(key))
                .setRangeEnd(KvProto.bytes(rangeEnd))
                .setLimit(limit)
                .setReadTs(readTs)
                .setKeysOnly(keysOnly)
                .setCountOnly(countOnly)
                .build();
        Kv.RangeResponse response = callWithRetry(
                (ch, req) -> KVGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                        .range((Kv.RangeRequest) req),
                request,
                "kvV2.range");
        return new RangeResult(
                KvProto.toSdk(response.getKvsList()),
                response.getCount(),
                response.getReadTs());
    }

    @Override
    public DeleteResult delete(byte[] key, byte[] rangeEnd, long commitTs, boolean prevKv,
                               byte[] requestId) {
        Kv.DeleteRequest request = Kv.DeleteRequest.newBuilder()
                .setKey(KvProto.bytes(key))
                .setRangeEnd(KvProto.bytes(rangeEnd))
                .setCommitTs(commitTs)
                .setPrevKv(prevKv)
                .setRequestId(KvProto.bytes(requestId))
                .build();
        Kv.DeleteResponse response = callWithRetry(
                (ch, req) -> KVGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                        .delete((Kv.DeleteRequest) req),
                request,
                "kvV2.delete");
        return new DeleteResult(
                response.getDeleted(),
                KvProto.toSdk(response.getPrevKvsList()),
                response.getCommitTs());
    }
}
