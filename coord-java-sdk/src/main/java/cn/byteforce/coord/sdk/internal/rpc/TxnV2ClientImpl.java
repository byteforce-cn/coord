package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.contracts.txn.v2.TxnGrpc;
import cn.byteforce.coord.contracts.txn.v2.TxnOuterClass;
import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.sdk.txn.v2.TxnClient;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * {@link TxnClient} backed by {@code coord.txn.v2} calls to the Coord Agent.
 * <p>
 * The SDK talks to one agent channel; the agent pins each {@code txnId} to the
 * coordinator that created the session (ADR-0015 D12.2), so the session calls
 * never re-resolve a leader here. An OCC conflict is a {@link CommitResult},
 * not an exception, mirroring the Rust client: it must be replayed with a new
 * {@code startTs}, and a hidden retry would return a stale snapshot twice.
 */
public final class TxnV2ClientImpl extends AgentRpcClient implements TxnClient {

    private final CoordConfig config;

    public TxnV2ClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                           RetryTemplate retryTemplate, ObservabilityProvider observability,
                           CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    private long deadlineMs() {
        return config.getRequestTimeout().toMillis();
    }

    @Override
    public Session begin(long startTs, int ttlMs) {
        if (startTs < 0) {
            throw new CoordException(ErrorCode.INVALID_ARGUMENT, "startTs must be >= 0");
        }
        if (ttlMs < 0) {
            throw new CoordException(ErrorCode.INVALID_ARGUMENT, "ttlMs must be >= 0");
        }
        TxnOuterClass.BeginRequest request = TxnOuterClass.BeginRequest.newBuilder()
                .setStartTs(startTs)
                .setTtlMs(ttlMs)
                .build();
        TxnOuterClass.BeginResponse response = callWithRetry(
                (ch, req) -> TxnGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                        .begin((TxnOuterClass.BeginRequest) req),
                request,
                "txnV2.begin");
        return new SessionImpl(response.getTxnId().toByteArray(), response.getStartTs());
    }

    private static List<TxnOuterClass.Mutation> toProto(List<Mutation> mutations) {
        List<TxnOuterClass.Mutation> out = new ArrayList<>(mutations.size());
        for (Mutation mutation : mutations) {
            if (mutation == null || mutation.op() == null) {
                throw new CoordException(ErrorCode.INVALID_ARGUMENT,
                        "mutation and its op must not be null");
            }
            switch (mutation.op()) {
                case PUT -> {
                    if (mutation.rangeEnd() != null && mutation.rangeEnd().length > 0) {
                        throw new CoordException(ErrorCode.INVALID_ARGUMENT,
                                "PUT must not carry a rangeEnd");
                    }
                }
                case DELETE -> {
                    if (mutation.rangeEnd() != null && mutation.rangeEnd().length > 0) {
                        throw new CoordException(ErrorCode.INVALID_ARGUMENT,
                                "DELETE must not carry a rangeEnd");
                    }
                }
                case DELETE_RANGE -> {
                    if (mutation.rangeEnd() == null || mutation.rangeEnd().length == 0) {
                        throw new CoordException(ErrorCode.INVALID_ARGUMENT,
                                "DELETE_RANGE requires a non-empty rangeEnd");
                    }
                }
            }
            out.add(TxnOuterClass.Mutation.newBuilder()
                    .setOp(TxnOuterClass.Mutation.Op.valueOf(mutation.op().name()))
                    .setKey(KvProto.bytes(mutation.key()))
                    .setRangeEnd(KvProto.bytes(mutation.rangeEnd()))
                    .setValue(KvProto.bytes(mutation.value()))
                    .setLeaseId(mutation.leaseId())
                    .build());
        }
        return out;
    }

    private final class SessionImpl implements Session {

        private final byte[] txnId;
        private final long startTs;

        private SessionImpl(byte[] txnId, long startTs) {
            this.txnId = txnId;
            this.startTs = startTs;
        }

        @Override
        public byte[] txnId() {
            return txnId.clone();
        }

        @Override
        public long startTs() {
            return startTs;
        }

        @Override
        public ReadResult read(byte[] key, byte[] rangeEnd, long limit, boolean keysOnly) {
            if (limit < 0) {
                throw new CoordException(ErrorCode.INVALID_ARGUMENT, "limit must be >= 0");
            }
            TxnOuterClass.ReadRequest request = TxnOuterClass.ReadRequest.newBuilder()
                    .setTxnId(ByteString.copyFrom(txnId))
                    .setKey(KvProto.bytes(key))
                    .setRangeEnd(KvProto.bytes(rangeEnd))
                    .setLimit(limit)
                    .setKeysOnly(keysOnly)
                    .build();
            TxnOuterClass.ReadResponse response = callWithRetry(
                    (ch, req) -> TxnGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                            .read((TxnOuterClass.ReadRequest) req),
                    request,
                    "txnV2.read");
            return new ReadResult(KvProto.toSdk(response.getKvsList()), response.getReadTs());
        }

        @Override
        public CommitResult commit(List<Mutation> mutations, byte[] requestId) {
            if (mutations == null) {
                throw new CoordException(ErrorCode.INVALID_ARGUMENT, "mutations must not be null");
            }
            TxnOuterClass.CommitRequest request = TxnOuterClass.CommitRequest.newBuilder()
                    .setTxnId(ByteString.copyFrom(txnId))
                    .addAllMutations(toProto(mutations))
                    .setRequestId(KvProto.bytes(requestId))
                    .build();
            try {
                TxnOuterClass.CommitResponse response = callWithRetry(
                        (ch, req) -> TxnGrpc.newBlockingStub(ch)
                                .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                                .commit((TxnOuterClass.CommitRequest) req),
                        request,
                        "txnV2.commit");
                return new CommitResult(true, response.getCommitTs());
            } catch (CoordException e) {
                if (e.getErrorCode() == ErrorCode.TXN_CONFLICT) {
                    return CommitResult.CONFLICT;
                }
                throw e;
            }
        }

        @Override
        public void abort() {
            TxnOuterClass.AbortRequest request = TxnOuterClass.AbortRequest.newBuilder()
                    .setTxnId(ByteString.copyFrom(txnId))
                    .build();
            try {
                callWithRetry(
                        (ch, req) -> TxnGrpc.newBlockingStub(ch)
                                .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                                .abort((TxnOuterClass.AbortRequest) req),
                        request,
                        "txnV2.abort");
            } catch (CoordException e) {
                if (e.getErrorCode() == ErrorCode.TXN_NOT_FOUND) {
                    // 会话已过期/已结束：放弃的意图已经成立，与服务端同口径。
                    return;
                }
                throw e;
            }
        }
    }
}
