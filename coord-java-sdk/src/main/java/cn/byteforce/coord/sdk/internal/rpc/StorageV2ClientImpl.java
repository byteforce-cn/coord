package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.contracts.storage.v2.StorageGrpc;
import cn.byteforce.coord.contracts.storage.v2.StorageOuterClass;
import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import cn.byteforce.coord.sdk.storage.v2.StorageClient;
import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.io.ByteArrayOutputStream;
import java.util.Iterator;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@link StorageClient} backed by {@code coord.storage.v2} to the Coord Agent.
 * <p>
 * The whole object is buffered client-side (the agent buffers it too, capped at
 * 256 MiB) so that {@code totalSize} can be declared exactly; a declared/actual
 * mismatch is rejected by the agent before it consumes a {@code commitTs}.
 * Streaming writes are rebuilt per attempt — a half-consumed request stream
 * cannot be replayed.
 */
public final class StorageV2ClientImpl extends AgentRpcClient implements StorageClient {

    /** Agent-side object buffer cap; larger objects must be split by the caller. */
    static final int MAX_OBJECT_BYTES = 256 * 1024 * 1024;
    /** Server chunk default. */
    static final int MAX_CHUNK_BYTES = 4 * 1024 * 1024;

    private final CoordConfig config;

    public StorageV2ClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                               RetryTemplate retryTemplate, ObservabilityProvider observability,
                               CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    private long deadlineMs() {
        return config.getRequestTimeout().toMillis();
    }

    private static void validateTarget(String bucket, byte[] objectId) {
        if (bucket == null || bucket.isEmpty()) {
            throw new CoordException(ErrorCode.INVALID_ARGUMENT, "bucket must not be empty");
        }
        if (objectId == null || objectId.length == 0) {
            throw new CoordException(ErrorCode.INVALID_ARGUMENT, "objectId must not be empty");
        }
    }

    @Override
    public PutResult put(String bucket, byte[] objectId, byte[] data, int chunkSizeBytes) {
        validateTarget(bucket, objectId);
        if (data == null || data.length == 0) {
            throw new CoordException(ErrorCode.INVALID_ARGUMENT, "object data must not be empty");
        }
        if (data.length > MAX_OBJECT_BYTES) {
            throw new CoordException(ErrorCode.RESOURCE_EXHAUSTED,
                    "object exceeds the " + MAX_OBJECT_BYTES + " byte client buffer");
        }
        if (chunkSizeBytes <= 0 || chunkSizeBytes > MAX_CHUNK_BYTES) {
            throw new CoordException(ErrorCode.INVALID_ARGUMENT,
                    "chunk size must be in (0, 4MiB]");
        }
        return retryTemplate.execute(ctx -> {
            try {
                return putOnce(bucket, objectId, data, chunkSizeBytes);
            } catch (StatusRuntimeException e) {
                throw errorMapper.map(e);
            }
        });
    }

    private PutResult putOnce(String bucket, byte[] objectId, byte[] data, int chunkSizeBytes) {
        ManagedChannel channel = channelManager.getChannel();
        StorageGrpc.StorageStub stub = StorageGrpc.newStub(channel)
                .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS);

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<StorageOuterClass.PutResponse> responseRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();
        StreamObserver<StorageOuterClass.PutResponse> responseObserver = new StreamObserver<>() {
            @Override
            public void onNext(StorageOuterClass.PutResponse value) {
                responseRef.set(value);
            }

            @Override
            public void onError(Throwable t) {
                errorRef.set(t);
                done.countDown();
            }

            @Override
            public void onCompleted() {
                done.countDown();
            }
        };

        StreamObserver<StorageOuterClass.PutRequest> requestObserver = stub.put(responseObserver);
        requestObserver.onNext(StorageOuterClass.PutRequest.newBuilder()
                .setMeta(StorageOuterClass.PutMeta.newBuilder()
                        .setBucket(bucket)
                        .setObjectId(ByteString.copyFrom(objectId))
                        .setTotalSize(data.length)
                        .build())
                .build());
        for (int offset = 0; offset < data.length; offset += chunkSizeBytes) {
            int length = Math.min(chunkSizeBytes, data.length - offset);
            requestObserver.onNext(StorageOuterClass.PutRequest.newBuilder()
                    .setChunk(ByteString.copyFrom(data, offset, length))
                    .build());
        }
        requestObserver.onCompleted();

        awaitStream(done, "object storage v2 put");
        Throwable error = errorRef.get();
        if (error instanceof StatusRuntimeException sre) {
            throw sre;
        }
        if (error != null) {
            throw new CoordException(ErrorCode.INTERNAL, "object storage v2 put failed", error);
        }
        StorageOuterClass.PutResponse response = responseRef.get();
        if (response == null) {
            throw new CoordException(ErrorCode.INTERNAL, "no put response received");
        }
        return new PutResult(response.getCommitTs(), response.getSize(), response.getChunks());
    }

    @Override
    public ObjectData get(String bucket, byte[] objectId, long readTs) {
        validateTarget(bucket, objectId);
        validateReadTs(readTs);
        try {
            return retryTemplate.execute(ctx -> {
                try {
                    return getOnce(bucket, objectId, readTs);
                } catch (StatusRuntimeException e) {
                    throw errorMapper.map(e);
                }
            });
        } catch (ObjectTooLarge e) {
            throw new CoordException(ErrorCode.RESOURCE_EXHAUSTED, e.getMessage());
        }
    }

    private ObjectData getOnce(String bucket, byte[] objectId, long readTs) {
        StorageOuterClass.GetRequest request = StorageOuterClass.GetRequest.newBuilder()
                .setBucket(bucket)
                .setObjectId(ByteString.copyFrom(objectId))
                .setReadTs(readTs)
                .build();
        ManagedChannel channel = channelManager.getChannel();
        ByteArrayOutputStream payload = new ByteArrayOutputStream();
        StorageOuterClass.ObjectStat stat = null;
        Iterator<StorageOuterClass.GetResponse> stream = StorageGrpc.newBlockingStub(channel)
                .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                .get(request);
        while (stream.hasNext()) {
            StorageOuterClass.GetResponse message = stream.next();
            switch (message.getPartCase()) {
                case STAT -> stat = message.getStat();
                case CHUNK -> {
                    payload.writeBytes(message.getChunk().toByteArray());
                    if (payload.size() > MAX_OBJECT_BYTES) {
                        // 非 Coord 异常：不进入重试矩阵，避免把 256MiB 再拉一遍。
                        throw new ObjectTooLarge();
                    }
                }
                default -> {
                    // 未知 part：向前兼容，忽略。
                }
            }
        }
        if (stat == null) {
            throw new CoordException(ErrorCode.INTERNAL, "get stream missing the stat frame");
        }
        return new ObjectData(toSdk(stat), payload.toByteArray());
    }

    @Override
    public ObjectStat stat(String bucket, byte[] objectId, long readTs) {
        validateTarget(bucket, objectId);
        validateReadTs(readTs);
        StorageOuterClass.StatRequest request = StorageOuterClass.StatRequest.newBuilder()
                .setBucket(bucket)
                .setObjectId(ByteString.copyFrom(objectId))
                .setReadTs(readTs)
                .build();
        try {
            StorageOuterClass.StatResponse response = callWithRetry(
                    (ch, req) -> StorageGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                            .stat((StorageOuterClass.StatRequest) req),
                    request,
                    "storageV2.stat");
            if (response.hasStat()) {
                return toSdk(response.getStat());
            }
            return missingStat(bucket, objectId);
        } catch (CoordException e) {
            if (e.getErrorCode() == ErrorCode.NOT_FOUND) {
                // 与 agent 的 v2 零 stat 同口径：缺席不是错误。
                return missingStat(bucket, objectId);
            }
            throw e;
        }
    }

    @Override
    public DeleteResult delete(String bucket, byte[] objectId, byte[] requestId) {
        validateTarget(bucket, objectId);
        StorageOuterClass.DeleteRequest request = StorageOuterClass.DeleteRequest.newBuilder()
                .setBucket(bucket)
                .setObjectId(ByteString.copyFrom(objectId))
                .setRequestId(KvProto.bytes(requestId))
                .build();
        StorageOuterClass.DeleteResponse response = callWithRetry(
                (ch, req) -> StorageGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                        .delete((StorageOuterClass.DeleteRequest) req),
                request,
                "storageV2.delete");
        return new DeleteResult(response.getDeleted(), response.getCommitTs());
    }

    private static void validateReadTs(long readTs) {
        if (readTs < 0) {
            throw new CoordException(ErrorCode.INVALID_ARGUMENT, "readTs must be >= 0");
        }
    }

    private static ObjectStat toSdk(StorageOuterClass.ObjectStat stat) {
        return new ObjectStat(
                stat.getBucket(),
                stat.getObjectId().toByteArray(),
                stat.getSize(),
                stat.getChunks(),
                stat.getCommitTs(),
                stat.getCreateTs(),
                stat.getExists(),
                stat.getCommitted());
    }

    private static ObjectStat missingStat(String bucket, byte[] objectId) {
        return new ObjectStat(bucket, objectId.clone(), 0, 0, 0, 0, false, false);
    }

    private void awaitStream(CountDownLatch done, String operation) {
        try {
            if (!done.await(deadlineMs() + 5000, TimeUnit.MILLISECONDS)) {
                throw new CoordException(ErrorCode.DEADLINE_EXCEEDED, operation + " timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CoordException(ErrorCode.DEADLINE_EXCEEDED,
                    "interrupted during " + operation, e);
        }
    }

    /** Client-side buffer cap hit while reading; not retryable by design. */
    private static final class ObjectTooLarge extends RuntimeException {
        private ObjectTooLarge() {
            super("object exceeds the " + MAX_OBJECT_BYTES + " byte client buffer");
        }
    }
}
