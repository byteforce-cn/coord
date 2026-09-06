package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.ErrorCode;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.objectstore.ObjectStoreClient;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import com.google.protobuf.ByteString;
import coord.storage.StorageGrpc;
import coord.storage.StorageOuterClass;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.stub.StreamObserver;

import java.util.Iterator;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Implementation of {@link ObjectStoreClient} backed by gRPC calls to the Coord
 * Agent (which proxies {@code coord.storage.Storage} to the server cluster).
 * <p>
 * Uploads use the async streaming stub (Put is client-streaming); downloads and
 * unary calls use the blocking stub. Errors are mapped through {@link ErrorMapper}.
 */
public final class ObjectStoreClientImpl extends AgentRpcClient implements ObjectStoreClient {

    /** Max gRPC message budget; server chunk defaults to 4MiB. */
    static final int MAX_CHUNK = 4 * 1024 * 1024;

    private final CoordConfig config;

    public ObjectStoreClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                                 RetryTemplate retryTemplate, ObservabilityProvider observability,
                                 CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    private long deadlineMs() {
        return config.getRequestTimeout().toMillis();
    }

    private static ObjectStoreClient.ObjectInfo infoOf(StorageOuterClass.ObjectStat s) {
        return new ObjectStoreClient.ObjectInfo(
                s.getBucket(), s.getObjectId().toByteArray(),
                s.getSize(), s.getChunks(), s.getRevision(), s.getExists(), s.getCommitted());
    }

    @Override
    public PutResult put(String bucket, byte[] objectId, byte[] data) {
        return put(bucket, objectId, data, MAX_CHUNK);
    }

    @Override
    public PutResult put(String bucket, byte[] objectId, byte[] data, int chunkSizeBytes) {
        if (bucket == null || bucket.isEmpty()) {
            throw new CoordException(ErrorCode.INTERNAL, "bucket must not be empty");
        }
        if (objectId == null || objectId.length == 0) {
            throw new CoordException(ErrorCode.INTERNAL, "objectId must not be empty");
        }
        if (data == null || data.length == 0) {
            throw new CoordException(ErrorCode.INTERNAL, "object data must not be empty");
        }
        if (chunkSizeBytes <= 0 || chunkSizeBytes > MAX_CHUNK) {
            throw new CoordException(ErrorCode.INTERNAL, "chunk size must be in (0, 4MiB]");
        }

        ManagedChannel ch = channelManager.getChannel();
        StorageGrpc.StorageStub stub = StorageGrpc.newStub(ch)
                .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS);

        CountDownLatch done = new CountDownLatch(1);
        AtomicReference<StorageOuterClass.PutResponse> responseRef = new AtomicReference<>();
        AtomicReference<Throwable> errorRef = new AtomicReference<>();

        StreamObserver<StorageOuterClass.PutResponse> responseObserver =
                new StreamObserver<>() {
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
        // 首条：meta
        StorageOuterClass.PutMeta meta = StorageOuterClass.PutMeta.newBuilder()
                .setBucket(bucket)
                .setObjectId(ByteString.copyFrom(objectId))
                .setTotalSize(data.length)
                .build();
        requestObserver.onNext(StorageOuterClass.PutRequest.newBuilder().setMeta(meta).build());
        // 后续：逐 chunk
        for (int off = 0; off < data.length; off += chunkSizeBytes) {
            int len = Math.min(chunkSizeBytes, data.length - off);
            requestObserver.onNext(StorageOuterClass.PutRequest.newBuilder()
                    .setChunk(ByteString.copyFrom(data, off, len))
                    .build());
        }
        requestObserver.onCompleted();

        try {
            if (!done.await(deadlineMs() + 5000, TimeUnit.MILLISECONDS)) {
                throw new CoordException(ErrorCode.DEADLINE_EXCEEDED,
                        "object storage put timed out");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new CoordException(ErrorCode.DEADLINE_EXCEEDED, "interrupted during put", e);
        }

        Throwable error = errorRef.get();
        if (error != null) {
            if (error instanceof StatusRuntimeException sre) {
                throw errorMapper.map(sre);
            }
            throw new CoordException(ErrorCode.INTERNAL, "object storage put failed", error);
        }
        StorageOuterClass.PutResponse response = responseRef.get();
        if (response == null) {
            throw new CoordException(ErrorCode.INTERNAL, "no put response received");
        }
        return new PutResult(response.getRevision(), response.getSize(), response.getChunks());
    }

    @Override
    public GetResult get(String bucket, byte[] objectId) {
        ManagedChannel ch = channelManager.getChannel();
        StorageOuterClass.GetRequest request = StorageOuterClass.GetRequest.newBuilder()
                .setBucket(bucket)
                .setObjectId(ByteString.copyFrom(objectId))
                .build();
        try {
            Iterator<StorageOuterClass.GetResponse> stream =
                    StorageGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                            .get(request);
            ObjectStoreClient.ObjectInfo info = null;
            java.io.ByteArrayOutputStream out = new java.io.ByteArrayOutputStream();
            while (stream.hasNext()) {
                StorageOuterClass.GetResponse msg = stream.next();
                switch (msg.getPartCase()) {
                    case STAT -> info = infoOf(msg.getStat());
                    case CHUNK -> out.write(msg.getChunk().toByteArray());
                    default -> {
                        // ignore unknown parts
                    }
                }
            }
            if (info == null) {
                throw new CoordException(ErrorCode.INTERNAL, "get stream missing stat message");
            }
            return new GetResult(info, out.toByteArray());
        } catch (StatusRuntimeException e) {
            throw errorMapper.map(e);
        } catch (java.io.IOException e) {
            throw new CoordException(ErrorCode.INTERNAL, "get stream write failed", e);
        }
    }

    @Override
    public Optional<ObjectInfo> stat(String bucket, byte[] objectId) {
        ManagedChannel ch = channelManager.getChannel();
        StorageOuterClass.StatRequest request = StorageOuterClass.StatRequest.newBuilder()
                .setBucket(bucket)
                .setObjectId(ByteString.copyFrom(objectId))
                .build();
        try {
            StorageOuterClass.StatResponse response =
                    StorageGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                            .stat(request);
            if (response.hasStat()) {
                return Optional.of(infoOf(response.getStat()));
            }
            return Optional.empty();
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return Optional.empty();
            }
            throw errorMapper.map(e);
        }
    }

    @Override
    public DeleteResult delete(String bucket, byte[] objectId) {
        ManagedChannel ch = channelManager.getChannel();
        StorageOuterClass.DeleteRequest request = StorageOuterClass.DeleteRequest.newBuilder()
                .setBucket(bucket)
                .setObjectId(ByteString.copyFrom(objectId))
                .build();
        try {
            StorageOuterClass.DeleteResponse response =
                    StorageGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(deadlineMs(), TimeUnit.MILLISECONDS)
                            .delete(request);
            return new DeleteResult(response.getDeleted(), response.getRevision());
        } catch (StatusRuntimeException e) {
            if (e.getStatus().getCode() == Status.Code.NOT_FOUND) {
                return new DeleteResult(false, 0);
            }
            throw errorMapper.map(e);
        }
    }
}
