package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.CoordException;
import cn.byteforce.coord.sdk.cache.CacheClient;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.sdk.internal.proto.*;
import cn.byteforce.coord.sdk.spi.ObservabilityProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Implementation of {@link CacheClient} backed by gRPC calls to the Coord Agent.
 */
public final class CacheClientImpl extends AgentRpcClient implements CacheClient {

    private static final Logger log = LoggerFactory.getLogger(CacheClientImpl.class);
    private final CoordConfig config;

    public CacheClientImpl(AgentChannelManager channelManager, ErrorMapper errorMapper,
                           RetryTemplate retryTemplate, ObservabilityProvider observability,
                           CoordConfig config) {
        super(channelManager, errorMapper, retryTemplate, observability);
        this.config = config;
    }

    @Override
    public byte[] get(String key) {
        CacheGetRequest request = CacheGetRequest.newBuilder().setKey(key).build();
        try {
            CacheGetResponse response = callWithRetry(
                    (ch, req) -> CacheGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                            .get((CacheGetRequest) req),
                    request, "cache.get");
            return response.getFound() ? response.getValue().toByteArray() : null;
        } catch (CoordException e) {
            log.debug("Cache get failed: key={}, error={}", key, e.getMessage());
            return null;
        }
    }

    @Override
    public void set(String key, byte[] value, long ttlSeconds) {
        CacheSetRequest request = CacheSetRequest.newBuilder()
                .setKey(key)
                .setValue(com.google.protobuf.ByteString.copyFrom(value))
                .setTtlSeconds(ttlSeconds)
                .build();
        callWithRetry(
                (ch, req) -> CacheGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .set((CacheSetRequest) req),
                request, "cache.set");
        log.debug("Cache set: key={}, ttl={}s", key, ttlSeconds);
    }

    @Override
    public boolean delete(String key) {
        CacheDeleteRequest request = CacheDeleteRequest.newBuilder().setKey(key).build();
        CacheDeleteResponse response = callWithRetry(
                (ch, req) -> CacheGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .delete((CacheDeleteRequest) req),
                request, "cache.delete");
        return response.getDeleted();
    }

    @Override
    public byte[] hget(String key, String field) {
        CacheHGetRequest request = CacheHGetRequest.newBuilder()
                .setKey(key).setField(field).build();
        try {
            CacheHGetResponse response = callWithRetry(
                    (ch, req) -> CacheGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                            .hGet((CacheHGetRequest) req),
                    request, "cache.hget");
            return response.getFound() ? response.getValue().toByteArray() : null;
        } catch (CoordException e) {
            log.debug("Cache hget failed: key={}, field={}", key, field);
            return null;
        }
    }

    @Override
    public void hset(String key, String field, byte[] value) {
        CacheHSetRequest request = CacheHSetRequest.newBuilder()
                .setKey(key).setField(field)
                .setValue(com.google.protobuf.ByteString.copyFrom(value))
                .build();
        callWithRetry(
                (ch, req) -> CacheGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .hSet((CacheHSetRequest) req),
                request, "cache.hset");
    }

    @Override
    public Map<String, byte[]> hgetAll(String key) {
        CacheHGetAllRequest request = CacheHGetAllRequest.newBuilder().setKey(key).build();
        try {
            CacheHGetAllResponse response = callWithRetry(
                    (ch, req) -> CacheGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                            .hGetAll((CacheHGetAllRequest) req),
                    request, "cache.hgetall");
            Map<String, byte[]> result = new LinkedHashMap<>();
            response.getFieldsMap().forEach((k, v) -> result.put(k, v.toByteArray()));
            return result;
        } catch (CoordException e) {
            log.debug("Cache hgetall failed: key={}", key);
            return Map.of();
        }
    }

    @Override
    public long lpush(String key, byte[] value) {
        CacheLPushRequest request = CacheLPushRequest.newBuilder()
                .setKey(key)
                .setValue(com.google.protobuf.ByteString.copyFrom(value))
                .build();
        CacheLPushResponse response = callWithRetry(
                (ch, req) -> CacheGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .lPush((CacheLPushRequest) req),
                request, "cache.lpush");
        return response.getLength();
    }

    @Override
    public List<byte[]> lrange(String key, long start, long stop) {
        CacheLRangeRequest request = CacheLRangeRequest.newBuilder()
                .setKey(key).setStart(start).setStop(stop).build();
        try {
            CacheLRangeResponse response = callWithRetry(
                    (ch, req) -> CacheGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                            .lRange((CacheLRangeRequest) req),
                    request, "cache.lrange");
            return response.getValuesList().stream()
                    .map(com.google.protobuf.ByteString::toByteArray)
                    .collect(java.util.stream.Collectors.toList());
        } catch (CoordException e) {
            log.debug("Cache lrange failed: key={}", key);
            return List.of();
        }
    }

    @Override
    public void sadd(String key, byte[] member) {
        CacheSAddRequest request = CacheSAddRequest.newBuilder()
                .setKey(key)
                .setMember(com.google.protobuf.ByteString.copyFrom(member))
                .build();
        callWithRetry(
                (ch, req) -> CacheGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .sAdd((CacheSAddRequest) req),
                request, "cache.sadd");
    }

    @Override
    public List<byte[]> smembers(String key) {
        CacheSMembersRequest request = CacheSMembersRequest.newBuilder().setKey(key).build();
        try {
            CacheSMembersResponse response = callWithRetry(
                    (ch, req) -> CacheGrpc.newBlockingStub(ch)
                            .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                            .sMembers((CacheSMembersRequest) req),
                    request, "cache.smembers");
            return response.getMembersList().stream()
                    .map(com.google.protobuf.ByteString::toByteArray)
                    .collect(java.util.stream.Collectors.toList());
        } catch (CoordException e) {
            log.debug("Cache smembers failed: key={}", key);
            return List.of();
        }
    }
}
