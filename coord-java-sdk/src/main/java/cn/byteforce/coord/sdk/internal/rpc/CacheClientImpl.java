package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.sdk.CoordConfig;
import cn.byteforce.coord.sdk.cache.CacheClearResult;
import cn.byteforce.coord.sdk.cache.CacheClient;
import cn.byteforce.coord.sdk.cache.CacheKeyPage;
import cn.byteforce.coord.sdk.internal.channel.AgentChannelManager;
import cn.byteforce.coord.contracts.cache.v1.*;
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
        CacheGetResponse response = callWithRetry(
                (ch, req) -> CacheGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .get((CacheGetRequest) req),
                request, "cache.get");
        return response.getFound() ? response.getValue().toByteArray() : null;
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
    public boolean setIfAbsent(String key, byte[] value, long ttlSeconds) {
        CacheSetIfAbsentRequest request = CacheSetIfAbsentRequest.newBuilder()
                .setKey(key)
                .setValue(com.google.protobuf.ByteString.copyFrom(value))
                .setTtlSeconds(ttlSeconds)
                .build();
        CacheSetIfAbsentResponse response = callWithRetry(
                (ch, req) -> CacheGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .setIfAbsent((CacheSetIfAbsentRequest) req),
                request, "cache.setIfAbsent");
        log.debug("Cache setIfAbsent: key={}, written={}", key, response.getWritten());
        return response.getWritten();
    }

    @Override
    public CacheKeyPage keys(String prefix, int limit, String cursor) {
        CacheKeysRequest request = CacheKeysRequest.newBuilder()
                .setPrefix(prefix == null ? "" : prefix)
                .setLimit(limit)
                .setCursor(cursor == null ? "" : cursor)
                .build();
        CacheKeysResponse response = callWithRetry(
                (ch, req) -> CacheGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .keys((CacheKeysRequest) req),
                request, "cache.keys");
        return new CacheKeyPage(response.getKeysList(), response.getNextCursor());
    }

    @Override
    public CacheClearResult clear(String prefix) {
        CacheClearRequest request = CacheClearRequest.newBuilder()
                .setPrefix(prefix == null ? "" : prefix)
                .build();
        CacheClearResponse response = callWithRetry(
                (ch, req) -> CacheGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .clear((CacheClearRequest) req),
                request, "cache.clear");
        log.debug("Cache clear: prefix='{}', removedEntries={}, bytesReclaimed={}",
                prefix, response.getRemovedEntries(), response.getBytesReclaimed());
        return new CacheClearResult(response.getRemovedEntries(), response.getBytesReclaimed());
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
        CacheHGetResponse response = callWithRetry(
                (ch, req) -> CacheGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .hGet((CacheHGetRequest) req),
                request, "cache.hget");
        return response.getFound() ? response.getValue().toByteArray() : null;
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
        CacheHGetAllResponse response = callWithRetry(
                (ch, req) -> CacheGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .hGetAll((CacheHGetAllRequest) req),
                request, "cache.hgetall");
        Map<String, byte[]> result = new LinkedHashMap<>();
        response.getFieldsMap().forEach((k, v) -> result.put(k, v.toByteArray()));
        return result;
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
        CacheLRangeResponse response = callWithRetry(
                (ch, req) -> CacheGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .lRange((CacheLRangeRequest) req),
                request, "cache.lrange");
        return response.getValuesList().stream()
                .map(com.google.protobuf.ByteString::toByteArray)
                .collect(java.util.stream.Collectors.toList());
    }

    @Override
    public byte[] rpop(String key) {
        CacheRPopRequest request = CacheRPopRequest.newBuilder().setKey(key).build();
        CacheRPopResponse response = callWithRetry(
                (ch, req) -> CacheGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .rPop((CacheRPopRequest) req),
                request, "cache.rpop");
        return response.getFound() ? response.getValue().toByteArray() : null;
    }

    @Override
    public long llen(String key) {
        CacheLLenRequest request = CacheLLenRequest.newBuilder().setKey(key).build();
        CacheLLenResponse response = callWithRetry(
                (ch, req) -> CacheGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .lLen((CacheLLenRequest) req),
                request, "cache.llen");
        return response.getLength();
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
        CacheSMembersResponse response = callWithRetry(
                (ch, req) -> CacheGrpc.newBlockingStub(ch)
                        .withDeadlineAfter(config.getRequestTimeout().toMillis(), TimeUnit.MILLISECONDS)
                        .sMembers((CacheSMembersRequest) req),
                request, "cache.smembers");
        return response.getMembersList().stream()
                .map(com.google.protobuf.ByteString::toByteArray)
                .collect(java.util.stream.Collectors.toList());
    }
}
