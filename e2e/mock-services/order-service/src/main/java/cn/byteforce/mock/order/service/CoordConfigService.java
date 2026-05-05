package cn.byteforce.mock.order.service;

import coord.v1.Coord;
import coord.v1.ConfigServiceGrpc;
import coord.v1.RegistryServiceGrpc;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 从 Coord 配置中心读取配置，后台定时刷新（模拟 Watch 推送）。
 */
@Service
public class CoordConfigService {
    private static final Logger log = LoggerFactory.getLogger(CoordConfigService.class);

    @Autowired
    private ConfigServiceGrpc.ConfigServiceBlockingStub configStub;

    private final Map<String, String> cache = new ConcurrentHashMap<>();

    /** 每 10 秒从 Coord 刷新一次关键配置 */
    @Scheduled(fixedDelay = 10_000, initialDelay = 0)
    public void refresh() {
        refreshKey("order.max-amount");
        refreshKey("order.retry-times");
        refreshKey("order.timeout");
    }

    private void refreshKey(String key) {
        try {
            Coord.ConfigResponse resp = configStub.getConfig(
                    Coord.ConfigRequest.newBuilder().setKey(key).build());
            if (!resp.getValue().isEmpty()) {
                cache.put(key, resp.getValue());
                log.debug("Config refresh: {}={}", key, resp.getValue());
            }
        } catch (Exception e) {
            log.debug("Config key not found in Coord: {}", key);
        }
    }

    public String getConfig(String key, String defaultValue) {
        return cache.getOrDefault(key, defaultValue);
    }

    public double getDoubleConfig(String key, double defaultValue) {
        String val = cache.get(key);
        if (val == null) return defaultValue;
        try { return Double.parseDouble(val); }
        catch (NumberFormatException e) { return defaultValue; }
    }

    public int getIntConfig(String key, int defaultValue) {
        String val = cache.get(key);
        if (val == null) return defaultValue;
        try { return Integer.parseInt(val); }
        catch (NumberFormatException e) { return defaultValue; }
    }

    /** 直接返回当前缓存（供测试端点查询） */
    public Map<String, String> allConfig() {
        return cache;
    }
}
