package cn.byteforce.coord.spring.autoconfigure;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

/**
 * Coord gRPC Stub 管理器
 *
 * 管理到 Coord Agent 的 gRPC Channel 连接。
 * Agent 运行在 localhost，无需 TLS。
 */
public class CoordStubManager {

    private static final Logger log = LoggerFactory.getLogger(CoordStubManager.class);

    private final CoordProperties properties;
    private volatile ManagedChannel channel;

    public CoordStubManager(CoordProperties properties) {
        this.properties = properties;
    }

    /**
     * 获取或创建 gRPC Channel（延迟连接）
     */
    public ManagedChannel getChannel() {
        if (channel == null) {
            synchronized (this) {
                if (channel == null) {
                    String target = properties.getAgentHost() + ":" + properties.getAgentPort();
                    log.info("Creating gRPC channel to Coord Agent at {}", target);
                    channel = ManagedChannelBuilder
                            .forAddress(properties.getAgentHost(), properties.getAgentPort())
                            .usePlaintext() // Agent 在 localhost，无 TLS
                            .build();
                }
            }
        }
        return channel;
    }

    /**
     * 关闭 gRPC Channel
     */
    @PreDestroy
    public void shutdown() {
        if (channel != null && !channel.isShutdown()) {
            log.info("Shutting down gRPC channel to Coord Agent");
            try {
                channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                channel.shutdownNow();
            }
        }
    }
}
