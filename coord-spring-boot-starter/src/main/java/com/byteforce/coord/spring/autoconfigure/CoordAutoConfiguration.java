package cn.byteforce.coord.spring.autoconfigure;

import cn.byteforce.coord.spring.beans.IdGenerator;
import cn.byteforce.coord.spring.beans.LeaderElection;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Coord Spring Boot 自动配置
 *
 * 自动装配 Coord Agent 客户端所需的 Bean。
 */
@AutoConfiguration
@ConditionalOnClass(name = "io.grpc.ManagedChannel")
@EnableConfigurationProperties(CoordProperties.class)
public class CoordAutoConfiguration {

    /**
     * ID 生成器 Bean
     * 默认使用 Snowflake 策略，可通过 coord.idgen.strategy 配置。
     */
    @Bean
    public IdGenerator idGenerator(CoordProperties properties) {
        // 从 Agent 端口推导 workerId（简化版：单机部署 workerId=1）
        int workerId = 1;
        int datacenterId = 1;
        return new IdGenerator(IdGenerator.Strategy.SNOWFLAKE, workerId, datacenterId);
    }

    /**
     * Leader 选举 Bean
     * 每个应用默认参与 "default" 选举活动。
     */
    @Bean
    public LeaderElection leaderElection() {
        return new LeaderElection("default");
    }
}
