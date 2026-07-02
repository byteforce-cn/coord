package cn.byteforce.coord.spring;

import cn.byteforce.coord.spring.autoconfigure.CoordAutoConfiguration;
import cn.byteforce.coord.spring.autoconfigure.CoordProperties;
import cn.byteforce.coord.spring.beans.IdGenerator;
import cn.byteforce.coord.spring.beans.LeaderElection;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import static org.assertj.core.api.Assertions.*;

/**
 * TDD RED: CoordAutoConfiguration 自动配置测试
 *
 * 验证:
 * 1. 自动配置加载 Bean
 * 2. 条件启用/禁用
 * 3. 属性绑定
 */
class CoordAutoConfigurationTest {

    private final ApplicationContextRunner contextRunner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(CoordAutoConfiguration.class));

    @Test
    void testDefaultContextLoads() {
        // 默认配置应成功加载
        contextRunner.run(ctx -> {
            assertThat(ctx).hasSingleBean(CoordProperties.class);
            assertThat(ctx).hasSingleBean(IdGenerator.class);
            assertThat(ctx).hasSingleBean(LeaderElection.class);
        });
    }

    @Test
    void testIdGeneratorBeanCreated() {
        contextRunner.run(ctx -> {
            IdGenerator idGen = ctx.getBean(IdGenerator.class);
            assertThat(idGen).isNotNull();
            // 默认使用 snowflake 策略
            assertThat(idGen.getStrategy()).isEqualTo(IdGenerator.Strategy.SNOWFLAKE);
        });
    }

    @Test
    void testLeaderElectionBeanCreated() {
        contextRunner.run(ctx -> {
            LeaderElection election = ctx.getBean(LeaderElection.class);
            assertThat(election).isNotNull();
            assertThat(election.isLeader()).isFalse();
        });
    }

    @Test
    void testPropertiesBinding() {
        contextRunner
                .withPropertyValues(
                        "coord.agent.host=10.0.0.1",
                        "coord.agent.port=29527",
                        "coord.lock.enabled=true"
                )
                .run(ctx -> {
                    CoordProperties props = ctx.getBean(CoordProperties.class);
                    assertThat(props.getAgentHost()).isEqualTo("10.0.0.1");
                    assertThat(props.getAgentPort()).isEqualTo(29527);
                    assertThat(props.getLock().isEnabled()).isTrue();
                });
    }

    @Test
    void testServicesDisabledByDefault() {
        contextRunner.run(ctx -> {
            CoordProperties props = ctx.getBean(CoordProperties.class);
            // 仅 discovery 和 config 默认启用
            assertThat(props.getLock().isEnabled()).isFalse();
            assertThat(props.getCache().isEnabled()).isFalse();
        });
    }
}
