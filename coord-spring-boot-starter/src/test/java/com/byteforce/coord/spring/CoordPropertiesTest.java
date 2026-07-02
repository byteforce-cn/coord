package cn.byteforce.coord.spring;

import cn.byteforce.coord.spring.autoconfigure.CoordProperties;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/**
 * TDD RED: CoordProperties 配置绑定测试
 *
 * 验证:
 * 1. 默认值正确
 * 2. 自定义属性绑定
 * 3. 服务启用/禁用配置
 */
class CoordPropertiesTest {

    @Test
    void testDefaultValues() {
        CoordProperties props = new CoordProperties();

        // Agent 连接默认值
        assertThat(props.getAgentHost()).isEqualTo("localhost");
        assertThat(props.getAgentPort()).isEqualTo(19527);
        assertThat(props.getRequestTimeoutMs()).isEqualTo(5000);
        assertThat(props.getMaxRetries()).isEqualTo(3);

        // 服务发现默认值
        assertThat(props.getDiscovery().isEnabled()).isTrue();
        assertThat(props.getDiscovery().getHeartbeatIntervalMs()).isEqualTo(10000);

        // 配置中心默认值
        assertThat(props.getConfig().isEnabled()).isTrue();
        assertThat(props.getConfig().getWatchEnabled()).isTrue();
    }

    @Test
    void testAgentConnectionSettings() {
        CoordProperties props = new CoordProperties();
        props.setAgentHost("192.168.1.100");
        props.setAgentPort(29527);
        props.setRequestTimeoutMs(10000);

        assertThat(props.getAgentHost()).isEqualTo("192.168.1.100");
        assertThat(props.getAgentPort()).isEqualTo(29527);
        assertThat(props.getRequestTimeoutMs()).isEqualTo(10000);
    }

    @Test
    void testDiscoveryProperties() {
        CoordProperties props = new CoordProperties();
        CoordProperties.DiscoveryProperties discovery = props.getDiscovery();

        // 默认启用
        assertThat(discovery.isEnabled()).isTrue();

        // 可禁用
        discovery.setEnabled(false);
        assertThat(discovery.isEnabled()).isFalse();
    }

    @Test
    void testServiceToggleProperties() {
        CoordProperties props = new CoordProperties();

        // 锁服务默认禁用（按需启用）
        assertThat(props.getLock().isEnabled()).isFalse();

        // 缓存服务默认禁用
        assertThat(props.getCache().isEnabled()).isFalse();

        // 启用
        props.getLock().setEnabled(true);
        assertThat(props.getLock().isEnabled()).isTrue();
    }
}
