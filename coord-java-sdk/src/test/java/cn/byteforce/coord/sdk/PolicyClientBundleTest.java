package cn.byteforce.coord.sdk;

import cn.byteforce.coord.sdk.policy.PolicyClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link PolicyClient} bundle management and explain methods.
 *
 * <p>These tests verify the SDK's data model and method signatures compile
 * correctly. End-to-end integration tests require a running Coord Agent.
 */
class PolicyClientBundleTest {

    @Test
    @DisplayName("BundleInfo constructor and getters")
    void testBundleInfoGetters() {
        PolicyClient.BundleInfo info = new PolicyClient.BundleInfo(
                "tenant-1/default/my-policy",
                "my-policy",
                "default",
                "tenant-1",
                true,
                1753000000L,
                1753000000L);

        assertThat(info.getBundleId()).isEqualTo("tenant-1/default/my-policy");
        assertThat(info.getName()).isEqualTo("my-policy");
        assertThat(info.getNamespace()).isEqualTo("default");
        assertThat(info.getTenantId()).isEqualTo("tenant-1");
        assertThat(info.isEnabled()).isTrue();
        assertThat(info.getCreatedAt()).isEqualTo(1753000000L);
        assertThat(info.getUpdatedAt()).isEqualTo(1753000000L);
    }

    @Test
    @DisplayName("BundleInfo toString contains key fields")
    void testBundleInfoToString() {
        PolicyClient.BundleInfo info = new PolicyClient.BundleInfo(
                "id-1", "test", "ns", "t1", false, 1L, 2L);

        String s = info.toString();
        assertThat(s).contains("id-1");
        assertThat(s).contains("test");
        assertThat(s).contains("ns");
        assertThat(s).contains("t1");
        assertThat(s).contains("false");
    }

    @Test
    @DisplayName("BundleInfo disabled state")
    void testBundleInfoDisabled() {
        PolicyClient.BundleInfo info = new PolicyClient.BundleInfo(
                "id-2", "disabled-bundle", "default", "tenant-2",
                false, 100L, 200L);

        assertThat(info.isEnabled()).isFalse();
    }
}
