package cn.byteforce.coord.sdk.registry;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * {@link FilterMode} 单元测试。
 *
 * <p>验证 proto 值映射正确性，确保 ALL 和 PREFIX 的值与 agent_api.proto 一致。
 */
@DisplayName("FilterMode 单元测试")
class FilterModeTest {

    @Test
    @DisplayName("EXACT should map to proto value 0 (FILTER_MODE_UNSPECIFIED)")
    void exactShouldMapToProtoZero() {
        assertThat(FilterMode.EXACT.toProtoValue()).isEqualTo(0);
    }

    @Test
    @DisplayName("PREFIX should map to proto value 2 (FILTER_MODE_PREFIX)")
    void prefixShouldMapToProtoTwo() {
        assertThat(FilterMode.PREFIX.toProtoValue()).isEqualTo(2);
    }

    @Test
    @DisplayName("ALL should map to proto value 3 (FILTER_MODE_ALL)")
    void allShouldMapToProtoThree() {
        assertThat(FilterMode.ALL.toProtoValue()).isEqualTo(3);
    }

    @Test
    @DisplayName("fromProtoValue should restore all enum values")
    void fromProtoValueShouldRestoreAll() {
        assertThat(FilterMode.fromProtoValue(0)).isEqualTo(FilterMode.EXACT);
        assertThat(FilterMode.fromProtoValue(1)).isEqualTo(FilterMode.EXACT); // backward compat
        assertThat(FilterMode.fromProtoValue(2)).isEqualTo(FilterMode.PREFIX);
        assertThat(FilterMode.fromProtoValue(3)).isEqualTo(FilterMode.ALL);
    }

    @Test
    @DisplayName("fromProtoValue should default to EXACT for unknown values")
    void fromProtoValueShouldDefaultToExact() {
        assertThat(FilterMode.fromProtoValue(99)).isEqualTo(FilterMode.EXACT);
        assertThat(FilterMode.fromProtoValue(-1)).isEqualTo(FilterMode.EXACT);
    }
}
