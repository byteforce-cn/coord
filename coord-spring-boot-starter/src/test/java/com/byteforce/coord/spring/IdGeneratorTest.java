package cn.byteforce.coord.spring;

import cn.byteforce.coord.spring.beans.IdGenerator;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/**
 * TDD RED: IdGenerator ID 生成器测试
 *
 * 验证:
 * 1. Snowflake 策略生成趋势递增 ID
 * 2. ID 唯一性
 * 3. 批量生成
 */
class IdGeneratorTest {

    @Test
    void testSnowflakeGeneratesNonZero() {
        IdGenerator gen = new IdGenerator(IdGenerator.Strategy.SNOWFLAKE, 1, 1);
        long id = gen.nextId();
        assertThat(id).isGreaterThan(0);
    }

    @Test
    void testSnowflakeGeneratesTrendIncreasing() {
        IdGenerator gen = new IdGenerator(IdGenerator.Strategy.SNOWFLAKE, 1, 1);
        long prev = gen.nextId();
        for (int i = 0; i < 100; i++) {
            long next = gen.nextId();
            assertThat(next).isGreaterThan(prev);
            prev = next;
        }
    }

    @Test
    void testBatchGenerationUnique() {
        IdGenerator gen = new IdGenerator(IdGenerator.Strategy.SNOWFLAKE, 1, 1);
        long[] ids = gen.nextIds(1000);
        assertThat(ids).hasSize(1000);

        // 所有 ID 唯一
        long distinct = java.util.Arrays.stream(ids).distinct().count();
        assertThat(distinct).isEqualTo(1000);
    }

    @Test
    void testGetStrategy() {
        IdGenerator gen = new IdGenerator(IdGenerator.Strategy.SNOWFLAKE, 1, 1);
        assertThat(gen.getStrategy()).isEqualTo(IdGenerator.Strategy.SNOWFLAKE);
    }
}
