package cn.byteforce.coord.spring.beans;

/**
 * 分布式 ID 生成器
 *
 * 支持多种策略:
 * - SNOWFLAKE: Twitter Snowflake 变体，趋势递增，本地生成延迟 <1ms
 * - SEGMENT: 号段模式（需 Agent 端 IdGenService）
 *
 * Snowflake 位分配 (64 bits):
 * [1 bit 保留] [41 bits 毫秒时间戳] [5 bits datacenterId] [5 bits workerId] [12 bits 序列号]
 */
public class IdGenerator {

    /** ID 生成策略 */
    public enum Strategy {
        SNOWFLAKE,
        SEGMENT
    }

    private final Strategy strategy;
    private final int workerId;
    private final int datacenterId;

    // Snowflake 内部状态
    private final long twepoch = 1700000000000L; // 2024-01-01 00:00:00 UTC
    private final int workerIdBits = 5;
    private final int datacenterIdBits = 5;
    private final int sequenceBits = 12;
    private final int workerIdShift = sequenceBits;
    private final int datacenterIdShift = sequenceBits + workerIdBits;
    private final int timestampLeftShift = sequenceBits + workerIdBits + datacenterIdBits;
    private final int sequenceMask = (1 << sequenceBits) - 1;

    private long lastTimestamp = -1L;
    private long sequence = 0L;

    public IdGenerator(Strategy strategy, int workerId, int datacenterId) {
        this.strategy = strategy;
        this.workerId = workerId & ((1 << workerIdBits) - 1);
        this.datacenterId = datacenterId & ((1 << datacenterIdBits) - 1);
    }

    public Strategy getStrategy() {
        return strategy;
    }

    /**
     * 生成下一个 ID
     */
    public synchronized long nextId() {
        if (strategy == Strategy.SNOWFLAKE) {
            return nextSnowflakeId();
        }
        // SEGMENT 模式暂不在此 Starter 中实现（需 Agent IdGenService）
        throw new UnsupportedOperationException("SEGMENT strategy requires Agent IdGenService");
    }

    /**
     * 批量生成 ID
     */
    public long[] nextIds(int count) {
        long[] ids = new long[count];
        for (int i = 0; i < count; i++) {
            ids[i] = nextId();
        }
        return ids;
    }

    private long nextSnowflakeId() {
        long timestamp = System.currentTimeMillis();

        if (timestamp < lastTimestamp) {
            // 时钟回拨: 等待追上或抛异常
            long offset = lastTimestamp - timestamp;
            if (offset <= 5) {
                try { Thread.sleep(offset << 1); } catch (InterruptedException e) { Thread.currentThread().interrupt(); }
                timestamp = System.currentTimeMillis();
            }
            if (timestamp < lastTimestamp) {
                throw new IllegalStateException("Clock moved backwards. Refusing to generate id for " + (lastTimestamp - timestamp) + "ms");
            }
        }

        if (timestamp == lastTimestamp) {
            sequence = (sequence + 1) & sequenceMask;
            if (sequence == 0) {
                // 当前毫秒序列号用完，等待下一毫秒
                timestamp = tilNextMillis(lastTimestamp);
            }
        } else {
            sequence = 0L;
        }

        lastTimestamp = timestamp;
        return ((timestamp - twepoch) << timestampLeftShift)
                | ((long) datacenterId << datacenterIdShift)
                | ((long) workerId << workerIdShift)
                | sequence;
    }

    private long tilNextMillis(long lastTimestamp) {
        long timestamp = System.currentTimeMillis();
        while (timestamp <= lastTimestamp) {
            timestamp = System.currentTimeMillis();
        }
        return timestamp;
    }
}
