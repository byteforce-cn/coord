package cn.byteforce.coord.example;

import com.google.protobuf.ByteString;

/**
 * 前缀扫描的 {@code range_end} 计算（本示例内**唯一**的定义）。
 *
 * <p><b>为什么需要它</b>：{@code setRangeEnd(prefix + "\0")} 是错的——
 * {@code prefix + "\0"} 是 {@code prefix} 的**最小后继**（0x00 是最小字节），
 * 区间 {@code [prefix, prefix + "\0")} 只包含 {@code prefix} 这一个 key，因此
 * 匹配 **0 条**。第四轮 §3.14.6 记录了两个受害者：
 * {@code ConfigClient.getAll()} 永远返回空 map、
 * {@code ServiceRegistry.discover()} 永远返回空列表。
 *
 * <p>正确做法与 Rust 侧 {@code coord-agent/src/services/workflow_store.rs::prefix_end()}
 * 一致：把**最后一个字节 +1**：
 * <pre>
 *   "/test/kv/prefix/"  →  "/test/kv/prefix0"
 * </pre>
 * 于是 {@code [prefix, end)} 恰好覆盖所有以 {@code prefix} 开头的 key。
 *
 * <p>{@code prefix} 全为 0xFF 时无有限上界（返回空 ByteString = "到无穷"，
 * 与 Rust 侧 {@code prefix_successor()} 的 {@code None} 同义）。
 *
 * <p><b>位置说明</b>：本类原先只存在于 {@code src/test/java}，因此主代码
 * （{@code CoordClient.scan()}）无从复用，只能自己再算一遍——两份实现正是这个 bug
 * 能长期存在的原因。现移至 {@code src/main/java}，测试与主代码共用同一实现。
 */
final class PrefixScan {

    private PrefixScan() {
    }

    /** 计算前缀扫描的 range_end（末字节 +1）。 */
    static ByteString end(String prefix) {
        byte[] bytes = prefix.getBytes(java.nio.charset.StandardCharsets.UTF_8);
        for (int i = bytes.length - 1; i >= 0; i--) {
            if ((bytes[i] & 0xFF) < 0xFF) {
                bytes[i] = (byte) (bytes[i] + 1);
                return ByteString.copyFrom(bytes, 0, i + 1);
            }
        }
        // 全 0xFF → 无有限上界（空 range_end = 到无穷）
        return ByteString.EMPTY;
    }

    /** 计算前缀扫描的 range_end（末字节 +1）。 */
    static ByteString end(ByteString prefix) {
        return end(prefix.toStringUtf8());
    }
}
