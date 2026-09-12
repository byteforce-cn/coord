package cn.byteforce.coord.example;

import com.google.protobuf.ByteString;

/**
 * 前缀扫描的 range_end 计算（与仓库其余部分的惯用法一致）。
 *
 * <p><b>为什么需要它</b>：本目录的集成测试此前统一写作
 * {@code setRangeEnd(prefix + "\0")}，并期望它等于"扫描整个前缀"。
 * 这在 etcd 语义里是错的——{@code prefix + "\0"} 是 {@code prefix} 的**最小后继**
 * （0x00 是最小字节），区间 {@code [prefix, prefix + "\0")} 只包含
 * {@code prefix} 这一个 key 本身，因此实际匹配 0 条。
 * 由于这批测试从未在 CI 中执行（见 D3），这个错误预期一直没被发现。
 *
 * <p>正确做法与 {@code coord-agent/src/services/workflow_store.rs::prefix_end()} 一致：
 * 把**最后一个字节 +1**：
 * <pre>
 *   "/test/kv/prefix/"  →  "/test/kv/prefix0"
 * </pre>
 * 于是 {@code [prefix, end)} 恰好覆盖所有以 {@code prefix} 开头的 key。
 *
 * <p>{@code prefix} 全为 0xFF 时无有限上界（返回空 ByteString = "到无穷"，
 * 与 Rust 侧 {@code prefix_end()} 的约定一致）。
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
