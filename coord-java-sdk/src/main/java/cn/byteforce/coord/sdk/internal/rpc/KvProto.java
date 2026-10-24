package cn.byteforce.coord.sdk.internal.rpc;

import cn.byteforce.coord.contracts.kv.v2.Kv;
import cn.byteforce.coord.sdk.kv.v2.KvClient;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.List;

/**
 * Shared {@code coord.kv.v2.KeyValue} conversions for the KV/Txn/Watch clients.
 * <p>
 * One mapping for all three surfaces: a per-client copy would let the watch
 * {@code prevKv} and transaction read-set views drift from the plain KV view.
 */
final class KvProto {

    private KvProto() {
    }

    static ByteString bytes(byte[] value) {
        return value == null || value.length == 0
                ? ByteString.EMPTY : ByteString.copyFrom(value);
    }

    static KvClient.KeyValue toSdk(Kv.KeyValue kv) {
        return new KvClient.KeyValue(
                kv.getKey().toByteArray(),
                kv.getValue().toByteArray(),
                kv.getCreateRevision(),
                kv.getModRevision(),
                kv.getVersion(),
                kv.getLeaseId());
    }

    static List<KvClient.KeyValue> toSdk(List<Kv.KeyValue> kvs) {
        List<KvClient.KeyValue> out = new ArrayList<>(kvs.size());
        for (Kv.KeyValue kv : kvs) {
            out.add(toSdk(kv));
        }
        return List.copyOf(out);
    }
}
