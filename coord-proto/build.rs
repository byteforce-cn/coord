fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .file_descriptor_set_path(
            std::path::PathBuf::from(std::env::var("OUT_DIR").unwrap())
                .join("coord_descriptor.bin"),
        )
        .compile_protos(
            &[
                "src/proto/kv.proto",
                "src/proto/txn.proto",
                "src/proto/lease.proto",
                "src/proto/watch.proto",
                "src/proto/maintenance.proto",
                "src/proto/storage.proto",
                "src/proto/raft.proto",
                "src/proto/auth.proto",
                "src/proto/capability.proto",
                // ── 契约面（contracts/v1.2.0）：每个 domain 一个文件，
                // package = coord.<domain>.v1，与 apis/contracts/proto/ 逐字一致
                "src/proto/registry.proto",
                "src/proto/lock.proto",
                "src/proto/election.proto",
                "src/proto/idgen.proto",
                "src/proto/event.proto",
                "src/proto/config.proto",
                "src/proto/pki.proto",
                "src/proto/policy.proto",
                "src/proto/circuitbreaker.proto",
                "src/proto/ratelimiter.proto",
                "src/proto/transit.proto",
                "src/proto/cache.proto",
                "src/proto/mq.proto",
                "src/proto/workflow.proto",
                "src/proto/scheduler.proto",
                "src/proto/featureflags.proto",
                // ── KV-T v2 契约面（package coord.<domain>.v2；ADR-0015）──
                // 路径与契约副本一致（apis/contracts/proto/coord/<domain>/v2/），
                // 两份副本逐字相同，便于 `check-wire-descriptor.sh` 按 package
                // 比对，且 v2 之间的 import 在两侧都解析到同一相对路径。
                "src/proto/coord/tso/v2/tso.proto",
                "src/proto/coord/kv/v2/kv.proto",
                "src/proto/coord/txn/v2/txn.proto",
                "src/proto/coord/watch/v2/watch.proto",
                "src/proto/coord/lease/v2/lease.proto",
                "src/proto/coord/storage/v2/storage.proto",
                // ── 内部面：Handshake / Health / Replica（不建对外契约包）
                "src/proto/agent_api.proto",
                "src/proto/plugin.proto",
            ],
            &["src/proto"],
        )?;
    Ok(())
}
