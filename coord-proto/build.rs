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
                // ── 内部面：Handshake / Health / Replica（不建对外契约包）
                "src/proto/agent_api.proto",
                "src/proto/plugin.proto",
            ],
            &["src/proto"],
        )?;
    Ok(())
}
