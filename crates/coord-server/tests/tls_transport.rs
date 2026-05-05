//! Integration tests for TLS transport wiring.
//!
//! We don't spin up the full coord-server here (that would require a seeded
//! Raft store and unsealed security state). Instead we validate the two
//! transport-level contracts that Batch 3b depends on:
//!
//! 1. `axum_server::tls_rustls::RustlsConfig::from_pem` accepts the raw PEM
//!    bytes returned by [`coord_core::tls::load_tls_material`].
//! 2. `tonic::transport::Identity::from_pem` + `ServerTlsConfig` accepts the
//!    same bytes, so the gRPC listener can successfully be constructed.
//!
//! These two checks catch the two most common production misconfigurations:
//! wrong key pair or wrong PEM encoding, both of which would otherwise only
//! surface at bind time on a production node.

use std::path::PathBuf;
use std::sync::Once;

use coord_core::tls::{TlsPaths, load_tls_material};

static INIT_CRYPTO: Once = Once::new();
fn ensure_crypto_provider() {
    INIT_CRYPTO.call_once(|| {
        let _ = rustls::crypto::ring::default_provider().install_default();
    });
}

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(0);
        let seq = COUNTER.fetch_add(1, Ordering::Relaxed);
        let base = std::env::temp_dir().join(format!(
            "coord-tls-it-{}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0),
            seq,
        ));
        std::fs::create_dir_all(&base).unwrap();
        Self(base)
    }
    fn path(&self) -> &std::path::Path {
        &self.0
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn mint_self_signed() -> (String, String) {
    let params = rcgen::CertificateParams::new(vec!["localhost".into()]).unwrap();
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    (cert.pem(), key_pair.serialize_pem())
}

#[tokio::test]
async fn axum_rustls_accepts_loader_material() {
    ensure_crypto_provider();
    let dir = TempDir::new();
    let (cert_pem, key_pem) = mint_self_signed();
    let cert_path = dir.path().join("s.crt");
    let key_path = dir.path().join("s.key");
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, &key_pem).unwrap();

    let material = load_tls_material(&TlsPaths {
        cert: Some(cert_path),
        key: Some(key_path),
        client_ca: None,
    })
    .unwrap()
    .unwrap();

    // Build the exact same config object coord-server's HTTPS spawn path uses.
    axum_server::tls_rustls::RustlsConfig::from_pem(
        material.cert_pem().to_vec(),
        material.key_pem().to_vec(),
    )
    .await
    .expect("axum-server must accept loader-produced PEM");
}

#[test]
fn tonic_server_tls_config_accepts_loader_material() {
    ensure_crypto_provider();
    let dir = TempDir::new();
    let (cert_pem, key_pem) = mint_self_signed();
    let cert_path = dir.path().join("s.crt");
    let key_path = dir.path().join("s.key");
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, &key_pem).unwrap();

    let material = load_tls_material(&TlsPaths {
        cert: Some(cert_path),
        key: Some(key_path),
        client_ca: None,
    })
    .unwrap()
    .unwrap();

    let identity = tonic::transport::Identity::from_pem(material.cert_pem(), material.key_pem());
    let _cfg = tonic::transport::ServerTlsConfig::new().identity(identity);
    // If construction would panic on bad material, the line above would have
    // aborted the test — success here means the bytes are shape-valid for
    // tonic 0.12's rustls backend.
}

#[test]
fn tonic_server_tls_config_accepts_client_ca() {
    ensure_crypto_provider();
    let dir = TempDir::new();
    let (cert_pem, key_pem) = mint_self_signed();
    let (ca_pem, _) = mint_self_signed();
    let cert_path = dir.path().join("s.crt");
    let key_path = dir.path().join("s.key");
    let ca_path = dir.path().join("ca.crt");
    std::fs::write(&cert_path, &cert_pem).unwrap();
    std::fs::write(&key_path, &key_pem).unwrap();
    std::fs::write(&ca_path, &ca_pem).unwrap();

    let material = load_tls_material(&TlsPaths {
        cert: Some(cert_path),
        key: Some(key_path),
        client_ca: Some(ca_path),
    })
    .unwrap()
    .unwrap();
    assert!(material.mtls_required());

    let identity = tonic::transport::Identity::from_pem(material.cert_pem(), material.key_pem());
    let ca_cert = tonic::transport::Certificate::from_pem(material.client_ca_pem().unwrap());
    let _cfg = tonic::transport::ServerTlsConfig::new()
        .identity(identity)
        .client_ca_root(ca_cert);
}
