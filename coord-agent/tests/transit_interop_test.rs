// 互操作验收（ADR-0019 D4）：coord 产出的签名必须能被 openssl 独立验证。
//
// 自己用自己的库验自己不算验收；本测试把私钥导出到临时目录，用 openssl 生成
// 公钥并验签。openssl 不可用时跳过（CI 的 ubuntu runner 自带 openssl 3.x）。

use std::process::Command;
use std::sync::Arc;

use coord_agent::{CreateKeySpec, KeyType, MaterialFormat, SignSpec, TransitKeyRegistry};

fn openssl_available() -> bool {
    Command::new("openssl")
        .arg("version")
        .output()
        .map(|out| out.status.success())
        .unwrap_or(false)
}

fn run(args: &[&str], cwd: &std::path::Path) {
    let output = Command::new("openssl")
        .args(args)
        .current_dir(cwd)
        .output()
        .expect("run openssl");
    assert!(
        output.status.success(),
        "openssl {:?} failed: {}{}",
        args,
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&output.stderr)
    );
}

async fn export_private_key_pem(keys: &TransitKeyRegistry, name: &str) -> String {
    let exported = keys.export_key(name, 0).await.expect("export");
    assert_eq!(exported.format, "pem");
    assert!(exported.contains_private_key);
    String::from_utf8(exported.material).expect("pem utf8")
}

async fn verify_with_openssl(
    dir: &std::path::Path,
    key_type: KeyType,
    signature_algorithm: &str,
    hash_algorithm: &str,
) {
    let keys = TransitKeyRegistry::in_memory().expect("registry");
    let name = "interop";
    keys.create_key(CreateKeySpec {
        name: name.into(),
        key_type,
        exportable: true,
    })
    .await
    .expect("create key");

    let message = b"coord interop message";
    // Ed25519 不接受 hash_algorithm（EdDSA 签全文）
    let hash_algorithm_arg = if matches!(key_type, KeyType::Ed25519) {
        None
    } else {
        Some(hash_algorithm.into())
    };
    let signed = keys
        .sign(SignSpec {
            name: name.into(),
            version: 0,
            input: message.to_vec(),
            prehashed: false,
            hash_algorithm: hash_algorithm_arg,
            signature_algorithm: Some(signature_algorithm.into()),
        })
        .await
        .expect("sign");

    std::fs::write(dir.join("message.bin"), message).unwrap();
    std::fs::write(dir.join("signature.bin"), &signed.signature).unwrap();
    std::fs::write(
        dir.join("key.pem"),
        export_private_key_pem(&keys, name).await,
    )
    .unwrap();

    run(
        &["pkey", "-in", "key.pem", "-pubout", "-out", "pub.pem"],
        dir,
    );

    match key_type {
        KeyType::Ed25519 => {
            // Ed25519 是纯签名（无摘要参数）：pkeyutl -rawin
            run(
                &[
                    "pkeyutl",
                    "-verify",
                    "-pubin",
                    "-inkey",
                    "pub.pem",
                    "-rawin",
                    "-in",
                    "message.bin",
                    "-sigfile",
                    "signature.bin",
                ],
                dir,
            );
        }
        KeyType::Rsa2048 | KeyType::Rsa3072 | KeyType::Rsa4096 if signature_algorithm == "pss" => {
            let digest = format!("-{hash_algorithm}");
            run(
                &[
                    "dgst",
                    &digest,
                    "-verify",
                    "pub.pem",
                    "-signature",
                    "signature.bin",
                    "-sigopt",
                    "rsa_padding_mode:pss",
                    "-sigopt",
                    "rsa_pss_saltlen:digest",
                    "message.bin",
                ],
                dir,
            );
        }
        _ => {
            let digest = format!("-{hash_algorithm}");
            run(
                &[
                    "dgst",
                    &digest,
                    "-verify",
                    "pub.pem",
                    "-signature",
                    "signature.bin",
                    "message.bin",
                ],
                dir,
            );
        }
    }
}

#[tokio::test]
async fn openssl_verifies_ed25519_ecdsa_and_rsa() {
    if !openssl_available() {
        eprintln!("skip: openssl not available");
        return;
    }
    let cases = [
        (KeyType::Ed25519, "", "sha256"),
        (KeyType::EcdsaP256, "ecdsa", "sha256"),
        (KeyType::EcdsaP384, "ecdsa", "sha384"),
        (KeyType::EcdsaP521, "ecdsa", "sha512"),
        (KeyType::Rsa2048, "pkcs1v15", "sha256"),
        (KeyType::Rsa2048, "pss", "sha256"),
    ];
    for (index, (key_type, signature_algorithm, hash_algorithm)) in cases.iter().enumerate() {
        let dir = tempfile::tempdir().expect("tempdir");
        verify_with_openssl(dir.path(), *key_type, signature_algorithm, hash_algorithm).await;
        eprintln!(
            "openssl verified case {index}: {key_type:?} {signature_algorithm} {hash_algorithm}"
        );
    }
}

/// 导入：coord 能读回 openssl 生成的 PKCS#8 私钥并签名（反向互操作）。
#[tokio::test]
async fn imports_openssl_generated_pkcs8_key() {
    if !openssl_available() {
        eprintln!("skip: openssl not available");
        return;
    }
    let dir = tempfile::tempdir().expect("tempdir");
    run(
        &[
            "genpkey",
            "-algorithm",
            "EC",
            "-pkeyopt",
            "ec_paramgen_curve:P-256",
            "-out",
            "key.pem",
        ],
        dir.path(),
    );
    let pem = std::fs::read(dir.path().join("key.pem")).unwrap();

    let keys = Arc::new(TransitKeyRegistry::in_memory().expect("registry"));
    keys.import_key(coord_agent::ImportKeySpec {
        name: "imported".into(),
        key_type: KeyType::EcdsaP256,
        material: pem,
        format: Some(MaterialFormat::Pem),
        exportable: false,
    })
    .await
    .expect("import openssl key");

    let signed = keys
        .sign(SignSpec {
            name: "imported".into(),
            version: 0,
            input: b"imported signing".to_vec(),
            prehashed: false,
            hash_algorithm: None,
            signature_algorithm: None,
        })
        .await
        .expect("sign with imported key");
    assert!(!signed.signature.is_empty());
}
