#!/usr/bin/env bash
# Generate a self-signed CA + per-node server certs + a client cert for e2e TLS.
#
# Output layout (all PEM):
#   tls/
#     ca.crt          root CA certificate (distributed to all clients)
#     ca.key          root CA private key (server-only)
#     coord-1.crt     server cert, SAN = DNS:coord-1, DNS:localhost, IP:127.0.0.1
#     coord-1.key
#     coord-2.crt     (SAN = DNS:coord-2, DNS:localhost, IP:127.0.0.1)
#     coord-2.key
#     coord-3.crt
#     coord-3.key
#     client.crt      client cert for mTLS (CN = coord-e2e-client)
#     client.key
#
# Idempotent: re-running regenerates all material. Run from the repo root
# or from e2e/; output path is always e2e/tls/.

set -euo pipefail

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/tls"
mkdir -p "${OUT_DIR}"
cd "${OUT_DIR}"

CA_DAYS=3650
LEAF_DAYS=825

if ! command -v openssl >/dev/null 2>&1; then
  echo "openssl is required on PATH" >&2
  exit 1
fi

echo "==> Generating CA (${OUT_DIR}/ca.{crt,key})"
openssl genrsa -out ca.key 4096 2>/dev/null
openssl req -x509 -new -nodes -key ca.key -sha256 -days "${CA_DAYS}" \
  -subj "/CN=coord-e2e-ca/O=coord/OU=e2e" \
  -out ca.crt

gen_server_cert() {
  local name="$1"
  echo "==> Generating server cert for ${name}"
  openssl genrsa -out "${name}.key" 2048 2>/dev/null
  cat > "${name}.ext" <<EOF
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = serverAuth, clientAuth
subjectAltName = @alt_names

[alt_names]
DNS.1 = ${name}
DNS.2 = localhost
IP.1  = 127.0.0.1
EOF
  openssl req -new -key "${name}.key" \
    -subj "/CN=${name}/O=coord/OU=e2e" \
    -out "${name}.csr"
  openssl x509 -req -in "${name}.csr" -CA ca.crt -CAkey ca.key \
    -CAcreateserial -out "${name}.crt" -days "${LEAF_DAYS}" -sha256 \
    -extfile "${name}.ext" 2>/dev/null
  rm -f "${name}.csr" "${name}.ext"
}

for node in coord-1 coord-2 coord-3; do
  gen_server_cert "${node}"
done

echo "==> Generating client cert (CN=coord-e2e-client)"
openssl genrsa -out client.key 2048 2>/dev/null
cat > client.ext <<'EOF'
authorityKeyIdentifier=keyid,issuer
basicConstraints=CA:FALSE
keyUsage = digitalSignature, keyEncipherment
extendedKeyUsage = clientAuth
EOF
openssl req -new -key client.key \
  -subj "/CN=coord-e2e-client/O=coord/OU=e2e" \
  -out client.csr
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out client.crt -days "${LEAF_DAYS}" -sha256 \
  -extfile client.ext 2>/dev/null
rm -f client.csr client.ext ca.srl

chmod 600 *.key
chmod 644 *.crt

echo
echo "==> Done. Certificates in ${OUT_DIR}"
ls -l "${OUT_DIR}"
