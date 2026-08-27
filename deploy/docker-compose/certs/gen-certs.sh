#!/usr/bin/env bash
# coord 集群 TLS/mTLS 证书生成脚本（compose / 测试用途）
#
# 产物（全部 PEM）：
#   ca.crt / ca.key         集群内部 CA
#   server.crt / server.key 三节点共用服务端证书（SAN：coord-1/2/3 + localhost + 127.0.0.1）
#   agent.crt / agent.key   coord-agent mTLS 客户端身份
#
# 生产环境请改用企业 CA / cert-manager / Vault 签发，勿使用本脚本产物。
set -euo pipefail
cd "$(dirname "$0")"

DAYS=3650

# 1. CA（幂等：已存在则复用）
if [ ! -f ca.crt ]; then
  openssl req -x509 -newkey rsa:2048 -nodes -keyout ca.key -out ca.crt -days "$DAYS" \
    -subj "/CN=coord-compose-ca"
fi

# 2. Server 证书（raft/gRPC 共用；extendedKeyUsage 含 clientAuth，供 join/raft mTLS 客户端身份）
cat > server.ext <<'EOF'
subjectAltName = DNS:coord-1,DNS:coord-2,DNS:coord-3,DNS:localhost,IP:127.0.0.1
extendedKeyUsage = serverAuth,clientAuth
EOF
openssl req -newkey rsa:2048 -nodes -keyout server.key -out server.csr -subj "/CN=coord" >/dev/null 2>&1
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out server.crt -days "$DAYS" -extfile server.ext

# 3. Agent 客户端证书（mTLS 身份，由同一 CA 签发）
cat > agent.ext <<'EOF'
extendedKeyUsage = clientAuth
EOF
openssl req -newkey rsa:2048 -nodes -keyout agent.key -out agent.csr -subj "/CN=coord-agent" >/dev/null 2>&1
openssl x509 -req -in agent.csr -CA ca.crt -CAkey ca.key -CAcreateserial \
  -out agent.crt -days "$DAYS" -extfile agent.ext

rm -f server.csr agent.csr server.ext agent.ext
echo "生成完成: ca.crt server.crt server.key agent.crt agent.key（已被 .gitignore 忽略，勿提交）"
