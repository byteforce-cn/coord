# TLS / mTLS 配置

---

## 一、服务端 TLS

在 `coord server` / `coord dev` 上启用 TLS：

```bash
coord server \
  --tls-cert /certs/server.crt \
  --tls-key  /certs/server.key
```

或通过环境变量：

```bash
COORD_TLS_CERT=/certs/server.crt
COORD_TLS_KEY=/certs/server.key
```

---

## 二、双向 TLS（mTLS）

额外指定客户端 CA，启用 mTLS：

```bash
coord server \
  --tls-cert      /certs/server.crt \
  --tls-key       /certs/server.key \
  --tls-client-ca /certs/ca.crt
```

启用 mTLS 后，所有 gRPC 连接必须提供由 CA 签发的客户端证书。

---

## 三、coord ctl 连接 TLS 端点

```bash
# 服务端自签名 CA
coord ctl \
  --endpoint https://coord:9090 \
  --tls-ca /certs/ca.crt \
  cluster status

# mTLS
coord ctl \
  --endpoint  https://coord:9090 \
  --tls-ca    /certs/ca.crt \
  --tls-cert  /certs/client.crt \
  --tls-key   /certs/client.key \
  cluster status

# 覆盖 SNI
coord ctl \
  --endpoint   https://127.0.0.1:9090 \
  --tls-ca     /certs/ca.crt \
  --tls-domain coord.internal \
  cluster status
```

也可通过环境变量：

```bash
export COORD_TLS_CA=/certs/ca.crt
export COORD_TLS_CERT=/certs/client.crt
export COORD_TLS_KEY=/certs/client.key
```

---

## 四、使用 coord PKI 颁发的证书

coord 内置 CA 可为节点间通信颁发证书，形成自举 mTLS 环境：

```bash
# 用 root token 颁发服务端证书
coord ctl --token <root_token> pki issue coord.internal \
  --san coord.internal \
  --san 127.0.0.1 \
  --ttl-seconds 2592000 \
  > /tmp/cert-bundle.txt

# 分别提取证书和私钥（实际场景通过 SDK 直接获取）
```

详细 PKI 操作见 [08-transit-pki.md](08-transit-pki.md)。

---

## 五、Docker Compose 示例

```yaml
services:
  coord-1:
    image: nexus.byteforce.cn/image-private/coord:0.1.14
    command: ["server"]
    environment:
      COORD_TLS_CERT: "/certs/server.crt"
      COORD_TLS_KEY: "/certs/server.key"
      COORD_TLS_CLIENT_CA: "/certs/ca.crt"
    volumes:
      - ./certs:/certs:ro
```

---

## 六、证书格式要求

- 所有证书和密钥使用 **PEM** 格式。
- 服务端证书的 SAN 应包含节点的 DNS 名称和 IP 地址。
- 客户端 CA（mTLS）只需包含 CA 根证书，不需要包含客户端证书本身。
