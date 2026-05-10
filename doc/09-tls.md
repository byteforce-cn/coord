# TLS / mTLS 配置指南

---

## 概述

coord 支持三种安全级别：

| 级别 | 配置 | 适用场景 |
|------|------|---------|
| 明文 | 无 TLS 参数 | 仅本地开发 |
| TLS（单向） | `--tls-cert` + `--tls-key` | 服务端身份验证 |
| mTLS（双向） | TLS + `--tls-client-ca` | 零信任生产环境 |

TLS 同时作用于 **gRPC 端口** 和 **HTTP 控制面端口**。

---

## 一、生成测试证书

### 使用 openssl（快速自签名）

```bash
mkdir -p certs && cd certs

# CA
openssl req -x509 -newkey rsa:4096 -keyout ca.key -out ca.crt \
  -days 3650 -nodes -subj "/CN=coord-ca"

# 服务端证书
openssl req -newkey rsa:4096 -keyout server.key -out server.csr \
  -nodes -subj "/CN=coord-server" \
  -addext "subjectAltName=DNS:coord.internal,DNS:localhost,IP:127.0.0.1"
openssl x509 -req -in server.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out server.crt -days 365 \
  -extensions san \
  -extfile <(printf "[san]\nsubjectAltName=DNS:coord.internal,DNS:localhost,IP:127.0.0.1")

# 客户端证书（mTLS 时使用）
openssl req -newkey rsa:4096 -keyout client.key -out client.csr \
  -nodes -subj "/CN=coord-client"
openssl x509 -req -in client.csr -CA ca.crt -CAkey ca.key \
  -CAcreateserial -out client.crt -days 365
```

### 使用 coord PKI 自签（服务已运行时）

```bash
# 获取 CA 链
coord-ctl pki ca-chain > certs/coord-ca.pem

# 签发服务端证书
coord-ctl --token <token> pki issue coord.internal \
  --san coord.internal --san localhost --san 127.0.0.1 \
  --ttl-seconds 31536000
```

---

## 二、服务端启用 TLS

### 单向 TLS（客户端验证服务端）

```bash
coord-server serve \
  --tls-cert certs/server.crt \
  --tls-key  certs/server.key \
  --grpc-addr 0.0.0.0:9090 \
  --http-addr 0.0.0.0:8080
```

环境变量方式：

```bash
COORD_TLS_CERT=certs/server.crt \
COORD_TLS_KEY=certs/server.key \
coord-server serve
```

### 双向 mTLS（客户端也需提供证书）

```bash
coord-server serve \
  --tls-cert       certs/server.crt \
  --tls-key        certs/server.key \
  --tls-client-ca  certs/ca.crt
```

配置 `--tls-client-ca` 后，所有 gRPC 和 HTTP 连接均要求客户端提供由该 CA 签名的证书；
未提供证书的请求直接被拒绝（TLS 握手失败）。

---

## 三、coord-ctl 连接 TLS 服务端

### 单向 TLS（自签名 CA）

```bash
coord-ctl \
  --endpoint https://coord.internal:9090 \
  --tls-ca certs/ca.crt \
  cluster status
```

### 双向 mTLS

```bash
coord-ctl \
  --endpoint   https://coord.internal:9090 \
  --tls-ca     certs/ca.crt \
  --tls-cert   certs/client.crt \
  --tls-key    certs/client.key \
  cluster status
```

### SNI / 域名覆盖（IP 访问时）

```bash
coord-ctl \
  --endpoint   https://10.0.0.1:9090 \
  --tls-ca     certs/ca.crt \
  --tls-domain coord.internal \
  cluster status
```

---

## 四、Docker Compose 中配置 TLS

```yaml
services:
  coord:
    image: nexus.byteforce.cn/image-private/coord:0.1.10
    command: ["serve"]
    ports:
      - "9090:9090"
      - "8080:8080"
    environment:
      COORD_GRPC_ADDR:     "0.0.0.0:9090"
      COORD_HTTP_ADDR:     "0.0.0.0:8080"
      COORD_DATA_DIR:      "/data"
      COORD_TLS_CERT:      "/certs/server.crt"
      COORD_TLS_KEY:       "/certs/server.key"
      COORD_TLS_CLIENT_CA: "/certs/ca.crt"   # mTLS（可选）
    volumes:
      - coord-data:/data
      - ./certs:/certs:ro
```

---

## 五、Kubernetes Secret 挂载

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: coord-tls
type: kubernetes.io/tls
data:
  tls.crt: <base64 server.crt>
  tls.key: <base64 server.key>
---
# 在 Deployment 中挂载
volumes:
  - name: coord-tls
    secret:
      secretName: coord-tls
containers:
  - name: coord
    env:
      - name: COORD_TLS_CERT
        value: /certs/tls.crt
      - name: COORD_TLS_KEY
        value: /certs/tls.key
    volumeMounts:
      - name: coord-tls
        mountPath: /certs
        readOnly: true
```

---

## 六、常见问题

**Q: 启动报 `invalid tonic TLS config`**  
A: 检查 `--tls-cert` 和 `--tls-key` 是否匹配；证书格式必须为 PEM，不支持 DER。

**Q: coord-ctl 报 `certificate verify failed`**  
A: 传入 `--tls-ca` 指定签发服务端证书的 CA 文件。

**Q: 配置了 `--tls-*` 但端点用的是 `http://`**  
A: coord-ctl 会报错拒绝连接，防止静默降级。将端点改为 `https://`。

**Q: mTLS 握手失败 `required client certificate`**  
A: 服务端配置了 `--tls-client-ca`，客户端需同时提供 `--tls-cert` 和 `--tls-key`。
