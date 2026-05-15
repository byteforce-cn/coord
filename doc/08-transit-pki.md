# Transit 加密与 PKI

---

## 一、Transit — 信封加密

Transit 提供托管密钥的加密 / 解密 / HMAC 服务，私钥始终保留在 coord 内部，不对外暴露。

### 创建密钥

```bash
coord ctl --token <token> transit create-key my-key
```

### 加密 / 解密

```bash
# 加密
coord ctl --token <token> transit encrypt my-key "sensitive data"
# → vault:v1:AAABBBCCC...

# 解密
coord ctl --token <token> transit decrypt my-key "vault:v1:AAABBBCCC..."
# → sensitive data
```

密文格式 `vault:v<version>:<base64_ciphertext>`，版本号对应密钥版本。

### 密钥轮换

```bash
coord ctl --token <token> transit rotate-key my-key
```

轮换后：
- 新加密使用最新版本密钥。
- 旧密文（v1、v2…）仍可解密（向后兼容）。

### HMAC

```bash
# 签名
coord ctl --token <token> transit hmac-sign my-hmac-key "payload"
# → hmac:v1:AAAA...

# 验证
coord ctl --token <token> transit hmac-verify my-hmac-key "payload" "hmac:v1:AAAA..."
# → valid: true
```

### Java SDK 示例

```java
TransitServiceBlockingStub transit = TransitServiceGrpc.newBlockingStub(channel)
    .withCallCredentials(new TokenCallCredentials(token));

// 加密
EncryptResponse resp = transit.encrypt(EncryptRequest.newBuilder()
    .setKeyName("my-key")
    .setPlaintext(ByteString.copyFromUtf8("sensitive data"))
    .build());
String ciphertext = resp.getCiphertext();

// 解密
DecryptResponse dec = transit.decrypt(DecryptRequest.newBuilder()
    .setKeyName("my-key")
    .setCiphertext(ciphertext)
    .build());
```

---

## 二、PKI — 内部 CA

coord 内置轻量级 CA，为微服务颁发短期 TLS 证书，无需依赖外部 PKI 基础设施。

### 颁发证书

```bash
coord ctl --token <token> pki issue api.internal \
  --san api.internal \
  --san 127.0.0.1 \
  --ttl-seconds 86400 \
  --auto-renew
```

输出：

```
serial_number: 01:AB:CD:...
not_before:    2026-05-15T00:00:00Z
not_after:     2026-05-16T00:00:00Z
certificate:   -----BEGIN CERTIFICATE-----
               ...
               -----END CERTIFICATE-----
ca_chain:      -----BEGIN CERTIFICATE-----
               ...
               -----END CERTIFICATE-----
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--san` | — | Subject Alternative Name（可重复） |
| `--ttl-seconds` | `86400` | 证书有效期（秒） |
| `--auto-renew` | `false` | 到期前自动续期 |
| `--renew-before-seconds` | `3600` | 到期前多少秒触发续期 |

### 续期证书

```bash
coord ctl --token <token> pki renew <serial_number> --ttl-seconds 86400
```

### 吊销证书

```bash
coord ctl --token <token> pki revoke <serial_number> --reason key-compromise
```

吊销原因（`--reason`）可选值：`unspecified` · `key-compromise` · `ca-compromise` · `affiliation-changed` · `superseded` · `cessation-of-operation`

### 获取 CA 链

```bash
coord ctl pki ca-chain
# 输出 PEM 格式的 CA 证书链
```

### 获取 CRL

```bash
coord ctl pki crl
# 输出 PEM 格式的证书吊销列表
```

### OCSP 查询

```bash
coord ctl pki ocsp <serial_number>
```

---

## 三、ACME 流程

coord 支持简化版 ACME HTTP-01 challenge，适用于内部 DNS 场景。

```bash
# 1. 创建订单
coord ctl --token <token> pki acme-order \
  --domain api.internal \
  --domain www.api.internal \
  --ttl-seconds 86400

# 2. 完成 HTTP-01 challenge（在目标服务暴露 /.well-known/acme-challenge/<token>）
coord ctl --token <token> pki acme-challenge <order_id> api.internal <challenge_token>

# 3. 颁发证书
coord ctl --token <token> pki acme-finalize <order_id>
```

---

## 四、自动续期

设置自动续期策略后，coord 内部定时任务会在证书到期前自动续期：

```bash
# 设置策略
coord ctl --token <token> pki set-auto-renew-policy <serial_number> \
  --enabled true \
  --renew-before-seconds 3600

# 手动触发（调试用）
coord ctl --token <token> pki run-auto-renew
```
