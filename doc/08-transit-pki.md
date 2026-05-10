# Transit 加密 & PKI 指南

---

## Transit 加密

Transit 是 coord 的加密即服务（Encryption-as-a-Service）模块。应用不存储密钥材料，
只与 coord 通信完成加解密操作，密钥集中管理、轮换和审计。

### 能力要求

| 操作 | 需要 policy |
|------|------------|
| 加密 | `transit.encrypt` |
| 解密 | `transit.decrypt` |
| HMAC 签名/验证 | `transit.sign` |
| 密钥管理（创建/轮换） | `*`（root） |

---

### 创建密钥

```bash
coord-ctl --token <root_token> transit create-key database-key
# → key_name: database-key, primary_version: 1
```

---

### 加密

```bash
# 加密任意字符串
coord-ctl --token <app_token> transit encrypt database-key "user-password-hash"
# → ciphertext: vault:v1:xyzABCDEF...
#    version: 1
```

密文格式 `vault:v{version}:{base64}` — 版本号用于轮换后的自动降级解密。

---

### 解密

```bash
coord-ctl --token <app_token> transit decrypt database-key "vault:v1:xyzABCDEF..."
# → plaintext_utf8: user-password-hash
#    plaintext_base64: dXNlci1wYXNzd29yZC1oYXNo
```

---

### 密钥轮换

```bash
coord-ctl --token <root_token> transit rotate-key database-key
# → primary_version: 2
```

轮换后：
- 新加密使用 v2 密钥
- 旧密文（`vault:v1:...`）仍可用 v1 密钥解密

---

### HMAC 签名与验证

```bash
# 签名
coord-ctl --token <app_token> transit hmac-sign database-key "webhook-payload"
# → signature: hmac:v1:ABCDEF..., version: 1

# 验证
coord-ctl --token <app_token> transit hmac-verify database-key \
  "webhook-payload" "hmac:v1:ABCDEF..."
# → ok: true
```

---

### 典型应用场景

**数据库字段加密**：
```
应用 → transit encrypt → 存储密文 → 读取时 transit decrypt
```

**Webhook 签名验证**：
```
服务端 → transit hmac-sign → 发送给接收方
接收方 → transit hmac-verify → 验证来源合法
```

---

## PKI 证书服务

coord 内置私有 CA，提供证书签发、自动续期、吊销、CRL、OCSP、ACME 支持。

### 能力要求

| 操作 | 需要 policy |
|------|------------|
| 签发证书 | `pki.issue` |
| 吊销证书 | `pki.revoke` |
| 查看 CA 链 / CRL / OCSP | 无需 token |

---

### 签发叶证书

```bash
coord-ctl --token <app_token> pki issue svc-a.internal \
  --san svc-a.internal \
  --san svc-a \
  --san 10.0.0.1 \
  --ttl-seconds 86400 \
  --auto-renew \
  --renew-before-seconds 3600
```

输出包含：
- `serial_number`
- `certificate_pem`（叶证书）
- `private_key_pem`（私钥）
- `ca_certificate_pem`（CA 证书链）

将 `certificate_pem` + `private_key_pem` 写入文件供服务使用：

```bash
coord-ctl --token <app_token> pki issue svc-a.internal \
  --san svc-a.internal \
  --ttl-seconds 86400 \
  | tee /dev/stdout \
  | grep -A9999 'certificate_pem:' | tail -n+2 > /etc/certs/svc-a.crt
```

---

### 续期证书

```bash
coord-ctl --token <app_token> pki renew <serial_number> \
  --ttl-seconds 86400
```

---

### 吊销证书

```bash
coord-ctl --token <app_token> pki revoke <serial_number> \
  --reason key-compromise
```

---

### 获取 CA 证书链

```bash
coord-ctl pki ca-chain > /etc/certs/coord-ca.pem
```

将此 CA 配置到需要信任 coord 颁发证书的服务（如 mTLS 场景）。

---

### 证书吊销列表（CRL）

```bash
# 获取 PEM 格式 CRL
coord-ctl pki crl --next-update-seconds 600 > /etc/certs/coord.crl

# 验证某证书是否在吊销列表中
openssl verify -CAfile coord-ca.pem -CRLfile coord.crl \
  -crl_check svc-a.crt
```

---

### OCSP 单证书状态查询

```bash
coord-ctl pki ocsp <serial_number>
```

---

### 自动续期

设置 `--auto-renew` 后，coord-server 会在 `renew_before_seconds` 窗口内自动签发新证书。
可通过以下命令手动触发：

```bash
coord-ctl --token <root_token> pki run-auto-renew
```

更新单张证书策略：

```bash
coord-ctl --token <root_token> pki set-auto-renew-policy <serial> \
  --enabled true \
  --renew-before-seconds 7200
```

---

### ACME（Let's Encrypt 兼容）工作流

```bash
# 1. 创建订单
coord-ctl --token <app_token> pki acme-order \
  --domain example.com \
  --domain www.example.com \
  --ttl-seconds 7776000 \
  --challenge-type http-01 \
  --auto-renew

# 2. 将 token 部署到 http://<domain>/.well-known/acme-challenge/<token>
coord-ctl --token <app_token> pki acme-challenge \
  <order_id> example.com <challenge_token>

# 3. Finalize 获取证书
coord-ctl --token <app_token> pki acme-finalize <order_id> \
  --common-name example.com
```

---

### 在微服务 mTLS 场景中使用 coord PKI

```bash
# 服务 A 获取证书
coord-ctl --token <svc-a-token> pki issue svc-a.internal \
  --san svc-a.internal --ttl-seconds 86400

# 服务 B 获取证书
coord-ctl --token <svc-b-token> pki issue svc-b.internal \
  --san svc-b.internal --ttl-seconds 86400

# 双方都信任 coord CA，从而实现 mTLS
coord-ctl pki ca-chain > /etc/certs/coord-ca.pem
```

应用配置 TLS 时：
- 证书：来自 `pki issue` 的 `certificate_pem`
- 私钥：来自 `pki issue` 的 `private_key_pem`
- CA 信任链：来自 `pki ca-chain` 的输出
