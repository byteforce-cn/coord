# coord-ctl 命令参考

`coord-ctl` 是 coord 的命令行管理工具，通过 gRPC 与 `coord-server` 通信。

---

## 全局选项

```
coord-ctl [全局选项] <命令> [子命令] [参数]
```

| 选项 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--endpoint <URL>` | — | `http://127.0.0.1:9090` | 服务端 gRPC 地址 |
| `--token <TOKEN>` | — | — | 请求鉴权 token（需要能力授权的端点必填） |
| `--tls-ca <PATH>` | `COORD_TLS_CA` | — | PEM CA 证书（自签名/私有 CA 时验证服务端证书） |
| `--tls-cert <PATH>` | `COORD_TLS_CERT` | — | PEM 客户端证书（mTLS） |
| `--tls-key <PATH>` | `COORD_TLS_KEY` | — | PEM 客户端私钥（mTLS） |
| `--tls-domain <DOMAIN>` | `COORD_TLS_DOMAIN` | 端点 hostname | SNI / 证书验证域名覆盖 |

> TLS 在以下情况自动启用：端点为 `https://`，或提供了任何 `--tls-*` 参数。

---

## cluster — 集群管理

### `cluster status`

查询节点状态和集群成员。

```bash
coord-ctl cluster status
```

输出示例：

```
node_id: node-1
state:   Leader
dev_mode: true
members: node-1, node-2, node-3
```

---

## member — Raft 成员管理

### `member add <NODE_ID> <ADDRESS>`

将新节点加入集群（通常由 auto-join 自动处理）。

```bash
coord-ctl member add node-4 coord-4:9090
```

### `member remove <NODE_ID>`

优雅移除节点。

```bash
coord-ctl member remove node-4

# 节点已不可达时强制移除
coord-ctl member remove node-4 --force-unreachable
```

---

## operator — 安全控制面

### `operator init`

初始化安全域，返回 Shamir shares 和 root_token。**每个集群生命周期只执行一次**。

```bash
coord-ctl operator init \
  --shares-total 5 \
  --threshold 3
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--shares-total` | `5` | 总份额数 |
| `--threshold` | `3` | 解封最低份额数 |

输出：

```
initialized: true
sealed:      true
shares_total: 5
threshold:   3
unseal_shares:
shamir:AAAAAAA...
shamir:BBBBBBB...
shamir:CCCCCCC...
shamir:DDDDDDD...
shamir:EEEEEEE...
root_token: s.xxxxxxxxxxxxxxxxxxxxxxxx
```

> ⚠️ **立即保存**上述输出。丢失 threshold 个以上 share 将导致安全域中的数据永久不可恢复。

### `operator seal-status`

查询当前 seal 状态。

```bash
coord-ctl operator seal-status
```

### `operator seal`

立即密封安全域（需要 root_token）。

```bash
coord-ctl --token <root_token> operator seal
```

### `operator unseal <SHARE>`

提交一个 Shamir share。达到 threshold 后自动解封。

```bash
coord-ctl operator unseal shamir:AAAAAAA...
# → sealed: true, progress: 1/3

coord-ctl operator unseal shamir:BBBBBBB...
# → sealed: true, progress: 2/3

coord-ctl operator unseal shamir:CCCCCCC...
# → sealed: false, progress: 0
```

### `operator rotate-root-key`

在已解封状态下轮换根密钥，返回新的 shares（旧 shares 立即失效）。

```bash
coord-ctl --token <root_token> operator rotate-root-key \
  --shares-total 5 \
  --threshold 3
```

---

## auth — 身份认证

### `auth approle create <ROLE_NAME>`

创建 AppRole。

```bash
coord-ctl --token <root_token> auth approle create order-svc \
  --policy transit.encrypt \
  --policy config.read \
  --token-ttl-seconds 3600 \
  --secret-id-ttl-seconds 86400 \
  --secret-id-num-uses 10
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--policy` | （必填，可多次） | 附加能力策略 |
| `--token-ttl-seconds` | `3600` | 登录后 token 有效期（秒） |
| `--secret-id-ttl-seconds` | `86400` | SecretId 有效期（秒） |
| `--secret-id-num-uses` | `10` | SecretId 最多使用次数（0 = 无限制） |

输出：

```
role_id: uuid-xxx
role_name: order-svc
policies: transit.encrypt, config.read
```

### `auth approle generate-secret-id <ROLE_ID>`

为指定 role 生成一次性 SecretId。

```bash
coord-ctl --token <root_token> auth approle generate-secret-id <role_id>
```

输出：

```
role_id: uuid-xxx
secret_id: sid-xxx
expires_unix_seconds: 1700000000
```

### `auth approle login <ROLE_ID> <SECRET_ID>`

用 role_id + secret_id 换取 access_token。

```bash
coord-ctl auth approle login <role_id> <secret_id>
```

输出：

```
access_token: tok-xxx
role_id: uuid-xxx
policies: transit.encrypt, config.read
expires_unix_seconds: 1700003600
```

### `auth approle lookup <ACCESS_TOKEN>`

验证并查看 token 信息。

```bash
coord-ctl auth approle lookup <access_token>
```

### `auth approle revoke <ACCESS_TOKEN>`

吊销 token。

```bash
coord-ctl --token <root_token> auth approle revoke <access_token>
```

---

## transit — Transit 加密

> 所有 transit 写操作需要携带有 `transit.*` 策略的 token。

### `transit create-key <KEY_NAME>`

创建加密密钥（AES-256-GCM）。

```bash
coord-ctl --token <token> transit create-key app-key
# → key_name: app-key, primary_version: 1
```

### `transit encrypt <KEY_NAME> <PLAINTEXT>`

加密明文字符串。

```bash
coord-ctl --token <token> transit encrypt app-key "hello coord"
# → ciphertext: vault:v1:AAAAAA..., version: 1
```

### `transit decrypt <KEY_NAME> <CIPHERTEXT>`

解密密文，输出 base64 和 UTF-8 明文。

```bash
coord-ctl --token <token> transit decrypt app-key "vault:v1:AAAAAA..."
# → plaintext_utf8: hello coord
```

### `transit rotate-key <KEY_NAME>`

轮换密钥（新 primary version + 1，旧版本密文仍可解密）。

```bash
coord-ctl --token <token> transit rotate-key app-key
# → primary_version: 2
```

### `transit hmac-sign <KEY_NAME> <DATA>`

HMAC 签名。

```bash
coord-ctl --token <token> transit hmac-sign app-key "payload-to-sign"
# → signature: hmac:v1:BBBBBB..., version: 1
```

### `transit hmac-verify <KEY_NAME> <DATA> <SIGNATURE>`

验证 HMAC 签名。

```bash
coord-ctl --token <token> transit hmac-verify app-key "payload-to-sign" "hmac:v1:BBBBBB..."
# → ok: true
```

---

## pki — PKI 证书管理

> PKI 写操作需要有 `pki.*` 策略的 token。

### `pki issue <COMMON_NAME>`

签发叶证书。

```bash
coord-ctl --token <token> pki issue svc-a.internal \
  --san svc-a.internal \
  --san 127.0.0.1 \
  --ttl-seconds 86400 \
  --auto-renew
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--san` | （可多次） | Subject Alternative Name（域名或 IP） |
| `--ttl-seconds` | `86400` | 证书有效期 |
| `--auto-renew` | `false` | 到期前自动续期 |
| `--renew-before-seconds` | `3600` | 提前续期时间窗口 |

输出：`serial_number`, `certificate_pem`, `private_key_pem`, `ca_certificate_pem`

### `pki renew <SERIAL>`

手动续期证书。

```bash
coord-ctl --token <token> pki renew <serial_number> --ttl-seconds 86400
```

### `pki revoke <SERIAL>`

吊销证书。

```bash
coord-ctl --token <token> pki revoke <serial_number> --reason key-compromise
```

可选 reason：`unspecified` | `key-compromise` | `ca-compromise` | `affiliation-changed` | `superseded` | `cessation-of-operation`

### `pki ca-chain`

输出 PEM 格式 CA 证书链。

```bash
coord-ctl pki ca-chain > ca-chain.pem
```

### `pki crl`

输出 PEM 格式证书吊销列表（CRL）。

```bash
coord-ctl pki crl --next-update-seconds 600
```

### `pki ocsp <SERIAL>`

查询证书 OCSP 状态。

```bash
coord-ctl pki ocsp <serial_number>
```

### `pki set-auto-renew-policy <SERIAL>`

设置证书自动续期策略。

```bash
coord-ctl --token <token> pki set-auto-renew-policy <serial> \
  --enabled true \
  --renew-before-seconds 3600
```

### `pki run-auto-renew`

立即触发一次自动续期扫描。

```bash
coord-ctl --token <token> pki run-auto-renew
```

### ACME 工作流

```bash
# 1. 创建 ACME 订单
coord-ctl --token <token> pki acme-order \
  --domain example.com \
  --domain www.example.com \
  --ttl-seconds 7776000 \
  --challenge-type http-01

# 2. 完成 HTTP-01 挑战（将 token 内容部署到 .well-known/acme-challenge/）
coord-ctl --token <token> pki acme-challenge \
  <order_id> example.com <token>

# 3. Finalize，获取证书
coord-ctl --token <token> pki acme-finalize \
  <order_id> --common-name example.com
```

---

## workflow — 工作流引擎

> **开发预览**：使用 `MemoryWorkflowStore`，重启后实例状态丢失。

### `workflow deploy <FILE>`

部署 CNCF Serverless Workflow DSL v2 定义文件。

```bash
coord-ctl workflow deploy payment.yaml \
  --definition-id payment-v1
```

### `workflow start`

启动工作流实例。

```bash
coord-ctl workflow start \
  --definition-id payment-v1 \
  --namespace default \
  --input-json '{"order_id":"123","amount":100}'
```

### `workflow get <INSTANCE_ID>`

查询实例状态。

```bash
coord-ctl workflow get <instance_id>
```

### `workflow list`

列出实例。

```bash
coord-ctl workflow list --namespace default --definition-name payment
```

### `workflow definitions`

列出已部署的工作流定义。

```bash
coord-ctl workflow definitions --namespace default
```

### `workflow definition <DEFINITION_ID>`

查看定义 YAML。

```bash
coord-ctl workflow definition payment-v1
```

---

## backup — 备份与恢复

### `backup create`

创建集群状态快照。

```bash
coord-ctl backup create --file coord-backup.json
```

### `backup restore`

从快照恢复。

```bash
coord-ctl backup restore coord-backup.json
```

---

## lock — 分布式锁（查询）

### `lock list`

列出当前所有持有中的锁。

```bash
coord-ctl lock list
```

输出示例：

```
lock=db-migration owner=svc-a-pod-1 expires_unix_ms=1700000000000
```

---

## TLS 连接示例

```bash
# 服务端自签名 CA 场景
coord-ctl \
  --endpoint https://coord.internal:9090 \
  --tls-ca /etc/coord/ca.pem \
  cluster status

# mTLS 场景
coord-ctl \
  --endpoint https://coord.internal:9090 \
  --tls-ca /etc/coord/ca.pem \
  --tls-cert /etc/coord/client.crt \
  --tls-key /etc/coord/client.key \
  cluster status
```
