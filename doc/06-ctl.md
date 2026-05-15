# coord ctl 命令参考

`coord ctl` 是内置的管理 CLI，通过 gRPC 连接运行中的 coord 实例。

命令组：`cluster` · `member` · `operator` · `auth` · `transit` · `pki` · `workflow` · `lock` · `backup`

---

## 全局选项

```
coord ctl [全局选项] <命令> [子命令] [参数]
```

| 选项 | 环境变量 | 默认值 | 说明 |
|------|----------|--------|------|
| `--endpoint <URL>` | — | `http://127.0.0.1:9090` | 服务端 gRPC 地址 |
| `--token <TOKEN>` | — | — | 请求鉴权 token（需要能力授权的端点必填） |
| `--tls-ca <PATH>` | `COORD_TLS_CA` | — | PEM CA 证书（自签名 / 私有 CA 时验证服务端证书） |
| `--tls-cert <PATH>` | `COORD_TLS_CERT` | — | PEM 客户端证书（mTLS） |
| `--tls-key <PATH>` | `COORD_TLS_KEY` | — | PEM 客户端私钥（mTLS） |
| `--tls-domain <DOMAIN>` | `COORD_TLS_DOMAIN` | 端点 hostname | SNI / 证书验证域名覆盖 |

> TLS 在以下情况自动启用：端点为 `https://`，或提供了任何 `--tls-*` 参数。

---

## cluster — 集群状态

### `cluster status`

查询节点状态和集群成员列表。

```bash
coord ctl cluster status
```

输出示例：

```
node_id: node-1
state:   Leader
dev_mode: false
members: node-1, node-2, node-3
```

---

## member — Raft 成员管理

### `member add <NODE_ID> <ADDRESS>`

将新节点加入集群（通常由 auto-join 自动处理）。

```bash
coord ctl member add node-4 coord-4:9090
```

### `member remove <NODE_ID>`

优雅移除节点（或强制移除不可达节点）。

```bash
coord ctl member remove node-4

# 节点已不可达时强制移除
coord ctl member remove node-4 --force-unreachable
```

---

## operator — 安全控制面

### `operator init`

初始化安全域，返回 Shamir shares 和 root_token。**每个集群生命周期只执行一次**。

```bash
coord ctl operator init --shares-total 5 --threshold 3
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--shares-total` | `5` | 总份额数 |
| `--threshold` | `3` | 解封最低份额数 |

### `operator seal-status`

查看当前 seal 状态。

```bash
coord ctl operator seal-status
```

### `operator seal`

封印安全域（操作后所有受保护 API 将返回 `FAILED_PRECONDITION`）。

```bash
coord ctl --token <root_token> operator seal
```

### `operator unseal <SHARE>`

提交一个 Shamir 份额。提交 threshold 份后自动解封。

```bash
coord ctl operator unseal shamir:AAAA...
```

### `operator rotate-root-key`

轮换根加密密钥，生成新的 Shamir shares（旧 shares 作废）。

```bash
coord ctl --token <root_token> operator rotate-root-key \
  --shares-total 5 --threshold 3
```

---

## auth — AppRole 认证管理

### `auth approle create <ROLE_NAME>`

创建 AppRole。

```bash
coord ctl --token <root_token> auth approle create order-service \
  --policy read-config \
  --policy transit.encrypt \
  --token-ttl-seconds 3600 \
  --secret-id-ttl-seconds 86400 \
  --secret-id-num-uses 10
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--policy` | — | 附加策略名称（可重复） |
| `--token-ttl-seconds` | `3600` | Token 有效期（秒） |
| `--secret-id-ttl-seconds` | `86400` | SecretId 有效期（秒） |
| `--secret-id-num-uses` | `10` | SecretId 最大使用次数（0=不限） |

### `auth approle generate-secret-id <ROLE_ID>`

为指定 AppRole 生成一次性 SecretId。

```bash
coord ctl --token <root_token> auth approle generate-secret-id <role_id>
```

### `auth approle login <ROLE_ID> <SECRET_ID>`

使用 AppRole 凭证登录，返回 access token。

```bash
coord ctl auth approle login <role_id> <secret_id>
```

### `auth approle lookup <ACCESS_TOKEN>`

验证 token 有效性并查看附加策略。

```bash
coord ctl auth approle lookup <access_token>
```

### `auth approle revoke <ACCESS_TOKEN>`

吊销 token（立即失效）。

```bash
coord ctl --token <root_token> auth approle revoke <access_token>
```

---

## transit — 加密密钥管理

### `transit create-key <KEY_NAME>`

创建新的加密密钥（默认算法 AES-256-GCM）。

```bash
coord ctl --token <token> transit create-key my-key
```

### `transit encrypt <KEY_NAME> <PLAINTEXT>`

```bash
coord ctl --token <token> transit encrypt my-key "hello world"
# → vault:v1:AAAA...
```

### `transit decrypt <KEY_NAME> <CIPHERTEXT>`

```bash
coord ctl --token <token> transit decrypt my-key "vault:v1:AAAA..."
# → hello world
```

### `transit rotate-key <KEY_NAME>`

轮换密钥（旧版本仍可解密；新版本用于加密）。

```bash
coord ctl --token <token> transit rotate-key my-key
```

### `transit hmac-sign <KEY_NAME> <DATA>`

```bash
coord ctl --token <token> transit hmac-sign my-hmac-key "payload"
```

### `transit hmac-verify <KEY_NAME> <DATA> <SIGNATURE>`

```bash
coord ctl --token <token> transit hmac-verify my-hmac-key "payload" "hmac:v1:..."
```

---

## pki — 证书管理

### `pki issue <COMMON_NAME>`

颁发 X.509 证书。

```bash
coord ctl --token <token> pki issue api.internal \
  --san api.internal \
  --san 127.0.0.1 \
  --ttl-seconds 86400 \
  --auto-renew
```

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `--san` | — | Subject Alternative Name（可重复） |
| `--ttl-seconds` | `86400` | 证书有效期（秒） |
| `--auto-renew` | `false` | 启用自动续期 |
| `--renew-before-seconds` | `3600` | 到期前多少秒触发续期 |

### `pki renew <SERIAL_NUMBER>`

续期已有证书。

```bash
coord ctl --token <token> pki renew <serial> --ttl-seconds 86400
```

### `pki revoke <SERIAL_NUMBER>`

吊销证书。

```bash
coord ctl --token <token> pki revoke <serial> --reason key-compromise
```

### `pki ca-chain`

获取 CA 证书链（PEM 格式）。

```bash
coord ctl pki ca-chain
```

### `pki crl`

获取证书吊销列表（CRL）。

```bash
coord ctl pki crl
```

### `pki ocsp <SERIAL_NUMBER>`

查询单张证书的 OCSP 状态。

```bash
coord ctl pki ocsp <serial>
```

### `pki run-auto-renew`

手动触发自动续期任务（通常由内部定时器自动调用）。

```bash
coord ctl --token <token> pki run-auto-renew
```

### ACME 相关

```bash
# 1. 创建 ACME 订单
coord ctl --token <token> pki acme-order \
  --domain example.com --domain www.example.com

# 2. 完成 HTTP-01 challenge
coord ctl --token <token> pki acme-challenge <order_id> example.com <token>

# 3. 颁发证书
coord ctl --token <token> pki acme-finalize <order_id>
```

---

## workflow — 工作流管理

### `workflow deploy <FILE>`

部署工作流定义（YAML 格式）。

```bash
coord ctl --token <token> workflow deploy ./order-flow.yaml
```

### `workflow start`

启动工作流实例。

```bash
coord ctl --token <token> workflow start \
  --definition-id order-flow \
  --input-json '{"order_id":"o-123"}'
```

### `workflow get <INSTANCE_ID>`

查看工作流实例状态。

```bash
coord ctl --token <token> workflow get <instance_id>
```

### `workflow list`

列出工作流实例。

```bash
coord ctl --token <token> workflow list --namespace payments
```

### `workflow definitions`

列出已部署的工作流定义。

```bash
coord ctl --token <token> workflow definitions
```

---

## lock — 分布式锁

### `lock list`

列出当前所有活跃锁。

```bash
coord ctl --token <token> lock list
```

---

## backup — 备份与恢复

### `backup create`

```bash
coord ctl --token <token> backup create --file coord-backup.json
```

### `backup restore`

> ⚠️ **危险操作**：会覆盖当前状态，操作前务必停服或确认数据。

```bash
coord ctl --token <token> backup restore coord-backup.json
```
