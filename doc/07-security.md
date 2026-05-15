# 安全域、Token 与 AppRole

coord 的安全模型基于 **Shamir Secret Sharing** 封印 / 解封机制，以及基于 **能力（capability）** 的访问控制。

---

## 一、安全域生命周期

```
未初始化 → 初始化（sealed） → 解封（unsealed）→ 工作状态
                                        ↑  封印   ↓
                                      sealed ←────
```

| 状态 | 说明 |
|------|------|
| 未初始化 | 全新节点，只有 `operator init` / `operator seal-status` 可用 |
| sealed | 已初始化但封印，只有 `operator unseal` / `operator seal-status` 可用 |
| unsealed | 正常工作状态，所有 API 可用 |

---

## 二、初始化

**每个集群生命周期只执行一次**：

```bash
coord ctl operator init --shares-total 5 --threshold 3
```

输出示例：

```
initialized: true
sealed:      true
shares_total: 5
threshold:   3
unseal_shares:
  shamir:AAAA...
  shamir:BBBB...
  shamir:CCCC...
  shamir:DDDD...
  shamir:EEEE...
root_token: s.xxxxxxxxxxxxxxxxxxxxxxxx
```

> ⚠️ **立即备份 `unseal_shares` 和 `root_token`**，丢失后无法恢复安全域内的加密数据。

---

## 三、解封

重启后节点处于 sealed 状态，需提交 threshold 份额：

```bash
coord ctl operator unseal shamir:AAAA...
coord ctl operator unseal shamir:BBBB...
coord ctl operator unseal shamir:CCCC...
# → sealed: false
```

查看当前状态：

```bash
coord ctl operator seal-status
```

---

## 四、封印

主动封印（触发紧急安全响应时使用）：

```bash
coord ctl --token <root_token> operator seal
```

---

## 五、Root Key 轮换

生成新的 root key 和 Shamir shares（旧 shares 立即作废）：

```bash
coord ctl --token <root_token> operator rotate-root-key \
  --shares-total 5 --threshold 3
```

---

## 六、能力（Capability）访问控制

所有受保护的 gRPC 方法都需要 token 携带相应能力。能力以字符串表示：

| 能力 | 覆盖的操作 |
|------|-----------|
| `registry.write` | 注册 / 注销 / 心跳 |
| `registry.read` | 发现服务 |
| `config.write` | 写入配置 |
| `config.read` | 读取配置 |
| `lock.write` | 获取 / 释放锁 |
| `lock.read` | 查看锁列表 |
| `transit.admin` | 创建 / 轮换密钥 |
| `transit.encrypt` | 加密 |
| `transit.decrypt` | 解密 |
| `transit.hmac_sign` | HMAC 签名 |
| `transit.hmac_verify` | HMAC 验证 |
| `transit.read` | 读取密钥信息 |
| `pki.issue` | 颁发证书 |
| `pki.renew` | 续期证书 |
| `pki.revoke` | 吊销证书 |
| `pki.read` | 读取 CA / CRL |
| `pki.admin` | PKI 管理操作 |
| `workflow.write` | 部署 / 启动工作流 |
| `workflow.read` | 查询工作流 |
| `policy.write` | 写入策略 bundle |
| `policy.read` | 读取策略 bundle |
| `policy.evaluate` | 策略评估 |
| `security.admin` | AppRole 管理 |
| `security.seal` | 封印操作 |
| `operator.rotate_key` | Root Key 轮换 |

Root token 携带 `"*"` 通配符，拥有全部能力。

---

## 七、AppRole 认证

### 创建 AppRole

```bash
coord ctl --token <root_token> auth approle create order-service \
  --policy registry.write \
  --policy registry.read \
  --policy transit.encrypt \
  --token-ttl-seconds 3600
```

### 生成 SecretId

```bash
coord ctl --token <root_token> auth approle generate-secret-id <role_id>
# → secret_id: xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx
```

### 登录获取 Token

```bash
coord ctl auth approle login <role_id> <secret_id>
# → access_token: s.yyyyyyyyyyyyyyyy
```

### 在 SDK 中使用

```java
// Java SDK
CoordClient client = CoordClient.builder()
    .endpoint("http://coord:9090")
    .token("s.yyyyyyyyyyyyyyyy")
    .build();
```

```go
// Go SDK
client, _ := coord.NewClient(coord.Config{
    Endpoint: "http://coord:9090",
    Token:    "s.yyyyyyyyyyyyyyyy",
})
```

---

## 八、Token 管理

```bash
# 查验 token
coord ctl auth approle lookup <token>

# 吊销 token（立即失效）
coord ctl --token <root_token> auth approle revoke <token>
```

---

## 九、开放端点（无需 Token）

以下端点不需要鉴权，无论安全域状态如何均可访问：

- `SealService/Init` / `SealService/InitSeal`
- `SealService/GetSealStatus`
- `SealService/Unseal`
- `AuthService/LoginAppRole`
- `AuthService/LookupToken`
- `AdminService/ClusterStatus`
- `RaftInternalService/*`（节点间内部通信）
