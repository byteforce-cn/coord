# 安全控制面指南

coord 内置类 Vault 风格的安全控制面，负责保护 Transit 密钥、PKI 私钥等敏感材料。

---

## 核心概念

| 概念 | 说明 |
|------|------|
| **安全域（Security Domain）** | 包含 Transit 密钥、PKI 密钥、AppRole 和 token 的加密容器 |
| **Barrier Key** | 保护域的 AES-256 对称密钥，由 Root Key 加密封存 |
| **Root Key** | 256 bit 随机密钥，通过 Shamir 拆分为多个 share 分发给操作员 |
| **Seal / Unseal** | Sealed 时 Barrier Key 不在内存中，所有加密操作不可用；Unseal 后恢复 |
| **root_token** | 拥有 `*` 策略的超级 token，仅在 `init` 时生成一次 |
| **AppRole** | 服务身份，通过 role_id + secret_id 换取限时 token |

---

## 一、安全域生命周期

```
init（生成 Root Key + shares）
  ↓
sealed（Barrier Key 内存中已清除）
  ↓  unseal（提交 ≥ threshold 个 shares）
unsealed（Barrier Key 加载到内存，服务可用）
  ↓  seal（手动密封 或 重启）
sealed
```

---

## 二、初始化

每个集群生命周期**只执行一次**：

```bash
coord-ctl operator init --shares-total 5 --threshold 3
```

输出所有 shares 和 root_token，**立即保存到安全位置**（密码管理器 / HSM 等）。

### 推荐 shares 分发策略

| 场景 | shares_total | threshold | 说明 |
|------|-------------|-----------|------|
| 单机开发 | 1 | 1 | 最简，无冗余 |
| 小团队（≤5 人） | 3 | 2 | 任意 2 人可恢复 |
| 生产（≥ 3 管理员） | 5 | 3 | 多数管理员认证 |

---

## 三、解封

服务启动（或重启）后处于 sealed 状态，需提交 threshold 份额：

```bash
# 查看当前状态
coord-ctl operator seal-status
# → sealed: true, progress: 0/3

coord-ctl operator unseal shamir:AAAA...
# → sealed: true, progress: 1/3

coord-ctl operator unseal shamir:BBBB...
# → sealed: true, progress: 2/3

coord-ctl operator unseal shamir:CCCC...
# → sealed: false
```

### 自动解封（K8s / 容器场景）

将 shares 写入文件（每行一个），启动时传入 `--auto-unseal-shares-file`：

```bash
# shares 文件（权限设为 0400）
cat > /run/secrets/unseal.shares << 'EOF'
shamir:AAAA...
shamir:BBBB...
shamir:CCCC...
EOF
chmod 0400 /run/secrets/unseal.shares

coord-server serve \
  --auto-unseal-shares-file /run/secrets/unseal.shares \
  ...
```

> ⚠️ 生产环境自动解封意味着 shares 必须保护好存储介质；服务端启动时会打印 WARN 日志提醒操作者。

### Dev 模式自动解封

详见 [Docker 部署 § Dev Root Token](03-deploy-docker.md#dev-root-token)。

---

## 四、手动密封

```bash
coord-ctl --token <root_token> operator seal
```

密封后：
- 所有 Transit 加解密 / HMAC 请求返回 `UNAVAILABLE`
- PKI 证书签发 / 续期失败
- 鉴权仍然可用（AppRole login / token lookup 不依赖 Barrier Key）

---

## 五、AppRole 身份认证

### 流程

```
管理员创建 AppRole（附加 policies）
  ↓
管理员生成 SecretId（下发给服务）
  ↓
服务用 role_id + secret_id 换取 access_token
  ↓
服务用 access_token 调用受保护 gRPC / HTTP 端点
```

### 示例：为微服务创建 AppRole

```bash
# 1. 创建角色（root_token 操作）
coord-ctl --token <root_token> auth approle create payment-svc \
  --policy transit.encrypt \
  --policy transit.decrypt \
  --policy config.read \
  --token-ttl-seconds 3600 \
  --secret-id-num-uses 0
# → role_id: uuid-AAA

# 2. 生成 SecretId（每次部署/重启生成新的）
coord-ctl --token <root_token> auth approle generate-secret-id uuid-AAA
# → secret_id: sid-BBB

# 3. 服务登录
coord-ctl auth approle login uuid-AAA sid-BBB
# → access_token: tok-CCC, expires: 3600s

# 4. 使用 token 调用 transit
coord-ctl --token tok-CCC transit encrypt app-key "sensitive data"
```

---

## 六、Policies（能力策略）

coord 通过字符串策略控制 gRPC 能力，当前内置策略：

| 策略字符串 | 允许操作 |
|-----------|---------|
| `*` | 所有操作（root） |
| `transit.encrypt` | Transit 加密 |
| `transit.decrypt` | Transit 解密 |
| `transit.sign` | Transit HMAC 签名/验证 |
| `pki.issue` | PKI 证书签发 |
| `pki.revoke` | PKI 证书吊销 |
| `config.read` | 配置读取 |
| `config.write` | 配置写入 |

创建 AppRole 时通过 `--policy` 指定，可同时附加多个。

---

## 七、根密钥轮换

在不中断服务的情况下更换根密钥并生成新的 shares：

```bash
coord-ctl --token <root_token> operator rotate-root-key \
  --shares-total 5 --threshold 3
```

轮换后旧 shares **立即失效**，保存并分发新 shares。

---

## 八、Token 管理

### 查看 token 信息

```bash
coord-ctl auth approle lookup <access_token>
```

### 吊销 token

```bash
coord-ctl --token <root_token> auth approle revoke <access_token>
```

---

## 九、安全注意事项

1. **root_token 不能泄漏**：root_token 拥有全量权限，应存入密码管理器，仅在初始化和应急操作时使用。
2. **Shares 分开保管**：不同 share 分给不同管理员，避免单点泄漏。
3. **dev_root_token 仅限测试**：`COORD_DEV_ROOT_TOKEN` 是明文配置，永远不要在生产环境使用。
4. **数据目录权限**：`<data_dir>/dev-unseal.share`、`dev-root-token.txt` 权限已设为 0600，确保宿主机上其他进程无法读取。
5. **TLS 加密传输**：生产环境必须配置 TLS，否则 token 和 shares 在网络中明文传输。
