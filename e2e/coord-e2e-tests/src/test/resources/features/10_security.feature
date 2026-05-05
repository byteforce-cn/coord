# language: zh-CN
@core
功能: 安全域 Seal/Unseal 与认证

  场景: 初始化安全域
    假如 Coord 集群已启动
    当 初始化安全域 shares=5 threshold=3
    那么 返回 5 个密钥分片
    并且 返回 root_token 非空

  场景: 密封安全域
    假如 安全域已初始化且已解封
    当 密封安全域
    那么 安全域状态为 sealed

  场景: 提交不足分片不触发解封
    假如 安全域已初始化且已解封
    当 密封安全域
    并且 提交前 2 个解封分片
    那么 安全域仍处于 sealed 状态

  场景: 提交足够分片完成解封
    假如 安全域已初始化且已解封
    当 密封安全域
    并且 提交第 1 个解封分片
    并且 提交足够分片完成解封
    那么 安全域状态为 unsealed

  场景: 创建 AppRole 并生成凭证
    假如 安全域已初始化且已解封
    当 创建 AppRole "order-role" policies=["read-config"]
    并且 生成 SecretId
    那么 返回 secret_id 非空

  场景: AppRole 登录获取 Token
    假如 安全域已初始化且已解封
    当 创建 AppRole "pay-role" policies=["pay-policy"]
    并且 生成 SecretId
    并且 LoginAppRole role="pay-role"
    那么 返回 token 非空
    并且 token 具有策略 "pay-policy"

  场景: LookupToken 验证 Token 有效
    假如 安全域已初始化且已解封
    当 创建 AppRole "inv-role" policies=["inv-policy"]
    并且 生成 SecretId
    并且 LoginAppRole role="inv-role"
    并且 LookupToken
    那么 token 有效

  场景: 吊销 Token 后不可用
    假如 安全域已初始化且已解封
    当 创建 AppRole "tmp-role" policies=["tmp-policy"]
    并且 生成 SecretId
    并且 LoginAppRole role="tmp-role"
    并且 RevokeToken
    那么 LookupToken 返回 invalid

  场景: Root Key 轮换生成新分片
    假如 安全域已初始化且已解封
    当 执行 RotateRootKey shares=3 threshold=2
    那么 RotateRootKey 成功 且返回 3 个新解封分片
    假如 安全域已初始化且已解封
    当 执行 RotateRootKey shares=5 threshold=3
    那么 RotateRootKey 成功 且返回 5 个新解封分片

  场景: SecretId 使用次数限制（num_uses=2）
    假如 安全域已初始化且已解封
    当 创建 AppRole "limited-role" policies=["limited"] num_uses=2
    并且 生成 SecretId
    当 LoginAppRole role="limited-role"
    那么 返回 token 非空
    当 LoginAppRole role="limited-role"
    那么 返回 token 非空
    当 尝试 LoginAppRole role="limited-role"
    那么 登录返回错误

  场景: Token TTL 过期后失效
    假如 安全域已初始化且已解封
    当 创建 AppRole "ttl-role" policies=["ttl"] token_ttl_seconds=3
    并且 生成 SecretId
    并且 LoginAppRole role="ttl-role"
    那么 返回 token 非空
    当 等待 5 秒
    并且 LookupToken
    那么 token 已失效

  场景: 权限越界操作被拒绝
    假如 安全域已初始化且已解封
    当 创建 AppRole "enc-only-role" policies=["transit.encrypt"]
    并且 生成 SecretId
    并且 LoginAppRole role="enc-only-role"
    当 使用当前 token 调用 Transit Decrypt
    那么 返回 PERMISSION_DENIED

  场景: 密封后数据持久化
    假如 安全域已初始化且已解封
    当 写入配置 key="seal-persist" value="ok"
    并且 密封安全域
    并且 提交足够分片完成解封
    当 读取配置 key="seal-persist"
    那么 配置值为 "ok"
