# language: zh-CN
@core
功能: Transit 加解密服务

  背景:
    假如 Coord 集群已启动
    假如 Transit密钥 "test-key" 已创建 algorithm="AES256-GCM96"

  场景: 加密明文后密文不等于原文
    当 加密明文 "Hello Coord"
    那么 返回密文非空
    并且 密文不等于明文

  场景: 解密恢复原文
    当 加密明文 "sensitive-data-123"
    并且 解密该密文
    那么 解密结果等于原明文

  场景: HMAC 签名与验证
    假如 Transit密钥 "hmac-key" 已创建 algorithm="HMAC-SHA256"
    当 对 "message-to-sign" 签名
    那么 返回 HMAC 非空
    当 验证签名
    那么 验证结果为 true

  场景: 错误 HMAC 验证失败
    假如 Transit密钥 "hmac-key" 已创建 algorithm="HMAC-SHA256"
    当 对 "message-to-sign" 签名
    当 用错误 HMAC 验证
    那么 验证结果为 false

  场景: 密钥轮换后旧密文仍可解密
    当 加密明文 "rotate-test"
    并且 轮换密钥 "test-key"
    那么 旧密文仍可解密

  场景: 轮换后新旧密文不同
    当 加密明文 "before-rotate"
    并且 轮换密钥 "test-key"
    并且 用新密钥加密 "before-rotate"
    那么 新密文和旧密文不同

  场景: 查询密钥信息
    当 查询密钥信息 "test-key"
    那么 key_name="test-key"
    并且 algorithm="AES256-GCM96"
