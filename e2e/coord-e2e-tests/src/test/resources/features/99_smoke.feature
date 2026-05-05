# language: zh-CN
@smoke
功能: 端到端完整冒烟测试

  背景:
    假如 Coord 集群已启动
    假如 所有服务均健康

  场景: 完整业务链路冒烟测试
    假如 库存服务中商品 "SMOKE-PROD" 库存为 100
    假如 Transit密钥 "smoke-key" 已创建 algorithm="AES256-GCM96"
    假如 PKI角色 "smoke-role" 已创建 allowed_domains="smoke.test" max_ttl="1h"
    当 用户 "smoke-user" 创建订单 商品="SMOKE-PROD" 数量=3 单价=29.9
    那么 返回订单 ID
    并且 订单状态最终变为 "CONFIRMED"
    并且 商品 "SMOKE-PROD" 库存扣减 3
    并且 支付服务创建支付记录
    并且 Order服务可发现 Pay服务
    并且 Order服务可发现 Inventory服务

  场景: 所有核心能力均可用
    当 生成 10 个 ID
    那么 所有 ID 唯一
    当 写入配置 key="smoke.test" value="ok"
    并且 读取配置 key="smoke.test"
    那么 配置值为 "ok"
    当 客户端 A 获取锁 "smoke-lock" ttl=10s
    那么 A 持有锁成功
    当 颁发证书 common_name="smoke.test" ttl="1h"
    那么 返回证书 PEM 非空
    当 加密明文 "smoke-secret"
    并且 解密该密文
    那么 解密结果等于原明文
