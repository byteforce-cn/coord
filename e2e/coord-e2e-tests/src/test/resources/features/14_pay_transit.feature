# language: zh-CN
@core
功能: 支付服务 Transit 加密集成

  背景:
    假如 Coord 集群已启动
    假如 所有服务均健康
    假如 Transit密钥 "pay-key" 已创建 algorithm="AES256-GCM96"

  场景: 支付卡号 Transit 加密存储
    假如 库存服务中商品 "PROD-PAY-ENC" 库存为 10
    当 用户 "user-pay-enc" 创建订单 商品="PROD-PAY-ENC" 数量=1 单价=19.9
    那么 支付服务创建支付记录
    并且 支付记录状态为 "COMPLETED"

  场景: 管理员解密卡号
    假如 库存服务中商品 "PROD-PAY-DEC" 库存为 10
    当 用户 "user-pay-dec" 创建订单 商品="PROD-PAY-DEC" 数量=1 单价=19.9
    那么 支付服务创建支付记录

  场景: 支付幂等性防重复扣款
    假如 库存服务中商品 "PROD-PAY-IDEM" 库存为 10
    当 用户 "user-pay-idem" 创建订单 商品="PROD-PAY-IDEM" 数量=1 单价=19.9
    那么 支付服务创建支付记录
    并且 支付记录状态为 "COMPLETED"
