# language: zh-CN
@core
功能: Order 服务动态配置刷新

  背景:
    假如 Coord 集群已启动
    假如 所有服务均健康

  场景: Order服务读取初始配置
    假如 配置 key="order.max-amount" value="10000" 已写入
    当 Order服务获取配置 "order.max-amount"
    那么 配置值为 "10000"

  场景: 动态更新超时配置
    假如 配置 key="order.timeout" value="60s" 已写入
    当 写入配置 key="order.timeout" value="30s"
    那么 在 10s 内监听到新值 "30s"

  场景: 配置变更不影响进行中订单
    假如 库存服务中商品 "PROD-CFG" 库存为 10
    假如 配置 key="order.max-amount" value="10000" 已写入
    当 用户 "user-cfg" 创建订单 商品="PROD-CFG" 数量=1 单价=50.0
    当 写入配置 key="order.max-amount" value="5000"
    那么 订单状态最终变为 "CONFIRMED"
