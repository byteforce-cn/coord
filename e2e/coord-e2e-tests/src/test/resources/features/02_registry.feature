# language: zh-CN
@core
功能: 服务注册与发现

  背景:
    假如 Coord 集群已启动

  场景: 注册服务实例
    当 注册服务 "order-service" host="127.0.0.1" port=18080 ttl=30s
    那么 返回 lease_id 非空

  场景: 发现已注册服务
    假如 服务 "order-service" 实例 host="127.0.0.1" port=18080 已注册
    当 发现服务 "order-service"
    那么 返回实例列表非空
    并且 包含 host="127.0.0.1" port=18080

  场景: 注销服务后不可发现
    假如 服务 "order-service" 实例 host="127.0.0.1" port=18080 已注册
    当 注销服务 lease_id
    并且 发现服务 "order-service"
    那么 实例列表不包含已注销实例

  场景: TTL 超时自动摘除
    假如 服务 "ephemeral-svc" 实例 host="10.0.0.1" port=9999 已注册 ttl=3s
    当 等待 5s
    并且 发现服务 "ephemeral-svc"
    那么 实例列表为空或不包含该实例

  场景: 持续心跳保持注册
    假如 服务 "order-service" 实例 host="127.0.0.1" port=18080 已注册 ttl=5s
    当 发送 3 次心跳续约
    并且 发现服务 "order-service"
    那么 返回实例列表非空

  场景: 内置业务服务自动注册并持续续约
    假如 所有服务均健康
    当 发现服务 "order-service"
    那么 返回实例列表非空
    并且 任一实例元数据包含 env=e2e
    当 记录当前发现实例 ID 列表
    并且 等待 35s
    并且 发现服务 "order-service"
    那么 返回实例列表非空
    并且 仍包含先前记录的任一实例

  场景: 注册时携带元数据
    当 注册服务 "pay-service" host="127.0.0.1" port=18081 metadata={version=v2}
    那么 发现实例元数据包含 version=v2

  场景: 多实例注册同一服务
    假如 服务 "inventory-service" host="127.0.0.1" port=18082 已注册
    假如 服务 "inventory-service" host="127.0.0.2" port=18082 已注册
    当 发现服务 "inventory-service"
    那么 返回实例数 >= 2

  场景: 重复注册相同实例更新租约
    假如 服务 "dup-service" 实例 host="10.0.0.5" port=7001 已注册
    当 注册服务 "dup-service" host="10.0.0.5" port=7001 ttl=60s
    那么 返回 lease_id 非空
    当 发现服务 "dup-service"
    那么 返回实例数 >= 1
    并且 包含 host="10.0.0.5" port=7001

  场景: 心跳续约不存在的租约应返回错误
    当 对未知租约 "no-such-lease-id" 发送心跳
    那么 应收到 NOT_FOUND 错误

  场景: 注销后发现结果立即不含已注销实例
    假如 服务 "cleanup-service" 实例 host="10.0.0.9" port=8888 已注册
    当 注销服务 lease_id
    并且 发现服务 "cleanup-service"
    那么 实例列表不包含已注销实例
