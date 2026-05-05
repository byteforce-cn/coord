# language: zh-CN
@core
功能: Prometheus 可观测性指标

  背景:
    假如 Coord 集群已启动

  场景: /metrics 返回 Prometheus 标准格式
    当 请求 /metrics 端点
    那么 HTTP 200 且 Content-Type 包含 text/plain
    并且 响应包含 "# TYPE" 行

  场景: 核心 metric 名称均存在
    当 请求 /metrics 端点
    那么 包含指标 "raft_node_state"
    并且 包含指标 "coord_services_registered_total"
    并且 包含指标 "coord_transit_encryption_requests_total"
    并且 包含指标 "coord_locks_held"

  场景: 服务注册后 registered_total 递增
    假如 抓取指标 "coord_services_registered_total" 当前值为基准
    当 注册服务 "obs-svc" 实例 "obs-inst-1"
    那么 指标 "coord_services_registered_total" 值比基准大

  场景: 锁持有计数反映实际状态
    假如 抓取指标 "coord_locks_held" 当前值为基准
    当 客户端 A 获取锁 "obs-lock" ttl=30s
    那么 指标 "coord_locks_held" 值比基准大
    当 客户端 A 释放锁
    并且 等待 2s 让 metric 更新

  场景: Transit 加密计数递增
    假如 Transit密钥 "obs-enc-key" 已创建 algorithm="AES256-GCM96"
    并且 抓取指标 "coord_transit_encryption_requests_total" 当前值为基准
    当 用密钥 "obs-enc-key" 执行 3 次加密
    那么 指标 "coord_transit_encryption_requests_total" 值比基准大
