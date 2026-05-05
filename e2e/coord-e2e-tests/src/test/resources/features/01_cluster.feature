# language: zh-CN
@core
功能: Coord 集群管理

  场景: 单节点集群启动
    假如 Coord 集群已启动
    当 查询集群状态
    那么 集群节点数 >= 1
    并且 至少有一个 Leader 节点

  场景: 节点心跳存活检测
    假如 Coord 集群已启动
    当 持续发送心跳 30s
    那么 Leader 节点保持稳定

  场景: HTTP 指标端点可用
    假如 Coord 集群已启动
    当 请求 /metrics 端点
    那么 返回内容包含 "coord_"

  场景: 查询集群 Leader
    假如 Coord 集群已启动
    当 查询集群状态
    那么 存在 Leader 节点

  场景: 集群成员列表非空
    假如 Coord 集群已启动
    当 查询集群状态
    那么 成员列表非空
