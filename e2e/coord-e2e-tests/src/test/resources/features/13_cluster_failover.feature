# language: zh-CN
@failover
功能: 集群容错

  背景:
    假如 Coord 三节点集群已启动

  @failover
  场景: Follower 宕机服务不中断
    假如 集群选举已完成
    当 停止 Follower 节点
    并且 等待 5s 让集群感知
    那么 Discover API 返回正常
    并且 GetConfig API 返回正常
    当 恢复 Follower 节点

  @failover
  场景: Leader 宕机后重新选举
    假如 集群选举已完成
    当 停止 Leader 节点
    并且 等待新选举完成最多 30s
    那么 新 Leader 被选出
    并且 Discover API 返回正常
    当 恢复停止的节点

  @failover
  场景: 多数派丢失后写操作不可用
    假如 集群选举已完成
    当 停止 2 个节点（仅剩 1 节点）
    并且 尝试写入配置 key="quorum-test" value="v1"
    那么 写入操作超时或返回 UNAVAILABLE
    当 恢复所有已停止节点

  @failover
  场景: 宕机节点恢复后追赶日志
    假如 集群选举已完成
    当 停止 1 个 Follower 节点
    并且 写入配置 key="catchup-key" value="catchup-val"
    并且 恢复已停止的 Follower 节点
    并且 等待日志追赶最多 30s
    那么 GetConfig "catchup-key" 返回 "catchup-val"
