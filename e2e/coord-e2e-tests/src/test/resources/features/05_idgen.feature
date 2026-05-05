# language: zh-CN
@core
功能: 分布式 ID 生成

  背景:
    假如 Coord 集群已启动

  场景: 生成单个 Snowflake ID
    当 生成 1 个 ID
    那么 返回 ID 非零
    并且 ID 为正整数

  场景: 批量生成 ID 保证唯一
    当 生成 100 个 ID
    那么 所有 ID 唯一

  场景: 并发生成 ID 无重复
    当 10 个线程各生成 50 个 ID
    那么 总计 500 个 ID 全部唯一

  场景: ID 单调递增趋势
    当 顺序生成 10 个 ID
    那么 ID 序列整体递增

  场景: ID 包含时间戳信息
    当 生成 1 个 ID
    那么 ID 的时间戳部分接近当前时间

  场景: 指定 worker_id 生成 ID
    当 worker_id=7 生成 10 个 ID
    那么 返回 10 个 ID 非零
