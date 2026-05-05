# language: zh-CN
@core
功能: 分布式配置管理

  背景:
    假如 Coord 集群已启动

  场景: 写入并读取配置
    当 写入配置 key="app.timeout" value="30s"
    并且 读取配置 key="app.timeout"
    那么 配置值为 "30s"

  场景: 覆盖配置值
    假如 配置 key="feature.flag" value="false" 已写入
    当 写入配置 key="feature.flag" value="true"
    并且 读取配置 key="feature.flag"
    那么 配置值为 "true"

  场景: 删除配置
    假如 配置 key="temp.config" value="xyz" 已写入
    当 删除配置 key="temp.config"
    并且 读取配置 key="temp.config"
    那么 返回空值或 NOT_FOUND

  场景: 动态配置变更通知（轮询模式）
    假如 配置 key="dynamic.rate" value="100" 已写入
    当 启动配置监听 key="dynamic.rate"
    并且 写入配置 key="dynamic.rate" value="200"
    那么 在 10s 内监听到新值 "200"

  场景: WatchConfig gRPC 流实时推送
    假如 配置 key="stream.test.key" value="v1" 已写入
    当 建立 WatchConfig gRPC 流订阅 "stream.test.key"
    并且 写入配置 key="stream.test.key" value="v2"
    那么 流在 5s 内收到推送值 "v2"

  场景: 配置版本号递增
    假如 配置 key="versioned.key" value="v1" 已写入
    当 写入配置 key="versioned.key" value="v2"
    那么 配置版本号递增

  场景: 列举命名空间下所有配置
    假如 配置 key="ns/k1" value="a" 已写入
    假如 配置 key="ns/k2" value="b" 已写入
    当 列举前缀 "ns/" 下所有配置
    那么 结果包含 "ns/k1" 和 "ns/k2"
