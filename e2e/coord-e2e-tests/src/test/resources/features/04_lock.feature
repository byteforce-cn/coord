# language: zh-CN
@core
功能: 分布式锁

  背景:
    假如 Coord 集群已启动

  场景: 客户端 A 获取锁成功
    当 客户端 A 获取锁 "lock-basic" ttl=30s
    那么 A 持有锁成功

  场景: 客户端 B 无法抢占 A 持有的锁
    假如 客户端 A 持有锁 "lock-exclusive"
    当 客户端 B 尝试获取锁 "lock-exclusive" wait=false
    那么 B 获取锁失败

  场景: A 释放锁后 B 可获取
    假如 客户端 A 持有锁 "lock-transfer"
    当 客户端 A 释放锁
    并且 客户端 B 获取锁 "lock-transfer" ttl=10s
    那么 B 持有锁成功

  场景: 锁 TTL 超时自动释放
    假如 客户端 A 持有锁 "lock-ttl" ttl=3s
    当 等待 5s
    并且 客户端 B 获取锁 "lock-ttl" wait=false ttl=10s
    那么 B 持有锁成功

  场景: 阻塞等待获取锁
    假如 客户端 A 持有锁 "lock-blocking" ttl=5s
    当 客户端 B 等待获取锁 "lock-blocking" wait=true timeout=15s
    并且 客户端 A 在 3s 后释放锁
    那么 B 在超时前获取锁成功

  场景: KeepAlive 续约使锁超过初始 TTL 仍然有效
    假如 客户端 A 持有锁 "lock-keepalive" ttl=3s
    当 客户端 A 每 1s 发送 KeepAlive 持续 8s
    并且 客户端 B 尝试获取锁 "lock-keepalive" wait=false
    那么 B 获取锁失败
