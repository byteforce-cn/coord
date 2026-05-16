# 集群测试步骤（3 节点 + Failover）

适用场景：测试 Raft 共识、Leader 选举、节点故障转移等高可用能力。

## 前提条件

同快速测试，额外需要足够内存（建议 Docker Desktop 分配 ≥ 6GB RAM）。

## 启动 3 节点集群

```bash
cd e2e

# 1. 编译业务服务 jar（首次或修改后）
make jars

# 2. 启动完整集群（coord-1、coord-2、coord-3 + mock services）
make e2e-up-cluster

# 3. 等待所有节点健康（约 60-90 秒）
docker ps --format '{{.Names}}\t{{.Status}}' | grep coord
```

> `make e2e-up-cluster` 会自动注入 `COORD_1_CLUSTER_PEERS=coord-2:9090,coord-3:9090`，使 `coord-1` 以三节点 auto-join 模式启动。若绕过 Makefile 直接执行 `docker compose`，需手工带上该环境变量。

等价的原生命令：

```bash
COORD_1_CLUSTER_PEERS=coord-2:9090,coord-3:9090 \
  docker compose --profile cluster up -d --build \
  coord-1 coord-2 coord-3 order-service pay-service inventory-service
```

预期状态：

```
coord-1    Up 2 minutes (healthy)
coord-2    Up 2 minutes (healthy)
coord-3    Up 2 minutes (healthy)
order-service   Up 2 minutes (healthy)
pay-service     Up 2 minutes (healthy)
inventory-service  Up 2 minutes (healthy)
```

## 运行 Failover 测试

```bash
# 仅运行 @failover 场景（约 2-5 分钟）
make e2e-failover
```

## 运行完整集群测试

```bash
# @smoke + @core + @failover（排除 @slow）
cd coord-e2e-tests && mvn test \
  -Dcoord.grpc.address=localhost:9090 \
  -Dcoord.http.address=http://localhost:8080 \
  -Dorder.service.url=http://localhost:18080 \
  -Dpay.service.url=http://localhost:18081 \
  -Dinventory.service.url=http://localhost:18082 \
  -Dcucumber.filter.tags="not @slow"
```

## 端口映射

| 节点 | gRPC 端口 | HTTP 端口 |
|------|-----------|-----------|
| coord-1 | 9090 | 8080 |
| coord-2 | 19090 | 8181 |
| coord-3 | 29090 | 8281 |
| order-service | — | 18080 |
| pay-service | — | 18081 |
| inventory-service | — | 18082 |

## 手动 Failover 验证

```bash
# 停掉当前 Leader（通常是 coord-1），观察集群重新选主
docker stop coord-1

# 查看 coord-2 日志确认当选 Leader
docker logs coord-2 2>&1 | grep -i 'leader\|elected'

# 恢复 coord-1
docker start coord-1
```

## 重置集群环境

```bash
# 停止所有节点（含 cluster profile）并清理 volume 和安全缓存
make e2e-reset

# 重新启动集群
make e2e-up-cluster
```

## 注意事项

- `e2e-reset` 会清除 `.cache/e2e-security.json`，下次测试将重新初始化 security domain，**不需要**手动干预。
- coord-2 和 coord-3 在 `docker-compose.yml` 中标记为 `profiles: [cluster]`，默认 `make e2e-up` 不启动它们。
- 直接执行 `docker compose --profile cluster up ...` 时，如未设置 `COORD_1_CLUSTER_PEERS`，`coord-1` 会保持单节点模式，`coord-2` / `coord-3` 不会被自动加入集群。
- `coord-1/2/3` 的 Docker 日志已配置 `json-file` 轮转，单容器上限为 `10m x 3`。
- 周期性 `persisted runtime snapshot to redb` 日志为 `debug` 级别，集群默认 `info` 日志下不会持续刷屏。
- Failover 场景要求 Raft quorum，至少 2/3 节点在线。
