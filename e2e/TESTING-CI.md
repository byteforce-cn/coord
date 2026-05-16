# CI 测试步骤（完整流水线）

适用场景：Pull Request 验证、Release 发布前门禁、定时回归。

## 架构说明

CI 模式使用 **容器化测试 runner**，不依赖宿主机 JDK/Maven：

- `docker-compose.yml`（基础）+ 无 override（禁止 local jar 挂载）
- 单节点路径下 `coord-1` 默认以 `COORD_CLUSTER_PEERS=""` 启动
- profile `cluster` 路径需在创建 `coord-1` 时注入 `COORD_1_CLUSTER_PEERS=coord-2:9090,coord-3:9090`
- profile `test`：启动 `e2e-tests` runner 容器，执行全量测试后退出

## 快速 CI 运行（全量）

```bash
cd e2e

# 构建所有镜像并执行全量测试
make e2e-full
```

等价于：

```bash
# 1. 构建 SDK base 镜像
make sdk-base

# 2. 构建所有服务镜像
docker compose -f docker-compose.yml build

# 3. 启动 3 节点集群（容器化镜像，不挂载本地 jar）
COORD_1_CLUSTER_PEERS=coord-2:9090,coord-3:9090 \
docker compose -f docker-compose.yml --profile cluster up -d --build \
  coord-1 coord-2 coord-3 order-service pay-service inventory-service

# 4. 运行测试容器（全量，完成后自动退出）
docker compose -f docker-compose.yml --profile cluster --profile test run --rm e2e-tests
```

## 分阶段 CI（推荐 Pipeline 写法）

```bash
# 阶段 1 — 构建镜像
make sdk-base
docker compose -f docker-compose.yml build

# 阶段 2 — 冒烟（快速门禁，约 1 分钟）
docker compose -f docker-compose.yml up -d coord-1 order-service pay-service inventory-service
# 等待 healthy（脚本或 CI healthcheck 已内置）
docker compose -f docker-compose.yml run --rm e2e-tests \
  mvn test -Dcucumber.filter.tags="@smoke"

# 阶段 3 — 核心回归（约 10 分钟）
docker compose -f docker-compose.yml run --rm e2e-tests \
  mvn test -Dcucumber.filter.tags="not @failover and not @slow"

# 阶段 4 — 集群 / Failover（约 5 分钟）
# 不要只追加启动 coord-2 / coord-3；需要重建 coord-1 使其带上 cluster peers
docker compose -f docker-compose.yml down
COORD_1_CLUSTER_PEERS=coord-2:9090,coord-3:9090 \
  docker compose -f docker-compose.yml --profile cluster up -d \
  coord-1 coord-2 coord-3 order-service pay-service inventory-service
docker compose -f docker-compose.yml run --rm e2e-tests \
  mvn test -Dcucumber.filter.tags="@failover"

# 清理
docker compose -f docker-compose.yml --profile cluster --profile test down -v
rm -f coord-e2e-tests/.cache/e2e-security.json
```

## 安全域初始化（CI 注意事项）

- 测试框架在 `Hooks.java` 中自动完成：`initSeal()` → 获取 root token → 缓存到 `.cache/e2e-security.json`。
- CI runner 容器内 `.cache/` 目录生命周期与容器相同，**无需手动初始化**。
- 若 CI Job 因异常退出后**复用了旧 volume**，下次运行可能出现 "security domain already initialised but no cached root token" 错误，此时需清理 volume：
  ```bash
  docker compose -f docker-compose.yml --profile cluster --profile test down -v
  ```

## 关键环境变量

测试容器内已通过 `docker-compose.yml` 注入以下参数，无需额外配置：

| 变量 | 值 |
|------|----|
| `COORD_GRPC_ADDRESS` | `coord-1:9090` |
| `COORD_HTTP_ADDRESS` | `http://coord-1:8080` |
| `ORDER_SERVICE_URL` | `http://order-service:18080` |
| `PAY_SERVICE_URL` | `http://pay-service:18081` |
| `INVENTORY_SERVICE_URL` | `http://inventory-service:18082` |

## 预期测试结果

| 阶段 | 场景数 | 通过标准 |
|------|--------|----------|
| @smoke | 2 | 0 failures |
| @core + @smoke | ~102 | 0 failures |
| @failover | 4 | 0 failures |
| @slow（backup/restore） | 3 | 0 failures（可选，耗时较长） |

## 常见 CI 故障排查

| 错误 | 原因 | 解法 |
|------|------|------|
| `not leader, current leader is unknown` | coord 集群未完全选主 | 增加 healthcheck 等待时间或重试 |
| `coord-2/coord-3` 一直未加入集群 | 启动 `coord-1` 时未注入 `COORD_1_CLUSTER_PEERS` | 重新以 `COORD_1_CLUSTER_PEERS=coord-2:9090,coord-3:9090 docker compose ... up` 创建 cluster |
| `security domain already initialised` | volume 未清理 | `docker compose down -v` |
| `expected: 97 but was: 100` | order-service 幂等缓存残留 | 确认 `/api/internal/reset` 在每个场景前被调用（已内置于 `Hooks.java`） |
| 构建失败 `coord-sdk-base:local not found` | 未执行 `make sdk-base` | 先运行 `make sdk-base` |

补充说明：

- `coord-1/2/3` 的 Docker 日志已配置 `json-file` 轮转，单容器上限为 `10m x 3`。
- 周期性 `persisted runtime snapshot to redb` 日志为 `debug` 级别，默认 `info` 日志下不会持续刷屏。
