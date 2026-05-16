# 测试指南

---

## 一、单元测试

```bash
# 运行所有单元测试
cargo test

# 运行特定 crate
cargo test -p coord-core
cargo test -p coord

# 运行特定测试
cargo test -p coord interceptors::tests
```

---

## 二、e2e 测试

e2e 测试位于 `e2e/` 目录，基于 Spring Boot + Cucumber（Java），对运行中的 coord Docker 容器执行黑盒验证。

### 本地单节点

```bash
cd e2e

# 编译本地业务服务 jar
make jars

# 启动单节点环境（coord-1 + mock services）
make e2e-up

# 冒烟 / 核心回归
make e2e-smoke
make e2e-test
```

> `make e2e-up` 只启动 `coord-1` 和 3 个业务服务。此路径下 `coord-1` 默认以 `COORD_CLUSTER_PEERS=""` 创建，不会主动探测 `coord-2` / `coord-3`。

### 集群 / Failover

```bash
cd e2e

# 启动三节点集群
make e2e-up-cluster

# 仅运行 failover 场景
make e2e-failover
```

> `make e2e-up-cluster` 与 `make e2e-full` 会自动注入 `COORD_1_CLUSTER_PEERS=coord-2:9090,coord-3:9090`。若绕过 Makefile 直接执行 `docker compose`，必须手工带上该环境变量。

### CI / 全量

```bash
cd e2e
make e2e-full
```

详细步骤见 [../e2e/TESTING-FAST.md](../e2e/TESTING-FAST.md)、[../e2e/TESTING-CLUSTER.md](../e2e/TESTING-CLUSTER.md) 与 [../e2e/TESTING-CI.md](../e2e/TESTING-CI.md)。

### 运行单个 Feature

```bash
# 指定 feature 文件名（不带路径和 .feature 扩展）
make e2e-feature FEATURE=08_transit
make e2e-feature FEATURE=18_policy
```

### 测试报告

测试完成后报告写入：
`e2e/coord-e2e-tests/target/cucumber-reports/report.json`

快速分析：

```bash
python3 -c "
import json
data = json.load(open('coord-e2e-tests/target/cucumber-reports/report.json'))
passed = failed = 0
for f in data:
    for e in f.get('elements', []):
        if e.get('type') == 'scenario':
            if any(s.get('result',{}).get('status') == 'failed' for s in e.get('steps',[])):
                failed += 1
            else:
                passed += 1
print(f'Passed: {passed}, Failed: {failed}')
"
```

---

## 三、Feature 文件列表

| 文件 | 覆盖范围 |
|------|---------|
| `01_cluster.feature` | 集群启动、心跳、Leader 查询 |
| `02_registry.feature` | 服务注册、发现、注销、TTL |
| `03_config.feature` | 配置读写、删除 |
| `04_lock.feature` | 分布式锁获取 / 释放 / 竞争 |
| `05_idgen.feature` | 全局唯一 ID 生成 |
| `07_workflow_v2.feature` | 工作流 v2 部署 / 启动 / 状态 |
| `08_transit.feature` | 加密 / 解密 / 密钥轮换 / HMAC |
| `09_pki.feature` | 证书颁发 / 续期 / 吊销 / CRL |
| `10_security.feature` | Seal/Unseal、AppRole、Token 管理 |
| `11_order_flow.feature` | 跨服务订单业务流程集成 |
| `12_backup_restore.feature` | 备份与恢复 |
| `13_workflow_integration.feature` | 工作流跨服务集成 |
| `14_pay_transit.feature` | 支付服务 Transit 集成 |
| `15_observability.feature` | Prometheus 指标 |
| `16_config_refresh.feature` | 动态配置刷新 |
| `17_workflow_integration.feature` | 工作流业务链路集成 |
| `18_policy.feature` | 策略 PDP 生命周期 |
| `99_smoke.feature` | 全量冒烟测试 |

标记为 `@failover` 或 `@slow` 的场景默认跳过，需多节点环境时单独执行。

---

## 四、环境变量（e2e）

宿主机直接执行 `make e2e-smoke` / `make e2e-test` / `make e2e-feature` 时，Makefile 会传入以下测试参数：

| 参数 | 值 |
|------|----|
| `coord.grpc.address` | `localhost:9090` |
| `coord.http.address` | `http://[::1]:8080` |
| `order.service.url` | `http://localhost:18080` |
| `pay.service.url` | `http://localhost:18081` |
| `inventory.service.url` | `http://localhost:18082` |

容器化 runner `make e2e-full` 使用的 compose 环境变量为：

| 变量 | 值 |
|------|----|
| `COORD_GRPC_ADDRESS` | `coord-1:9090` |
| `COORD_HTTP_ADDRESS` | `http://coord-1:8080` |
| `ORDER_SERVICE_URL` | `http://order-service:18080` |
| `PAY_SERVICE_URL` | `http://pay-service:18081` |
| `INVENTORY_SERVICE_URL` | `http://inventory-service:18082` |

补充说明：

- `coord-1/2/3` 的 Docker 日志已配置 `json-file` 轮转，单容器上限为 `10m x 3`。
- 周期性 `persisted runtime snapshot to redb` 日志为 `debug` 级别，默认 `info` 日志下不会持续刷屏。

---

## 五、Docker 镜像构建原理

e2e 使用 BuildKit `--mount=type=cache` 实现增量编译：

- `coord-cargo-registry` — 已下载 crate 源码（首次下载后不再重复）
- `coord-cargo-git` — git 依赖缓存
- `coord-cargo-target` — 编译产物（只重编改动的文件）

重置 BuildKit 缓存（完全重建）：

```bash
docker builder prune --filter type=exec.cachemount
```
