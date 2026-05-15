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

### 快速启动

```bash
cd e2e

# 构建 Docker 镜像（首次约 5-10 分钟，后续增量编译约 2 分钟）
DOCKER_HOST=unix:///var/run/docker.sock \
  docker build \
  --secret id=cargo_credentials,src=~/.cargo/credentials.toml \
  -t e2e-coord-1:latest -t e2e-coord-2:latest -t e2e-coord-3:latest \
  -f coord-cluster/Dockerfile ..

# 启动测试环境
make e2e-up

# 运行测试
make e2e-test

# 清理
make e2e-reset
```

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

| 变量 | 默认值 | 说明 |
|------|--------|------|
| `COORD_GRPC_ENDPOINT` | `http://localhost:9090` | gRPC 端点 |
| `COORD_HTTP_ENDPOINT` | `http://localhost:9091` | HTTP 端点 |

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
