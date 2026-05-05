# 快速测试步骤（本地单节点）

适用场景：日常功能开发、PR 提交前自查，不含集群故障注入场景。

## 前提条件

| 依赖 | 版本 |
|------|------|
| Docker Desktop | 4.x+ |
| JDK | 17 |
| Maven | 3.8+ |

## 第一次运行

```bash
# 在 e2e/ 目录下执行
cd e2e

# 1. 编译业务服务 jar（首次或修改 mock-services 后需要）
make jars

# 2. 启动单节点环境（coord-1 + 3 个业务服务）
make e2e-up

# 3. 等待所有容器变为 healthy（约 30-60 秒）
docker ps --format '{{.Names}}\t{{.Status}}' | grep -E 'coord-1|order|pay|inventory'

# 4. 运行冒烟测试（@smoke，约 20-30 秒）
make e2e-smoke

# 5. 运行核心回归测试（@core + @smoke，约 5-10 分钟）
make e2e-test
```

## 后续运行（环境已存在）

```bash
# 直接运行冒烟
make e2e-smoke

# 或核心回归
make e2e-test
```

## 运行单个 feature 文件

```bash
# 以 04_lock 为例（不含 .feature 后缀）
make e2e-feature FEATURE=04_lock
```

## 测试标签说明

| 标签 | 场景数 | 描述 |
|------|--------|------|
| `@smoke` | 2 | 端到端冒烟，验证核心链路可用 |
| `@core` | ~100 | 功能回归，覆盖所有服务 |
| `@failover` | 4 | 集群故障注入（需 3 节点环境） |
| `@slow` | 3 | 耗时较长（backup/restore 等） |

`make e2e-test` 等价于 `not @failover and not @slow`，即运行 `@smoke` + `@core`。

## 重置环境

```bash
# 清理所有 volume 和安全缓存，下次会重新初始化 security domain
make e2e-reset

# 重新启动
make e2e-up
```

## 调试技巧

```bash
# 查看实时日志
make logs

# 查看容器状态
make ps

# 仅重新打某个服务 jar 并重启（约 15s）
make reload-order-service
make reload-pay-service
make reload-inventory-service
```

## 预期结果

```
[INFO] Results:
[WARNING] Tests run: 112, Failures: 0, Errors: 0, Skipped: 110
[INFO] BUILD SUCCESS
```

- `Tests run: 112`：JUnit 发现的测试方法数（CucumberTestRunner 计数方式）
- `Skipped: 110`：被过滤掉的场景（`@failover`、`@slow`）
- 两个 `@smoke` 场景均通过即为冒烟成功
