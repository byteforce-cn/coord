# 策略（Policy PDP）

coord 内置策略决策点（PDP），基于 [Regorus](https://github.com/microsoft/regorus)（Microsoft OPA/Rego 兼容引擎）进行策略评估，支持运行时动态加载 / 启用 / 禁用 Rego bundle。

---

## 一、PolicyService gRPC 接口

所有策略操作通过 `coord.v1.PolicyService` 进行：

| 方法 | 能力要求 | 说明 |
|------|----------|------|
| `PutPolicyBundle` | `policy.write` | 上传或更新 bundle |
| `SetBundleEnabled` | `policy.write` | 启用 / 禁用 bundle |
| `DeletePolicyBundle` | `policy.write` | 删除 bundle |
| `ListPolicyBundles` | `policy.read` | 列出所有 bundle |
| `Evaluate` | `policy.evaluate` | 执行策略评估 |
| `Explain` | `policy.evaluate` | 执行策略评估并返回推理路径 |

> **注意**：`coord ctl` CLI **没有** `policy` 子命令，策略管理需通过 Java SDK、Go SDK 或 gRPC 直接调用。

---

## 二、Bundle 格式

Bundle 采用 YAML 格式，包含 Rego 模块定义：

```yaml
id: authz-bundle
description: "服务间授权策略"
modules:
  - name: "authz"
    rego: |
      package authz

      default allow = false

      allow {
        input.subject.role == "admin"
      }

      allow {
        input.resource.type == "public"
      }
```

### 字段说明

| 字段 | 必填 | 说明 |
|------|------|------|
| `id` | ✅ | Bundle 唯一标识，更新时使用相同 ID 覆盖 |
| `description` | — | 人类可读描述 |
| `modules[].name` | ✅ | Rego 模块名 |
| `modules[].rego` | ✅ | Rego 策略内容 |

---

## 三、Java SDK 使用示例

```java
PolicyServiceBlockingStub policy = PolicyServiceGrpc.newBlockingStub(channel)
    .withCallCredentials(new TokenCallCredentials(adminToken));

// 上传 bundle
PutPolicyBundleResponse putResp = policy.putPolicyBundle(
    PutPolicyBundleRequest.newBuilder()
        .setBundleId("authz-bundle")
        .setBundle(bundleYaml)
        .build()
);

// 启用 bundle
policy.setBundleEnabled(SetBundleEnabledRequest.newBuilder()
    .setBundleId("authz-bundle")
    .setEnabled(true)
    .build());

// 评估
EvaluateResponse evalResp = policy.evaluate(EvaluateRequest.newBuilder()
    .setQuery("data.authz.allow")
    .setInputJson("{\"subject\":{\"role\":\"admin\"},\"resource\":{\"type\":\"config\"}}")
    .build());
System.out.println(evalResp.getResultJson());  // true
```

---

## 四、Evaluate / Explain

`Evaluate` 返回 Rego query 的求值结果（JSON）：

```json
// query: "data.authz.allow"
// input: {"subject":{"role":"admin"}}
true
```

`Explain` 在结果之外返回推理路径，便于调试策略逻辑：

```json
{
  "result": true,
  "trace": [
    {"op": "eval", "node": "authz.allow", "result": true},
    ...
  ]
}
```

---

## 五、Bundle 生命周期

```
PutPolicyBundle  →  SetBundleEnabled(true)  →  Evaluate / Explain
                 ↓
         SetBundleEnabled(false)  →  bundle 不参与评估（但仍存储）
                 ↓
         DeletePolicyBundle  →  彻底删除
```

---

## 六、e2e 测试参考

策略功能的完整端到端测试见：
`e2e/coord-e2e-tests/src/test/resources/features/18_policy.feature`

该 feature 覆盖了 bundle 上传、启用、禁用、评估、解释、删除的完整生命周期场景。
