# language: zh-CN
@core
功能: 策略决策点（PDP）

  背景:
    假如 Coord 集群已启动

  场景: 写入策略 bundle 并列表查询
    当 写入策略 bundle tenant="e2e" namespace="authz" name="allow-all" rego="package authz\nallow = true"
    那么 返回 bundle_id 非空
    当 列出 tenant="e2e" 的策略 bundle
    那么 策略列表包含 name="allow-all"

  场景: 评估策略返回 allowed=true
    假如 策略 bundle tenant="e2e" namespace="authz" name="allow-all" rego="package authz\nallow = true" 已写入
    当 评估策略 bundle_id query="data.authz.allow" input="{}"
    那么 评估结果 allowed=true

  场景: 评估策略返回 allowed=false
    假如 策略 bundle tenant="e2e" namespace="authz" name="deny-all" rego="package authz\nallow = false" 已写入
    当 评估策略 bundle_id query="data.authz.allow" input="{}"
    那么 评估结果 allowed=false

  场景: 基于输入条件的动态策略
    假如 策略 bundle tenant="e2e" namespace="rbac" name="role-check" rego="package rbac\nallow {\n  input.role == \"admin\"\n}" 已写入
    当 评估策略 bundle_id query="data.rbac.allow" input="{\"role\":\"admin\"}"
    那么 评估结果 allowed=true
    当 评估策略 bundle_id query="data.rbac.allow" input="{\"role\":\"guest\"}"
    那么 评估结果 allowed=false

  场景: 禁用 bundle 后评估应返回错误
    假如 策略 bundle tenant="e2e" namespace="authz" name="to-disable" rego="package authz\nallow = true" 已写入
    当 禁用 bundle_id
    当 评估策略 bundle_id query="data.authz.allow" input="{}"
    那么 评估应返回错误

  场景: 解释策略返回调试信息
    假如 策略 bundle tenant="e2e" namespace="authz" name="explain-test" rego="package authz\nallow = true" 已写入
    当 解释策略 bundle_id query="data.authz.allow" input="{}"
    那么 返回解释行数 >= 1

  场景: 删除 bundle 后不可查询
    假如 策略 bundle tenant="e2e" namespace="authz" name="to-delete" rego="package authz\nallow = true" 已写入
    当 删除 bundle_id
    当 列出 tenant="e2e" 的策略 bundle
    那么 策略列表不包含 name="to-delete"
