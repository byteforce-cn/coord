# language: zh-CN
@core
功能: 工作流引擎 v2 — CNCF Serverless Workflow

  背景:
    假如 Coord 集群已启动

  场景: 部署工作流定义
    当 部署工作流定义 "order-flow" v"1.0"
    那么 返回 definition_id 非空
    并且 definition_id="order-flow" version="1.0"

  场景: 启动工作流实例
    假如 已部署工作流定义 "order-flow" v"1.0"
    当 启动工作流实例 definition="order-flow" version="1.0" input={}
    那么 返回 instance_id 非空

  场景: 实例状态最终完成
    假如 已部署工作流定义 "order-flow" v"1.0"
    当 启动工作流实例 definition="order-flow" version="1.0" input={}
    那么 实例状态为 "COMPLETED"

  场景: 列举工作流定义
    假如 已部署工作流定义 "list-wf" v"2.0"
    当 列出工作流定义
    那么 列表包含 "list-wf" v"2.0"

  场景: 列举工作流实例
    假如 已部署工作流定义 "inst-wf" v"1.0"
    当 启动工作流实例 definition="inst-wf" version="1.0" input={}
    当 列出工作流实例 definition="inst-wf"
    那么 列表包含当前实例

  场景: 更新工作流定义
    假如 已部署工作流定义 "update-wf" v"1.0"
    当 更新工作流定义 "update-wf" v"1.0" 修改描述
    那么 定义 "update-wf" v"1.0" 描述包含 "Updated"

  # ── DSL 控制流场景（P0 补齐）──────────────────────────────────────────────────

  场景: switch 条件分支命中 valid 分支
    假如 已部署 switch 工作流 "switch-wf" v"1.0"
    当 启动工作流实例 definition="switch-wf" version="1.0" input={"is_valid":true}
    那么 实例状态为 "COMPLETED"
    并且 实例上下文中 "branch" 为 "valid"

  场景: switch 默认分支兜底
    假如 已部署 switch 工作流 "switch-default-wf" v"1.0"
    当 启动工作流实例 definition="switch-default-wf" version="1.0" input={"status":"unknown"}
    那么 实例状态为 "COMPLETED"
    并且 实例上下文中 "branch" 为 "default"

  场景: fork 两个分支并行执行
    假如 已部署 fork 工作流 "fork-wf" v"1.0"
    当 启动工作流实例 definition="fork-wf" version="1.0" input={}
    那么 实例状态为 "COMPLETED"

  场景: for 循环迭代列表
    假如 已部署 for 工作流 "for-wf" v"1.0"
    当 启动工作流实例 definition="for-wf" version="1.0" input={"items":[1,2,3]}
    那么 实例状态为 "COMPLETED"

  场景: try/catch 捕获 raise 错误后完成
    假如 已部署 try-catch 工作流 "trycatch-wf" v"1.0"
    当 启动工作流实例 definition="trycatch-wf" version="1.0" input={}
    那么 实例状态为 "COMPLETED"

  场景: jq 表达式变换输入
    假如 已部署 jq 工作流 "jq-wf" v"1.0"
    当 启动工作流实例 definition="jq-wf" version="1.0" input={"items":[1,2,3]}
    那么 实例状态为 "COMPLETED"
    并且 实例上下文中 "itemCount" 为数字 3
