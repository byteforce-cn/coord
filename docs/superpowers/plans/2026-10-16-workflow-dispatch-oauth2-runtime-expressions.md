# Workflow 派发：运行时表达式求值 + OAuth2 client_credentials Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 关闭 byteforce-cn/coord#96 与 #97：让 `call`/operation 任务的 `with` 支持 `$context` / `$input` 运行时表达式求值，并让 HTTP 派发的 `auth` 支持 OAuth2 client_credentials（短 TTL 令牌取用、缓存、刷新、fail-closed）。

**Architecture:** 两处改动都收敛在 `coord-agent/src/services/workflow.rs` 的 `phase4::HttpTaskDispatcher`——派发前对 `with` 做递归运行时求值（求值器复用 `coord-core::workflow::expression::ExpressionEvaluator`，`$context`/`$input` 绑定到实例上下文），随后保留既有 `$secrets` / `$constants` 配置注入语义；OAuth2 令牌在 `apply_auth` 中以 client_credentials 换取，进程内按 (tokenUrl, clientId, scope) 缓存、按过期时间提前刷新，令牌端点失败按 5xx/timeout 可重试、4xx 不可重试映射到既有 `DispatchResult::Failure { retryable }`。

**Tech Stack:** Rust（coord-agent / coord-core、reqwest、tokio）、本地 TCP mock 服务器（既有测试风格）、Markdown（workflow-semantics §6）。

**Spec:** GitHub issue #96（`with` 运行时表达式与文档/实现不一致）与 #97（OAuth2 client_credentials），验收标准逐条见两 issue 正文。

## 本计划锁定的决定（实现者逐字遵守；偏差须走评审裁定）

1. **求值位置**：`HttpTaskDispatcher::dispatch` 内、`resolve_placeholders_checked` 之前，对 `with` 全树（`body` / `headers` / `auth` / `query` / `endpoint`）递归处理。
2. **表达式形态**：仅支持**整串单段** `${ ... }`（trim 后以 `${` 开头、以 `}` 结尾）；含内嵌 `${...}` 的字符串 fail-closed（与 `set` 既有"无字符串插值"口径一致），不静默当字面量发出。
3. **绑定白名单**：`$context`（= 实例上下文，即 `dispatch(.., input, ..)` 的 `input`）与 `$input`（同值）。其他 `$name`（如 `$workflow` / `$output` / `$secrets`）出现在运行时表达式里 ⇒ fail-closed 并点名绑定，绝不静默 `null`。
4. **表达式首字符闸门**：内层表达式首字符必须是 `$` / `.` / `"` / `(` / `{` / `-` / 数字，或整串为 `true` / `false` / `null`；否则 fail-closed（防求值器"未识别 token → 返回整个 context"的静默误读）。空表达式同样 fail-closed。
5. **`$secrets` / `$constants` 优先级**：含 `$secrets` / `$constants` 的字符串交给既有配置注入（支持内嵌，如 `Bearer ${ $secrets.token }`），不参与运行时求值；同一字符串若还含其他 `${...}` ⇒ fail-closed（混合形态）。
6. **令牌缓存**：cache key = (tokenUrl, clientId, scope)，**不含 client_secret**；提前刷新窗口 = min(30s, ttl/10)，`expires_in` 缺失/非正 ⇒ 不缓存（每次取）。单飞：同一 dispatcher 内令牌获取串行（tokio Mutex 跨请求持有）。
7. **令牌端点失败口径**：连接/超时 ⇒ `retryable: true`；5xx ⇒ `retryable: true`；4xx / 响应缺 `access_token` / 非 JSON ⇒ `retryable: false`。错误信息可含 OAuth2 `error` / `error_description`（截断），但必须对 client_secret 做字面 + `[redacted]` 替换后输出；请求失败路径不含请求体。
8. **secret 不进日志/错误**：oauth2 配置缺失只点名字段名（`auth.clientSecret` 等），不含值。
9. **文档单一归属**：口径写进 `docs/production/workflow-semantics.md` §6（issue 验收项），不在别处复述。

## Global Constraints

- 现有测试不得回归：`coord-agent` 的 `phase4_tests` 全绿（`$secrets` / `$constants` 行为逐字保留）。
- 只用现有依赖（reqwest/tokio/serde_json），不新增 crate。
- 注释遵守 AGENTS.md：只写当前「为什么」，无日期/章节号/变更史，不新增 `TODO/FIXME`。
- 临时文件只放 `/tmp/coord/`；仓库内不建临时目录。
- 每个任务收尾至少跑：`cargo test -p coord-agent`、`cargo fmt --all -- --check`、`cargo clippy --workspace --lib --bins -- -D warnings`、`bash scripts/check-doc-refs.sh`。

## Review Focus

- `with` 中未配置的 `$secrets` key：必须仍在派发前失败且不得发出请求（既有 e2e 判据）。
- 表达式命中缺失字段（`$context.a.b` 中 `a` 不存在）：jq 语义返回 `null`，不视为求值失败（与引擎其余表达式一致）。
- 令牌端点返回的 `expires_in` 为 0 / 缺失：不得缓存到下次派发。
- 4xx 响应体回显 client_secret：错误信息必须已脱敏。
- 运行时表达式与 `$secrets` 混在同一字符串：fail-closed，不发出占位符字面量。

---

### Task 1: `with` 运行时表达式求值（#96）

**Files:**
- Modify: `coord-agent/src/services/workflow.rs`（`phase4::HttpTaskDispatcher` 求值管线 + `phase4_tests`）
- Modify: `docs/production/workflow-semantics.md` §6

**Interfaces:**
- Consumes: `coord_core::workflow::expression::ExpressionEvaluator::{evaluate_with_vars, unwrap_expression}`（已有）。
- Produces（`phase4` 内私有）：
  - `fn evaluate_runtime_expressions(&self, value: &Value, context: &Value) -> Result<Value, String>` — 递归求值，`Err` 为 fail-closed 错误（不含凭据值；调用方加 `workflow dispatch blocked:` 前缀）。
  - `fn expression_bindings(expr: &str) -> Vec<String>` — 提取 `$name`（忽略字符串字面量）。
  - `fn strip_config_placeholders(s: &str) -> String` — 去掉 `${ $secrets.<key> }` / `${ $constants.<key> }` 片段（混合形态检测用）。

- [ ] **Step 1: 写失败用例（phase4_tests）**

```rust
/// `$context` 在 body / headers / query / auth 中的整串表达式求值。
#[tokio::test]
async fn test_http_dispatcher_context_expression_evaluated() {
    let (addr, log) = spawn_request_recording_server().await;
    let dispatcher = HttpTaskDispatcher::new();
    let with = serde_json::json!({
        "method": "POST",
        "endpoint": format!("http://{addr}/commands/activate"),
        "query": {"emp": "${ $context.target.id }"},
        "headers": {"X-Employee": "${ $context.target.id }"},
        "body": {"payload": {"employee_id": "${ $context.target.id }"}, "n": "${ $input.count }"},
        "auth": {"scheme": "bearer", "token": "${ $context.token }"},
    });
    let input = serde_json::json!({"target": {"id": "E-42"}, "count": 2, "token": "CTX-TOKEN"});
    match dispatcher.dispatch("http", Some(&with), &input, None).await {
        DispatchResult::Success { .. } => {}
        DispatchResult::Failure { error, .. } => panic!("HTTP dispatch failed: {error}"),
    }
    let req = log.lock().join("\n");
    assert!(req.contains("POST /commands/activate?emp=E-42 "), "query 未求值：{req}");
    assert!(req.contains("x-employee: E-42"), "header 未求值：{req}");
    assert!(req.contains(r#""employee_id":"E-42""#), "body 未求值：{req}");
    assert!(req.contains(r#""n":2"#), "$input 未求值：{req}");
    assert!(req.contains("authorization: Bearer CTX-TOKEN"), "auth 未求值：{req}");
    assert!(!req.contains("${"), "占位符字面量不得发出：{req}");
}

/// 未知绑定 / 内嵌表达式 ⇒ fail-closed，且请求不发出。
#[tokio::test]
async fn test_http_dispatcher_expression_failure_fails_closed() {
    let (addr, log) = spawn_request_recording_server().await;
    for with in [
        serde_json::json!({"method":"GET","endpoint":format!("http://{addr}/x"),
                           "body": {"v": "${ $workflow.id }"}}),
        serde_json::json!({"method":"GET","endpoint":format!("http://{addr}/x"),
                           "query": {"v": "id=${ $context.target.id }"}}),
        serde_json::json!({"method":"GET","endpoint":format!("http://{addr}/x"),
                           "body": {"v": "${ $context.target.id } ${ $context.token }"}}),
    ] {
        match dispatcher.dispatch("http", Some(&with), &input, None).await {
            DispatchResult::Failure { error, retryable } => {
                assert!(!retryable);
                assert!(error.contains("workflow dispatch blocked"), "error: {error}");
                assert!(!error.contains("${"), "错误不得回显占位符：{error}");
            }
            DispatchResult::Success { .. } => panic!("fail-closed 不得派发成功"),
        }
    }
    assert!(log.lock().is_empty(), "fail-closed 不得发出请求");
}
```

（`spawn_request_recording_server` 为新增测试辅助：逐连接记录完整请求原文，供 body/header/query 断言。）

- [ ] **Step 2: 跑测试确认按预期失败**

Run: `cargo test -p coord-agent test_http_dispatcher_context_expression_evaluated test_http_dispatcher_expression_failure_fails_closed`
Expected: FAIL——`request recording server` 未定义 / 断言 `query 未求值`（当前实现原样发出 `${ $context.target.id }`）。

- [ ] **Step 3: 实现求值管线**

`HttpTaskDispatcher::dispatch` 在 `resolve_placeholders_checked` 前插入：
`let with = self.resolve_runtime_expressions_checked(w, input)?`；
`resolve_runtime_expressions_checked` 递归遍历，逐字符串按锁定决定 2–5 判定，返回 `Result<Value, String>`；`dispatch` 将 `Err(msg)` 映射为 `DispatchResult::Failure { error: format!("workflow dispatch blocked: {msg}"), retryable: false }`。

- [ ] **Step 4: 跑测试确认通过（含既有 $secrets/$constants 用例）**

Run: `cargo test -p coord-agent phase4`
Expected: PASS（新用例 + `test_http_dispatcher_secret_injected_into_authorization_header`、`test_http_dispatcher_constants_replacement`、`test_http_dispatcher_missing_placeholders_fail_closed` 全绿）。

- [ ] **Step 5: 更新 `workflow-semantics.md` §6**

补：`with` 内 `${ $context.x }` / `${ $input.x }` 求值口径、单一整串表达式（无插值）、未知绑定/内嵌/求值失败 fail-closed、与 `$secrets`/`$constants` 的先后与混合拒绝；判据列表追加新用例名。

- [ ] **Step 6: 门禁 + 提交**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-doc-refs.sh
git add coord-agent/src/services/workflow.rs docs/production/workflow-semantics.md
git commit -m "fix(workflow): evaluate \$context/\$input runtime expressions in call.with"
```

---

### Task 2: OAuth2 client_credentials 派发认证（#97）

**Files:**
- Modify: `coord-agent/src/services/workflow.rs`（`phase4::HttpTaskDispatcher::apply_auth` + 缓存 + `phase4_tests`）
- Modify: `docs/production/workflow-semantics.md` §6

**Interfaces:**
- Consumes: `Task 1` 的求值管线（oauth2 字段中的 `$secrets` 占位符先被注入）。
- Produces（`phase4` 内私有）：
  - `struct OAuth2CacheKey { token_url: String, client_id: String, scope: Option<String> }`（`Hash + Eq`）
  - `struct CachedToken { token: String, expires_at: std::time::Instant }`
  - `async fn apply_auth(&self, req: RequestBuilder, auth: &Value) -> Result<RequestBuilder, DispatchResult>`
  - `async fn oauth2_token(&self, token_url: &str, client_id: &str, client_secret: &str, scope: Option<&str>) -> Result<String, DispatchResult>`
  - `fn redact_secret(text: &str, secret: &str) -> String`

- [ ] **Step 1: 写失败用例**

```rust
/// 按 auth.scheme=oauth2 取令牌并作为 Bearer 发出；client_id/secret/scope 只走表单。
#[tokio::test]
async fn test_http_dispatcher_oauth2_gets_token_and_sends_bearer() { /* token server 断言 grant_type/client_id/client_secret/scope + resource 断言 Bearer MOCK-TOKEN-1 */ }

/// 未过期令牌复用（两次派发 = 一次取令牌）。
#[tokio::test]
async fn test_http_dispatcher_oauth2_token_cached_until_expiry() { /* expires_in=300 */ }

/// 过期/无 TTL 令牌刷新（两次派发 = 两次取令牌）。
#[tokio::test]
async fn test_http_dispatcher_oauth2_token_refreshed_when_expired() { /* expires_in=0 */ }

/// 令牌端点 5xx ⇒ retryable；4xx ⇒ 非 retryable 且错误脱敏。
#[tokio::test]
async fn test_http_dispatcher_oauth2_token_endpoint_failure_semantics() { /* 503 → retryable=true；400+回显 secret → retryable=false 且不含 secret */ }

/// 配置缺失 ⇒ fail-closed 且不发请求。
#[tokio::test]
async fn test_http_dispatcher_oauth2_missing_config_fails_closed() { /* 缺 tokenUrl */ }
```

（`spawn_oauth2_token_server` 为新增测试辅助：可切换响应（token/5xx/4xx）、记录表单与请求次数。）

- [ ] **Step 2: 跑测试确认按预期失败**

Run: `cargo test -p coord-agent test_http_dispatcher_oauth2`
Expected: FAIL——`oauth2` scheme 当前落到 `_ => req`，资源请求无 `Authorization: Bearer <token>`。

- [ ] **Step 3: 实现 oauth2 认证与缓存**

`apply_auth` 改 async `Result`；`"oauth2"` 分支校验 `tokenUrl`/`clientId`/`clientSecret`（缺失 ⇒ 非重试失败，只点名字段），`oauth2_token` 命中缓存直接返回，未命中持锁取令牌（表单 `grant_type=client_credentials&client_id=..&client_secret=..[&scope=..]`），成功后按锁定决定 6 缓存；`dispatch_http` 的错误分支把 `apply_auth` 的 `Err` 直接短路返回。

- [ ] **Step 4: 跑测试确认通过（全 phase4 套件）**

Run: `cargo test -p coord-agent phase4`
Expected: PASS。

- [ ] **Step 5: 更新 `workflow-semantics.md` §6**

补 oauth2 配置形状（`tokenUrl` / `clientId` / `clientSecret` / `scope`，credential 必须来自 `$secrets`）、缓存/提前刷新窗口、令牌端点 5xx/4xx/timeout 的可重试映射、错误脱敏；判据追加新用例名。

- [ ] **Step 6: 门禁 + 提交**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
cargo test -p coord-agent
bash scripts/check-doc-refs.sh
git add coord-agent/src/services/workflow.rs docs/production/workflow-semantics.md
git commit -m "feat(workflow): oauth2 client_credentials auth for HTTP dispatch"
```

---

### Task 3（合并后，main）: 发布 v0.3.2 版本对齐

**Files:**
- Modify: `Cargo.toml`（workspace version + 7 个内部 path 依赖同口径）、`Cargo.lock`
- Modify: `README.md`、`README.zh-CN.md`、`SECURITY.md`
- Modify: `coord-java-sdk/pom.xml`、`coord-java-sdk/README.md`
- Modify: `deploy/k8s/statefulset.yaml`（镜像 tag）
- Modify: `docs/production/ops/k8s-verification.md`、`docs/production/ops/runbook.md`、`docs/production/volume-object-storage.md`

- [ ] **Step 1: 全量版本事实从 0.3.1 → 0.3.2**

`rg -n '0\.3\.1'` 逐处判定（排除历史 plan / jepsen 第三方版本），只改「当前版本事实」；镜像 tag 与 `coord-java-sdk` pom 必须同为 `0.3.2`。

- [ ] **Step 2: 门禁**

```bash
cargo fmt --all -- --check
cargo clippy --workspace --lib --bins -- -D warnings
bash scripts/check-panics.sh
bash scripts/check-doc-refs.sh
cargo test --workspace --lib --bins
# Java SDK：JDK21 语义 target 下 mvn -q verify
```

- [ ] **Step 3: 提交并打 tag**

```bash
git commit -m "chore(release): v0.3.2"
git tag v0.3.2
# push main + tag
```

---

## 执行记录

- Task 1 / 2 在 `fix/workflow-dispatch-oauth2-expressions` 分支经 PR 合并（Closes #96 / #97）。
- Task 3 在合并后的 main 上执行，远端 tag `v0.3.2` 与 `Cargo.toml` / `coord-java-sdk` / k8s 镜像 tag 对齐。
