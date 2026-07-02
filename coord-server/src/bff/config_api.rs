// BFF Internal Routes — Config Center API
//
// REST 端点用于配置中心：
// - GET    /v1/configs                                  → 配置列表
// - POST   /v1/configs                                  → 创建配置
// - GET    /v1/configs/{group}/{key}                     → 配置详情（当前版本）
// - PUT    /v1/configs/{group}/{key}                     → 更新配置（CAS）
// - DELETE /v1/configs/{group}/{key}                     → 删除配置及历史版本
// - GET    /v1/configs/{group}/{key}/versions            → 版本历史列表
// - GET    /v1/configs/{group}/{key}/versions/{version}  → 指定版本详情
// - POST   /v1/configs/{group}/{key}/rollback            → 回滚至指定版本
//
// 数据存储于 KV store 中，key 前缀约定：
//   /coord/configs/data/{group}/{key}                      → JSON 配置信封（当前版本）
//   /coord/configs/versions/{group}/{key}/{version:020}    → JSON 历史版本
//
// CAS 并发控制：使用配置信封中的 version 字段做乐观锁。

use std::sync::Arc;

use axum::{
    Json,
    extract::{Path, Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::internal::InternalState;
use crate::raft::type_config::{Command, Response};
use crate::server::CoordNode;

// ──── 请求/响应类型 ────

#[derive(Debug, Deserialize)]
pub struct ListConfigsQuery {
    pub group: Option<String>,
    pub q: Option<String>,
    pub page: Option<u64>,
    #[serde(rename = "pageSize")]
    pub page_size: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct CreateConfigRequest {
    pub group: String,
    pub key: String,
    pub format: String,
    pub data: String,
    #[serde(rename = "changeNote")]
    pub change_note: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateConfigRequest {
    pub data: String,
    pub version: u64,
    #[serde(rename = "changeNote")]
    pub change_note: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct RollbackRequest {
    pub version: u64,
}

// ──── 配置信封（存储在 KV 中） ────

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ConfigEnvelope {
    pub group: String,
    pub key: String,
    pub version: u64,
    pub format: String,
    pub data: String,
    #[serde(rename = "createdAt")]
    pub created_at: String,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    #[serde(rename = "updatedBy")]
    pub updated_by: String,
    #[serde(rename = "changeNote", default)]
    pub change_note: String,
}

// ──── 配置列表条目（返回给前端） ────

#[derive(Debug, Serialize)]
pub struct ConfigListItem {
    pub group: String,
    pub key: String,
    pub format: String,
    pub version: u64,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    #[serde(rename = "updatedBy")]
    pub updated_by: String,
}

// ──── 版本历史条目 ────

#[derive(Debug, Serialize)]
pub struct VersionEntry {
    pub version: u64,
    #[serde(rename = "updatedAt")]
    pub updated_at: String,
    #[serde(rename = "updatedBy")]
    pub updated_by: String,
    #[serde(rename = "changeNote")]
    pub change_note: String,
}

// ──── KV Key 构造 ────

const CONFIG_DATA_PREFIX: &str = "/coord/configs/data/";
const CONFIG_VERSIONS_PREFIX: &str = "/coord/configs/versions/";

fn config_data_key(group: &str, key: &str) -> Vec<u8> {
    format!("{}{}/{}", CONFIG_DATA_PREFIX, group, key).into_bytes()
}

fn config_data_prefix_for_group(group: &str) -> Vec<u8> {
    format!("{}{}/", CONFIG_DATA_PREFIX, group).into_bytes()
}

fn config_data_prefix() -> Vec<u8> {
    CONFIG_DATA_PREFIX.as_bytes().to_vec()
}

fn config_version_key(group: &str, key: &str, version: u64) -> Vec<u8> {
    format!("{}{}/{}/{:020}", CONFIG_VERSIONS_PREFIX, group, key, version).into_bytes()
}

fn config_version_prefix(group: &str, key: &str) -> Vec<u8> {
    format!("{}{}/{}/", CONFIG_VERSIONS_PREFIX, group, key).into_bytes()
}

// ──── 辅助函数 ────

fn ok_json(data: Value) -> (StatusCode, Json<Value>) {
    (
        StatusCode::OK,
        Json(json!({"code": 0, "data": data, "message": "success"})),
    )
}

fn err_json(code: i32, message: &str) -> (StatusCode, Json<Value>) {
    (
        StatusCode::OK,
        Json(json!({"code": code, "data": Value::Null, "message": message})),
    )
}

fn validate_token(state: &InternalState, headers: &HeaderMap) -> Result<String, (StatusCode, Json<Value>)> {
    let token = crate::bff::internal::extract_bearer_token(headers)
        .ok_or_else(|| err_json(401, "缺少认证 Token"))?;
    state.token_manager.validate(&token)
        .map_err(|_| err_json(403, "Token 无效或已过期"))
}

async fn raft_put(
    node: &CoordNode,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), (StatusCode, Json<Value>)> {
    if let Some(ref raft) = node.raft {
        let cmd = Command::Put { key, value, lease_id: None };
        let resp = raft.client_write(cmd).await.map_err(|e| {
            err_json(500, &format!("Raft 写入失败: {e}"))
        })?;
        match resp.response() {
            Response::Put { .. } => Ok(()),
            _ => Err(err_json(500, "意外的 Raft 响应")),
        }
    } else {
        node.storage.put(&key, &value, None)
            .map_err(|e| err_json(500, &format!("存储写入失败: {e}")))?;
        Ok(())
    }
}

async fn raft_delete(
    node: &CoordNode,
    key: Vec<u8>,
) -> Result<(), (StatusCode, Json<Value>)> {
    if let Some(ref raft) = node.raft {
        let cmd = Command::Delete { key };
        let resp = raft.client_write(cmd).await.map_err(|e| {
            err_json(500, &format!("Raft 写入失败: {e}"))
        })?;
        match resp.response() {
            Response::Delete { .. } => Ok(()),
            _ => Err(err_json(500, "意外的 Raft 响应")),
        }
    } else {
        node.storage.delete(&key)
            .map_err(|e| err_json(500, &format!("存储删除失败: {e}")))?;
        Ok(())
    }
}

fn current_time_iso() -> String {
    use std::time::SystemTime;
    let dur = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    let days = secs / 86400;
    let secs_in_day = secs % 86400;
    let h = secs_in_day / 3600;
    let m = (secs_in_day % 3600) / 60;
    let s = secs_in_day % 60;

    let mut y = 1970i64;
    let mut rd = days as i64;
    loop {
        let diy = if is_leap(y) { 366 } else { 365 };
        if rd < diy { break; }
        rd -= diy;
        y += 1;
    }
    let md = if is_leap(y) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut mo = 1u32;
    for &days_in_m in &md {
        if rd < days_in_m as i64 { break; }
        rd -= days_in_m as i64;
        mo += 1;
    }
    let d = rd as u32 + 1;
    format!("{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z", y, mo, d, h, m, s)
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}

// ──── Handler: GET /v1/configs ────

/// 配置列表：前缀扫描 /coord/configs/data/
pub async fn list_configs(
    State(state): State<Arc<InternalState>>,
    headers: HeaderMap,
    Query(query): Query<ListConfigsQuery>,
) -> impl IntoResponse {
    let _username = match validate_token(&state, &headers) {
        Ok(u) => u,
        Err(e) => return e.into_response(),
    };

    let node = &state.coord_node;

    // 根据 group 筛选决定扫描前缀
    let scan_prefix = if let Some(ref g) = query.group {
        config_data_prefix_for_group(g)
    } else {
        config_data_prefix()
    };

    let results = match node.storage.range(&scan_prefix, usize::MAX) {
        Ok(r) => r,
        Err(e) => return err_json(500, &format!("存储读取失败: {e}")).into_response(),
    };

    let mut configs: Vec<ConfigListItem> = Vec::new();

    for (_k, v) in &results {
        let envelope: ConfigEnvelope = match serde_json::from_slice(v) {
            Ok(e) => e,
            Err(_) => continue,
        };

        // 搜索过滤
        if let Some(ref q) = query.q {
            let q_lower = q.to_lowercase();
            if !envelope.key.to_lowercase().contains(&q_lower) {
                continue;
            }
        }

        configs.push(ConfigListItem {
            group: envelope.group,
            key: envelope.key,
            format: envelope.format,
            version: envelope.version,
            updated_at: envelope.updated_at,
            updated_by: envelope.updated_by,
        });
    }

    // 排序：先按 group 再按 key
    configs.sort_by(|a, b| a.group.cmp(&b.group).then(a.key.cmp(&b.key)));

    let total = configs.len() as u64;

    // 分页
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(20).min(100).max(10);
    let start = ((page - 1) * page_size) as usize;
    let end = (start + page_size as usize).min(configs.len());
    let paged: Vec<&ConfigListItem> = if start < configs.len() {
        configs[start..end].iter().collect()
    } else {
        vec![]
    };

    ok_json(json!({
        "configs": paged,
        "total": total,
    }))
    .into_response()
}

// ──── Handler: GET /v1/configs/{group}/{key} ────

/// 配置详情：读取当前版本
pub async fn get_config(
    State(state): State<Arc<InternalState>>,
    headers: HeaderMap,
    Path((group, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let _username = match validate_token(&state, &headers) {
        Ok(u) => u,
        Err(e) => return e.into_response(),
    };

    let node = &state.coord_node;
    let ck = config_data_key(&group, &key);

    let envelope: ConfigEnvelope = match node.storage.get(&ck) {
        Ok(Some(v)) => match serde_json::from_slice(&v) {
            Ok(e) => e,
            Err(_) => return err_json(500, "配置数据格式错误").into_response(),
        },
        Ok(None) => return err_json(404, "配置未找到").into_response(),
        Err(e) => return err_json(500, &format!("存储读取失败: {e}")).into_response(),
    };

    ok_json(json!({
        "group": envelope.group,
        "key": envelope.key,
        "version": envelope.version,
        "createdAt": envelope.created_at,
        "updatedAt": envelope.updated_at,
        "updatedBy": envelope.updated_by,
        "changeNote": envelope.change_note,
        "format": envelope.format,
        "data": envelope.data,
    }))
    .into_response()
}

// ──── Handler: POST /v1/configs ────

/// 创建配置
pub async fn create_config(
    State(state): State<Arc<InternalState>>,
    headers: HeaderMap,
    Json(body): Json<CreateConfigRequest>,
) -> impl IntoResponse {
    let username = match validate_token(&state, &headers) {
        Ok(u) => u,
        Err(e) => return e.into_response(),
    };

    // 参数校验
    if body.group.is_empty() || body.key.is_empty() {
        return err_json(400, "group 和 key 不能为空").into_response();
    }
    if !["yaml", "json", "toml", "text"].contains(&body.format.as_str()) {
        return err_json(400, "无效的格式，可选: yaml, json, toml, text").into_response();
    }
    if body.data.is_empty() {
        return err_json(400, "配置内容不能为空").into_response();
    }

    let node = &state.coord_node;
    let ck = config_data_key(&body.group, &body.key);

    // 检查是否已存在
    if matches!(node.storage.get(&ck), Ok(Some(_))) {
        return err_json(409, "配置已存在").into_response();
    }

    let now = current_time_iso();
    let envelope = ConfigEnvelope {
        group: body.group.clone(),
        key: body.key.clone(),
        version: 1,
        format: body.format,
        data: body.data,
        created_at: now.clone(),
        updated_at: now.clone(),
        updated_by: username,
        change_note: body.change_note.unwrap_or_default(),
    };

    let value = serde_json::to_vec(&envelope).unwrap_or_default();

    // 写入当前版本
    if let Err(e) = raft_put(node, ck.clone(), value.clone()).await {
        return e.into_response();
    }

    // 同时写入历史版本 v1
    let vk = config_version_key(&body.group, &body.key, 1);
    let _ = raft_put(node, vk, value).await;

    ok_json(json!({"version": 1}))
    .into_response()
}

// ──── Handler: PUT /v1/configs/{group}/{key} ────

/// 更新配置（CAS：乐观锁版本检查）
pub async fn update_config(
    State(state): State<Arc<InternalState>>,
    headers: HeaderMap,
    Path((group, key)): Path<(String, String)>,
    Json(body): Json<UpdateConfigRequest>,
) -> impl IntoResponse {
    let username = match validate_token(&state, &headers) {
        Ok(u) => u,
        Err(e) => return e.into_response(),
    };

    let node = &state.coord_node;
    let ck = config_data_key(&group, &key);

    // 读取当前版本
    let current: ConfigEnvelope = match node.storage.get(&ck) {
        Ok(Some(v)) => match serde_json::from_slice(&v) {
            Ok(e) => e,
            Err(_) => return err_json(500, "配置数据格式错误").into_response(),
        },
        Ok(None) => return err_json(404, "配置未找到").into_response(),
        Err(e) => return err_json(500, &format!("存储读取失败: {e}")).into_response(),
    };

    // CAS 检查
    if current.version != body.version {
        return err_json(409, &format!(
            "CAS 冲突：期望版本 v{}，当前版本 v{}，请刷新后重试",
            body.version, current.version
        )).into_response();
    }

    let now = current_time_iso();
    let new_version = current.version + 1;

    // 备份当前版本到历史
    let vk_old = config_version_key(&group, &key, current.version);
    let old_value = serde_json::to_vec(&current).unwrap_or_default();
    let _ = raft_put(node, vk_old, old_value).await;

    // 写入新版本
    let new_envelope = ConfigEnvelope {
        group: current.group,
        key: current.key,
        version: new_version,
        format: current.format,
        data: body.data,
        created_at: current.created_at,
        updated_at: now.clone(),
        updated_by: username,
        change_note: body.change_note.unwrap_or_default(),
    };

    let new_value = serde_json::to_vec(&new_envelope).unwrap_or_default();

    // 写入当前数据
    if let Err(e) = raft_put(node, ck.clone(), new_value.clone()).await {
        return e.into_response();
    }

    // 写入历史版本
    let vk_new = config_version_key(&group, &key, new_version);
    let _ = raft_put(node, vk_new, new_value).await;

    ok_json(json!({"version": new_version}))
    .into_response()
}

// ──── Handler: DELETE /v1/configs/{group}/{key} ────

/// 删除配置及所有历史版本
pub async fn delete_config(
    State(state): State<Arc<InternalState>>,
    headers: HeaderMap,
    Path((group, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let _username = match validate_token(&state, &headers) {
        Ok(u) => u,
        Err(e) => return e.into_response(),
    };

    let node = &state.coord_node;
    let ck = config_data_key(&group, &key);

    // 验证配置存在
    if !matches!(node.storage.get(&ck), Ok(Some(_))) {
        return err_json(404, "配置未找到").into_response();
    }

    // 删除当前数据
    if let Err(e) = raft_delete(node, ck).await {
        return e.into_response();
    }

    // 删除所有历史版本
    let vp = config_version_prefix(&group, &key);
    if let Ok(versions) = node.storage.range(&vp, usize::MAX) {
        for (vk, _) in versions {
            let _ = raft_delete(node, vk).await;
        }
    }

    ok_json(json!({}))
    .into_response()
}

// ──── Handler: GET /v1/configs/{group}/{key}/versions ────

/// 历史版本列表
pub async fn list_versions(
    State(state): State<Arc<InternalState>>,
    headers: HeaderMap,
    Path((group, key)): Path<(String, String)>,
) -> impl IntoResponse {
    let _username = match validate_token(&state, &headers) {
        Ok(u) => u,
        Err(e) => return e.into_response(),
    };

    let node = &state.coord_node;
    let vp = config_version_prefix(&group, &key);

    let results = match node.storage.range(&vp, usize::MAX) {
        Ok(r) => r,
        Err(e) => return err_json(500, &format!("存储读取失败: {e}")).into_response(),
    };

    let mut versions: Vec<VersionEntry> = Vec::new();
    for (_k, v) in &results {
        if let Ok(envelope) = serde_json::from_slice::<ConfigEnvelope>(v) {
            versions.push(VersionEntry {
                version: envelope.version,
                updated_at: envelope.updated_at,
                updated_by: envelope.updated_by,
                change_note: envelope.change_note,
            });
        }
    }

    // 按版本号降序排列（最新在前）
    versions.sort_by(|a, b| b.version.cmp(&a.version));

    ok_json(json!(versions))
    .into_response()
}

// ──── Handler: GET /v1/configs/{group}/{key}/versions/{version} ────

/// 指定版本详情
pub async fn get_version(
    State(state): State<Arc<InternalState>>,
    headers: HeaderMap,
    Path((group, key, version)): Path<(String, String, u64)>,
) -> impl IntoResponse {
    let _username = match validate_token(&state, &headers) {
        Ok(u) => u,
        Err(e) => return e.into_response(),
    };

    let node = &state.coord_node;
    let vk = config_version_key(&group, &key, version);

    let envelope: ConfigEnvelope = match node.storage.get(&vk) {
        Ok(Some(v)) => match serde_json::from_slice(&v) {
            Ok(e) => e,
            Err(_) => return err_json(500, "版本数据格式错误").into_response(),
        },
        Ok(None) => return err_json(404, "版本未找到").into_response(),
        Err(e) => return err_json(500, &format!("存储读取失败: {e}")).into_response(),
    };

    ok_json(json!({
        "group": envelope.group,
        "key": envelope.key,
        "version": envelope.version,
        "createdAt": envelope.created_at,
        "format": envelope.format,
        "data": envelope.data,
    }))
    .into_response()
}

// ──── Handler: POST /v1/configs/{group}/{key}/rollback ────

/// 回滚至指定版本（生成新版本，内容等于旧版本）
pub async fn rollback(
    State(state): State<Arc<InternalState>>,
    headers: HeaderMap,
    Path((group, key)): Path<(String, String)>,
    Json(body): Json<RollbackRequest>,
) -> impl IntoResponse {
    let username = match validate_token(&state, &headers) {
        Ok(u) => u,
        Err(e) => return e.into_response(),
    };

    let node = &state.coord_node;
    let ck = config_data_key(&group, &key);

    // 读取当前配置
    let current: ConfigEnvelope = match node.storage.get(&ck) {
        Ok(Some(v)) => match serde_json::from_slice(&v) {
            Ok(e) => e,
            Err(_) => return err_json(500, "配置数据格式错误").into_response(),
        },
        Ok(None) => return err_json(404, "配置未找到").into_response(),
        Err(e) => return err_json(500, &format!("存储读取失败: {e}")).into_response(),
    };

    // 读取目标版本
    let vk_old = config_version_key(&group, &key, body.version);
    let old_envelope: ConfigEnvelope = match node.storage.get(&vk_old) {
        Ok(Some(v)) => match serde_json::from_slice(&v) {
            Ok(e) => e,
            Err(_) => return err_json(500, "版本数据格式错误").into_response(),
        },
        Ok(None) => return err_json(404, "目标版本未找到").into_response(),
        Err(e) => return err_json(500, &format!("存储读取失败: {e}")).into_response(),
    };

    let now = current_time_iso();
    let new_version = current.version + 1;

    // 备份当前版本到历史
    let vk_current = config_version_key(&group, &key, current.version);
    let current_value = serde_json::to_vec(&current).unwrap_or_default();
    let _ = raft_put(node, vk_current, current_value).await;

    // 创建新版本（内容来自目标版本）
    let new_envelope = ConfigEnvelope {
        group: current.group,
        key: current.key,
        version: new_version,
        format: old_envelope.format,
        data: old_envelope.data,
        created_at: current.created_at,
        updated_at: now.clone(),
        updated_by: username,
        change_note: format!("回滚至 v{}", body.version),
    };

    let new_value = serde_json::to_vec(&new_envelope).unwrap_or_default();

    // 写入当前数据
    if let Err(e) = raft_put(node, ck.clone(), new_value.clone()).await {
        return e.into_response();
    }

    // 写入历史版本
    let vk_new = config_version_key(&group, &key, new_version);
    let _ = raft_put(node, vk_new, new_value).await;

    ok_json(json!({"newVersion": new_version}))
    .into_response()
}
