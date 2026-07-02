// BFF Internal Routes — Registry API
//
// REST 端点用于服务注册与发现：
// - GET  /v1/registry/services                      → 服务列表（支持搜索、状态过滤、分页）
// - GET  /v1/registry/services/{name}                → 服务详情（含实例列表）
// - PUT  /v1/registry/services/{name}/instances/{id}  → 修改实例状态
// - POST /v1/registry/services/{name}/health-check    → 强制触发健康检查
//
// 数据存储于 KV store 中，key 前缀约定：
//   /coord/registry/services/{name}                  → JSON 服务元数据
//   /coord/registry/services/{name}/instances/{id}   → JSON 实例数据

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
pub struct ListServicesQuery {
    pub q: Option<String>,
    pub status: Option<String>,
    pub page: Option<u64>,
    #[serde(rename = "pageSize")]
    pub page_size: Option<u64>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateInstanceRequest {
    pub status: String,
}

#[derive(Debug, Serialize, Clone)]
pub struct ServiceInfo {
    pub name: String,
    pub tags: Vec<String>,
    pub status: String,
    pub instances: InstanceStats,
    pub address: String,
    pub port: u32,
}

#[derive(Debug, Serialize, Clone)]
pub struct InstanceStats {
    pub total: usize,
    pub healthy: usize,
    pub warning: usize,
    pub critical: usize,
}

#[derive(Debug, Serialize, Clone)]
pub struct InstanceInfo {
    pub id: String,
    pub address: String,
    pub port: u32,
    pub status: String,
    #[serde(rename = "lastCheck")]
    pub last_check: String,
}

#[derive(Debug, Deserialize)]
struct ServiceData {
    name: String,
    tags: Vec<String>,
    address: String,
    port: u32,
}

#[derive(Debug, Deserialize, Serialize)]
struct InstanceData {
    id: String,
    address: String,
    port: u32,
    status: String,
    #[serde(rename = "lastCheck", default)]
    last_check: String,
}

// ──── KV Key 构造 ────

const REGISTRY_PREFIX: &str = "/coord/registry/services/";

fn service_key(name: &str) -> Vec<u8> {
    format!("{}{}", REGISTRY_PREFIX, name).into_bytes()
}

fn instance_key(service_name: &str, instance_id: &str) -> Vec<u8> {
    format!("{}{}/instances/{}", REGISTRY_PREFIX, service_name, instance_id).into_bytes()
}

fn instance_prefix(service_name: &str) -> Vec<u8> {
    format!("{}{}/instances/", REGISTRY_PREFIX, service_name).into_bytes()
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

/// 验证 Token，返回用户名；验证失败返回错误响应
fn validate_token(state: &InternalState, headers: &HeaderMap) -> Result<String, (StatusCode, Json<Value>)> {
    let token = crate::bff::internal::extract_bearer_token(headers)
        .ok_or_else(|| err_json(401, "缺少认证 Token"))?;
    state.token_manager.validate(&token)
        .map_err(|_| err_json(403, "Token 无效或已过期"))
}

/// 通过 Raft 写入 KV，单节点模式回退到直接存储写入
async fn raft_put(
    node: &CoordNode,
    key: Vec<u8>,
    value: Vec<u8>,
) -> Result<(), (StatusCode, Json<Value>)> {
    if let Some(ref raft) = node.raft {
        let cmd = Command::Put {
            key,
            value,
            lease_id: None,
        };
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

// ──── Handler: GET /v1/registry/services ────

/// 服务列表：前缀扫描 /coord/registry/services/，支持搜索和状态过滤
pub async fn list_services(
    State(state): State<Arc<InternalState>>,
    headers: HeaderMap,
    Query(query): Query<ListServicesQuery>,
) -> impl IntoResponse {
    // 验证 Token
    let _username = match validate_token(&state, &headers) {
        Ok(u) => u,
        Err(e) => return e.into_response(),
    };

    let node = &state.coord_node;

    // 扫描所有服务
    let prefix = REGISTRY_PREFIX.as_bytes().to_vec();
    let results = match node.storage.range(&prefix, usize::MAX) {
        Ok(r) => r,
        Err(e) => return err_json(500, &format!("存储读取失败: {e}")).into_response(),
    };

    let mut services: Vec<ServiceInfo> = Vec::new();

    for (k, v) in &results {
        let key_str = String::from_utf8_lossy(k);

        // 跳过实例记录（只取服务级别的 key）
        if key_str.contains("/instances/") {
            continue;
        }

        let service: ServiceData = match serde_json::from_slice(v) {
            Ok(s) => s,
            Err(_) => continue,
        };

        // 搜索过滤（按服务名匹配）
        if let Some(ref q) = query.q {
            if !service.name.to_lowercase().contains(&q.to_lowercase()) {
                continue;
            }
        }

        // 收集该服务的实例并统计状态
        let inst_prefix = instance_prefix(&service.name);
        let instances_data = node.storage.range(&inst_prefix, usize::MAX).unwrap_or_default();

        let mut total = 0usize;
        let mut healthy = 0usize;
        let mut warning = 0usize;
        let mut critical = 0usize;

        for (_, iv) in &instances_data {
            if let Ok(inst) = serde_json::from_slice::<InstanceData>(iv) {
                total += 1;
                match inst.status.as_str() {
                    "passing" => healthy += 1,
                    "warning" => warning += 1,
                    "critical" => critical += 1,
                    _ => {}
                }
            }
        }

        // 计算服务整体状态
        let service_status = if total == 0 {
            "unknown"
        } else if critical > 0 {
            "critical"
        } else if warning > 0 {
            "warning"
        } else {
            "passing"
        };

        // 状态过滤
        if let Some(ref status_filter) = query.status {
            if status_filter != "all" && service_status != status_filter {
                continue;
            }
        }

        services.push(ServiceInfo {
            name: service.name,
            tags: service.tags,
            status: service_status.to_string(),
            instances: InstanceStats { total, healthy, warning, critical },
            address: service.address,
            port: service.port,
        });
    }

    // 排序：按名称
    services.sort_by(|a, b| a.name.cmp(&b.name));

    let total = services.len() as u64;

    // 分页
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(20).min(100).max(10);
    let start = ((page - 1) * page_size) as usize;
    let end = (start + page_size as usize).min(services.len());

    let paged: Vec<&ServiceInfo> = if start < services.len() {
        services[start..end].iter().collect()
    } else {
        vec![]
    };

    ok_json(json!({
        "services": paged,
        "total": total,
    }))
    .into_response()
}

// ──── Handler: GET /v1/registry/services/{name} ────

/// 服务详情：读取服务元数据 + 所有实例
pub async fn get_service(
    State(state): State<Arc<InternalState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let _username = match validate_token(&state, &headers) {
        Ok(u) => u,
        Err(e) => return e.into_response(),
    };

    let node = &state.coord_node;

    // 读取服务元数据
    let sk = service_key(&name);
    let service_data = match node.storage.get(&sk) {
        Ok(Some(v)) => match serde_json::from_slice::<ServiceData>(&v) {
            Ok(s) => s,
            Err(_) => return err_json(500, "服务数据格式错误").into_response(),
        },
        Ok(None) => return err_json(404, "服务未找到").into_response(),
        Err(e) => return err_json(500, &format!("存储读取失败: {e}")).into_response(),
    };

    // 扫描实例
    let inst_prefix = instance_prefix(&name);
    let instances_data = node.storage.range(&inst_prefix, usize::MAX)
        .unwrap_or_default();

    let mut instances: Vec<InstanceInfo> = Vec::new();
    let mut healthy_count = 0usize;

    for (_, iv) in &instances_data {
        if let Ok(inst) = serde_json::from_slice::<InstanceData>(iv) {
            if inst.status == "passing" {
                healthy_count += 1;
            }
            instances.push(InstanceInfo {
                id: inst.id,
                address: inst.address,
                port: inst.port,
                status: inst.status,
                last_check: inst.last_check,
            });
        }
    }

    let health_rate = if instances.is_empty() {
        100.0
    } else {
        (healthy_count as f64 / instances.len() as f64 * 100.0).round()
    };

    ok_json(json!({
        "name": service_data.name,
        "tags": service_data.tags,
        "healthRate": health_rate,
        "instances": instances,
    }))
    .into_response()
}

// ──── Handler: PUT /v1/registry/services/{name}/instances/{id} ────

/// 修改实例状态（上线/下线）
pub async fn update_instance(
    State(state): State<Arc<InternalState>>,
    headers: HeaderMap,
    Path((name, id)): Path<(String, String)>,
    Json(body): Json<UpdateInstanceRequest>,
) -> impl IntoResponse {
    let _username = match validate_token(&state, &headers) {
        Ok(u) => u,
        Err(e) => return e.into_response(),
    };

    let node = &state.coord_node;

    // 验证状态值
    if !["passing", "warning", "critical"].contains(&body.status.as_str()) {
        return err_json(400, "无效的状态值，可选: passing, warning, critical").into_response();
    }

    // 读取现有实例
    let ik = instance_key(&name, &id);
    let existing = match node.storage.get(&ik) {
        Ok(Some(v)) => v,
        Ok(None) => return err_json(404, "实例未找到").into_response(),
        Err(e) => return err_json(500, &format!("存储读取失败: {e}")).into_response(),
    };

    let mut inst: InstanceData = match serde_json::from_slice(&existing) {
        Ok(i) => i,
        Err(_) => return err_json(500, "实例数据格式错误").into_response(),
    };

    // 更新状态和时间
    inst.status = body.status;
    inst.last_check = chrono_now();

    let new_value = serde_json::to_vec(&inst)
        .unwrap_or_default();

    if let Err(e) = raft_put(node, ik, new_value).await {
        return e.into_response();
    }

    ok_json(json!({}))
    .into_response()
}

// ──── Handler: POST /v1/registry/services/{name}/health-check ────

/// 强制触发健康检查
pub async fn health_check(
    State(state): State<Arc<InternalState>>,
    headers: HeaderMap,
    Path(name): Path<String>,
) -> impl IntoResponse {
    let _username = match validate_token(&state, &headers) {
        Ok(u) => u,
        Err(e) => return e.into_response(),
    };

    let node = &state.coord_node;

    // 验证服务存在
    let sk = service_key(&name);
    if !matches!(node.storage.get(&sk), Ok(Some(_))) {
        return err_json(404, "服务未找到").into_response();
    }

    // 扫描所有实例
    let inst_prefix = instance_prefix(&name);
    let instances_data = match node.storage.range(&inst_prefix, usize::MAX) {
        Ok(d) => d,
        Err(e) => return err_json(500, &format!("存储读取失败: {e}")).into_response(),
    };

    let now = chrono_now();

    // 更新每个实例的 lastCheck（简单健康检查：标记为当前时间）
    for (ik, iv) in &instances_data {
        if let Ok(mut inst) = serde_json::from_slice::<InstanceData>(iv) {
            inst.last_check = now.clone();
            let new_value = serde_json::to_vec(&inst).unwrap_or_default();
            let _ = raft_put(node, ik.clone(), new_value).await;
        }
    }

    ok_json(json!({"checked": instances_data.len()}))
    .into_response()
}

// ──── 辅助: 当前时间 ISO 字符串 ────

fn chrono_now() -> String {
    // 使用标准库格式化 UTC 时间（不依赖 chrono crate）
    use std::time::SystemTime;
    let dur = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default();
    let secs = dur.as_secs();
    // 简单 ISO 8601 格式
    let days_since_epoch = secs / 86400;
    let secs_in_day = secs % 86400;
    let hours = secs_in_day / 3600;
    let minutes = (secs_in_day % 3600) / 60;
    let secs_remainder = secs_in_day % 60;

    // 计算年月日（简化：从 Unix epoch 1970-01-01 开始）
    let mut y = 1970i64;
    let mut remaining_days = days_since_epoch as i64;
    loop {
        let days_in_year = if is_leap(y) { 366 } else { 365 };
        if remaining_days < days_in_year {
            break;
        }
        remaining_days -= days_in_year;
        y += 1;
    }
    let month_days = if is_leap(y) {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };
    let mut m = 1u32;
    for &md in &month_days {
        if remaining_days < md as i64 {
            break;
        }
        remaining_days -= md as i64;
        m += 1;
    }
    let d = remaining_days as u32 + 1;

    format!(
        "{:04}-{:02}-{:02}T{:02}:{:02}:{:02}Z",
        y, m, d, hours, minutes, secs_remainder
    )
}

fn is_leap(y: i64) -> bool {
    (y % 4 == 0 && y % 100 != 0) || (y % 400 == 0)
}
