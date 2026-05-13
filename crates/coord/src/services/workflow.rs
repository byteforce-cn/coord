//! gRPC service impl: `workflow`.
//!
//! Split out from the monolithic `services.rs` in Batch 4a of the
//! April-2026 code review. Shared imports live in [`super`].
//!
//! ## 实现路径说明
//!
//! 本模块实现 CNCF Serverless Workflow DSL v2 引擎，详见
//! `doc/adr/adr-001-workflow-migration.md`。
//!
//! 对应接口：`deploy_workflow_definition`、`start_workflow_v2`、`resume_workflow`、
//! `get_workflow_instance`、`list_workflow_instances`、`list_workflow_definitions`、
//! `get_workflow_definition`。

use super::*;
use crate::application::workflow_app::{WorkflowApp, WorkflowAppError};
use crate::wire::error::coord_status;
use coord_core::error::CoordError;
use coord_core::workflow::engine::WorkflowInstance;

#[derive(Clone)]
pub struct WorkflowGrpc {
    workflow_app: WorkflowApp,
}

impl WorkflowGrpc {
    pub fn new(workflow_app: WorkflowApp) -> Self {
        Self { workflow_app }
    }
}

fn workflow_app_error_to_status(e: WorkflowAppError) -> Status {
    match &e {
        WorkflowAppError::Runtime(_) => coord_status(CoordError::NotFound {
            resource: "workflow",
            id: e.to_string(),
        }),
        _ => coord_status(CoordError::Internal(e.to_string())),
    }
}

#[tonic::async_trait]
impl WorkflowService for WorkflowGrpc {
    // ─── CNCF Serverless Workflow v2 RPCs ────────────────────────────────────

    async fn deploy_workflow_definition(
        &self,
        request: Request<DeployWorkflowDefinitionRequest>,
    ) -> Result<Response<DeployWorkflowDefinitionResponse>, Status> {
        let req = request.into_inner();
        let def = parse_yaml(&req.definition_yaml).map_err(|e| {
            coord_status(CoordError::InvalidArgument(format!(
                "invalid workflow YAML: {}",
                e
            )))
        })?;

        let ns = def.document.namespace.clone();
        let name = def.document.name.clone();
        let version = def.document.version.clone();
        let definition_id = if req.definition_id.is_empty() {
            name.clone()
        } else {
            req.definition_id.clone()
        };

        self.workflow_app
            .deploy(def)
            .await
            .map_err(workflow_app_error_to_status)?;

        Ok(Response::new(DeployWorkflowDefinitionResponse {
            namespace: ns,
            name,
            version,
            definition_id,
        }))
    }

    async fn start_workflow_v2(
        &self,
        request: Request<StartWorkflowV2Request>,
    ) -> Result<Response<StartWorkflowV2Response>, Status> {
        let req = request.into_inner();
        let input: serde_json::Value = if req.input_json.is_empty() {
            serde_json::json!({})
        } else {
            serde_json::from_str(&req.input_json).map_err(|e| {
                coord_status(CoordError::InvalidArgument(format!(
                    "invalid input JSON: {}",
                    e
                )))
            })?
        };

        // Resolve definition by id/name + optional version.
        let (ns, name, version) = if !req.definition_id.is_empty() {
            let defs = self
                .workflow_app
                .list_definitions(None)
                .await
                .map_err(workflow_app_error_to_status)?;
            let def = defs
                .into_iter()
                .find(|d| {
                    d.document.name == req.definition_id
                        && (req.version.is_empty() || d.document.version == req.version)
                })
                .ok_or_else(|| {
                    coord_status(CoordError::NotFound {
                        resource: "workflow_definition",
                        id: req.definition_id.clone(),
                    })
                })?;
            (
                def.document.namespace,
                def.document.name,
                def.document.version,
            )
        } else {
            (req.namespace, req.name, req.version)
        };

        let instance = self
            .workflow_app
            .start(&ns, &name, &version, input)
            .await
            .map_err(workflow_app_error_to_status)?;

        let status = instance_status_str(&instance.status);
        Ok(Response::new(StartWorkflowV2Response {
            instance_id: instance.id,
            status,
        }))
    }

    async fn resume_workflow(
        &self,
        request: Request<ResumeWorkflowRequest>,
    ) -> Result<Response<ResumeWorkflowResponse>, Status> {
        let req = request.into_inner();
        let result: serde_json::Value = if req.result_json.is_empty() {
            serde_json::json!({})
        } else {
            serde_json::from_str(&req.result_json).map_err(|e| {
                coord_status(CoordError::InvalidArgument(format!(
                    "invalid result JSON: {}",
                    e
                )))
            })?
        };

        let instance = self
            .workflow_app
            .resume(&req.instance_id, result)
            .await
            .map_err(workflow_app_error_to_status)?;

        Ok(Response::new(ResumeWorkflowResponse {
            instance_id: instance.id,
            status: instance_status_str(&instance.status),
        }))
    }

    async fn get_workflow_instance(
        &self,
        request: Request<GetWorkflowInstanceRequest>,
    ) -> Result<Response<GetWorkflowInstanceResponse>, Status> {
        let req = request.into_inner();
        let instance = self
            .workflow_app
            .get_instance(&req.instance_id)
            .await
            .map_err(workflow_app_error_to_status)?
            .ok_or_else(|| {
                coord_status(CoordError::NotFound {
                    resource: "workflow_instance",
                    id: req.instance_id.clone(),
                })
            })?;

        Ok(Response::new(GetWorkflowInstanceResponse {
            instance: Some(to_proto_instance_info(instance)),
        }))
    }

    async fn list_workflow_instances(
        &self,
        request: Request<ListWorkflowInstancesRequest>,
    ) -> Result<Response<ListWorkflowInstancesResponse>, Status> {
        let req = request.into_inner();
        let definition_name = if req.definition_name.is_empty() {
            req.definition_id
        } else {
            req.definition_name
        };
        let instances = self
            .workflow_app
            .list_instances(&req.namespace, &definition_name)
            .await
            .map_err(workflow_app_error_to_status)?
            .into_iter()
            .map(to_proto_instance_info)
            .collect();

        Ok(Response::new(ListWorkflowInstancesResponse { instances }))
    }

    async fn list_workflow_definitions(
        &self,
        request: Request<ListWorkflowDefinitionsRequest>,
    ) -> Result<Response<ListWorkflowDefinitionsResponse>, Status> {
        let req = request.into_inner();
        let ns_filter = if req.namespace.is_empty() {
            None
        } else {
            Some(req.namespace.as_str())
        };
        let defs = self
            .workflow_app
            .list_definitions(ns_filter)
            .await
            .map_err(workflow_app_error_to_status)?;

        let definitions = defs
            .into_iter()
            .map(|d| WorkflowDefinitionInfo {
                definition_id: d.document.name.clone(),
                namespace: d.document.namespace,
                name: d.document.name,
                version: d.document.version,
            })
            .collect();

        Ok(Response::new(ListWorkflowDefinitionsResponse {
            definitions,
        }))
    }

    async fn get_workflow_definition(
        &self,
        request: Request<GetWorkflowDefinitionRequest>,
    ) -> Result<Response<GetWorkflowDefinitionResponse>, Status> {
        let req = request.into_inner();
        let def = self
            .workflow_app
            .get_definition(&req.definition_id, &req.version)
            .await
            .map_err(workflow_app_error_to_status)?
            .ok_or_else(|| {
                coord_status(CoordError::NotFound {
                    resource: "workflow_definition",
                    id: req.definition_id.clone(),
                })
            })?;
        let yaml = coord_core::workflow::parser::to_yaml(&def)
            .map_err(|e| coord_status(CoordError::Internal(e)))?;
        Ok(Response::new(GetWorkflowDefinitionResponse {
            definition_yaml: yaml,
        }))
    }
}

// ─── WorkflowGrpc v2 helper functions ────────────────────────────────────────

fn instance_status_str(status: &InstanceStatus) -> String {
    match status {
        InstanceStatus::Running => "RUNNING",
        InstanceStatus::Suspended => "SUSPENDED",
        InstanceStatus::Completed => "COMPLETED",
        InstanceStatus::Failed => "FAILED",
        InstanceStatus::Cancelled => "CANCELLED",
    }
    .to_string()
}

fn to_proto_instance_info(
    i: coord_core::workflow::engine::WorkflowInstance,
) -> WorkflowInstanceInfo {
    WorkflowInstanceInfo {
        instance_id: i.id,
        definition_ns: i.definition_ns,
        definition_name: i.definition_name,
        definition_version: i.definition_version,
        status: instance_status_str(&i.status),
        context_json: serde_json::to_string(&i.context).unwrap_or_default(),
        output_json: i
            .output
            .as_ref()
            .and_then(|v| serde_json::to_string(v).ok())
            .unwrap_or_default(),
        fault_json: i
            .fault
            .as_ref()
            .and_then(|f| serde_json::to_string(f).ok())
            .unwrap_or_default(),
        created_at_ms: i.created_at,
        updated_at_ms: i.updated_at,
    }
}
