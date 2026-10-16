// coord-agent: KvStepStore —— 基于 KV Txn 的 step token 存储
//
// 实现 `coord-core` 的 `StepTokenStore`：`call` 派发前登记确定性 token（幂等键），
// 成功后标 `Done`；重试/崩溃重建复用同一 token，下游据此去重即得「不重」。
//
// 键布局（`/_workflow/v3/` 与 `KvWorkflowStore` 同一前缀族）：
//
// ```text
// /_workflow/v3/steps/{instance_id}/{step_id}   → StepRecord JSON
// ```
//
// `register` = `Txn(Compare(key VERSION==0) → Put)`：create-if-absent。失败方**必须
// 回读**既有记录（并发/重放的赢家已写入），读到才返回 `(record, false)`；读不到说明
// CAS 失败后键又消失（存在删除者）—— fail-closed 报错，绝不自造 token。

use std::sync::Arc;

use async_trait::async_trait;

use coord_core::workflow::durable::{StepRecord, StepState, StepTokenStore};
use coord_core::workflow::ports::StoreError;
use coord_proto::kv::PutRequest;
use coord_proto::txn::compare::{CompareResult, Target, TargetValue};
use coord_proto::txn::request_op::Op;
use coord_proto::txn::{Compare, RequestOp};

use crate::proxy::AgentInner;

/// KV 持久化 step token 存储。
pub struct KvStepStore {
    inner: Arc<AgentInner>,
}

impl KvStepStore {
    pub fn new(inner: Arc<AgentInner>) -> Self {
        Self { inner }
    }

    /// step token 键：`/_workflow/v3/steps/{instance_id}/{step_id}`。
    fn step_key(instance_id: &str, step_id: &str) -> Vec<u8> {
        format!("/_workflow/v3/steps/{instance_id}/{step_id}").into_bytes()
    }

    async fn get_record(
        &self,
        instance_id: &str,
        step_id: &str,
    ) -> Result<Option<StepRecord>, StoreError> {
        let key = Self::step_key(instance_id, step_id);
        let pairs = self
            .inner
            .client
            .kv()
            .range(&key, &[], 0, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        match pairs.into_iter().next() {
            None => Ok(None),
            Some((_, value)) => serde_json::from_slice(&value)
                .map(Some)
                .map_err(|e| StoreError::SerializationError(e.to_string())),
        }
    }
}

#[async_trait]
impl StepTokenStore for KvStepStore {
    async fn register(
        &self,
        instance_id: &str,
        step_id: &str,
        token: &str,
        now_ms: i64,
    ) -> Result<(StepRecord, bool), StoreError> {
        let key = Self::step_key(instance_id, step_id);
        let rec = StepRecord {
            token: token.to_string(),
            state: StepState::Pending,
            at_ms: now_ms,
        };
        let raw =
            serde_json::to_vec(&rec).map_err(|e| StoreError::SerializationError(e.to_string()))?;

        let compare = Compare {
            result: CompareResult::Equal as i32,
            target: Target::Version as i32,
            key: key.clone(),
            target_value: Some(TargetValue::Version(0)),
        };
        let put = RequestOp {
            op: Some(Op::RequestPut(PutRequest {
                key,
                value: raw,
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })),
        };

        let resp = self
            .inner
            .client
            .txn()
            .txn(vec![compare], vec![put], vec![])
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        if resp.succeeded {
            return Ok((rec, true));
        }
        // CAS 失败 = 键已存在（并发/重放赢家）。回读既有记录，token 以既有值为准。
        match self.get_record(instance_id, step_id).await? {
            Some(existing) => Ok((existing, false)),
            None => Err(StoreError::IoError(
                "step key vanished after CAS failure".to_string(),
            )),
        }
    }

    async fn mark_done(&self, instance_id: &str, step_id: &str) -> Result<(), StoreError> {
        let Some(mut rec) = self.get_record(instance_id, step_id).await? else {
            return Ok(());
        };
        rec.state = StepState::Done;
        let key = Self::step_key(instance_id, step_id);
        let raw =
            serde_json::to_vec(&rec).map_err(|e| StoreError::SerializationError(e.to_string()))?;
        self.inner
            .client
            .kv()
            .put(&key, &raw)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;
        Ok(())
    }

    async fn get(
        &self,
        instance_id: &str,
        step_id: &str,
    ) -> Result<Option<StepRecord>, StoreError> {
        self.get_record(instance_id, step_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn step_key_layout_is_namespaced_by_instance_and_step() {
        let key = KvStepStore::step_key("inst-1", "0:callApi");
        assert_eq!(
            String::from_utf8(key).unwrap(),
            "/_workflow/v3/steps/inst-1/0:callApi"
        );
    }
}
