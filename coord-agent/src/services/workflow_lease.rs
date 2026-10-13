// coord-agent: KvInstanceLease —— 基于 coord Lease + KV Txn 的实例驱动租约
//
// 「一实例一驱动」：同一实例同一时刻只允许一个驱动者。
//
// coord Lease **没有 fencing token**（见 `docs/production/consumer-contract.md` §3），
// 因此不能只靠租约互斥。本实现用「**租约 key 的 mod_revision 作为 epoch**」+
// 写回时的**双 compare**（实例 mod_revision + 租约 key mod_revision）承担 fencing：
// 失去租约的驱动者在租约被撤销/重授后，epoch 不再匹配，其迟到写被 Txn 拒绝。
//
// 残余风险：若引擎在两次写之间已经产生了**外部副作用**（call 节点已发出请求），
// fencing 只能拒绝状态写回，无法撤回副作用 —— 这需要 StepToken 去重兜底
// （见 `spike/2026-10/spike-2-concurrency/STEPTOKEN.md`）。

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;

use coord_core::workflow::model::WorkflowInstance;
use coord_core::workflow::ports::{InstanceLease, LeaseGuard, StoreError};
use coord_proto::kv::PutRequest;
use coord_proto::txn::compare::{CompareResult, Target, TargetValue};
use coord_proto::txn::request_op::Op;
use coord_proto::txn::{Compare, RequestOp};

use crate::proxy::AgentInner;
use crate::services::workflow_store::KvWorkflowStore;

/// 实例驱动租约 key 前缀。与实例 key 同在 `/_workflow/v3/` 键空间下，
/// 由 `KvWorkflowStore` 的 Watch 消费（非 `instances/` 前缀，不会被误当作实例）。
const DRIVER_PREFIX: &str = "/_workflow/v3/drivers/";

/// 基于 coord Lease 的实例驱动租约。
///
/// 获取流程（`acquire`）：
/// 1. `LeaseGrant(ttl)` 得到一个空租约；
/// 2. `Txn(Compare(driver_key VERSION==0), Put(driver_key=token, lease_id))` —— 仅当
///    无人持有时创建；失败方撤销自己的空租约后返回 `None`（跳过驱动）；
/// 3. 线性一致读 driver_key 取 `mod_revision` 作为本租约的 fencing epoch。
///
/// 释放：`LeaseRevoke(lease_id)` —— 服务端级联删除 driver_key（删除事件投递给 Watch）。
pub struct KvInstanceLease {
    inner: Arc<AgentInner>,
    ttl_secs: i64,
}

impl KvInstanceLease {
    /// `ttl_secs` 为租约 TTL（秒）；续租间隔由 TTL 推导（`ttl/3`，下限 1s）。
    pub fn new(inner: Arc<AgentInner>, ttl_secs: i64) -> Self {
        Self {
            inner,
            ttl_secs: ttl_secs.max(2),
        }
    }

    /// 实例 `instance_id` 的驱动租约 key。
    pub fn driver_key(instance_id: &str) -> Vec<u8> {
        format!("{DRIVER_PREFIX}{instance_id}").into_bytes()
    }

    fn compare_version_zero(key: &[u8]) -> Compare {
        Compare {
            result: CompareResult::Equal as i32,
            target: Target::Version as i32,
            key: key.to_vec(),
            target_value: Some(TargetValue::Version(0)),
        }
    }
}

#[async_trait]
impl InstanceLease for KvInstanceLease {
    async fn acquire(&self, instance_id: &str) -> Result<Option<Arc<dyn LeaseGuard>>, StoreError> {
        let lease_id = self
            .inner
            .client
            .lease()
            .grant(self.ttl_secs)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        let key = Self::driver_key(instance_id);
        // 每次获取生成新 token：租约 ID 可能在 revoke 后被复用，token 不会。
        let token = format!(
            "{:016x}{:016x}",
            rand::random::<u64>(),
            rand::random::<u64>()
        );
        let put = RequestOp {
            op: Some(Op::RequestPut(PutRequest {
                key: key.clone(),
                value: token.clone().into_bytes(),
                lease_id,
                prev_kv: false,
                request_id: vec![],
            })),
        };

        let resp = self
            .inner
            .client
            .txn()
            .txn(vec![Self::compare_version_zero(&key)], vec![put], vec![])
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        if !resp.succeeded {
            // 已被其他驱动者持有：撤销自己刚授的空租约，不占用资源。
            let _ = self.inner.client.lease().revoke(lease_id).await;
            return Ok(None);
        }

        // 读取 driver_key 的 mod_revision 作为 fencing epoch（写入后线性一致读）。
        let epoch = self
            .inner
            .client
            .kv()
            .range_with_rev(&key, &key, 1, 0)
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?
            .into_iter()
            .next()
            .map(|(_k, _v, mod_rev)| mod_rev)
            .ok_or_else(|| {
                StoreError::IoError("driver key vanished immediately after acquire".into())
            })?;

        Ok(Some(Arc::new(KvLeaseGuard {
            inner: Arc::clone(&self.inner),
            lease_id,
            key,
            token,
            epoch,
            renew_interval: Duration::from_secs((self.ttl_secs as u64 / 3).max(1)),
        })))
    }
}

/// 租约句柄 —— 持有期间代表本驱动者拥有实例写权。
struct KvLeaseGuard {
    inner: Arc<AgentInner>,
    lease_id: i64,
    key: Vec<u8>,
    token: String,
    epoch: i64,
    renew_interval: Duration,
}

#[async_trait]
impl LeaseGuard for KvLeaseGuard {
    async fn renew(&self) -> Result<bool, StoreError> {
        // ttl=0 / 错误 => 租约已不存在（过期或 Revoke）=> 写权失效。
        match self.inner.client.lease().keep_alive(self.lease_id).await {
            Ok(ttl) => Ok(ttl > 0),
            Err(_) => Ok(false),
        }
    }

    async fn release(&self) {
        // 尽力而为：Revoke 失败由 TTL 过期兜底回收。
        let _ = self.inner.client.lease().revoke(self.lease_id).await;
    }

    fn epoch(&self) -> i64 {
        self.epoch
    }

    fn renew_interval(&self) -> Duration {
        self.renew_interval
    }

    async fn fenced_save(
        &self,
        inst: &WorkflowInstance,
        expected_mod_rev: i64,
    ) -> Result<bool, StoreError> {
        let inst_key = KvWorkflowStore::instance_key(&inst.id);
        let value =
            serde_json::to_vec(inst).map_err(|e| StoreError::SerializationError(e.to_string()))?;

        let compares = vec![
            // 1) 实例状态未被其他驱动者改动（乐观并发）。
            Compare {
                result: CompareResult::Equal as i32,
                target: Target::ModRev as i32,
                key: inst_key.clone(),
                target_value: Some(TargetValue::ModRevision(expected_mod_rev)),
            },
            // 2) 租约仍属本驱动者：driver_key 的 mod_revision == epoch
            //    （被撤销/重授后 mod_revision 变化，旧驱动者的写被拒绝）。
            Compare {
                result: CompareResult::Equal as i32,
                target: Target::ModRev as i32,
                key: self.key.clone(),
                target_value: Some(TargetValue::ModRevision(self.epoch)),
            },
            // 3) 且 driver_key 的值 == 本驱动 token（防跨租约复用）。
            Compare {
                result: CompareResult::Equal as i32,
                target: Target::Value as i32,
                key: self.key.clone(),
                target_value: Some(TargetValue::Value(self.token.clone().into_bytes())),
            },
        ];

        let put = RequestOp {
            op: Some(Op::RequestPut(PutRequest {
                key: inst_key,
                value,
                lease_id: 0,
                prev_kv: false,
                request_id: vec![],
            })),
        };

        let resp = self
            .inner
            .client
            .txn()
            .txn(compares, vec![put], vec![])
            .await
            .map_err(|e| StoreError::IoError(e.to_string()))?;

        Ok(resp.succeeded)
    }
}
