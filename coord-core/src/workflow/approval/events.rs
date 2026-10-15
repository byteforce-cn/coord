//! v2 审批事件目录与出站记录构造（已固化契约）。
//!
//! 事件类型带 `.v1` 版本后缀：这些名字一旦流出即对外契约，只能新增 `.v2` 而不能原地
//! 改名。每个类型在 `apis/contracts/schemas/events/<type>.json` 有对应 JSON Schema，
//! 由 `scripts/check-event-schemas.sh` 门禁。事件写入沿用 v3 outbox 传输设施，故记录
//! 类型复用 [`OutboxRecord`]。

use serde_json::Value;

use crate::workflow::durable::OutboxRecord;

use super::model::ApprovalError;

/// 已移植的 v2 审批引擎会发出的 7 个事件类型（逐字取自引擎 `emit` 调用点）。
///
/// 目录必须与引擎 emit 点一致：多一个就是无人产生的死契约面，少一个会在
/// [`event_record`] 处被 fail-closed 拒绝。每个类型都带 `.v1` 版本后缀（见
/// `catalog_matches_v1_suffix_convention`）。
pub const APPROVAL_EVENT_TYPES: &[&str] = &[
    "com.china-workflow.instance.returned.v1",
    "com.china-workflow.instance.cancelled.v1",
    "com.china-workflow.instance.terminated.v1",
    "com.china-workflow.instance.completed.v1",
    "com.china-workflow.instance.rejected.v1",
    "com.china-workflow.approval.round.passed.v1",
    "com.china-workflow.approval.round.rejected.v1",
];

/// 构造一条 v2 审批生命周期事件记录。
///
/// `name` 必须逐字落在 [`APPROVAL_EVENT_TYPES`] 内，否则返回
/// [`ApprovalError::InvalidArgument`]（fail-closed）：错拼的事件类型一旦写进 outbox
/// 就会流出成事实契约，无法回撤。`sequence == 0` 表示由存储侧在同一提交里分配。
pub fn event_record(
    aggregate_id: &str,
    name: &str,
    data: &Value,
    now_ms: i64,
    id: String,
) -> Result<OutboxRecord, ApprovalError> {
    if !APPROVAL_EVENT_TYPES.contains(&name) {
        return Err(ApprovalError::InvalidArgument(name.to_string()));
    }
    Ok(OutboxRecord::lifecycle(
        aggregate_id,
        name,
        data.clone(),
        now_ms,
        id,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::workflow::approval::engine::{Engine, StartSpec};
    use crate::workflow::approval::model::{
        ApprovalError, Effect, JoinPolicy, RejectPolicy, Scope, SeatSpec,
    };
    use crate::workflow::durable::OutboxStatus;
    use serde_json::json;
    use std::collections::BTreeSet;

    const INSTANCE_ID: &str = "inst-t4";

    fn start() -> Engine {
        Engine::start(
            StartSpec {
                id: INSTANCE_ID.into(),
                definition_ns: "eis".into(),
                definition_name: "leave-approval".into(),
                definition_version: "1.0.0".into(),
                definition_hash: "sha256:t4".into(),
                initiator: "zhangsan".into(),
                context: Scope::new(),
            },
            1_000,
        )
        .expect("start instance")
    }

    fn tick(t: &mut i64) -> i64 {
        *t += 1;
        *t
    }

    fn push_names(out: &mut Vec<String>, effects: &[Effect]) {
        for effect in effects {
            if let Effect::Event { name, .. } = effect {
                out.push(name.clone());
            }
        }
    }

    #[test]
    fn event_record_rejects_unknown_type() {
        let err = event_record(
            INSTANCE_ID,
            "com.china-workflow.instance.made_up",
            &json!({}),
            1_000,
            "evt-unknown".into(),
        )
        .unwrap_err();
        assert_eq!(
            err,
            ApprovalError::InvalidArgument("com.china-workflow.instance.made_up".into())
        );
        assert_eq!(
            err.to_string(),
            "invalid argument: com.china-workflow.instance.made_up"
        );
    }

    #[test]
    fn event_record_builds_pending_lifecycle_record() {
        let rec = event_record(
            INSTANCE_ID,
            "com.china-workflow.instance.completed.v1",
            &json!({ "instance_id": INSTANCE_ID }),
            1_000,
            "evt-1".into(),
        )
        .expect("known event type");
        assert_eq!(rec.id, "evt-1");
        assert_eq!(rec.aggregate_id, INSTANCE_ID);
        assert_eq!(rec.event_type, "com.china-workflow.instance.completed.v1");
        assert_eq!(rec.sequence, 0);
        assert_eq!(rec.status, OutboxStatus::Pending);
        assert_eq!(
            rec.dataschema,
            "https://schemas.coord.dev/events/com.china-workflow.instance.completed.v1.json"
        );
    }

    #[test]
    fn catalog_matches_v1_suffix_convention() {
        for name in APPROVAL_EVENT_TYPES {
            assert!(
                name.starts_with("com.china-workflow."),
                "event type outside namespace: {name}"
            );
            assert!(
                name.ends_with(".v1"),
                "event type missing v1 suffix: {name}"
            );
        }
    }

    #[test]
    fn all_emitted_event_names_are_in_catalog() {
        assert_eq!(APPROVAL_EVENT_TYPES.len(), 7);
        let mut emitted: Vec<String> = Vec::new();

        // 通过路径：轮次通过 + 实例收口完成。
        {
            let mut e = start();
            let mut t = 1_000;
            let (b, _) = e
                .enter_branch(
                    e.instance.revision,
                    "apply",
                    None,
                    JoinPolicy::All,
                    RejectPolicy::AnyReject,
                    tick(&mut t),
                )
                .unwrap();
            let (_, ts, out) = e
                .open_approval(
                    e.instance.revision,
                    &b,
                    vec![SeatSpec::approver("alice")],
                    None,
                    tick(&mut t),
                )
                .unwrap();
            push_names(&mut emitted, &out);
            let out = e
                .approve(e.instance.revision, &ts[0], "alice", None, tick(&mut t))
                .unwrap();
            push_names(&mut emitted, &out);
            let out = e.finish(e.instance.revision, tick(&mut t)).unwrap();
            push_names(&mut emitted, &out);
        }

        // 否决路径：轮次否决 + 根分支否决收口实例。
        {
            let mut e = start();
            let mut t = 1_000;
            let (b, _) = e
                .enter_branch(
                    e.instance.revision,
                    "apply",
                    None,
                    JoinPolicy::All,
                    RejectPolicy::AnyReject,
                    tick(&mut t),
                )
                .unwrap();
            let (_, ts, _) = e
                .open_approval(
                    e.instance.revision,
                    &b,
                    vec![SeatSpec::approver("alice")],
                    None,
                    tick(&mut t),
                )
                .unwrap();
            let out = e
                .reject(e.instance.revision, &ts[0], "alice", None, tick(&mut t))
                .unwrap();
            push_names(&mut emitted, &out);
        }

        // 驳回至节点路径。
        {
            let mut e = start();
            let mut t = 1_000;
            let (b1, _) = e
                .enter_branch(
                    e.instance.revision,
                    "apply",
                    None,
                    JoinPolicy::All,
                    RejectPolicy::AnyReject,
                    tick(&mut t),
                )
                .unwrap();
            let (_, ts, _) = e
                .open_approval(
                    e.instance.revision,
                    &b1,
                    vec![SeatSpec::approver("alice")],
                    None,
                    tick(&mut t),
                )
                .unwrap();
            e.approve(e.instance.revision, &ts[0], "alice", None, tick(&mut t))
                .unwrap();
            let (b2, _) = e
                .enter_branch(
                    e.instance.revision,
                    "review",
                    None,
                    JoinPolicy::All,
                    RejectPolicy::AnyReject,
                    tick(&mut t),
                )
                .unwrap();
            let (_, out) = e
                .return_to_node(e.instance.revision, &b2, "apply", "bob", None, tick(&mut t))
                .unwrap();
            push_names(&mut emitted, &out);
        }

        // 撤回路径。
        {
            let mut e = start();
            let mut t = 1_000;
            e.enter_branch(
                e.instance.revision,
                "apply",
                None,
                JoinPolicy::All,
                RejectPolicy::AnyReject,
                tick(&mut t),
            )
            .unwrap();
            let out = e
                .withdraw(e.instance.revision, "zhangsan", tick(&mut t))
                .unwrap();
            push_names(&mut emitted, &out);
        }

        // 终止路径。
        {
            let mut e = start();
            let mut t = 1_000;
            e.enter_branch(
                e.instance.revision,
                "apply",
                None,
                JoinPolicy::All,
                RejectPolicy::AnyReject,
                tick(&mut t),
            )
            .unwrap();
            let out = e
                .terminate(e.instance.revision, "admin", "audit", tick(&mut t))
                .unwrap();
            push_names(&mut emitted, &out);
        }

        let catalog: BTreeSet<&str> = APPROVAL_EVENT_TYPES.iter().copied().collect();
        for name in &emitted {
            assert!(
                catalog.contains(name.as_str()),
                "emitted event not in catalog: {name}"
            );
        }
        let emitted_set: BTreeSet<&str> = emitted.iter().map(String::as_str).collect();
        assert_eq!(
            emitted_set, catalog,
            "catalog contains names no scenario can emit (dead contract surface)"
        );
    }
}
