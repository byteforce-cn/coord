// 第三轮复核 P0-2 回归测试：Leader 选举的**互斥**
//
// 背景：`campaign()` 原实现用无条件 `put_lease` 写选举 key。服务端 `Put` 是纯
// upsert（无存在性前置校验），于是「键已存在 → 作为 Follower」的分支是**死代码**：
// 两个候选者各自 grant 租约、各自 Put（后者覆盖前者）→ **同时自认 Leader**，
// 且各自续租都成功 ⇒ C3 的退位机制永不触发 ⇒ **永久双主且不自愈**。
// 该服务此前 11 个单元测试**无一调用过 `campaign()`**，功能从未被执行。
//
// 本套件用**真实** gRPC server（进程内单节点 raft + 真实状态机）+ 两个独立客户端
// 直接调用 `LeaderElectionService::campaign`，把 §7.3 的一票否决项
// 「任一时刻不得超过一个节点自认 leader」变成可执行断言。
//
// 诚实边界：这是 L2（进程内真实 server/client，真实状态机与网络栈），
// 不是 L3 多进程 + 网络分区注入。分区/时钟回拨下的选举行为仍属**未覆盖**，
// 见整改报告"残余"一节。

mod common;

use std::sync::Arc;
use std::time::Duration;

use coord_agent::cache::AgentCache;
use coord_agent::services::leader_election::{LeaderElectionService, LeaderRole};
use coord_agent::AgentInner;

const GROUP: &str = "round3-election-group";

async fn inner_for(addr: &str) -> Arc<AgentInner> {
    Arc::new(
        AgentInner::new(
            vec![addr.to_string()],
            AgentCache::new(128, 0, 128, 0),
            None,
        )
        .await
        .expect("connect agent inner to test server"),
    )
}

/// 8 个候选者**并发**竞选同一 group：必须恰好 1 个 Leader。
///
/// 修复前：全部 8 个都会返回 `Leader`（无条件 upsert，后者覆盖前者）。
#[tokio::test(flavor = "multi_thread")]
async fn concurrent_campaign_has_exactly_one_winner() {
    let (addr, _sd, _g, _r, _tmp) = common::start_test_server().await;

    let mut services = Vec::new();
    for i in 0..8 {
        let inner = inner_for(&addr).await;
        services.push((
            format!("cand-{i}"),
            Arc::new(LeaderElectionService::new(inner, 16)),
        ));
    }

    let mut handles = Vec::new();
    for (name, svc) in &services {
        let name = name.clone();
        let svc = Arc::clone(svc);
        handles.push(tokio::spawn(async move {
            svc.campaign(GROUP, &name, 30).await.map(|r| (name, r))
        }));
    }

    let mut leaders = Vec::new();
    let mut followers = Vec::new();
    for h in handles {
        let (name, role) = h
            .await
            .expect("campaign task panicked")
            .expect("campaign failed");
        match role {
            LeaderRole::Leader => leaders.push(name),
            LeaderRole::Follower => followers.push(name),
            other => panic!("unexpected role {other:?}"),
        }
    }

    assert_eq!(
        leaders.len(),
        1,
        "竞选必须互斥：期望恰好 1 个 Leader，实际 {leaders:?}（Follower: {followers:?}）"
    );
    assert_eq!(followers.len(), 7);

    // 本地自认与服务端记录必须一致：选举 key 上的 leader_id == 唯一赢家
    let winner = &leaders[0];
    let winner_svc = services
        .iter()
        .find(|(n, _)| n == winner)
        .map(|(_, s)| s)
        .unwrap();
    let info = winner_svc
        .get_group_info(GROUP)
        .expect("winner must record group info");
    assert_eq!(&info.leader_id, winner);

    let recorded = read_election_key(&addr, GROUP).await;
    assert_eq!(
        recorded.as_deref(),
        Some(winner.as_str()),
        "服务端选举 key 必须指向唯一赢家（否则本地/服务端状态分裂）"
    );

    // 其它候选者本地不得自认 Leader
    for (name, svc) in &services {
        if name != winner {
            assert!(!svc.is_leader(GROUP), "{name} 不得自认 Leader");
            assert_eq!(svc.get_role(GROUP), Some(LeaderRole::Follower));
        }
    }
}

/// 在任 Leader 的租约仍有效期间，其它候选者反复竞选一律只能成为 Follower。
#[tokio::test(flavor = "multi_thread")]
async fn follower_cannot_usurp_while_leader_lease_alive() {
    let (addr, _sd, _g, _r, _tmp) = common::start_test_server().await;

    let leader_svc = LeaderElectionService::new(inner_for(&addr).await, 16);
    let other_svc = LeaderElectionService::new(inner_for(&addr).await, 16);

    assert_eq!(
        leader_svc.campaign(GROUP, "leader", 60).await.unwrap(),
        LeaderRole::Leader
    );

    for attempt in 0..5 {
        assert_eq!(
            other_svc.campaign(GROUP, "usurper", 60).await.unwrap(),
            LeaderRole::Follower,
            "第 {attempt} 次抢占必须失败（Leader 租约仍有效）"
        );
        assert!(!other_svc.is_leader(GROUP));
    }
    assert!(leader_svc.is_leader(GROUP), "在任 Leader 不得被误降级");
    assert_eq!(
        read_election_key(&addr, GROUP).await.as_deref(),
        Some("leader")
    );
}

/// 在任 Leader 重复 campaign（后台自动重选路径）必须**保持** Leader——
/// 否则本地自认 Follower 而服务端 key 仍指向自己，会同时阻塞其它节点当选。
#[tokio::test(flavor = "multi_thread")]
async fn recampaign_by_current_leader_stays_leader() {
    let (addr, _sd, _g, _r, _tmp) = common::start_test_server().await;

    let svc = LeaderElectionService::new(inner_for(&addr).await, 16);
    assert_eq!(
        svc.campaign(GROUP, "me", 60).await.unwrap(),
        LeaderRole::Leader
    );
    let first = svc.get_group_info(GROUP).expect("group info");

    assert_eq!(
        svc.campaign(GROUP, "me", 60).await.unwrap(),
        LeaderRole::Leader,
        "重复 campaign 不得把自己降为 Follower"
    );
    let second = svc.get_group_info(GROUP).expect("group info");
    assert_eq!(
        first.lease_id, second.lease_id,
        "保持身份时必须沿用**原有**租约，而不是这次多申请的那一个"
    );
    assert_eq!(read_election_key(&addr, GROUP).await.as_deref(), Some("me"));
}

/// 故障注入（Leader 消失）：在任 Leader 的租约被撤销后，另一候选者必须能当选——
/// 即双主防护不得退化为"永久无主"。
#[tokio::test(flavor = "multi_thread")]
async fn new_leader_elected_after_leader_lease_revoked() {
    let (addr, _sd, _g, _r, _tmp) = common::start_test_server().await;

    let leader_svc = LeaderElectionService::new(inner_for(&addr).await, 16);
    let standby_svc = LeaderElectionService::new(inner_for(&addr).await, 16);

    assert_eq!(
        leader_svc.campaign(GROUP, "leader", 60).await.unwrap(),
        LeaderRole::Leader
    );
    let lease_id = leader_svc.get_group_info(GROUP).unwrap().lease_id;

    // 模拟 Leader 进程消失：由**另一个**客户端撤销其租约（等价于 TTL 到期）。
    let raw = inner_for(&addr).await;
    raw.client
        .lease()
        .revoke(lease_id)
        .await
        .expect("revoke leader lease");
    common::wait_until(
        || async { read_election_key(&addr, GROUP).await.is_none() },
        Duration::from_secs(10),
        "election key must disappear after lease revoke",
    )
    .await;

    assert_eq!(
        standby_svc.campaign(GROUP, "standby", 60).await.unwrap(),
        LeaderRole::Leader,
        "Leader 消失后备用节点必须能当选（不得永久无主）"
    );
    assert_eq!(
        read_election_key(&addr, GROUP).await.as_deref(),
        Some("standby")
    );
}

/// 读取选举 key 上记录的 leader_id（原始 KV 读，绕过本地缓存）。
async fn read_election_key(addr: &str, group: &str) -> Option<String> {
    let inner = inner_for(addr).await;
    let key = format!("/_election/{group}").into_bytes();
    let kvs = inner.client.kv().range(&key, &[], 1, 0).await.ok()?;
    let (_, value) = kvs.into_iter().next()?;
    let parsed: serde_json::Value = serde_json::from_slice(&value).ok()?;
    parsed
        .get("leader_id")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string())
}
