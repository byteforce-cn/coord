//! Pure helper functions for `RaftRuntime`.
//!
//! Small utilities for quorum math, Members<->MembershipNode conversion,
//! randomised election timeouts, and endpoint normalisation.

use std::collections::HashMap;
use std::time::Duration;

use rand::Rng;

use crate::raft_store::MembershipNode;

type Members = HashMap<String, String>;

pub(super) const ELECTION_TIMEOUT_BASE: Duration = Duration::from_millis(2200);
pub(super) const ELECTION_TIMEOUT_JITTER_MAX: Duration = Duration::from_millis(1800);

pub(super) fn majority(member_count: usize) -> usize {
    (member_count / 2) + 1
}

pub(super) fn members_to_nodes(members: &Members) -> Vec<MembershipNode> {
    let mut nodes = members
        .iter()
        .map(|(node_id, address)| MembershipNode {
            node_id: node_id.clone(),
            address: address.clone(),
        })
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    nodes
}

pub(super) fn nodes_to_members(nodes: &[MembershipNode]) -> Members {
    nodes
        .iter()
        .map(|node| (node.node_id.clone(), node.address.clone()))
        .collect()
}

pub(super) fn random_election_timeout() -> Duration {
    let mut rng = rand::thread_rng();
    let jitter_ms = rng.gen_range(0..=ELECTION_TIMEOUT_JITTER_MAX.as_millis() as u64);
    ELECTION_TIMEOUT_BASE + Duration::from_millis(jitter_ms)
}

pub(super) fn normalize_endpoint(address: &str) -> String {
    let trimmed = address.trim();
    if trimmed.starts_with("http://") || trimmed.starts_with("https://") {
        trimmed.to_string()
    } else {
        format!("http://{trimmed}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn majority_matches_standard_raft_quorum_sizes() {
        assert_eq!(majority(1), 1);
        assert_eq!(majority(2), 2);
        assert_eq!(majority(3), 2);
        assert_eq!(majority(4), 3);
        assert_eq!(majority(5), 3);
        assert_eq!(majority(7), 4);
    }

    #[test]
    fn members_nodes_roundtrip_is_stable_and_sorted() {
        let mut m: Members = HashMap::new();
        m.insert("b".into(), "127.0.0.1:2".into());
        m.insert("a".into(), "127.0.0.1:1".into());
        m.insert("c".into(), "127.0.0.1:3".into());
        let nodes = members_to_nodes(&m);
        assert_eq!(
            nodes.iter().map(|n| n.node_id.as_str()).collect::<Vec<_>>(),
            vec!["a", "b", "c"]
        );
        let back = nodes_to_members(&nodes);
        assert_eq!(back, m);
    }

    #[test]
    fn random_election_timeout_stays_within_bounds() {
        for _ in 0..200 {
            let t = random_election_timeout();
            assert!(t >= ELECTION_TIMEOUT_BASE);
            assert!(t <= ELECTION_TIMEOUT_BASE + ELECTION_TIMEOUT_JITTER_MAX);
        }
    }

    #[test]
    fn normalize_endpoint_preserves_explicit_scheme() {
        assert_eq!(normalize_endpoint("http://a:1"), "http://a:1");
        assert_eq!(normalize_endpoint("https://a:1"), "https://a:1");
        assert_eq!(normalize_endpoint("  https://a:1  "), "https://a:1");
    }

    #[test]
    fn normalize_endpoint_defaults_to_http_when_scheme_absent() {
        assert_eq!(
            normalize_endpoint("127.0.0.1:9090"),
            "http://127.0.0.1:9090"
        );
        assert_eq!(normalize_endpoint("  node-1:7000 "), "http://node-1:7000");
    }
}
