//! Pre-defined console role templates used by the bootstrap endpoint.
//!
//! These templates seed four AppRoles tailored for console users:
//! - `console_readonly` — dashboards and audit viewers
//! - `console_operator` — day-to-day ops (config put, pki issue, workflow start)
//! - `console_risk_ops` — seal/unseal, cluster member remove, pki revoke
//! - `console_admin` — superset of the above for break-glass usage
//!
//! Templates are data; the HTTP handler lives in `http_api::security_bootstrap_console_roles`.

pub(super) struct ConsoleRoleTemplate {
    pub(super) role_name: &'static str,
    pub(super) policies: Vec<String>,
    pub(super) token_ttl_seconds: i64,
    pub(super) secret_id_ttl_seconds: i64,
    pub(super) secret_id_num_uses: u32,
}

pub(super) fn console_role_templates() -> Vec<ConsoleRoleTemplate> {
    vec![
        ConsoleRoleTemplate {
            role_name: "console_readonly",
            policies: vec![
                "overview.read".to_string(),
                "cluster.read".to_string(),
                "registry.read".to_string(),
                "config.read".to_string(),
                "lock.read".to_string(),
                "workflow.read".to_string(),
                "transit.read".to_string(),
                "pki.read".to_string(),
                "security.read".to_string(),
            ],
            token_ttl_seconds: 1800,
            secret_id_ttl_seconds: 86_400,
            secret_id_num_uses: 20,
        },
        ConsoleRoleTemplate {
            role_name: "console_operator",
            policies: vec![
                "overview.read".to_string(),
                "cluster.read".to_string(),
                "registry.read".to_string(),
                "config.read".to_string(),
                "lock.read".to_string(),
                "workflow.read".to_string(),
                "transit.read".to_string(),
                "pki.read".to_string(),
                "security.read".to_string(),
                "config.put".to_string(),
                "workflow.start".to_string(),
                "workflow.intervene".to_string(),
                "workflow.poll".to_string(),
                "workflow.complete".to_string(),
                "pki.issue".to_string(),
                "pki.renew".to_string(),
                "pki.admin".to_string(),
                "cluster.member_add".to_string(),
            ],
            token_ttl_seconds: 1800,
            secret_id_ttl_seconds: 43_200,
            secret_id_num_uses: 10,
        },
        ConsoleRoleTemplate {
            role_name: "console_risk_ops",
            policies: vec![
                "security.seal".to_string(),
                "cluster.member_remove".to_string(),
                "admin.backup".to_string(),
                "pki.revoke".to_string(),
                "transit.admin".to_string(),
                "security.read".to_string(),
            ],
            token_ttl_seconds: 600,
            secret_id_ttl_seconds: 7_200,
            secret_id_num_uses: 3,
        },
        ConsoleRoleTemplate {
            role_name: "console_admin",
            policies: vec![
                "overview.read".to_string(),
                "cluster.read".to_string(),
                "registry.read".to_string(),
                "config.read".to_string(),
                "lock.read".to_string(),
                "workflow.read".to_string(),
                "transit.read".to_string(),
                "pki.read".to_string(),
                "security.read".to_string(),
                "cluster.member_add".to_string(),
                "cluster.member_remove".to_string(),
                "admin.backup".to_string(),
                "config.put".to_string(),
                "workflow.start".to_string(),
                "workflow.intervene".to_string(),
                "workflow.poll".to_string(),
                "workflow.complete".to_string(),
                "transit.admin".to_string(),
                "pki.issue".to_string(),
                "pki.renew".to_string(),
                "pki.revoke".to_string(),
                "pki.admin".to_string(),
                "security.seal".to_string(),
                "security.admin".to_string(),
            ],
            token_ttl_seconds: 1800,
            secret_id_ttl_seconds: 43_200,
            secret_id_num_uses: 10,
        },
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn templates_cover_four_role_families() {
        let t = console_role_templates();
        let names: Vec<&str> = t.iter().map(|x| x.role_name).collect();
        assert_eq!(
            names,
            vec![
                "console_readonly",
                "console_operator",
                "console_risk_ops",
                "console_admin",
            ]
        );
    }

    #[test]
    fn admin_template_is_a_strict_superset_of_readonly_caps() {
        let t = console_role_templates();
        let readonly = t
            .iter()
            .find(|x| x.role_name == "console_readonly")
            .unwrap();
        let admin = t.iter().find(|x| x.role_name == "console_admin").unwrap();
        for cap in &readonly.policies {
            assert!(
                admin.policies.contains(cap),
                "admin missing readonly cap: {cap}"
            );
        }
    }

    #[test]
    fn risk_ops_template_has_short_token_ttl() {
        let t = console_role_templates();
        let risk = t
            .iter()
            .find(|x| x.role_name == "console_risk_ops")
            .unwrap();
        assert!(
            risk.token_ttl_seconds <= 900,
            "risk ops tokens must be short-lived for break-glass semantics"
        );
        assert!(risk.secret_id_num_uses <= 5);
    }
}
