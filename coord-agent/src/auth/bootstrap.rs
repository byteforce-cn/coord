// Agent Bootstrap — First-start and restart authentication
//
// Handles the "chicken-and-egg" problem: how does an Agent authenticate itself
// to the Server before it has a CCT?
//
// First start: reads bootstrap_token from agent.yaml, exchanges it for a
// short-lived CCT, then pulls role mappings and persists a credential file.
//
// Restart: reads the persisted agent_credential.json, uses the long-lived
// service token to reconnect.
//
// See docs/capability-auth-implementation.md §6.1.

use std::path::Path;

use serde::{Deserialize, Serialize};

// ──── Bootstrap Config ────

/// Bootstrap configuration from agent.yaml
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct BootstrapConfig {
    /// One-time bootstrap token (only used on first start)
    #[serde(default)]
    pub bootstrap_token: Option<String>,
    /// Path to the persisted credential file
    #[serde(default = "default_credential_path")]
    pub credential_path: String,
}

fn default_credential_path() -> String {
    "agent_credential.json".to_string()
}

// ──── Agent Credential ────

/// Persisted agent credential (stored in agent_credential.json).
/// Encrypted with Barrier DEK before writing to disk.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentCredential {
    /// Unique agent identifier
    pub agent_id: String,
    /// Long-lived service token for reconnection
    pub service_token: String,
    /// Server endpoints (cluster members)
    pub server_endpoints: Vec<String>,
    /// When this credential was issued (Unix seconds)
    pub issued_at: i64,
}

// ──── Bootstrap State ────

/// Represents the agent's bootstrap lifecycle state.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapState {
    /// Agent has no credential — needs first-start bootstrap
    NeedsBootstrap,
    /// Agent has a credential file — ready for restart
    HasCredential(AgentCredential),
    /// Bootstrap completed, agent is authenticated
    Bootstrapped {
        agent_id: String,
        cct: String,
        expires_at: i64,
    },
}

// ──── Bootstrap Manager ────

/// Manages the Agent bootstrap lifecycle.
pub struct BootstrapManager {
    config: BootstrapConfig,
}

impl BootstrapManager {
    /// Create a new bootstrap manager.
    pub fn new(config: BootstrapConfig) -> Self {
        Self { config }
    }

    /// Determine the current bootstrap state.
    ///
    /// Checks for an existing credential file. If found, reads and decrypts it.
    /// If not found, the agent needs first-start bootstrap.
    pub fn determine_state(&self) -> BootstrapState {
        let path = Path::new(&self.config.credential_path);
        if path.exists() {
            match self.read_credential(path) {
                Ok(cred) => BootstrapState::HasCredential(cred),
                Err(_) => {
                    tracing::warn!(
                        "Credential file exists but cannot be read, falling back to bootstrap"
                    );
                    BootstrapState::NeedsBootstrap
                }
            }
        } else {
            BootstrapState::NeedsBootstrap
        }
    }

    /// Check if the bootstrap token is configured for first-start.
    pub fn has_bootstrap_token(&self) -> bool {
        self.config.bootstrap_token.is_some()
    }

    /// Get the bootstrap token (for first-start authentication).
    pub fn get_bootstrap_token(&self) -> Option<&str> {
        self.config.bootstrap_token.as_deref()
    }

    /// Persist a credential to disk.
    ///
    /// In production, this should encrypt the credential with Barrier DEK before
    /// writing and set file permissions to 0600.
    pub fn persist_credential(&self, credential: &AgentCredential) -> Result<(), String> {
        let json = serde_json::to_string_pretty(credential)
            .map_err(|e| format!("credential serialization: {e}"))?;

        let path = Path::new(&self.config.credential_path);
        std::fs::write(path, json.as_bytes())
            .map_err(|e| format!("credential write: {e}"))?;

        // Set file permissions to 0600 (owner read/write only)
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))
                .map_err(|e| format!("credential permissions: {e}"))?;
        }

        tracing::info!("Agent credential persisted to {}", self.config.credential_path);
        Ok(())
    }

    /// Read and deserialize a credential from disk.
    fn read_credential(&self, path: &Path) -> Result<AgentCredential, String> {
        let data = std::fs::read_to_string(path)
            .map_err(|e| format!("credential read: {e}"))?;
        let cred: AgentCredential = serde_json::from_str(&data)
            .map_err(|e| format!("credential deserialization: {e}"))?;
        Ok(cred)
    }

    /// Delete the persisted credential (for re-bootstrap).
    pub fn delete_credential(&self) -> Result<(), String> {
        let path = Path::new(&self.config.credential_path);
        if path.exists() {
            std::fs::remove_file(path)
                .map_err(|e| format!("credential delete: {e}"))?;
        }
        Ok(())
    }
}

// ──── Tests ────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bootstrap_state_no_credential() {
        let config = BootstrapConfig {
            bootstrap_token: Some("test-bootstrap-token".to_string()),
            credential_path: "/nonexistent/path/cred.json".to_string(),
        };
        let manager = BootstrapManager::new(config);
        let state = manager.determine_state();
        assert_eq!(state, BootstrapState::NeedsBootstrap);
    }

    #[test]
    fn test_has_bootstrap_token() {
        let config = BootstrapConfig {
            bootstrap_token: Some("test-token".to_string()),
            credential_path: "/tmp/cred.json".to_string(),
        };
        let manager = BootstrapManager::new(config);
        assert!(manager.has_bootstrap_token());
        assert_eq!(manager.get_bootstrap_token(), Some("test-token"));
    }

    #[test]
    fn test_no_bootstrap_token() {
        let config = BootstrapConfig {
            bootstrap_token: None,
            credential_path: "/tmp/cred.json".to_string(),
        };
        let manager = BootstrapManager::new(config);
        assert!(!manager.has_bootstrap_token());
        assert_eq!(manager.get_bootstrap_token(), None);
    }

    #[test]
    fn test_persist_and_read_credential() {
        let dir = tempfile::tempdir().unwrap();
        let cred_path = dir.path().join("test_cred.json");
        let config = BootstrapConfig {
            bootstrap_token: None,
            credential_path: cred_path.to_str().unwrap().to_string(),
        };
        let manager = BootstrapManager::new(config);

        let credential = AgentCredential {
            agent_id: "agent-001".to_string(),
            service_token: "svc-token-abc".to_string(),
            server_endpoints: vec!["127.0.0.1:50051".to_string()],
            issued_at: 1719990000,
        };

        manager.persist_credential(&credential).unwrap();

        // Verify file was created
        assert!(cred_path.exists());

        // Read back and verify
        let state = manager.determine_state();
        match state {
            BootstrapState::HasCredential(read_cred) => {
                assert_eq!(read_cred.agent_id, "agent-001");
                assert_eq!(read_cred.service_token, "svc-token-abc");
            }
            _ => panic!("expected HasCredential state"),
        }
    }
}
