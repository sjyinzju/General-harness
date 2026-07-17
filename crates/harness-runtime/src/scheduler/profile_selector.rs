//! RuntimeProfileSelector — deterministic profile selection for task dispatch.
//! Never guesses capability from model name. Never treats env var presence as auth.

use harness_core::contracts::scheduler::ProfileSelection;
use harness_core::contracts::runtime_profile::{
    AuthCheckStatus, CoreStatus, ExecutionStatus, RuntimeProfile,
};
use harness_core::{CoreError, ErrorCode, ErrorSource};
use sqlx::SqlitePool;

pub struct RuntimeProfileSelector {
    pool: SqlitePool,
}

impl RuntimeProfileSelector {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    /// Select the best RuntimeProfile for a task.
    /// - `preferred_profile_id`: explicit user preference (optional)
    /// - `allowed_agent_kinds`: which agent kinds are acceptable (empty = all)
    /// - `required_capabilities`: capability constraints
    pub async fn select(
        &self,
        preferred_profile_id: Option<&str>,
        allowed_agent_kinds: &[String],
        _required_capabilities: &[String],
    ) -> Result<ProfileSelection, CoreError> {
        // 1. Explicit preference — check first regardless of what else exists
        if let Some(pref_id) = preferred_profile_id {
            let exists: Option<(String, String, String, String, String, String)> = sqlx::query_as(
                "SELECT id, agent_kind, adapter_kind, agent_version, core_status, execution_status FROM runtime_profiles WHERE id = ?",
            )
            .bind(pref_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

            match exists {
                Some((id, kind, adapter, _, _, _)) => {
                    if allowed_agent_kinds.is_empty() || allowed_agent_kinds.contains(&kind) {
                        return Ok(ProfileSelection::Selected {
                            profile_id: id,
                            agent_kind: kind,
                            adapter_kind: adapter,
                            reason: "explicit user preference".to_string(),
                        });
                    }
                    return Ok(ProfileSelection::ExplicitProfileUnavailable {
                        requested_profile_id: pref_id.to_string(),
                        reason: "agent kind not in allowed list".to_string(),
                    });
                }
                None => {
                    return Ok(ProfileSelection::ExplicitProfileUnavailable {
                        requested_profile_id: pref_id.to_string(),
                        reason: "profile not found".to_string(),
                    });
                }
            }
        }

        // Load all non-terminal profiles
        let rows: Vec<(
            String, String, String, String, String, String,
        )> = sqlx::query_as(
            "SELECT id, agent_kind, adapter_kind, agent_version, core_status, execution_status FROM runtime_profiles WHERE core_status != 'unavailable' ORDER BY id",
        )
        .fetch_all(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

        if rows.is_empty() {
            return Ok(ProfileSelection::NoCompatibleProfile {
                required_capabilities: vec![],
            });
        }

        // 2. Filter by agent kind
        let mut candidates: Vec<&(String, String, String, String, String, String)> = rows
            .iter()
            .filter(|r| allowed_agent_kinds.is_empty() || allowed_agent_kinds.contains(&r.1))
            .collect();

        if candidates.is_empty() {
            return Ok(ProfileSelection::NoCompatibleProfile {
                required_capabilities: vec![],
            });
        }

        // 3. Sort by priority
        candidates.sort_by(|a, b| {
            let score_a = profile_priority_score(&a.4, &a.5);
            let score_b = profile_priority_score(&b.4, &b.5);
            score_b.cmp(&score_a).then_with(|| a.0.cmp(&b.0))
        });

        let best = &candidates[0];
        Ok(ProfileSelection::Selected {
            profile_id: best.0.clone(),
            agent_kind: best.1.clone(),
            adapter_kind: best.2.clone(),
            reason: format!("deterministic selection: core={}, exec={}", best.4, best.5),
        })
    }

    /// Load a RuntimeProfile from the database by ID.
    pub async fn get_profile(&self, profile_id: &str) -> Result<Option<RuntimeProfile>, CoreError> {
        let row: Option<(
            String, String, String, String, String, String, String, String, String, String, String, String, String,
        )> = sqlx::query_as(
            "SELECT id, agent_definition_id, agent_kind, adapter_kind, agent_version, executable_path, provider, provider_source, model, base_url, auth_mode, auth_status, core_status FROM runtime_profiles WHERE id = ?",
        )
        .bind(profile_id)
        .fetch_optional(&self.pool)
        .await
        .map_err(|e| CoreError::new(ErrorCode::PersistenceError, e.to_string(), ErrorSource::System))?;

        Ok(row.map(
            |(id, def_id, kind, adapter, ver, exe, prov, prov_src, mdl, url, auth_m, auth_s, core)| {
                RuntimeProfile {
                    id,
                    agent_definition_id: def_id,
                    label: String::new(),
                    agent_kind: kind,
                    adapter_kind: adapter,
                    agent_version: ver,
                    executable_path: exe,
                    provider: prov,
                    provider_source: serde_json::from_str(&prov_src).unwrap_or(
                        harness_core::contracts::runtime_profile::ProviderSource::UserDeclared,
                    ),
                    model: Some(mdl),
                    base_url: Some(url),
                    auth_mode: serde_json::from_str(&auth_m).unwrap_or(
                        harness_core::contracts::runtime_profile::AuthMode::Unknown,
                    ),
                    auth_status: serde_json::from_str(&auth_s).unwrap_or(
                        harness_core::contracts::runtime_profile::AuthStatus::Unknown,
                    ),
                    credential_ref: None,
                    capabilities: harness_core::contracts::runtime_profile::CapabilitySet {
                        required: harness_core::contracts::runtime_profile::RequiredCapabilities {
                            execute: harness_core::contracts::runtime_profile::TriState::Unknown,
                            working_directory: harness_core::contracts::runtime_profile::TriState::Unknown,
                            stream_output: harness_core::contracts::runtime_profile::TriState::Unknown,
                            process_exit: harness_core::contracts::runtime_profile::TriState::Unknown,
                            cancellation: harness_core::contracts::runtime_profile::TriState::Unknown,
                            timeout: harness_core::contracts::runtime_profile::TriState::Unknown,
                            final_result: harness_core::contracts::runtime_profile::TriState::Unknown,
                        },
                        optional: harness_core::contracts::runtime_profile::OptionalCapabilities {
                            native_session_resume: harness_core::contracts::runtime_profile::TriState::Unknown,
                            structured_output: harness_core::contracts::runtime_profile::TriState::Unknown,
                            tool_events: harness_core::contracts::runtime_profile::TriState::Unknown,
                            file_change_events: harness_core::contracts::runtime_profile::TriState::Unknown,
                            reasoning_summary: harness_core::contracts::runtime_profile::TriState::Unknown,
                            interactive_approval: harness_core::contracts::runtime_profile::TriState::Unknown,
                            usage_reporting: harness_core::contracts::runtime_profile::TriState::Unknown,
                        },
                        workspace_modes: vec![],
                        supported_languages: vec![],
                        mcp_tools: vec![],
                        supported_platforms: vec![],
                    },
                    core_status: serde_json::from_str(&core).unwrap_or(CoreStatus::Available),
                    authentication_status: AuthCheckStatus::Unknown,
                    execution_status: ExecutionStatus::Untested,
                    optional_integrations: vec![],
                    discovery_source: String::new(),
                    passive_probe: None,
                    active_validation: None,
                    concurrency_max: 1,
                    created_at: chrono::Utc::now(),
                    updated_at: chrono::Utc::now(),
                }
            },
        ))
    }
}

fn profile_priority_score(core_status: &str, exec_status: &str) -> i32 {
    let core_score = match core_status {
        "available" => 3,
        "degraded" => 1,
        _ => 0,
    };
    let exec_score = match exec_status {
        "smoke_test_passed" => 2,
        "untested" => 1,
        _ => 0,
    };
    core_score + exec_score
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;

    async fn setup() -> Database {
        Database::open_in_memory().await.unwrap()
    }

    async fn insert_profile(db: &Database, id: &str, kind: &str, adapter: &str, core: &str, exec: &str) {
        sqlx::query(
            "INSERT INTO runtime_profiles (id, agent_kind, adapter_kind, agent_version, executable_path, provider, provider_source, auth_mode, auth_status, core_status, authentication_status, execution_status) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
        )
        .bind(id).bind(kind).bind(adapter).bind("1.0").bind("/bin/agent").bind("test").bind("user_declared").bind("unknown").bind("unknown")
        .bind(core).bind("unknown").bind(exec)
        .execute(&db.pool).await.unwrap();
    }

    #[tokio::test]
    async fn test_explicit_profile_selected() {
        let db = setup().await;
        insert_profile(&db, "p1", "claude-code", "claude-cli", "available", "untested").await;
        insert_profile(&db, "p2", "codex", "codex-cli", "available", "untested").await;
        let selector = RuntimeProfileSelector::new(db.pool.clone());
        let result = selector.select(Some("p1"), &[], &[]).await.unwrap();
        assert!(matches!(result, ProfileSelection::Selected { profile_id, .. } if profile_id == "p1"));
    }

    #[tokio::test]
    async fn test_explicit_profile_unavailable() {
        let db = setup().await;
        let selector = RuntimeProfileSelector::new(db.pool.clone());
        let result = selector.select(Some("nonexistent"), &[], &[]).await.unwrap();
        assert!(matches!(result, ProfileSelection::ExplicitProfileUnavailable { .. }));
    }

    #[tokio::test]
    async fn test_deterministic_tie_break() {
        let db = setup().await;
        insert_profile(&db, "p-b", "claude-code", "claude-cli", "available", "untested").await;
        insert_profile(&db, "p-a", "claude-code", "claude-cli", "available", "untested").await;
        let selector = RuntimeProfileSelector::new(db.pool.clone());
        // Should pick p-a due to alphabetical sort
        let result = selector.select(None, &[], &[]).await.unwrap();
        assert!(matches!(result, ProfileSelection::Selected { profile_id, .. } if profile_id == "p-a"));
    }

    #[tokio::test]
    async fn test_no_compatible_profile() {
        let db = setup().await;
        insert_profile(&db, "p1", "codex", "codex-cli", "available", "untested").await;
        let selector = RuntimeProfileSelector::new(db.pool.clone());
        let result = selector.select(None, &["claude-code".to_string()], &[]).await.unwrap();
        assert!(matches!(result, ProfileSelection::NoCompatibleProfile { .. }));
    }

    #[tokio::test]
    async fn test_no_silent_provider_switching() {
        let db = setup().await;
        insert_profile(&db, "p-claude", "claude-code", "claude-cli", "available", "untested").await;
        insert_profile(&db, "p-codex", "codex", "codex-cli", "available", "untested").await;
        let selector = RuntimeProfileSelector::new(db.pool.clone());
        // Asking for claude-code should return claude, not codex
        let result = selector.select(None, &["claude-code".to_string()], &[]).await.unwrap();
        assert!(matches!(result, ProfileSelection::Selected { agent_kind, .. } if agent_kind == "claude-code"));
    }
}
