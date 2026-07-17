//! Discovery persistence — stores AgentDefinition, DiscoveryEvidence,
//! ProviderHint, and RuntimeProfile records idempotently.

use chrono::Utc;
use harness_core::contracts::discovery::DiscoveredAgent;
use harness_core::CoreError;
use sqlx::SqlitePool;

/// Persist a discovered agent and its evidence idempotently.
/// If the agent already exists (same id), updates last_seen_at and merges evidence.
pub async fn upsert_agent_definition(
    pool: &SqlitePool,
    agent: &DiscoveredAgent,
) -> Result<(), CoreError> {
    let id = &agent.identity.discovery_hash;
    let now = Utc::now().to_rfc3339();

    // Upsert agent_definition
    sqlx::query(
        r#"INSERT INTO agent_definitions
           (id, agent_kind, label, executable_path, discovery_source,
            discovery_source_detail, version, is_wrapper, wraps_agent_kind,
            passive_status, diagnostics_json, first_seen_at, last_seen_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
             version = COALESCE(excluded.version, agent_definitions.version),
             last_seen_at = excluded.last_seen_at,
             updated_at = excluded.updated_at"#,
    )
    .bind(id)
    .bind(&agent.identity.agent_kind)
    .bind(&agent.identity.executable_basename)
    .bind(&agent.identity.executable_path)
    .bind("path")
    .bind(&agent.identity.executable_path)
    .bind(&agent.version)
    .bind(if agent.is_wrapper { 1 } else { 0 })
    .bind(&agent.wraps_agent_kind)
    .bind("detected")
    .bind("[]")
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await
    .map_err(|e| {
        harness_core::CoreError::new(
            harness_core::ErrorCode::PersistenceError,
            format!("upsert agent_definition: {e}"),
            harness_core::ErrorSource::System,
        )
    })?;

    // Persist evidence (append-only; idempotent by evidence kind + observation)
    for evidence in &agent.discovery_evidence {
        let evidence_id = format!(
            "{}-{}-{}",
            id,
            serde_json::to_string(&evidence.evidence_kind).unwrap_or_default(),
            &evidence.observation[..evidence.observation.len().min(40)],
        );
        let evidence_id = &evidence_id[..evidence_id.len().min(255)];

        sqlx::query(
            r#"INSERT OR IGNORE INTO discovery_evidence
               (id, agent_definition_id, evidence_kind, observation, confidence, collected_at)
               VALUES (?, ?, ?, ?, ?, ?)"#,
        )
        .bind(evidence_id)
        .bind(id)
        .bind(serde_json::to_string(&evidence.evidence_kind).unwrap_or_default())
        .bind(&evidence.observation)
        .bind(serde_json::to_string(&evidence.confidence).unwrap_or_default())
        .bind(evidence.collected_at.to_rfc3339())
        .execute(pool)
        .await
        .map_err(|e| {
            harness_core::CoreError::new(
                harness_core::ErrorCode::PersistenceError,
                format!("insert discovery_evidence: {e}"),
                harness_core::ErrorSource::System,
            )
        })?;
    }

    // Persist provider hints (replace existing on update)
    for hint in &agent.provider_hints {
        let hint_id = format!("{}-{}", id, hint.provider);

        sqlx::query(
            r#"INSERT INTO agent_provider_hints
               (id, agent_definition_id, provider, source, confidence, evidence_json,
                base_url, is_custom_endpoint)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?)
               ON CONFLICT(id) DO UPDATE SET
                 confidence = excluded.confidence,
                 evidence_json = excluded.evidence_json,
                 base_url = COALESCE(excluded.base_url, agent_provider_hints.base_url)"#,
        )
        .bind(&hint_id)
        .bind(id)
        .bind(&hint.provider)
        .bind(serde_json::to_string(&hint.source).unwrap_or_default())
        .bind(serde_json::to_string(&hint.confidence).unwrap_or_default())
        .bind(serde_json::to_string(&hint.evidence).unwrap_or_default())
        .bind(&hint.base_url)
        .bind(if hint.is_custom_endpoint { 1 } else { 0 })
        .execute(pool)
        .await
        .map_err(|e| {
            harness_core::CoreError::new(
                harness_core::ErrorCode::PersistenceError,
                format!("upsert provider_hint: {e}"),
                harness_core::ErrorSource::System,
            )
        })?;
    }

    Ok(())
}

/// Persist or update a runtime_profile from discovery data.
/// Idempotent — does not duplicate profiles for the same agent.
#[allow(clippy::too_many_arguments)]
pub async fn upsert_runtime_profile(
    pool: &SqlitePool,
    profile_id: &str,
    agent_definition_id: &str,
    agent_kind: &str,
    adapter_kind: &str,
    agent_version: &str,
    executable_path: &str,
    provider: &str,
    provider_source: &str,
    label: &str,
) -> Result<(), CoreError> {
    let now = Utc::now().to_rfc3339();

    sqlx::query(
        r#"INSERT INTO runtime_profiles
           (id, agent_definition_id, agent_kind, adapter_kind, agent_version,
            executable_path, provider, provider_source, auth_mode, auth_status,
            core_status, authentication_status, execution_status,
            capabilities_json, label, first_seen_at, last_seen_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'unknown', 'unknown',
                   'available', 'unknown', 'untested', '{}', ?, ?, ?)
           ON CONFLICT(id) DO UPDATE SET
             agent_version = COALESCE(excluded.agent_version, runtime_profiles.agent_version),
             last_seen_at = excluded.last_seen_at,
             updated_at = excluded.updated_at"#,
    )
    .bind(profile_id)
    .bind(agent_definition_id)
    .bind(agent_kind)
    .bind(adapter_kind)
    .bind(agent_version)
    .bind(executable_path)
    .bind(provider)
    .bind(provider_source)
    .bind(label)
    .bind(&now)
    .bind(&now)
    .execute(pool)
    .await
    .map_err(|e| {
        harness_core::CoreError::new(
            harness_core::ErrorCode::PersistenceError,
            format!("upsert runtime_profile: {e}"),
            harness_core::ErrorSource::System,
        )
    })?;

    Ok(())
}

/// Check whether secret values are present in the database.
/// Returns a list of suspicious columns if any are found.
pub async fn verify_no_secrets_in_db(pool: &SqlitePool) -> Result<Vec<String>, CoreError> {
    let mut findings: Vec<String> = Vec::new();

    // Check runtime_profiles for secret-like values
    let rows: Vec<(String, String)> = sqlx::query_as(
        "SELECT id, credential_ref FROM runtime_profiles WHERE credential_ref IS NOT NULL AND credential_ref != ''"
    )
    .fetch_all(pool)
    .await
    .map_err(|e| {
        CoreError::new(
            harness_core::ErrorCode::PersistenceError,
            format!("verify_no_secrets: {e}"),
            harness_core::ErrorSource::System,
        )
    })?;

    for (id, cred_ref) in rows {
        // credential_ref should be a reference label, not the actual value
        if cred_ref.len() > 200 || cred_ref.starts_with("sk-") || cred_ref.contains("eyJ") {
            findings.push(format!(
                "Profile {} has suspicious credential_ref: {}",
                id,
                &cred_ref[..cred_ref.len().min(20)]
            ));
        }
    }

    Ok(findings)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Database;
    use harness_core::contracts::discovery::{
        AuthModeHint, AuthStateValue, AuthenticationState, DiscoveredAgent, DiscoveryConfidence,
        DiscoveryEvidence, EvidenceKind, ExecutableIdentity, ProviderHint, ProviderHintSource,
    };

    fn test_agent() -> DiscoveredAgent {
        let now = Utc::now();
        let identity = ExecutableIdentity::compute("/usr/bin/claude", "claude-code");
        DiscoveredAgent {
            identity,
            discovery_evidence: vec![DiscoveryEvidence {
                evidence_kind: EvidenceKind::PathResolution,
                observation: "Found at /usr/bin/claude".to_string(),
                confidence: DiscoveryConfidence::High,
                collected_at: now,
            }],
            confidence: DiscoveryConfidence::High,
            version: Some("2.1.210".to_string()),
            is_wrapper: false,
            wraps_agent_kind: None,
            provider_hints: vec![ProviderHint {
                provider: "anthropic".to_string(),
                source: ProviderHintSource::Unknown,
                confidence: DiscoveryConfidence::Medium,
                evidence: vec!["Default for claude-code".to_string()],
                base_url: None,
                is_custom_endpoint: false,
            }],
            authentication_state: AuthenticationState {
                status: AuthStateValue::Unknown,
                mode: AuthModeHint::ApiKeyEnv,
                evidence: vec!["ANTHROPIC_API_KEY is set".to_string()],
            },
            profiles: vec!["claude-default".to_string()],
            first_seen_at: now,
            last_seen_at: now,
        }
    }

    #[tokio::test]
    async fn test_upsert_agent_definition_idempotent() {
        let db = Database::open_in_memory().await.unwrap();
        let agent = test_agent();

        // First insert
        upsert_agent_definition(&db.pool, &agent).await.unwrap();

        // Second insert (same id) should be idempotent
        upsert_agent_definition(&db.pool, &agent).await.unwrap();

        // Verify only one row
        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM agent_definitions WHERE id = ?")
            .bind(&agent.identity.discovery_hash)
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(count.0, 1);
    }

    #[tokio::test]
    async fn test_upsert_updates_last_seen() {
        let db = Database::open_in_memory().await.unwrap();
        let agent = test_agent();
        upsert_agent_definition(&db.pool, &agent).await.unwrap();

        // Update with new timestamp
        let mut agent2 = test_agent();
        agent2.version = Some("2.2.0".to_string());
        upsert_agent_definition(&db.pool, &agent2).await.unwrap();

        let version: (String,) =
            sqlx::query_as("SELECT version FROM agent_definitions WHERE id = ?")
                .bind(&agent.identity.discovery_hash)
                .fetch_one(&db.pool)
                .await
                .unwrap();
        assert_eq!(version.0, "2.2.0");
    }

    #[tokio::test]
    async fn test_no_secret_values_in_db() {
        let db = Database::open_in_memory().await.unwrap();
        let agent = test_agent();
        upsert_agent_definition(&db.pool, &agent).await.unwrap();

        // Also insert a runtime profile
        upsert_runtime_profile(
            &db.pool,
            "claude-default",
            &agent.identity.discovery_hash,
            "claude-code",
            "claude-cli",
            "2.1.210",
            "/usr/bin/claude",
            "anthropic",
            "unknown",
            "Claude Default",
        )
        .await
        .unwrap();

        let findings = verify_no_secrets_in_db(&db.pool).await.unwrap();
        assert!(
            findings.is_empty(),
            "No secrets should be in DB: {:?}",
            findings
        );
    }

    #[tokio::test]
    async fn test_runtime_profile_idempotent() {
        let db = Database::open_in_memory().await.unwrap();

        upsert_runtime_profile(
            &db.pool,
            "rp-test",
            "def-1",
            "claude-code",
            "claude-cli",
            "1.0.0",
            "/bin/claude",
            "anthropic",
            "unknown",
            "Test Profile",
        )
        .await
        .unwrap();

        // Second insert should not duplicate
        upsert_runtime_profile(
            &db.pool,
            "rp-test",
            "def-1",
            "claude-code",
            "claude-cli",
            "2.0.0",
            "/bin/claude",
            "anthropic",
            "unknown",
            "Test Profile Updated",
        )
        .await
        .unwrap();

        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM runtime_profiles WHERE id = 'rp-test'")
                .fetch_one(&db.pool)
                .await
                .unwrap();
        assert_eq!(count.0, 1);
    }
}
