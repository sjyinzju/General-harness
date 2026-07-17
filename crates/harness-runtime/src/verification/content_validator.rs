//! VerificationContentValidator — fail-closed content validation that
//! rejects secrets, tokens, and credentials before they reach persistence.
//!
//! "Field documentation says don't include secrets" is NOT security.
//! This validator is the enforcement layer.

use harness_core::{CoreError, ErrorCode, ErrorSource};

/// Maximum allowed size for any single detail_json or context_json field.
const MAX_DETAIL_BYTES: usize = 256 * 1024; // 256 KiB

/// Maximum allowed size for any single text field.
const MAX_TEXT_BYTES: usize = 64 * 1024; // 64 KiB

/// Validates verification content before persistence.
/// Fail-closed: unknown patterns are treated as potentially sensitive
/// and rejected by default.
pub struct VerificationContentValidator;

impl VerificationContentValidator {
    /// Validate a detail_json or context_json string.
    /// Returns Ok(()) if the content is safe to persist.
    /// Returns Err with a structured diagnostic if secrets or tokens are detected.
    pub fn validate_detail_json(json: &str) -> Result<(), CoreError> {
        if json.is_empty() {
            return Ok(());
        }

        // Size limit.
        if json.len() > MAX_DETAIL_BYTES {
            return Err(CoreError::new(
                ErrorCode::ConfigInvalid,
                format!(
                    "detail_json exceeds max size: {} bytes (limit: {})",
                    json.len(),
                    MAX_DETAIL_BYTES
                ),
                ErrorSource::System,
            ));
        }

        // Check for lease tokens.
        if json.contains("lease_token") {
            return Err(CoreError::new(
                ErrorCode::ConfigInvalid,
                "detail_json contains 'lease_token' — rejected".to_string(),
                ErrorSource::System,
            ));
        }

        // Check for API key patterns (sk-..., ai-..., key-..., etc.).
        let lower = json.to_lowercase();
        if Self::contains_secret_patterns(&lower) {
            return Err(CoreError::new(
                ErrorCode::ConfigInvalid,
                "detail_json contains potential secret or token pattern — rejected".to_string(),
                ErrorSource::System,
            ));
        }

        // Reject large base64 blobs (potential encoded secrets or binary dumps).
        if json.contains("base64,") || json.contains(";base64,") {
            return Err(CoreError::new(
                ErrorCode::ConfigInvalid,
                "detail_json contains base64-encoded data — rejected".to_string(),
                ErrorSource::System,
            ));
        }

        Ok(())
    }

    /// Validate a summary or message text field.
    pub fn validate_text(text: &str) -> Result<(), CoreError> {
        if text.len() > MAX_TEXT_BYTES {
            return Err(CoreError::new(
                ErrorCode::ConfigInvalid,
                format!(
                    "text field exceeds max size: {} bytes (limit: {})",
                    text.len(),
                    MAX_TEXT_BYTES
                ),
                ErrorSource::System,
            ));
        }

        // Check for secrets in text fields too.
        let lower = text.to_lowercase();
        if Self::contains_secret_patterns(&lower) {
            return Err(CoreError::new(
                ErrorCode::ConfigInvalid,
                "text field contains potential secret or token pattern — rejected".to_string(),
                ErrorSource::System,
            ));
        }

        Ok(())
    }

    /// Check for common secret and token patterns.
    /// Operates on lowercase input.
    fn contains_secret_patterns(lower: &str) -> bool {
        // API key prefixes.
        if lower.contains("sk-")
            || lower.contains("sk_")
            || lower.contains("api_key")
            || lower.contains("apikey")
            || lower.contains("api-key")
        {
            return true;
        }

        // Token patterns.
        if lower.contains("bearer") && lower.contains("eyj") {
            // Bearer token with JWT-like structure.
            return true;
        }
        if lower.contains("access_token")
            || lower.contains("access-token")
            || lower.contains("refresh_token")
            || lower.contains("refresh-token")
        {
            return true;
        }

        // Common credential keys in JSON.
        if lower.contains("\"password\"")
            || lower.contains("\"secret\"")
            || lower.contains("\"credential\"")
            || lower.contains("\"token\"")
            || lower.contains("\"key\"")
        {
            return true;
        }

        // AWS-style key patterns.
        if lower.contains("akia")
            || lower.contains("aws_access_key")
            || lower.contains("aws_secret")
        {
            return true;
        }

        // GitHub tokens.
        if lower.contains("ghp_")
            || lower.contains("gho_")
            || lower.contains("ghu_")
            || lower.contains("ghs_")
            || lower.contains("github_pat_")
        {
            return true;
        }

        // Private key headers.
        if lower.contains("-----begin")
            && (lower.contains("private key")
                || lower.contains("rsa")
                || lower.contains("ec ")
                || lower.contains("dsa"))
        {
            return true;
        }

        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_reject_api_key_in_detail() {
        let detail = r#"{"summary": "found key", "value": "sk-abc123"}"#;
        let result = VerificationContentValidator::validate_detail_json(detail);
        assert!(result.is_err(), "API key pattern 'sk-' must be rejected");
    }

    #[tokio::test]
    async fn test_reject_bearer_token_in_detail() {
        let detail = r#"{"auth": "Bearer eyJhbGciOiJIUzI1NiJ9.xxx"}"#;
        let result = VerificationContentValidator::validate_detail_json(detail);
        assert!(result.is_err(), "Bearer JWT token must be rejected");
    }

    #[tokio::test]
    async fn test_reject_lease_token_in_detail() {
        let detail = r#"{"note": "the lease_token field should not be here"}"#;
        let result = VerificationContentValidator::validate_detail_json(detail);
        assert!(result.is_err(), "'lease_token' must be rejected");
    }

    #[tokio::test]
    async fn test_reject_private_key_header() {
        let detail = r#"{"key": "-----BEGIN RSA PRIVATE KEY-----"}"#;
        let result = VerificationContentValidator::validate_detail_json(detail);
        assert!(result.is_err(), "Private key header must be rejected");
    }

    #[tokio::test]
    async fn test_reject_github_token() {
        let detail = r#"{"token": "ghp_abcdef1234567890"}"#;
        let result = VerificationContentValidator::validate_detail_json(detail);
        assert!(result.is_err(), "GitHub token must be rejected");
    }

    #[tokio::test]
    async fn test_reject_aws_key() {
        let detail = r#"{"key": "AKIAIOSFODNN7EXAMPLE"}"#;
        let result = VerificationContentValidator::validate_detail_json(detail);
        assert!(result.is_err(), "AWS key must be rejected");
    }

    #[tokio::test]
    async fn test_reject_oversized_detail() {
        let detail = "x".repeat(300_000);
        let result = VerificationContentValidator::validate_detail_json(&detail);
        assert!(result.is_err(), "Oversized detail must be rejected");
    }

    #[tokio::test]
    async fn test_reject_base64_data() {
        let detail = r#"{"img": "data:image/png;base64,iVBORw0KGgo="}"#;
        let result = VerificationContentValidator::validate_detail_json(detail);
        assert!(result.is_err(), "Base64 data must be rejected");
    }

    #[tokio::test]
    async fn test_allow_safe_detail() {
        let detail = r#"{"files": 3, "added": ["src/lib.rs"], "modified": ["src/main.rs"]}"#;
        let result = VerificationContentValidator::validate_detail_json(detail);
        assert!(result.is_ok(), "Safe detail must be allowed");
    }

    #[tokio::test]
    async fn test_allow_empty_detail() {
        let result = VerificationContentValidator::validate_detail_json("");
        assert!(result.is_ok(), "Empty detail must be allowed");
    }

    #[tokio::test]
    async fn test_allow_reference_based_detail() {
        let detail = r#"{"artifact_ref": "art-abc123", "summary": "3 files changed"}"#;
        let result = VerificationContentValidator::validate_detail_json(detail);
        assert!(result.is_ok(), "Reference-based detail must be allowed");
    }

    #[tokio::test]
    async fn test_reject_password_in_detail() {
        let detail = r#"{"connection": {"password": "super-secret-123"}}"#;
        let result = VerificationContentValidator::validate_detail_json(detail);
        assert!(result.is_err(), "'password' key must be rejected");
    }

    #[tokio::test]
    async fn test_reject_secret_key_in_text() {
        let text = "API key used: sk-live-abcdef";
        let result = VerificationContentValidator::validate_text(text);
        assert!(result.is_err(), "Secret in text must be rejected");
    }

    #[tokio::test]
    async fn test_allow_safe_text() {
        let text = "All 3 files were checked and no issues were found.";
        let result = VerificationContentValidator::validate_text(text);
        assert!(result.is_ok(), "Safe text must be allowed");
    }

    #[tokio::test]
    async fn test_reject_oversized_text() {
        let text = "x".repeat(100_000);
        let result = VerificationContentValidator::validate_text(&text);
        assert!(result.is_err(), "Oversized text must be rejected");
    }
}
