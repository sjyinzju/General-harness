//! ProcessEventRedactor — best-effort redaction of known secret values.
//!
//! Scope and boundaries (explicitly best-effort):
//! - Redacts ONLY values registered for the current execution (e.g. injected
//!   credential env values). It does not scan, guess, or pattern-match.
//! - Applied to human/log surfaces: stdout/stderr previews, error messages,
//!   tracing fields. Spool files keep raw process output by design — they are
//!   the artifact of record and live in a harness-owned directory.
//! - Operates on UTF-8 (lossy) text. A secret split across separate preview
//!   truncation boundaries or re-encoded by the child process is NOT detected.
//! - Never reads `auth.json`, OS credential stores, or agent-private config;
//!   the only inputs are the values the caller registers.
//! - Environment variables are never logged by value: `env_presence` reports
//!   names only.

use std::collections::HashMap;

const PLACEHOLDER: &str = "[REDACTED]";
/// Values shorter than this are ignored — redacting 1–3 char fragments would
/// mangle output without hiding anything meaningful.
const MIN_SECRET_LEN: usize = 4;

#[derive(Debug, Default, Clone)]
pub struct ProcessEventRedactor {
    /// Sorted longest-first so overlapping secrets redact deterministically.
    secrets: Vec<String>,
}

impl ProcessEventRedactor {
    pub fn new() -> Self {
        Self::default()
    }

    /// Build a redactor from the execution's known secret values.
    pub fn with_secrets<I, S>(values: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let mut r = Self::new();
        for v in values {
            r.register_secret(v);
        }
        r
    }

    /// Register a known secret value for the current execution.
    pub fn register_secret(&mut self, value: impl Into<String>) {
        let value = value.into();
        if value.len() < MIN_SECRET_LEN || self.secrets.contains(&value) {
            return;
        }
        self.secrets.push(value);
        self.secrets.sort_by_key(|s| std::cmp::Reverse(s.len()));
    }

    pub fn is_empty(&self) -> bool {
        self.secrets.is_empty()
    }

    /// Redact all registered secrets from a string.
    pub fn redact_str(&self, input: &str) -> String {
        let mut out = input.to_string();
        for secret in &self.secrets {
            if out.contains(secret.as_str()) {
                out = out.replace(secret.as_str(), PLACEHOLDER);
            }
        }
        out
    }

    /// Lossy-decode bytes and redact. Invalid UTF-8 becomes U+FFFD; it never
    /// panics and never re-emits the raw bytes.
    pub fn redact_lossy(&self, bytes: &[u8]) -> String {
        self.redact_str(&String::from_utf8_lossy(bytes))
    }

    /// Report environment as names + presence only — values are never logged.
    pub fn env_presence(env: &HashMap<String, String>) -> Vec<String> {
        let mut names: Vec<String> = env.keys().cloned().collect();
        names.sort();
        names
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_registered_secret() {
        let r = ProcessEventRedactor::with_secrets(["sk-super-secret-123"]);
        let out = r.redact_str("token=sk-super-secret-123;rest");
        assert_eq!(out, "token=[REDACTED];rest");
    }

    #[test]
    fn overlapping_secrets_longest_first() {
        let r = ProcessEventRedactor::with_secrets(["abcd", "abcdefgh"]);
        let out = r.redact_str("x abcdefgh y abcd z");
        assert_eq!(out, "x [REDACTED] y [REDACTED] z");
    }

    #[test]
    fn short_values_ignored() {
        let r = ProcessEventRedactor::with_secrets(["ab"]);
        assert_eq!(r.redact_str("ab ab"), "ab ab");
        assert!(r.is_empty());
    }

    #[test]
    fn invalid_utf8_lossy_safe() {
        let r = ProcessEventRedactor::with_secrets(["secret-value"]);
        let out = r.redact_lossy(&[0xFF, 0xFE, b'o', b'k']);
        assert!(out.contains("ok"));
    }

    #[test]
    fn env_presence_names_only() {
        let mut env = HashMap::new();
        env.insert(
            "MY_TOKEN".to_string(),
            "value-should-not-appear".to_string(),
        );
        env.insert("PATH".to_string(), "C:\\bin".to_string());
        let names = ProcessEventRedactor::env_presence(&env);
        assert_eq!(names, vec!["MY_TOKEN".to_string(), "PATH".to_string()]);
    }
}
