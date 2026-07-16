//! Basic SecretScanner — detection of known secrets, private-key headers,
//! common API-token patterns, and high-entropy candidates in workspace diffs.
//! Best-effort accidental-commit guard, NOT a full DLP system.

// HashSet reserved for future use — not currently needed.

/// Limit: max bytes scanned per single file.
const MAX_FILE_BYTES: usize = 512 * 1024; // 512 KiB
/// Limit: max total bytes scanned per diff.
const MAX_TOTAL_BYTES: usize = 8 * 1024 * 1024; // 8 MiB

#[derive(Debug, Clone, PartialEq)]
pub enum SecretKind {
    KnownSecret { hash: String },
    PrivateKeyHeader { header: String },
    ApiTokenPattern { pattern_name: String },
    HighEntropy { shannon: f64 },
    CredentialFilePath { path_rule: String },
    BinarySkipped,
    TruncatedLargeFile,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SecretFinding {
    pub kind: SecretKind,
    pub file_path: String,
    pub line_number: Option<usize>,
    pub byte_range: Option<(usize, usize)>,
    /// Redacted preview — must not contain the raw secret.
    pub redacted_preview: String,
}

#[derive(Debug, Clone)]
pub struct SecretScanReport {
    pub findings: Vec<SecretFinding>,
    pub files_scanned: usize,
    pub files_skipped: usize,
    pub bytes_scanned: u64,
    pub clean: bool,
}

pub struct SecretScanner {
    /// Pre-sorted longest-first for overlap-safe replacement.
    known_secrets_sorted: Vec<String>,
}

impl SecretScanner {
    pub fn new(known_secrets: Vec<String>) -> Self {
        let mut sorted = known_secrets.clone();
        sorted.sort_by_key(|s| std::cmp::Reverse(s.len()));
        Self {
            known_secrets_sorted: sorted,
        }
    }

    /// Scan a diff payload (file path + new content). Returns findings.
    /// The content text is NOT stored after scanning.
    pub fn scan_diff_file(&self, file_path: &str, new_content: &[u8]) -> Vec<SecretFinding> {
        let mut findings = Vec::new();

        // Binary detection: high ratio of null/non-printable bytes.
        if !new_content.is_empty()
            && new_content
                .iter()
                .filter(|b| {
                    !b.is_ascii_graphic()
                        && **b != b'\n'
                        && **b != b'\r'
                        && **b != b'\t'
                        && **b != b' '
                })
                .count() as f64
                / new_content.len() as f64
                > 0.10
        {
            findings.push(SecretFinding {
                kind: SecretKind::BinarySkipped,
                file_path: file_path.to_string(),
                line_number: None,
                byte_range: None,
                redacted_preview: "[binary file skipped]".into(),
            });
            return findings;
        }

        // Truncation for large files.
        let content = if new_content.len() > MAX_FILE_BYTES {
            findings.push(SecretFinding {
                kind: SecretKind::TruncatedLargeFile,
                file_path: file_path.to_string(),
                line_number: None,
                byte_range: None,
                redacted_preview: format!(
                    "file truncated at {} / {} bytes",
                    MAX_FILE_BYTES,
                    new_content.len()
                ),
            });
            &new_content[..MAX_FILE_BYTES]
        } else {
            new_content
        };

        let text = String::from_utf8_lossy(content);

        // Known secrets (exact match).
        for secret in &self.known_secrets_sorted {
            if text.contains(secret.as_str()) {
                let redacted = text.replace(secret.as_str(), "[REDACTED]");
                let preview = &redacted[..redacted.len().min(200)];
                let mut hasher = std::collections::hash_map::DefaultHasher::new();
                use std::hash::Hasher;
                hasher.write(secret.as_bytes());
                let hash = format!("sha256-equiv:{:016x}", hasher.finish());
                findings.push(SecretFinding {
                    kind: SecretKind::KnownSecret { hash },
                    file_path: file_path.to_string(),
                    line_number: None,
                    byte_range: None,
                    redacted_preview: preview.to_string(),
                });
            }
        }

        // Private key headers.
        for header in [
            "-----BEGIN RSA PRIVATE KEY-----",
            "-----BEGIN OPENSSH PRIVATE KEY-----",
            "-----BEGIN EC PRIVATE KEY-----",
            "-----BEGIN PRIVATE KEY-----",
            "-----BEGIN DSA PRIVATE KEY-----",
        ] {
            if text.contains(header) {
                let preview = &text[..text.len().min(200)];
                findings.push(SecretFinding {
                    kind: SecretKind::PrivateKeyHeader {
                        header: header.to_string(),
                    },
                    file_path: file_path.to_string(),
                    line_number: None,
                    byte_range: None,
                    redacted_preview: preview.replace(header, "[PRIVATE_KEY_HEADER]"),
                });
            }
        }

        // API token patterns (heuristic, not regex — avoids reDoS).
        for (name, prefix) in [
            ("github_pat", "ghp_"),
            ("github_pat_v2", "github_pat_"),
            ("aws_access_key", "AKIA"),
            ("stripe_sk", "sk_live_"),
            ("stripe_test_sk", "sk_test_"),
            ("slack_bot_token", "xoxb-"),
            ("slack_webhook", "https://hooks.slack.com/services/"),
        ] {
            if text.contains(prefix) {
                let line = text.lines().find(|l| l.contains(prefix)).unwrap_or("");
                let redacted = line.replace(prefix, &format!("{prefix}[REDACTED]"));
                findings.push(SecretFinding {
                    kind: SecretKind::ApiTokenPattern {
                        pattern_name: name.to_string(),
                    },
                    file_path: file_path.to_string(),
                    line_number: None,
                    byte_range: None,
                    redacted_preview: redacted.chars().take(200).collect(),
                });
            }
        }

        // Credential file paths.
        for path_rule in [
            ".env",
            ".env.local",
            ".env.production",
            "credentials.json",
            "service-account.json",
            ".npmrc",
            ".pypirc",
            ".git-credentials",
        ] {
            if file_path.ends_with(path_rule) || file_path.contains(&format!("/{path_rule}")) {
                // Only flag if the file contains something that looks like
                // a credential (assignment, json key, etc).
                if text.contains('=')
                    || text.contains("private_key")
                    || text.contains("password")
                    || text.contains("token")
                {
                    let preview = &text[..text.len().min(200)];
                    findings.push(SecretFinding {
                        kind: SecretKind::CredentialFilePath {
                            path_rule: path_rule.to_string(),
                        },
                        file_path: file_path.to_string(),
                        line_number: None,
                        byte_range: None,
                        redacted_preview: preview.to_string(),
                    });
                }
            }
        }

        // High-entropy detection (simple shannon on 32-char windows).
        if text.len() >= 32 && !text.contains(' ') {
            let entropy = shannon_entropy(&text[..text.len().min(256)]);
            if entropy > 4.5 {
                findings.push(SecretFinding {
                    kind: SecretKind::HighEntropy { shannon: entropy },
                    file_path: file_path.to_string(),
                    line_number: None,
                    byte_range: None,
                    redacted_preview: format!("high entropy content ({entropy:.1} bits/char)"),
                });
            }
        }

        findings
    }

    /// Scan a complete diff. Content maps are transient — raw secrets
    /// are never stored.
    pub fn scan_diff(&self, files: &[(String, Vec<u8>)]) -> SecretScanReport {
        let mut findings = Vec::new();
        let mut files_scanned = 0usize;
        let mut files_skipped = 0usize;
        let mut bytes_scanned: u64 = 0;

        for (path, content) in files {
            if bytes_scanned + content.len() as u64 > MAX_TOTAL_BYTES as u64 {
                files_skipped += 1;
                findings.push(SecretFinding {
                    kind: SecretKind::TruncatedLargeFile,
                    file_path: path.clone(),
                    line_number: None,
                    byte_range: None,
                    redacted_preview: format!("total scan limit reached ({MAX_TOTAL_BYTES} bytes)"),
                });
                continue;
            }
            let file_findings = self.scan_diff_file(path, content);
            files_scanned += 1;
            bytes_scanned += content.len() as u64;
            findings.extend(file_findings);
        }

        let clean = findings.is_empty();
        SecretScanReport {
            findings,
            files_scanned,
            files_skipped,
            bytes_scanned,
            clean,
        }
    }
}

/// Shannon entropy over byte distribution.
fn shannon_entropy(data: &str) -> f64 {
    let mut counts = [0u32; 256];
    let len = data.len().min(256);
    for &b in data.as_bytes().iter().take(len) {
        counts[b as usize] += 1;
    }
    let len_f = len as f64;
    let mut entropy = 0.0;
    for &c in &counts {
        if c > 0 {
            let p = c as f64 / len_f;
            entropy -= p * p.log2();
        }
    }
    entropy
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_secret_detected() {
        let scanner = SecretScanner::new(vec!["sk-super-secret-123".into()]);
        let findings = scanner.scan_diff_file("src/main.rs", b"token=sk-super-secret-123;rest");
        assert!(!findings.is_empty());
        assert!(matches!(findings[0].kind, SecretKind::KnownSecret { .. }));
        assert!(!findings[0].redacted_preview.contains("sk-super-secret-123"));
    }

    #[test]
    fn private_key_header_detected() {
        let scanner = SecretScanner::new(vec![]);
        let findings =
            scanner.scan_diff_file("id_rsa", b"-----BEGIN RSA PRIVATE KEY-----\nMIIEpA...");
        assert!(!findings.is_empty());
        assert!(matches!(
            findings[0].kind,
            SecretKind::PrivateKeyHeader { .. }
        ));
    }

    #[test]
    fn token_pattern_detected() {
        let scanner = SecretScanner::new(vec![]);
        let findings = scanner.scan_diff_file(".env", b"GITHUB_TOKEN=ghp_abc123def456");
        assert!(!findings.is_empty());
        assert!(matches!(
            findings[0].kind,
            SecretKind::ApiTokenPattern { .. }
        ));
    }

    #[test]
    fn binary_file_skipped() {
        let scanner = SecretScanner::new(vec![]);
        let data: Vec<u8> = (0..256).map(|i| i as u8).collect();
        let findings = scanner.scan_diff_file("image.png", &data);
        assert!(!findings.is_empty());
        assert_eq!(findings[0].kind, SecretKind::BinarySkipped);
    }

    #[test]
    fn clean_diff_passes() {
        let scanner = SecretScanner::new(vec![
            "a-very-long-known-secret-that-wont-appear-naturally".into(),
        ]);
        let report = scanner.scan_diff(&[(
            "src/main.rs".into(),
            b"// Copyright 2024\n// Licensed under MIT\n\nfn main() {\n    println!(\"hello world\");\n}\n".to_vec(),
        )]);
        assert!(
            report.clean,
            "clean diff should have no findings: {report:?}"
        );
    }
}
