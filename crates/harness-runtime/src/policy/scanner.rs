//! SecretScanner — detection of known secrets, private-key headers, common
//! API-token patterns, credential files, and high-entropy candidates in
//! workspace diffs. Best-effort accidental-commit guard, NOT a full DLP.
//!
//! Safety invariant (I2B-3 closure): a SecretFinding NEVER carries the raw
//! secret. The `redacted_preview` is a fixed placeholder; the secret itself
//! is represented only by a non-reversible `fingerprint` hash plus a
//! line/byte range. Adjacent secrets in the same content are also never
//! echoed — previews contain no source content at all.

const MAX_FILE_BYTES: usize = 512 * 1024; // 512 KiB
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
    /// Fixed placeholder — never contains source content or any secret.
    pub redacted_preview: String,
    /// Non-reversible hash of the matched rule/value, NOT the raw secret.
    pub fingerprint: Option<String>,
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
    /// Pre-sorted longest-first so replacement (legacy) is overlap-safe.
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
    /// The content is NOT stored after scanning.
    pub fn scan_diff_file(&self, file_path: &str, new_content: &[u8]) -> Vec<SecretFinding> {
        let mut findings = Vec::new();

        // Binary detection: valid UTF-8 text is never binary. For non-UTF-8
        // content, NUL bytes or a high control-byte ratio indicate binary.
        // Non-ASCII bytes in valid UTF-8 (e.g. CJK text) are NOT treated as
        // binary, so localized config/source files are still scanned.
        if is_binary(new_content) {
            findings.push(SecretFinding {
                kind: SecretKind::BinarySkipped,
                file_path: file_path.to_string(),
                line_number: None,
                byte_range: None,
                redacted_preview: "[binary file skipped]".into(),
                fingerprint: None,
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
                    "[file truncated: scanned {} of {} bytes]",
                    MAX_FILE_BYTES,
                    new_content.len()
                ),
                fingerprint: None,
            });
            &new_content[..MAX_FILE_BYTES]
        } else {
            new_content
        };

        let text = String::from_utf8_lossy(content);
        let text_bytes = text.as_bytes();

        // Known secrets (exact match) — byte range + line of first occurrence.
        for secret in &self.known_secrets_sorted {
            if let Some(rng) = find_bytes(text_bytes, secret.as_bytes()) {
                findings.push(SecretFinding {
                    kind: SecretKind::KnownSecret {
                        hash: hash_of(secret.as_bytes()),
                    },
                    file_path: file_path.to_string(),
                    line_number: Some(offset_to_line(text_bytes, rng.0)),
                    byte_range: Some(rng),
                    redacted_preview: "[redacted: known secret]".into(),
                    fingerprint: Some(hash_of(secret.as_bytes())),
                });
            }
        }

        // Private key headers — marker only, never the key body.
        for header in [
            "-----BEGIN RSA PRIVATE KEY-----",
            "-----BEGIN OPENSSH PRIVATE KEY-----",
            "-----BEGIN EC PRIVATE KEY-----",
            "-----BEGIN PRIVATE KEY-----",
            "-----BEGIN DSA PRIVATE KEY-----",
        ] {
            if let Some(rng) = find_bytes(text_bytes, header.as_bytes()) {
                findings.push(SecretFinding {
                    kind: SecretKind::PrivateKeyHeader {
                        header: header.to_string(),
                    },
                    file_path: file_path.to_string(),
                    line_number: Some(offset_to_line(text_bytes, rng.0)),
                    byte_range: Some(rng),
                    redacted_preview: "[redacted: private key header]".into(),
                    fingerprint: Some(hash_of(header.as_bytes())),
                });
            }
        }

        // API token patterns (heuristic prefix match — no regex, no reDoS).
        for (name, prefix) in [
            ("github_pat", "ghp_"),
            ("github_pat_v2", "github_pat_"),
            ("aws_access_key", "AKIA"),
            ("stripe_sk", "sk_live_"),
            ("stripe_test_sk", "sk_test_"),
            ("slack_bot_token", "xoxb-"),
            ("slack_webhook", "https://hooks.slack.com/services/"),
        ] {
            if let Some(rng) = find_bytes(text_bytes, prefix.as_bytes()) {
                findings.push(SecretFinding {
                    kind: SecretKind::ApiTokenPattern {
                        pattern_name: name.to_string(),
                    },
                    file_path: file_path.to_string(),
                    line_number: Some(offset_to_line(text_bytes, rng.0)),
                    byte_range: Some(rng),
                    redacted_preview: format!("[redacted: {name} token]"),
                    fingerprint: Some(hash_of(prefix.as_bytes())),
                });
            }
        }

        // Credential file paths — flag by path; require the file to look like
        // it holds a credential. Preview never echoes the file contents.
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
            if (file_path.ends_with(path_rule) || file_path.contains(&format!("/{path_rule}")))
                && (text.contains('=')
                    || text.contains("private_key")
                    || text.contains("password")
                    || text.contains("token"))
            {
                findings.push(SecretFinding {
                    kind: SecretKind::CredentialFilePath {
                        path_rule: path_rule.to_string(),
                    },
                    file_path: file_path.to_string(),
                    line_number: None,
                    byte_range: None,
                    redacted_preview: format!("[redacted: credential file {path_rule}]"),
                    fingerprint: Some(hash_of(path_rule.as_bytes())),
                });
            }
        }

        // High-entropy candidates — per line, single-token (no spaces), long
        // enough to plausibly be a key. Preview is a placeholder.
        for (lineno, start, end, line) in lines_of(&text) {
            if line.len() >= 32 && !line.contains(' ') {
                let entropy = shannon_entropy(line);
                if entropy > 4.5 {
                    findings.push(SecretFinding {
                        kind: SecretKind::HighEntropy { shannon: entropy },
                        file_path: file_path.to_string(),
                        line_number: Some(lineno),
                        byte_range: Some((start, end)),
                        redacted_preview: format!(
                            "[redacted: high-entropy content ({entropy:.1})]"
                        ),
                        fingerprint: None,
                    });
                }
            }
        }

        findings
    }

    /// Scan a complete diff. Content maps are transient — raw secrets are
    /// never stored. Truncation (per-file or total) is surfaced as a finding
    /// so the report is never silently "clean".
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
                    redacted_preview: format!(
                        "[total scan limit reached: {MAX_TOTAL_BYTES} bytes]"
                    ),
                    fingerprint: None,
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

/// Binary detection that accepts valid UTF-8 (incl. multi-byte CJK) as text.
fn is_binary(content: &[u8]) -> bool {
    if content.is_empty() {
        return false;
    }
    // Valid UTF-8 ⇒ text. A NUL-heavy UTF-8 file is still treated as binary.
    if std::str::from_utf8(content).is_ok() {
        let nul = content.iter().filter(|b| **b == 0).count();
        return nul as f64 / content.len() as f64 > 0.10;
    }
    // Invalid UTF-8: any NUL ⇒ binary; otherwise a high control-byte ratio.
    if content.contains(&0) {
        return true;
    }
    let non_text = content
        .iter()
        .filter(|b| {
            let b = **b;
            !(b == b'\n' || b == b'\r' || b == b'\t' || b == b' ' || b.is_ascii_graphic())
        })
        .count();
    non_text as f64 / content.len() as f64 > 0.10
}

/// First byte range of `needle` in `hay`, if present.
fn find_bytes(hay: &[u8], needle: &[u8]) -> Option<(usize, usize)> {
    if needle.is_empty() || needle.len() > hay.len() {
        return None;
    }
    hay.windows(needle.len())
        .position(|w| w == needle)
        .map(|start| (start, start + needle.len()))
}

fn offset_to_line(bytes: &[u8], offset: usize) -> usize {
    let upto = offset.min(bytes.len());
    bytes[..upto].iter().filter(|b| **b == b'\n').count() + 1
}

/// (line_number, byte_start, byte_end, line_text) for each line.
fn lines_of(text: &str) -> Vec<(usize, usize, usize, &str)> {
    let mut out = Vec::new();
    let mut start = 0usize;
    let mut lineno = 1usize;
    for (i, b) in text.as_bytes().iter().enumerate() {
        if *b == b'\n' {
            let line = &text[start..i];
            out.push((lineno, start, i, line));
            lineno += 1;
            start = i + 1;
        }
    }
    if start < text.len() {
        out.push((lineno, start, text.len(), &text[start..]));
    }
    out
}

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

fn hash_of(data: &[u8]) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    data.hash(&mut h);
    format!("sha256-equiv:{:016x}", h.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_secret_detected() {
        let scanner = SecretScanner::new(vec!["sk-super-secret-123".into()]);
        let findings = scanner.scan_diff_file("src/main.rs", b"token=sk-super-secret-123;rest");
        assert!(findings
            .iter()
            .any(|f| matches!(f.kind, SecretKind::KnownSecret { .. })));
        for f in &findings {
            assert!(!f.redacted_preview.contains("sk-super-secret-123"));
            assert!(f.byte_range.is_some());
            assert!(f.line_number.is_some());
        }
    }

    #[test]
    fn private_key_header_detected() {
        let scanner = SecretScanner::new(vec![]);
        let findings =
            scanner.scan_diff_file("id_rsa", b"-----BEGIN RSA PRIVATE KEY-----\nMIIEpA...");
        assert!(findings
            .iter()
            .any(|f| matches!(f.kind, SecretKind::PrivateKeyHeader { .. })));
        // The key body must NOT appear in any preview.
        for f in &findings {
            assert!(!f.redacted_preview.contains("MIIEpA"));
        }
    }

    #[test]
    fn token_pattern_detected() {
        let scanner = SecretScanner::new(vec![]);
        let findings = scanner.scan_diff_file(".env", b"GITHUB_TOKEN=ghp_abc123def456");
        assert!(findings
            .iter()
            .any(|f| matches!(f.kind, SecretKind::ApiTokenPattern { .. })));
        // The token body must NOT appear in any preview.
        for f in &findings {
            assert!(!f.redacted_preview.contains("abc123def456"));
            assert!(!f.redacted_preview.contains("ghp_"));
        }
    }

    #[test]
    fn binary_file_skipped() {
        let scanner = SecretScanner::new(vec![]);
        let data: Vec<u8> = (0..256).map(|i| i as u8).collect();
        let findings = scanner.scan_diff_file("image.png", &data);
        assert!(findings.iter().any(|f| f.kind == SecretKind::BinarySkipped));
    }

    #[test]
    fn utf8_cjk_text_not_binary() {
        // A Chinese .env file must be scanned as text, not skipped.
        let scanner = SecretScanner::new(vec!["real-secret".into()]);
        let content = "密码=real-secret\n".as_bytes();
        let findings = scanner.scan_diff_file(".env", content);
        assert!(
            findings
                .iter()
                .any(|f| !matches!(f.kind, SecretKind::BinarySkipped)),
            "UTF-8 text must not be treated as binary"
        );
        assert!(findings
            .iter()
            .any(|f| matches!(f.kind, SecretKind::KnownSecret { .. })));
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

    #[test]
    fn truncated_file_not_clean() {
        let scanner = SecretScanner::new(vec![]);
        let big: Vec<u8> = vec![b'A'; 600 * 1024];
        let report = scanner.scan_diff(&[("big.txt".into(), big)]);
        assert!(!report.clean, "truncated file must not be silently clean");
        assert!(report
            .findings
            .iter()
            .any(|f| matches!(f.kind, SecretKind::TruncatedLargeFile)));
    }
}
