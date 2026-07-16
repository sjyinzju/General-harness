//! Path normalization for resource claims.
//!
//! All path resources use repository-relative logical paths with `/` as the
//! wire separator. Normalization is deterministic and platform-aware:
//!
//! - Unicode NFC normalization (NFD-encoded names cannot bypass rules).
//! - Windows case-insensitivity: paths are lowercased on Windows.
//! - Separator normalization: `\` → `/`.
//! - Component collapsing: `.` components removed, empty components skipped.
//! - `..` components are **rejected** (traversal).
//! - Trailing slashes stripped.
//! - Leading slashes stripped (paths are repo-relative).
//! - Windows reserved device names and ADS (`:`) are rejected.
//! - Empty paths are rejected.

/// A validated, normalized repository-relative path.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct NormalizedResourcePath(String);

impl NormalizedResourcePath {
    /// Normalize a repository-relative path for resource claim use.
    ///
    /// Returns an error string when the path is invalid (traversal, empty,
    /// reserved, etc.).
    pub fn new(raw: &str) -> Result<Self, String> {
        // 1. Normalize separators.
        let n = raw.replace('\\', "/");

        // 2. Unicode NFC normalization.
        let n = unicode_normalization::UnicodeNormalization::nfc(n.chars()).collect::<String>();

        // 3. Windows case folding (no-op on non-Windows for behaviour stability).
        #[cfg(windows)]
        let n = n.to_lowercase();
        // On non-Windows, we still lowercase ASCII for cross-platform determinism
        // of resource claim hashes. Resource claims use repository-relative logical
        // paths; the actual filesystem case is not relevant for conflict detection.
        #[cfg(not(windows))]
        let n = n.to_lowercase();

        // 4. Strip leading/trailing whitespace-adjacent junk, trailing `/`.
        let n = n.trim().trim_end_matches('/').to_string();

        if n.is_empty() {
            return Err("resource path must not be empty".into());
        }

        // 5. Per-component validation.
        let mut components: Vec<&str> = Vec::new();
        for c in n.split('/') {
            if c.is_empty() || c == "." {
                continue; // skip empty and current-dir components
            }
            if c == ".." {
                return Err(format!(
                    "resource path contains '..' traversal component: '{}'",
                    raw
                ));
            }
            // Windows: reject ADS (`:` in any component).
            if c.contains(':') {
                return Err(format!(
                    "resource path component contains ':' (alternate data stream): '{}'",
                    raw
                ));
            }
            // Windows: reject reserved device names.
            if is_windows_reserved_device_name(c) {
                return Err(format!(
                    "resource path component is a reserved device name: '{}'",
                    raw
                ));
            }
            // Reject component that starts or ends with space (ambiguous).
            if c.starts_with(' ') || c.ends_with(' ') {
                return Err(format!(
                    "resource path component has leading/trailing space: '{}'",
                    raw
                ));
            }
            // Reject very long components.
            if c.len() > 255 {
                return Err(format!(
                    "resource path component exceeds 255 characters: '{}'",
                    raw
                ));
            }
            components.push(c);
        }

        if components.is_empty() {
            return Err("resource path resolves to empty path after normalization".into());
        }

        let normalized = components.join("/");
        if normalized.len() > 4096 {
            return Err("resource path exceeds 4096 characters".into());
        }

        Ok(NormalizedResourcePath(normalized))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// The path components (split by `/`).
    pub fn components(&self) -> Vec<&str> {
        self.0.split('/').collect()
    }

    /// Whether `self` is a prefix of `other` using component semantics.
    ///
    /// `src/a` is a prefix of `src/a/b.rs` (component boundary at `/`).
    /// `src/a` is NOT a prefix of `src/ab` (would be substring match, which is wrong).
    pub fn is_component_prefix_of(&self, other: &NormalizedResourcePath) -> bool {
        let self_comps = self.components();
        let other_comps = other.components();
        if self_comps.len() > other_comps.len() {
            return false;
        }
        self_comps == other_comps[..self_comps.len()]
    }

    /// Whether `self` starts with `prefix` using component semantics.
    pub fn starts_with_component_prefix(&self, prefix: &NormalizedResourcePath) -> bool {
        prefix.is_component_prefix_of(self)
    }

    /// The parent directory as a NormalizedResourcePath, or `None` if at root.
    pub fn parent(&self) -> Option<NormalizedResourcePath> {
        let mut comps = self.components();
        comps.pop()?;
        if comps.is_empty() {
            None
        } else {
            Some(NormalizedResourcePath(comps.join("/")))
        }
    }
}

impl std::fmt::Display for NormalizedResourcePath {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.0)
    }
}

/// Windows reserved device/file names (case-insensitive stem).
fn is_windows_reserved_device_name(component: &str) -> bool {
    // Strip trailing dots and spaces (Windows ignores them).
    let stem = component.trim_end_matches(['.', ' ']);
    // Extract the base name before any `.` extension.
    let base = stem.split('.').next().unwrap_or(stem);
    let base_upper = base.to_uppercase();
    matches!(
        base_upper.as_str(),
        "CON"
            | "PRN"
            | "AUX"
            | "NUL"
            | "COM1"
            | "COM2"
            | "COM3"
            | "COM4"
            | "COM5"
            | "COM6"
            | "COM7"
            | "COM8"
            | "COM9"
            | "LPT1"
            | "LPT2"
            | "LPT3"
            | "LPT4"
            | "LPT5"
            | "LPT6"
            | "LPT7"
            | "LPT8"
            | "LPT9"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_normalization() {
        let p = NormalizedResourcePath::new("src/auth/callback.rs").unwrap();
        assert_eq!(p.as_str(), "src/auth/callback.rs");
    }

    #[test]
    fn test_separator_normalization() {
        let p = NormalizedResourcePath::new("src\\auth\\callback.rs").unwrap();
        assert_eq!(p.as_str(), "src/auth/callback.rs");
    }

    #[test]
    fn test_trailing_slash() {
        let p = NormalizedResourcePath::new("src/auth/").unwrap();
        assert_eq!(p.as_str(), "src/auth");
    }

    #[test]
    fn test_dot_component_removed() {
        let p = NormalizedResourcePath::new("src/./auth/callback.rs").unwrap();
        assert_eq!(p.as_str(), "src/auth/callback.rs");
    }

    #[test]
    fn test_empty_rejected() {
        assert!(NormalizedResourcePath::new("").is_err());
        assert!(NormalizedResourcePath::new("   ").is_err());
    }

    #[test]
    fn test_traversal_rejected() {
        assert!(NormalizedResourcePath::new("../outside").is_err());
        assert!(NormalizedResourcePath::new("src/../../outside").is_err());
    }

    #[test]
    fn test_windows_ads_rejected() {
        assert!(NormalizedResourcePath::new("file.txt:stream").is_err());
        assert!(NormalizedResourcePath::new("dir:alt/file.txt").is_err());
    }

    #[test]
    fn test_reserved_device_rejected() {
        assert!(NormalizedResourcePath::new("CON").is_err());
        assert!(NormalizedResourcePath::new("PRN").is_err());
        assert!(NormalizedResourcePath::new("NUL").is_err());
        assert!(NormalizedResourcePath::new("LPT1").is_err());
        assert!(NormalizedResourcePath::new("COM1").is_err());
    }

    #[test]
    fn test_reserved_device_with_extension_rejected() {
        // "CON.txt" is still a reserved device name on Windows.
        assert!(NormalizedResourcePath::new("CON.txt").is_err());
    }

    #[test]
    fn test_reserved_device_trailing_dots() {
        assert!(NormalizedResourcePath::new("CON...").is_err());
    }

    #[test]
    fn test_component_prefix_true() {
        let dir = NormalizedResourcePath::new("src/auth").unwrap();
        let file = NormalizedResourcePath::new("src/auth/callback.rs").unwrap();
        assert!(dir.is_component_prefix_of(&file));
    }

    #[test]
    fn test_component_prefix_false_substring() {
        let dir = NormalizedResourcePath::new("src/a").unwrap();
        let other = NormalizedResourcePath::new("src/ab").unwrap();
        assert!(!dir.is_component_prefix_of(&other));
    }

    #[test]
    fn test_component_prefix_same() {
        let a = NormalizedResourcePath::new("src/auth").unwrap();
        assert!(a.is_component_prefix_of(&a));
    }

    #[test]
    fn test_component_prefix_shorter() {
        let dir = NormalizedResourcePath::new("src/auth/sub").unwrap();
        let file = NormalizedResourcePath::new("src/auth").unwrap();
        assert!(!dir.is_component_prefix_of(&file));
    }

    #[test]
    fn test_parent() {
        let p = NormalizedResourcePath::new("src/auth/callback.rs").unwrap();
        let parent = p.parent().unwrap();
        assert_eq!(parent.as_str(), "src/auth");
        let grandparent = parent.parent().unwrap();
        assert_eq!(grandparent.as_str(), "src");
        let root = grandparent.parent();
        assert!(root.is_none());
    }

    #[test]
    fn test_unicode_normalization() {
        // NFD-encoded 'é' (e + combining acute) → NFC 'é'
        let nfd = "src/re\u{0301}sume\u{0301}.rs"; // NFD
        let p = NormalizedResourcePath::new(nfd).unwrap();
        // After NFC, it should be the precomposed form.
        assert!(p.as_str().contains("résumé") || p.as_str().contains("re\u{00e9}sume\u{00e9}"));
    }

    #[test]
    fn test_windows_case_lowercase() {
        let p = NormalizedResourcePath::new("Src/Auth/Callback.RS").unwrap();
        assert_eq!(p.as_str(), "src/auth/callback.rs");
    }

    #[test]
    fn test_component_leading_trailing_space_rejected() {
        // Space within a component (after path separators) is rejected when
        // it appears at the start of a component.
        assert!(NormalizedResourcePath::new("src/ file.txt").is_err());
        // Multiple leading spaces in a component.
        assert!(NormalizedResourcePath::new("src/  leading").is_err());
    }

    #[test]
    fn test_long_component_rejected() {
        let long = "a".repeat(256);
        assert!(NormalizedResourcePath::new(&format!("src/{long}")).is_err());
    }

    #[test]
    fn test_nested_dirs() {
        let dir = NormalizedResourcePath::new("src/auth").unwrap();
        let sub = NormalizedResourcePath::new("src/auth/sub").unwrap();
        let nested = NormalizedResourcePath::new("src/auth/sub/deep").unwrap();
        assert!(dir.is_component_prefix_of(&sub));
        assert!(sub.is_component_prefix_of(&nested));
        assert!(dir.is_component_prefix_of(&nested));
    }
}
