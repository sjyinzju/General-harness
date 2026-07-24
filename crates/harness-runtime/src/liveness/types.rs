//! Liveness types — ownership markers, cleanup actions, safety verdicts,
//! and configuration for the unified temporary artifact lifecycle system.
//!
//! These types form the foundation for DeletionGuard, managed directories
//! (harness-temp, harness-evidence, harness-cargo-runs), janitors, and
//! the LivenessOrchestrator.

use std::path::PathBuf;
use std::time::Duration;

/// Schema version for the `.harness-owned.json` marker file.
pub const MARKER_SCHEMA_VERSION: u32 = 1;

/// Filename of the ownership marker written into every managed directory.
pub const OWNERSHIP_MARKER_FILENAME: &str = ".harness-owned.json";

// ── Ownership marker ──────────────────────────────────────────────

/// Kind of a harness-managed directory. Determines which managed root
/// and which cleanup policy applies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum ManagedDirKind {
    /// Runtime temporary files (fixtures, temp repos, IPC, capture buffers).
    HarnessManagedTemp,
    /// Evidence output (results, summaries, logs, command traces).
    HarnessManagedEvidence,
    /// Isolated Cargo target directories for a single run.
    HarnessManagedCargoRun,
}

impl ManagedDirKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::HarnessManagedTemp => "harness-managed-temp",
            Self::HarnessManagedEvidence => "harness-managed-evidence",
            Self::HarnessManagedCargoRun => "harness-managed-cargo-run",
        }
    }

    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "harness-managed-temp" => Some(Self::HarnessManagedTemp),
            "harness-managed-evidence" => Some(Self::HarnessManagedEvidence),
            "harness-managed-cargo-run" => Some(Self::HarnessManagedCargoRun),
            _ => None,
        }
    }
}

/// State of an ownership marker. Drives cleanup eligibility.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MarkerState {
    /// Currently in use by a live run.
    Active,
    /// Run completed successfully.
    Completed,
    /// Run failed.
    Failed,
    /// Run was abandoned (crash / force-kill).
    Abandoned,
}

impl MarkerState {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Active => "active",
            Self::Completed => "completed",
            Self::Failed => "failed",
            Self::Abandoned => "abandoned",
        }
    }
}

/// The `.harness-owned.json` marker written atomically into every
/// managed directory.  Survives a crash and carries enough identity
/// to prove ownership during startup janitor passes.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct OwnershipMarker {
    pub schema_version: u32,
    pub kind: ManagedDirKind,
    pub run_id: String,
    pub owner_pid: u32,
    #[serde(with = "chrono::serde::ts_seconds")]
    pub owner_process_created_at: chrono::DateTime<chrono::Utc>,
    pub created_at: chrono::DateTime<chrono::Utc>,
    pub code_head: String,
    pub state: MarkerState,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub completed_at: Option<chrono::DateTime<chrono::Utc>>,
}

impl OwnershipMarker {
    /// Create a marker for a freshly-created managed directory.
    pub fn new_active(
        kind: ManagedDirKind,
        run_id: String,
        owner_pid: u32,
        code_head: String,
    ) -> Self {
        let now = chrono::Utc::now();
        Self {
            schema_version: MARKER_SCHEMA_VERSION,
            kind,
            run_id,
            owner_pid,
            owner_process_created_at: now,
            created_at: now,
            code_head,
            state: MarkerState::Active,
            completed_at: None,
        }
    }

    /// Mark the directory as completed with the given final state.
    pub fn finalize(mut self, state: MarkerState) -> Self {
        self.state = state;
        self.completed_at = Some(chrono::Utc::now());
        self
    }

    pub fn is_active(&self) -> bool {
        self.state == MarkerState::Active
    }
}

// ── Cleanup action ─────────────────────────────────────────────────

/// What the system decided to do with a managed directory.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CleanupAction {
    /// Left in place (still active, unowned, or safety-blocked).
    Preserve,
    /// Deleted after passing all safety checks.
    Delete,
}

// ── Safety verdict ─────────────────────────────────────────────────

/// Result of DeletionGuard evaluation.  NEVER a bare bool — every
/// denial carries at least one human-readable reason.
#[derive(Debug, Clone)]
pub enum SafetyVerdict {
    Allowed,
    Denied { reasons: Vec<String> },
}

impl SafetyVerdict {
    pub fn is_allowed(&self) -> bool {
        matches!(self, Self::Allowed)
    }

    pub fn deny(reason: impl Into<String>) -> Self {
        Self::Denied {
            reasons: vec![reason.into()],
        }
    }

    pub fn deny_many(reasons: Vec<String>) -> Self {
        Self::Denied { reasons }
    }

    /// Combine two verdicts: if either is Denied, the result is Denied
    /// with all reasons merged.
    pub fn and(self, other: SafetyVerdict) -> Self {
        match (self, other) {
            (Self::Allowed, v) | (v, Self::Allowed) => v,
            (Self::Denied { reasons: mut a }, Self::Denied { reasons: b }) => {
                a.extend(b);
                Self::Denied { reasons: a }
            }
        }
    }
}

// ── Cleanup result ─────────────────────────────────────────────────

/// Aggregate result of a cleanup pass.
#[derive(Debug, Clone, Default)]
pub struct CleanupResult {
    pub examined: usize,
    pub deleted: usize,
    pub preserved: usize,
    pub failed: usize,
    pub reclaimed_bytes: u64,
    /// Per-entry details (path + action + reason).
    pub entries: Vec<CleanupEntry>,
}

#[derive(Debug, Clone)]
pub struct CleanupEntry {
    pub path: PathBuf,
    pub action: CleanupAction,
    /// Human-readable reason when preserved or when deletion fails.
    pub reason: String,
}

impl CleanupResult {
    pub fn merge(&mut self, other: CleanupResult) {
        self.examined += other.examined;
        self.deleted += other.deleted;
        self.preserved += other.preserved;
        self.failed += other.failed;
        self.reclaimed_bytes += other.reclaimed_bytes;
        self.entries.extend(other.entries);
    }
}

// ── Protected path set ─────────────────────────────────────────────

/// Set of paths that must NEVER be deleted by any automated cleanup.
/// Constructed once at startup; shared immutably.
#[derive(Debug, Clone)]
pub struct ProtectedPaths {
    pub repo_root: PathBuf,
    pub target_root: PathBuf,
    pub shared_cargo_target: PathBuf,
    pub user_profile: PathBuf,
    pub system_temp_root: PathBuf,
}

impl ProtectedPaths {
    /// Build from the current environment.
    pub fn detect(repo_root: &std::path::Path) -> Self {
        let target_root = repo_root.join("target");
        let shared_cargo_target = target_root.join("debug");
        let user_profile = std::env::var_os("USERPROFILE")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from("C:\\Users"));
        let system_temp_root = std::env::temp_dir();

        Self {
            repo_root: repo_root.to_path_buf(),
            target_root,
            shared_cargo_target,
            user_profile,
            system_temp_root,
        }
    }

    /// Check whether a canonical path is one of the protected roots
    /// or a direct child that must never be deleted by automation.
    pub fn is_protected(&self, canonical: &std::path::Path) -> bool {
        canonical == self.repo_root
            || canonical == self.target_root
            || canonical == self.shared_cargo_target
            || canonical == self.user_profile
            || canonical == self.system_temp_root
            // Also compare canonicalized versions (Windows \\?\ prefix).
            || self.try_canonical_eq(canonical, &self.repo_root)
            || self.try_canonical_eq(canonical, &self.target_root)
            || self.try_canonical_eq(canonical, &self.shared_cargo_target)
            || self.try_canonical_eq(canonical, &self.user_profile)
            || self.try_canonical_eq(canonical, &self.system_temp_root)
            || canonical.starts_with(&self.user_profile)
                && canonical
                    .components()
                    .count()
                    <= self.user_profile.components().count() + 1
        // system TEMP is allowed for managed children, but the root is protected
    }

    /// Compare two paths by canonicalizing both (handles \\?\ prefix on Windows).
    pub(crate) fn try_canonical_eq(&self, a: &std::path::Path, b: &std::path::Path) -> bool {
        match (a.canonicalize(), b.canonicalize()) {
            (Ok(ca), Ok(cb)) => ca == cb,
            _ => false,
        }
    }

    /// Check if `a` starts with `b`, also trying canonicalized forms.
    fn try_canonical_prefix(&self, a: &std::path::Path, b: &std::path::Path) -> bool {
        a.starts_with(b)
            || match (a.canonicalize(), b.canonicalize()) {
                (Ok(ca), Ok(cb)) => ca.starts_with(&cb),
                _ => false,
            }
    }

    /// Check if a path IS or IS UNDER the shared cargo target (debug/).
    pub fn is_under_shared_cargo(&self, canonical: &std::path::Path) -> bool {
        canonical == self.shared_cargo_target
            || canonical.starts_with(&self.shared_cargo_target)
            || self.try_canonical_prefix(canonical, &self.shared_cargo_target)
    }

    /// Check if a path is the repo's `.git` directory or any path within it.
    pub fn is_git_dir(&self, canonical: &std::path::Path) -> bool {
        let git = self.repo_root.join(".git");
        let git_canonical = git.canonicalize().unwrap_or(git);
        canonical == git_canonical
            || canonical.starts_with(&git_canonical)
            || self.try_canonical_prefix(canonical, &git_canonical)
    }
}

// ── Configuration ──────────────────────────────────────────────────

/// Retention policy for evidence directories.
#[derive(Debug, Clone)]
pub struct EvidenceRetention {
    /// Keep at most this many successful evidence runs.
    pub max_successful: usize,
    /// Keep at most this many failed evidence runs.
    pub max_failed: usize,
}

impl Default for EvidenceRetention {
    fn default() -> Self {
        Self {
            max_successful: 3,
            max_failed: 1,
        }
    }
}

/// Central configuration for the liveness / cleanup subsystem.
#[derive(Debug, Clone)]
pub struct LivenessConfig {
    /// Root for managed temp directories.
    pub managed_temp_root: PathBuf,
    /// Root for managed evidence directories.
    pub managed_evidence_root: PathBuf,
    /// Root for managed isolated Cargo target directories.
    pub managed_cargo_root: PathBuf,

    /// Grace period before a stale-but-owned temp dir is eligible
    /// for reclamation (allows for clock skew / brief restarts).
    pub stale_temp_grace: Duration,

    /// How long a failed/abandoned temp dir is kept before reclamation.
    pub failed_temp_ttl: Duration,

    /// Evidence retention policy.
    pub evidence_retention: EvidenceRetention,

    /// Paths that are absolutely protected from automated deletion.
    pub protected: ProtectedPaths,

    /// Supervisor instance id for the current process.
    pub supervisor_id: String,
}

impl LivenessConfig {
    /// Build a production configuration rooted at `repo_root/target`.
    pub fn for_repo(repo_root: &std::path::Path, supervisor_id: String) -> Self {
        let target = repo_root.join("target");
        Self {
            managed_temp_root: target.join("harness-temp"),
            managed_evidence_root: target.join("harness-evidence"),
            managed_cargo_root: target.join("harness-cargo-runs"),
            stale_temp_grace: Duration::from_secs(30 * 60), // 30 min
            failed_temp_ttl: Duration::from_secs(6 * 3600), // 6 hours
            evidence_retention: EvidenceRetention::default(),
            protected: ProtectedPaths::detect(repo_root),
            supervisor_id,
        }
    }

    /// Build a test configuration rooted at a temp directory.
    pub fn for_test(temp_root: &std::path::Path) -> Self {
        let supervisor_id = format!("test-sup-{}", uuid::Uuid::new_v4());
        Self {
            managed_temp_root: temp_root.join("harness-temp"),
            managed_evidence_root: temp_root.join("harness-evidence"),
            managed_cargo_root: temp_root.join("harness-cargo-runs"),
            stale_temp_grace: Duration::from_secs(1), // fast for tests
            failed_temp_ttl: Duration::from_secs(10), // fast for tests
            evidence_retention: EvidenceRetention::default(),
            protected: ProtectedPaths::detect(temp_root),
            supervisor_id,
        }
    }

    /// Validate that managed roots are not pointing at dangerous locations.
    /// Returns a list of configuration errors; empty = valid.
    pub fn validate(&self) -> Vec<String> {
        let mut errors = Vec::new();

        for (name, root) in [
            ("managed_temp_root", &self.managed_temp_root),
            ("managed_evidence_root", &self.managed_evidence_root),
            ("managed_cargo_root", &self.managed_cargo_root),
        ] {
            // Must be absolute.
            if !root.is_absolute() {
                errors.push(format!("{name} is not absolute: {}", root.display()));
            }
            // Must not be the repo root.
            if root == &self.protected.repo_root {
                errors.push(format!(
                    "{name} points to the repo root: {}",
                    root.display()
                ));
            }
            // Must not be the user profile root.
            if root.starts_with(&self.protected.user_profile)
                && root.components().count() <= self.protected.user_profile.components().count() + 1
            {
                errors.push(format!(
                    "{name} points inside user profile: {}",
                    root.display()
                ));
            }
        }

        errors
    }
}

// ── Helpers ────────────────────────────────────────────────────────

/// Compute the total on-disk size of a directory (bytes).
pub(crate) fn dir_size(path: &std::path::Path) -> u64 {
    walk_size(path)
}

fn walk_size(path: &std::path::Path) -> u64 {
    let mut total: u64 = 0;
    if let Ok(entries) = std::fs::read_dir(path) {
        for entry in entries.flatten() {
            let p = entry.path();
            if p.is_dir() {
                total += walk_size(&p);
            } else if let Ok(meta) = p.metadata() {
                total += meta.len();
            }
        }
    }
    total
}

/// Check whether a PID corresponds to a live process.  On Windows this
/// is a best-effort check; PID reuse is guarded against by also
/// comparing process creation time with the marker.
#[cfg(windows)]
#[allow(unsafe_code)]
pub(crate) fn is_pid_alive(pid: u32) -> bool {
    use std::os::windows::io::RawHandle;

    // STILL_ACTIVE = 259 (0x103)
    const STILL_ACTIVE: u32 = 259;

    unsafe {
        let handle = windows_sys::Win32::System::Threading::OpenProcess(
            windows_sys::Win32::System::Threading::PROCESS_QUERY_LIMITED_INFORMATION,
            0,
            pid,
        );
        if handle.is_null() {
            return false;
        }
        let mut exit_code: u32 = 0;
        let ok = windows_sys::Win32::System::Threading::GetExitCodeProcess(
            handle as RawHandle,
            &mut exit_code,
        );
        windows_sys::Win32::Foundation::CloseHandle(handle);
        ok != 0 && exit_code == STILL_ACTIVE
    }
}

#[cfg(not(windows))]
pub(crate) fn is_pid_alive(pid: u32) -> bool {
    // Unix: sending signal 0 checks existence.
    unsafe { libc::kill(pid as i32, 0) == 0 }
}
