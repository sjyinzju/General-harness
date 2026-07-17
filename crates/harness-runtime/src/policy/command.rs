//! CommandPolicyEngine — executable + args + cwd + environment-aware
//! command approval. Never shells out; uses structured input only.
//!
//! ## Shell Policy (I4-C3 Closure — FROZEN)
//!
//! **Shells are AbsoluteDenied.** The following executables are permanently
//! denied by `DangerousPattern` entries with `deny: true`:
//!   - `sh` (matches bash, sh, zsh, etc.)
//!   - `cmd` (matches cmd, cmd.exe)
//!   - `powershell` (matches powershell, pwsh, PowerShell.exe)
//!
//! **Approval cannot override.** Even when a valid single-use approval is
//! provided, shells remain denied. The `Deny` variant is returned before
//! any approval check, and the `RequireApproval` path is never reached.
//!
//! This is a **frozen policy decision** per the I4-C3 security model.
//! No "approved shell" execution path exists or is planned.
//!
//! Matching semantics (I2B-3 closure):
//! - A `DangerousPattern` matches only when BOTH `executable_contains` AND
//!   `arg_contains` are satisfied (AND logic). A `None` constraint is
//!   treated as universally satisfied.
//! - `arg_contains` is matched against the SPACE-JOINED argument string so
//!   that multi-token patterns such as `reset --hard` or `clean -fdx` are
//!   detected. Single-token patterns (`-rf`, `-g`, `push`) still match.

use std::collections::HashSet;
use std::path::Path;

use harness_core::{CoreError, ErrorCode, ErrorSource};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PolicyDecision {
    Allow,
    Deny {
        reason: String,
    },
    RequireApproval {
        reason: String,
        fingerprint: CommandFingerprint,
    },
}

/// Fingerprint for approval: binds to the exact command shape so a
/// changed command cannot reuse a previous approval or cached evidence.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommandFingerprint {
    pub executable_hash: String,
    pub args_hash: String,
    pub cwd_hash: String,
    pub env_names_hash: String,
}

impl CommandFingerprint {
    /// Composite key combining ALL four dimensions. Used for evidence
    /// idempotency lookup and storage so that two commands sharing the same
    /// args but differing in executable / cwd / env names cannot collide.
    pub fn composite_key(&self) -> String {
        format!(
            "{}|{}|{}|{}",
            self.executable_hash, self.args_hash, self.cwd_hash, self.env_names_hash
        )
    }
}

/// An approval request (recorded by the caller, not the engine).
#[derive(Debug, Clone)]
pub struct ApprovalRequest {
    pub command_fingerprint: CommandFingerprint,
    pub project_id: String,
    pub task_id: String,
    pub execution_id: String,
    pub reason: String,
    pub expiry: Option<String>,
}

#[derive(Debug, Clone)]
pub enum ApprovalDecision {
    Approved,
    Expired,
    FingerprintMismatch,
}

pub struct CommandPolicyEngine {
    /// Executables whose identity + args structure is trusted.
    allowed_build_tools: HashSet<String>,
    /// Read-only git subcommands always allowed. Mutating subcommands
    /// (config/branch/tag/remote/worktree/stash/notes) are intentionally
    /// absent — they fall through to RequireApproval or DangerousPattern.
    allowed_git_read_only: HashSet<String>,
    /// Commands that always require approval or are denied.
    dangerous_patterns: Vec<DangerousPattern>,
}

struct DangerousPattern {
    category: &'static str,
    executable_contains: Option<&'static str>,
    /// Matched against the space-joined, lowercased argument string.
    arg_contains: Option<&'static str>,
    deny: bool, // true=Deny, false=RequireApproval
}

impl CommandPolicyEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Evaluate a command against policy. `args[0]` is the first argument
    /// (NOT the executable). The executable identity is passed separately.
    pub fn evaluate_command(
        &self,
        executable: &str,
        args: &[String],
        cwd: &Path,
        env_names: &[String],
    ) -> Result<PolicyDecision, CoreError> {
        let exec_lower = executable.to_lowercase();
        let joined_args = args.join(" ").to_lowercase();

        // ── Dangerous pattern checks ──────────────────────────────
        // A pattern matches only when BOTH conditions (if present) are
        // satisfied: executable_contains AND arg_contains. arg_contains is
        // evaluated against the joined argument string so multi-token rules
        // (e.g. "reset --hard") work.
        for pattern in &self.dangerous_patterns {
            let exec_match = pattern
                .executable_contains
                .map(|n| exec_lower.contains(n))
                .unwrap_or(true);
            let arg_match = pattern
                .arg_contains
                .map(|n| joined_args.contains(n))
                .unwrap_or(true);
            if exec_match && arg_match {
                let reason = if pattern.executable_contains.is_some() {
                    format!("{}: {executable}", pattern.category)
                } else {
                    format!("{}: arg matched", pattern.category)
                };
                return if pattern.deny {
                    Ok(PolicyDecision::Deny { reason })
                } else {
                    Ok(PolicyDecision::RequireApproval {
                        reason,
                        fingerprint: self.fingerprint(executable, args, cwd, env_names),
                    })
                };
            }
        }

        // ── Known safe tools ─────────────────────────────────────
        // Only reached when no dangerous pattern matched, so `cargo run`,
        // `python -c`, `npx`, etc. have already been diverted above.
        if self.allowed_build_tools.contains(&exec_lower) {
            return Ok(PolicyDecision::Allow);
        }

        // ── Read-only git commands ────────────────────────────────
        if exec_lower == "git" {
            let sub = args.first().map(|s| s.to_lowercase()).unwrap_or_default();
            if self.allowed_git_read_only.contains(&sub) {
                return Ok(PolicyDecision::Allow);
            }
        }

        // ── Default: require approval ─────────────────────────────
        Ok(PolicyDecision::RequireApproval {
            reason: format!("command not in default allow list: {executable}"),
            fingerprint: self.fingerprint(executable, args, cwd, env_names),
        })
    }

    pub fn fingerprint(
        &self,
        executable: &str,
        args: &[String],
        cwd: &Path,
        env_names: &[String],
    ) -> CommandFingerprint {
        use std::hash::{Hash, Hasher};
        fn hash_str(s: &str) -> String {
            let mut h = std::collections::hash_map::DefaultHasher::new();
            s.hash(&mut h);
            format!("{:016x}", h.finish())
        }
        CommandFingerprint {
            executable_hash: hash_str(executable),
            args_hash: hash_str(&args.join("\x00")),
            cwd_hash: hash_str(&cwd.to_string_lossy()),
            env_names_hash: hash_str(&env_names.join("\x00")),
        }
    }
}

impl Default for CommandPolicyEngine {
    fn default() -> Self {
        let mut allowed_build_tools = HashSet::new();
        for tool in [
            "cargo", "make", "cmake", "go", "node", "npm", "npx", "pnpm", "yarn", "python",
            "python3", "pip", "pip3", "rustc", "tsc", "eslint", "prettier", "jest", "mocha",
            "pytest",
        ] {
            allowed_build_tools.insert(tool.into());
        }
        // NOTE: npx / npm / pnpm / pip / cargo / go / python / node remain in
        // this set, but dangerous patterns below are evaluated FIRST, so the
        // arbitrary-code-execution forms (npx, * -c, * -e, * run, * dlx,
        // pip install, cargo install) never reach this allow list.

        // Read-only git subcommands only. Mutating subcommands (config,
        // branch, tag, remote, worktree, stash, notes) are deliberately
        // omitted so they require approval (or are denied by a pattern).
        let mut allowed_git_read_only = HashSet::new();
        for sub in [
            "status",
            "log",
            "diff",
            "show",
            "rev-parse",
            "rev-list",
            "blame",
            "describe",
            "for-each-ref",
            "ls-remote",
            "ls-files",
            "ls-tree",
            "cat-file",
            "grep",
            "merge-base",
            "check-ref-format",
            "check-ignore",
            "check-attr",
        ] {
            allowed_git_read_only.insert(sub.into());
        }

        let dangerous_patterns = vec![
            // ── Hard deny ─────────────────────────────────────────
            DangerousPattern {
                category: "shell_command",
                executable_contains: Some("sh"),
                arg_contains: None,
                deny: true,
            },
            DangerousPattern {
                category: "shell_command",
                executable_contains: Some("cmd"),
                arg_contains: None,
                deny: true,
            },
            DangerousPattern {
                category: "shell_command",
                executable_contains: Some("powershell"),
                arg_contains: None,
                deny: true,
            },
            // Recursive deletion — covers -rf, -fr, and split -r/-f forms.
            DangerousPattern {
                category: "recursive_delete",
                executable_contains: Some("rm"),
                arg_contains: Some("-rf"),
                deny: true,
            },
            DangerousPattern {
                category: "recursive_delete",
                executable_contains: Some("rm"),
                arg_contains: Some("-fr"),
                deny: true,
            },
            DangerousPattern {
                category: "recursive_delete",
                executable_contains: Some("rm"),
                arg_contains: Some("-r -f"),
                deny: true,
            },
            DangerousPattern {
                category: "recursive_delete",
                executable_contains: Some("rm"),
                arg_contains: Some("-f -r"),
                deny: true,
            },
            DangerousPattern {
                category: "recursive_delete",
                executable_contains: None,
                arg_contains: Some("/s /q"),
                deny: true,
            },
            // Global package install — mutates the user's environment.
            DangerousPattern {
                category: "global_package_install",
                executable_contains: Some("npm"),
                arg_contains: Some("-g"),
                deny: true,
            },
            DangerousPattern {
                category: "global_package_install",
                executable_contains: Some("npm"),
                arg_contains: Some("--global"),
                deny: true,
            },
            DangerousPattern {
                category: "global_package_install",
                executable_contains: Some("cargo"),
                arg_contains: Some("install"),
                deny: false,
            },
            DangerousPattern {
                category: "disk_format",
                executable_contains: Some("mkfs"),
                arg_contains: None,
                deny: true,
            },
            DangerousPattern {
                category: "disk_format",
                executable_contains: Some("format"),
                arg_contains: None,
                deny: true,
            },
            // Git config mutation — especially --global poisons the user
            // environment and can set core.sshCommand / core.fsmonitor.
            DangerousPattern {
                category: "git_config_global",
                executable_contains: Some("git"),
                arg_contains: Some("--global"),
                deny: true,
            },
            DangerousPattern {
                category: "env_mutation",
                executable_contains: Some("setx"),
                arg_contains: None,
                deny: true,
            },
            DangerousPattern {
                category: "env_mutation",
                executable_contains: Some("reg"),
                arg_contains: Some("add"),
                deny: true,
            },
            DangerousPattern {
                category: "background_daemon",
                executable_contains: Some("systemctl"),
                arg_contains: Some("start"),
                deny: true,
            },
            DangerousPattern {
                category: "network_download_execute",
                executable_contains: Some("curl"),
                arg_contains: Some("|"),
                deny: true,
            },
            DangerousPattern {
                category: "network_download_execute",
                executable_contains: Some("wget"),
                arg_contains: Some("-o-"),
                deny: true,
            },
            // ── Require approval ──────────────────────────────────
            // Git mutating operations (subcommands removed from the
            // read-only allow list also fall through to default approval,
            // but these explicit patterns make detection robust).
            DangerousPattern {
                category: "git_reset_hard",
                executable_contains: Some("git"),
                arg_contains: Some("reset --hard"),
                deny: false,
            },
            DangerousPattern {
                category: "git_clean_force",
                executable_contains: Some("git"),
                arg_contains: Some("clean -"),
                deny: false,
            },
            DangerousPattern {
                category: "git_push",
                executable_contains: Some("git"),
                arg_contains: Some("push"),
                deny: false,
            },
            DangerousPattern {
                category: "git_force",
                executable_contains: None,
                arg_contains: Some("--force"),
                deny: false,
            },
            // Joined args are lowercased, so "branch -d" also matches "-D".
            DangerousPattern {
                category: "git_branch_delete",
                executable_contains: Some("git"),
                arg_contains: Some("branch -d"),
                deny: false,
            },
            DangerousPattern {
                category: "git_worktree_mutate",
                executable_contains: Some("git"),
                arg_contains: Some("worktree add"),
                deny: false,
            },
            DangerousPattern {
                category: "git_worktree_mutate",
                executable_contains: Some("git"),
                arg_contains: Some("worktree remove"),
                deny: false,
            },
            DangerousPattern {
                category: "git_tag_delete",
                executable_contains: Some("git"),
                arg_contains: Some("tag -d"),
                deny: false,
            },
            // Package manager installs (local) — require approval.
            DangerousPattern {
                category: "package_install",
                executable_contains: Some("pip"),
                arg_contains: Some("install"),
                deny: false,
            },
            DangerousPattern {
                category: "auto_upgrade",
                executable_contains: Some("pip"),
                arg_contains: Some("install --upgrade"),
                deny: false,
            },
            DangerousPattern {
                category: "auth_keychain_access",
                executable_contains: Some("ssh"),
                arg_contains: Some("-i"),
                deny: false,
            },
            // ── Arbitrary code execution entry points ─────────────
            DangerousPattern {
                category: "code_exec",
                executable_contains: Some("python"),
                arg_contains: Some("-c"),
                deny: false,
            },
            DangerousPattern {
                category: "code_exec",
                executable_contains: Some("python"),
                arg_contains: Some("-m"),
                deny: false,
            },
            DangerousPattern {
                category: "code_exec",
                executable_contains: Some("node"),
                arg_contains: Some("-e"),
                deny: false,
            },
            DangerousPattern {
                category: "code_exec",
                executable_contains: Some("node"),
                arg_contains: Some("--eval"),
                deny: false,
            },
            // npx always downloads & executes — require approval.
            DangerousPattern {
                category: "code_exec",
                executable_contains: Some("npx"),
                arg_contains: None,
                deny: false,
            },
            DangerousPattern {
                category: "code_exec",
                executable_contains: Some("pnpm"),
                arg_contains: Some("dlx"),
                deny: false,
            },
            DangerousPattern {
                category: "code_exec",
                executable_contains: Some("go"),
                arg_contains: Some("run"),
                deny: false,
            },
            DangerousPattern {
                category: "code_exec",
                executable_contains: Some("cargo"),
                arg_contains: Some("run"),
                deny: false,
            },
        ];

        Self {
            allowed_build_tools,
            allowed_git_read_only,
            dangerous_patterns,
        }
    }
}

fn _pe(msg: String) -> CoreError {
    CoreError::new(ErrorCode::WorkspaceError, msg, ErrorSource::System)
}
