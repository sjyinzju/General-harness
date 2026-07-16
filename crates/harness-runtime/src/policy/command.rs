//! CommandPolicyEngine — executable + args + cwd + environment-aware
//! command approval. Never shells out; uses structured input only.

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
/// changed command cannot reuse a previous approval.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CommandFingerprint {
    pub executable_hash: String,
    pub args_hash: String,
    pub cwd_hash: String,
    pub env_names_hash: String,
}

/// An approval decision (recorded by the caller, not the engine).
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
    /// Read-only git subcommands always allowed.
    allowed_git_read_only: HashSet<String>,
    /// Commands that always require approval.
    dangerous_patterns: Vec<DangerousPattern>,
}

struct DangerousPattern {
    category: &'static str,
    executable_contains: Option<&'static str>,
    arg_contains: Option<&'static str>,
    deny: bool, // true=Deny, false=RequireApproval
}

impl CommandPolicyEngine {
    pub fn new() -> Self {
        Self::default()
    }

    /// Evaluate a command against policy. `args[0]` is the executable
    /// name/identity (not the full path to `git`/`npm` etc).
    pub fn evaluate_command(
        &self,
        executable: &str,
        args: &[String],
        cwd: &Path,
        env_names: &[String],
    ) -> Result<PolicyDecision, CoreError> {
        let exec_lower = executable.to_lowercase();

        // ── Dangerous pattern checks ──────────────────────────────
        // A pattern matches only when BOTH conditions (if present) are
        // satisfied: executable_contains AND arg_contains.
        for pattern in &self.dangerous_patterns {
            let exec_match = pattern
                .executable_contains
                .map(|n| exec_lower.contains(n))
                .unwrap_or(true); // no exec constraint → always matches
            let arg_match = pattern
                .arg_contains
                .map(|n| args.iter().any(|a| a.to_lowercase().contains(n)))
                .unwrap_or(true); // no arg constraint → always matches
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

        let mut allowed_git_read_only = HashSet::new();
        for sub in [
            "status",
            "log",
            "diff",
            "show",
            "rev-parse",
            "rev-list",
            "branch",
            "tag",
            "remote",
            "ls-remote",
            "worktree",
            "stash",
            "blame",
            "describe",
            "for-each-ref",
            "config",
            "ls-files",
            "ls-tree",
            "cat-file",
            "grep",
            "merge-base",
            "check-ref-format",
            "check-ignore",
            "check-attr",
            "notes",
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
                executable_contains: Some("cmd.exe"),
                arg_contains: None,
                deny: true,
            },
            DangerousPattern {
                category: "shell_command",
                executable_contains: Some("powershell"),
                arg_contains: None,
                deny: true,
            },
            DangerousPattern {
                category: "shell_command",
                executable_contains: Some("bash"),
                arg_contains: None,
                deny: true,
            },
            DangerousPattern {
                category: "recursive_delete",
                executable_contains: None,
                arg_contains: Some("-rf"),
                deny: true,
            },
            DangerousPattern {
                category: "recursive_delete",
                executable_contains: None,
                arg_contains: Some("/s /q"),
                deny: true,
            },
            DangerousPattern {
                category: "global_package_install",
                executable_contains: Some("npm"),
                arg_contains: Some("-g"),
                deny: true,
            },
            DangerousPattern {
                category: "global_package_install",
                executable_contains: Some("pip"),
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
            // ── Require approval ──────────────────────────────────
            DangerousPattern {
                category: "git_reset_hard",
                executable_contains: None,
                arg_contains: Some("reset --hard"),
                deny: false,
            },
            DangerousPattern {
                category: "git_push",
                executable_contains: Some("git"),
                arg_contains: Some("push"),
                deny: false,
            },
            DangerousPattern {
                category: "git_force_push",
                executable_contains: None,
                arg_contains: Some("--force"),
                deny: false,
            },
            DangerousPattern {
                category: "git_clean_force",
                executable_contains: None,
                arg_contains: Some("clean -"),
                deny: false,
            },
            DangerousPattern {
                category: "auto_upgrade",
                executable_contains: Some("pip"),
                arg_contains: Some("install --upgrade pip"),
                deny: false,
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
                category: "auth_keychain_access",
                executable_contains: Some("ssh"),
                arg_contains: Some("-i"),
                deny: false,
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
                arg_contains: Some("-O-"),
                deny: true,
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
