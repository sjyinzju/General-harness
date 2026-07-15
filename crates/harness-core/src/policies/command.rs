/// Blocked command patterns (always denied).
pub const BLOCKED_PATTERNS: &[&str] = &[
    "rm -rf /",
    "git push --force",
    "curl | sh",
    "curl | bash",
    "wget | sh",
    "eval ",
    "> /dev/sda",
];

/// Dangerous patterns requiring approval.
pub const DANGEROUS_PATTERNS: &[&str] = &[
    "rm -rf",
    "sudo ",
    "npm publish",
    "git push",
    "shutdown",
    "reboot",
];

/// Check if a command matches any blocked or dangerous pattern.
pub fn classify_command(cmd: &str) -> CommandClass {
    let lower = cmd.to_lowercase();
    for pat in BLOCKED_PATTERNS {
        if lower.contains(pat) {
            return CommandClass::Blocked;
        }
    }
    for pat in DANGEROUS_PATTERNS {
        if lower.contains(pat) {
            return CommandClass::Dangerous;
        }
    }
    CommandClass::Allowed
}

#[derive(Debug, PartialEq, Eq)]
pub enum CommandClass {
    Allowed,
    Dangerous,
    Blocked,
}
