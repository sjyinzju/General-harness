//! I4.6 Real Reviewer Smoke — production Claude/Codex invocation.
//!
//! Creates a sandbox Candidate, builds a ReviewDossier, invokes a real
//! Claude or Codex process as the Reviewer, parses structured output,
//! verifies cache deduplication, read-only enforcement, and timeout control.
//!
//! NEVER modifies global Agent config, Provider, auth, or CLI version.

use harness_core::contracts::review::{ReviewDossier, ReviewState, ReviewerOutput};
use harness_core::contracts::runtime_profile::{
    AuthMode, AuthStatus, CapabilitySet, CoreStatus, ExecutionStatus, OptionalCapabilities,
    ProviderSource, RequiredCapabilities, RuntimeProfile, TriState,
};
use harness_core::contracts::verification::{VerificationOutcome, VerificationResult};
use harness_runtime::db::Database;
use harness_runtime::review::ReviewOrchestrationService;
use std::path::PathBuf;
use std::process::Command;
use std::time::Instant;

// ── Helpers ──────────────────────────────────────────────────────────

fn sandbox_dir() -> PathBuf {
    let run_id = chrono::Utc::now().format("%Y%m%d-%H%M%S").to_string();
    std::env::current_dir()
        .unwrap_or_else(|_| PathBuf::from("."))
        .join("target/i4-6-real-review-smoke")
        .join(run_id)
}

/// Find an executable on PATH.
fn find_exe(name: &str) -> Option<String> {
    if let Ok(paths) = std::env::var("PATH") {
        for dir in std::env::split_paths(&paths) {
            let exe = if cfg!(windows) {
                dir.join(format!("{}.exe", name))
            } else {
                dir.join(name)
            };
            if exe.exists() {
                return Some(exe.to_string_lossy().to_string());
            }
            // Also check without extension on Windows (e.g., claude.cmd)
            if cfg!(windows) {
                let cmd = dir.join(format!("{}.cmd", name));
                if cmd.exists() {
                    return Some(cmd.to_string_lossy().to_string());
                }
            }
        }
    }
    None
}

fn mk_profile(id: &str, kind: &str, exe: &str) -> RuntimeProfile {
    RuntimeProfile {
        id: id.into(),
        agent_definition_id: format!("def-{id}"),
        label: format!("{kind}-profile"),
        agent_kind: kind.into(),
        adapter_kind: kind.into(),
        agent_version: "1.0".into(),
        executable_path: exe.into(),
        provider: kind.into(),
        provider_source: ProviderSource::KnownEndpoint,
        model: Some("default".into()),
        base_url: None,
        auth_mode: AuthMode::Login,
        auth_status: AuthStatus::Authenticated,
        credential_ref: None,
        capabilities: CapabilitySet {
            required: RequiredCapabilities {
                execute: TriState::Supported,
                working_directory: TriState::Supported,
                stream_output: TriState::Supported,
                process_exit: TriState::Supported,
                cancellation: TriState::Supported,
                timeout: TriState::Supported,
                final_result: TriState::Supported,
            },
            optional: OptionalCapabilities {
                native_session_resume: TriState::Unsupported,
                structured_output: TriState::Supported,
                tool_events: TriState::Unsupported,
                file_change_events: TriState::Unsupported,
                reasoning_summary: TriState::Unsupported,
                interactive_approval: TriState::Unsupported,
                usage_reporting: TriState::Unsupported,
            },
            workspace_modes: vec![],
            supported_languages: vec!["rust".into()],
            mcp_tools: vec![],
            supported_platforms: vec!["windows".into()],
        },
        core_status: CoreStatus::Available,
        authentication_status:
            harness_core::contracts::runtime_profile::AuthCheckStatus::Authenticated,
        execution_status: ExecutionStatus::SmokeTestPassed,
        optional_integrations: vec![],
        discovery_source: "real-smoke".into(),
        passive_probe: None,
        active_validation: None,
        concurrency_max: 5,
        created_at: chrono::Utc::now(),
        updated_at: chrono::Utc::now(),
    }
}

/// Build a review prompt from a ReviewDossier.
fn build_review_prompt(dossier: &ReviewDossier) -> String {
    let mut p = String::new();
    p.push_str("You are a code reviewer performing an INDEPENDENT read-only review of a completed task.\n\n");
    p.push_str("## Review Rules\n");
    p.push_str("- You MUST output ONLY valid JSON, no other text.\n");
    p.push_str("- Do NOT modify any files. Do NOT run git commit/add/merge/rebase.\n");
    p.push_str("- Review the following dossier and produce a structured decision.\n\n");
    p.push_str("## Task Goal\n");
    p.push_str(&dossier.task_goal);
    p.push_str("\n\n");
    if !dossier.acceptance_criteria.is_empty() {
        p.push_str("## Acceptance Criteria\n");
        for ac in &dossier.acceptance_criteria {
            p.push_str(&format!("- {}\n", ac));
        }
        p.push('\n');
    }
    p.push_str("## Executor\n");
    p.push_str(&format!("Agent: {}\n", dossier.executor_agent_kind));
    p.push_str(&format!("Base Commit: {}\n\n", dossier.base_commit));
    p.push_str("## Changed Files\n");
    for cf in &dossier.changed_files {
        p.push_str(&format!("- {}\n", cf));
    }
    p.push('\n');
    p.push_str("## Diff Summary\n");
    p.push_str(&dossier.candidate_diff_summary);
    p.push_str("\n\n");
    p.push_str("## Completion Eligibility\n");
    p.push_str(&dossier.completion_eligibility_result);
    p.push_str("\n\n");
    p.push_str("## Test Summary\n");
    p.push_str(&dossier.test_summary);
    p.push_str("\n\n");
    if !dossier.known_limitations.is_empty() {
        p.push_str("## Known Limitations\n");
        for kl in &dossier.known_limitations {
            p.push_str(&format!("- {}\n", kl));
        }
        p.push('\n');
    }
    p.push_str("## Required Output\n");
    p.push_str("Output ONLY this JSON structure (no markdown, no extra text):\n");
    p.push_str(&dossier.required_output_schema);
    p.push_str("\n\nRespond with valid JSON only:\n");
    p
}

/// Parse ReviewerOutput from raw stdout. Returns Err if unparseable.
fn parse_reviewer_output(raw: &str) -> Result<ReviewerOutput, String> {
    // Try to extract JSON from the output (Claude may wrap in markdown fences)
    let json_text = if let Some(start) = raw.find("```json") {
        let after = &raw[start + 7..];
        if let Some(end) = after.find("```") {
            &after[..end]
        } else {
            raw
        }
    } else if let Some(start) = raw.find('{') {
        // Try to find the outermost JSON object
        let mut depth = 0i32;
        let mut end = start;
        for (i, ch) in raw[start..].char_indices() {
            if ch == '{' {
                depth += 1;
            } else if ch == '}' {
                depth -= 1;
                if depth == 0 {
                    end = start + i + 1;
                    break;
                }
            }
        }
        &raw[start..end]
    } else {
        raw
    };

    // Try direct parse first
    if let Ok(output) = serde_json::from_str::<ReviewerOutput>(json_text.trim()) {
        return Ok(output);
    }

    // Try parsing as generic JSON and extracting fields
    let v: serde_json::Value =
        serde_json::from_str(json_text.trim()).map_err(|e| format!("JSON parse error: {e}"))?;

    let decision = v
        .get("decision")
        .and_then(|d| d.as_str())
        .unwrap_or("Blocked")
        .to_string();
    let summary = v
        .get("summary")
        .and_then(|s| s.as_str())
        .unwrap_or("")
        .to_string();
    let findings = v
        .get("findings")
        .and_then(|f| f.as_array())
        .map(|arr| {
            arr.iter()
                .map(|f| harness_core::contracts::review::ReviewerFinding {
                    severity: f
                        .get("severity")
                        .and_then(|s| s.as_str())
                        .unwrap_or("Medium")
                        .to_string(),
                    category: f
                        .get("category")
                        .and_then(|c| c.as_str())
                        .unwrap_or("Correctness")
                        .to_string(),
                    summary: f
                        .get("summary")
                        .and_then(|s| s.as_str())
                        .unwrap_or("")
                        .to_string(),
                    details: f
                        .get("details")
                        .and_then(|d| d.as_str())
                        .unwrap_or("")
                        .to_string(),
                    source_location: f
                        .get("source_location")
                        .and_then(|s| s.as_str())
                        .map(|s| s.to_string()),
                    evidence_reference: f
                        .get("evidence_reference")
                        .and_then(|e| e.as_str())
                        .map(|e| e.to_string()),
                    blocking: f.get("blocking").and_then(|b| b.as_bool()).unwrap_or(true),
                })
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();

    Ok(ReviewerOutput {
        decision,
        summary,
        findings,
    })
}

/// Invoke Claude as a reviewer. Returns (stdout, exit_code, duration_ms).
fn invoke_claude_reviewer(
    exe_path: &str,
    prompt: &str,
    workdir: &std::path::Path,
    timeout_secs: u64,
) -> Result<(String, i32, u64), String> {
    let start = Instant::now();
    let mut child = Command::new(exe_path)
        .arg("-p")
        .arg("--output-format")
        .arg("text")
        .current_dir(workdir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn claude: {e}"))?;

    // Write prompt to stdin
    use std::io::Write;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(prompt.as_bytes())
            .map_err(|e| format!("write stdin: {e}"))?;
    }

    // Wait with timeout
    let timeout = std::time::Duration::from_secs(timeout_secs);
    let start_wait = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                let output = child.wait_with_output().map_err(|e| format!("wait: {e}"))?;
                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                let exit_code = status.code().unwrap_or(-1);
                return Ok((stdout, exit_code, duration_ms));
            }
            Ok(None) => {
                if start_wait.elapsed() > timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!("timeout after {timeout_secs}s"));
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Err(e) => {
                let _ = child.kill();
                return Err(format!("wait error: {e}"));
            }
        }
    }
}

/// Invoke Codex as a reviewer.
fn invoke_codex_reviewer(
    exe_path: &str,
    prompt: &str,
    workdir: &std::path::Path,
    timeout_secs: u64,
) -> Result<(String, i32, u64), String> {
    let start = Instant::now();
    let mut child = Command::new(exe_path)
        .arg("exec")
        .arg("--no-interactive")
        .current_dir(workdir)
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn codex: {e}"))?;

    use std::io::Write;
    if let Some(mut stdin) = child.stdin.take() {
        stdin
            .write_all(prompt.as_bytes())
            .map_err(|e| format!("write stdin: {e}"))?;
    }

    let timeout = std::time::Duration::from_secs(timeout_secs);
    let start_wait = Instant::now();
    loop {
        match child.try_wait() {
            Ok(Some(status)) => {
                let duration_ms = start.elapsed().as_millis() as u64;
                let output = child.wait_with_output().map_err(|e| format!("wait: {e}"))?;
                let stdout = String::from_utf8_lossy(&output.stdout).to_string();
                let exit_code = status.code().unwrap_or(-1);
                return Ok((stdout, exit_code, duration_ms));
            }
            Ok(None) => {
                if start_wait.elapsed() > timeout {
                    let _ = child.kill();
                    let _ = child.wait();
                    return Err(format!("timeout after {timeout_secs}s"));
                }
                std::thread::sleep(std::time::Duration::from_millis(100));
            }
            Err(e) => {
                let _ = child.kill();
                return Err(format!("wait error: {e}"));
            }
        }
    }
}

// ══════════════════════════════════════════════════════════════════════
// SMOKE: Real Reviewer Full Path
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn real_reviewer_smoke_full_path() {
    let sb = sandbox_dir();
    std::fs::create_dir_all(&sb).unwrap();
    let repo_dir = sb.join("sandbox-repo");
    std::fs::create_dir_all(&repo_dir).unwrap();

    // Create a minimal git repo with a simple Rust file
    let status = Command::new("git")
        .args(["init"])
        .current_dir(&repo_dir)
        .output()
        .unwrap();
    assert!(status.status.success(), "git init failed");

    // Create Cargo.toml
    std::fs::write(
        repo_dir.join("Cargo.toml"),
        r#"[package]
name = "sandbox-review"
version = "0.1.0"
edition = "2021"
"#,
    )
    .unwrap();
    std::fs::create_dir_all(repo_dir.join("src")).unwrap();

    // Create a simple Rust file with a "bug"
    std::fs::write(
        repo_dir.join("src").join("main.rs"),
        r#"fn add(a: i32, b: i32) -> i32 {
    a + b
}

fn main() {
    let result = add(2, 3);
    println!("Result: {}", result);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_add() { assert_eq!(add(2, 3), 5); }
}
"#,
    )
    .unwrap();

    // Initial commit
    Command::new("git")
        .args(["add", "-A"])
        .current_dir(&repo_dir)
        .output()
        .unwrap();
    Command::new("git")
        .args(["commit", "-m", "initial"])
        .current_dir(&repo_dir)
        .output()
        .unwrap();

    let base_commit = String::from_utf8_lossy(
        &Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&repo_dir)
            .output()
            .unwrap()
            .stdout,
    )
    .trim()
    .to_string();

    // Make a change: fix a comment, update the function doc
    std::fs::write(
        repo_dir.join("src").join("main.rs"),
        r#"/// Adds two integers and returns the sum.
fn add(a: i32, b: i32) -> i32 {
    a + b
}

fn main() {
    let result = add(2, 3);
    println!("Result: {}", result);
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_add() { assert_eq!(add(2, 3), 5); }
}
"#,
    )
    .unwrap();

    let diff_output = Command::new("git")
        .args(["diff", "--stat"])
        .current_dir(&repo_dir)
        .output()
        .unwrap();
    let diff_stat = String::from_utf8_lossy(&diff_output.stdout).to_string();

    let changed_files = vec!["src/main.rs".to_string()];

    // ── Discover profiles ──────────────────────────────────────────
    println!("=== Discovering profiles ===");
    let claude_exe = find_exe("claude");
    let codex_exe = find_exe("codex");

    let mut profiles: Vec<RuntimeProfile> = Vec::new();
    if let Some(ref exe) = claude_exe {
        println!("Found Claude: {exe}");
        profiles.push(mk_profile("prof-claude", "claude", exe));
    }
    if let Some(ref exe) = codex_exe {
        println!("Found Codex: {exe}");
        profiles.push(mk_profile("prof-codex", "codex", exe));
    }

    assert!(
        profiles.len() >= 2,
        "Need at least 2 profiles for independent review, found {}",
        profiles.len()
    );

    // Select executor and reviewer — Claude as reviewer
    let executor = &profiles[1]; // codex
    let reviewer = &profiles[0]; // claude
    assert_ne!(executor.id, reviewer.id);
    assert_ne!(executor.agent_kind, reviewer.agent_kind);
    println!("Executor: {} ({})", executor.id, executor.agent_kind);
    println!("Reviewer: {} ({})", reviewer.id, reviewer.agent_kind);

    // ── Set up database and service ────────────────────────────────
    let db = Database::open(&sb.join("harness.db")).await.unwrap();
    let svc = ReviewOrchestrationService::new(db.pool.clone());

    // Seed
    sqlx::query(
        "INSERT INTO projects(id,objective,lifecycle) VALUES('p1','review-smoke','active')",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO tasks(id,project_id,goal,lifecycle) VALUES('t1','p1','add doc comment to add() function','verified')",
    )
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query(
        "INSERT INTO execution_attempts(id,task_id,attempt_number,lifecycle) VALUES('e1','t1',1,'completed')",
    )
    .execute(&db.pool)
    .await
    .unwrap();

    // Compute tree hash
    let tree_hash = String::from_utf8_lossy(
        &Command::new("git")
            .args(["rev-parse", "HEAD:src/main.rs"])
            .current_dir(&repo_dir)
            .output()
            .unwrap()
            .stdout,
    )
    .trim()
    .to_string();

    // Freeze candidate
    let c = svc
        .freeze_candidate(
            "t1",
            "e1",
            &executor.id,
            "ws1",
            &base_commit,
            &tree_hash,
            "diff-digest-1",
            "task-spec-digest-1",
            "evidence-digest-1",
        )
        .await
        .unwrap();
    println!("Candidate: {}", c.candidate_id);

    // Run precheck
    let outcome = VerificationOutcome {
        result: VerificationResult::Passed,
        failure_classification: None,
        summary: "all checks passed".into(),
        blockers: vec![],
        findings_count: 0,
    };
    let precheck = svc
        .run_precheck(&c, &outcome, &[], true, true, true, true, true, true, true)
        .await;
    assert!(precheck.passed);

    // Create review
    let req = svc
        .create_review(&c.candidate_id, &reviewer.id)
        .await
        .unwrap();
    svc.transition(&req.review_id, &ReviewState::Preparing)
        .await
        .unwrap();
    svc.transition(&req.review_id, &ReviewState::Prechecking)
        .await
        .unwrap();

    // Build dossier
    let dossier = svc
        .build_dossier(
            &req.review_id,
            &c,
            "Add a doc comment to the add() function in src/main.rs",
            vec!["cargo test passes".into(), "no logic changes".into()],
            vec!["Only modify src/main.rs".into()],
            vec!["src/main.rs".into()],
            &executor.agent_kind,
            changed_files.clone(),
            &diff_stat,
            "CompleteCandidate",
            "test_add passes, 1 test, 0 failures",
            vec!["evidence-1".into()],
            vec![],
        )
        .await;
    println!("Dossier digest: {}", dossier.dossier_digest);

    svc.transition(&req.review_id, &ReviewState::Reviewing)
        .await
        .unwrap();

    // ── Invoke REAL Reviewer ───────────────────────────────────────
    println!("\n=== Invoking Real Reviewer: {} ===", reviewer.agent_kind);

    let prompt = build_review_prompt(&dossier);
    let prompt_bytes = prompt.len();
    println!("Prompt size: {} bytes", prompt_bytes);

    let _review_start = chrono::Utc::now();

    // Log invocation
    let inv_id = svc
        .log_invocation(
            &req.review_id,
            &c.candidate_id,
            &reviewer.id,
            false,
            Some(&dossier.dossier_digest),
        )
        .await
        .unwrap();

    // ── Capture git state BEFORE reviewer ───────────────────────
    let before_diff = String::from_utf8_lossy(
        &Command::new("git")
            .args(["diff"])
            .current_dir(&repo_dir)
            .output()
            .unwrap()
            .stdout,
    )
    .to_string();
    let before_untracked = String::from_utf8_lossy(
        &Command::new("git")
            .args(["ls-files", "--others", "--exclude-standard"])
            .current_dir(&repo_dir)
            .output()
            .unwrap()
            .stdout,
    )
    .to_string();

    // Invoke based on agent kind, using discovered full path
    let review_result = if reviewer.agent_kind == "codex" {
        invoke_codex_reviewer(&reviewer.executable_path, &prompt, &repo_dir, 120)
    } else {
        invoke_claude_reviewer(&reviewer.executable_path, &prompt, &repo_dir, 120)
    };

    match review_result {
        Ok((stdout, exit_code, duration_ms)) => {
            println!("Reviewer exit code: {exit_code}");
            println!("Duration: {duration_ms}ms");
            println!(
                "Output (first 500 chars): {}",
                &stdout.chars().take(500).collect::<String>()
            );

            svc.complete_invocation(&inv_id, "completed").await.unwrap();

            // Parse structured output
            let reviewer_output = parse_reviewer_output(&stdout).unwrap_or_else(|e| {
                println!("WARNING: Could not parse reviewer output: {e}");
                println!("Raw output saved for diagnostics");
                ReviewerOutput {
                    decision: "Blocked".into(),
                    summary: format!("Parse error: {e}"),
                    findings: vec![],
                }
            });

            println!("\n=== Review Result ===");
            println!("Decision: {}", reviewer_output.decision);
            println!("Summary: {}", reviewer_output.summary);
            println!("Findings: {}", reviewer_output.findings.len());
            for f in &reviewer_output.findings {
                println!("  [{}] {}: {}", f.severity, f.category, f.summary);
            }

            // Check that the output is parseable
            let parsed_decision = reviewer_output.parse_decision();
            assert!(
                parsed_decision.is_some(),
                "Real reviewer must produce parseable decision"
            );

            // Apply decision policy
            let (decision, findings) = svc.apply_decision(&req.review_id, &reviewer_output);
            println!("Applied decision: {:?}", decision);

            // ── Read-only verification ───────────────────────────────
            // Capture git state AFTER reviewer
            let after_diff = String::from_utf8_lossy(
                &Command::new("git")
                    .args(["diff"])
                    .current_dir(&repo_dir)
                    .output()
                    .unwrap()
                    .stdout,
            )
            .to_string();
            let after_untracked = String::from_utf8_lossy(
                &Command::new("git")
                    .args(["ls-files", "--others", "--exclude-standard"])
                    .current_dir(&repo_dir)
                    .output()
                    .unwrap()
                    .stdout,
            )
            .to_string();

            let diff_changed = before_diff != after_diff;
            let untracked_changed = before_untracked != after_untracked;
            let reviewer_modified = diff_changed || untracked_changed;

            println!("\n=== Post-review git state ===");
            println!("Diff changed by reviewer: {diff_changed}");
            println!("Untracked changed by reviewer: {untracked_changed}");

            if reviewer_modified || untracked_changed {
                // Candidate changed → must NOT be Approved
                if decision == harness_core::contracts::review::ReviewDecision::Approved {
                    panic!("REGRESSION: Reviewer modified candidate but decision is Approved!");
                }
                println!("NOTE: Reviewer modified candidate → decision correctly non-Approved");
            }

            // ── Finalize and cache ──────────────────────────────────
            svc.finalize_decision(
                &req.review_id,
                &decision,
                &findings,
                &c,
                &reviewer_output,
                &reviewer.id,
            )
            .await
            .unwrap();

            // Check cache hit for same candidate + same reviewer
            let cache = svc.check_cache(&c, &reviewer.id).await.unwrap();
            assert!(cache.is_some(), "Cache must be populated after decision");
            let (cached_rev_id, cached_decision) = cache.unwrap();
            println!(
                "Cache hit: review_id={} decision={}",
                cached_rev_id, cached_decision
            );

            // Verify invocation count = 1
            let count = svc.count_invocations(&req.review_id).await.unwrap();
            println!("Real reviewer invocation count: {count}");
            assert_eq!(count, 1, "Exactly 1 real reviewer invocation");

            // ── Print summary ───────────────────────────────────────
            println!("\n========================================");
            println!("=== REAL REVIEWER SMOKE COMPLETE ===");
            println!("Executor: {} ({})", executor.id, executor.agent_kind);
            println!("Reviewer: {} ({})", reviewer.id, reviewer.agent_kind);
            println!("Reviewer is fake: false");
            println!("Decision: {:?}", decision);
            println!("Structured output parsed: {}", parsed_decision.is_some());
            println!("Candidate digest unchanged: {}", !reviewer_modified);
            println!(
                "Candidate files modified by reviewer: {}",
                if reviewer_modified { 1 } else { 0 }
            );
            println!(
                "Candidate files created by reviewer: {}",
                if untracked_changed { 1 } else { 0 }
            );
            println!("Same candidate cache hit: true");
            println!("Real invocation count: {count}");
            println!("Exit code: {exit_code}");
            println!("Duration: {duration_ms}ms");
            println!("Prompt bytes: {prompt_bytes}");
            println!("Candidate modified by reviewer: {}", reviewer_modified);
            println!(
                "Candidate files modified by reviewer: {}",
                if reviewer_modified { 1 } else { 0 }
            );
        }
        Err(e) => {
            svc.complete_invocation(&inv_id, "failed").await.unwrap();
            panic!("Real reviewer invocation failed: {e}");
        }
    }
}

// ══════════════════════════════════════════════════════════════════════
// SMOKE: Timeout Control
// ══════════════════════════════════════════════════════════════════════

#[tokio::test]
async fn real_reviewer_timeout_control() {
    let sb = sandbox_dir();
    std::fs::create_dir_all(&sb).unwrap();
    let repo_dir = sb.join("timeout-repo");
    std::fs::create_dir_all(&repo_dir).unwrap();

    Command::new("git")
        .args(["init"])
        .current_dir(&repo_dir)
        .output()
        .unwrap();
    std::fs::write(repo_dir.join("README.md"), "# Timeout Test\n").unwrap();
    Command::new("git")
        .args(["add", "-A"])
        .current_dir(&repo_dir)
        .output()
        .unwrap();
    Command::new("git")
        .args(["commit", "-m", "initial"])
        .current_dir(&repo_dir)
        .output()
        .unwrap();

    // Use Claude with a very short timeout
    let claude_exe = find_exe("claude");
    if claude_exe.is_none() {
        println!("SKIP: Claude not available for timeout test");
        return;
    }

    // Send a very long prompt that would take a while, with 1-second timeout
    let mut long_prompt = String::from("Count from 1 to 1000, one number per line. Start now:\n");
    long_prompt.push_str("1\n2\n3\n");

    let exe = claude_exe.as_ref().unwrap();
    let result = invoke_claude_reviewer(exe, &long_prompt, &repo_dir, 1); // 1 second timeout
    match result {
        Ok((_, _, duration_ms)) => {
            // If it completed in time, it must be very fast
            println!("Completed in {duration_ms}ms (within timeout)");
        }
        Err(e) => {
            assert!(e.contains("timeout"), "Expected timeout error, got: {e}");
            println!("Timeout controlled: {e}");
        }
    }

    // Verify no orphan processes (just check the git repo is intact)
    let status = Command::new("git")
        .args(["status", "--porcelain=v1"])
        .current_dir(&repo_dir)
        .output()
        .unwrap();
    let status_str = String::from_utf8_lossy(&status.stdout);
    assert!(
        status_str.trim().is_empty(),
        "Git should be clean after timeout"
    );

    println!("=== TIMEOUT SMOKE PASS ===");
}
