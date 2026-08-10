//! I8B regression tests — supervisor bootstrap safety nets.
//!
//! These tests verify the three production fixes that close the I8B
//! runtime closure:
//!
//! 1. `path_inside_git_worktree` — correctly detects git repositories.
//! 2. `safe_worktree_root` — never returns a path inside a git worktree.
//! 3. `read_child_stderr` — captures crash diagnostics from a piped child.
//! 4. Early-exit detection pattern — `try_wait` + `read_child_stderr`
//!    surfaces the real crash reason instead of a 15s timeout.

use std::process::{Command, Stdio};

// ── 1. path_inside_git_worktree ──────────────────────────────────────

#[test]
fn path_inside_git_worktree_detects_git_repo() {
    // Create a temp directory, git-init it, then check a subdirectory.
    let tmp = std::env::temp_dir().join(format!("harness-test-git-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&tmp).expect("create temp dir");

    let _ = Command::new("git")
        .args(["init", "."])
        .current_dir(&tmp)
        .output();

    let subdir = tmp.join("target").join("tmp");
    std::fs::create_dir_all(&subdir).expect("create subdir");

    assert!(
        super::path_inside_git_worktree(&subdir),
        "subdirectory of a git repo should be detected as inside a worktree"
    );

    let _ = std::fs::remove_dir_all(&tmp);
}

#[test]
fn path_inside_git_worktree_false_for_non_git_dir() {
    // Create a directory and do NOT git-init it.  If any ancestor happens
    // to be a git repo, skip the assertion rather than fail spuriously.
    let non_git = std::env::temp_dir().join(format!("harness-test-nogit-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&non_git).expect("create temp dir");

    let parent = non_git.parent().expect("temp dir has a parent");
    if !super::path_inside_git_worktree(parent) {
        assert!(
            !super::path_inside_git_worktree(&non_git),
            "directory outside any git repo should not be detected as inside a worktree"
        );
    }

    let _ = std::fs::remove_dir_all(&non_git);
}

// ── 2. safe_worktree_root ────────────────────────────────────────────

#[test]
fn safe_worktree_root_returns_default_when_not_in_git() {
    let non_git = std::env::temp_dir().join(format!("harness-test-safe-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&non_git).expect("create temp dir");

    let parent = non_git.parent().expect("temp dir has a parent");
    if super::path_inside_git_worktree(parent) {
        // Environment has a git ancestor — skip, can't verify the
        // "not in git" path.
        let _ = std::fs::remove_dir_all(&non_git);
        return;
    }

    let result = super::safe_worktree_root(Some(non_git.to_str().unwrap()));
    assert!(result.is_some(), "should return a worktree root");

    let root = result.unwrap();
    let root_path = std::path::Path::new(&root);
    assert!(
        root_path.ends_with("tmp"),
        "default worktree root should end with target/tmp, got: {root}"
    );
    assert!(
        !super::path_inside_git_worktree(root_path),
        "safe_worktree_root must never return a path inside a git worktree"
    );

    let _ = std::fs::remove_dir_all(&non_git);
}

#[test]
fn safe_worktree_root_avoids_git_worktree_for_git_repo() {
    // Reproduce the exact I8B bug scenario: repo_root is a git repo,
    // so repo_root/target/tmp would be rejected by WorktreeManager.
    let git_repo = std::env::temp_dir().join(format!("harness-test-bug-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&git_repo).expect("create temp dir");

    let _ = Command::new("git")
        .args(["init", "."])
        .current_dir(&git_repo)
        .output();

    // Precondition: the default path IS inside the git worktree.
    let default = git_repo.join("target").join("tmp");
    assert!(
        super::path_inside_git_worktree(&default),
        "default worktree root should be inside the git worktree (precondition)"
    );

    // safe_worktree_root must return a path OUTSIDE the git worktree.
    let result = super::safe_worktree_root(Some(git_repo.to_str().unwrap()));
    assert!(result.is_some(), "should return a worktree root");

    let root = result.unwrap();
    assert_ne!(
        root,
        default.to_string_lossy().to_string(),
        "should not return the default path when it's inside a git worktree"
    );
    assert!(
        !super::path_inside_git_worktree(std::path::Path::new(&root)),
        "safe_worktree_root must return a path outside any git worktree, got: {root}"
    );

    let _ = std::fs::remove_dir_all(&git_repo);
}

// ── 3. read_child_stderr ─────────────────────────────────────────────

#[test]
fn read_child_stderr_captures_output() {
    // Spawn a process that writes to stderr and exits with a non-zero code.
    let mut cmd = if cfg!(windows) {
        let mut c = Command::new("cmd");
        c.args(["/c", "echo crash-reason-here 1>&2 && exit /b 42"]);
        c
    } else {
        let mut c = Command::new("sh");
        c.args(["-c", "echo crash-reason-here >&2; exit 42"]);
        c
    };
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("spawn test process");
    let status = child.wait().expect("wait for child");
    assert_eq!(status.code(), Some(42), "child should exit with code 42");

    let stderr = super::read_child_stderr(&mut child, 4096);
    assert!(stderr.is_some(), "stderr should be captured");
    let text = stderr.unwrap();
    assert!(
        text.contains("crash-reason-here"),
        "stderr should contain the crash message, got: {text}"
    );
}

#[test]
fn read_child_stderr_returns_none_when_no_output() {
    let mut cmd = if cfg!(windows) {
        let mut c = Command::new("cmd");
        c.args(["/c", "exit /b 0"]);
        c
    } else {
        let mut c = Command::new("sh");
        c.args(["-c", "true"]);
        c
    };
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("spawn test process");
    child.wait().expect("wait for child");

    let stderr = super::read_child_stderr(&mut child, 4096);
    assert!(
        stderr.is_none(),
        "stderr should be None when no output was produced"
    );
}

#[test]
fn read_child_stderr_truncates_to_max_bytes() {
    // Use a message that fits in the OS pipe buffer (4096 on Windows)
    // to avoid a deadlock between the child writing and `wait()`.
    let long_msg = "X".repeat(500);
    let mut cmd = if cfg!(windows) {
        let mut c = Command::new("cmd");
        c.args(["/c", &format!("echo {} 1>&2", long_msg)]);
        c
    } else {
        let mut c = Command::new("sh");
        c.args(["-c", &format!("echo {} >&2", long_msg)]);
        c
    };
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("spawn test process");
    child.wait().expect("wait for child");

    let stderr = super::read_child_stderr(&mut child, 100);
    assert!(stderr.is_some(), "stderr should be captured");
    let text = stderr.unwrap();
    assert!(
        text.len() <= 100,
        "stderr should be truncated to max_bytes, got {} bytes: {text}",
        text.len()
    );
}

// ── 4. Early-exit detection pattern (try_wait) ───────────────────────

#[test]
fn try_wait_detects_immediate_exit() {
    // Spawn a process that exits immediately with a non-zero code.
    let mut cmd = if cfg!(windows) {
        let mut c = Command::new("cmd");
        c.args(["/c", "exit /b 99"]);
        c
    } else {
        let mut c = Command::new("sh");
        c.args(["-c", "exit 99"]);
        c
    };
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("spawn test process");

    // Give the process time to exit.
    std::thread::sleep(std::time::Duration::from_millis(100));

    let result = child.try_wait();
    assert!(result.is_ok(), "try_wait should not error");
    let status = result.unwrap();
    assert!(status.is_some(), "child should have exited");
    assert_eq!(
        status.unwrap().code(),
        Some(99),
        "child should exit with code 99"
    );
}

#[test]
fn try_wait_returns_none_for_running_process() {
    // Spawn a long-running process.
    let mut cmd = if cfg!(windows) {
        let mut c = Command::new("cmd");
        c.args(["/c", "ping -n 30 127.0.0.1 > nul"]);
        c
    } else {
        let mut c = Command::new("sh");
        c.args(["-c", "sleep 30"]);
        c
    };
    cmd.stdout(Stdio::null());
    cmd.stderr(Stdio::piped());

    let mut child = cmd.spawn().expect("spawn test process");

    let result = child.try_wait();
    assert!(result.is_ok(), "try_wait should not error");
    assert!(result.unwrap().is_none(), "child should still be running");

    // Clean up.
    let _ = child.kill();
    let _ = child.wait();
}
