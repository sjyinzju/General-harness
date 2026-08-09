//! Minimal runner for individual fault injection scenarios (F5-F8).
//! Usage: cargo run --bin run-fault -- F5
#![allow(unused)]

#[path = "../fault_scenario.rs"]
mod fault_scenario;

use std::path::PathBuf;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: run-fault <F5|F6|F7|F8>");
        std::process::exit(1);
    }

    let scenario_id = match args[1].to_uppercase().as_str() {
        "F5" => fault_scenario::FaultScenarioId::F5,
        "F6" => fault_scenario::FaultScenarioId::F6,
        "F7" => fault_scenario::FaultScenarioId::F7,
        "F8" => fault_scenario::FaultScenarioId::F8,
        other => {
            eprintln!("Unknown scenario: {}", other);
            std::process::exit(1);
        }
    };

    let repo_root = std::env::current_dir()?;
    let code_head = {
        let output = std::process::Command::new("git")
            .args(["rev-parse", "HEAD"])
            .current_dir(&repo_root)
            .output()?;
        String::from_utf8(output.stdout)?.trim().to_string()
    };

    // Find the harness binary
    let harness_bin = repo_root
        .join("target")
        .join("debug")
        .join("harness-cli.exe");
    if !harness_bin.exists() {
        eprintln!("harness binary not found at: {}", harness_bin.display());
        std::process::exit(1);
    }

    // Use isolated work directory
    let run_tag = format!(
        "{}-{}",
        scenario_id.as_str(),
        &chrono::Utc::now().format("%Y%m%d-%H%M%S")
    );
    let work_dir = repo_root.join("target").join("fault-runs").join(&run_tag);
    std::fs::create_dir_all(&work_dir)?;

    eprintln!("=== Running {} ===", scenario_id.as_str());
    eprintln!("Code head: {}", code_head);
    eprintln!("Work dir: {}", work_dir.display());
    eprintln!("Harness: {}", harness_bin.display());

    let runner = fault_scenario::FaultScenarioRunner::new(
        repo_root.clone(),
        code_head.clone(),
        harness_bin,
        work_dir.clone(),
    );

    // Build the fault scenario using the standard goal spec
    let goal_id = format!("g-sys-{}-{}", scenario_id.as_str(), uuid::Uuid::new_v4());
    let goal_spec = fault_scenario::make_fault_goal_spec(scenario_id.as_str(), &goal_id);

    let scenario = fault_scenario::FaultScenario {
        id: scenario_id,
        failpoint_name: scenario_id.failpoint_name(),
        description: "Targeted fault injection",
        failpoint_required: true,
        goal_setup: fault_scenario::GoalSetup::ViaStandalone {
            goal_spec_json: goal_spec.to_string(),
        },
        pre_crash_assertions: vec![fault_scenario::Assertion::FailpointHit {
            name: scenario_id.failpoint_name().into(),
        }],
        recovery_expectations: vec![
            fault_scenario::Assertion::GoalRecovered {
                goal_id: goal_id.clone(),
            },
            fault_scenario::Assertion::GoalTerminalState {
                goal_id: goal_id.clone(),
                expected_state: "succeeded".into(),
            },
            fault_scenario::Assertion::SupervisorBReady,
        ],
        duplicate_constraints: vec![
            fault_scenario::DuplicateCheck::GoalCount {
                goal_id: goal_id.clone(),
                expected: 1,
            },
            fault_scenario::DuplicateCheck::TaskCount {
                goal_id: goal_id.clone(),
                max: 1,
            },
            fault_scenario::DuplicateCheck::CommitCount {
                goal_id: goal_id.clone(),
                max: 1,
            },
        ],
        cleanup_constraints: vec![fault_scenario::CleanupCheck::OrphanProcesses { max: 0 }],
    };

    let result = runner.run_scenario(&scenario).await;

    eprintln!("\n=== {} Results ===", scenario_id.as_str());
    eprintln!("passed: {}", result.passed);
    eprintln!("failpoint_hit: {}", result.failpoint_hit);
    eprintln!("goal_recovered: {}", result.goal_recovered);
    eprintln!("goal_terminal_state: {:?}", result.goal_terminal_state);
    eprintln!("token_b_greater: {}", result.token_b_greater);
    eprintln!("duplicates_ok: {}", result.duplicates_ok);
    eprintln!("cleanup_ok: {}", result.cleanup_ok);
    if let Some(ref e) = result.error {
        eprintln!("error: {}", e);
    }
    eprintln!(
        "evidence: {}",
        serde_json::to_string_pretty(&result.evidence)?
    );

    if result.passed {
        println!("PASS");
        Ok(())
    } else {
        eprintln!("FAIL");
        std::process::exit(1)
    }
}
