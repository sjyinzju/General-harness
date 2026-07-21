//! I4.5 Certification Manifest — the single source of truth for what
//! constitutes a passing I4.5 certification.
//!
//! Every scenario, repeat group, fault case, C8 schedule, crash-prefix
//! case, and workspace run is declared here with its required evidence
//! level, repeat count, timeout, expected counters, and forbidden
//! shortcuts.
//!
//! The consistency test at the bottom asserts the exact counts so
//! reports are never hand-counted.

use serde::{Deserialize, Serialize};

// ── Evidence levels ──────────────────────────────────────────────────

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum EvidenceLevel {
    /// Full production I4 dispatch, Agent started, Verification run, Finalization completed.
    RealI4Executed,
    /// Correctly blocked before dispatch by a pre-condition guard (budget, ownership, profile).
    PreDispatchBlockedAsDesigned,
    /// Old Pool/Service destroyed, new Pool/Service created, resume from durable facts only.
    RecoveryFromDurableFacts,
    /// Compiled binary spawned as a subprocess through ProductionGraph.
    BinaryProcessE2E,
    /// Controlled barrier/channel/fault-hook deterministic concurrency test.
    DeterministicConcurrency,
    /// Non-certification component/unit test — excluded from certification matrix.
    ComponentEvidence,
    /// Explicitly excluded from certification evidence.
    NotCertificationEvidence,
}

// ── Manifest entries ─────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScenarioEntry {
    pub id: String,
    pub name: String,
    pub evidence_level: EvidenceLevel,
    /// Rust test target path, e.g. "task_loop_fault_tests::test_gp01_first_attempt_passes"
    pub test_target: String,
    pub repeat_count: u32,
    pub timeout_secs: u64,
    pub expected_classification: Option<String>,
    pub forbidden_shortcuts: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepeatGroupEntry {
    pub id: String,
    pub name: String,
    pub test_target: String,
    pub repeat_count: u32,
    pub timeout_secs: u64,
    pub evidence_level: EvidenceLevel,
    pub expected_counters: Vec<(String, u64)>,
    pub forbidden_shortcuts: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FaultCaseEntry {
    pub id: String,
    pub name: String,
    pub test_target: String,
    pub fault_boundary: String,
    pub fault_kind: String,
    pub timeout_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct C8ScheduleEntry {
    pub id: String,
    pub name: String,
    pub description: String,
    pub test_target: String,
    pub repeat_count: u32,
    pub timeout_secs: u64,
    pub expected_counters: Vec<(String, u64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CrashPrefixEntry {
    pub id: String,
    pub name: String,
    pub crash_point: String,
    pub test_target: String,
    pub repeat_count: u32,
    pub timeout_secs: u64,
    pub expected_counters: Vec<(String, u64)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkspaceRunEntry {
    pub run_number: u32,
    pub command: String,
    pub required_exit_code: i32,
    pub required_failed: u64,
    pub required_ignored: u64,
}

// ── The Manifest ─────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct I45CertificationManifest {
    pub version: String,
    pub scenarios: Vec<ScenarioEntry>,
    pub repeat_groups: Vec<RepeatGroupEntry>,
    pub fault_cases: Vec<FaultCaseEntry>,
    pub c8_schedules: Vec<C8ScheduleEntry>,
    pub crash_prefixes: Vec<CrashPrefixEntry>,
    pub workspace_runs: Vec<WorkspaceRunEntry>,
}

pub fn build_manifest() -> I45CertificationManifest {
    I45CertificationManifest {
        version: "1.0.0".into(),

        // ── 27 Certification Scenarios ───────────────────────────────
        scenarios: vec![
            // gp01-gp16: Golden path scenarios
            ScenarioEntry {
                id: "gp01".into(), name: "First Attempt Passes".into(),
                evidence_level: EvidenceLevel::RealI4Executed,
                test_target: "real_i4_e2e_tests::test_real_i4_first_attempt_pass".into(),
                repeat_count: 1, timeout_secs: 120,
                expected_classification: Some("CompleteCandidate".into()),
                forbidden_shortcuts: vec!["FixtureI4Gateway".into(), "stage_outcome".into()],
            },
            ScenarioEntry {
                id: "gp02".into(), name: "One Repair Then Pass".into(),
                evidence_level: EvidenceLevel::RealI4Executed,
                test_target: "real_i4_e2e_tests::test_real_i4_repair_then_pass".into(),
                repeat_count: 1, timeout_secs: 120,
                expected_classification: Some("CompleteCandidate".into()),
                forbidden_shortcuts: vec!["FixtureI4Gateway".into(), "stage_outcome".into()],
            },
            ScenarioEntry {
                id: "gp03".into(), name: "Progressive Repairs Budget Allows".into(),
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                test_target: "task_loop_fault_tests::test_gp03_progressive_repairs_budget_allows".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: Some("ContinueRepair".into()),
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp04".into(), name: "No Progress Stop".into(),
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                test_target: "task_loop_fault_tests::test_gp04_no_progress_stop".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: Some("NoProgress".into()),
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp05".into(), name: "Cycle Detection".into(),
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                test_target: "task_loop_fault_tests::test_gp05_cycle_detection".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: Some("NoProgress".into()),
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp06".into(), name: "Hard Attempt Budget".into(),
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                test_target: "task_loop_fault_tests::test_gp06_hard_attempt_budget".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: Some("BudgetExhausted".into()),
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp07".into(), name: "Unknown Token Usage".into(),
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                test_target: "task_loop_fault_tests::test_gp07_unknown_token_usage".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: Some("BudgetExhausted".into()),
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp08".into(), name: "Hard Token Budget".into(),
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                test_target: "task_loop_fault_tests::test_gp08_hard_token_budget".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: Some("BudgetExhausted".into()),
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp09".into(), name: "Hard Tool Call Budget".into(),
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                test_target: "task_loop_fault_tests::test_gp09_hard_tool_call_budget".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: Some("BudgetExhausted".into()),
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp10".into(), name: "Hard Cost Budget".into(),
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                test_target: "task_loop_fault_tests::test_gp10_hard_cost_budget".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: Some("BudgetExhausted".into()),
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp11".into(), name: "Infrastructure Blocked".into(),
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                test_target: "task_loop_fault_tests::test_gp11_infrastructure_blocked".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: Some("InfrastructureBlocked".into()),
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp12".into(), name: "Reconciliation Required".into(),
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                test_target: "task_loop_fault_tests::test_gp12_reconciliation_required".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: Some("AwaitingReconciliation".into()),
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp13".into(), name: "Awaiting Human".into(),
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                test_target: "task_loop_fault_tests::test_gp13_awaiting_human".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: Some("AwaitingHuman".into()),
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp14".into(), name: "Project Escalation".into(),
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                test_target: "task_loop_fault_tests::test_gp14_project_escalation".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: Some("EscalateToProjectPlanner".into()),
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp15".into(), name: "Cancellation Classification".into(),
                evidence_level: EvidenceLevel::ComponentEvidence,
                test_target: "task_loop_fault_tests::test_gp15_cancellation_classification".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: Some("Cancelled".into()),
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp16".into(), name: "Cancellation Overrides".into(),
                evidence_level: EvidenceLevel::ComponentEvidence,
                test_target: "task_loop_fault_tests::test_gp16_cancellation_overrides".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: Some("Cancelled".into()),
                forbidden_shortcuts: vec![],
            },
            // gp17-gp22: Response-lost and crash scenarios
            ScenarioEntry {
                id: "gp17".into(), name: "Response Lost After Attempt".into(),
                evidence_level: EvidenceLevel::RecoveryFromDurableFacts,
                test_target: "task_loop_fault_tests::test_fc07_attempt_insert_response_lost".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: None,
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp18".into(), name: "Response Lost After Dispatch".into(),
                evidence_level: EvidenceLevel::RecoveryFromDurableFacts,
                test_target: "task_loop_fault_tests::test_fc17_dispatch_response_lost".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: None,
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp19".into(), name: "Response Lost After Decision".into(),
                evidence_level: EvidenceLevel::RecoveryFromDurableFacts,
                test_target: "task_loop_fault_tests::test_fc21_decision_response_lost".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: None,
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp20".into(), name: "Crash After Outcome Before Decision".into(),
                evidence_level: EvidenceLevel::RecoveryFromDurableFacts,
                test_target: "verification_finalization_recovery::crash_after_outcome_commit_restart_runs_all_steps".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: None,
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp21".into(), name: "Crash After Decision Before Context Pack".into(),
                evidence_level: EvidenceLevel::RecoveryFromDurableFacts,
                test_target: "task_loop_fault_tests::test_fc20_decision_insert_before_effect".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: None,
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp22".into(), name: "Crash After Context Pack Before Attempt".into(),
                evidence_level: EvidenceLevel::RecoveryFromDurableFacts,
                test_target: "task_loop_fault_tests::test_fc22_context_pack_before_effect".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: None,
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp23".into(), name: "Two-Pool Full Controller".into(),
                evidence_level: EvidenceLevel::RealI4Executed,
                test_target: "real_i4_e2e_tests::test_real_i4_two_pool_full_lifecycle".into(),
                repeat_count: 1, timeout_secs: 120,
                expected_classification: None,
                forbidden_shortcuts: vec!["FixtureI4Gateway".into(), "stage_outcome".into()],
            },
            ScenarioEntry {
                id: "gp24".into(), name: "Owner Takeover".into(),
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                test_target: "task_loop_fault_tests::test_owner_takeover_blocks_old_owner".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: None,
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp25".into(), name: "Workspace Continuation".into(),
                evidence_level: EvidenceLevel::RealI4Executed,
                test_target: "real_i4_e2e_tests::test_real_i4_workspace_continuation".into(),
                repeat_count: 1, timeout_secs: 120,
                expected_classification: None,
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp26".into(), name: "Profile Selection All Scenarios".into(),
                evidence_level: EvidenceLevel::ComponentEvidence,
                test_target: "task_loop_fault_tests::test_gp26_profile_selection_all_scenarios".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: None,
                forbidden_shortcuts: vec![],
            },
            ScenarioEntry {
                id: "gp27".into(), name: "Context Security".into(),
                evidence_level: EvidenceLevel::ComponentEvidence,
                test_target: "task_loop_fault_tests::test_gp27_context_security".into(),
                repeat_count: 1, timeout_secs: 30,
                expected_classification: None,
                forbidden_shortcuts: vec![],
            },
        ],

        // ── 18 Repeat Groups ─────────────────────────────────────────
        repeat_groups: vec![
            RepeatGroupEntry {
                id: "rg01".into(), name: "First Attempt Passes".into(),
                test_target: "real_i4_e2e_tests::test_real_i4_first_attempt_pass".into(),
                repeat_count: 20, timeout_secs: 5,
                evidence_level: EvidenceLevel::RealI4Executed,
                expected_counters: vec![("staged_outcome".into(), 0)],
                forbidden_shortcuts: vec!["FixtureI4Gateway".into()],
            },
            RepeatGroupEntry {
                id: "rg02".into(), name: "One Repair Then Pass".into(),
                test_target: "real_i4_e2e_tests::test_real_i4_repair_then_pass".into(),
                repeat_count: 20, timeout_secs: 5,
                evidence_level: EvidenceLevel::RealI4Executed,
                expected_counters: vec![("staged_outcome".into(), 0)],
                forbidden_shortcuts: vec!["FixtureI4Gateway".into()],
            },
            RepeatGroupEntry {
                id: "rg03".into(), name: "Progressive Repairs".into(),
                test_target: "task_loop_fault_tests::test_gp03_progressive_repairs_budget_allows".into(),
                repeat_count: 20, timeout_secs: 5,
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                expected_counters: vec![],
                forbidden_shortcuts: vec![],
            },
            RepeatGroupEntry {
                id: "rg04".into(), name: "No Progress Stop".into(),
                test_target: "task_loop_fault_tests::test_gp04_no_progress_stop".into(),
                repeat_count: 20, timeout_secs: 5,
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                expected_counters: vec![],
                forbidden_shortcuts: vec![],
            },
            RepeatGroupEntry {
                id: "rg05".into(), name: "Two-Pool Full Controller".into(),
                test_target: "real_i4_e2e_tests::test_real_i4_two_pool_full_lifecycle".into(),
                repeat_count: 50, timeout_secs: 10,
                evidence_level: EvidenceLevel::RealI4Executed,
                expected_counters: vec![],
                forbidden_shortcuts: vec!["FixtureI4Gateway".into()],
            },
            RepeatGroupEntry {
                id: "rg06".into(), name: "Two-Pool Attempt Creation".into(),
                test_target: "task_loop_fault_tests::test_repeat_two_pool_attempt_creation_100".into(),
                repeat_count: 100, timeout_secs: 5,
                evidence_level: EvidenceLevel::RealI4Executed,
                expected_counters: vec![],
                forbidden_shortcuts: vec![],
            },
            RepeatGroupEntry {
                id: "rg07".into(), name: "Response-Lost Attempt Creation".into(),
                test_target: "task_loop_fault_tests::test_fc07_attempt_insert_response_lost".into(),
                repeat_count: 20, timeout_secs: 5,
                evidence_level: EvidenceLevel::RecoveryFromDurableFacts,
                expected_counters: vec![("attempt_create".into(), 1)],
                forbidden_shortcuts: vec![],
            },
            RepeatGroupEntry {
                id: "rg08".into(), name: "Response-Lost Dispatch".into(),
                test_target: "task_loop_fault_tests::test_fc17_dispatch_response_lost".into(),
                repeat_count: 20, timeout_secs: 5,
                evidence_level: EvidenceLevel::RecoveryFromDurableFacts,
                expected_counters: vec![("execution_create".into(), 1)],
                forbidden_shortcuts: vec![],
            },
            RepeatGroupEntry {
                id: "rg09".into(), name: "Response-Lost Decision".into(),
                test_target: "task_loop_fault_tests::test_fc21_decision_response_lost".into(),
                repeat_count: 20, timeout_secs: 5,
                evidence_level: EvidenceLevel::RecoveryFromDurableFacts,
                expected_counters: vec![],
                forbidden_shortcuts: vec![],
            },
            RepeatGroupEntry {
                id: "rg10".into(), name: "Decision Exactly-Once".into(),
                test_target: "task_loop_fault_tests::test_fc20_decision_insert_before_effect".into(),
                repeat_count: 20, timeout_secs: 5,
                evidence_level: EvidenceLevel::RecoveryFromDurableFacts,
                expected_counters: vec![("decision".into(), 1)],
                forbidden_shortcuts: vec![],
            },
            RepeatGroupEntry {
                id: "rg11".into(), name: "Context Pack Exactly-Once".into(),
                test_target: "task_loop_fault_tests::test_fc22_context_pack_before_effect".into(),
                repeat_count: 20, timeout_secs: 5,
                evidence_level: EvidenceLevel::RecoveryFromDurableFacts,
                expected_counters: vec![("context_pack".into(), 1)],
                forbidden_shortcuts: vec![],
            },
            RepeatGroupEntry {
                id: "rg12".into(), name: "Budget Reservation Exactly-Once".into(),
                test_target: "task_loop_fault_tests::test_fc08_budget_reservation_before_effect".into(),
                repeat_count: 20, timeout_secs: 5,
                evidence_level: EvidenceLevel::RecoveryFromDurableFacts,
                expected_counters: vec![("budget_reserve".into(), 1)],
                forbidden_shortcuts: vec![],
            },
            RepeatGroupEntry {
                id: "rg13".into(), name: "Usage Exactly-Once".into(),
                test_target: "task_loop_fault_tests::test_fc24_usage_write_before_effect".into(),
                repeat_count: 20, timeout_secs: 5,
                evidence_level: EvidenceLevel::RecoveryFromDurableFacts,
                expected_counters: vec![],
                forbidden_shortcuts: vec![],
            },
            RepeatGroupEntry {
                id: "rg14".into(), name: "Crash/Resume Loop".into(),
                test_target: "real_i4_e2e_tests::test_real_i4_crash_restart".into(),
                repeat_count: 10, timeout_secs: 15,
                evidence_level: EvidenceLevel::RecoveryFromDurableFacts,
                expected_counters: vec![],
                forbidden_shortcuts: vec![],
            },
            RepeatGroupEntry {
                id: "rg15".into(), name: "Workspace Continuation".into(),
                test_target: "real_i4_e2e_tests::test_real_i4_workspace_continuation".into(),
                repeat_count: 10, timeout_secs: 15,
                evidence_level: EvidenceLevel::RealI4Executed,
                expected_counters: vec![],
                forbidden_shortcuts: vec![],
            },
            RepeatGroupEntry {
                id: "rg16".into(), name: "Profile Switch Allowed".into(),
                test_target: "task_loop_i4_integration::test_profile_policy_allows_switch_within_provider".into(),
                repeat_count: 10, timeout_secs: 5,
                evidence_level: EvidenceLevel::ComponentEvidence,
                expected_counters: vec![],
                forbidden_shortcuts: vec![],
            },
            RepeatGroupEntry {
                id: "rg17".into(), name: "Profile Switch Forbidden".into(),
                test_target: "task_loop_i4_integration::test_profile_policy_rejects_cross_provider".into(),
                repeat_count: 10, timeout_secs: 5,
                evidence_level: EvidenceLevel::ComponentEvidence,
                expected_counters: vec![],
                forbidden_shortcuts: vec![],
            },
            RepeatGroupEntry {
                id: "rg18".into(), name: "Stale Ownership Takeover".into(),
                test_target: "task_loop_fault_tests::test_stale_fencing_rejected".into(),
                repeat_count: 50, timeout_secs: 5,
                evidence_level: EvidenceLevel::PreDispatchBlockedAsDesigned,
                expected_counters: vec![],
                forbidden_shortcuts: vec![],
            },
        ],

        // ── 30 Fault Cases ───────────────────────────────────────────
        fault_cases: (1..=30).map(|n| {
            let (name, boundary, kind) = match n {
                1 => ("Loop Insert Before Effect", "LoopInsert", "FailBeforeEffect"),
                2 => ("Loop Insert Response Lost", "LoopInsert", "ResponseLostAfterSuccess"),
                3 => ("Ownership Before Effect", "LoopOwnership", "FailBeforeEffect"),
                4 => ("Ownership Response Lost", "LoopOwnership", "ResponseLostAfterSuccess"),
                5 => ("Stale Takeover Response Lost", "LoopOwnership", "OwnerTakeover"),
                6 => ("Attempt Insert Before Effect", "AttemptInsert", "FailBeforeEffect"),
                7 => ("Attempt Insert Response Lost", "AttemptInsert", "ResponseLostAfterSuccess"),
                8 => ("Budget Reservation Before Effect", "BudgetReservation", "FailBeforeEffect"),
                9 => ("Budget Reservation Response Lost", "BudgetReservation", "ResponseLostAfterSuccess"),
                10 => ("Profile Selection Before Effect", "ProfileSelection", "FailBeforeEffect"),
                11 => ("Profile Selection Response Lost", "ProfileSelection", "ResponseLostAfterSuccess"),
                12 => ("Execution Create Before Effect", "ExecutionCreate", "FailBeforeEffect"),
                13 => ("Execution Create Response Lost", "ExecutionCreate", "ResponseLostAfterSuccess"),
                14 => ("Execution Binding Before Effect", "ExecutionBind", "FailBeforeEffect"),
                15 => ("Execution Binding Response Lost", "ExecutionBind", "ResponseLostAfterSuccess"),
                16 => ("Dispatch Before Effect", "Dispatch", "FailBeforeEffect"),
                17 => ("Dispatch Response Lost", "Dispatch", "ResponseLostAfterSuccess"),
                18 => ("Outcome Observation Failure", "OutcomeObserve", "FailBeforeEffect"),
                19 => ("Dossier Read Failure", "DossierRead", "FailBeforeEffect"),
                20 => ("Decision Insert Before Effect", "DecisionInsert", "FailBeforeEffect"),
                21 => ("Decision Response Lost", "DecisionInsert", "ResponseLostAfterSuccess"),
                22 => ("Context Pack Before Effect", "ContextPackInsert", "FailBeforeEffect"),
                23 => ("Context Pack Response Lost", "ContextPackInsert", "ResponseLostAfterSuccess"),
                24 => ("Usage Write Before Effect", "UsageWrite", "FailBeforeEffect"),
                25 => ("Usage Response Lost", "UsageWrite", "ResponseLostAfterSuccess"),
                26 => ("Workspace Continuation Before Effect", "WorkspaceContinuation", "FailBeforeEffect"),
                27 => ("Workspace Transfer Response Lost", "WorkspaceContinuation", "ResponseLostAfterSuccess"),
                28 => ("Terminal Transition Response Lost", "TerminalTransition", "ResponseLostAfterSuccess"),
                29 => ("Terminal Event Response Lost", "EventWrite", "ResponseLostAfterSuccess"),
                30 => ("Owner Fencing Change Before Effect", "LoopOwnership", "FailBeforeEffect"),
                _ => unreachable!(),
            };
            FaultCaseEntry {
                id: format!("fc{:02}", n),
                name: name.into(),
                test_target: format!("task_loop_fault_tests::test_fc{:02}_{}", n,
                    name.to_lowercase().replace(' ', "_")),
                fault_boundary: boundary.into(),
                fault_kind: kind.into(),
                timeout_secs: 30,
            }
        }).collect(),

        // ── 5 C8 Deterministic Schedules ─────────────────────────────
        c8_schedules: vec![
            C8ScheduleEntry {
                id: "c8s_a".into(), name: "Schedule A — HandoffRelease pause → Worker B resumes".into(),
                description: "Worker A completes HandoffRelease → pause; Worker B observes running → resumes; Worker A disappears".into(),
                test_target: "verification_finalization_recovery::c8_schedule_a_handoff_pause_worker_b_resumes".into(),
                repeat_count: 100, timeout_secs: 15,
                expected_counters: vec![
                    ("ResourcesReleasedEvent".into(), 1),
                    ("OperationCompletion".into(), 1),
                    ("OrphanRunningOperation".into(), 0),
                ],
            },
            C8ScheduleEntry {
                id: "c8s_b".into(), name: "Schedule B — ReleasedEvent inserted → crash → resume".into(),
                description: "ResourcesReleasedEvent inserted → crash before step completion → old Pool destroyed → new Pool resumes".into(),
                test_target: "verification_finalization_recovery::c8_schedule_b_released_event_crash_resume".into(),
                repeat_count: 100, timeout_secs: 15,
                expected_counters: vec![
                    ("ResourcesReleasedEvent".into(), 1),
                    ("OperationCompletion".into(), 1),
                ],
            },
            C8ScheduleEntry {
                id: "c8s_c".into(), name: "Schedule C — ReleasedEvent done → crash before completion".into(),
                description: "ReleasedEvent completed → crash before OperationCompletion → new Pool/Service resumes".into(),
                test_target: "verification_finalization_recovery::c8_schedule_c_released_event_done_crash_before_completion".into(),
                repeat_count: 100, timeout_secs: 15,
                expected_counters: vec![
                    ("OperationCompletion".into(), 1),
                    ("DuplicateEffect".into(), 0),
                ],
            },
            C8ScheduleEntry {
                id: "c8s_d".into(), name: "Schedule D — Old owner/fencing → takeover → old write rejected".into(),
                description: "Old owner/fencing → new Worker takeover → old Worker attempts write → rejected; new Worker completes".into(),
                test_target: "verification_finalization_recovery::c8_schedule_d_old_owner_takeover_old_rejected".into(),
                repeat_count: 100, timeout_secs: 15,
                expected_counters: vec![
                    ("old_worker_rejected".into(), 1),
                    ("new_worker_completed".into(), 1),
                ],
            },
            C8ScheduleEntry {
                id: "c8s_e".into(), name: "Schedule E — Completion success → response lost → retry".into(),
                description: "OperationCompletion succeeds → response lost → retry → all effects strictly 1".into(),
                test_target: "verification_finalization_recovery::c8_schedule_e_completion_response_lost_retry".into(),
                repeat_count: 100, timeout_secs: 15,
                expected_counters: vec![
                    ("ClaimRelease".into(), 1),
                    ("LeaseRelease".into(), 1),
                    ("HeartbeatUnregister".into(), 1),
                    ("HandoffRelease".into(), 1),
                    ("ResourcesReleasedEvent".into(), 1),
                    ("OperationCompletion".into(), 1),
                    ("DuplicateEffect".into(), 0),
                ],
            },
        ],

        // ── 8 Crash Prefix Cases ─────────────────────────────────────
        crash_prefixes: vec![
            CrashPrefixEntry {
                id: "cp01".into(), name: "Before ClaimRelease".into(),
                crash_point: "before ClaimRelease claim".into(),
                test_target: "verification_finalization_recovery::crash_after_outcome_commit_restart_runs_all_steps".into(),
                repeat_count: 50, timeout_secs: 10,
                expected_counters: vec![("ClaimRelease".into(), 1), ("LeaseRelease".into(), 1),
                    ("HeartbeatUnregister".into(), 1), ("HandoffRelease".into(), 1),
                    ("ResourcesReleasedEvent".into(), 1), ("OperationCompletion".into(), 1)],
            },
            CrashPrefixEntry {
                id: "cp02".into(), name: "After Claim claim before effect".into(),
                crash_point: "Claim step claimed, before effect".into(),
                test_target: "verification_finalization_recovery::crash_after_claim_step_claimed_before_effect".into(),
                repeat_count: 50, timeout_secs: 10,
                expected_counters: vec![("ClaimRelease".into(), 1), ("LeaseRelease".into(), 1),
                    ("HeartbeatUnregister".into(), 1), ("HandoffRelease".into(), 1),
                    ("ResourcesReleasedEvent".into(), 1), ("OperationCompletion".into(), 1)],
            },
            CrashPrefixEntry {
                id: "cp03".into(), name: "After Claim effect".into(),
                crash_point: "ClaimRelease effect done".into(),
                test_target: "verification_finalization_recovery::crash_after_claim_effect_restart_skips_claim".into(),
                repeat_count: 50, timeout_secs: 10,
                expected_counters: vec![("ClaimRelease".into(), 0), ("LeaseRelease".into(), 1),
                    ("HeartbeatUnregister".into(), 1), ("HandoffRelease".into(), 1),
                    ("ResourcesReleasedEvent".into(), 1), ("OperationCompletion".into(), 1)],
            },
            CrashPrefixEntry {
                id: "cp04".into(), name: "After Lease effect".into(),
                crash_point: "LeaseRelease effect done".into(),
                test_target: "verification_finalization_recovery::crash_after_lease_effect_restart".into(),
                repeat_count: 50, timeout_secs: 10,
                expected_counters: vec![("ClaimRelease".into(), 0), ("LeaseRelease".into(), 0),
                    ("HeartbeatUnregister".into(), 1), ("HandoffRelease".into(), 1),
                    ("ResourcesReleasedEvent".into(), 1), ("OperationCompletion".into(), 1)],
            },
            CrashPrefixEntry {
                id: "cp05".into(), name: "After Heartbeat effect".into(),
                crash_point: "HeartbeatUnregister effect done".into(),
                test_target: "verification_finalization_recovery::crash_after_heartbeat_effect_restart".into(),
                repeat_count: 50, timeout_secs: 10,
                expected_counters: vec![("ClaimRelease".into(), 0), ("LeaseRelease".into(), 0),
                    ("HeartbeatUnregister".into(), 0), ("HandoffRelease".into(), 1),
                    ("ResourcesReleasedEvent".into(), 1), ("OperationCompletion".into(), 1)],
            },
            CrashPrefixEntry {
                id: "cp06".into(), name: "After Handoff effect".into(),
                crash_point: "HandoffRelease effect done".into(),
                test_target: "verification_finalization_recovery::crash_after_handoff_effect_restart".into(),
                repeat_count: 50, timeout_secs: 10,
                expected_counters: vec![("ClaimRelease".into(), 0), ("LeaseRelease".into(), 0),
                    ("HeartbeatUnregister".into(), 0), ("HandoffRelease".into(), 0),
                    ("ResourcesReleasedEvent".into(), 1), ("OperationCompletion".into(), 1)],
            },
            CrashPrefixEntry {
                id: "cp07".into(), name: "After ReleasedEvent effect".into(),
                crash_point: "ResourcesReleasedEvent done".into(),
                test_target: "verification_finalization_recovery::crash_after_released_event_restart".into(),
                repeat_count: 50, timeout_secs: 10,
                expected_counters: vec![("ClaimRelease".into(), 0), ("LeaseRelease".into(), 0),
                    ("HeartbeatUnregister".into(), 0), ("HandoffRelease".into(), 0),
                    ("ResourcesReleasedEvent".into(), 0), ("OperationCompletion".into(), 1)],
            },
            CrashPrefixEntry {
                id: "cp08".into(), name: "Before OperationCompletion".into(),
                crash_point: "before OperationCompletion claim".into(),
                test_target: "verification_finalization_recovery::crash_before_operation_completion_restart".into(),
                repeat_count: 50, timeout_secs: 10,
                expected_counters: vec![("ClaimRelease".into(), 0), ("LeaseRelease".into(), 0),
                    ("HeartbeatUnregister".into(), 0), ("HandoffRelease".into(), 0),
                    ("ResourcesReleasedEvent".into(), 0), ("OperationCompletion".into(), 1)],
            },
        ],

        // ── 3 Workspace Runs ─────────────────────────────────────────
        workspace_runs: vec![
            WorkspaceRunEntry { run_number: 1, command: "cargo test --workspace".into(),
                required_exit_code: 0, required_failed: 0, required_ignored: 0 },
            WorkspaceRunEntry { run_number: 2, command: "cargo test --workspace".into(),
                required_exit_code: 0, required_failed: 0, required_ignored: 0 },
            WorkspaceRunEntry { run_number: 3, command: "cargo test --workspace".into(),
                required_exit_code: 0, required_failed: 0, required_ignored: 0 },
        ],
    }
}

// ── Consistency Tests ────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn manifest_scenario_count_is_exactly_27() {
        let m = build_manifest();
        assert_eq!(m.scenarios.len(), 27, "scenario count must be exactly 27");
    }

    #[test]
    fn manifest_repeat_group_count_is_exactly_18() {
        let m = build_manifest();
        assert_eq!(
            m.repeat_groups.len(),
            18,
            "repeat group count must be exactly 18"
        );
    }

    #[test]
    fn manifest_fault_case_count_is_exactly_30() {
        let m = build_manifest();
        assert_eq!(
            m.fault_cases.len(),
            30,
            "fault case count must be exactly 30"
        );
    }

    #[test]
    fn manifest_c8_schedule_count_is_exactly_5() {
        let m = build_manifest();
        assert_eq!(
            m.c8_schedules.len(),
            5,
            "C8 schedule count must be exactly 5"
        );
    }

    #[test]
    fn manifest_crash_prefix_count_is_exactly_8() {
        let m = build_manifest();
        assert_eq!(
            m.crash_prefixes.len(),
            8,
            "crash prefix count must be exactly 8"
        );
    }

    #[test]
    fn manifest_workspace_run_count_is_exactly_3() {
        let m = build_manifest();
        assert_eq!(
            m.workspace_runs.len(),
            3,
            "workspace run count must be exactly 3"
        );
    }

    #[test]
    fn manifest_all_scenario_ids_are_unique() {
        let m = build_manifest();
        let mut ids: Vec<&str> = m.scenarios.iter().map(|s| s.id.as_str()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 27, "all 27 scenario IDs must be unique");
    }

    #[test]
    fn manifest_all_repeat_group_ids_are_unique() {
        let m = build_manifest();
        let mut ids: Vec<&str> = m.repeat_groups.iter().map(|r| r.id.as_str()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 18, "all 18 repeat group IDs must be unique");
    }

    #[test]
    fn manifest_all_fault_case_ids_are_unique() {
        let m = build_manifest();
        let mut ids: Vec<&str> = m.fault_cases.iter().map(|f| f.id.as_str()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 30, "all 30 fault case IDs must be unique");
    }

    #[test]
    fn manifest_all_c8_schedule_ids_are_unique() {
        let m = build_manifest();
        let mut ids: Vec<&str> = m.c8_schedules.iter().map(|s| s.id.as_str()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), 5, "all 5 C8 schedule IDs must be unique");
    }

    #[test]
    fn manifest_gp01_and_gp02_are_real_i4_executed() {
        let m = build_manifest();
        for id in &["gp01", "gp02"] {
            let s = m.scenarios.iter().find(|s| s.id == *id).unwrap();
            assert_eq!(
                s.evidence_level,
                EvidenceLevel::RealI4Executed,
                "{id} must be RealI4Executed"
            );
        }
    }

    #[test]
    fn manifest_gp01_gp02_forbid_fixture_gateway() {
        let m = build_manifest();
        for id in &["gp01", "gp02", "gp23"] {
            let s = m.scenarios.iter().find(|s| s.id == *id).unwrap();
            assert!(
                s.forbidden_shortcuts
                    .contains(&"FixtureI4Gateway".to_string()),
                "{id} must forbid FixtureI4Gateway"
            );
            assert!(
                s.forbidden_shortcuts.contains(&"stage_outcome".to_string()),
                "{id} must forbid stage_outcome"
            );
        }
    }

    #[test]
    fn manifest_c8_schedules_each_100_repeats() {
        let m = build_manifest();
        for s in &m.c8_schedules {
            assert_eq!(
                s.repeat_count, 100,
                "C8 schedule {} must have 100 repeats",
                s.id
            );
        }
    }

    #[test]
    fn manifest_crash_prefixes_each_50_repeats() {
        let m = build_manifest();
        for cp in &m.crash_prefixes {
            assert_eq!(
                cp.repeat_count, 50,
                "crash prefix {} must have 50 repeats",
                cp.id
            );
        }
    }

    #[test]
    fn manifest_can_serialize_to_json() {
        let m = build_manifest();
        let json = serde_json::to_string_pretty(&m).unwrap();
        assert!(json.contains("gp01"));
        assert!(json.contains("RealI4Executed"));
        assert!(json.len() > 1000, "manifest JSON must be substantial");
    }
}
