-- Migration 028: Goal Loop tables (I7.1).
--
-- Tables:
--   goals                    — Durable GoalSpec records
--   goal_revisions           — Immutable Goal revision history
--   goal_success_criteria    — Per-goal success criteria
--   goal_constraints         — Per-goal constraints
--   plan_revisions           — Immutable PlanRevision records
--   plan_milestones          — Milestones within a plan
--   planned_tasks            — Tasks planned by a Planner
--   planned_task_dependencies — DAG edges between planned tasks
--   goal_loop_runs           — GoalLoopRun state machine
--   goal_observations        — Evidence observations
--   goal_progress_assessments — ProgressAssessment results
--   goal_events              — Append-only goal lifecycle events
--   plan_events              — Append-only plan lifecycle events
--   planner_invocations      — Durable Planner/Evaluator invocation records
--   approval_requests        — Human approval requests
--
-- Additive only. Migrations 001–027 frozen.

-- ── Goals ────────────────────────────────────────────────────────────────────

CREATE TABLE goals (
    goal_id TEXT PRIMARY KEY NOT NULL,
    revision INTEGER NOT NULL DEFAULT 1,
    title TEXT NOT NULL,
    objective TEXT NOT NULL,
    repository_id TEXT NOT NULL,
    target_ref TEXT NOT NULL DEFAULT 'refs/heads/main',
    initial_base_head TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'draft'
        CHECK (state IN (
            'draft','validated','planning','active',
            'waiting_for_approval','paused','blocked',
            'succeeded','failed','cancelled')),
    budget_json TEXT NOT NULL DEFAULT '{}',
    approval_policy_json TEXT NOT NULL DEFAULT '{}',
    created_by_json TEXT NOT NULL DEFAULT '{}',
    non_goals_json TEXT NOT NULL DEFAULT '[]',
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT
);

CREATE INDEX idx_goals_state ON goals(state);
CREATE INDEX idx_goals_repository ON goals(repository_id);

-- ── Goal Revisions ───────────────────────────────────────────────────────────

CREATE TABLE goal_revisions (
    goal_revision_id TEXT PRIMARY KEY NOT NULL,
    goal_id TEXT NOT NULL REFERENCES goals(goal_id),
    revision_number INTEGER NOT NULL,
    spec_snapshot_json TEXT NOT NULL DEFAULT '{}',
    spec_digest TEXT NOT NULL,
    created_by TEXT NOT NULL DEFAULT 'system',
    reason TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX idx_goal_revisions_number ON goal_revisions(goal_id, revision_number);

-- ── Goal Success Criteria ────────────────────────────────────────────────────

CREATE TABLE goal_success_criteria (
    criterion_id TEXT PRIMARY KEY NOT NULL,
    goal_id TEXT NOT NULL REFERENCES goals(goal_id),
    description TEXT NOT NULL,
    evidence_policy TEXT NOT NULL DEFAULT 'task_terminal_result',
    evidence_policy_config TEXT NOT NULL DEFAULT '{}',
    verification_policy TEXT NOT NULL DEFAULT 'existence_only',
    verification_policy_config TEXT NOT NULL DEFAULT '{}',
    subjectivity TEXT NOT NULL DEFAULT 'objective'
        CHECK (subjectivity IN ('objective','subjective')),
    required INTEGER NOT NULL DEFAULT 1 CHECK (required IN (0,1)),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_success_criteria_goal ON goal_success_criteria(goal_id);

-- ── Goal Constraints ─────────────────────────────────────────────────────────

CREATE TABLE goal_constraints (
    constraint_id TEXT PRIMARY KEY NOT NULL,
    goal_id TEXT NOT NULL REFERENCES goals(goal_id),
    description TEXT NOT NULL,
    constraint_type TEXT NOT NULL,
    constraint_config TEXT NOT NULL DEFAULT '{}',
    blocking INTEGER NOT NULL DEFAULT 1 CHECK (blocking IN (0,1)),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_goal_constraints_goal ON goal_constraints(goal_id);

-- ── Plan Revisions ───────────────────────────────────────────────────────────

CREATE TABLE plan_revisions (
    plan_revision_id TEXT PRIMARY KEY NOT NULL,
    goal_id TEXT NOT NULL REFERENCES goals(goal_id),
    goal_revision INTEGER NOT NULL,
    revision_number INTEGER NOT NULL,
    base_repository_head TEXT NOT NULL,
    planner_profile_id TEXT NOT NULL DEFAULT '',
    planner_invocation_id TEXT NOT NULL DEFAULT '',
    proposal_digest TEXT NOT NULL DEFAULT '',
    validation_digest TEXT,
    state TEXT NOT NULL DEFAULT 'proposed'
        CHECK (state IN (
            'proposed','validating','validated','active',
            'superseded','completed','rejected','invalid','cancelled')),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    activated_at TEXT,
    superseded_at TEXT
);

CREATE UNIQUE INDEX idx_plan_revisions_number ON plan_revisions(goal_id, revision_number);
CREATE INDEX idx_plan_revisions_state ON plan_revisions(goal_id, state);

-- Partial unique: at most one active plan per goal
CREATE UNIQUE INDEX idx_plan_one_active_per_goal ON plan_revisions(goal_id)
    WHERE state = 'active';

-- ── Plan Milestones ──────────────────────────────────────────────────────────

CREATE TABLE plan_milestones (
    milestone_id TEXT PRIMARY KEY NOT NULL,
    plan_revision_id TEXT NOT NULL REFERENCES plan_revisions(plan_revision_id),
    client_ref TEXT NOT NULL,
    title TEXT NOT NULL,
    objective TEXT NOT NULL,
    success_criteria_refs_json TEXT NOT NULL DEFAULT '[]',
    dependencies_json TEXT NOT NULL DEFAULT '[]',
    priority INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending','in_progress','completed','blocked','cancelled')),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX idx_milestone_client_ref ON plan_milestones(plan_revision_id, client_ref);
CREATE INDEX idx_milestone_plan ON plan_milestones(plan_revision_id);

-- ── Planned Tasks ────────────────────────────────────────────────────────────

CREATE TABLE planned_tasks (
    planned_task_id TEXT PRIMARY KEY NOT NULL,
    plan_revision_id TEXT NOT NULL REFERENCES plan_revisions(plan_revision_id),
    milestone_id TEXT NOT NULL REFERENCES plan_milestones(milestone_id),
    client_ref TEXT NOT NULL,
    title TEXT NOT NULL,
    objective TEXT NOT NULL,
    acceptance_criteria_json TEXT NOT NULL DEFAULT '[]',
    dependency_refs_json TEXT NOT NULL DEFAULT '[]',
    expected_evidence_json TEXT NOT NULL DEFAULT '[]',
    expected_resource_scope_json TEXT NOT NULL DEFAULT '[]',
    risk_level TEXT NOT NULL DEFAULT 'low'
        CHECK (risk_level IN ('low','medium','high','critical')),
    requires_approval INTEGER NOT NULL DEFAULT 0 CHECK (requires_approval IN (0,1)),
    task_fingerprint TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN (
            'pending','materialized','running',
            'completed','failed','cancelled','superseded')),
    materialized_task_id TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX idx_planned_task_client_ref ON planned_tasks(plan_revision_id, client_ref);
CREATE INDEX idx_planned_task_plan ON planned_tasks(plan_revision_id);
CREATE INDEX idx_planned_task_milestone ON planned_tasks(milestone_id);
CREATE INDEX idx_planned_task_fingerprint ON planned_tasks(task_fingerprint);
CREATE INDEX idx_planned_task_materialized ON planned_tasks(materialized_task_id);

-- ── Planned Task Dependencies ────────────────────────────────────────────────

CREATE TABLE planned_task_dependencies (
    planned_task_id TEXT NOT NULL REFERENCES planned_tasks(planned_task_id),
    depends_on_client_ref TEXT NOT NULL,
    PRIMARY KEY (planned_task_id, depends_on_client_ref)
);

-- ── Goal Loop Runs ───────────────────────────────────────────────────────────

CREATE TABLE goal_loop_runs (
    run_id TEXT PRIMARY KEY NOT NULL,
    goal_id TEXT NOT NULL REFERENCES goals(goal_id),
    plan_revision_id TEXT REFERENCES plan_revisions(plan_revision_id),
    state TEXT NOT NULL DEFAULT 'created'
        CHECK (state IN (
            'created','planning','activating_plan','selecting_work',
            'dispatching_tasks','waiting_for_results','collecting_evidence',
            'assessing_progress','replanning','waiting_for_approval',
            'paused','completed','blocked','failed','cancelled')),
    iteration_number INTEGER NOT NULL DEFAULT 0,
    tasks_dispatched_this_run INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    started_at TEXT,
    completed_at TEXT
);

CREATE INDEX idx_goal_loop_runs_goal ON goal_loop_runs(goal_id);
CREATE INDEX idx_goal_loop_runs_state ON goal_loop_runs(state);

-- Partial unique: at most one active run per goal
CREATE UNIQUE INDEX idx_goal_loop_one_active_per_goal ON goal_loop_runs(goal_id)
    WHERE state NOT IN ('completed','failed','cancelled');

-- ── Goal Observations ────────────────────────────────────────────────────────

CREATE TABLE goal_observations (
    observation_id TEXT PRIMARY KEY NOT NULL,
    goal_id TEXT NOT NULL REFERENCES goals(goal_id),
    plan_revision_id TEXT REFERENCES plan_revisions(plan_revision_id),
    planned_task_id TEXT REFERENCES planned_tasks(planned_task_id),
    source_aggregate_type TEXT NOT NULL,
    source_aggregate_id TEXT NOT NULL,
    source_event_id TEXT NOT NULL,
    source_digest TEXT NOT NULL,
    repository_head TEXT NOT NULL DEFAULT '',
    claim TEXT NOT NULL DEFAULT '',
    evidence_type TEXT NOT NULL DEFAULT 'task_result',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX idx_goal_obs_source ON goal_observations(source_aggregate_type, source_aggregate_id, source_event_id);
CREATE INDEX idx_goal_obs_goal ON goal_observations(goal_id);
CREATE INDEX idx_goal_obs_task ON goal_observations(planned_task_id);

-- ── Goal Progress Assessments ────────────────────────────────────────────────

CREATE TABLE goal_progress_assessments (
    assessment_id TEXT PRIMARY KEY NOT NULL,
    goal_id TEXT NOT NULL REFERENCES goals(goal_id),
    plan_revision_id TEXT REFERENCES plan_revisions(plan_revision_id),
    goal_loop_run_id TEXT REFERENCES goal_loop_runs(run_id),
    evaluator_profile_id TEXT NOT NULL DEFAULT '',
    evaluator_invocation_id TEXT NOT NULL DEFAULT '',
    proposed_assessment_json TEXT NOT NULL DEFAULT '{}',
    rust_validation_result_json TEXT NOT NULL DEFAULT '{}',
    completion_recommended INTEGER NOT NULL DEFAULT 0 CHECK (completion_recommended IN (0,1)),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_assessments_goal ON goal_progress_assessments(goal_id);
CREATE INDEX idx_assessments_run ON goal_progress_assessments(goal_loop_run_id);

-- ── Goal Events ──────────────────────────────────────────────────────────────

CREATE TABLE goal_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    goal_id TEXT NOT NULL REFERENCES goals(goal_id),
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    occurred_at TEXT NOT NULL DEFAULT (datetime('now')),
    sequence_num INTEGER NOT NULL
);

CREATE INDEX idx_goal_events_goal ON goal_events(goal_id, sequence_num);

-- ── Plan Events ──────────────────────────────────────────────────────────────

CREATE TABLE plan_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    plan_revision_id TEXT NOT NULL REFERENCES plan_revisions(plan_revision_id),
    goal_id TEXT NOT NULL REFERENCES goals(goal_id),
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    occurred_at TEXT NOT NULL DEFAULT (datetime('now')),
    sequence_num INTEGER NOT NULL
);

CREATE INDEX idx_plan_events_plan ON plan_events(plan_revision_id, sequence_num);

-- ── Planner / Evaluator Invocations ──────────────────────────────────────────

CREATE TABLE planner_invocations (
    invocation_id TEXT PRIMARY KEY NOT NULL,
    goal_id TEXT NOT NULL REFERENCES goals(goal_id),
    plan_revision_id TEXT REFERENCES plan_revisions(plan_revision_id),
    invocation_kind TEXT NOT NULL CHECK (invocation_kind IN ('planner','evaluator')),
    profile_id TEXT NOT NULL,
    idempotency_key TEXT NOT NULL,
    input_digest TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending','running','completed','failed','cancelled')),
    output_digest TEXT,
    output_json TEXT,
    started_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE UNIQUE INDEX idx_planner_invocation_idempotency ON planner_invocations(idempotency_key);
CREATE INDEX idx_planner_invocation_goal ON planner_invocations(goal_id);

-- ── Approval Requests ────────────────────────────────────────────────────────

CREATE TABLE approval_requests (
    approval_id TEXT PRIMARY KEY NOT NULL,
    goal_id TEXT NOT NULL REFERENCES goals(goal_id),
    plan_revision_id TEXT REFERENCES plan_revisions(plan_revision_id),
    approval_type TEXT NOT NULL
        CHECK (approval_type IN (
            'approve_initial_plan','approve_high_risk_task',
            'approve_scope_change','approve_budget_increase',
            'provide_missing_information','approve_goal_completion',
            'approve_resume_after_no_progress')),
    requested_action_json TEXT NOT NULL DEFAULT '{}',
    payload_digest TEXT NOT NULL,
    reason TEXT NOT NULL DEFAULT '',
    state TEXT NOT NULL DEFAULT 'pending'
        CHECK (state IN ('pending','approved','rejected','expired','cancelled')),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    resolved_at TEXT,
    resolved_by TEXT
);

CREATE INDEX idx_approval_goal ON approval_requests(goal_id);
CREATE INDEX idx_approval_state ON approval_requests(state);
