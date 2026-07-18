-- Migration 021: Task Engineering Loop persistence (I4.5 Batch 1).
--
-- Additive only. Migrations 001–020 frozen. No FSM changes to Gate C.
-- I4.5 overlays a Task-level loop state machine on top of the existing
-- Task / Execution / Verification model; it does NOT alter those tables.
--
-- Six new tables:
--   task_engineering_loops       — one active loop per Task
--   task_engineering_attempts    — immutable Attempt lineage
--   task_attempt_decisions       — immutable per-Attempt decisions
--   task_context_packs           — immutable repair context
--   task_usage_ledger            — per-Attempt usage records
--   task_loop_operations         — formal Operation authority

-- ── Loop ─────────────────────────────────────────────────────────────

CREATE TABLE task_engineering_loops (
    loop_id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id),
    task_id TEXT NOT NULL REFERENCES tasks(id),
    lifecycle TEXT NOT NULL DEFAULT 'created'
        CHECK (lifecycle IN (
            'created','ready','preparing_attempt','attempt_active',
            'evaluating','complete_candidate','waiting_for_reconciliation',
            'waiting_for_infrastructure','waiting_for_human',
            'budget_exhausted','no_progress','non_retryable',
            'escalated','cancelled','reconciliation_required','failed')),
    policy_json TEXT NOT NULL DEFAULT '{}',
    policy_fingerprint TEXT,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    owner_id TEXT,
    fencing_token INTEGER NOT NULL DEFAULT 1,
    lease_expires_at TEXT,
    active_attempt_id TEXT,
    current_attempt_ordinal INTEGER NOT NULL DEFAULT 0,
    attempt_count INTEGER NOT NULL DEFAULT 0,
    no_progress_streak INTEGER NOT NULL DEFAULT 0,
    same_failure_streak INTEGER NOT NULL DEFAULT 0,
    profile_switch_count INTEGER NOT NULL DEFAULT 0,
    started_at TEXT,
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    terminal_at TEXT,
    last_error_classification TEXT,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE UNIQUE INDEX idx_loop_one_active_per_task
    ON task_engineering_loops(task_id)
    WHERE lifecycle NOT IN (
        'complete_candidate','budget_exhausted','no_progress',
        'non_retryable','escalated','cancelled','failed');

CREATE INDEX idx_loop_project ON task_engineering_loops(project_id);
CREATE INDEX idx_loop_lifecycle ON task_engineering_loops(lifecycle);

-- ── Attempt ──────────────────────────────────────────────────────────

CREATE TABLE task_engineering_attempts (
    attempt_id TEXT PRIMARY KEY NOT NULL,
    loop_id TEXT NOT NULL REFERENCES task_engineering_loops(loop_id),
    ordinal INTEGER NOT NULL,
    parent_attempt_id TEXT REFERENCES task_engineering_attempts(attempt_id),
    execution_id TEXT UNIQUE REFERENCES execution_attempts(id),
    verification_run_id TEXT,
    context_pack_id TEXT,
    runtime_profile_id TEXT NOT NULL DEFAULT '',
    workspace_source_kind TEXT NOT NULL DEFAULT 'initial'
        CHECK (workspace_source_kind IN ('initial','continue_from_attempt')),
    source_execution_id TEXT,
    source_worktree_id TEXT,
    source_baseline_commit TEXT,
    source_head TEXT,
    source_diff_fingerprint TEXT,
    lifecycle TEXT NOT NULL DEFAULT 'created'
        CHECK (lifecycle IN (
            'created','prepared','dispatched','executing',
            'terminal','cancelled','failed')),
    outcome_kind TEXT,
    outcome_fingerprint TEXT,
    dossier_fingerprint TEXT,
    decision_id TEXT,
    started_at TEXT,
    terminal_at TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    UNIQUE(loop_id, ordinal)
);

CREATE UNIQUE INDEX idx_attempt_one_active_per_loop
    ON task_engineering_attempts(loop_id)
    WHERE lifecycle NOT IN ('terminal','cancelled','failed');

CREATE INDEX idx_attempt_execution ON task_engineering_attempts(execution_id);
CREATE INDEX idx_attempt_loop ON task_engineering_attempts(loop_id);

-- ── Decision ─────────────────────────────────────────────────────────

CREATE TABLE task_attempt_decisions (
    decision_id TEXT PRIMARY KEY NOT NULL,
    loop_id TEXT NOT NULL REFERENCES task_engineering_loops(loop_id),
    attempt_id TEXT NOT NULL REFERENCES task_engineering_attempts(attempt_id),
    classification TEXT NOT NULL
        CHECK (classification IN (
            'CompleteCandidate','ContinueRepair',
            'AwaitingReconciliation','InfrastructureBlocked',
            'AwaitingHuman','BudgetExhausted','NoProgress',
            'NonRetryable','Cancelled','EscalateToProjectPlanner')),
    action TEXT NOT NULL DEFAULT 'none'
        CHECK (action IN (
            'none','create_attempt','wait_reconciliation',
            'wait_infrastructure','wait_human','stop','escalate')),
    reason_codes_json TEXT NOT NULL DEFAULT '[]',
    observed_state_fingerprint TEXT,
    outcome_fingerprint TEXT,
    dossier_fingerprint TEXT,
    progress_fingerprint TEXT,
    budget_snapshot_fingerprint TEXT,
    selected_profile_id TEXT,
    next_context_pack_id TEXT,
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_decision_attempt ON task_attempt_decisions(attempt_id);
CREATE INDEX idx_decision_loop ON task_attempt_decisions(loop_id);

-- ── Context Pack ─────────────────────────────────────────────────────

CREATE TABLE task_context_packs (
    context_pack_id TEXT PRIMARY KEY NOT NULL,
    loop_id TEXT NOT NULL REFERENCES task_engineering_loops(loop_id),
    source_attempt_id TEXT REFERENCES task_engineering_attempts(attempt_id),
    target_attempt_ordinal INTEGER NOT NULL,
    schema_version INTEGER NOT NULL DEFAULT 1,
    payload_json TEXT NOT NULL DEFAULT '{}',
    source_fingerprints_json TEXT NOT NULL DEFAULT '{}',
    context_fingerprint TEXT NOT NULL,
    estimated_input_tokens INTEGER,
    validation_status TEXT NOT NULL DEFAULT 'pending'
        CHECK (validation_status IN ('pending','valid','rejected')),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_context_pack_loop ON task_context_packs(loop_id);

-- ── Usage Ledger ─────────────────────────────────────────────────────

CREATE TABLE task_usage_ledger (
    usage_id TEXT PRIMARY KEY NOT NULL,
    loop_id TEXT NOT NULL REFERENCES task_engineering_loops(loop_id),
    attempt_id TEXT NOT NULL REFERENCES task_engineering_attempts(attempt_id),
    execution_id TEXT REFERENCES execution_attempts(id),
    runtime_profile_id TEXT NOT NULL DEFAULT '',
    model_identifier TEXT,
    provider_identifier TEXT,
    input_tokens INTEGER,
    output_tokens INTEGER,
    cached_input_tokens INTEGER,
    tool_calls INTEGER,
    wall_time_ms INTEGER,
    estimated_cost_micros INTEGER,
    usage_source TEXT NOT NULL DEFAULT 'unknown'
        CHECK (usage_source IN ('provider_reported','estimated','unknown')),
    usage_known INTEGER NOT NULL DEFAULT 0 CHECK (usage_known IN (0,1)),
    usage_fingerprint TEXT,
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_usage_attempt ON task_usage_ledger(attempt_id);
CREATE INDEX idx_usage_loop ON task_usage_ledger(loop_id);

-- ── Loop Operations ──────────────────────────────────────────────────

CREATE TABLE task_loop_operations (
    operation_id TEXT PRIMARY KEY NOT NULL,
    loop_id TEXT NOT NULL REFERENCES task_engineering_loops(loop_id),
    operation_kind TEXT NOT NULL
        CHECK (operation_kind IN (
            'create_loop','acquire_loop_ownership','prepare_attempt',
            'create_execution','dispatch_attempt','observe_attempt_outcome',
            'record_decision','create_context_pack','advance_loop',
            'cancel_loop','reconcile_loop')),
    idempotency_key TEXT NOT NULL UNIQUE,
    request_hash TEXT NOT NULL,
    observed_state_fingerprint TEXT,
    lifecycle TEXT NOT NULL DEFAULT 'running'
        CHECK (lifecycle IN ('running','completed','failed','blocked')),
    owner_id TEXT,
    fencing_token INTEGER,
    result_fingerprint TEXT,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    terminal_at TEXT,
    last_error_classification TEXT,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX idx_loop_op_loop ON task_loop_operations(loop_id);
