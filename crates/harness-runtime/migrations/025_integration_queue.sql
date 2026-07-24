-- Migration 025: Durable Integration Queue (I5.2).
--
-- Tables:
--   integration_requests      — enqueued integration requests
--   integration_attempts       — individual integration attempts
--   integration_leases         — lease records for integration workers
--   integration_results        — terminal integration results
--   integration_verifications  — verification command results
--   integration_events         — append-only event log
--
-- Additive only. Migrations 001–024 frozen.

-- ── Integration Request ─────────────────────────────────────────────────

CREATE TABLE integration_requests (
    integration_id TEXT PRIMARY KEY NOT NULL,
    commit_request_id TEXT NOT NULL REFERENCES commit_requests(commit_request_id),
    candidate_id TEXT NOT NULL REFERENCES candidate_snapshots(candidate_id),
    review_id TEXT NOT NULL REFERENCES review_requests(review_id),
    repository_id TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    expected_target_head TEXT NOT NULL,
    priority INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'queued'
        CHECK (state IN (
            'queued','waiting_for_lease','preparing','applying','verifying',
            'ready_to_publish','integrated',
            'conflict','blocked','failed','cancelled','stale')),
    idempotency_key TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT
);

CREATE INDEX idx_integration_request_candidate ON integration_requests(candidate_id);
CREATE INDEX idx_integration_request_state ON integration_requests(state);
CREATE INDEX idx_integration_request_queue
    ON integration_requests(repository_id, target_ref, priority DESC, created_at ASC);

-- Exactly one active request per candidate+repo+target_ref
CREATE UNIQUE INDEX idx_integration_one_active_per_scope
    ON integration_requests(candidate_id, repository_id, target_ref)
    WHERE state NOT IN ('integrated','conflict','blocked','failed','cancelled','stale');

-- ── Integration Attempt ─────────────────────────────────────────────────

CREATE TABLE integration_attempts (
    attempt_id TEXT PRIMARY KEY NOT NULL,
    integration_id TEXT NOT NULL REFERENCES integration_requests(integration_id),
    attempt_number INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'queued'
        CHECK (state IN (
            'queued','waiting_for_lease','preparing','applying','verifying',
            'ready_to_publish','integrated',
            'conflict','blocked','failed','cancelled','stale')),
    commit_oid TEXT NOT NULL,
    parent_oid TEXT NOT NULL,
    target_head_at_start TEXT NOT NULL,
    integration_tree_oid TEXT,
    integration_commit_oid TEXT,
    lease_id TEXT,
    fencing_token INTEGER,
    worktree_path TEXT,
    strategy TEXT CHECK (strategy IN ('fast_forward','cherry_pick','conflict')),
    error_message TEXT,
    started_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_integration_attempt_integration ON integration_attempts(integration_id);

-- ── Integration Lease ───────────────────────────────────────────────────

CREATE TABLE integration_leases (
    lease_id TEXT PRIMARY KEY NOT NULL,
    integration_id TEXT NOT NULL REFERENCES integration_requests(integration_id),
    attempt_id TEXT NOT NULL REFERENCES integration_attempts(attempt_id),
    repository_id TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    lease_token TEXT NOT NULL,
    fencing_token INTEGER NOT NULL DEFAULT 0,
    lifecycle TEXT NOT NULL DEFAULT 'active'
        CHECK (lifecycle IN ('active','released','expired')),
    acquired_at TEXT NOT NULL DEFAULT (datetime('now')),
    heartbeat_at TEXT NOT NULL DEFAULT (datetime('now')),
    expires_at TEXT NOT NULL,
    released_at TEXT,
    version INTEGER NOT NULL DEFAULT 1
);

CREATE INDEX idx_integration_lease_repo_ref
    ON integration_leases(repository_id, target_ref);

-- At most one active lease per (repo, target_ref)
CREATE UNIQUE INDEX idx_integration_lease_one_active_per_scope
    ON integration_leases(repository_id, target_ref)
    WHERE lifecycle = 'active';

-- ── Integration Result ──────────────────────────────────────────────────

CREATE TABLE integration_results (
    integration_id TEXT PRIMARY KEY NOT NULL REFERENCES integration_requests(integration_id),
    attempt_id TEXT NOT NULL REFERENCES integration_attempts(attempt_id),
    state TEXT NOT NULL,
    previous_target_head TEXT NOT NULL,
    new_target_head TEXT,
    commit_oid TEXT NOT NULL,
    strategy TEXT,
    verification_status TEXT,
    conflict_json TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_integration_result_attempt ON integration_results(attempt_id);

-- ── Integration Verification ────────────────────────────────────────────

CREATE TABLE integration_verifications (
    verification_id TEXT PRIMARY KEY NOT NULL,
    attempt_id TEXT NOT NULL REFERENCES integration_attempts(attempt_id),
    command_text TEXT NOT NULL,
    exit_code INTEGER,
    output_truncated INTEGER NOT NULL DEFAULT 0 CHECK (output_truncated IN (0,1)),
    duration_ms INTEGER,
    status TEXT NOT NULL DEFAULT 'pending'
        CHECK (status IN ('pending','running','passed','failed','timeout','error')),
    started_at TEXT,
    completed_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_integration_verification_attempt ON integration_verifications(attempt_id);

-- ── Integration Events (append-only) ────────────────────────────────────

CREATE TABLE integration_events (
    event_id TEXT PRIMARY KEY NOT NULL,
    integration_id TEXT NOT NULL REFERENCES integration_requests(integration_id),
    attempt_id TEXT,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_integration_events_integration ON integration_events(integration_id);
