-- 030: I8A Human Interaction Protocol
--
-- Additive schema for the durable interaction domain:
--  1. approval_requests gains the user's response payload, the originating
--     IPC request id, and the request source (system | user | ipc).
--  2. user_interventions: user→harness messages that do not block progress
--     by themselves; consumed by future planning iterations.
--  3. UNIQUE (goal_id, sequence_num) on goal_events backs the atomic
--     single-statement sequence allocation in append_goal_event.

-- ── 1. approval_requests extensions ─────────────────────────────────────────

ALTER TABLE approval_requests ADD COLUMN response_json TEXT;
ALTER TABLE approval_requests ADD COLUMN request_id TEXT;
ALTER TABLE approval_requests ADD COLUMN source TEXT NOT NULL DEFAULT 'system';

-- ── 2. User Interventions ────────────────────────────────────────────────────

CREATE TABLE user_interventions (
    intervention_id TEXT PRIMARY KEY NOT NULL,
    goal_id TEXT NOT NULL REFERENCES goals(goal_id),
    request_id TEXT,
    source TEXT NOT NULL DEFAULT 'ipc',
    message TEXT NOT NULL,
    classification TEXT NOT NULL DEFAULT 'constraint_addition'
        CHECK (classification IN (
            'informational','constraint_addition','plan_change_required',
            'pause_requested','cancel_requested')),
    state TEXT NOT NULL DEFAULT 'received'
        CHECK (state IN ('received','applied','superseded')),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    processed_at TEXT,
    applied_plan_revision_id TEXT REFERENCES plan_revisions(plan_revision_id)
);

CREATE INDEX idx_user_interventions_goal_state
    ON user_interventions(goal_id, state);

-- ── 3. Atomic goal event sequencing ──────────────────────────────────────────

CREATE UNIQUE INDEX idx_goal_events_goal_seq
    ON goal_events(goal_id, sequence_num);
