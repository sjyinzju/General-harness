-- Migration 024: Controlled Commit (I5.1).
--
-- Three new tables:
--   commit_requests       — commit requests with admission state
--   commit_candidates     — successfully created commit objects
--   commit_creation_attempts — individual creation attempts (retry/idempotency)
--
-- Additive only. Migrations 001–023 frozen.

-- ── Commit Request ─────────────────────────────────────────────────────

CREATE TABLE commit_requests (
    commit_request_id TEXT PRIMARY KEY NOT NULL,
    candidate_id TEXT NOT NULL REFERENCES candidate_snapshots(candidate_id),
    review_id TEXT NOT NULL REFERENCES review_requests(review_id),
    repository_id TEXT NOT NULL,
    target_ref TEXT NOT NULL,
    expected_base_commit TEXT NOT NULL,
    author_name TEXT NOT NULL,
    author_email TEXT NOT NULL,
    committer_name TEXT NOT NULL,
    committer_email TEXT NOT NULL,
    commit_timestamp TEXT NOT NULL,
    message TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'requested'
        CHECK (state IN ('requested','materializing','created','blocked','failed','cancelled')),
    idempotency_key TEXT NOT NULL UNIQUE,
    idempotency_digest TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT
);

CREATE INDEX idx_commit_request_candidate ON commit_requests(candidate_id);
CREATE INDEX idx_commit_request_review ON commit_requests(review_id);
CREATE INDEX idx_commit_request_state ON commit_requests(state);

-- Exactly one active commit per candidate+review+target_ref
CREATE UNIQUE INDEX idx_commit_one_active_per_scope
    ON commit_requests(candidate_id, review_id, target_ref)
    WHERE state NOT IN ('created','blocked','failed','cancelled');

-- ── Commit Candidate ───────────────────────────────────────────────────

CREATE TABLE commit_candidates (
    commit_request_id TEXT PRIMARY KEY NOT NULL REFERENCES commit_requests(commit_request_id),
    candidate_id TEXT NOT NULL REFERENCES candidate_snapshots(candidate_id),
    review_id TEXT NOT NULL REFERENCES review_requests(review_id),
    repository_id TEXT NOT NULL,
    commit_oid TEXT NOT NULL,
    parent_oid TEXT NOT NULL,
    tree_oid TEXT NOT NULL,
    diff_digest TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_commit_candidate_candidate ON commit_candidates(candidate_id);
CREATE INDEX idx_commit_candidate_oid ON commit_candidates(commit_oid);

-- ── Commit Creation Attempt ─────────────────────────────────────────────

CREATE TABLE commit_creation_attempts (
    attempt_id TEXT PRIMARY KEY NOT NULL,
    commit_request_id TEXT NOT NULL REFERENCES commit_requests(commit_request_id),
    attempt_number INTEGER NOT NULL DEFAULT 1,
    state TEXT NOT NULL DEFAULT 'started'
        CHECK (state IN ('started','created','failed','recovered')),
    commit_oid TEXT,
    error_message TEXT,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT
);

CREATE INDEX idx_commit_attempt_request ON commit_creation_attempts(commit_request_id);

-- ── Commit Events (append-only) ─────────────────────────────────────────

CREATE TABLE commit_events (
    event_id TEXT PRIMARY KEY NOT NULL,
    commit_request_id TEXT NOT NULL REFERENCES commit_requests(commit_request_id),
    candidate_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_commit_events_request ON commit_events(commit_request_id);
