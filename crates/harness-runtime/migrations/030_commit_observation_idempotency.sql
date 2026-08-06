-- Migration 030: Commit idempotency constraint (F7).
--
-- F7: One commit_candidate per candidate_id (one controlled commit
--      per approved review decision / candidate).
--
-- The existing schema uses commit_request_id as PK, which already
-- ensures one candidate per request. This additional UNIQUE index
-- ensures one candidate per (candidate_id) at the database level,
-- preventing any code path from creating duplicate commit_candidates
-- for the same candidate.
--
-- Additive only. Migrations 001–029 frozen.

-- ── F7: One commit per candidate ──────────────────────────────────────

CREATE UNIQUE INDEX IF NOT EXISTS idx_commit_candidate_one_per_candidate
    ON commit_candidates(candidate_id);
