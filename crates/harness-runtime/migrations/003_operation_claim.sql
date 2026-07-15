-- Migration v3: Operation claim columns.
-- Adds claim_token, claim_expires_at, attempt_count, last_error for operation ownership.

ALTER TABLE operations ADD COLUMN claimed_by TEXT;
ALTER TABLE operations ADD COLUMN claim_token TEXT;
ALTER TABLE operations ADD COLUMN claim_expires_at TEXT;
ALTER TABLE operations ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 1;
ALTER TABLE operations ADD COLUMN last_error TEXT;
ALTER TABLE operations ADD COLUMN updated_at TEXT NOT NULL DEFAULT (datetime('now'));
