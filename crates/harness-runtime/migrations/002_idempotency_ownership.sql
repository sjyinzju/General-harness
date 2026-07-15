-- Migration v2: Idempotency ownership model.
-- Adds request_hash, status, owner_token, lease_expires_at, attempt_count, error_json.

ALTER TABLE idempotency_records ADD COLUMN request_hash TEXT;
ALTER TABLE idempotency_records ADD COLUMN status TEXT NOT NULL DEFAULT 'completed';
ALTER TABLE idempotency_records ADD COLUMN owner_token TEXT;
ALTER TABLE idempotency_records ADD COLUMN lease_expires_at TEXT;
ALTER TABLE idempotency_records ADD COLUMN attempt_count INTEGER NOT NULL DEFAULT 1;
ALTER TABLE idempotency_records ADD COLUMN error_json TEXT;
ALTER TABLE idempotency_records ADD COLUMN updated_at TEXT NOT NULL DEFAULT (datetime('now'));
ALTER TABLE idempotency_records ADD COLUMN completed_at TEXT;

-- Existing records (v1 style) are marked completed for backwards compat.
