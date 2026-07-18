-- Migration 020: Idempotency fencing — adds version and fencing_token columns
-- to idempotency_records so stale-lease takeover is CAS'd under version.
--
-- Additive only. Migrations 001–019 frozen. No FSM changes to Gate C.

ALTER TABLE idempotency_records ADD COLUMN version INTEGER NOT NULL DEFAULT 1;
ALTER TABLE idempotency_records ADD COLUMN fencing_token INTEGER NOT NULL DEFAULT 1;
