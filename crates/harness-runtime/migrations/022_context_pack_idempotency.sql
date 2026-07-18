-- Migration 022: Context Pack idempotency — adds unique constraints
-- so that exactly one Context Pack can exist per (loop, target_ordinal)
-- and per canonical fingerprint.
--
-- Additive only. Migrations 001–021 frozen. No FSM changes to Gate C.

CREATE UNIQUE INDEX idx_context_pack_loop_ordinal
    ON task_context_packs(loop_id, target_attempt_ordinal);

CREATE UNIQUE INDEX idx_context_pack_fingerprint
    ON task_context_packs(context_fingerprint);
