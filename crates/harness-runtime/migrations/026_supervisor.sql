-- Migration 026: Supervisor instances, leases, and events (I6.1).
--
-- Tables:
--   supervisor_instances  — one row per Supervisor boot
--   supervisor_leases     — exclusive ownership leases
--   supervisor_events     — append-only lifecycle event log
--
-- Additive only. Migrations 001–025 frozen.

-- ── Supervisor Instances ───────────────────────────────────────────────────

CREATE TABLE supervisor_instances (
    instance_id TEXT PRIMARY KEY NOT NULL,
    state_directory_id TEXT NOT NULL,

    pid INTEGER NOT NULL,
    process_started_at TEXT NOT NULL,
    boot_nonce TEXT NOT NULL,

    state TEXT NOT NULL DEFAULT 'created'
        CHECK (state IN (
            'created','starting','acquiring_ownership','recovering','ready',
            'draining','stopping','stopped','failed','taking_over')),
    fencing_token INTEGER NOT NULL DEFAULT 0,

    started_at TEXT NOT NULL,
    heartbeat_at TEXT NOT NULL,
    lease_expires_at TEXT NOT NULL,

    protocol_version TEXT NOT NULL DEFAULT '1.0',
    binary_version TEXT NOT NULL DEFAULT '0.1.0',

    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_supervisor_instance_state_dir
    ON supervisor_instances(state_directory_id, state);

-- ── Supervisor Leases ──────────────────────────────────────────────────────

CREATE TABLE supervisor_leases (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    state_directory_id TEXT NOT NULL,
    instance_id TEXT NOT NULL REFERENCES supervisor_instances(instance_id),
    fencing_token INTEGER NOT NULL DEFAULT 1,
    acquired_at TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1
        CHECK (is_active IN (0, 1)),

    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- Exactly one active lease per state_directory_id.
CREATE UNIQUE INDEX idx_supervisor_lease_one_active
    ON supervisor_leases(state_directory_id)
    WHERE is_active = 1;

CREATE INDEX idx_supervisor_lease_instance
    ON supervisor_leases(instance_id);
CREATE INDEX idx_supervisor_lease_state_dir
    ON supervisor_leases(state_directory_id);

-- ── Supervisor Events ──────────────────────────────────────────────────────

CREATE TABLE supervisor_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id TEXT NOT NULL REFERENCES supervisor_instances(instance_id),
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL,
    occurred_at TEXT NOT NULL DEFAULT (datetime('now')),
    sequence_num INTEGER NOT NULL
);

CREATE INDEX idx_supervisor_events_instance
    ON supervisor_events(instance_id, sequence_num);
CREATE INDEX idx_supervisor_events_type
    ON supervisor_events(event_type, occurred_at);
