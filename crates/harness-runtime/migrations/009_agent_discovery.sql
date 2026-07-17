-- Migration 009: Agent Discovery persistence.
-- Adds agent_definitions table and extends runtime_profiles with
-- discovery tracking columns (first_seen_at, last_seen_at, label).
-- Additive only — migrations 001–008 are frozen.

-- ── New: agent_definitions ──────────────────────────────────────────

CREATE TABLE agent_definitions (
    id TEXT PRIMARY KEY NOT NULL,
    agent_kind TEXT NOT NULL,
    label TEXT NOT NULL DEFAULT '',
    executable_path TEXT NOT NULL,
    discovery_source TEXT NOT NULL DEFAULT 'path',
    discovery_source_detail TEXT,
    version TEXT,
    is_wrapper INTEGER NOT NULL DEFAULT 0,
    wraps_agent_kind TEXT,
    passive_status TEXT NOT NULL DEFAULT 'detected',
    diagnostics_json TEXT NOT NULL DEFAULT '[]',
    first_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    last_seen_at TEXT NOT NULL DEFAULT (datetime('now')),
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_agent_definitions_kind ON agent_definitions(agent_kind);
CREATE INDEX idx_agent_definitions_path ON agent_definitions(executable_path);

-- ── New: discovery_evidence ────────────────────────────────────────

CREATE TABLE discovery_evidence (
    id TEXT PRIMARY KEY NOT NULL,
    agent_definition_id TEXT NOT NULL REFERENCES agent_definitions(id) ON DELETE CASCADE,
    evidence_kind TEXT NOT NULL,
    observation TEXT NOT NULL,
    confidence TEXT NOT NULL DEFAULT 'medium',
    collected_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_discovery_evidence_agent ON discovery_evidence(agent_definition_id);

-- ── New: agent_provider_hints ──────────────────────────────────────

CREATE TABLE agent_provider_hints (
    id TEXT PRIMARY KEY NOT NULL,
    agent_definition_id TEXT NOT NULL REFERENCES agent_definitions(id) ON DELETE CASCADE,
    provider TEXT NOT NULL,
    source TEXT NOT NULL DEFAULT 'unknown',
    confidence TEXT NOT NULL DEFAULT 'low',
    evidence_json TEXT NOT NULL DEFAULT '[]',
    base_url TEXT,
    is_custom_endpoint INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX idx_provider_hints_agent ON agent_provider_hints(agent_definition_id);

-- ── Extend runtime_profiles with discovery tracking ─────────────────

ALTER TABLE runtime_profiles ADD COLUMN label TEXT NOT NULL DEFAULT '';
ALTER TABLE runtime_profiles ADD COLUMN first_seen_at TEXT NOT NULL DEFAULT (datetime('now'));
ALTER TABLE runtime_profiles ADD COLUMN last_seen_at TEXT NOT NULL DEFAULT (datetime('now'));
ALTER TABLE runtime_profiles ADD COLUMN capability_negotiation_json TEXT NOT NULL DEFAULT '{}';
ALTER TABLE runtime_profiles ADD COLUMN validation_status_json TEXT;
