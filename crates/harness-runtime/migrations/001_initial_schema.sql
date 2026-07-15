-- Migration v1: Foundation Persistence Kernel
-- Gate C frozen schema.

CREATE TABLE projects (
    id TEXT PRIMARY KEY NOT NULL,
    objective TEXT NOT NULL,
    lifecycle TEXT NOT NULL DEFAULT 'created',
    goal_contract_version INTEGER,
    plan_version INTEGER,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE tasks (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    goal TEXT NOT NULL DEFAULT '',
    lifecycle TEXT NOT NULL DEFAULT 'pending',
    retry_count INTEGER NOT NULL DEFAULT 0,
    max_retries INTEGER NOT NULL DEFAULT 3,
    current_execution_id TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_tasks_project ON tasks(project_id);

CREATE TABLE task_dependencies (
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    depends_on_task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    PRIMARY KEY (task_id, depends_on_task_id)
);

CREATE TABLE execution_attempts (
    id TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    attempt_number INTEGER NOT NULL,
    lifecycle TEXT NOT NULL DEFAULT 'created',
    profile_id TEXT NOT NULL DEFAULT '',
    agent_session_id TEXT,
    native_session_id TEXT,
    pid INTEGER,
    version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now')),
    UNIQUE(task_id, attempt_number)
);
CREATE INDEX idx_executions_task ON execution_attempts(task_id);

CREATE TABLE workspace_leases (
    id TEXT PRIMARY KEY NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    owner_execution_id TEXT REFERENCES execution_attempts(id) ON DELETE SET NULL,
    lifecycle TEXT NOT NULL DEFAULT 'acquired',
    worktree_path TEXT NOT NULL DEFAULT '',
    branch_name TEXT NOT NULL DEFAULT '',
    ownership_marker TEXT,
    heartbeat_at TEXT,
    expires_at TEXT NOT NULL,
    version INTEGER NOT NULL DEFAULT 1,
    acquired_at TEXT NOT NULL DEFAULT (datetime('now')),
    released_at TEXT
);

CREATE TABLE runtime_profiles (
    id TEXT PRIMARY KEY NOT NULL,
    agent_definition_id TEXT NOT NULL DEFAULT '',
    agent_kind TEXT NOT NULL DEFAULT '',
    adapter_kind TEXT NOT NULL DEFAULT '',
    agent_version TEXT NOT NULL DEFAULT '',
    executable_path TEXT NOT NULL DEFAULT '',
    provider TEXT NOT NULL DEFAULT '',
    provider_source TEXT NOT NULL DEFAULT 'custom_unknown',
    model TEXT,
    base_url TEXT,
    auth_mode TEXT NOT NULL DEFAULT 'unknown',
    auth_status TEXT NOT NULL DEFAULT 'unknown',
    credential_ref TEXT,
    core_status TEXT NOT NULL DEFAULT 'available',
    authentication_status TEXT NOT NULL DEFAULT 'unknown',
    execution_status TEXT NOT NULL DEFAULT 'untested',
    capabilities_json TEXT NOT NULL DEFAULT '{}',
    passive_probe_json TEXT,
    active_validation_json TEXT,
    concurrency_max INTEGER NOT NULL DEFAULT 1,
    concurrency_current INTEGER NOT NULL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE resource_claims (
    id TEXT PRIMARY KEY NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    execution_id TEXT REFERENCES execution_attempts(id) ON DELETE SET NULL,
    resource_kind TEXT NOT NULL,
    normalized_resource TEXT NOT NULL,
    access_mode TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'active',
    heartbeat_at TEXT,
    expires_at TEXT,
    acquired_at TEXT NOT NULL DEFAULT (datetime('now')),
    released_at TEXT
);
CREATE INDEX idx_resource_claims_active ON resource_claims(normalized_resource, access_mode) WHERE status = 'active';

CREATE TABLE event_log (
    id TEXT PRIMARY KEY NOT NULL,
    stream_id TEXT NOT NULL,
    stream_version INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    payload_json TEXT NOT NULL DEFAULT '{}',
    schema_version INTEGER NOT NULL DEFAULT 1,
    correlation_id TEXT NOT NULL DEFAULT '',
    causation_id TEXT,
    idempotency_key TEXT NOT NULL UNIQUE,
    source TEXT NOT NULL DEFAULT 'harness',
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX idx_event_log_stream ON event_log(stream_id, stream_version);
CREATE UNIQUE INDEX idx_event_log_stream_version ON event_log(stream_id, stream_version);

CREATE TABLE operations (
    id TEXT PRIMARY KEY NOT NULL,
    operation_id TEXT NOT NULL UNIQUE,
    operation_type TEXT NOT NULL,
    task_id TEXT NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    status TEXT NOT NULL DEFAULT 'pending',
    payload_json TEXT NOT NULL DEFAULT '{}',
    result_json TEXT,
    version INTEGER NOT NULL DEFAULT 1,
    idempotency_key TEXT NOT NULL UNIQUE,
    started_at TEXT NOT NULL DEFAULT (datetime('now')),
    completed_at TEXT
);

CREATE TABLE idempotency_records (
    key TEXT PRIMARY KEY NOT NULL,
    result_json TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
