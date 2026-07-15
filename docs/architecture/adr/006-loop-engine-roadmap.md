# ADR-006: Loop Engine Roadmap Registration

> **状态**: Accepted (backlog)
> **日期**: 2026-07-15

---

## Context

Foundation Release implements persistence, repositories, and transition services. Higher-level orchestration loops (task completion detection, project goal evaluation) are deferred to Functional Release.

## Decision

Register the following deferred items in the architecture backlog:

- **I4.5**: Deterministic Task Completion Loop — detect Execution completion, trigger verification, commit, integration
- **I7**: Project Goal Loop — evaluate all Task states against Goal Contract, trigger delivery
- **Rust Harness** retains state and execution authority
- **LangGraph** (or any external orchestrator) can only operate as an optional `GoalLoopProvider` sidecar via structured IPC
- **LangGraph sidecar MUST NOT**: write to SQLite, execute Git operations, spawn Agent subprocesses directly
- **LangGraph sidecar MAY**: read state via query interface, propose state transitions, propose task DAG revisions

## Consequences

- Foundation Release has no automated completion detection beyond manual `harness status` query
- Functional Release must implement I4.5 and I7
- LangGraph integration requires a clear IPC boundary and authorization model
