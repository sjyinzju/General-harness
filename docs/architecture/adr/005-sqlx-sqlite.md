# ADR-005: SQLx for SQLite Persistence

> **状态**: Accepted
> **日期**: 2026-07-15

---

## Context

Persistence Kernel needs an async-compatible SQLite driver for `harness-runtime`. Options: SQLx, rusqlite.

## Decision

**SQLx with SQLite backend**.

## Rationale

1. **Async-native**: SQLx provides async query execution without `spawn_blocking`. SQLite is single-writer, but SQLx handles connection pooling and WAL-mode concurrent reads correctly.
2. **Compile-time query verification**: `sqlx::query!()` and `sqlx::query_as!()` verify SQL against the actual database schema at compile time (with `SQLX_OFFLINE` or a live DB).
3. **Migration framework**: Built-in `sqlx::migrate!()` macro with versioned SQL files.
4. **Type-safe**: `FromRow` derive for mapping rows to Rust structs.
5. **Single dependency**: Replaces rusqlite + migration runner + custom connection pool.

## Consequences

- **Positive**: Async-native, compile-time query checks, built-in migrations
- **Positive**: `sqlx::SqlitePool` handles WAL-mode concurrent reads
- **Negative**: SQLx SQLite driver wraps libsqlite3-sys (same underlying C lib as rusqlite)
- **Negative**: Compile-time query checking requires a live DB or `SQLX_OFFLINE=true` in CI
- **Negative**: Write operations still serialize on SQLite's single-writer lock — this is inherent to SQLite, not SQLx
