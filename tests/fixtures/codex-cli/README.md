# Codex CLI Fixtures

Codex CLI version: 0.116.0
Auth status: BLOCKED by config.toml issue (service_tier: "default" not recognized)
Expected format: `codex exec --json` produces JSONL on stdout

## Issue

Codex CLI 0.116.0 fails to start due to config.toml validation:
```
Error loading configuration: unknown variant `default`, expected `fast` or `flex`
in `service_tier`
```

This is a user configuration issue, not a Harness bug.
Resolution: user must fix `~/.codex/config.toml` service_tier value.

## Commands available

- `codex exec --json <PROMPT>` — non-interactive JSONL execution
- `codex login status` — auth check (currently blocked by config)
- `codex --version` — 0.116.0

## Key findings

- No `--cwd` flag on `codex exec` — uses process cwd
- JSONL format on stdout
- Stderr collected separately
- `codex exec --help` works despite config issue
