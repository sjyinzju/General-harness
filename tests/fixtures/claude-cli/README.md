# Claude CLI Fixtures

Claude Code version: 2.1.210
Auth method: api_key (ANTHROPIC_API_KEY)
Model: deepseek-v4-pro (custom provider)
Stream format: application/x-ndjson (one JSON object per line)

## Event types observed

- `system` (subtype: `init`) — session initialization
- `system` (subtype: `thinking_tokens`) — token usage estimates
- `assistant` — content blocks: `thinking`, `tool_use`, `text`
- `user` — tool_result blocks
- `result` — final result

## Key fields per event

- `type`: "system" | "assistant" | "user" | "result"
- `session_id`: UUID present on every event
- `uuid`: per-event UUID
- `message.id`: per-message UUID
- `message.content[]`: array of content blocks
- `message.model`: model name
- `message.usage`: token usage

## Note

All actual event payloads are NOT stored here to avoid leaking code context.
Protocol structure is documented above.
