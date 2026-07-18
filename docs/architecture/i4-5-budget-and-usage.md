# I4.5 — Budget and Usage Model

## Budget Policy

```rust
LoopBudgetPolicy {
    max_attempts: Option<u32>,
    max_wall_time_secs: Option<u64>,
    max_tool_calls: Option<u64>,
    max_input_tokens: Option<u64>,
    max_output_tokens: Option<u64>,
    max_total_tokens: Option<u64>,
    max_estimated_cost_micros: Option<u64>,
    max_no_progress_streak: Option<u32>,
    max_same_failure_streak: Option<u32>,
    max_profile_switches: Option<u32>,
}
```

## Budget Modes

| Mode | Behavior |
|------|----------|
| `Hard` | Enforce strictly; exceed → BudgetExhausted |
| `Advisory` | Log warning but continue |
| `ObserveOnly` | Record usage only; never stop |
| `Unset` | No limit (equivalent to None) |

## Unknown Usage Policy

When token/cost usage is unknown (provider didn't report):

| Policy | Behavior |
|--------|----------|
| `BlockUnknown` | Stop; enter WaitingForInfrastructure |
| `AllowWithWarning` | Continue; log warning event |
| `AwaitHuman` | Stop; enter WaitingForHuman |

Default: `AllowWithWarning` for ObserveOnly mode; `BlockUnknown` for Hard mode.

## Budget Reservation

- Budget is checked and reserved BEFORE Attempt creation
- Reservation is atomic (version-CAS on loop row)
- Two controllers cannot double-reserve
- If Attempt creation fails, reservation is released
- After Execution creation succeeds, reservation is bound to the Attempt
- Response-lost does not double-reserve

## Usage Ledger

- Records per-Attempt usage from I4 Adapter/Execution facts
- Provider-reported values are authoritative
- Unknown values are recorded as NULL (never 0)
- `usage_known` flag distinguishes reported vs estimated
- Usage is exactly-once (idempotency key per provider event)
- No authentication data is stored

## Principles

1. Correctness > Recoverability > Verification Trust > Convergence > Cost Optimization
2. Token optimization follows after full-path verification with real data
3. Never sacrifice engineering quality for token savings
4. Never fabricate or guess token counts
