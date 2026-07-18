# I4.5 — Loop State Machine

## States

| State | Description | Terminal |
|-------|-------------|----------|
| `created` | Loop record created, not yet started | No |
| `ready` | Ownership acquired, ready to prepare first Attempt | No |
| `preparing_attempt` | Building Context Pack and creating next Attempt row | No |
| `attempt_active` | An Execution is dispatched and running (or being verified) | No |
| `evaluating` | Execution terminal, reading I4 facts to decide next action | No |
| `complete_candidate` | I4 Passed + full Dossier — ready for I5 | Yes |
| `waiting_for_reconciliation` | I4 reconciliation not yet complete; retry after I4 Reconciler | No |
| `waiting_for_infrastructure` | Infrastructure failure; retry after manual/explicit resume | No |
| `waiting_for_human` | Ambiguity requires human decision | Yes* |
| `budget_exhausted` | Hard budget limit reached without passing | Yes |
| `no_progress` | Consecutive Attempts show no progress or cycle detected | Yes |
| `non_retryable` | Outcome class is explicitly not retryable | Yes |
| `escalated` | Task scope insufficient; escalation dossier written for I7 | Yes |
| `cancelled` | Explicit cancellation processed | Yes |
| `reconciliation_required` | Loop-level anomaly detected; needs reconciler | No |
| `failed` | Loop-level infrastructure failure | Yes |

* awaiting_human can be transitioned out by explicit human resume.

## Legal Transitions

```
created                → ready, failed
ready                  → preparing_attempt, cancelled, failed
preparing_attempt      → attempt_active, waiting_for_infrastructure, failed
attempt_active         → evaluating, cancelled, waiting_for_reconciliation
evaluating             → complete_candidate, preparing_attempt (ContinueRepair),
                         waiting_for_reconciliation, waiting_for_infrastructure,
                         waiting_for_human, budget_exhausted, no_progress,
                         non_retryable, escalated, failed
waiting_for_reconciliation → evaluating, waiting_for_human, failed
waiting_for_infrastructure → ready, evaluating, waiting_for_human, failed
waiting_for_human       → (terminal — only explicit resume by human)
```

## Hard Rules

- Terminal states NEVER transition back to active
- `complete_candidate` does NOT modify Task to `delivered`
- `waiting_for_human` does NOT auto-resume
- `escalated` does NOT create I7 Tasks
- `reconciliation_required` does NOT create a new Attempt
- `attempt_active` prevents creating a second Attempt
- After `cancelled`, no Attempt may be created
