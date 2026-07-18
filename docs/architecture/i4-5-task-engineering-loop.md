# I4.5 — Evidence-Gated Task Engineering Loop

## Overview

I4.5 runs the **Task-level engineering loop**: for one Task, it creates a bounded sequence of
immutable Execution Attempts, each going through the full I4 dispatch→execute→verify→finalize
pipeline, then reads the certified VerificationOutcome + Dossier to deterministically decide
the next action.

I4 owns **one Attempt**. I4.5 owns **the loop across Attempts**.

## Architecture Layers

```
┌──────────────────────────────────────────────┐
│ I4.5  TaskEngineeringLoopService             │
│  create_loop / start / observe / decide /    │
│  reconcile / cancel / inspect                │
├──────────────────────────────────────────────┤
│ I4    Scheduler → Adapter → Verification →   │
│       Finalization → Reconciliation          │
│       (certified, immutable per Attempt)      │
├──────────────────────────────────────────────┤
│ I4.5  Persistence                            │
│  task_engineering_loops                      │
│  task_engineering_attempts                   │
│  task_attempt_decisions                      │
│  task_context_packs                          │
│  task_usage_ledger                           │
│  task_loop_operations                        │
└──────────────────────────────────────────────┘
```

## Key Rules

1. One Task → at most one active Loop
2. One Loop → at most one active Attempt
3. Each Attempt → a new, immutable Execution
4. Old Executions are never re-opened
5. CompleteCandidate ≠ Delivered (that's I5)
6. I4.5 never commits, merges, or rebases
7. I4.5 never deletes Worktrees
8. I4.5 never calls Agent/LLM directly
9. I4.5 never modifies verified I4 outcomes

## Loop Lifecycle

```
Created → Ready → PreparingAttempt → AttemptActive → Evaluating
                                                         │
                    ┌────────────────────────────────────┤
                    ▼                                    ▼
            CompleteCandidate                    WaitingForReconciliation
            BudgetExhausted                      WaitingForInfrastructure
            NoProgress                           WaitingForHuman
            NonRetryable                         ReconciliationRequired
            Escalated
            Cancelled
            Failed
```

## Decision Precedence (stable, deterministic)

1. User/Task cancellation
2. I4 ReconciliationRequired / active or unknown process
3. Ownership/fencing/worktree/security ambiguity
4. Verified CompleteCandidate (I4 Passed + full trusted Dossier)
5. Explicit AwaitingHuman / NonRetryable
6. Hard budget exhausted
7. No-progress or cycle detected
8. InfrastructureBlocked
9. Repairable verified failure → ContinueRepair
10. Project-level escalation → EscalateToProjectPlanner
