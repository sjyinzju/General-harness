# I4.5 — Decision Classification Table

## Decision Classifications

| # | Classification | Auto-Action | Creates Attempt? | Releases Resources? |
|---|---------------|-------------|------------------|---------------------|
| 1 | `CompleteCandidate` | Mark loop complete | No | N/A (I4 already released) |
| 2 | `ContinueRepair` | Build Context Pack → new Attempt | Yes | N/A |
| 3 | `AwaitingReconciliation` | Call I4 Reconciler, wait | No | No |
| 4 | `InfrastructureBlocked` | Wait, retry on resume | No | No |
| 5 | `AwaitingHuman` | Persist structured reason | No | No |
| 6 | `BudgetExhausted` | Stop, write event | No | No |
| 7 | `NoProgress` | Stop, write event | No | No |
| 8 | `NonRetryable` | Stop, write event | No | No |
| 9 | `Cancelled` | Stop, cancel Execution if active | No | No |
| 10 | `EscalateToProjectPlanner` | Write escalation dossier | No | No |

## CompleteCandidate Gating (ALL must hold)

- Execution terminal
- VerificationRun terminal
- outcome = Passed
- next_action = CompleteCandidate
- All required steps passed
- No missing/stale Evidence
- No active command/policy operation
- No reconciliation_required operation
- No active/unknown process
- Worktree identity valid
- Outcome/dossier fingerprint consistent
- No security blocker
- No ownership/fencing conflict

## ContinueRepair Gating (ALL must hold)

- I4 outcome clearly repairable
- Dossier next_action = Repairable
- No reconciliation requirement
- Worktree continuable
- Budget allows
- No no-progress/cycle
- Allowed RuntimeProfile available
- No active Attempt

## Default Repairable Mapping

| Failure Classification | Default Action | Configurable |
|------------------------|---------------|--------------|
| BuildFailure | ContinueRepair | Yes |
| TestFailure | ContinueRepair | Yes |
| LintFailure | ContinueRepair | Yes |
| TypecheckFailure | ContinueRepair | Yes |
| CommandFailure | ContinueRepair | Yes |
| OutputMismatch | ContinueRepair | Yes |
| RequiredFileMissing | Policy-dependent | Yes |
| ArtifactMissing | Policy-dependent | Yes |
| ArtifactCorruption | Policy-dependent | Yes |
| ScopeViolation | ContinueRepair (with scope reminder) | Yes |
| ForbiddenChange | ContinueRepair (with constraint reminder) | Yes |
| PolicyViolation | ContinueRepair (with policy reminder) | Yes |
| SecretExposure | AwaitingHuman (security block) | Must be explicit |
| InfrastructureFailure | InfrastructureBlocked | Yes |
| OwnershipLost | AwaitingReconciliation | No |
| StaleFencing | AwaitingReconciliation | No |
| WorktreeMissing | AwaitingHuman | No |
| OutcomeConflict | NonRetryable / AwaitingHuman | No |
| IrrecoverableAmbiguity | AwaitingHuman | No |

## Escalation Triggers

- Acceptance criteria contradictory
- Task needs modification of another Task's contract
- New Tasks needed
- Task DAG must change
- Architecture decision needs rewrite
- Current Task scope insufficient
- Consecutive repairs show root cause is project-level planning
