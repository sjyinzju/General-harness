# I4.5 — Boundaries and Forbidden Behaviors

## What I4.5 MAY Do

- Create new immutable Execution Attempts for the same Task
- Use I4's formal Scheduler dispatch entry point
- Read I4's Outcome, Dossier, Evidence, StepResult, and resource state
- Construct the next Attempt's Repair Context Pack
- Select allowed RuntimeProfiles by explicit policy
- Record token counts, cost, time, and tool calls
- Detect repeated failures and no-progress loops
- Stop, block, and await human
- Generate project-level escalation records for future I7 consumption

## What I4.5 MUST NOT Do

### I4 Integrity
- Modify certified Verification Outcomes
- Bypass I4 to execute Agent CLI directly
- Bypass Scheduler to create child processes
- Bypass formal Repository to fabricate Executions
- Re-execute on an old Execution
- Run two repair Attempts concurrently for the same Task

### Task/Project Scope
- Auto-create new Tasks
- Modify the project Task DAG
- Perform project-level re-planning
- Implement I7

### Git/Integration (I5 territory)
- Git commit
- Git merge
- Git rebase
- Git cherry-pick
- Integration queue
- Implement I5

### Resource Management
- Delete Worktrees
- Reacquire released resources

### Provider/Agent
- Silently switch Provider
- Auto-upgrade Agent
- Modify user global configuration
- Modify API keys or authentication
- Install new Agents

### Decision Making
- Call an additional LLM as Judge
- Decide success via Agent self-report
- Mark complete without I4 Verification evidence

## Highest Completion Status

I4.5's highest output is `CompleteCandidate`. This does NOT equal:
- Delivered
- Integrated
- Merged
- ProjectComplete

Final delivery, commit, and integration belong to future I5.
Project-level decomposition, Task DAG modification, and goal re-planning belong to future I7.
