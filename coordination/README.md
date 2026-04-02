# Dual Codex Coordination

This directory is the repo-local coordination surface for multi-machine, multi-agent OldIron work.

Use it together with GitHub issue / PR updates:

- `coordination/` is the machine-readable real-time state
- GitHub issue / PR updates are the human-visible audit trail

## Files

- `active_tasks.json`
  - who is actively working
  - which machine / branch / scope they own
  - which files or directories they plan to touch
- `shared_locks.json`
  - which high-risk shared paths are currently locked
  - who locked them
  - which GitHub issue / PR tracks the change
- `handoffs/`
  - markdown notes for partial work, blockers, or safe resume context

## High-Risk Shared Zone

Claim a shared lock before editing any of these paths:

- `shared/`
- repo-root `product.py`
- repo-root `AGENTS.md`
- repo-root `README.md`
- `.github/`
- `coordination/`
- any `<Country>/shared/`
- any `<Country>/src/*/delivery.py`

Country/site-local changes outside the paths above still require an active task entry, but do not require a shared lock.

## Required Workflow

1. `git pull`
2. Read `AGENTS.md`
3. Read `coordination/active_tasks.json`
4. Read `coordination/shared_locks.json`
5. Decide whether the planned scope is free
6. Register the task in `active_tasks.json`
7. If touching the high-risk shared zone, also claim the exact path in `shared_locks.json`
8. Mirror the task on GitHub with an issue or PR
9. Implement and verify
10. Push code
11. Update task status and release the shared lock
12. If the work is partial, add a handoff note under `coordination/handoffs/`

## Conflict Rule

If another active task already owns or locks the same scope:

- do not proceed
- sync latest Git state again if needed
- report the conflict to the user
- wait for reassignment or sequencing

## `active_tasks.json` Record Shape

```json
{
  "task_id": "coord-2026-04-02-england-shared-cleanup",
  "status": "in_progress",
  "machine": "Machine 1",
  "agent": "codex-windows",
  "branch": "main",
  "scope": [
    "England/sites/companyname",
    "README.md"
  ],
  "planned_files": [
    "England/src/england_crawler/sites/companyname/pipeline.py",
    "README.md"
  ],
  "shared_lock_ids": [
    "lock-readme"
  ],
  "github_ref": "issue#123",
  "started_at": "2026-04-02T10:00:00Z",
  "last_heartbeat_at": "2026-04-02T10:20:00Z",
  "notes": "Fix Windows runtime note and shared docs."
}
```

## `shared_locks.json` Record Shape

```json
{
  "lock_id": "lock-readme",
  "status": "locked",
  "path": "README.md",
  "machine": "Machine 1",
  "agent": "codex-windows",
  "task_id": "coord-2026-04-02-england-shared-cleanup",
  "github_ref": "issue#123",
  "locked_at": "2026-04-02T10:00:00Z",
  "notes": "Updating shared coordination rules."
}
```

## Update Discipline

- Keep entries UTF-8 and valid JSON.
- Prefer updating only the records you own.
- Use exact file paths for shared locks whenever possible.
- If you pause for more than a short break, refresh `last_heartbeat_at`.
- When a task is done, mark it `completed` and remove or release its shared locks in the same push.
