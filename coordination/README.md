# Dual Codex Coordination

This directory is the repo-local coordination surface for multi-machine, multi-agent OldIron work.

Use it together with GitHub issue / PR updates:

- `coordination/` is the machine-readable real-time state
- GitHub issue / PR updates are the human-visible audit trail

## Files

- `active_tasks.json`
  - who is actively working
  - which machine / branch / scope they own
  - whether the task is `site_local` or `shared_zone`
  - which files or directories they plan to touch
- `shared_locks.json`
  - which high-risk shared paths are currently locked
  - who locked them
  - lease timing (`expires_at`, `heartbeat_at`)
  - which GitHub issue / PR tracks the change
- `handoffs/`
  - markdown notes for partial work, blockers, or safe resume context

## High-Risk Shared Zone

Claim a shared lease lock before editing any of these paths:

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
5. Classify the task:
   - `site_local`
   - `shared_zone`
6. Register the task in `active_tasks.json`
7. If the task is `site_local`:
   - create a task branch
   - push the branch early so the remote machine can see the work started
8. If the task is `shared_zone`:
   - claim the exact shared path(s) in `shared_locks.json`
   - set `expires_at` and `heartbeat_at`
   - push the lock state to the remote before editing the shared-zone files
9. Mirror the task on GitHub with an issue or PR
10. Implement and verify
11. `git pull --rebase origin main` before final push when appropriate
12. Push code
13. Update task status and release the shared lock if one exists
14. If the work is partial, add a handoff note under `coordination/handoffs/`

## Conflict Rule

If another active task already owns or locks the same scope:

- do not proceed
- sync latest Git state again if needed
- report the conflict to the user
- wait for reassignment or sequencing

If a shared lock is expired:

- verify there is no recent heartbeat
- verify no active branch/PR update suggests the owner is still working
- write a takeover note in the task or handoff record
- only then claim the lock for the new task

## `active_tasks.json` Record Shape

```json
{
  "task_id": "coord-2026-04-02-england-shared-cleanup",
  "status": "in_progress",
  "machine": "Machine 1",
  "agent": "codex-windows",
  "change_class": "shared_zone",
  "base_branch": "main",
  "working_branch": "machine1/england/shared-cleanup",
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
  "heartbeat_at": "2026-04-02T10:20:00Z",
  "expires_at": "2026-04-02T10:40:00Z",
  "notes": "Updating shared coordination rules."
}
```

## Update Discipline

- Keep entries UTF-8 and valid JSON.
- Prefer updating only the records you own.
- Use exact file paths for shared locks whenever possible.
- If you pause for more than a short break, refresh `last_heartbeat_at`.
- Shared locks are lease locks, not permanent locks. Refresh `heartbeat_at` and extend `expires_at` if the shared task is still active.
- When a task is done, mark it `completed` and remove or release its shared locks in the same push.
