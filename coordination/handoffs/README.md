# Handoffs

Use this directory when a Codex task is paused, partially complete, blocked, or intentionally handed to another machine.

## When To Write A Handoff

- shared lock is released but the work is not fully complete
- task is blocked by runtime data, env sync, or another machine
- you want the other machine to continue without re-reading the whole diff
- the task changed runtime data assumptions or delivery assumptions

## File Naming

Use:

`YYYY-MM-DD-HHMM-<machine>-<short-scope>.md`

Example:

`2026-04-02-1030-machine1-england-companyname.md`

## Required Sections

```markdown
# Handoff: <scope>

- Task ID:
- Machine:
- Agent:
- Base Branch:
- Working Branch:
- Change Class:
- GitHub Ref:
- Related Shared Locks:
- Lease Expiry At:

## What Was Done

## Current Status

## Files Touched

## Runtime / Data Notes

## Safe Next Steps

## Verification Already Done

## Known Risks / Blockers
```

## Rule

Do not leave a partial shared-zone change without either:

- an updated task + lock entry that clearly shows it is still owned
- or a handoff note that explains what is safe to continue

For expired shared locks, the handoff note should say whether takeover is safe or not.
