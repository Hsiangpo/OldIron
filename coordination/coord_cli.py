"""Dual Codex 协调命令行工具。"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _utc_after(minutes: int) -> str:
    target = datetime.now(timezone.utc) + timedelta(minutes=max(int(minutes), 0))
    return target.replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _normalize_scope_item(value: str) -> str:
    return str(value or "").strip().replace("\\", "/").strip("/")


def _paths_overlap(left: str, right: str) -> bool:
    a = _normalize_scope_item(left)
    b = _normalize_scope_item(right)
    if not a or not b:
        return False
    return a == b or a.startswith(f"{b}/") or b.startswith(f"{a}/")


def _parse_utc(raw: str) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _is_expired(expires_at: str) -> bool:
    parsed = _parse_utc(expires_at)
    if parsed is None:
        return True
    return parsed <= datetime.now(timezone.utc)


class CoordinationConflictError(RuntimeError):
    """表示协调范围冲突。"""


@dataclass(slots=True)
class CoordinationStore:
    """协调文件存取。"""

    active_tasks_path: Path
    shared_locks_path: Path

    def read_active_tasks(self) -> dict[str, Any]:
        return self._read_json(self.active_tasks_path, default={"version": 1, "updated_at": _utc_now(), "tasks": []})

    def read_shared_locks(self) -> dict[str, Any]:
        return self._read_json(self.shared_locks_path, default={"version": 1, "updated_at": _utc_now(), "locks": []})

    def write_active_tasks(self, payload: dict[str, Any]) -> None:
        payload["updated_at"] = _utc_now()
        self._write_json(self.active_tasks_path, payload)

    def write_shared_locks(self, payload: dict[str, Any]) -> None:
        payload["updated_at"] = _utc_now()
        self._write_json(self.shared_locks_path, payload)

    def _read_json(self, path: Path, *, default: dict[str, Any]) -> dict[str, Any]:
        if not path.exists():
            return dict(default)
        return json.loads(path.read_text(encoding="utf-8"))

    def _write_json(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def begin_task(
    *,
    store: CoordinationStore,
    task_id: str,
    change_class: str,
    machine: str,
    agent: str,
    base_branch: str,
    working_branch: str,
    scope: list[str],
    planned_files: list[str],
    github_ref: str,
    lock_paths: list[str],
    lease_minutes: int,
    notes: str,
) -> dict[str, Any]:
    tasks_doc = store.read_active_tasks()
    locks_doc = store.read_shared_locks()
    _assert_task_id_free(tasks_doc["tasks"], task_id)
    _assert_scope_free(tasks_doc["tasks"], scope)
    normalized_locks = [_normalize_scope_item(item) for item in lock_paths if _normalize_scope_item(item)]
    if change_class == "shared_zone":
        if not normalized_locks:
            raise CoordinationConflictError("shared_zone 任务必须提供 lock_paths。")
        _assert_locks_free(locks_doc["locks"], normalized_locks)
    started_at = _utc_now()
    task: dict[str, Any] = {
        "task_id": task_id,
        "status": "in_progress",
        "machine": machine,
        "agent": agent,
        "change_class": change_class,
        "base_branch": base_branch,
        "working_branch": working_branch,
        "scope": [_normalize_scope_item(item) for item in scope if _normalize_scope_item(item)],
        "planned_files": [_normalize_scope_item(item) for item in planned_files if _normalize_scope_item(item)],
        "shared_lock_ids": [],
        "github_ref": github_ref,
        "started_at": started_at,
        "last_heartbeat_at": started_at,
        "notes": notes,
    }
    if change_class == "shared_zone":
        for index, path in enumerate(normalized_locks, start=1):
            lock_id = f"lock-{task_id}-{index}"
            locks_doc["locks"].append(
                {
                    "lock_id": lock_id,
                    "status": "locked",
                    "path": path,
                    "machine": machine,
                    "agent": agent,
                    "task_id": task_id,
                    "github_ref": github_ref,
                    "locked_at": started_at,
                    "heartbeat_at": started_at,
                    "expires_at": _utc_after(lease_minutes),
                    "notes": notes,
                }
            )
            task["shared_lock_ids"].append(lock_id)
    tasks_doc["tasks"].append(task)
    store.write_active_tasks(tasks_doc)
    store.write_shared_locks(locks_doc)
    return task


def heartbeat_task(*, store: CoordinationStore, task_id: str, lease_minutes: int) -> None:
    tasks_doc = store.read_active_tasks()
    locks_doc = store.read_shared_locks()
    task = _find_task(tasks_doc["tasks"], task_id)
    now = _utc_now()
    task["last_heartbeat_at"] = now
    for lock in locks_doc["locks"]:
        if lock.get("task_id") == task_id:
            lock["heartbeat_at"] = now
            lock["expires_at"] = _utc_after(lease_minutes)
    store.write_active_tasks(tasks_doc)
    store.write_shared_locks(locks_doc)


def finish_task(*, store: CoordinationStore, task_id: str, completion_notes: str = "") -> None:
    tasks_doc = store.read_active_tasks()
    locks_doc = store.read_shared_locks()
    task = _find_task(tasks_doc["tasks"], task_id)
    lock_ids = set(task.get("shared_lock_ids", []))
    locks_doc["locks"] = [lock for lock in locks_doc["locks"] if lock.get("lock_id") not in lock_ids]
    tasks_doc["tasks"] = [item for item in tasks_doc["tasks"] if item.get("task_id") != task_id]
    store.write_active_tasks(tasks_doc)
    store.write_shared_locks(locks_doc)


def force_expire_lock(*, store: CoordinationStore, lock_id: str) -> None:
    locks_doc = store.read_shared_locks()
    lock = _find_lock(locks_doc["locks"], lock_id)
    expired_at = datetime.now(timezone.utc) - timedelta(minutes=1)
    lock["expires_at"] = expired_at.replace(microsecond=0).isoformat().replace("+00:00", "Z")
    store.write_shared_locks(locks_doc)


def takeover_lock(
    *,
    store: CoordinationStore,
    previous_lock_id: str,
    new_task_id: str,
    machine: str,
    agent: str,
    base_branch: str,
    working_branch: str,
    scope: list[str],
    planned_files: list[str],
    github_ref: str,
    lease_minutes: int,
    notes: str,
) -> dict[str, Any]:
    tasks_doc = store.read_active_tasks()
    locks_doc = store.read_shared_locks()
    previous_lock = _find_lock(locks_doc["locks"], previous_lock_id)
    if not _is_expired(str(previous_lock.get("expires_at", ""))):
        raise CoordinationConflictError("锁尚未过期，不能接管。")
    previous_task = _find_task(tasks_doc["tasks"], str(previous_lock.get("task_id", "")))
    tasks_doc["tasks"] = [item for item in tasks_doc["tasks"] if item.get("task_id") != previous_task.get("task_id")]
    _assert_task_id_free(tasks_doc["tasks"], new_task_id)
    started_at = _utc_now()
    new_task = {
        "task_id": new_task_id,
        "status": "in_progress",
        "machine": machine,
        "agent": agent,
        "change_class": "shared_zone",
        "base_branch": base_branch,
        "working_branch": working_branch,
        "scope": [_normalize_scope_item(item) for item in scope if _normalize_scope_item(item)],
        "planned_files": [_normalize_scope_item(item) for item in planned_files if _normalize_scope_item(item)],
        "shared_lock_ids": [previous_lock_id],
        "github_ref": github_ref,
        "started_at": started_at,
        "last_heartbeat_at": started_at,
        "notes": notes,
    }
    previous_lock["machine"] = machine
    previous_lock["agent"] = agent
    previous_lock["task_id"] = new_task_id
    previous_lock["github_ref"] = github_ref
    previous_lock["heartbeat_at"] = started_at
    previous_lock["expires_at"] = _utc_after(lease_minutes)
    previous_lock["notes"] = notes
    tasks_doc["tasks"].append(new_task)
    store.write_active_tasks(tasks_doc)
    store.write_shared_locks(locks_doc)
    return new_task


def render_issue_body(*, store: CoordinationStore, task_id: str) -> str:
    task, locks = _task_with_locks(store, task_id)
    lock_lines = "\n".join(f"- {lock['path']}" for lock in locks) or "- none"
    lease_lines = "\n".join(
        f"- {lock['lock_id']}: heartbeat={lock.get('heartbeat_at', '')} expires={lock.get('expires_at', '')}"
        for lock in locks
    ) or "- none"
    return (
        "## Task\n\n"
        f"- Task ID: {task['task_id']}\n"
        f"- Machine: {task['machine']}\n"
        f"- Agent: {task['agent']}\n"
        f"- Change Class: {task['change_class']}\n"
        f"- Base Branch: {task.get('base_branch', '')}\n"
        f"- Working Branch: {task.get('working_branch', '')}\n\n"
        "## Scope\n\n"
        f"- Country / Site: {', '.join(task.get('scope', []))}\n"
        f"- Planned files / paths: {', '.join(task.get('planned_files', []))}\n\n"
        "## Coordination\n\n"
        f"- Related `coordination/active_tasks.json` entry: {task['task_id']}\n"
        f"- Related `coordination/shared_locks.json` entry: {', '.join(task.get('shared_lock_ids', [])) or 'none'}\n"
        f"- Expected lock paths:\n{lock_lines}\n"
        f"- Lease heartbeat:\n{lease_lines}\n"
    )


def render_pr_summary(*, store: CoordinationStore, task_id: str) -> str:
    task, locks = _task_with_locks(store, task_id)
    return (
        "## Coordination\n\n"
        f"- Task ID: {task['task_id']}\n"
        f"- Machine: {task['machine']}\n"
        f"- Agent: {task['agent']}\n"
        f"- Change Class: {task['change_class']}\n"
        f"- Base Branch: {task.get('base_branch', '')}\n"
        f"- Working Branch: {task.get('working_branch', '')}\n"
        f"- Related shared lock paths: {', '.join(lock['path'] for lock in locks) or 'none'}\n"
        f"- Key files: {', '.join(task.get('planned_files', []))}\n"
    )


def status_summary(*, store: CoordinationStore) -> str:
    tasks_doc = store.read_active_tasks()
    locks_doc = store.read_shared_locks()
    return (
        f"tasks={len(tasks_doc['tasks'])} active={sum(1 for t in tasks_doc['tasks'] if t.get('status') == 'in_progress')} "
        f"locks={len(locks_doc['locks'])}"
    )


def preflight_check(
    *,
    store: CoordinationStore,
    change_class: str,
    scope: list[str],
    lock_paths: list[str],
) -> dict[str, Any]:
    issues: list[str] = []
    tasks_doc = store.read_active_tasks()
    locks_doc = store.read_shared_locks()
    targets = [_normalize_scope_item(item) for item in scope if _normalize_scope_item(item)]
    normalized_locks = [_normalize_scope_item(item) for item in lock_paths if _normalize_scope_item(item)]

    for task in tasks_doc["tasks"]:
        if str(task.get("status")) != "in_progress":
            continue
        for owned in task.get("scope", []):
            if any(_paths_overlap(owned, target) for target in targets):
                issues.append(f"scope conflict: {owned} <- {task.get('task_id')}")
                break

    if change_class == "shared_zone" and not normalized_locks:
        issues.append("shared_zone task requires at least one lock_path")

    for lock in locks_doc["locks"]:
        if str(lock.get("status")) != "locked" or _is_expired(str(lock.get("expires_at", ""))):
            continue
        if any(_paths_overlap(str(lock.get("path", "")), target) for target in normalized_locks):
            issues.append(f"lock conflict: {lock.get('path')} <- {lock.get('task_id')}")

    return {"ok": not issues, "issues": issues}


def lease_doctor_report(*, store: CoordinationStore) -> dict[str, Any]:
    tasks_doc = store.read_active_tasks()
    locks_doc = store.read_shared_locks()
    active_task_ids = {
        str(task.get("task_id", ""))
        for task in tasks_doc["tasks"]
        if str(task.get("status")) == "in_progress"
    }
    expired_locks: list[str] = []
    orphan_locks: list[str] = []
    shared_tasks_without_locks: list[str] = []

    for lock in locks_doc["locks"]:
        lock_id = str(lock.get("lock_id", ""))
        if _is_expired(str(lock.get("expires_at", ""))):
            expired_locks.append(lock_id)
        if str(lock.get("task_id", "")) not in active_task_ids:
            orphan_locks.append(lock_id)

    for task in tasks_doc["tasks"]:
        if str(task.get("status")) != "in_progress":
            continue
        if str(task.get("change_class", "")) != "shared_zone":
            continue
        if not task.get("shared_lock_ids"):
            shared_tasks_without_locks.append(str(task.get("task_id", "")))

    return {
        "expired_locks": expired_locks,
        "orphan_locks": orphan_locks,
        "shared_tasks_without_locks": shared_tasks_without_locks,
    }


def _task_with_locks(store: CoordinationStore, task_id: str) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    tasks_doc = store.read_active_tasks()
    locks_doc = store.read_shared_locks()
    task = _find_task(tasks_doc["tasks"], task_id)
    lock_ids = set(task.get("shared_lock_ids", []))
    locks = [lock for lock in locks_doc["locks"] if lock.get("lock_id") in lock_ids]
    return task, locks


def _assert_task_id_free(tasks: list[dict[str, Any]], task_id: str) -> None:
    if any(str(task.get("task_id", "")) == task_id for task in tasks):
        raise CoordinationConflictError(f"task_id 已存在：{task_id}")


def _assert_scope_free(tasks: list[dict[str, Any]], scope: list[str]) -> None:
    targets = [_normalize_scope_item(item) for item in scope if _normalize_scope_item(item)]
    for task in tasks:
        if str(task.get("status")) != "in_progress":
            continue
        for owned in task.get("scope", []):
            if any(_paths_overlap(owned, target) for target in targets):
                raise CoordinationConflictError(f"scope 已被占用：{owned} <- {task.get('task_id')}")


def _assert_locks_free(locks: list[dict[str, Any]], lock_paths: list[str]) -> None:
    for lock in locks:
        if str(lock.get("status")) != "locked":
            continue
        if _is_expired(str(lock.get("expires_at", ""))):
            continue
        owned_path = str(lock.get("path", ""))
        if any(_paths_overlap(owned_path, target) for target in lock_paths):
            raise CoordinationConflictError(f"共享锁冲突：{owned_path} <- {lock.get('task_id')}")


def _find_task(tasks: list[dict[str, Any]], task_id: str) -> dict[str, Any]:
    for task in tasks:
        if str(task.get("task_id", "")) == task_id:
            return task
    raise CoordinationConflictError(f"找不到任务：{task_id}")


def _find_lock(locks: list[dict[str, Any]], lock_id: str) -> dict[str, Any]:
    for lock in locks:
        if str(lock.get("lock_id", "")) == lock_id:
            return lock
    raise CoordinationConflictError(f"找不到锁：{lock_id}")


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Dual Codex coordination helper")
    parser.add_argument("--active-tasks", default="coordination/active_tasks.json")
    parser.add_argument("--shared-locks", default="coordination/shared_locks.json")
    sub = parser.add_subparsers(dest="command", required=True)

    begin = sub.add_parser("begin")
    begin.add_argument("--task-id", required=True)
    begin.add_argument("--change-class", choices=["site_local", "shared_zone"], required=True)
    begin.add_argument("--machine", required=True)
    begin.add_argument("--agent", required=True)
    begin.add_argument("--base-branch", default="main")
    begin.add_argument("--working-branch", required=True)
    begin.add_argument("--scope", action="append", default=[])
    begin.add_argument("--planned-file", action="append", default=[])
    begin.add_argument("--github-ref", default="not-created")
    begin.add_argument("--lock-path", action="append", default=[])
    begin.add_argument("--lease-minutes", type=int, default=20)
    begin.add_argument("--notes", default="")

    heartbeat = sub.add_parser("heartbeat")
    heartbeat.add_argument("--task-id", required=True)
    heartbeat.add_argument("--lease-minutes", type=int, default=20)

    finish = sub.add_parser("finish")
    finish.add_argument("--task-id", required=True)
    finish.add_argument("--notes", default="")

    takeover = sub.add_parser("takeover")
    takeover.add_argument("--previous-lock-id", required=True)
    takeover.add_argument("--new-task-id", required=True)
    takeover.add_argument("--machine", required=True)
    takeover.add_argument("--agent", required=True)
    takeover.add_argument("--base-branch", default="main")
    takeover.add_argument("--working-branch", required=True)
    takeover.add_argument("--scope", action="append", default=[])
    takeover.add_argument("--planned-file", action="append", default=[])
    takeover.add_argument("--github-ref", default="not-created")
    takeover.add_argument("--lease-minutes", type=int, default=20)
    takeover.add_argument("--notes", default="")

    render_issue = sub.add_parser("render-issue")
    render_issue.add_argument("--task-id", required=True)
    render_pr = sub.add_parser("render-pr")
    render_pr.add_argument("--task-id", required=True)
    check = sub.add_parser("check")
    check.add_argument("--change-class", choices=["site_local", "shared_zone"], required=True)
    check.add_argument("--scope", action="append", default=[])
    check.add_argument("--lock-path", action="append", default=[])
    sub.add_parser("lease-doctor")
    sub.add_parser("status")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)
    store = CoordinationStore(active_tasks_path=Path(args.active_tasks), shared_locks_path=Path(args.shared_locks))
    if args.command == "begin":
        begin_task(
            store=store,
            task_id=args.task_id,
            change_class=args.change_class,
            machine=args.machine,
            agent=args.agent,
            base_branch=args.base_branch,
            working_branch=args.working_branch,
            scope=args.scope,
            planned_files=args.planned_file,
            github_ref=args.github_ref,
            lock_paths=args.lock_path,
            lease_minutes=args.lease_minutes,
            notes=args.notes,
        )
        print(args.task_id)
        return 0
    if args.command == "heartbeat":
        heartbeat_task(store=store, task_id=args.task_id, lease_minutes=args.lease_minutes)
        print(args.task_id)
        return 0
    if args.command == "finish":
        finish_task(store=store, task_id=args.task_id, completion_notes=args.notes)
        print(args.task_id)
        return 0
    if args.command == "takeover":
        takeover_lock(
            store=store,
            previous_lock_id=args.previous_lock_id,
            new_task_id=args.new_task_id,
            machine=args.machine,
            agent=args.agent,
            base_branch=args.base_branch,
            working_branch=args.working_branch,
            scope=args.scope,
            planned_files=args.planned_file,
            github_ref=args.github_ref,
            lease_minutes=args.lease_minutes,
            notes=args.notes,
        )
        print(args.new_task_id)
        return 0
    if args.command == "render-issue":
        print(render_issue_body(store=store, task_id=args.task_id))
        return 0
    if args.command == "render-pr":
        print(render_pr_summary(store=store, task_id=args.task_id))
        return 0
    if args.command == "check":
        report = preflight_check(
            store=store,
            change_class=args.change_class,
            scope=args.scope,
            lock_paths=args.lock_path,
        )
        print(json.dumps(report, ensure_ascii=False))
        return 0 if report["ok"] else 2
    if args.command == "lease-doctor":
        print(json.dumps(lease_doctor_report(store=store), ensure_ascii=False))
        return 0
    if args.command == "status":
        print(status_summary(store=store))
        return 0
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
