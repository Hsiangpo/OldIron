from __future__ import annotations

import json
import sys
import tempfile
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


class CoordinationCliTests(unittest.TestCase):
    def test_begin_site_local_task_creates_task_without_lock(self) -> None:
        from coordination.coord_cli import CoordinationStore
        from coordination.coord_cli import begin_task

        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(Path(tmpdir))
            begin_task(
                store=store,
                task_id="task-site-local",
                change_class="site_local",
                machine="Machine 1",
                agent="codex-windows",
                base_branch="main",
                working_branch="machine1/england/fast-fix",
                scope=["England/sites/companyname"],
                planned_files=["England/src/england_crawler/sites/companyname/pipeline.py"],
                github_ref="issue#1",
                lock_paths=[],
                lease_minutes=20,
                notes="site-local change",
            )

            tasks = store.read_active_tasks()["tasks"]
            locks = store.read_shared_locks()["locks"]
            self.assertEqual(1, len(tasks))
            self.assertEqual("site_local", tasks[0]["change_class"])
            self.assertEqual([], tasks[0]["shared_lock_ids"])
            self.assertEqual([], locks)

    def test_begin_shared_zone_task_creates_lease_lock(self) -> None:
        from coordination.coord_cli import CoordinationStore
        from coordination.coord_cli import begin_task

        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(Path(tmpdir))
            begin_task(
                store=store,
                task_id="task-shared",
                change_class="shared_zone",
                machine="Machine 1",
                agent="codex-windows",
                base_branch="main",
                working_branch="machine1/shared/docs",
                scope=["AGENTS.md"],
                planned_files=["AGENTS.md"],
                github_ref="issue#2",
                lock_paths=["AGENTS.md"],
                lease_minutes=20,
                notes="shared-zone change",
            )

            tasks = store.read_active_tasks()["tasks"]
            locks = store.read_shared_locks()["locks"]
            self.assertEqual(1, len(tasks))
            self.assertEqual(1, len(locks))
            self.assertEqual("shared_zone", tasks[0]["change_class"])
            self.assertTrue(tasks[0]["shared_lock_ids"])
            self.assertEqual("locked", locks[0]["status"])
            self.assertIn("heartbeat_at", locks[0])
            self.assertIn("expires_at", locks[0])

    def test_begin_shared_zone_task_rejects_active_lock_conflict(self) -> None:
        from coordination.coord_cli import CoordinationConflictError
        from coordination.coord_cli import CoordinationStore
        from coordination.coord_cli import begin_task

        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(Path(tmpdir))
            begin_task(
                store=store,
                task_id="task-shared-1",
                change_class="shared_zone",
                machine="Machine 1",
                agent="codex-windows",
                base_branch="main",
                working_branch="machine1/shared/docs-a",
                scope=["AGENTS.md"],
                planned_files=["AGENTS.md"],
                github_ref="issue#3",
                lock_paths=["AGENTS.md"],
                lease_minutes=20,
                notes="first lock",
            )

            with self.assertRaises(CoordinationConflictError):
                begin_task(
                    store=store,
                    task_id="task-shared-2",
                    change_class="shared_zone",
                    machine="Machine 2",
                    agent="codex-mac",
                    base_branch="main",
                    working_branch="machine2/shared/docs-b",
                    scope=["AGENTS.md"],
                    planned_files=["AGENTS.md"],
                    github_ref="issue#4",
                    lock_paths=["AGENTS.md"],
                    lease_minutes=20,
                    notes="conflicting lock",
                )

    def test_heartbeat_refreshes_lock_lease(self) -> None:
        from coordination.coord_cli import CoordinationStore
        from coordination.coord_cli import begin_task
        from coordination.coord_cli import heartbeat_task

        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(Path(tmpdir))
            begin_task(
                store=store,
                task_id="task-heartbeat",
                change_class="shared_zone",
                machine="Machine 1",
                agent="codex-windows",
                base_branch="main",
                working_branch="machine1/shared/docs",
                scope=["AGENTS.md"],
                planned_files=["AGENTS.md"],
                github_ref="issue#5",
                lock_paths=["AGENTS.md"],
                lease_minutes=1,
                notes="heartbeat test",
            )
            first_expiry = store.read_shared_locks()["locks"][0]["expires_at"]
            heartbeat_task(store=store, task_id="task-heartbeat", lease_minutes=30)
            second_expiry = store.read_shared_locks()["locks"][0]["expires_at"]
            self.assertNotEqual(first_expiry, second_expiry)

    def test_finish_task_releases_lock(self) -> None:
        from coordination.coord_cli import CoordinationStore
        from coordination.coord_cli import begin_task
        from coordination.coord_cli import finish_task

        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(Path(tmpdir))
            begin_task(
                store=store,
                task_id="task-finish",
                change_class="shared_zone",
                machine="Machine 1",
                agent="codex-windows",
                base_branch="main",
                working_branch="machine1/shared/docs",
                scope=["AGENTS.md"],
                planned_files=["AGENTS.md"],
                github_ref="issue#6",
                lock_paths=["AGENTS.md"],
                lease_minutes=20,
                notes="finish test",
            )
            finish_task(store=store, task_id="task-finish", completion_notes="done")
            tasks = store.read_active_tasks()["tasks"]
            locks = store.read_shared_locks()["locks"]
            self.assertEqual([], tasks)
            self.assertEqual([], locks)

    def test_takeover_replaces_expired_lock_owner(self) -> None:
        from coordination.coord_cli import CoordinationStore
        from coordination.coord_cli import begin_task
        from coordination.coord_cli import force_expire_lock
        from coordination.coord_cli import takeover_lock

        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(Path(tmpdir))
            begin_task(
                store=store,
                task_id="task-old-owner",
                change_class="shared_zone",
                machine="Machine 1",
                agent="codex-windows",
                base_branch="main",
                working_branch="machine1/shared/docs",
                scope=["AGENTS.md"],
                planned_files=["AGENTS.md"],
                github_ref="issue#7",
                lock_paths=["AGENTS.md"],
                lease_minutes=20,
                notes="old owner",
            )
            force_expire_lock(store=store, lock_id="lock-task-old-owner-1")
            takeover_lock(
                store=store,
                previous_lock_id="lock-task-old-owner-1",
                new_task_id="task-new-owner",
                machine="Machine 2",
                agent="codex-mac",
                base_branch="main",
                working_branch="machine2/shared/docs",
                scope=["AGENTS.md"],
                planned_files=["AGENTS.md"],
                github_ref="issue#8",
                lease_minutes=20,
                notes="takeover",
            )
            locks = store.read_shared_locks()["locks"]
            tasks = store.read_active_tasks()["tasks"]
            self.assertEqual("Machine 2", locks[0]["machine"])
            self.assertEqual("task-new-owner", locks[0]["task_id"])
            self.assertFalse(any(t["task_id"] == "task-old-owner" for t in tasks))

    def test_render_issue_and_pr_text_include_core_fields(self) -> None:
        from coordination.coord_cli import CoordinationStore
        from coordination.coord_cli import begin_task
        from coordination.coord_cli import render_issue_body
        from coordination.coord_cli import render_pr_summary

        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(Path(tmpdir))
            begin_task(
                store=store,
                task_id="task-render",
                change_class="shared_zone",
                machine="Machine 1",
                agent="codex-windows",
                base_branch="main",
                working_branch="machine1/shared/docs",
                scope=["AGENTS.md"],
                planned_files=["AGENTS.md"],
                github_ref="issue#9",
                lock_paths=["AGENTS.md"],
                lease_minutes=20,
                notes="render",
            )
            issue_text = render_issue_body(store=store, task_id="task-render")
            pr_text = render_pr_summary(store=store, task_id="task-render")
            self.assertIn("Task ID: task-render", issue_text)
            self.assertIn("Change Class: shared_zone", issue_text)
            self.assertIn("Task ID: task-render", pr_text)
            self.assertIn("Change Class: shared_zone", pr_text)

    def test_preflight_check_reports_conflict(self) -> None:
        from coordination.coord_cli import CoordinationStore
        from coordination.coord_cli import begin_task
        from coordination.coord_cli import preflight_check

        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(Path(tmpdir))
            begin_task(
                store=store,
                task_id="task-preflight-owner",
                change_class="shared_zone",
                machine="Machine 1",
                agent="codex-windows",
                base_branch="main",
                working_branch="machine1/shared/docs",
                scope=["AGENTS.md"],
                planned_files=["AGENTS.md"],
                github_ref="issue#10",
                lock_paths=["AGENTS.md"],
                lease_minutes=20,
                notes="owner",
            )
            report = preflight_check(
                store=store,
                change_class="shared_zone",
                scope=["AGENTS.md"],
                lock_paths=["AGENTS.md"],
            )
            self.assertFalse(report["ok"])
            self.assertTrue(report["issues"])

    def test_lease_doctor_reports_expired_lock(self) -> None:
        from coordination.coord_cli import CoordinationStore
        from coordination.coord_cli import begin_task
        from coordination.coord_cli import force_expire_lock
        from coordination.coord_cli import lease_doctor_report

        with tempfile.TemporaryDirectory() as tmpdir:
            store = self._make_store(Path(tmpdir))
            begin_task(
                store=store,
                task_id="task-doctor",
                change_class="shared_zone",
                machine="Machine 1",
                agent="codex-windows",
                base_branch="main",
                working_branch="machine1/shared/docs",
                scope=["AGENTS.md"],
                planned_files=["AGENTS.md"],
                github_ref="issue#11",
                lock_paths=["AGENTS.md"],
                lease_minutes=20,
                notes="doctor",
            )
            force_expire_lock(store=store, lock_id="lock-task-doctor-1")
            report = lease_doctor_report(store=store)
            self.assertIn("lock-task-doctor-1", report["expired_locks"])

    def _make_store(self, root: Path):
        from coordination.coord_cli import CoordinationStore

        coordination_dir = root / "coordination"
        coordination_dir.mkdir(parents=True, exist_ok=True)
        active = coordination_dir / "active_tasks.json"
        locks = coordination_dir / "shared_locks.json"
        active.write_text(json.dumps({"version": 1, "updated_at": "2026-04-03T00:00:00Z", "tasks": []}, indent=2), encoding="utf-8")
        locks.write_text(json.dumps({"version": 1, "updated_at": "2026-04-03T00:00:00Z", "locks": []}, indent=2), encoding="utf-8")
        return CoordinationStore(active_tasks_path=active, shared_locks_path=locks)


if __name__ == "__main__":
    unittest.main()
