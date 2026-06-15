"""runner 进度和站点状态保护测试。"""

from __future__ import annotations

import threading
import time
from types import SimpleNamespace
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oldironcrawler import runner as runner_module  # noqa: E402
from oldironcrawler.importer import ImportedWebsite  # noqa: E402
from oldironcrawler.runtime.store import RuntimeStore, SiteResult, SiteStageMetrics  # noqa: E402


class _FakeStore:
    def progress(self) -> dict[str, int]:
        return {"done": 7, "running": 50, "dropped": 3, "pending": 60, "failed_temp": 1}


def test_emit_progress_snapshot_translates_store_progress(monkeypatch) -> None:
    captured: dict[str, int] = {}
    monkeypatch.setattr(runner_module, "print_progress_heartbeat", lambda **kw: captured.update(kw))

    runner_module._emit_progress_snapshot(_FakeStore(), total=121)

    # 待处理 = 存储 pending(60) + failed_temp(1)，其余直传。
    assert captured == {"total": 121, "done": 7, "running": 50, "dropped": 3, "pending": 61}


def test_late_site_result_does_not_overwrite_dropped_timeout(tmp_path: Path) -> None:
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    store.prepare_job(
        input_name="sites.txt",
        fingerprint="abc",
        rows=[
            ImportedWebsite(
                input_index=1,
                raw_website="https://slow.example.com",
                website="https://slow.example.com",
                dedupe_key="slow.example.com",
            )
        ],
    )
    task = store.claim_next_site()

    assert task is not None
    store.mark_dropped(task.id, "site_deadline_exceeded")
    store.mark_done(
        task.id,
        SiteResult(
            company_name="Late Ltd",
            representative="",
            emails="late@example.com",
            website="https://slow.example.com",
        ),
    )

    progress = store.progress()
    assert progress["dropped"] == 1
    assert progress["done"] == 0
    assert store.delivery_report_rows()[0]["emails"] == ""


def test_runtime_store_drops_slow_network_errors_without_same_batch_retry(tmp_path: Path) -> None:
    slow_errors = [
        "site_open_timeout: http://slow.example.com",
        "page_batch_timeout",
        "request_slot_timeout",
        "Failed to perform, curl: (28) Operation timed out after 61533 milliseconds",
        "temporary_request: http://slow.example.com",
    ]
    for index, error_text in enumerate(slow_errors, start=1):
        store = RuntimeStore(tmp_path / f"runtime-{index}.sqlite3")
        store.prepare_job(
            input_name="sites.txt",
            fingerprint=f"abc-{index}",
            rows=[
                ImportedWebsite(
                    input_index=1,
                    raw_website="https://slow.example.com",
                    website="https://slow.example.com",
                    dedupe_key=f"slow-{index}.example.com",
                )
            ],
        )
        task = store.claim_next_site()

        assert task is not None
        assert store.mark_failed(task.id, error_text) == "dropped"
        progress = store.progress()
        assert progress["failed_temp"] == 0
        assert progress["dropped"] == 1


def test_run_crawl_session_drops_stale_site_without_waiting_for_thread(tmp_path: Path, monkeypatch) -> None:
    slow_done = threading.Event()

    class TimeoutStore:
        def __init__(self) -> None:
            self.claimed = False
            self.dropped: list[tuple[int, str]] = []
            self.done: list[int] = []

        def progress(self) -> dict[str, int]:
            pending = 0 if self.claimed else 1
            running = 1 if self.claimed and not self.dropped and not self.done else 0
            return {
                "total": 1,
                "done": len(self.done),
                "running": running,
                "dropped": len(self.dropped),
                "pending": pending,
                "failed_temp": 0,
            }

        def claim_next_site(self):
            if self.claimed:
                return None
            self.claimed = True
            return SimpleNamespace(
                id=1,
                input_index=1,
                website="https://slow.example.com",
                dedupe_key="slow.example.com",
                retry_count=0,
                company_name="Slow Ltd",
            )

        def mark_dropped(self, site_id: int, error_text: str) -> bool:
            self.dropped.append((site_id, error_text))
            return True

        def mark_done(self, site_id: int, _result: SiteResult) -> bool:
            self.done.append(site_id)
            return True

        def load_stage_metrics(self, _site_id: int) -> SiteStageMetrics:
            return SiteStageMetrics()

        def delivery_rows(self) -> list[dict[str, str]]:
            return []

    class Closable:
        def close(self) -> None:
            return None

    def fake_run_single_site(_config, _store, _learning_store, _llm_client, _page_pool, _task, *_extra):
        try:
            time.sleep(1.0)
            return SimpleNamespace(
                result=SiteResult(
                    company_name="Late Ltd",
                    representative="",
                    emails="late@example.com",
                    website="https://slow.example.com",
                ),
                learning_feedback=SimpleNamespace(
                    rep_positive_tokens=[],
                    rep_negative_tokens=[],
                    email_positive_tokens=[],
                    email_negative_tokens=[],
                ),
                stage_metrics=SiteStageMetrics(),
            )
        finally:
            slow_done.set()

    monkeypatch.setattr(runner_module, "_run_single_site", fake_run_single_site)
    monkeypatch.setattr(runner_module, "WebsiteLlmClient", lambda **_kwargs: Closable())
    monkeypatch.setattr(runner_module, "PageFetchPool", lambda _config: Closable())
    monkeypatch.setattr(
        runner_module,
        "GlobalLearningStore",
        lambda _path: SimpleNamespace(
            close=lambda: None,
            record_success=lambda *args, **kwargs: None,
            record_failure=lambda *args, **kwargs: None,
        ),
    )
    monkeypatch.setattr(runner_module, "print_site_result", lambda **_kwargs: None)
    monkeypatch.setattr(runner_module, "print_progress_heartbeat", lambda **_kwargs: None)
    monkeypatch.setattr(runner_module, "write_delivery_csv", lambda _delivery_path, _rows, **_kwargs: None)

    config = SimpleNamespace(
        llm_key="test-key",
        llm_base_url="https://example.com/v1",
        llm_model="gpt-5.4-mini",
        llm_api_style="responses",
        llm_reasoning_effort="low",
        proxy_url="",
        request_timeout_seconds=10.0,
        llm_concurrency=1,
        page_worker_count=1,
        page_host_limit=1,
        runtime_dir=tmp_path,
        site_concurrency=1,
        total_wait_seconds=0.05,
        collect_company_name_enabled=False,
        collect_email_enabled=True,
        collect_phone_enabled=False,
        extract_representative_enabled=False,
        search_representative_enabled=False,
    )
    store = TimeoutStore()

    started = time.monotonic()
    runner_module.run_crawl_session(config, store, tmp_path / "delivery.csv")
    elapsed = time.monotonic() - started
    slow_done.wait(2.0)

    assert elapsed < 0.4
    assert store.dropped == [(1, "site_deadline_exceeded")]
    assert store.done == []


def test_run_crawl_session_keeps_site_that_finishes_in_deadline_grace(tmp_path: Path, monkeypatch) -> None:
    class GraceStore:
        def __init__(self) -> None:
            self.claimed = False
            self.dropped: list[tuple[int, str]] = []
            self.done: list[int] = []

        def progress(self) -> dict[str, int]:
            pending = 0 if self.claimed else 1
            running = 1 if self.claimed and not self.dropped and not self.done else 0
            return {
                "total": 1,
                "done": len(self.done),
                "running": running,
                "dropped": len(self.dropped),
                "pending": pending,
                "failed_temp": 0,
            }

        def claim_next_site(self):
            if self.claimed:
                return None
            self.claimed = True
            return SimpleNamespace(
                id=1,
                input_index=1,
                website="https://grace.example.com",
                dedupe_key="grace.example.com",
                retry_count=0,
                company_name="Grace Ltd",
            )

        def mark_dropped(self, site_id: int, error_text: str) -> bool:
            self.dropped.append((site_id, error_text))
            return True

        def mark_done(self, site_id: int, _result: SiteResult) -> bool:
            self.done.append(site_id)
            return True

        def load_stage_metrics(self, _site_id: int) -> SiteStageMetrics:
            return SiteStageMetrics(fetched_page_count=1)

        def delivery_rows(self) -> list[dict[str, str]]:
            return []

    class Closable:
        def close(self) -> None:
            return None

    def fake_run_single_site(_config, _store, _learning_store, _llm_client, _page_pool, _task, *_extra):
        time.sleep(0.23)
        return SimpleNamespace(
            result=SiteResult(
                company_name="Grace Ltd",
                representative="",
                emails="contact@grace.example.com",
                website="https://grace.example.com",
            ),
            learning_feedback=SimpleNamespace(
                rep_positive_tokens=[],
                rep_negative_tokens=[],
                email_positive_tokens=[],
                email_negative_tokens=[],
            ),
            stage_metrics=SiteStageMetrics(fetched_page_count=1),
        )

    monkeypatch.setattr(runner_module, "_run_single_site", fake_run_single_site)
    monkeypatch.setattr(runner_module, "WebsiteLlmClient", lambda **_kwargs: Closable())
    monkeypatch.setattr(runner_module, "PageFetchPool", lambda _config: Closable())
    monkeypatch.setattr(
        runner_module,
        "GlobalLearningStore",
        lambda _path: SimpleNamespace(
            close=lambda: None,
            record_success=lambda *args, **kwargs: None,
            record_failure=lambda *args, **kwargs: None,
        ),
    )
    monkeypatch.setattr(runner_module, "print_site_result", lambda **_kwargs: None)
    monkeypatch.setattr(runner_module, "print_progress_heartbeat", lambda **_kwargs: None)
    monkeypatch.setattr(runner_module, "write_delivery_csv", lambda _delivery_path, _rows, **_kwargs: None)

    config = SimpleNamespace(
        llm_key="test-key",
        llm_base_url="https://example.com/v1",
        llm_model="gpt-5.4-mini",
        llm_api_style="responses",
        llm_reasoning_effort="low",
        proxy_url="",
        request_timeout_seconds=10.0,
        llm_concurrency=1,
        page_worker_count=1,
        page_host_limit=1,
        runtime_dir=tmp_path,
        site_concurrency=1,
        total_wait_seconds=0.2,
        collect_company_name_enabled=False,
        collect_email_enabled=True,
        collect_phone_enabled=False,
        extract_representative_enabled=False,
        search_representative_enabled=False,
    )
    store = GraceStore()

    runner_module.run_crawl_session(config, store, tmp_path / "delivery.csv")

    assert store.done == [1]
    assert store.dropped == []
