import requests
import pytest
import sqlite3
from types import SimpleNamespace

from malaysia_crawler.businesslist.models import BusinessListCompany
from malaysia_crawler.businesslist.cdp_crawler import BusinessListBlockedError
from malaysia_crawler.streaming.pipeline import MalaysiaStreamingPipeline
from malaysia_crawler.streaming.pipeline import StreamingPipelineConfig
from malaysia_crawler.streaming.pipeline import _backoff_seconds
from malaysia_crawler.streaming.pipeline import _extract_http_status
from malaysia_crawler.streaming.pipeline import _extract_retry_after_seconds
from malaysia_crawler.streaming.pipeline import _is_transient_error
from malaysia_crawler.streaming.pipeline import _pick_contact_phone
from malaysia_crawler.streaming.store_manager import ManagerTask


class _DummyCtosCrawler:
    def fetch_list_page(self, prefix: str, page: int):  # noqa: D401
        raise RuntimeError("不应被调用")


class _DummyBusinessListCrawler:
    def fetch_company_profile(self, company_id: int):  # noqa: D401
        return None

    def close(self) -> None:
        return


class _DummyBusinessListCrawlerAlwaysBlocked:
    def __init__(self) -> None:
        self.calls = 0

    def fetch_company_profile(self, company_id: int):  # noqa: D401
        self.calls += 1
        raise BusinessListBlockedError("cf blocked")

    def close(self) -> None:
        return


class _DummySnovClient:
    def get_domain_emails(self, domain: str):  # noqa: D401
        return []


class _DummySnovClient422:
    def get_domain_emails(self, domain: str):  # noqa: D401
        response = requests.Response()
        response.status_code = 422
        raise requests.exceptions.HTTPError(response=response)


class _DummySnovClient429:
    def get_domain_emails(self, domain: str):  # noqa: D401
        response = requests.Response()
        response.status_code = 429
        response.headers["Retry-After"] = "77"
        raise requests.exceptions.HTTPError(response=response)


class _DummyManagerAgentSuccess:
    def enrich_manager(self, *, company_name, domain, candidate_pool, tried_urls):  # noqa: D401
        return SimpleNamespace(
            success=True,
            manager_name="AMIR HARIS",
            manager_role="Managing Director",
            evidence_url="https://example.com/about",
            evidence_quote="Managing Director: AMIR HARIS",
            candidate_pool=["https://example.com/about"],
            tried_urls=["https://example.com/about"],
            error_code="",
            error_text="",
            retry_after=0.0,
        )


class _DummyManagerAgentNoCandidate:
    def __init__(self) -> None:
        self.calls = 0
        self.last_candidate_pool: list[str] = []

    def enrich_manager(self, *, company_name, domain, candidate_pool, tried_urls):  # noqa: D401
        self.calls += 1
        self.last_candidate_pool = list(candidate_pool)
        return SimpleNamespace(
            success=False,
            manager_name="",
            manager_role="",
            evidence_url="",
            evidence_quote="",
            candidate_pool=candidate_pool,
            tried_urls=tried_urls,
            error_code="no_candidate",
            error_text="没有可尝试的候选链接。",
            retry_after=0.0,
        )


class _DummyManagerAgentInfraError:
    def enrich_manager(self, *, company_name, domain, candidate_pool, tried_urls):  # noqa: D401
        return SimpleNamespace(
            success=False,
            manager_name="",
            manager_role="",
            evidence_url="",
            evidence_quote="",
            candidate_pool=candidate_pool,
            tried_urls=tried_urls,
            error_code="InternalServerError",
            error_text="gateway 500",
            retry_after=0.0,
        )


class _DummyManagerAgentNoKeyRuntimeError:
    def enrich_manager(self, *, company_name, domain, candidate_pool, tried_urls):  # noqa: D401
        raise RuntimeError("没有可用 firecrawl key。")


def test_extract_http_status_from_http_error() -> None:
    response = requests.Response()
    response.status_code = 429
    error = requests.exceptions.HTTPError(response=response)
    assert _extract_http_status(error) == 429


def test_extract_retry_after_seconds_from_http_error() -> None:
    response = requests.Response()
    response.status_code = 429
    response.headers["Retry-After"] = "61"
    error = requests.exceptions.HTTPError(response=response)
    assert _extract_retry_after_seconds(error) == 61


def test_is_transient_error_recognizes_common_transient_errors() -> None:
    assert _is_transient_error(BusinessListBlockedError("blocked")) is True
    assert _is_transient_error(requests.exceptions.Timeout("timeout")) is True
    response = requests.Response()
    response.status_code = 503
    assert _is_transient_error(requests.exceptions.HTTPError(response=response)) is True


def test_backoff_seconds_respects_cap() -> None:
    assert _backoff_seconds(base=2.0, attempt=1, cap=20.0) == 2.0
    assert _backoff_seconds(base=2.0, attempt=3, cap=20.0) == 8.0
    assert _backoff_seconds(base=2.0, attempt=10, cap=20.0) == 20.0


def test_run_worker_guard_retries_transient_store_error(tmp_path, monkeypatch) -> None:
    pipeline = MalaysiaStreamingPipeline(
        config=StreamingPipelineConfig(db_path=tmp_path / "pipeline.db", retry_sleep_seconds=0.01),
        ctos_crawler=_DummyCtosCrawler(),
        businesslist_crawler=_DummyBusinessListCrawler(),
        snov_client=_DummySnovClient(),
    )
    calls = {"count": 0}
    reconnect_calls = {"count": 0}

    def _worker() -> None:
        calls["count"] += 1
        if calls["count"] == 1:
            raise sqlite3.OperationalError("disk I/O error")

    def _fake_reconnect() -> None:
        reconnect_calls["count"] += 1

    monkeypatch.setattr(pipeline.store, "reconnect", _fake_reconnect)
    monkeypatch.setattr("malaysia_crawler.streaming.pipeline.time.sleep", lambda _s: None)

    pipeline._run_worker_guard("manager-worker-21", _worker)
    assert calls["count"] == 2
    assert reconnect_calls["count"] == 1
    assert pipeline._get_fatal() == ""
    pipeline.store.close()


def test_pick_contact_phone_prefers_contact_numbers_then_director_phone() -> None:
    with_contact = BusinessListCompany(
        company_id=1,
        company_url="https://www.businesslist.my/company/1/a",
        company_name="A",
        registration_code="",
        address="",
        contact_numbers=["+60312345678"],
        employees=[{"name": "Director A", "role": "DIRECTOR", "phone": "+60129998888"}],
    )
    assert _pick_contact_phone(with_contact) == "+60312345678"

    director_only = BusinessListCompany(
        company_id=2,
        company_url="https://www.businesslist.my/company/2/b",
        company_name="B",
        registration_code="",
        address="",
        contact_numbers=[],
        employees=[{"name": "Director B", "role": "DIRECTOR", "phone": "+60135556666"}],
    )
    assert _pick_contact_phone(director_only) == "+60135556666"


def test_ctos_worker_pauses_when_businesslist_cf_waiting(tmp_path, monkeypatch) -> None:
    pipeline = MalaysiaStreamingPipeline(
        config=StreamingPipelineConfig(db_path=tmp_path / "pipeline.db", retry_sleep_seconds=0.01),
        ctos_crawler=_DummyCtosCrawler(),
        businesslist_crawler=_DummyBusinessListCrawler(),
        snov_client=_DummySnovClient(),
    )
    pipeline._bl_cf_wait_event.set()
    slept = {"calls": 0}

    def _fake_sleep(_seconds: float) -> None:
        slept["calls"] += 1
        pipeline.stop_event.set()

    monkeypatch.setattr("malaysia_crawler.streaming.pipeline.time.sleep", _fake_sleep)
    pipeline._ctos_worker()
    assert slept["calls"] >= 1
    pipeline.store.close()


def test_businesslist_cf_blocked_id_will_be_marked_error_and_skipped(tmp_path, monkeypatch) -> None:
    blocked = _DummyBusinessListCrawlerAlwaysBlocked()
    pipeline = MalaysiaStreamingPipeline(
        config=StreamingPipelineConfig(
            db_path=tmp_path / "pipeline.db",
            businesslist_start_id=100,
            businesslist_end_id=100,
            retry_sleep_seconds=0.01,
            businesslist_cf_block_retry_limit=2,
            businesslist_cf_backoff_base_seconds=0.01,
            businesslist_cf_backoff_cap_seconds=0.01,
        ),
        ctos_crawler=_DummyCtosCrawler(),
        businesslist_crawler=blocked,
        snov_client=_DummySnovClient(),
    )

    def _fake_sleep(_seconds: float) -> None:
        row = pipeline.store._conn.execute(  # noqa: SLF001
            "SELECT status FROM businesslist_scan WHERE company_id = 100"
        ).fetchone()
        if row is not None and str(row["status"]) == "error:cf_blocked":
            pipeline.stop_event.set()

    monkeypatch.setattr("malaysia_crawler.streaming.pipeline.time.sleep", _fake_sleep)
    pipeline._businesslist_worker(0)
    row = pipeline.store._conn.execute(  # noqa: SLF001
        "SELECT status FROM businesslist_scan WHERE company_id = 100"
    ).fetchone()
    assert row is not None
    assert str(row["status"]) == "error:cf_blocked"
    assert blocked.calls == 2
    pipeline.store.close()


def test_acquire_instance_lock_blocks_existing_process(tmp_path, monkeypatch) -> None:
    pipeline = MalaysiaStreamingPipeline(
        config=StreamingPipelineConfig(db_path=tmp_path / "pipeline.db"),
        ctos_crawler=_DummyCtosCrawler(),
        businesslist_crawler=_DummyBusinessListCrawler(),
        snov_client=_DummySnovClient(),
    )
    lock_path = (tmp_path / "pipeline.db").with_suffix(".pipeline.lock")
    lock_path.write_text("99999", encoding="utf-8")
    monkeypatch.setattr(pipeline, "_is_pid_running", lambda pid: pid == 99999)
    with pytest.raises(RuntimeError):
        pipeline._acquire_instance_lock()
    pipeline.store.close()


def test_snov_fast_path_uses_businesslist_contact_email(tmp_path) -> None:
    pipeline = MalaysiaStreamingPipeline(
        config=StreamingPipelineConfig(db_path=tmp_path / "pipeline.db", contact_email_fast_path=True),
        ctos_crawler=_DummyCtosCrawler(),
        businesslist_crawler=_DummyBusinessListCrawler(),
        snov_client=_DummySnovClient(),
    )
    pipeline.store.enqueue_snov_task(
        normalized_name="securepaysdnbhd",
        company_name="Securepay Sdn Bhd",
        domain="securepay.my",
        company_manager="AMIR",
        contact_email="hello@securepay.my",
        company_id=381082,
    )
    task = pipeline.store.claim_snov_task()
    assert task is not None
    pipeline._process_snov_task(task)
    stats = pipeline.store.get_stats()
    assert stats["queue_done"] == 1
    assert stats["final_companies"] == 1
    pipeline.store.close()


def test_snov_done_without_manager_will_not_land_final(tmp_path) -> None:
    pipeline = MalaysiaStreamingPipeline(
        config=StreamingPipelineConfig(db_path=tmp_path / "pipeline.db", contact_email_fast_path=True),
        ctos_crawler=_DummyCtosCrawler(),
        businesslist_crawler=_DummyBusinessListCrawler(),
        snov_client=_DummySnovClient(),
    )
    pipeline.store.enqueue_snov_task(
        normalized_name="nomanager",
        company_name="No Manager Company",
        domain="example.com",
        company_manager="",
        contact_email="hello@example.com",
        company_id=900010,
    )
    task = pipeline.store.claim_snov_task()
    assert task is not None
    pipeline._process_snov_task(task)
    stats = pipeline.store.get_stats()
    assert stats["queue_done"] == 1
    assert stats["final_companies"] == 0
    pipeline.store.close()


def test_snov_422_with_contact_email_still_writes_final(tmp_path) -> None:
    pipeline = MalaysiaStreamingPipeline(
        config=StreamingPipelineConfig(db_path=tmp_path / "pipeline.db", contact_email_fast_path=False),
        ctos_crawler=_DummyCtosCrawler(),
        businesslist_crawler=_DummyBusinessListCrawler(),
        snov_client=_DummySnovClient422(),
    )
    pipeline.store.enqueue_snov_task(
        normalized_name="samplecompany",
        company_name="Sample Company Sdn Bhd",
        domain="facebook.com",
        company_manager="MANAGER",
        contact_email="owner@sample.com",
        company_id=900001,
    )
    task = pipeline.store.claim_snov_task()
    assert task is not None
    pipeline._process_snov_task(task)
    stats = pipeline.store.get_stats()
    assert stats["queue_done"] == 1
    assert stats["final_companies"] == 1
    assert stats["queue_no_email"] == 0
    pipeline.store.close()


def test_snov_429_is_deferred_instead_of_failed(tmp_path) -> None:
    pipeline = MalaysiaStreamingPipeline(
        config=StreamingPipelineConfig(db_path=tmp_path / "pipeline.db", contact_email_fast_path=False),
        ctos_crawler=_DummyCtosCrawler(),
        businesslist_crawler=_DummyBusinessListCrawler(),
        snov_client=_DummySnovClient429(),
    )
    pipeline.store.enqueue_snov_task(
        normalized_name="ratelimitcompany",
        company_name="Rate Limit Company",
        domain="ratelimit.example",
        company_manager="",
        contact_email="",
        company_id=900002,
    )
    task = pipeline.store.claim_snov_task()
    assert task is not None
    pipeline._process_snov_task(task)
    stats = pipeline.store.get_stats()
    assert stats["queue_failed"] == 0
    assert stats["queue_pending"] == 1
    pipeline.store.close()


def test_snov_no_email_with_manager_will_not_write_final(tmp_path) -> None:
    pipeline = MalaysiaStreamingPipeline(
        config=StreamingPipelineConfig(db_path=tmp_path / "pipeline.db", contact_email_fast_path=False),
        ctos_crawler=_DummyCtosCrawler(),
        businesslist_crawler=_DummyBusinessListCrawler(),
        snov_client=_DummySnovClient(),
    )
    pipeline.store.enqueue_snov_task(
        normalized_name="noemailmanager",
        company_name="No Email Manager Company",
        domain="example.com",
        company_manager="MANAGER",
        contact_email="",
        company_id=900003,
    )
    task = pipeline.store.claim_snov_task()
    assert task is not None
    pipeline._process_snov_task(task)
    stats = pipeline.store.get_stats()
    assert stats["queue_no_email"] == 1
    assert stats["final_companies"] == 0
    pipeline.store.close()


def test_manager_success_will_enqueue_snov_task(tmp_path) -> None:
    pipeline = MalaysiaStreamingPipeline(
        config=StreamingPipelineConfig(db_path=tmp_path / "pipeline.db", contact_email_fast_path=True),
        ctos_crawler=_DummyCtosCrawler(),
        businesslist_crawler=_DummyBusinessListCrawler(),
        snov_client=_DummySnovClient(),
        manager_agent=_DummyManagerAgentSuccess(),
    )
    pipeline.store.mark_businesslist_scan(
        company_id=901001,
        normalized_name="nomanagercompany",
        company_name="No Manager Company",
        domain="example.com",
        company_manager="",
        contact_email="hello@example.com",
        status="queued_manager_enrich",
    )
    pipeline.store.enqueue_manager_task(
        normalized_name="nomanagercompany",
        company_name="No Manager Company",
        domain="example.com",
        contact_email="hello@example.com",
        company_id=901001,
        contact_phone="",
    )
    task = pipeline.store.claim_manager_task()
    assert task is not None
    pipeline._process_manager_task(task)
    stats = pipeline.store.get_stats()
    assert stats["manager_queue_done"] == 1
    assert stats["queue_pending"] == 1
    snov_task = pipeline.store.claim_snov_task()
    assert snov_task is not None
    assert snov_task.company_manager == "AMIR HARIS"
    pipeline.store.close()


def test_manager_no_candidate_will_fail_without_retry(tmp_path) -> None:
    manager_agent = _DummyManagerAgentNoCandidate()
    pipeline = MalaysiaStreamingPipeline(
        config=StreamingPipelineConfig(db_path=tmp_path / "pipeline.db", contact_email_fast_path=True),
        ctos_crawler=_DummyCtosCrawler(),
        businesslist_crawler=_DummyBusinessListCrawler(),
        snov_client=_DummySnovClient(),
        manager_agent=manager_agent,
    )
    pipeline.store.mark_businesslist_scan(
        company_id=901002,
        normalized_name="nomanagercompany2",
        company_name="No Manager Company 2",
        domain="example.com",
        company_manager="",
        contact_email="hello@example.com",
        status="queued_manager_enrich",
    )
    pipeline.store.enqueue_manager_task(
        normalized_name="nomanagercompany2",
        company_name="No Manager Company 2",
        domain="example.com",
        contact_email="hello@example.com",
        company_id=901002,
        contact_phone="",
    )
    task = pipeline.store.claim_manager_task()
    assert task is not None
    pipeline._process_manager_task(task)
    stats = pipeline.store.get_stats()
    assert manager_agent.calls == 1
    assert stats["manager_queue_failed"] == 1
    assert stats["manager_queue_pending"] == 0
    assert stats["queue_pending"] == 0
    assert manager_agent.last_candidate_pool
    pipeline.store.close()


def test_manager_infra_error_will_defer_without_consuming_round(tmp_path) -> None:
    manager_agent = _DummyManagerAgentInfraError()
    pipeline = MalaysiaStreamingPipeline(
        config=StreamingPipelineConfig(db_path=tmp_path / "pipeline.db", retry_sleep_seconds=0.1),
        ctos_crawler=_DummyCtosCrawler(),
        businesslist_crawler=_DummyBusinessListCrawler(),
        snov_client=_DummySnovClient(),
        manager_agent=manager_agent,
    )
    pipeline.store.mark_businesslist_scan(
        company_id=901003,
        normalized_name="infraerrorcompany",
        company_name="Infra Error Company",
        domain="example.com",
        company_manager="",
        contact_email="hello@example.com",
        status="queued_manager_enrich",
    )
    pipeline.store.enqueue_manager_task(
        normalized_name="infraerrorcompany",
        company_name="Infra Error Company",
        domain="example.com",
        contact_email="hello@example.com",
        company_id=901003,
        contact_phone="",
    )
    task = pipeline.store.claim_manager_task()
    assert task is not None
    pipeline._process_manager_task(task)
    stats = pipeline.store.get_stats()
    assert stats["manager_queue_failed"] == 0
    assert stats["manager_queue_pending"] == 1
    row = pipeline.store._conn.execute(  # noqa: SLF001
        "SELECT retries, round_index FROM manager_enrich_queue WHERE normalized_name = ?",
        ("infraerrorcompany",),
    ).fetchone()
    assert row is not None
    assert int(row["retries"]) == 0
    assert int(row["round_index"]) == 0
    pipeline.store.close()


def test_manager_no_key_runtime_error_will_defer_without_consuming_round(tmp_path) -> None:
    pipeline = MalaysiaStreamingPipeline(
        config=StreamingPipelineConfig(db_path=tmp_path / "pipeline.db", retry_sleep_seconds=0.1),
        ctos_crawler=_DummyCtosCrawler(),
        businesslist_crawler=_DummyBusinessListCrawler(),
        snov_client=_DummySnovClient(),
        manager_agent=_DummyManagerAgentNoKeyRuntimeError(),
    )
    pipeline.store.mark_businesslist_scan(
        company_id=901004,
        normalized_name="nokeycompany",
        company_name="No Key Company",
        domain="example.com",
        company_manager="",
        contact_email="hello@example.com",
        status="queued_manager_enrich",
    )
    pipeline.store.enqueue_manager_task(
        normalized_name="nokeycompany",
        company_name="No Key Company",
        domain="example.com",
        contact_email="hello@example.com",
        company_id=901004,
        contact_phone="",
    )
    task = pipeline.store.claim_manager_task()
    assert task is not None
    pipeline._process_manager_task(task)
    stats = pipeline.store.get_stats()
    assert stats["manager_queue_failed"] == 0
    assert stats["manager_queue_pending"] == 1
    row = pipeline.store._conn.execute(  # noqa: SLF001
        "SELECT retries, round_index, last_error FROM manager_enrich_queue WHERE normalized_name = ?",
        ("nokeycompany",),
    ).fetchone()
    assert row is not None
    assert int(row["retries"]) == 0
    assert int(row["round_index"]) == 0
    assert "firecrawl key" in str(row["last_error"]).lower()
    pipeline.store.close()
