from pathlib import Path

from malaysia_crawler.streaming.store import PipelineStore


def test_store_queue_and_final_deduplicate(tmp_path: Path) -> None:
    store = PipelineStore(tmp_path / "pipeline.db")
    store.upsert_ctos_company(
        normalized_name="securepaysdnbhd",
        company_name="Securepay Sdn Bhd",
        registration_no="1358366-A",
        prefix="s",
        page=1,
    )
    assert store.has_ctos_name("securepaysdnbhd") is True

    store.enqueue_snov_task(
        normalized_name="securepaysdnbhd",
        company_name="Securepay Sdn Bhd",
        domain="securepay.my",
        company_manager="AMIR",
        contact_email="hello@securepay.my",
        company_id=381082,
        contact_phone="+60322424255",
    )
    task = store.claim_snov_task()
    assert task is not None
    assert task.contact_phone == "+60322424255"
    store.mark_snov_done(
        normalized_name=task.normalized_name,
        final_status="done",
        contact_eamils=["hello@securepay.my", "support@securepay.my"],
        company_name=task.company_name,
        domain=task.domain,
        company_manager=task.company_manager,
        company_id=task.company_id,
        phone=task.contact_phone,
    )

    # 中文注释：同一公司再次入队，最终成品仍应按公司去重更新。
    store.enqueue_snov_task(
        normalized_name="securepaysdnbhd",
        company_name="Securepay Sdn Bhd",
        domain="securepay.my",
        company_manager="AMIR",
        contact_email="hello@securepay.my",
        company_id=381082,
    )
    task2 = store.claim_snov_task()
    assert task2 is not None
    store.mark_snov_done(
        normalized_name=task2.normalized_name,
        final_status="done",
        contact_eamils=["hello@securepay.my"],
        company_name=task2.company_name,
        domain=task2.domain,
        company_manager=task2.company_manager,
        company_id=task2.company_id,
        phone=task2.contact_phone,
    )

    stats = store.get_stats()
    assert stats["final_companies"] == 1
    final_row = store._conn.execute(
        "SELECT phone FROM final_companies WHERE normalized_name = 'securepaysdnbhd'"
    ).fetchone()
    assert final_row is not None
    assert final_row["phone"] == "+60322424255"
    store.close()


def test_store_no_email_with_manager_will_not_land_final(tmp_path: Path) -> None:
    store = PipelineStore(tmp_path / "pipeline.db")
    store.enqueue_snov_task(
        normalized_name="noemailcompany",
        company_name="No Email Company",
        domain="noemail.example",
        company_manager="AMIR",
        contact_email="",
        company_id=900123,
    )
    task = store.claim_snov_task()
    assert task is not None
    inserted = store.mark_snov_done(
        normalized_name=task.normalized_name,
        final_status="no_email",
        contact_eamils=[],
        company_name=task.company_name,
        domain=task.domain,
        company_manager=task.company_manager,
        company_id=task.company_id,
        phone=task.contact_phone,
    )
    assert inserted is False
    stats = store.get_stats()
    assert stats["queue_no_email"] == 1
    assert stats["final_companies"] == 0
    row = store._conn.execute(
        "SELECT contact_eamils FROM final_companies WHERE normalized_name = 'noemailcompany'"
    ).fetchone()
    assert row is None
    store.close()


def test_late_ctos_match_can_enqueue_existing_businesslist_scan(tmp_path: Path) -> None:
    store = PipelineStore(tmp_path / "pipeline.db")
    store.mark_businesslist_scan(
        company_id=500001,
        normalized_name="latematchsdnbhd",
        company_name="Late Match Sdn Bhd",
        domain="late-match.my",
        company_manager="MANAGER",
        contact_email="hello@late-match.my",
        status="not_in_ctos",
    )
    queued = store.enqueue_from_businesslist_if_ready("latematchsdnbhd")
    assert queued is True
    task = store.claim_snov_task()
    assert task is not None
    assert task.normalized_name == "latematchsdnbhd"
    assert task.domain == "late-match.my"
    store.close()


def test_runtime_repairs_clean_404_noise_and_allow_retry_error_rows(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    store = PipelineStore(db_path)
    store.mark_businesslist_scan(
        company_id=17,
        normalized_name="404errorpagenotfound",
        company_name="404 error: Page not found",
        domain="",
        company_manager="",
        contact_email="",
        status="not_in_ctos",
    )
    store.mark_businesslist_scan(
        company_id=88,
        normalized_name="badstatus",
        company_name="Bad Status Sdn Bhd",
        domain="bad.my",
        company_manager="X",
        contact_email="",
        status="error:ProtocolError",
    )
    store.mark_businesslist_scan(
        company_id=99,
        normalized_name="managerfailedcompany",
        company_name="Manager Failed Company",
        domain="mf.example",
        company_manager="",
        contact_email="",
        status="queued_manager_enrich",
    )
    store.enqueue_manager_task(
        normalized_name="managerfailedcompany",
        company_name="Manager Failed Company",
        domain="mf.example",
        contact_email="",
        company_id=99,
        contact_phone="",
    )
    task = store.claim_manager_task()
    assert task is not None
    store.mark_manager_failed(
        normalized_name=task.normalized_name,
        retries=1,
        round_index=1,
        candidate_pool=[],
        tried_urls=[],
        error_text="manager_not_found",
    )
    store.enqueue_snov_task(
        normalized_name="404errorpagenotfound",
        company_name="404 error: Page not found",
        domain="bad.my",
        company_manager="X",
        contact_email="",
        company_id=17,
    )
    store.enqueue_snov_task(
        normalized_name="noemailrepair",
        company_name="No Email Repair Company",
        domain="repair.example",
        company_manager="MANAGER",
        contact_email="",
        company_id=12345,
        contact_phone="+60000000000",
    )
    no_email_task = store.claim_snov_task()
    assert no_email_task is not None
    if no_email_task.normalized_name != "noemailrepair":
        no_email_task = store.claim_snov_task()
        assert no_email_task is not None
    assert no_email_task.normalized_name == "noemailrepair"
    store.mark_snov_done(
        normalized_name=no_email_task.normalized_name,
        final_status="no_email",
        contact_eamils=[],
        company_name=no_email_task.company_name,
        domain=no_email_task.domain,
        company_manager=no_email_task.company_manager,
        company_id=no_email_task.company_id,
        phone=no_email_task.contact_phone,
    )
    # 中文注释：重开后不应把 no_email 回填为成品。
    store.close()

    # 中文注释：重开数据库触发启动修复逻辑。
    store2 = PipelineStore(db_path)
    row = store2._conn.execute(
        "SELECT company_name, normalized_name, status FROM businesslist_scan WHERE company_id = 17"
    ).fetchone()
    assert row is not None
    assert row["status"] == "miss"
    assert row["company_name"] == ""
    assert row["normalized_name"] == ""
    queue_row = store2._conn.execute(
        "SELECT 1 FROM snov_queue WHERE normalized_name = '404errorpagenotfound'"
    ).fetchone()
    assert queue_row is None
    assert store2.is_businesslist_scanned(88) is False
    repaired = store2._conn.execute(
        "SELECT status FROM businesslist_scan WHERE company_id = 99"
    ).fetchone()
    assert repaired is not None
    assert repaired["status"] == "no_manager"
    repaired_final = store2._conn.execute(
        "SELECT company_manager, contact_eamils, phone FROM final_companies WHERE normalized_name = 'noemailrepair'"
    ).fetchone()
    assert repaired_final is None
    store2.close()


def test_get_next_businesslist_id_respects_configured_start(tmp_path: Path) -> None:
    store = PipelineStore(tmp_path / "pipeline.db")
    store.set_next_businesslist_id(245)
    assert store.get_next_businesslist_id(381000) == 381000
    store.close()


def test_has_ctos_registration_ignores_dash_and_space(tmp_path: Path) -> None:
    store = PipelineStore(tmp_path / "pipeline.db")
    store.upsert_ctos_company(
        normalized_name="securepaysdnbhd",
        company_name="Securepay Sdn Bhd",
        registration_no="1358366-A",
        prefix="s",
        page=1,
    )
    assert store.has_ctos_registration("1358366a") is True
    assert store.has_ctos_registration("1358366-a") is True
    assert store.has_ctos_registration("1358366 A") is True
    assert store.has_ctos_registration("0000000A") is False
    store.close()


def test_store_reconnect_keeps_read_write(tmp_path: Path) -> None:
    store = PipelineStore(tmp_path / "pipeline.db")
    store.upsert_ctos_company(
        normalized_name="reconnectcompany",
        company_name="Reconnect Company",
        registration_no="R-1",
        prefix="r",
        page=1,
    )
    store.reconnect()
    assert store.has_ctos_name("reconnectcompany") is True
    store.close()


def test_backfill_unmatched_businesslist_to_queue(tmp_path: Path) -> None:
    store = PipelineStore(tmp_path / "pipeline.db")
    store.mark_businesslist_scan(
        company_id=900001,
        normalized_name="abcsdnbhd",
        company_name="ABC Sdn Bhd",
        domain="abc.my",
        company_manager="MANAGER",
        contact_email="hello@abc.my",
        status="not_in_ctos",
    )
    changed = store.backfill_unmatched_businesslist_to_queue(batch_size=10)
    assert changed == 1
    task = store.claim_snov_task()
    assert task is not None
    assert task.domain == "abc.my"
    stats = store.get_stats()
    assert stats["businesslist_queued_without_ctos"] == 1
    store.close()


def test_requeue_stale_running_tasks(tmp_path: Path) -> None:
    store = PipelineStore(tmp_path / "pipeline.db")
    store._conn.execute(
        """
        INSERT INTO snov_queue(
            normalized_name, company_name, domain, company_manager, contact_email,
            company_id, status, retries, last_error, updated_at
        ) VALUES(?, ?, ?, ?, ?, ?, 'running', 0, '', ?)
        """,
        (
            "staletask",
            "Stale Task Sdn Bhd",
            "stale.example",
            "MANAGER",
            "",
            1,
            "2000-01-01T00:00:00Z",
        ),
    )
    store._conn.commit()
    recovered = store.requeue_stale_running_tasks(older_than_seconds=60)
    assert recovered == 1
    row = store._conn.execute(
        "SELECT status FROM snov_queue WHERE normalized_name = 'staletask'"
    ).fetchone()
    assert row is not None
    assert row["status"] == "pending"
    store.close()


def test_backfill_no_manager_to_queue(tmp_path: Path) -> None:
    store = PipelineStore(tmp_path / "pipeline.db")
    store.mark_businesslist_scan(
        company_id=900002,
        normalized_name="nomanagercompany",
        company_name="No Manager Sdn Bhd",
        domain="nomanager.my",
        company_manager="",
        contact_email="hello@nomanager.my",
        status="no_manager",
    )
    changed = store.backfill_no_manager_to_queue(batch_size=10)
    assert changed == 1
    task = store.claim_snov_task()
    assert task is not None
    assert task.company_manager == ""
    stats = store.get_stats()
    assert stats["businesslist_queued_no_manager"] == 1
    store.close()


def test_defer_snov_task_and_recover_429_failed(tmp_path: Path) -> None:
    store = PipelineStore(tmp_path / "pipeline.db")
    store.enqueue_snov_task(
        normalized_name="ratelimitcompany",
        company_name="Rate Limit Company",
        domain="ratelimit.example",
        company_manager="",
        contact_email="",
        company_id=123,
    )
    task = store.claim_snov_task()
    assert task is not None

    store.defer_snov_task(
        normalized_name="ratelimitcompany",
        delay_seconds=120,
        error_text="HTTPError: 429 Too Many Requests",
    )
    assert store.claim_snov_task() is None

    store._conn.execute(
        "UPDATE snov_queue SET updated_at = '2000-01-01T00:00:00Z' WHERE normalized_name = 'ratelimitcompany'"
    )
    store._conn.commit()
    task2 = store.claim_snov_task()
    assert task2 is not None

    store.mark_snov_failed(
        normalized_name="ratelimitcompany",
        error_text="HTTPError: 429 Too Many Requests",
        max_retries=1,
    )
    revived = store.requeue_rate_limited_failed_tasks()
    assert revived == 1
    row = store._conn.execute(
        "SELECT status, retries FROM snov_queue WHERE normalized_name = 'ratelimitcompany'"
    ).fetchone()
    assert row is not None
    assert row["status"] == "pending"
    assert int(row["retries"]) == 0
    store.close()


def test_manager_queue_lifecycle(tmp_path: Path) -> None:
    store = PipelineStore(tmp_path / "pipeline.db")
    store.enqueue_manager_task(
        normalized_name="nomanagercompany",
        company_name="No Manager Sdn Bhd",
        domain="nomanager.my",
        contact_email="hello@nomanager.my",
        company_id=900003,
        contact_phone="+60112223344",
    )
    task = store.claim_manager_task()
    assert task is not None
    assert task.company_name == "No Manager Sdn Bhd"
    assert task.contact_phone == "+60112223344"
    store.defer_manager_task(
        normalized_name=task.normalized_name,
        delay_seconds=1,
        retries=1,
        round_index=1,
        candidate_pool=["https://nomanager.my/about"],
        tried_urls=["https://nomanager.my/about"],
        error_text="manager_not_found",
    )
    store._conn.execute(
        "UPDATE manager_enrich_queue SET updated_at = '2000-01-01T00:00:00Z' WHERE normalized_name = 'nomanagercompany'"
    )
    store._conn.commit()
    task2 = store.claim_manager_task()
    assert task2 is not None
    assert task2.retries == 1
    assert task2.round_index == 1
    store.mark_manager_done(
        normalized_name=task2.normalized_name,
        retries=2,
        round_index=2,
        candidate_pool=task2.candidate_pool,
        tried_urls=task2.tried_urls,
    )
    stats = store.get_stats()
    assert stats["manager_queue_done"] == 1
    store.close()
