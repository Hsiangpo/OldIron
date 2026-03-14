import sqlite3
from pathlib import Path

from malaysia_crawler.manager_agent.key_pool import FirecrawlKeyPool
from malaysia_crawler.manager_agent.llm_client import ManagerExtractResult
from malaysia_crawler.manager_agent.service import ManagerAgentService


def test_is_valid_manager_accepts_ceo_role() -> None:
    service = ManagerAgentService.__new__(ManagerAgentService)
    result = ManagerExtractResult(
        manager_name="Alice Tan",
        manager_role="Chief Executive Officer",
        evidence_url="https://example.com/about",
        evidence_quote="Chief Executive Officer: Alice Tan",
        confidence=0.9,
    )
    assert service._is_valid_manager(result) is True


def test_is_valid_manager_rejects_non_management_role() -> None:
    service = ManagerAgentService.__new__(ManagerAgentService)
    result = ManagerExtractResult(
        manager_name="Bob Lee",
        manager_role="Sales Associate",
        evidence_url="https://example.com/team",
        evidence_quote="Sales Associate: Bob Lee",
        confidence=0.9,
    )
    assert service._is_valid_manager(result) is False


def test_key_pool_init_will_reset_stale_inflight(tmp_path: Path) -> None:
    db_path = tmp_path / "firecrawl_keys.db"
    key_file = tmp_path / "keys.txt"
    key_file.write_text("fc-test-key-a\nfc-test-key-b\n", encoding="utf-8")

    pool = FirecrawlKeyPool(
        keys=["fc-test-key-a", "fc-test-key-b"],
        key_file=key_file,
        db_path=db_path,
    )
    lease = pool.acquire()
    pool.release(lease)

    conn = sqlite3.connect(str(db_path))
    conn.execute("UPDATE keys SET in_flight = 2")
    conn.commit()
    conn.close()

    FirecrawlKeyPool(
        keys=["fc-test-key-a", "fc-test-key-b"],
        key_file=key_file,
        db_path=db_path,
    )

    conn = sqlite3.connect(str(db_path))
    row = conn.execute("SELECT SUM(in_flight) FROM keys").fetchone()
    conn.close()
    assert row is not None
    assert int(row[0] or 0) == 0
