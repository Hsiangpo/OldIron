import sqlite3
from argparse import Namespace
from pathlib import Path

import malaysia_crawler.cli as cli_module
import pytest
from malaysia_crawler.cli import _build_parser
from malaysia_crawler.cli import _resolve_streaming_businesslist_end_id
from malaysia_crawler.cli import _resolve_streaming_businesslist_start_id


def _build_args(*, start_id: int, end_id: int, tail_window: int) -> Namespace:
    return Namespace(
        businesslist_start_id=start_id,
        businesslist_end_id=end_id,
        businesslist_resume_tail_window=tail_window,
        businesslist_source="cf",
        businesslist_cf_cookies_file="cookies/businesslist.cf.cookie.txt",
        businesslist_cf_user_agent="ua",
        timeout=30.0,
        delay_min=0.1,
        delay_max=0.3,
    )


def test_resolve_streaming_businesslist_end_id_without_history(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    args = _build_args(start_id=381000, end_id=500000, tail_window=5000)
    result = _resolve_streaming_businesslist_end_id(args, db_path)
    assert result == 500000


def test_resolve_streaming_businesslist_end_id_with_history_cap(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE businesslist_scan (
            company_id INTEGER PRIMARY KEY,
            status TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "INSERT INTO businesslist_scan(company_id, status) VALUES(?, ?)",
        (384690, "queued_late"),
    )
    conn.commit()
    conn.close()

    args = _build_args(start_id=381000, end_id=500000, tail_window=5000)
    result = _resolve_streaming_businesslist_end_id(args, db_path)
    assert result == 389690


def test_resolve_streaming_businesslist_start_id_prefers_history(tmp_path: Path) -> None:
    db_path = tmp_path / "pipeline.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute(
        """
        CREATE TABLE businesslist_scan (
            company_id INTEGER PRIMARY KEY,
            status TEXT NOT NULL
        )
        """
    )
    conn.execute(
        "INSERT INTO businesslist_scan(company_id, status) VALUES(?, ?)",
        (50000, "queued"),
    )
    conn.commit()
    conn.close()

    args = _build_args(start_id=1, end_id=500000, tail_window=5000)
    result = _resolve_streaming_businesslist_start_id(args, db_path)
    assert result == 45000


def test_resolve_streaming_businesslist_start_id_probe_when_no_history(
    tmp_path: Path, monkeypatch
) -> None:
    db_path = tmp_path / "pipeline.db"
    cookie_file = tmp_path / "cookie.txt"
    cookie_file.write_text("cf_clearance=x; CAKEPHP=y", encoding="utf-8")
    args = _build_args(start_id=1, end_id=500000, tail_window=5000)
    args.businesslist_cf_cookies_file = str(cookie_file)

    monkeypatch.setattr(cli_module, "_probe_first_businesslist_hit_id", lambda **_: 50000)
    result = _resolve_streaming_businesslist_start_id(args, db_path)
    assert result == 50000


def test_businesslist_use_system_proxy_default_true_when_proxy_env_set(monkeypatch) -> None:
    monkeypatch.setenv("HTTP_PROXY", "http://127.0.0.1:7897")
    monkeypatch.delenv("BUSINESSLIST_USE_SYSTEM_PROXY", raising=False)
    parser = _build_parser()
    args = parser.parse_args(["streaming-run"])
    assert args.businesslist_use_system_proxy is True


def test_businesslist_use_system_proxy_default_false_without_proxy_env(monkeypatch) -> None:
    monkeypatch.delenv("HTTP_PROXY", raising=False)
    monkeypatch.delenv("HTTPS_PROXY", raising=False)
    monkeypatch.delenv("ALL_PROXY", raising=False)
    monkeypatch.delenv("BUSINESSLIST_USE_SYSTEM_PROXY", raising=False)
    parser = _build_parser()
    args = parser.parse_args(["streaming-run"])
    assert args.businesslist_use_system_proxy is False


def test_ensure_cf_cookie_file_requires_login(tmp_path: Path, monkeypatch) -> None:
    cookie_file = tmp_path / "cf.txt"
    cookie_file.write_text("cf_clearance=x; CAKEPHP=y", encoding="utf-8")
    args = Namespace(
        businesslist_source="cf",
        businesslist_cf_cookies_file=str(cookie_file),
        businesslist_require_login=True,
        businesslist_login_probe_company_id=62731,
        timeout=10.0,
    )
    monkeypatch.setattr(
        cli_module,
        "probe_businesslist_login_status",
        lambda *_, **__: (False, "login_page_title", "https://www.businesslist.my/sign-in/email:62731"),
    )
    with pytest.raises(ValueError, match="未处于登录态"):
        cli_module._ensure_cf_cookie_file(args)


def test_ensure_cf_cookie_file_allows_skip_login_check(tmp_path: Path, monkeypatch) -> None:
    cookie_file = tmp_path / "cf.txt"
    cookie_file.write_text("cf_clearance=x; CAKEPHP=y", encoding="utf-8")
    args = Namespace(
        businesslist_source="cf",
        businesslist_cf_cookies_file=str(cookie_file),
        businesslist_require_login=False,
        businesslist_login_probe_company_id=62731,
        timeout=10.0,
    )
    monkeypatch.setattr(
        cli_module,
        "probe_businesslist_login_status",
        lambda *_, **__: (_ for _ in ()).throw(RuntimeError("should not be called")),
    )
    cli_module._ensure_cf_cookie_file(args)
