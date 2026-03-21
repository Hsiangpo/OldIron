from pathlib import Path

from malaysia_crawler.businesslist.cf_crawler import BusinessListCFCrawler
from malaysia_crawler.businesslist.cf_crawler import _detect_block_reason
from malaysia_crawler.businesslist.cf_crawler import _is_blocked_page
from malaysia_crawler.businesslist.cf_crawler import _parse_cookie_header
from malaysia_crawler.businesslist.cf_crawler import _read_cookie_map


def test_parse_cookie_header() -> None:
    data = _parse_cookie_header("cf_clearance=abc; session=xyz; foo=bar")
    assert data["cf_clearance"] == "abc"
    assert data["session"] == "xyz"
    assert data["foo"] == "bar"


def test_read_cookie_map_from_json_cookies(tmp_path: Path) -> None:
    file_path = tmp_path / "cookies.json"
    file_path.write_text(
        '{"cookies":[{"name":"cf_clearance","value":"aaa"},{"name":"session","value":"bbb"}]}',
        encoding="utf-8",
    )
    cookies = _read_cookie_map(str(file_path))
    assert cookies == {"cf_clearance": "aaa", "session": "bbb"}


def test_read_cookie_map_from_plain_header(tmp_path: Path) -> None:
    file_path = tmp_path / "cookies.txt"
    file_path.write_text("cf_clearance=abc; _session=hello", encoding="utf-8")
    cookies = _read_cookie_map(str(file_path))
    assert cookies == {"cf_clearance": "abc", "_session": "hello"}


def test_blocked_page_detection() -> None:
    assert _is_blocked_page("Just a moment, checking your browser")
    assert _is_blocked_page("cf-turnstile")
    assert (
        _detect_block_reason(
            "Error 1005 Access denied has banned the autonomous system number"
        )
        == "error_1005_asn_blocked"
    )
    assert not _is_blocked_page("<html><div id='company_name'>ok</div></html>")
    assert not _is_blocked_page("<script src='/cdn-cgi/challenge-platform/scripts/jsd/main.js'></script><div id='company_name'>ok</div>")


def test_refresh_cookies_from_file_can_reload_updated_cookie(tmp_path: Path) -> None:
    cookie_file = tmp_path / "cf.txt"
    cookie_file.write_text("cf_clearance=aaa; CAKEPHP=old", encoding="utf-8")
    crawler = BusinessListCFCrawler(cookies_file=str(cookie_file))
    try:
        assert "cf_clearance=aaa" in crawler.session.headers.get("Cookie", "")
        cookie_file.write_text("cf_clearance=bbb; CAKEPHP=new", encoding="utf-8")
        changed = crawler.refresh_cookies_from_file(force=True)
        assert changed is True
        assert "cf_clearance=bbb" in crawler.session.headers.get("Cookie", "")
    finally:
        crawler.close()


def test_refresh_cookies_force_with_same_value_returns_false(tmp_path: Path) -> None:
    cookie_file = tmp_path / "cf.txt"
    cookie_file.write_text("cf_clearance=same; CAKEPHP=stable", encoding="utf-8")
    crawler = BusinessListCFCrawler(cookies_file=str(cookie_file))
    try:
        changed = crawler.refresh_cookies_from_file(force=True)
        assert changed is False
    finally:
        crawler.close()


def test_proxy_url_is_applied_to_session(tmp_path: Path) -> None:
    cookie_file = tmp_path / "cf.txt"
    cookie_file.write_text("cf_clearance=same; CAKEPHP=stable", encoding="utf-8")
    crawler = BusinessListCFCrawler(
        cookies_file=str(cookie_file),
        proxy_url="http://127.0.0.1:7890",
    )
    try:
        assert crawler.session.proxies["http"] == "http://127.0.0.1:7890"
        assert crawler.session.proxies["https"] == "http://127.0.0.1:7890"
    finally:
        crawler.close()
