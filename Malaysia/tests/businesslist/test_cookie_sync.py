import types

import malaysia_crawler.businesslist.cookie_sync as cookie_sync_module
from malaysia_crawler.businesslist.cookie_sync import _build_runtime_cookie_header
from malaysia_crawler.businesslist.cookie_sync import _pick_businesslist_cookies
from malaysia_crawler.businesslist.cookie_sync import probe_businesslist_login_status


def test_pick_businesslist_cookies_only_keeps_target_domain_and_valid_items() -> None:
    raw_items = [
        {"name": "cf_clearance", "value": "aaa", "domain": ".businesslist.my"},
        {"name": "CAKEPHP", "value": "bbb", "domain": "www.businesslist.my"},
        {"name": "other", "value": "ccc", "domain": ".example.com"},
        {"name": "", "value": "ddd", "domain": ".businesslist.my"},
        {"name": "empty", "value": "", "domain": ".businesslist.my"},
    ]
    cookies = _pick_businesslist_cookies(raw_items)
    assert cookies == {"cf_clearance": "aaa", "CAKEPHP": "bbb"}


def test_build_runtime_cookie_header_requires_both_required_cookies() -> None:
    assert _build_runtime_cookie_header({"cf_clearance": "x"}) == ""
    assert _build_runtime_cookie_header({"CAKEPHP": "y"}) == ""
    assert (
        _build_runtime_cookie_header({"cf_clearance": "x", "CAKEPHP": "y", "__cf_bm": "z"})
        == "cf_clearance=x; CAKEPHP=y; __cf_bm=z"
    )


def test_probe_businesslist_login_status_detects_not_logged(monkeypatch) -> None:
    class _FakeSession:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}

        def get(self, url: str, timeout: float, allow_redirects: bool):  # noqa: ARG002
            return types.SimpleNamespace(
                url="https://www.businesslist.my/sign-in/email:62731",
                text="<html><title>Members Sign In - Malaysia Business Directory</title></html>",
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr(cookie_sync_module.requests, "Session", lambda: _FakeSession())
    ok, reason, probe_url = probe_businesslist_login_status("cf_clearance=x; CAKEPHP=y")
    assert ok is False
    assert reason == "login_page_title"
    assert probe_url.endswith("/sign-in/email:62731")


def test_probe_businesslist_login_status_detects_logged(monkeypatch) -> None:
    class _FakeSession:
        def __init__(self) -> None:
            self.headers: dict[str, str] = {}

        def get(self, url: str, timeout: float, allow_redirects: bool):  # noqa: ARG002
            return types.SimpleNamespace(
                url="https://www.businesslist.my/company/62731/upright-logistics-sdn-bhd",
                text="<html><title>Upright Express Sdn Bhd</title><div>Company manager</div></html>",
            )

        def close(self) -> None:
            return None

    monkeypatch.setattr(cookie_sync_module.requests, "Session", lambda: _FakeSession())
    ok, reason, probe_url = probe_businesslist_login_status("cf_clearance=x; CAKEPHP=y")
    assert ok is True
    assert reason == "ok"
    assert probe_url.endswith("/company/62731/upright-logistics-sdn-bhd")
