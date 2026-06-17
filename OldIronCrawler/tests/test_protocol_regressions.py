from __future__ import annotations

import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import SimpleNamespace

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.extractor.protocol_client import ProtocolPermanentError, ProtocolTemporaryError, SiteProtocolClient, SiteProtocolConfig
from oldironcrawler.extractor.page_pool import PageFetchPool, PageFetchPoolConfig
from oldironcrawler.extractor.protocol.fallbacks import build_host_fallback_urls
from oldironcrawler.extractor.service import _build_site_protocol_config
from oldironcrawler import challenge_solver as challenge_module
from oldironcrawler.extractor import protocol_client as protocol_module


def test_detect_challenge_kind_supports_sgcaptcha_pages() -> None:
    html_text = (
        '<html><head><meta http-equiv="refresh" '
        'content="0;/.well-known/sgcaptcha/?r=%2F"></head></html>'
    )

    assert SiteProtocolClient.__module__  # 保持导入使用，避免静态检查误报
    from oldironcrawler.extractor import protocol_client as protocol_module

    assert protocol_module._detect_challenge_kind(html_text) == "sgcaptcha_challenge"


def test_protocol_fetch_html_uses_httpx_fallback_when_curl_gets_sgcaptcha(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeResponse:
        def __init__(self, status_code: int, html_text: str, content_type: str = "text/html") -> None:
            self.status_code = status_code
            self.headers = {"Content-Type": content_type}
            self.text = html_text
            self.content = html_text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, _url, timeout):
            assert timeout == 10.0
            return FakeResponse(
                202,
                '<html><head><meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=%2F"></head></html>',
            )

    monkeypatch.setattr(
        client,
        "_fetch_httpx_snapshot",
        lambda *_args, **_kwargs: (200, "text/html", "<html><body>httpx fallback ok</body></html>"),
    )

    html = client._fetch_html(FakeSession(), "https://example.com", required=False)

    assert "httpx fallback ok" in html
    client.close()


def test_discovery_homepage_timeout_respects_client_deadline(monkeypatch) -> None:
    captured_timeouts: list[float] = []
    client = SiteProtocolClient(
        SiteProtocolConfig(
            timeout_seconds=10.0,
            deadline_monotonic=time.monotonic() + 1.0,
        )
    )

    def fake_fetch_homepage(_url: str, timeout_seconds: float) -> str:
        captured_timeouts.append(timeout_seconds)
        return "<html>home</html>"

    monkeypatch.setattr(client, "_fetch_discovery_homepage_httpx", fake_fetch_homepage)

    html = client._fetch_discovery_homepage(object(), "https://example.com")

    assert html == "<html>home</html>"
    assert captured_timeouts
    assert 0.0 < captured_timeouts[0] <= 1.25
    client.close()


def test_sitemap_fetch_timeout_respects_client_deadline(monkeypatch) -> None:
    captured_timeouts: list[float] = []
    client = SiteProtocolClient(
        SiteProtocolConfig(
            timeout_seconds=10.0,
            deadline_monotonic=time.monotonic() + 1.0,
        )
    )

    class FakeResponse:
        status_code = 404
        content = b""
        headers = {}

    class FakeSession:
        def get(self, _url: str, timeout: float):
            captured_timeouts.append(timeout)
            return FakeResponse()

    class NoopRequestSlot:
        def __enter__(self):
            return None

        def __exit__(self, _exc_type, _exc, _tb):
            return False

    monkeypatch.setattr(protocol_module, "request_slot", lambda **_kwargs: NoopRequestSlot())

    text = client._fetch_sitemap_text(FakeSession(), "https://example.com/robots.txt")

    assert text == ""
    assert captured_timeouts
    assert 0.0 < captured_timeouts[0] <= 1.25
    client.close()


def test_protocol_fetch_html_uses_httpx_fallback_when_curl_returns_false_404(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeResponse:
        def __init__(self, status_code: int, html_text: str, content_type: str = "text/html") -> None:
            self.status_code = status_code
            self.headers = {"Content-Type": content_type}
            self.text = html_text
            self.content = html_text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, _url, timeout):
            assert timeout == 10.0
            return FakeResponse(404, "<html><body>wix error page</body></html>")

    monkeypatch.setattr(
        client,
        "_fetch_httpx_snapshot",
        lambda *_args, **_kwargs: (200, "text/html", "<html><body>real homepage</body></html>"),
    )

    html = client._fetch_html(FakeSession(), "https://example.com", required=False)

    assert "real homepage" in html
    client.close()


def test_protocol_fetch_html_uses_httpx_fallback_when_curl_returns_false_403(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeResponse:
        def __init__(self, status_code: int, html_text: str, content_type: str = "text/html") -> None:
            self.status_code = status_code
            self.headers = {"Content-Type": content_type}
            self.text = html_text
            self.content = html_text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, _url, timeout):
            assert timeout == 10.0
            return FakeResponse(403, "<html><body>Forbidden</body></html>")

    monkeypatch.setattr(
        client,
        "_fetch_httpx_snapshot",
        lambda *_args, **_kwargs: (200, "text/html", "<html><body>real contact sales@example.com</body></html>"),
    )

    html = client._fetch_html(FakeSession(), "https://example.com/contact", required=False)

    assert "real contact" in html
    client.close()


def test_protocol_fetch_html_follows_same_site_meta_refresh() -> None:
    client = SiteProtocolClient(SiteProtocolConfig())
    calls: list[str] = []

    class FakeResponse:
        def __init__(self, html_text: str) -> None:
            self.status_code = 200
            self.headers = {"Content-Type": "text/html"}
            self.text = html_text
            self.content = html_text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, url, timeout):
            calls.append(url)
            if url == "https://example.com":
                return FakeResponse(
                    '<meta http-equiv="refresh" content="0;URL=\'https://www.example.com\'" />'
                )
            return FakeResponse("<html><body>real homepage info@example.com</body></html>")

    html = client._fetch_html(FakeSession(), "https://example.com", required=False)

    assert "real homepage" in html
    assert calls == ["https://example.com", "https://www.example.com"]
    client.close()


def test_discovery_homepage_follows_cross_site_script_redirect_shell(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())
    calls: list[str] = []

    class FakeResponse:
        def __init__(self, url: str, html_text: str) -> None:
            self.url = url
            self.status_code = 200
            self.headers = {"Content-Type": "text/html"}
            self.text = html_text
            self.content = html_text.encode("utf-8")

        def close(self) -> None:
            return None

    def fake_fetch_direct(url: str, _timeout_seconds: float):
        calls.append(url)
        if url == "http://old.example.br":
            return FakeResponse(
                url,
                """
                <html><head><title>Redirect</title>
                <script>window.location.href = "https://new.example.com";</script>
                </head><body><p>Redirecting</p></body></html>
                """,
            )
        return FakeResponse(
            url,
            "<html><body><a href='/es/contacto/'>Contacto</a> contact@new.example.com</body></html>",
        )

    monkeypatch.setattr(client, "_fetch_discovery_homepage_httpx_direct", fake_fetch_direct)

    try:
        result = client.discover_primary_urls("http://old.example.br", limit=20)
    finally:
        client.close()

    assert calls == ["http://old.example.br", "https://new.example.com"]
    assert "contact@new.example.com" in result.homepage_html
    assert "https://new.example.com/es/contacto/" in result.urls


def test_protocol_fetch_html_uses_www_fallback_after_dns_failure() -> None:
    client = SiteProtocolClient(SiteProtocolConfig())
    calls: list[str] = []

    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "text/html"}
        text = "<html><body>real brazil homepage contato@rcpcontadores.com</body></html>"
        content = text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, url, timeout):
            calls.append(url)
            if url == "http://rcpcontadores.com":
                raise RuntimeError("Failed to perform, curl: (6) Could not resolve host")
            return FakeResponse()

    html = client._fetch_html(FakeSession(), "http://rcpcontadores.com", required=False)

    assert "real brazil homepage" in html
    assert calls == ["http://rcpcontadores.com", "http://www.rcpcontadores.com"]
    client.close()


def test_protocol_fetch_html_uses_registrable_root_after_subdomain_dns_failure() -> None:
    client = SiteProtocolClient(SiteProtocolConfig())
    calls: list[str] = []

    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "text/html"}
        text = "<html><body>root homepage contato@altcom.com.br</body></html>"
        content = text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, url, timeout):
            calls.append(url)
            if "info.altcom.com.br" in url:
                raise RuntimeError("Failed to perform, curl: (6) Could not resolve host")
            return FakeResponse()

    html = client._fetch_html(FakeSession(), "http://info.altcom.com.br", required=False)

    assert "root homepage" in html
    assert calls == [
        "http://info.altcom.com.br",
        "http://www.info.altcom.com.br",
        "http://altcom.com.br",
    ]
    client.close()


def test_getaddrinfo_failure_builds_registrable_root_fallback_url() -> None:
    urls = build_host_fallback_urls(
        "http://info.altcom.com.br",
        "[errno 11001] getaddrinfo failed",
    )

    assert "http://altcom.com.br" in urls


def test_discovery_homepage_uses_registrable_root_after_getaddrinfo_failure(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())
    calls: list[str] = []

    def fake_homepage_httpx(url: str, _timeout: float) -> str:
        calls.append(url)
        if "info.altcom.com.br" in url:
            raise ProtocolTemporaryError("[Errno 11001] getaddrinfo failed")
        return "<html><body>root homepage contato@altcom.com.br</body></html>"

    monkeypatch.setattr(client, "_fetch_discovery_homepage_httpx", fake_homepage_httpx)

    try:
        result = client.discover_primary_urls("http://info.altcom.com.br", limit=20)
    finally:
        client.close()

    assert "root homepage" in result.homepage_html
    assert calls[:3] == [
        "http://info.altcom.com.br",
        "http://www.info.altcom.com.br",
        "http://altcom.com.br",
    ]


def test_protocol_fetch_html_marks_plain_403_as_blocked(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeResponse:
        def __init__(self) -> None:
            self.status_code = 403
            self.headers = {"Content-Type": "text/html"}
            self.text = "<html><body>Access denied</body></html>"
            self.content = self.text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, _url, timeout):
            assert timeout == 10.0
            return FakeResponse()

    monkeypatch.setattr(client, "_maybe_challenge_fallback", lambda *_args, **_kwargs: "<html><body>Access denied</body></html>")
    monkeypatch.setattr(
        client,
        "_fetch_httpx_snapshot",
        lambda *_args, **_kwargs: (403, "text/html", "<html><body>Access denied</body></html>"),
    )

    with pytest.raises(ProtocolPermanentError, match="http_403"):
        client._fetch_html(FakeSession(), "https://example.com", required=False)

    client.close()


def test_protocol_fetch_html_uses_httpx_fallback_when_curl_times_out(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeSession:
        def get(self, _url, timeout):
            assert timeout == 10.0
            raise RuntimeError("Failed to perform, curl: (28) Operation timed out after 5004 milliseconds")

    monkeypatch.setattr(
        client,
        "_fetch_httpx_snapshot",
        lambda *_args, **_kwargs: (200, "text/html", "<html><body>timeout fallback ok</body></html>"),
    )

    html = client._fetch_html(FakeSession(), "https://example.com", required=False)

    assert "timeout fallback ok" in html
    client.close()


def test_protocol_client_fetch_pages_raises_when_all_optional_fetches_return_empty() -> None:
    class EmptyClient(SiteProtocolClient):
        def _fetch_page_optional(self, url: str, *, timeout_seconds: float | None = None):
            return None

    client = EmptyClient(SiteProtocolConfig(page_batch_timeout_seconds=0.2))

    try:
        client.fetch_pages(["https://example.com/a", "https://example.com/b"], max_workers=2)
        raise AssertionError("expected ProtocolTemporaryError")
    except ProtocolTemporaryError as exc:
        assert "empty_page_batch" in str(exc)
    finally:
        client.close()


def test_protocol_fetch_html_retries_httpx_fallback_after_soft_challenge(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeResponse:
        def __init__(self, status_code: int, html_text: str, content_type: str = "text/html") -> None:
            self.status_code = status_code
            self.headers = {"Content-Type": content_type}
            self.text = html_text
            self.content = html_text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, _url, timeout):
            assert timeout == 10.0
            return FakeResponse(
                202,
                '<html><head><meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=%2F"></head></html>',
            )

    responses = iter(
        [
            (202, "text/html", '<html><head><meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=%2F"></head></html>'),
            (200, "text/html", "<html><body>retry success</body></html>"),
        ]
    )

    def fake_httpx_snapshot(*_args, **_kwargs):
        return next(responses)

    monkeypatch.setattr(client, "_fetch_httpx_snapshot", fake_httpx_snapshot)

    html = client._fetch_html(FakeSession(), "https://example.com", required=False)

    assert "retry success" in html
    client.close()


def test_protocol_fetch_html_raises_sgcaptcha_when_httpx_fallback_is_still_challenged(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeResponse:
        def __init__(self, status_code: int, html_text: str, content_type: str = "text/html") -> None:
            self.status_code = status_code
            self.headers = {"Content-Type": content_type}
            self.content = html_text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, _url, timeout):
            assert timeout == 10.0
            return FakeResponse(
                202,
                '<html><head><meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=%2F"></head></html>',
            )

    monkeypatch.setattr(
        client,
        "_fetch_httpx_snapshot",
        lambda *_args, **_kwargs: (
            202,
            "text/html",
            '<html><head><meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=%2F"></head></html>',
        ),
    )

    with pytest.raises(ProtocolPermanentError, match="sgcaptcha_challenge"):
        client._fetch_html(FakeSession(), "https://example.com", required=False)

    client.close()


def test_protocol_common_probe_does_not_keep_challenge_pages(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    def fake_fetch_html(*_args, **_kwargs):
        raise ProtocolPermanentError("sgcaptcha_challenge: https://example.com/executive-team")

    monkeypatch.setattr(client, "_fetch_html", fake_fetch_html)

    assert client._probe_common_value_url("https://example.com/executive-team") is None

    client.close()


def test_discover_primary_urls_still_probes_when_homepage_is_temporary_failure(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    def fake_fetch_homepage(_session, url: str):
        assert url == "https://example.com"
        raise ProtocolTemporaryError("temporary_http_503: https://example.com")

    monkeypatch.setattr(client, "_fetch_discovery_homepage", fake_fetch_homepage)
    monkeypatch.setattr(client, "_probe_common_value_urls", lambda *_args, **_kwargs: ["https://example.com/impressum"])

    urls, homepage_html = client._discover_primary_urls(object(), "https://example.com", limit=20)

    assert urls == ["https://example.com/impressum"]
    assert homepage_html == ""
    client.close()


def test_resolve_cloudflare_challenge_skips_non_cloudflare_pages(monkeypatch) -> None:
    monkeypatch.setattr(
        challenge_module,
        "_run_cloudscraper_fallback",
        lambda **_kwargs: pytest.fail("non-cloudflare pages must not enter cloudscraper fallback"),
    )
    monkeypatch.setattr(
        challenge_module,
        "_run_capsolver_fallback",
        lambda **_kwargs: pytest.fail("non-cloudflare pages must not enter capsolver fallback"),
    )

    html_text = '<html><head><meta http-equiv="refresh" content="0;/.well-known/sgcaptcha/?r=%2F"></head></html>'
    result = challenge_module.resolve_cloudflare_challenge(
        url="https://example.com",
        html_text=html_text,
        timeout_seconds=20.0,
        proxy_url="http://127.0.0.1:7897",
        cloudflare_proxy_url="",
        max_html_chars=250000,
        session_headers={"User-Agent": "UA-1"},
        cookie_jar=None,
        detect_challenge_kind=lambda _value: "sgcaptcha_challenge",
        refetch_html=lambda: "",
        impersonate="chrome110",
        capsolver_api_key="capsolver-key",
        capsolver_api_base_url="https://api.capsolver.com",
        capsolver_proxy="1.2.3.4:8080:user:pass",
        capsolver_poll_seconds=1.0,
        capsolver_max_wait_seconds=5.0,
    )

    assert result == html_text


def test_build_site_protocol_config_keeps_brazil_value_page_batch_window() -> None:
    config = SimpleNamespace(
        request_timeout_seconds=10.0,
        total_wait_seconds=180.0,
        proxy_url="",
        capsolver_api_key="",
        capsolver_api_base_url="https://api.capsolver.com",
        capsolver_proxy="",
        capsolver_poll_seconds=3.0,
        capsolver_max_wait_seconds=40.0,
        cloudflare_proxy_url="",
        page_concurrency=32,
    )

    protocol_config = _build_site_protocol_config(config, None)

    assert protocol_config.page_batch_timeout_seconds == 20.0


def test_fetch_page_optional_allows_fast_httpx_fallback_but_disables_slow_fallbacks(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())
    captured_kwargs: dict[str, object] = {}

    def fake_fetch_html(*_args, **kwargs):
        captured_kwargs.update(kwargs)
        return "<html>ok</html>"

    monkeypatch.setattr(client, "_fetch_html", fake_fetch_html)

    page = client._fetch_page_optional("https://example.com/contact")

    assert page is not None
    assert captured_kwargs["allow_httpx_fallback"] is True
    assert captured_kwargs["allow_error_fallbacks"] is False
    assert captured_kwargs["allow_tls_error_fallback"] is True
    client.close()


def test_fetch_html_recomputes_timeout_after_request_slot_wait(monkeypatch) -> None:
    current_time = [100.0]
    captured_timeouts: list[float] = []
    client = SiteProtocolClient(SiteProtocolConfig(timeout_seconds=10.0))

    class FakeSlot:
        def __enter__(self):
            current_time[0] = 104.5
            return None

        def __exit__(self, *_args):
            return False

    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "text/html"}
        content = b"<html>ok</html>"
        text = "<html>ok</html>"

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, _url: str, timeout: float):
            captured_timeouts.append(float(timeout))
            return FakeResponse()

    monkeypatch.setattr(protocol_module.time, "monotonic", lambda: current_time[0])
    monkeypatch.setattr(protocol_module, "request_slot", lambda **_kwargs: FakeSlot())

    html = client._fetch_html(
        FakeSession(),
        "https://example.com",
        required=True,
        timeout_seconds=5.0,
        max_retries_override=0,
        request_deadline_monotonic=105.0,
    )

    assert html == "<html>ok</html>"
    assert captured_timeouts
    assert 0.0 < captured_timeouts[0] <= 0.6
    client.close()


def test_request_slot_wait_timeout_is_capped_for_high_concurrency() -> None:
    client = SiteProtocolClient(SiteProtocolConfig(timeout_seconds=8.0, max_retries=0))

    try:
        assert client._resolve_request_slot_wait_timeout(8.0) <= 6.0
    finally:
        client.close()


def test_page_fetch_request_timeout_cap_allows_slow_brazil_value_pages() -> None:
    assert protocol_module._cap_page_fetch_timeout(30.0, 30.0) == 12.0


def test_fetch_pages_passes_batch_deadline_to_page_pool_requests() -> None:
    captured_deadlines: list[float | None] = []
    started = time.monotonic()

    class FakePool:
        def fetch_pages(
            self,
            *,
            urls: list[str],
            fetch_one,
            deadline_monotonic: float,
            batch_timeout_seconds: float | None = None,
        ) -> list:
            fetch_one(urls[0])
            return []

    client = SiteProtocolClient(
        SiteProtocolConfig(
            timeout_seconds=30.0,
            page_batch_timeout_seconds=8.0,
            deadline_monotonic=started + 180.0,
        )
    )

    def fake_fetch_page_optional(
        _url: str,
        *,
        timeout_seconds: float | None = None,
        request_deadline_monotonic: float | None = None,
    ):
        captured_deadlines.append(request_deadline_monotonic)
        return None

    client._fetch_page_optional = fake_fetch_page_optional
    try:
        try:
            client.fetch_pages(["https://example.com/contact"], max_workers=1, page_pool=FakePool())
        except ProtocolTemporaryError:
            pass
    finally:
        client.close()

    assert captured_deadlines
    assert captured_deadlines[0] is not None
    assert captured_deadlines[0] - started <= 9.0


def test_fetch_pages_page_pool_request_deadline_starts_after_dispatch() -> None:
    pool = PageFetchPool(PageFetchPoolConfig(worker_count=1, per_host_limit=1))
    slow_client = SiteProtocolClient(SiteProtocolConfig(timeout_seconds=30.0, page_batch_timeout_seconds=0.2))
    fast_client = SiteProtocolClient(SiteProtocolConfig(timeout_seconds=30.0, page_batch_timeout_seconds=0.05))
    captured_remaining: list[float] = []

    def slow_fetch_page_optional(
        url: str,
        *,
        timeout_seconds: float | None = None,
        request_deadline_monotonic: float | None = None,
    ):
        time.sleep(0.08)
        return SimpleNamespace(url=url, html="<html>slow</html>")

    def fast_fetch_page_optional(
        url: str,
        *,
        timeout_seconds: float | None = None,
        request_deadline_monotonic: float | None = None,
    ):
        assert request_deadline_monotonic is not None
        captured_remaining.append(request_deadline_monotonic - time.monotonic())
        return SimpleNamespace(url=url, html="<html>fast</html>")

    slow_client._fetch_page_optional = slow_fetch_page_optional
    fast_client._fetch_page_optional = fast_fetch_page_optional
    try:
        with ThreadPoolExecutor(max_workers=2) as executor:
            slow = executor.submit(
                slow_client.fetch_pages,
                ["https://slow.example/contact"],
                max_workers=1,
                page_pool=pool,
            )
            time.sleep(0.01)
            fast = executor.submit(
                fast_client.fetch_pages,
                ["https://fast.example/contact"],
                max_workers=1,
                page_pool=pool,
            )

            assert [page.url for page in fast.result(timeout=1.0)] == ["https://fast.example/contact"]
            assert [page.url for page in slow.result(timeout=1.0)] == ["https://slow.example/contact"]
    finally:
        slow_client.close()
        fast_client.close()
        pool.close()

    assert captured_remaining
    assert 0.03 <= captured_remaining[0] <= 0.06


def test_page_fetch_pool_batch_timeout_starts_after_first_dispatch() -> None:
    pool = PageFetchPool(PageFetchPoolConfig(worker_count=1, per_host_limit=1))
    try:
        def fetch_one(url: str):
            if "slow" in url:
                time.sleep(0.08)
            return SimpleNamespace(url=url, html="<html>ok</html>")

        with ThreadPoolExecutor(max_workers=2) as executor:
            first = executor.submit(
                pool.fetch_pages,
                urls=["https://slow.example/contact"],
                fetch_one=fetch_one,
                deadline_monotonic=time.monotonic() + 1.0,
                batch_timeout_seconds=0.2,
            )
            time.sleep(0.01)
            second = executor.submit(
                pool.fetch_pages,
                urls=["https://fast.example/contact"],
                fetch_one=fetch_one,
                deadline_monotonic=time.monotonic() + 1.0,
                batch_timeout_seconds=0.05,
            )

            assert [page.url for page in second.result(timeout=1.0)] == ["https://fast.example/contact"]
            assert [page.url for page in first.result(timeout=1.0)] == ["https://slow.example/contact"]
    finally:
        pool.close()


def test_discovery_homepage_timeout_cap_stays_bounded() -> None:
    assert 7.5 <= protocol_module._DISCOVERY_HOMEPAGE_TIMEOUT_CAP_SECONDS <= 8.0


def test_discovery_homepage_uses_httpx_fast_path(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig(timeout_seconds=8.0, max_retries=0))

    def fail_curl_path(*_args, **_kwargs):
        raise AssertionError("discovery homepage should use fast httpx path first")

    monkeypatch.setattr(client, "_fetch_html", fail_curl_path)
    monkeypatch.setattr(client, "_fetch_discovery_homepage_httpx", lambda _url, _timeout: "<html>ok</html>")

    try:
        assert client._fetch_discovery_homepage(SimpleNamespace(), "https://example.com") == "<html>ok</html>"
    finally:
        client.close()


def test_discovery_homepage_httpx_fast_path_has_hard_timeout(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig(timeout_seconds=8.0, max_retries=0))

    def slow_fetch(*_args, **_kwargs):
        time.sleep(0.2)
        return SimpleNamespace(status_code=200, headers={"Content-Type": "text/html"}, text="<html>late</html>")

    monkeypatch.setattr(client, "_fetch_discovery_homepage_httpx_direct", slow_fetch)

    started = time.monotonic()
    try:
        with pytest.raises(ProtocolTemporaryError, match="site_open_timeout"):
            client._fetch_discovery_homepage_httpx("https://slow.example.com", 0.02)
    finally:
        client.close()

    assert time.monotonic() - started < 0.1


def test_discovery_homepage_budget_allows_moderately_slow_homepages(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig(timeout_seconds=75.0, max_retries=0))
    captured_timeouts: list[float] = []

    def fake_fetch(_url: str, timeout_seconds: float) -> str:
        captured_timeouts.append(timeout_seconds)
        return "<html><a href='/contact/'>お問い合わせ</a></html>"

    monkeypatch.setattr(client, "_fetch_discovery_homepage_httpx", fake_fetch)

    try:
        html_text = client._fetch_discovery_homepage(object(), "https://example.co.jp")
    finally:
        client.close()

    assert html_text
    assert captured_timeouts
    assert captured_timeouts[0] >= 8.0


def test_extract_same_site_links_handles_large_unclosed_anchor_markup_quickly() -> None:
    html_text = '<html><body><a href="/Home/Contato">Contato' + ("x" * 200_000) + "</body></html>"

    started = time.monotonic()
    urls = protocol_module._extract_same_site_links(html_text, "https://example.com.br", limit=5)
    elapsed = time.monotonic() - started

    assert urls == ["https://example.com.br/Home/Contato"]
    assert elapsed < 0.2


def test_discovery_uses_speculative_common_paths_after_homepage_timeout(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig(common_probe_target=4))

    monkeypatch.setattr(
        client,
        "_fetch_discovery_homepage",
        lambda _session, _url: (_ for _ in ()).throw(ProtocolTemporaryError("site_open_timeout")),
    )
    monkeypatch.setattr(client, "_probe_common_value_urls", lambda *_args, **_kwargs: [])

    try:
        result = client.discover_primary_urls("https://example.com.br", limit=10)
    finally:
        client.close()

    assert "https://example.com.br/pt/contato" in result.urls
    assert "https://example.com.br/fale-conosco" in result.urls


def test_discovery_limits_speculative_common_paths_after_homepage_timeout(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig(common_probe_target=4))

    monkeypatch.setattr(
        client,
        "_fetch_discovery_homepage",
        lambda _session, _url: (_ for _ in ()).throw(ProtocolTemporaryError("site_open_timeout")),
    )
    monkeypatch.setattr(client, "_probe_common_value_urls", lambda *_args, **_kwargs: [])

    try:
        result = client.discover_primary_urls("https://example.com.br", limit=80)
    finally:
        client.close()

    assert len(result.urls) == 4


def test_fetch_html_allows_fast_tls_fallback_without_slow_target_fallbacks(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeSession:
        def get(self, _url, timeout):
            raise RuntimeError("SSL certificate problem: unable to get local issuer certificate")

    monkeypatch.setattr(
        client,
        "_try_insecure_https_fallback",
        lambda url, lowered_error, **_kwargs: "<html>tls fallback ok</html>",
    )

    html = client._fetch_html(
        FakeSession(),
        "https://example.com/contact",
        required=False,
        allow_httpx_fallback=False,
        allow_error_fallbacks=False,
        allow_tls_error_fallback=True,
    )

    assert "tls fallback ok" in html
    client.close()


def test_certificate_subject_error_builds_www_fallback_url() -> None:
    urls = build_host_fallback_urls(
        "https://ispak.com/tr-tr",
        "ssl: no alternative certificate subject name matches target hostname",
    )

    assert "https://www.ispak.com/tr-tr" in urls


def test_fetch_html_tls_fast_path_tries_www_fallback(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeSession:
        def get(self, _url, timeout):
            raise RuntimeError("no alternative certificate subject name matches target hostname")

    monkeypatch.setattr(client, "_try_insecure_https_fallback", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(client, "_try_www_fallback", lambda *_args, **_kwargs: "<html>www fallback ok</html>")

    html = client._fetch_html(
        FakeSession(),
        "https://ispak.com/tr-tr",
        required=False,
        allow_httpx_fallback=False,
        allow_error_fallbacks=False,
        allow_tls_error_fallback=True,
    )

    assert "www fallback ok" in html
    client.close()


def test_fetch_html_tls_www_fallback_keeps_page_batch_timeout() -> None:
    client = SiteProtocolClient(
        SiteProtocolConfig(
            timeout_seconds=30.0,
            deadline_monotonic=time.monotonic() + 60.0,
        )
    )
    calls: list[tuple[str, float]] = []

    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "text/html"}
        text = "<html><body>www fallback ok</body></html>"
        content = text.encode("utf-8")

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, url: str, timeout: float):
            calls.append((url, float(timeout)))
            if url == "https://ispak.com/tr-tr":
                raise RuntimeError("no alternative certificate subject name matches target hostname")
            return FakeResponse()

    html = client._fetch_html(
        FakeSession(),
        "https://ispak.com/tr-tr",
        required=False,
        timeout_seconds=5.0,
        max_retries_override=0,
        allow_httpx_fallback=False,
        allow_error_fallbacks=False,
        allow_tls_error_fallback=True,
    )

    assert "www fallback ok" in html
    assert len(calls) == 2
    assert calls[0][1] <= 5.1
    assert calls[1][1] <= 5.1
    client.close()


def test_common_probe_scan_stops_after_total_budget(monkeypatch) -> None:
    client = SiteProtocolClient(
        SiteProtocolConfig(
            common_probe_concurrency=1,
            common_probe_target=10,
            common_probe_patience_batches=100,
            common_probe_min_hits_after_patience=100,
        )
    )
    calls: list[list[str]] = []

    monkeypatch.setattr(
        protocol_module,
        "_build_common_probe_urls",
        lambda _start_url: [f"https://example.com/path-{index}" for index in range(5)],
    )
    monkeypatch.setattr(protocol_module, "_COMMON_PROBE_TOTAL_WAIT_CAP_SECONDS", 0.01, raising=False)

    def slow_batch(batch, *args, **_kwargs):
        calls.append(list(batch))
        time.sleep(0.03)
        return []

    monkeypatch.setattr(client, "_probe_common_value_batch", slow_batch)

    result = client._probe_common_value_urls(object(), "https://example.com", limit=10)

    assert result == []
    assert calls == [["https://example.com/path-0"]]
    client.close()


def test_common_probe_fetch_uses_global_request_slot(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())

    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "text/html"}
        text = "<html>ok</html>"
        content = b"<html>ok</html>"

        def close(self) -> None:
            return None

    class FakeSession:
        def get(self, _url, timeout):
            return FakeResponse()

    monkeypatch.setattr(client, "_get_or_create_session", lambda: FakeSession())

    calls: list[dict[str, object]] = []

    class FakeRequestSlot:
        def __enter__(self):
            return None

        def __exit__(self, *_args):
            return False

    def fake_request_slot(**kwargs):
        calls.append(dict(kwargs))
        return FakeRequestSlot()

    monkeypatch.setattr(protocol_module, "request_slot", fake_request_slot)

    assert client._probe_common_value_url("https://example.com/contact") == "https://example.com/contact"
    assert calls
    client.close()
