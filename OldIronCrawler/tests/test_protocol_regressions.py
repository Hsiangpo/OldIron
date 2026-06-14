from __future__ import annotations

import sys
import time
from pathlib import Path
from types import SimpleNamespace

import pytest


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.extractor.protocol_client import ProtocolPermanentError, ProtocolTemporaryError, SiteProtocolClient, SiteProtocolConfig
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

    def fake_fetch_html(_session, url: str, *, required: bool, timeout_seconds=None, max_retries_override=None):
        assert url == "https://example.com"
        raise ProtocolTemporaryError("temporary_http_503: https://example.com")

    monkeypatch.setattr(client, "_fetch_html", fake_fetch_html)
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


def test_build_site_protocol_config_caps_page_batch_timeout() -> None:
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


def test_fetch_page_optional_disables_slow_fallbacks_for_budgeted_targets(monkeypatch) -> None:
    client = SiteProtocolClient(SiteProtocolConfig())
    captured_kwargs: dict[str, object] = {}

    def fake_fetch_html(*_args, **kwargs):
        captured_kwargs.update(kwargs)
        return "<html>ok</html>"

    monkeypatch.setattr(client, "_fetch_html", fake_fetch_html)

    page = client._fetch_page_optional("https://example.com/contact")

    assert page is not None
    assert captured_kwargs["allow_httpx_fallback"] is False
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


def test_common_probe_fetch_does_not_hold_global_request_slot(monkeypatch) -> None:
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

    def fail_request_slot(*_args, **_kwargs):
        raise AssertionError("common probe should not consume the global page-fetch slot")

    monkeypatch.setattr(protocol_module, "request_slot", fail_request_slot)

    assert client._probe_common_value_url("https://example.com/contact") == "https://example.com/contact"
    client.close()
