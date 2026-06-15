from __future__ import annotations

import sys
import time
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.extractor.email_rules import collect_emails_for_pages
from oldironcrawler.extractor.phone_rules import collect_phones_for_pages
from oldironcrawler.extractor.protocol_client import HtmlPage
from oldironcrawler.extractor import shell_page as shell_module
from oldironcrawler.extractor.shell_page import (
    build_shell_alias_map,
    build_shell_evidence_html,
    canonicalize_shell_target_urls,
)


def test_shell_evidence_keeps_site_emails_but_drops_placeholder_and_regulator_noise() -> None:
    shell_html = """
    <html>
      <head>
        <script src="/assets/index-xPWHrNF8.js"></script>
      </head>
      <body>
        <div id="root"></div>
      </body>
    </html>
    """
    asset_texts = {
        "https://0xam.de/assets/index-xPWHrNF8.js": """
        "E-Mail: post@0xam.de"
        "mailto:m@0xam.de"
        "placeholder:\\"mail@beispiel.de\\""
        "E-Mail: poststelle@lda.bayern.de"
        "Geschaeftsfuehrer: Marcel Uhlmann"
        """,
    }

    enriched_html = build_shell_evidence_html("https://0xam.de/", shell_html, asset_texts)
    emails, _page_hits = collect_emails_for_pages("https://0xam.de/", [("https://0xam.de/", enriched_html)])

    assert len(emails) == 2
    assert set(emails) == {"post@0xam.de", "m@0xam.de"}


def test_shell_alias_map_collapses_same_shell_fallback_routes() -> None:
    shell_html = """
    <html>
      <head>
        <script src="/assets/app.js"></script>
      </head>
      <body>
        <div id="root"></div>
      </body>
    </html>
    """
    homepage = "https://0xam.de/"
    fake_people = "https://0xam.de/about-us/our-people"
    fake_leadership = "https://0xam.de/executive-team"
    page_map = {
        homepage: HtmlPage(url=homepage, html=shell_html),
        fake_people: HtmlPage(url=fake_people, html=shell_html),
        fake_leadership: HtmlPage(url=fake_leadership, html=shell_html),
    }

    alias_map = build_shell_alias_map(
        start_url=homepage,
        page_map=page_map,
        target_urls=[homepage, fake_people, fake_leadership],
    )
    canonical_urls = canonicalize_shell_target_urls(
        [fake_people, fake_leadership, homepage],
        alias_map,
    )

    assert fake_people not in canonical_urls
    assert fake_leadership not in canonical_urls
    assert homepage in canonical_urls


def test_shell_asset_fetch_respects_deadline_and_stops_extra_requests(monkeypatch) -> None:
    calls: list[str] = []

    class FakeResponse:
        status_code = 200

        def __init__(self, url: str) -> None:
            self.url = url
            self.headers = {"Content-Type": "application/javascript"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def iter_bytes(self):
            yield b'console.log("ok")'

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def stream(self, _method: str, url: str, timeout=None):
            calls.append(url)
            time.sleep(0.03)
            return FakeResponse(url)

    monkeypatch.setattr(shell_module.httpx, "Client", lambda **_kwargs: FakeClient())

    result = shell_module.fetch_first_party_asset_texts(
        ["https://0xam.de/assets/a.js", "https://0xam.de/assets/b.js"],
        proxy_url="",
        timeout_seconds=1.0,
        deadline_monotonic=time.monotonic() + 0.01,
    )

    assert result == {}
    assert calls == ["https://0xam.de/assets/a.js"]


def test_shell_asset_fetch_uses_streaming_instead_of_full_get(monkeypatch) -> None:
    captured_client_kwargs: dict[str, object] = {}

    class FakeResponse:
        status_code = 200
        headers = {"Content-Type": "application/javascript"}

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def iter_bytes(self):
            yield b'const email = "info@acme.example";'

    class FakeClient:
        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb) -> None:
            return None

        def get(self, *_args, **_kwargs):
            raise AssertionError("shell asset fetch must not read a whole response with get().text")

        def stream(self, *_args, **_kwargs):
            return FakeResponse()

    def fake_client_factory(**kwargs):
        captured_client_kwargs.update(kwargs)
        return FakeClient()

    monkeypatch.setattr(shell_module.httpx, "Client", fake_client_factory)

    result = shell_module.fetch_first_party_asset_texts(
        ["https://acme.example/assets/app.js"],
        proxy_url="",
        timeout_seconds=10.0,
        deadline_monotonic=time.monotonic() + 10.0,
    )

    assert result["https://acme.example/assets/app.js"] == 'const email = "info@acme.example";'
    assert captured_client_kwargs["verify"] is False


def test_shell_asset_urls_use_canonical_url_for_relative_assets() -> None:
    html_text = """
    <html>
      <head>
        <link rel="canonical" href="https://biggbrandsglobal.com" />
        <script type="module" src="/assets/index.js"></script>
      </head>
      <body><div id="root"></div></body>
    </html>
    """

    urls = shell_module.extract_first_party_asset_urls("https://biggbrands.com", html_text)

    assert urls == ["https://biggbrandsglobal.com/assets/index.js"]


def test_shell_replacement_preserves_existing_page_emails(monkeypatch) -> None:
    shell_html = """
    <html>
      <head><script src="/assets/app.js"></script></head>
      <body>
        <div id="root"></div>
        <p>Brand@wppmedia.com</p>
      </body>
    </html>
    """
    page_map = {"https://www.wppmedia.com/contact": HtmlPage(url="https://www.wppmedia.com/contact", html=shell_html)}

    monkeypatch.setattr(
        shell_module,
        "fetch_first_party_asset_texts",
        lambda *_args, **_kwargs: {"https://www.wppmedia.com/assets/app.js": '"Telefon: +90 212 123 4567"'},
    )

    shell_module.replace_shell_pages_with_evidence(
        page_map,
        ["https://www.wppmedia.com/contact"],
        proxy_url="",
        timeout_seconds=1.0,
        deadline_monotonic=None,
    )
    emails, _page_hits = collect_emails_for_pages(
        "https://wppmedia.com",
        [("https://www.wppmedia.com/contact", page_map["https://www.wppmedia.com/contact"].html)],
    )

    assert emails == ["brand@wppmedia.com"]


def test_shell_replacement_stops_after_total_budget(monkeypatch) -> None:
    shell_html = """
    <html><head><script src="/assets/app.js"></script></head><body><div id="root"></div></body></html>
    """
    page_map = {
        "https://acme.example/contact": HtmlPage(url="https://acme.example/contact", html=shell_html),
        "https://acme.example/about": HtmlPage(url="https://acme.example/about", html=shell_html),
    }
    calls: list[str] = []

    def slow_enrich(page_url: str, page_html: str, **_kwargs) -> str:
        calls.append(page_url)
        time.sleep(0.03)
        return page_html

    monkeypatch.setattr(shell_module, "_SHELL_REPLACE_BUDGET_SECONDS", 0.01, raising=False)
    monkeypatch.setattr(shell_module, "enrich_shell_page_html", slow_enrich)

    elapsed_start = time.monotonic()
    shell_module.replace_shell_pages_with_evidence(
        page_map,
        ["https://acme.example/contact", "https://acme.example/about"],
        proxy_url="",
        timeout_seconds=1.0,
        deadline_monotonic=None,
    )
    elapsed = time.monotonic() - elapsed_start

    assert elapsed < 0.1
    assert calls == ["https://acme.example/contact"]


def test_shell_replacement_does_not_wait_for_stuck_enrichment(monkeypatch) -> None:
    shell_html = """
    <html><head><script src="/assets/app.js"></script></head><body><div id="root"></div></body></html>
    """
    page_map = {
        "https://acme.example/contact": HtmlPage(url="https://acme.example/contact", html=shell_html),
    }

    def stuck_enrich(page_url: str, page_html: str, **_kwargs) -> str:
        time.sleep(0.2)
        return page_html.replace("</body>", "<p>late@acme.example</p></body>")

    monkeypatch.setattr(shell_module, "_SHELL_REPLACE_BUDGET_SECONDS", 0.02, raising=False)
    monkeypatch.setattr(shell_module, "enrich_shell_page_html", stuck_enrich)

    started = time.monotonic()
    shell_module.replace_shell_pages_with_evidence(
        page_map,
        ["https://acme.example/contact"],
        proxy_url="",
        timeout_seconds=1.0,
        deadline_monotonic=None,
    )

    assert time.monotonic() - started < 0.1
    assert "late@acme.example" not in page_map["https://acme.example/contact"].html


def test_shell_page_detects_root_container_even_with_cookie_text() -> None:
    html_text = """
    <html>
      <head>
        <script src="/assets/app.js"></script>
      </head>
      <body>
        <div id="root"></div>
        <div>
          This website uses cookies to improve your browsing experience and provide detailed analytics.
          This website uses cookies to improve your browsing experience and provide detailed analytics.
          This website uses cookies to improve your browsing experience and provide detailed analytics.
        </div>
      </body>
    </html>
    """

    assert shell_module.looks_like_shell_page(html_text) is True


def test_shell_evidence_recovers_phone_signals() -> None:
    shell_html = """
    <html>
      <head>
        <script src="/assets/index.js"></script>
      </head>
      <body>
        <div id="root"></div>
      </body>
    </html>
    """
    asset_texts = {
        "https://0xam.de/assets/index.js": """
        "Telefon: +49 30 123 4567"
        "tel:+49 30 123 4568"
        """,
    }

    enriched_html = build_shell_evidence_html("https://0xam.de/", shell_html, asset_texts)
    phones, _page_hits = collect_phones_for_pages([("https://0xam.de/", enriched_html)])

    assert set(phones) == {"+49301234567", "+49301234568"}
