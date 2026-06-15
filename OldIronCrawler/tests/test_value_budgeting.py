from __future__ import annotations

import sys
import threading
import time
from pathlib import Path
from types import SimpleNamespace

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.config import AppConfig
from oldironcrawler.extractor.email_page_selection import build_email_teacher_pool
from oldironcrawler.extractor.llm_client import LlmExtractionResult
from oldironcrawler.extractor.page_pool import PageFetchPool, PageFetchPoolConfig
from oldironcrawler.extractor.protocol_client import HtmlPage
from oldironcrawler.extractor.service import DiscoverySnapshot, SiteProfileService
from oldironcrawler.extractor import service as service_module
from oldironcrawler.extractor import discovery_timeout as discovery_timeout_module
from oldironcrawler.extractor import value_rules as value_rules_module
from oldironcrawler.importer import ImportedWebsite
from oldironcrawler.runtime.global_learning import GlobalLearningStore
from oldironcrawler.runtime.store import RuntimeStore


def test_app_config_loads_value_budget_defaults(tmp_path: Path, monkeypatch) -> None:
    for name in (
        "REP_PAGE_LIMIT",
        "EMAIL_PAGE_SOFT_LIMIT",
        "EMAIL_PAGE_HARD_LIMIT",
        "PAGE_TOTAL_HARD_LIMIT",
        "EMAIL_STOP_SAME_DOMAIN_COUNT",
        "DISCOVERY_BUDGET_SECONDS",
    ):
        monkeypatch.delenv(name, raising=False)

    config = AppConfig.load(tmp_path)

    assert config.rep_page_limit == 5
    assert config.email_page_soft_limit == 8
    assert config.email_page_hard_limit == 16
    assert config.page_total_hard_limit == 20
    assert config.email_stop_same_domain_count == 2
    assert config.ai_email_concurrency == 32
    assert config.discovery_budget_seconds == 45.0


def test_app_config_supports_value_budget_dotenv_override(tmp_path: Path) -> None:
    (tmp_path / ".env").write_text(
        "\n".join(
            [
                "REP_PAGE_LIMIT=6",
                "EMAIL_PAGE_SOFT_LIMIT=9",
                "EMAIL_PAGE_HARD_LIMIT=18",
                "PAGE_TOTAL_HARD_LIMIT=22",
                "EMAIL_STOP_SAME_DOMAIN_COUNT=3",
                "DISCOVERY_BUDGET_SECONDS=55",
            ]
        ),
        encoding="utf-8",
    )

    config = AppConfig.load(tmp_path)

    assert config.rep_page_limit == 6
    assert config.email_page_soft_limit == 9
    assert config.email_page_hard_limit == 18
    assert config.page_total_hard_limit == 22
    assert config.email_stop_same_domain_count == 3
    assert config.ai_email_concurrency == 32
    assert config.discovery_budget_seconds == 55.0


def test_discovery_email_only_stops_after_two_email_families() -> None:
    website = "https://acme.example"

    class FakeProtocol:
        def __init__(self) -> None:
            self.sitemap_calls = 0
            self.related_calls = 0

        def discover_primary_urls(self, _website: str, *, limit: int):
            return SimpleNamespace(
                urls=[f"{website}/contact", f"{website}/privacy"],
                homepage_html="<html>home</html>",
            )

        def discover_sitemap_urls(self, _website: str, *, limit: int) -> list[str]:
            self.sitemap_calls += 1
            return [f"{website}/legal"]

        def discover_related_subdomain_urls(self, *_args, **_kwargs) -> list[str]:
            self.related_calls += 1
            return [f"{website}/support"]

    protocol = FakeProtocol()

    snapshot = service_module._discover_value_snapshot(
        protocol,
        website,
        {},
        {},
        rep_target_count=0,
        contact_target_enabled=True,
    )

    assert f"{website}/contact" in snapshot.email_urls
    assert f"{website}/privacy" in snapshot.email_urls
    assert protocol.sitemap_calls == 0
    assert protocol.related_calls == 0


def test_discovery_email_only_stops_after_primary_contact_page() -> None:
    website = "https://acme.example"

    class FakeProtocol:
        def __init__(self) -> None:
            self.sitemap_calls = 0
            self.related_calls = 0

        def discover_primary_urls(self, _website: str, *, limit: int):
            return SimpleNamespace(
                urls=[f"{website}/fale-conosco"],
                homepage_html="<html>home</html>",
            )

        def discover_sitemap_urls(self, _website: str, *, limit: int) -> list[str]:
            self.sitemap_calls += 1
            return [f"{website}/privacidade"]

        def discover_related_subdomain_urls(self, *_args, **_kwargs) -> list[str]:
            self.related_calls += 1
            return [f"{website}/atendimento"]

    protocol = FakeProtocol()

    snapshot = service_module._discover_value_snapshot(
        protocol,
        website,
        {},
        {},
        rep_target_count=0,
        contact_target_enabled=True,
    )

    assert f"{website}/fale-conosco" in snapshot.email_urls
    assert protocol.sitemap_calls == 0
    assert protocol.related_calls == 0


def test_discovery_budget_skips_extra_stages_after_primary(monkeypatch) -> None:
    website = "https://acme.example"

    class FakeProtocol:
        def __init__(self) -> None:
            self.sitemap_calls = 0
            self.related_calls = 0

        def discover_primary_urls(self, _website: str, *, limit: int):
            return SimpleNamespace(
                urls=[f"{website}/products"],
                homepage_html="<html>home</html>",
            )

        def discover_sitemap_urls(self, _website: str, *, limit: int) -> list[str]:
            self.sitemap_calls += 1
            return [f"{website}/contact"]

        def discover_related_subdomain_urls(self, *_args, **_kwargs) -> list[str]:
            self.related_calls += 1
            return [f"{website}/privacy"]

    monkeypatch.setattr(service_module.time, "monotonic", lambda: 2.0)
    protocol = FakeProtocol()

    snapshot = service_module._discover_value_snapshot(
        protocol,
        website,
        {},
        {},
        rep_target_count=0,
        contact_target_enabled=True,
        discovery_deadline_monotonic=1.0,
    )

    assert snapshot.urls == [f"{website}/products"]
    assert protocol.sitemap_calls == 0
    assert protocol.related_calls == 0


def test_discovery_timeout_does_not_block_following_discovery() -> None:
    slow_started = []

    def slow_discover(*_args, **_kwargs):
        slow_started.append(True)
        time.sleep(0.25)
        return service_module.DiscoverySnapshot(
            urls=["https://slow.example/contact"],
            candidates=[],
            rep_urls=[],
            teacher_pool=[],
            email_urls=["https://slow.example/contact"],
            homepage_html="",
        )

    def fast_discover(*_args, **_kwargs):
        return service_module.DiscoverySnapshot(
            urls=["https://fast.example/contact"],
            candidates=[],
            rep_urls=[],
            teacher_pool=[],
            email_urls=["https://fast.example/contact"],
            homepage_html="",
        )

    slow_snapshot = discovery_timeout_module.discover_value_snapshot_or_homepage(
        slow_discover,
        object(),
        "https://slow.example",
        {},
        {},
        rep_target_count=0,
        contact_target_enabled=True,
        discovery_deadline_monotonic=time.monotonic() + 0.02,
        discovery_workers=1,
    )
    fast_snapshot = discovery_timeout_module.discover_value_snapshot_or_homepage(
        fast_discover,
        object(),
        "https://fast.example",
        {},
        {},
        rep_target_count=0,
        contact_target_enabled=True,
        discovery_deadline_monotonic=time.monotonic() + 0.10,
        discovery_workers=1,
    )

    assert slow_started
    assert slow_snapshot.urls == ["https://slow.example"]
    assert fast_snapshot.urls == ["https://fast.example/contact"]


def test_build_fetch_plan_preserves_rep_pages_and_total_budget() -> None:
    plan = value_rules_module.build_fetch_plan(
        "https://example.com",
        [
            "https://example.com",
            "https://example.com/about",
            "https://example.com/team",
            "https://example.com/leadership",
            "https://example.com/founder",
            "https://example.com/board",
        ],
        [
            "https://example.com/contact",
            "https://example.com/privacy",
            "https://example.com/support",
        ],
        rep_limit=5,
        email_soft_limit=8,
        email_hard_limit=16,
        total_hard_limit=6,
    )

    assert plan["rep_urls"] == [
        "https://example.com",
        "https://example.com/about",
        "https://example.com/team",
        "https://example.com/leadership",
        "https://example.com/founder",
    ]
    assert plan["all_primary_urls"] == [
        "https://example.com",
        "https://example.com/about",
        "https://example.com/team",
        "https://example.com/leadership",
        "https://example.com/founder",
        "https://example.com/contact",
    ]
    assert len(set(plan["rep_urls"] + plan["email_primary_urls"] + plan["email_overflow_urls"])) == 6


def test_build_fetch_plan_splits_email_budget_into_primary_and_overflow() -> None:
    plan = value_rules_module.build_fetch_plan(
        "https://example.com",
        [
            "https://example.com",
            "https://example.com/about",
            "https://example.com/team",
        ],
        [
            "https://example.com/contact",
            "https://example.com/privacy",
            "https://example.com/legal",
            "https://example.com/support",
            "https://example.com/help",
            "https://example.com/careers",
        ],
        rep_limit=5,
        email_soft_limit=2,
        email_hard_limit=5,
        total_hard_limit=8,
    )

    assert plan["email_primary_urls"] == [
        "https://example.com/contact",
        "https://example.com/privacy",
    ]
    assert plan["email_overflow_urls"] == [
        "https://example.com/legal",
        "https://example.com/support",
        "https://example.com/help",
    ]


def test_build_fetch_plan_keeps_selected_homepage_in_primary_phase() -> None:
    plan = value_rules_module.build_fetch_plan(
        "https://example.com",
        [
            "https://example.com/about",
            "https://example.com/team",
        ],
        [
            "https://example.com/contact",
            "https://example.com",
            "https://example.com/privacy",
        ],
        rep_limit=2,
        email_soft_limit=2,
        email_hard_limit=3,
        total_hard_limit=4,
    )

    assert "https://example.com" in plan["all_primary_urls"]
    assert "https://example.com" not in plan["email_overflow_urls"]


def test_build_email_teacher_pool_excludes_homepage_scheme_variants() -> None:
    website = "http://newton.ag"
    homepage_variant = "https://newton.ag/"
    careers_url = "https://newton.ag/carreiras/"
    candidates = value_rules_module.build_candidates(website, [homepage_variant, careers_url], {}, {})

    pool = build_email_teacher_pool(
        SimpleNamespace(
            urls=[website, homepage_variant, careers_url],
            candidates=candidates,
            email_urls=[website],
        ),
        website,
    )

    assert homepage_variant not in pool
    assert careers_url in pool


def test_build_fetch_plan_can_include_homepage_even_when_homepage_not_in_rep_or_email_candidates() -> None:
    plan = value_rules_module.build_fetch_plan(
        "https://example.com",
        [
            "https://example.com/about",
            "https://example.com/team",
        ],
        [
            "https://example.com/contact",
            "https://example.com/privacy",
            "https://example.com/legal",
        ],
        rep_limit=2,
        email_soft_limit=1,
        email_hard_limit=2,
        total_hard_limit=4,
    )

    assert plan["all_primary_urls"] == [
        "https://example.com/about",
        "https://example.com/team",
        "https://example.com",
        "https://example.com/contact",
    ]
    assert len(plan["all_primary_urls"]) + len(plan["email_overflow_urls"]) <= 4


def test_build_fetch_plan_dedupes_rep_and_email_overlap() -> None:
    plan = value_rules_module.build_fetch_plan(
        "https://example.com",
        [
            "https://example.com",
            "https://example.com/about",
            "https://example.com/contact",
        ],
        [
            "https://example.com/contact",
            "https://example.com/privacy",
            "https://example.com",
            "https://example.com/legal",
        ],
        rep_limit=3,
        email_soft_limit=2,
        email_hard_limit=3,
        total_hard_limit=5,
    )

    assert plan["all_primary_urls"] == [
        "https://example.com",
        "https://example.com/about",
        "https://example.com/contact",
        "https://example.com/privacy",
        "https://example.com/legal",
    ]
    assert plan["email_primary_urls"].count("https://example.com/contact") == 0


def test_site_profile_service_fetches_email_overflow_after_primary_phase_when_primary_has_no_email(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    about_url = f"{website}/about"
    team_url = f"{website}/team"
    contact_url = f"{website}/contact"
    privacy_url = f"{website}/privacy"
    fetch_calls: list[list[str]] = []
    llm_calls: list[list[str]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def discover_primary_urls(self, website: str, *, limit: int):
            return SimpleNamespace(urls=[about_url, team_url, contact_url, privacy_url], homepage_html="")

        def discover_sitemap_urls(self, website: str, *, limit: int) -> list[str]:
            return []

        def discover_related_subdomain_urls(self, website: str, *, homepage_html: str, direct_urls: list[str], limit: int) -> list[str]:
            return []

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            fetch_calls.append(list(urls))
            html_map = {
                about_url: "<html><h1>About</h1><p>Alice Example</p></html>",
                team_url: "<html><h1>Team</h1><p>Alice Example</p></html>",
                contact_url: "<html><p>Contact form only</p></html>",
                privacy_url: "<html>privacy@acmeholdings.co.uk</html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_representative_urls(self, **_kwargs):
            return []

        def extract_company_and_representative(self, *, homepage: str, pages: list[dict[str, str]], deadline_monotonic):
            llm_calls.append([page["url"] for page in pages])
            return LlmExtractionResult(
                company_name="Example Co",
                representative="Alice Example",
                evidence_url=about_url,
                evidence_quote="Alice Example",
            )

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[about_url, team_url, contact_url, privacy_url],
            candidates=[],
            rep_urls=[about_url, team_url],
            teacher_pool=[],
            email_urls=[contact_url, privacy_url],
        ),
    )

    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(_build_service_config(), store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert fetch_calls == [
        [about_url, team_url, website, contact_url],
        [privacy_url],
    ]
    assert llm_calls == [[about_url, team_url]]
    assert result.result.representative == "Alice Example"
    assert result.result.emails == "privacy@acmeholdings.co.uk"
    learning_store.close()
    store.close()


def test_site_profile_service_skips_email_overflow_after_first_primary_email_for_hit_rate(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    contact_url = f"{website}/contact"
    privacy_url = f"{website}/privacy"
    fetch_calls: list[list[str]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            fetch_calls.append(list(urls))
            html_map = {
                contact_url: "<html>info@acmeholdings.co.uk</html>",
                privacy_url: "<html>privacy@acmeholdings.co.uk</html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_email_urls(self, **_kwargs):
            return []

        def extract_emails_from_pages(self, **_kwargs):
            raise AssertionError("规则已经命中邮箱时不应再跑 AI 邮箱补全")

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[contact_url, privacy_url],
            candidates=[],
            rep_urls=[],
            teacher_pool=[],
            email_urls=[contact_url, privacy_url],
        ),
    )

    config = _build_service_config()
    config.extract_representative_enabled = False
    config.collect_company_name_enabled = False
    config.collect_email_enabled = True
    config.collect_phone_enabled = False
    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(config, store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert fetch_calls == [[website, contact_url]]
    assert result.result.emails == "info@acmeholdings.co.uk"
    assert result.stage_metrics.ai_email_ms == 0
    learning_store.close()
    store.close()


def test_site_profile_service_uses_homepage_when_discovery_deadline_expires(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acme.example"

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            return [HtmlPage(url=website, html="<html>info@acme.example</html>") for url in urls if url == website]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_email_urls(self, **_kwargs):
            return []

        def extract_emails_from_pages(self, **_kwargs):
            return []

    def slow_discovery(*_args, **_kwargs):
        time.sleep(0.2)
        return DiscoverySnapshot(urls=[], candidates=[], rep_urls=[], teacher_pool=[], email_urls=[])

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(service_module, "_discover_value_snapshot", slow_discovery)
    monkeypatch.setattr(service_module, "_resolve_discovery_deadline", lambda *_args, **_kwargs: time.monotonic() + 0.03)

    config = _build_service_config()
    config.extract_representative_enabled = False
    config.collect_company_name_enabled = False
    config.collect_email_enabled = True
    config.collect_phone_enabled = False
    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(config, store, learning_store, FakeLlmClient(), page_pool=None)

    started = time.monotonic()
    result = service.process(task.id, task.website)

    assert time.monotonic() - started < 0.15
    assert result.result.emails == "info@acme.example"
    assert result.stage_metrics.discovered_url_count == 1
    learning_store.close()
    store.close()


def test_site_profile_service_uses_llm_email_picker_when_rules_only_select_homepage(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    hidden_contact_url = f"{website}/central-relacionamento"
    product_url = f"{website}/products"
    fetch_calls: list[list[str]] = []
    picker_calls: list[dict[str, object]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            fetch_calls.append(list(urls))
            html_map = {
                hidden_contact_url: "<html><p>hidden@acmeholdings.co.uk</p></html>",
                product_url: "<html><p>Products</p></html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_email_urls(self, **kwargs):
            picker_calls.append(kwargs)
            return [hidden_contact_url]

        def pick_representative_urls(self, **_kwargs):
            return []

        def extract_company_and_representative(self, **_kwargs):
            raise AssertionError("代表人 LLM 不应在该用例里运行")

        def extract_emails_from_pages(self, **_kwargs):
            return []

    candidates = value_rules_module.build_candidates(
        website,
        [hidden_contact_url, product_url],
        {},
        {},
    )
    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[website, hidden_contact_url, product_url],
            candidates=candidates,
            rep_urls=[],
            teacher_pool=[],
            email_urls=[website],
            homepage_html="<html><p>Home</p></html>",
        ),
    )
    monkeypatch.setattr(service_module, "replace_shell_pages_with_evidence", lambda *_args, **_kwargs: 0)

    config = _build_service_config()
    config.extract_representative_enabled = False
    config.collect_email_enabled = True
    config.collect_phone_enabled = False
    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(config, store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website, input_company_name="Acme Holdings")

    assert picker_calls
    assert hidden_contact_url in picker_calls[0]["candidate_urls"]
    assert fetch_calls == [[hidden_contact_url]]
    assert result.result.emails == "hidden@acmeholdings.co.uk"
    learning_store.close()
    store.close()


def test_site_profile_service_reuses_discovery_homepage_html_in_primary_phase(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    about_url = f"{website}/about"
    contact_url = f"{website}/contact"
    fetch_calls: list[list[str]] = []
    llm_calls: list[list[str]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def discover_primary_urls(self, website: str, *, limit: int):
            return SimpleNamespace(urls=[website, about_url, contact_url], homepage_html="<html><h1>Acme Holdings</h1><p>Alice Example alice@acmeholdings.co.uk</p></html>")

        def discover_sitemap_urls(self, website: str, *, limit: int) -> list[str]:
            return []

        def discover_related_subdomain_urls(self, website: str, *, homepage_html: str, direct_urls: list[str], limit: int) -> list[str]:
            return []

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            fetch_calls.append(list(urls))
            html_map = {
                about_url: "<html><h1>About</h1><p>Alice Example</p></html>",
                contact_url: "<html>info@acmeholdings.co.uk</html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_representative_urls(self, **_kwargs):
            return []

        def extract_company_and_representative(self, *, homepage: str, pages: list[dict[str, str]], deadline_monotonic):
            llm_calls.append([page["url"] for page in pages])
            homepage_page = next(page for page in pages if page["url"] == website)
            assert "alice@acmeholdings.co.uk" in homepage_page["html"]
            return LlmExtractionResult(
                company_name="Acme Holdings",
                representative="Alice Example",
                evidence_url=website,
                evidence_quote="Alice Example",
            )

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[website, about_url, contact_url],
            candidates=[],
            rep_urls=[website, about_url],
            teacher_pool=[],
            email_urls=[contact_url],
            homepage_html="<html><h1>Acme Holdings</h1><p>Alice Example alice@acmeholdings.co.uk</p></html>",
        ),
    )

    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(_build_service_config(), store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert fetch_calls == [[about_url, contact_url]]
    assert llm_calls == [[website, about_url]]
    assert result.stage_metrics.fetched_page_count == 3
    assert result.result.company_name == "Acme Holdings"
    learning_store.close()
    store.close()


def test_site_profile_service_skips_email_overflow_when_representative_and_primary_email_are_enough(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    about_url = f"{website}/about"
    team_url = f"{website}/team"
    contact_url = f"{website}/contact"
    privacy_url = f"{website}/privacy"
    events: list[str] = []
    fetch_calls: list[list[str]] = []
    llm_calls: list[list[str]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def discover_primary_urls(self, website: str, *, limit: int):
            return SimpleNamespace(urls=[about_url, team_url, contact_url, privacy_url], homepage_html="")

        def discover_sitemap_urls(self, website: str, *, limit: int) -> list[str]:
            return []

        def discover_related_subdomain_urls(self, website: str, *, homepage_html: str, direct_urls: list[str], limit: int) -> list[str]:
            return []

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            events.append("fetch_primary" if privacy_url not in urls else "fetch_overflow")
            fetch_calls.append(list(urls))
            html_map = {
                about_url: "<html><h1>About</h1><p>Alice Example</p></html>",
                team_url: "<html><h1>Team</h1><p>Alice Example</p></html>",
                contact_url: "<html>info@acmeholdings.co.uk support@acmeholdings.co.uk</html>",
                privacy_url: "<html>privacy@acmeholdings.co.uk</html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_representative_urls(self, **_kwargs):
            return []

        def extract_company_and_representative(self, *, homepage: str, pages: list[dict[str, str]], deadline_monotonic):
            events.append("extract_representative")
            llm_calls.append([page["url"] for page in pages])
            return LlmExtractionResult(
                company_name="Example Co",
                representative="Alice Example",
                evidence_url=about_url,
                evidence_quote="Alice Example",
            )

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[about_url, team_url, contact_url, privacy_url],
            candidates=[],
            rep_urls=[about_url, team_url],
            teacher_pool=[],
            email_urls=[contact_url, privacy_url],
        ),
    )

    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(_build_service_config(), store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert events == ["fetch_primary", "extract_representative"]
    assert fetch_calls == [[about_url, team_url, website, contact_url]]
    assert llm_calls == [[about_url, team_url]]
    assert result.result.emails == "info@acmeholdings.co.uk; support@acmeholdings.co.uk"
    learning_store.close()
    store.close()


def test_site_profile_service_extracts_emails_from_representative_pages(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    about_url = f"{website}/about"
    contact_url = f"{website}/contact"

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def discover_primary_urls(self, website: str, *, limit: int):
            return SimpleNamespace(urls=[website, about_url, contact_url], homepage_html="")

        def discover_sitemap_urls(self, website: str, *, limit: int) -> list[str]:
            return []

        def discover_related_subdomain_urls(self, website: str, *, homepage_html: str, direct_urls: list[str], limit: int) -> list[str]:
            return []

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            html_map = {
                website: "<html><h1>Home</h1></html>",
                about_url: "<html><h1>About</h1><p>Alice Example founder@acmeholdings.co.uk</p></html>",
                contact_url: "<html><p>Contact us</p></html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_representative_urls(self, **_kwargs):
            return []

        def extract_company_and_representative(self, *, homepage: str, pages: list[dict[str, str]], deadline_monotonic):
            return LlmExtractionResult(
                company_name="Acme Holdings",
                representative="Alice Example",
                evidence_url=about_url,
                evidence_quote="Alice Example",
            )

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[website, about_url, contact_url],
            candidates=[],
            rep_urls=[about_url],
            teacher_pool=[],
            email_urls=[contact_url],
        ),
    )

    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(_build_service_config(), store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert result.result.emails == "founder@acmeholdings.co.uk"
    learning_store.close()
    store.close()


def test_site_profile_service_skips_email_overflow_when_primary_email_is_enough_even_if_representative_is_missing(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    about_url = f"{website}/about"
    team_url = f"{website}/team"
    contact_url = f"{website}/contact"
    privacy_url = f"{website}/privacy"
    events: list[str] = []
    fetch_calls: list[list[str]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def discover_primary_urls(self, website: str, *, limit: int):
            return SimpleNamespace(urls=[about_url, team_url, contact_url, privacy_url], homepage_html="")

        def discover_sitemap_urls(self, website: str, *, limit: int) -> list[str]:
            return []

        def discover_related_subdomain_urls(self, website: str, *, homepage_html: str, direct_urls: list[str], limit: int) -> list[str]:
            return []

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            events.append("fetch_primary" if privacy_url not in urls else "fetch_overflow")
            fetch_calls.append(list(urls))
            html_map = {
                about_url: "<html><h1>About</h1><p>About us</p></html>",
                team_url: "<html><h1>Team</h1><p>Our team</p></html>",
                contact_url: "<html>info@acmeholdings.co.uk support@acmeholdings.co.uk</html>",
                privacy_url: "<html>privacy@acmeholdings.co.uk</html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_representative_urls(self, **_kwargs):
            return []

        def extract_company_and_representative(self, *, homepage: str, pages: list[dict[str, str]], deadline_monotonic):
            events.append("extract_representative")
            return LlmExtractionResult(
                company_name="Example Co",
                representative="",
                evidence_url="",
                evidence_quote="",
            )

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[about_url, team_url, contact_url, privacy_url],
            candidates=[],
            rep_urls=[about_url, team_url],
            teacher_pool=[],
            email_urls=[contact_url, privacy_url],
        ),
    )

    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(_build_service_config(), store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert events == ["fetch_primary", "extract_representative"]
    assert fetch_calls == [[about_url, team_url, website, contact_url]]
    assert result.result.emails == "info@acmeholdings.co.uk; support@acmeholdings.co.uk"
    assert result.stage_metrics.ai_email_ms >= 0
    learning_store.close()
    store.close()


def test_site_profile_service_waits_for_ai_email_when_rules_find_no_email(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    contact_url = f"{website}/contact"

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            return [
                HtmlPage(
                    url=contact_url,
                    html="<html><p>Email sales - acmeholdings.co.uk</p></html>",
                )
            ]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_email_urls(self, **_kwargs):
            return []

        def extract_emails_from_pages(self, **_kwargs):
            return ["ai@acmeholdings.co.uk"]

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[contact_url],
            candidates=[],
            rep_urls=[],
            teacher_pool=[],
            email_urls=[contact_url],
        ),
    )

    config = _build_service_config()
    config.extract_representative_enabled = False
    config.collect_company_name_enabled = False
    config.collect_email_enabled = True
    config.collect_phone_enabled = False
    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(config, store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert result.result.emails == "ai@acmeholdings.co.uk"
    assert result.stage_metrics.ai_email_ms >= 0
    loaded = store.load_stage_metrics(task.id)
    assert loaded.ai_email_ms == result.stage_metrics.ai_email_ms
    learning_store.close()
    store.close()


def test_site_profile_service_email_only_skips_phone_rules(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    contact_url = f"{website}/contact"

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            return [HtmlPage(url=contact_url, html="<html>info@acmeholdings.co.uk</html>")]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_email_urls(self, **_kwargs):
            return []

        def extract_emails_from_pages(self, **_kwargs):
            return []

    def fail_phone_rules(*_args, **_kwargs):
        raise AssertionError("email-only mode should not run phone rules")

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(service_module, "collect_phones_for_pages", fail_phone_rules)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[contact_url],
            candidates=[],
            rep_urls=[],
            teacher_pool=[],
            email_urls=[contact_url],
        ),
    )

    config = _build_service_config()
    config.extract_representative_enabled = False
    config.collect_company_name_enabled = False
    config.collect_email_enabled = True
    config.collect_phone_enabled = False
    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(config, store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert result.result.emails == "info@acmeholdings.co.uk"
    assert result.result.phones == ""
    learning_store.close()
    store.close()


def test_site_profile_service_skips_remaining_primary_email_pages_after_fast_hit(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    contact_url = f"{website}/contact"
    support_url = f"{website}/support"
    legal_url = f"{website}/legal"
    privacy_url = f"{website}/privacy"
    fetch_calls: list[list[str]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            fetch_calls.append(list(urls))
            html_map = {
                website: "<html>Home</html>",
                contact_url: "<html>info@acmeholdings.co.uk</html>",
                support_url: "<html>Support form</html>",
                legal_url: "<html>Legal text</html>",
                privacy_url: "<html>privacy@acmeholdings.co.uk</html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_email_urls(self, **_kwargs):
            return []

        def extract_emails_from_pages(self, **_kwargs):
            raise AssertionError("rules already found an email")

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[contact_url, support_url, legal_url, privacy_url],
            candidates=[],
            rep_urls=[],
            teacher_pool=[],
            email_urls=[contact_url, support_url, legal_url, privacy_url],
        ),
    )

    config = _build_service_config()
    config.extract_representative_enabled = False
    config.collect_company_name_enabled = False
    config.collect_email_enabled = True
    config.collect_phone_enabled = False
    config.email_page_soft_limit = 4
    config.email_page_hard_limit = 4
    config.page_total_hard_limit = 6
    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(config, store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert fetch_calls == [[website, contact_url]]
    assert result.result.emails == "info@acmeholdings.co.uk"
    learning_store.close()
    store.close()


def test_site_profile_service_fetches_remaining_primary_email_pages_after_fast_miss(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    contact_url = f"{website}/contact"
    support_url = f"{website}/support"
    legal_url = f"{website}/legal"
    privacy_url = f"{website}/privacy"
    fetch_calls: list[list[str]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            fetch_calls.append(list(urls))
            html_map = {
                website: "<html>Home</html>",
                contact_url: "<html>Contact form</html>",
                support_url: "<html>Support form</html>",
                legal_url: "<html>Legal text</html>",
                privacy_url: "<html>privacy@acmeholdings.co.uk</html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_email_urls(self, **_kwargs):
            return []

        def extract_emails_from_pages(self, **_kwargs):
            return []

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[contact_url, support_url, legal_url, privacy_url],
            candidates=[],
            rep_urls=[],
            teacher_pool=[],
            email_urls=[contact_url, support_url, legal_url, privacy_url],
        ),
    )

    config = _build_service_config()
    config.extract_representative_enabled = False
    config.collect_company_name_enabled = False
    config.collect_email_enabled = True
    config.collect_phone_enabled = False
    config.email_page_soft_limit = 4
    config.email_page_hard_limit = 4
    config.page_total_hard_limit = 6
    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(config, store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert fetch_calls == [[website, contact_url], [support_url, legal_url, privacy_url]]
    assert result.result.emails == "privacy@acmeholdings.co.uk"
    learning_store.close()
    store.close()


def test_site_profile_service_fetches_remaining_primary_email_pages_after_fast_timeout(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    contact_url = f"{website}/contact"
    support_url = f"{website}/support"
    legal_url = f"{website}/legal"
    privacy_url = f"{website}/privacy"
    fetch_calls: list[list[str]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            fetch_calls.append(list(urls))
            if contact_url in urls:
                raise TimeoutError("page_batch_timeout")
            html_map = {
                website: "<html>Home</html>",
                support_url: "<html>Support form</html>",
                legal_url: "<html>Legal text</html>",
                privacy_url: "<html>privacy@acmeholdings.co.uk</html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_email_urls(self, **_kwargs):
            return []

        def extract_emails_from_pages(self, **_kwargs):
            return []

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[contact_url, support_url, legal_url, privacy_url],
            candidates=[],
            rep_urls=[],
            teacher_pool=[],
            email_urls=[contact_url, support_url, legal_url, privacy_url],
        ),
    )

    config = _build_service_config()
    config.extract_representative_enabled = False
    config.collect_company_name_enabled = False
    config.collect_email_enabled = True
    config.collect_phone_enabled = False
    config.email_page_soft_limit = 4
    config.email_page_hard_limit = 4
    config.page_total_hard_limit = 6
    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(config, store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert fetch_calls == [[website, contact_url], [support_url, legal_url, privacy_url]]
    assert result.result.emails == "privacy@acmeholdings.co.uk"
    learning_store.close()
    store.close()


def test_site_profile_service_keeps_reused_homepage_when_initial_email_fetch_times_out(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    contact_url = f"{website}/contact"
    fetch_calls: list[list[str]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            fetch_calls.append(list(urls))
            raise TimeoutError("page_batch_timeout")

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_email_urls(self, **_kwargs):
            return []

        def extract_emails_from_pages(self, **_kwargs):
            return []

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[website, contact_url],
            candidates=[],
            rep_urls=[],
            teacher_pool=[],
            email_urls=[website, contact_url],
            homepage_html="<html><p>Contact us at info@acmeholdings.co.uk</p></html>",
        ),
    )

    config = _build_service_config()
    config.extract_representative_enabled = False
    config.collect_company_name_enabled = False
    config.collect_email_enabled = True
    config.collect_phone_enabled = False
    config.email_page_soft_limit = 2
    config.email_page_hard_limit = 2
    config.page_total_hard_limit = 4
    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(config, store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert fetch_calls == [[contact_url]]
    assert result.result.emails == "info@acmeholdings.co.uk"
    assert result.stage_metrics.fetched_page_count == 1
    learning_store.close()
    store.close()


def test_ai_email_extraction_uses_short_deadline() -> None:
    captured_remaining: list[float] = []

    class FakeLlmClient:
        def extract_emails_from_pages(self, *, homepage: str, pages: list[dict[str, str]], deadline_monotonic):
            captured_remaining.append(deadline_monotonic - service_module.time.monotonic())
            return []

    service_module._AI_EMAIL_SEMAPHORE = None
    service_module._AI_EMAIL_SEMAPHORE_LIMIT = 0

    result = service_module._extract_ai_emails_or_empty(
        llm_client=FakeLlmClient(),
        homepage="https://acmeholdings.co.uk",
        email_rule_pages=[
            ("https://acmeholdings.co.uk/contact", "<html>Email sales - acmeholdings.co.uk</html>")
        ],
        deadline_monotonic=service_module.time.monotonic() + 180.0,
        ai_email_concurrency=32,
        ai_email_timeout_seconds=7.0,
    )

    assert result == []
    assert captured_remaining
    assert 0.0 < captured_remaining[0] <= 7.5


def test_email_url_picker_uses_short_deadline(tmp_path: Path, monkeypatch) -> None:
    website = "https://acmeholdings.co.uk"
    captured_remaining: list[float] = []

    def fake_pick_email_urls_or_empty(_llm_client, **kwargs):
        deadline_monotonic = kwargs["deadline_monotonic"]
        captured_remaining.append(deadline_monotonic - service_module.time.monotonic())
        return [kwargs["candidate_urls"][0]]

    monkeypatch.setattr(service_module, "pick_email_urls_or_empty", fake_pick_email_urls_or_empty)

    config = _build_service_config()
    config.ai_email_timeout_seconds = 7.0
    store, learning_store, _task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(config, store, learning_store, object(), page_pool=None)
    snapshot = DiscoverySnapshot(
        urls=[website, f"{website}/products", f"{website}/hidden-contact"],
        candidates=[],
        rep_urls=[],
        teacher_pool=[],
        email_urls=[website],
    )

    result = service._resolve_email_urls(
        snapshot,
        website,
        service_module.SiteStageMetrics(),
        service_module.time.monotonic() + 180.0,
    )

    assert result == [website, f"{website}/products"]
    assert captured_remaining
    assert 0.0 < captured_remaining[0] <= 7.5
    learning_store.close()
    store.close()


def test_email_url_picker_timeout_keeps_discovered_urls(tmp_path: Path, monkeypatch) -> None:
    website = "https://acmeholdings.co.uk"

    def fake_pick_email_urls_or_empty(_llm_client, **_kwargs):
        service_module.time.sleep(0.35)
        return [f"{website}/hidden-contact"]

    monkeypatch.setattr(service_module, "pick_email_urls_or_empty", fake_pick_email_urls_or_empty)

    config = _build_service_config()
    config.ai_email_timeout_seconds = 0.05
    store, learning_store, _task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(config, store, learning_store, object(), page_pool=None)
    snapshot = DiscoverySnapshot(
        urls=[website, f"{website}/products", f"{website}/hidden-contact"],
        candidates=[],
        rep_urls=[],
        teacher_pool=[],
        email_urls=[website],
    )

    begin = service_module.time.monotonic()
    result = service._resolve_email_urls(
        snapshot,
        website,
        service_module.SiteStageMetrics(),
        service_module.time.monotonic() + 180.0,
    )

    assert result == [website]
    assert service_module.time.monotonic() - begin < 0.2
    learning_store.close()
    store.close()


def test_site_profile_service_uses_discovery_deadline_for_discovery_client(tmp_path: Path, monkeypatch) -> None:
    website = "https://acmeholdings.co.uk"
    contact_url = f"{website}/contact"
    captured_deadlines: list[float | None] = []

    class FakeProtocolClient:
        def __init__(self, protocol_config) -> None:
            captured_deadlines.append(protocol_config.deadline_monotonic)

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            return [HtmlPage(url=url, html="<html>contact@acmeholdings.co.uk</html>") for url in urls]

        def close(self) -> None:
            return None

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(service_module, "_resolve_discovery_deadline", lambda _config, _deadline: 123.0)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[website, contact_url],
            candidates=[],
            rep_urls=[],
            teacher_pool=[],
            email_urls=[contact_url],
            homepage_html="<html>home</html>",
        ),
    )

    config = _build_service_config()
    config.collect_email_enabled = True
    config.collect_phone_enabled = False
    config.collect_company_name_enabled = False
    config.extract_representative_enabled = False
    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(config, store, learning_store, object(), page_pool=None)

    result = service.process(task.id, task.website, deadline_monotonic=999.0)

    assert captured_deadlines == [123.0, 999.0]
    assert result.result.emails == "contact@acmeholdings.co.uk"
    learning_store.close()
    store.close()


def test_site_profile_service_skips_email_overflow_when_representative_page_emails_are_enough(
    tmp_path: Path,
    monkeypatch,
) -> None:
    website = "https://acmeholdings.co.uk"
    about_url = f"{website}/about"
    contact_url = f"{website}/contact"
    privacy_url = f"{website}/privacy"
    events: list[str] = []
    fetch_calls: list[list[str]] = []

    class FakeProtocolClient:
        def __init__(self, _config) -> None:
            return None

        def discover_primary_urls(self, website: str, *, limit: int):
            return SimpleNamespace(urls=[website, about_url, contact_url, privacy_url], homepage_html="")

        def discover_sitemap_urls(self, website: str, *, limit: int) -> list[str]:
            return []

        def discover_related_subdomain_urls(self, website: str, *, homepage_html: str, direct_urls: list[str], limit: int) -> list[str]:
            return []

        def fetch_pages(self, urls: list[str], *, max_workers: int, page_pool=None):
            events.append("fetch_primary" if privacy_url not in urls else "fetch_overflow")
            fetch_calls.append(list(urls))
            html_map = {
                website: "<html><h1>Home</h1></html>",
                about_url: "<html><h1>About</h1><p>founder@acmeholdings.co.uk support@acmeholdings.co.uk</p></html>",
                contact_url: "<html><p>Contact us</p></html>",
                privacy_url: "<html><p>privacy@acmeholdings.co.uk</p></html>",
            }
            return [HtmlPage(url=url, html=html_map[url]) for url in urls if url in html_map]

        def close(self) -> None:
            return None

    class FakeLlmClient:
        def pick_representative_urls(self, **_kwargs):
            return []

        def extract_company_and_representative(self, *, homepage: str, pages: list[dict[str, str]], deadline_monotonic):
            events.append("extract_representative")
            return LlmExtractionResult(
                company_name="Acme Holdings",
                representative="Alice Example",
                evidence_url=about_url,
                evidence_quote="Alice Example",
            )

    monkeypatch.setattr(service_module, "SiteProtocolClient", FakeProtocolClient)
    monkeypatch.setattr(
        service_module,
        "_discover_value_snapshot",
        lambda *_args, **_kwargs: DiscoverySnapshot(
            urls=[website, about_url, contact_url, privacy_url],
            candidates=[],
            rep_urls=[about_url],
            teacher_pool=[],
            email_urls=[contact_url, privacy_url],
        ),
    )

    store, learning_store, task = _prepare_service_task(tmp_path, website=website)
    service = SiteProfileService(_build_service_config(), store, learning_store, FakeLlmClient(), page_pool=None)

    result = service.process(task.id, task.website)

    assert events == ["fetch_primary", "extract_representative"]
    assert fetch_calls == [[about_url, website, contact_url]]
    assert result.result.emails == "support@acmeholdings.co.uk; founder@acmeholdings.co.uk"
    learning_store.close()
    store.close()


def test_page_fetch_pool_batch_timeout_starts_while_waiting_for_slot() -> None:
    pool = PageFetchPool(PageFetchPoolConfig(worker_count=1, per_host_limit=1))
    started = threading.Event()
    release = threading.Event()

    def blocking_fetch(url: str) -> HtmlPage:
        started.set()
        release.wait(timeout=0.5)
        return HtmlPage(url=url, html="<html>ok</html>")

    first_done: list[object] = []

    def run_first_batch() -> None:
        try:
            first_done.extend(
                pool.fetch_pages(
                    urls=["https://a.example/slow"],
                    fetch_one=blocking_fetch,
                    deadline_monotonic=time.monotonic() + 2.0,
                    batch_timeout_seconds=0.5,
                )
            )
        except Exception as exc:  # noqa: BLE001
            first_done.append(exc)

    thread = threading.Thread(target=run_first_batch)
    thread.start()
    assert started.wait(timeout=0.3)

    begin = time.monotonic()
    try:
        try:
            pool.fetch_pages(
                urls=["https://b.example/queued"],
                fetch_one=lambda url: HtmlPage(url=url, html="<html>queued</html>"),
                deadline_monotonic=time.monotonic() + 1.0,
                batch_timeout_seconds=0.05,
            )
        except TimeoutError:
            pass
        else:
            raise AssertionError("queued batch should time out before a worker is free")
    finally:
        release.set()
        thread.join(timeout=1.0)
        pool.close()

    assert time.monotonic() - begin < 0.2


def _build_service_config() -> SimpleNamespace:
    return SimpleNamespace(
        request_timeout_seconds=10.0,
        total_wait_seconds=180.0,
        proxy_url="",
        capsolver_api_key="",
        capsolver_api_base_url="https://api.capsolver.com",
        capsolver_proxy="",
        capsolver_poll_seconds=3.0,
        capsolver_max_wait_seconds=40.0,
        cloudflare_proxy_url="",
        page_concurrency=8,
        rep_page_limit=2,
        email_page_soft_limit=1,
        email_page_hard_limit=2,
        page_total_hard_limit=5,
        email_stop_same_domain_count=2,
        ai_email_concurrency=8,
        discovery_budget_seconds=45.0,
    )


def _prepare_service_task(tmp_path: Path, *, website: str) -> tuple[RuntimeStore, GlobalLearningStore, object]:
    store = RuntimeStore(tmp_path / "runtime.sqlite3")
    learning_store = GlobalLearningStore(tmp_path / "global_learning.sqlite3")
    store.prepare_job(
        input_name="sites.txt",
        fingerprint="budgeting",
        rows=[
            ImportedWebsite(
                input_index=1,
                raw_website=website,
                website=website,
                dedupe_key=website,
            )
        ],
    )
    task = store.claim_next_site()
    assert task is not None
    return store, learning_store, task
