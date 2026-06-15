from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from oldironcrawler.extractor.protocol_client import HtmlPage, ProtocolPermanentError, ProtocolTemporaryError, SiteProtocolClient
from oldironcrawler.extractor.protocol_discovery import build_common_probe_urls, extract_path_locale_prefix
from oldironcrawler.extractor.value_rules import build_candidates, canonicalize_target_url, select_email_urls

_EMAIL_FALLBACK_PROBE_LIMIT = 16
_EMAIL_FALLBACK_WORKERS = 4
_LOCALE_EMAIL_PHRASES = {
    "/tr": ("bize-ulasin", "hakkimizda", "iletisim", "insan-kaynaklari", "kariyer", "kurumsal", "kvkk"),
    "/pt": ("atendimento", "contato", "fale-conosco", "faleconosco", "lgpd", "ouvidoria", "privacidade"),
    "/br": ("atendimento", "contato", "fale-conosco", "faleconosco", "lgpd", "ouvidoria", "privacidade"),
    "/ja": ("contact", "inquiry", "mailform", "otoiawase", "recruit/form", "toiawase"),
    "/jp": ("contact", "inquiry", "mailform", "otoiawase", "recruit/form", "toiawase"),
    "/en": ("contact", "contact-us", "inquiry", "mailform", "privacy", "support"),
    "/gb": ("contact", "contact-us", "inquiry", "mailform", "privacy", "support"),
}


def has_non_homepage_email_target(website: str, email_urls: list[str]) -> bool:
    homepage = canonicalize_target_url(website)
    return any(canonicalize_target_url(url) != homepage for url in email_urls)


def probe_common_email_value_urls(
    protocol: SiteProtocolClient,
    website: str,
    snapshot: Any,
) -> list[str]:
    return [page.url for page in probe_common_email_value_pages(protocol, website, snapshot)]


def probe_common_email_value_pages(
    protocol: SiteProtocolClient,
    website: str,
    snapshot: Any,
) -> list[HtmlPage]:
    probe_urls = _select_common_email_probe_urls(website, snapshot)
    if not probe_urls:
        return []
    try:
        pages = protocol.fetch_pages(
            probe_urls,
            max_workers=min(_EMAIL_FALLBACK_WORKERS, len(probe_urls)),
            page_pool=None,
        )
    except (ProtocolPermanentError, ProtocolTemporaryError):
        return []
    except Exception:  # noqa: BLE001
        return []
    return [page for page in pages if str(getattr(page, "html", "") or "").strip()]


def _select_common_email_probe_urls(website: str, snapshot: Any) -> list[str]:
    homepage = _probe_identity_url(website)
    known = {_probe_identity_url(url) for url in [website, *snapshot.urls] if str(url or "").strip()}
    seen = set(known)
    candidates = build_candidates(website, _build_common_email_probe_candidates(website, snapshot.urls), {}, {})
    ordered_urls = [
        *_build_locale_priority_email_probe_urls(website, snapshot.urls),
        *_prioritize_discovered_locale_probe_urls(select_email_urls(candidates), snapshot.urls),
    ]
    selected: list[str] = []
    for url in ordered_urls:
        identity = _probe_identity_url(url)
        if not identity or identity == homepage or identity in seen:
            continue
        seen.add(identity)
        selected.append(url)
        if len(selected) >= _EMAIL_FALLBACK_PROBE_LIMIT:
            break
    return selected


def _build_common_email_probe_candidates(website: str, discovered_urls: list[str]) -> list[str]:
    result: list[str] = []
    seen: set[str] = set()
    for seed_url in _build_common_probe_seed_urls(website, discovered_urls):
        for probe_url in build_common_probe_urls(seed_url):
            identity = _probe_identity_url(probe_url)
            if not identity or identity in seen:
                continue
            seen.add(identity)
            result.append(probe_url)
    return result


def _build_locale_priority_email_probe_urls(website: str, discovered_urls: list[str]) -> list[str]:
    base_host = _normalized_probe_host(website)
    prefixes = _extract_discovered_locale_prefixes(discovered_urls)
    if not prefixes:
        return []
    result: list[str] = []
    seen: set[str] = set()
    for seed_url in _build_common_probe_seed_urls(website, discovered_urls)[1:]:
        if _normalized_probe_host(seed_url) != base_host:
            continue
        for probe_url in build_common_probe_urls(seed_url):
            prefix = _matching_locale_prefix(probe_url, prefixes)
            identity = _probe_identity_url(probe_url)
            if not prefix or not identity or identity in seen:
                continue
            if not _locale_email_phrase_matches(probe_url, prefix):
                continue
            seen.add(identity)
            result.append(probe_url)
    return result


def _build_common_probe_seed_urls(website: str, discovered_urls: list[str]) -> list[str]:
    base_host = _normalized_probe_host(website)
    seeds = [website]
    locale_prefixes: set[str] = set()
    for url in discovered_urls:
        if _normalized_probe_host(url) != base_host:
            continue
        prefix = _extract_probe_locale_prefix(url)
        if not prefix or prefix in locale_prefixes:
            continue
        locale_prefixes.add(prefix)
        seeds.append(url)
        if len(seeds) >= 5:
            break
    return seeds


def _normalized_probe_host(url: str) -> str:
    host = str(urlparse(str(url or "").strip()).netloc or "").strip().lower()
    return host[4:] if host.startswith("www.") else host


def _prioritize_discovered_locale_probe_urls(urls: list[str], discovered_urls: list[str]) -> list[str]:
    prefixes = _extract_discovered_locale_prefixes(discovered_urls)
    if not prefixes:
        return urls
    strong: list[str] = []
    locale_scoped: list[str] = []
    general: list[str] = []
    for url in urls:
        prefix = _matching_locale_prefix(url, prefixes)
        if not prefix:
            general.append(url)
        elif _locale_email_phrase_matches(url, prefix):
            strong.append(url)
        else:
            locale_scoped.append(url)
    return [*strong, *locale_scoped, *general]


def _extract_discovered_locale_prefixes(discovered_urls: list[str]) -> list[str]:
    prefixes: list[str] = []
    for url in discovered_urls:
        prefix = _extract_probe_locale_prefix(url)
        if prefix and prefix not in prefixes:
            prefixes.append(prefix)
    return prefixes


def _extract_probe_locale_prefix(url: str) -> str:
    return extract_path_locale_prefix(urlparse(str(url or "")).path)


def _matching_locale_prefix(url: str, prefixes: list[str]) -> str:
    path = str(urlparse(str(url or "")).path or "").rstrip("/")
    for prefix in prefixes:
        if path == prefix or path.startswith(f"{prefix}/"):
            return prefix
    return ""


def _locale_email_phrase_matches(url: str, prefix: str) -> bool:
    phrases = _LOCALE_EMAIL_PHRASES.get(prefix, ())
    lowered = str(url or "").lower()
    return any(phrase in lowered for phrase in phrases)


def _probe_identity_url(url: str) -> str:
    parsed = urlparse(str(url or "").strip())
    if not parsed.netloc:
        return canonicalize_target_url(url)
    host = str(parsed.netloc or "").strip().lower()
    if host.startswith("www."):
        host = host[4:]
    path = str(parsed.path or "").rstrip("/")
    query = str(parsed.query or "").strip()
    suffix = f"?{query}" if query else ""
    return f"{host}{path}{suffix}".lower()
