"""Italy DNB 官网邮箱服务。"""

from __future__ import annotations

import html
import logging
import re
from concurrent.futures import ThreadPoolExecutor
from concurrent.futures import wait
from concurrent.futures import FIRST_COMPLETED
from dataclasses import dataclass
from urllib.parse import urljoin
from urllib.parse import urlparse

from oldiron_core.fc_email.normalization import extract_registrable_domain
from oldiron_core.fc_email.normalization import filter_emails_for_website
from oldiron_core.fc_email.normalization import normalize_email_candidate
from oldiron_core.protocol_crawler import SiteCrawlClient
from oldiron_core.protocol_crawler import SiteCrawlConfig
from oldiron_core.protocol_crawler.link_extractor import extract_same_site_links

from .email_rules import build_common_probe_urls
from .email_rules import build_email_candidates
from .email_rules import build_email_fetch_plan
from .email_rules import looks_related_subdomain_seed
from .email_rules import normalize_discovery_url
from .email_rules import pick_subdomain_probe_urls
from .email_rules import select_email_urls


LOGGER = logging.getLogger(__name__)
_EMAIL_RE = re.compile(r"([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})", re.IGNORECASE)
_SCRIPT_BLOCK_RE = re.compile(r"(?is)<(script|style|template)\b[^>]*>.*?</\1>")
_FREE_MAIL_DOMAINS = {
    "aol.com",
    "gmail.com",
    "googlemail.com",
    "hotmail.com",
    "icloud.com",
    "live.com",
    "mac.com",
    "me.com",
    "msn.com",
    "outlook.com",
    "pm.me",
    "proton.me",
    "protonmail.com",
    "yahoo.co.jp",
    "yahoo.com",
    "yahoo.com.br",
}


@dataclass(slots=True)
class ItalyDnbEmailSettings:
    proxy_url: str
    timeout_seconds: float = 20.0
    map_limit: int = 200
    common_probe_target: int = 8
    common_probe_concurrency: int = 8
    common_probe_patience_batches: int = 2
    common_probe_min_hits_after_patience: int = 2
    email_page_soft_limit: int = 8
    email_page_hard_limit: int = 16
    email_total_hard_limit: int = 20
    email_stop_same_domain_count: int = 2


@dataclass(slots=True)
class EmailDiscoveryResult:
    emails: list[str]
    evidence_url: str
    selected_urls: list[str]


class ItalyDnbEmailService:
    """面向 Italy DNB 的官网邮箱规则抽取。"""

    def __init__(self, settings: ItalyDnbEmailSettings) -> None:
        self._settings = settings
        self._crawler = SiteCrawlClient(
            SiteCrawlConfig(
                proxy_url=settings.proxy_url,
                timeout_seconds=settings.timeout_seconds,
            )
        )
        self._probe_crawler = SiteCrawlClient(
            SiteCrawlConfig(
                proxy_url=settings.proxy_url,
                timeout_seconds=min(settings.timeout_seconds, 4.0),
                max_retries=0,
            )
        )

    def close(self) -> None:
        self._crawler.close()
        self._probe_crawler.close()

    def discover_emails(self, website: str) -> EmailDiscoveryResult:
        start_url = self._normalize_start_url(website)
        if not start_url:
            return EmailDiscoveryResult(emails=[], evidence_url="", selected_urls=[])
        homepage_html = self._fetch_homepage_html(start_url)
        mapped_urls = self._crawler.map_site(start_url, limit=max(self._settings.map_limit, 1))
        homepage_links = extract_same_site_links(homepage_html, start_url, limit=max(self._settings.map_limit, 1)) if homepage_html else []
        probe_urls = self._probe_common_value_urls(start_url)
        related_urls = self._discover_related_subdomain_urls(
            start_url,
            homepage_html=homepage_html,
            direct_urls=[*homepage_links, *mapped_urls],
        )
        discovered_urls = _merge_unique_urls(
            homepage_links,
            mapped_urls,
            probe_urls,
            related_urls,
            limit=max(self._settings.map_limit * 2, 240),
        )
        candidates = build_email_candidates(start_url, discovered_urls)
        selected_email_urls = select_email_urls(candidates)
        fetch_plan = build_email_fetch_plan(
            start_url,
            selected_email_urls,
            email_soft_limit=self._settings.email_page_soft_limit,
            email_hard_limit=self._settings.email_page_hard_limit,
            total_hard_limit=self._settings.email_total_hard_limit,
        )
        page_map = self._fetch_primary_pages(fetch_plan, homepage_html=homepage_html)
        emails, page_hits = collect_emails_for_pages(start_url, _page_map_to_pairs(page_map, fetch_plan["all_primary_urls"]))
        if not self._enough_same_domain_emails(start_url, emails) and fetch_plan["email_overflow_urls"]:
            overflow_map = self._fetch_urls(fetch_plan["email_overflow_urls"])
            page_map.update(overflow_map)
            emails, page_hits = collect_emails_for_pages(
                start_url,
                _page_map_to_pairs(page_map, [*fetch_plan["all_primary_urls"], *fetch_plan["email_overflow_urls"]]),
            )
        emails = _prune_non_company_emails(start_url, emails)
        evidence_url = next(iter(page_hits.keys()), start_url)
        return EmailDiscoveryResult(
            emails=emails,
            evidence_url=evidence_url,
            selected_urls=[*fetch_plan["all_primary_urls"], *fetch_plan["email_overflow_urls"]],
        )

    def _fetch_primary_pages(self, fetch_plan: dict[str, list[str]], *, homepage_html: str) -> dict[str, str]:
        page_map: dict[str, str] = {}
        homepage_url = fetch_plan["homepage_primary_urls"][0] if fetch_plan["homepage_primary_urls"] else ""
        if homepage_url and homepage_html.strip():
            page_map[homepage_url] = homepage_html
        elif homepage_url:
            try:
                page_map[homepage_url] = self._crawler.scrape_html(homepage_url, truncate_html=False).html
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("官网首页抓取失败：website=%s error=%s", homepage_url, exc)
        page_map.update(self._fetch_urls(fetch_plan["email_primary_urls"]))
        return page_map

    def _fetch_urls(self, urls: list[str]) -> dict[str, str]:
        if not urls:
            return {}
        try:
            pages = self._crawler.scrape_html_pages(urls, truncate_html=False)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("官网子页批抓失败：count=%d error=%s", len(urls), exc)
            return {}
        result: dict[str, str] = {}
        for page in pages:
            html = str(page.html or "")
            if html.strip():
                result[str(page.url or "").strip()] = html
        return result

    def _normalize_start_url(self, website: str) -> str:
        raw = str(website or "").strip()
        if not raw:
            return ""
        if not raw.startswith(("http://", "https://")):
            raw = f"https://{raw}"
        return raw if extract_registrable_domain(raw) else ""

    def _fetch_homepage_html(self, start_url: str) -> str:
        try:
            return str(self._crawler.scrape_html(start_url, truncate_html=False).html or "")
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("官网首页预抓失败：website=%s error=%s", start_url, exc)
            return ""

    def _enough_same_domain_emails(self, website: str, emails: list[str]) -> bool:
        site_domain = extract_registrable_domain(website)
        if not site_domain:
            return False
        same_domain = [
            email
            for email in emails
            if "@" in email and (
                email.split("@", 1)[1] == site_domain
                or email.split("@", 1)[1].endswith(f".{site_domain}")
            )
        ]
        return len(same_domain) >= self._settings.email_stop_same_domain_count

    def _probe_common_value_urls(self, start_url: str) -> list[str]:
        probe_urls = build_common_probe_urls(start_url)
        if not probe_urls:
            return []
        result: list[str] = []
        batch_size = min(max(self._settings.common_probe_concurrency, 1), len(probe_urls))
        probe_target = min(max(self._settings.common_probe_target, 1), len(probe_urls))
        start_index = 0
        empty_batches = 0
        while start_index < len(probe_urls) and len(result) < probe_target:
            batch = probe_urls[start_index : start_index + batch_size]
            start_index += batch_size
            batch_hits = self._probe_common_value_batch(batch)
            result = _merge_unique_urls(result, batch_hits, limit=probe_target)
            if batch_hits:
                empty_batches = 0
            else:
                empty_batches += 1
            if len(result) >= probe_target:
                break
            if empty_batches >= max(self._settings.common_probe_patience_batches, 1):
                break
            if start_index >= batch_size * max(self._settings.common_probe_patience_batches, 1):
                if len(result) < max(self._settings.common_probe_min_hits_after_patience, 1):
                    break
        return result

    def _discover_related_subdomain_urls(self, start_url: str, *, homepage_html: str, direct_urls: list[str]) -> list[str]:
        seeds = _collect_related_subdomain_seed_urls(self._probe_crawler, start_url, homepage_html, direct_urls)
        if not seeds:
            return []
        discovered: list[str] = []
        for seed in seeds[:4]:
            try:
                urls = self._crawler.map_site(seed, limit=40)
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("关联子域探测失败：seed=%s error=%s", seed, exc)
                continue
            discovered = _merge_unique_urls(discovered, [seed, *urls], limit=120)
        return discovered

    def _probe_common_value_batch(self, probe_urls: list[str]) -> list[str]:
        if not probe_urls:
            return []
        results: list[str] = []
        futures = {}
        with ThreadPoolExecutor(max_workers=min(len(probe_urls), max(self._settings.common_probe_concurrency, 1))) as executor:
            for probe_url in probe_urls:
                futures[executor.submit(_probe_single_url, probe_url, self._settings)] = probe_url
            done, pending = wait(futures.keys(), timeout=min(self._settings.timeout_seconds, 6.0), return_when=FIRST_COMPLETED)
            while done:
                for future in list(done):
                    futures.pop(future, None)
                    try:
                        hit = future.result()
                    except Exception:
                        hit = None
                    if hit:
                        results.append(hit)
                if not futures:
                    break
                done, pending = wait(futures.keys(), timeout=min(self._settings.timeout_seconds, 6.0), return_when=FIRST_COMPLETED)
            for future in futures:
                future.cancel()
        return results


def _page_map_to_pairs(page_map: dict[str, str], urls: list[str]) -> list[tuple[str, str]]:
    seen: set[str] = set()
    pages: list[tuple[str, str]] = []
    for url in urls:
        value = str(url or "").strip()
        if not value or value in seen:
            continue
        html = str(page_map.get(value, "") or "")
        if html.strip():
            pages.append((value, html))
            seen.add(value)
    return pages


def _merge_unique_urls(*groups: list[str], limit: int) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for group in groups:
        for url in group:
            normalized = normalize_discovery_url(str(url or "").strip())
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            result.append(normalized)
            if len(result) >= limit:
                return result
    return result


def _collect_related_subdomain_seed_urls(
    crawler: SiteCrawlClient,
    start_url: str,
    homepage_html: str,
    direct_urls: list[str],
) -> list[str]:
    seeds = list(pick_subdomain_probe_urls(start_url, direct_urls))
    for normalized in _extract_same_org_seed_urls(homepage_html or "", start_url, start_url):
        if normalized not in seeds:
            seeds.append(normalized)
        if len(seeds) >= 8:
            return seeds
    for probe_url in [url for url in pick_subdomain_probe_urls(start_url, direct_urls) if url != start_url]:
        try:
            html_text = crawler.scrape_html(probe_url, truncate_html=False).html
        except Exception:
            continue
        for normalized in _extract_same_org_seed_urls(html_text or "", probe_url, start_url):
            if normalized not in seeds:
                seeds.append(normalized)
            if len(seeds) >= 8:
                return seeds
    return seeds


def _extract_same_org_seed_urls(html_text: str, page_url: str, start_url: str) -> list[str]:
    site_domain = extract_registrable_domain(start_url)
    page_host = (urlparse(page_url).netloc or "").strip().lower()
    result: list[str] = []
    seen: set[str] = set()
    for raw_href in re.findall(r'''(?:href|src)=["']([^"'#]+)["']''', html_text or "", flags=re.I):
        absolute = urljoin(page_url, raw_href.strip())
        normalized = normalize_discovery_url(absolute)
        if not normalized or normalized in seen:
            continue
        host = (urlparse(normalized).netloc or "").strip().lower()
        if not host or host == page_host:
            continue
        if extract_registrable_domain(host) != site_domain:
            continue
        if not looks_related_subdomain_seed(normalized, start_url):
            continue
        seen.add(normalized)
        result.append(normalized)
        if len(result) >= 8:
            break
    return result


def _probe_single_url(probe_url: str, settings: ItalyDnbEmailSettings) -> str | None:
    crawler = SiteCrawlClient(
        SiteCrawlConfig(
            proxy_url=settings.proxy_url,
            timeout_seconds=min(settings.timeout_seconds, 3.0),
            max_retries=0,
        )
    )
    try:
        html_text = crawler.scrape_html(probe_url, truncate_html=False).html
        return probe_url if str(html_text or "").strip() else None
    except Exception:
        return None
    finally:
        crawler.close()


def collect_emails_for_pages(website: str, pages: list[tuple[str, str]]) -> tuple[list[str], dict[str, list[str]]]:
    collected: list[str] = []
    page_hits: dict[str, list[str]] = {}
    for url, html_text in pages:
        page_emails = extract_emails_from_html(html_text)
        embedded_same_domain = extract_same_domain_emails_from_embedded_content(website, html_text)
        for email in embedded_same_domain:
            if email not in page_emails:
                page_emails.append(email)
        cleaned = filter_emails_for_website(website, page_emails)
        if cleaned:
            page_hits[url] = cleaned
        for email in cleaned:
            if email not in collected:
                collected.append(email)
    return filter_emails_for_website(website, collected), page_hits


def extract_emails_from_html(raw_html: str) -> list[str]:
    html_text = str(raw_html or "")
    if not html_text.strip():
        return []
    normalized = html.unescape(html_text)
    normalized = _SCRIPT_BLOCK_RE.sub(" ", normalized)
    normalized = normalized.replace("%40", "@").replace("%2E", ".")
    normalized = re.sub(r"(?i)\[(?:at)\]|\((?:at)\)|\s+at\s+", "@", normalized)
    normalized = re.sub(r"(?i)\[(?:dot)\]|\((?:dot)\)|\s+dot\s+", ".", normalized)
    found: list[str] = []
    for match in _EMAIL_RE.findall(normalized):
        value = normalize_email_candidate(match)
        if value and value not in found:
            found.append(value)
    return found


def extract_same_domain_emails_from_embedded_content(website: str, raw_html: str) -> list[str]:
    html_text = str(raw_html or "")
    if not html_text.strip():
        return []
    normalized = html.unescape(html_text)
    normalized = normalized.replace("%40", "@").replace("%2E", ".")
    normalized = re.sub(r"(?i)\[(?:at)\]|\((?:at)\)|\s+at\s+", "@", normalized)
    normalized = re.sub(r"(?i)\[(?:dot)\]|\((?:dot)\)|\s+dot\s+", ".", normalized)
    site_domain = extract_registrable_domain(website)
    found: list[str] = []
    for match in _EMAIL_RE.findall(normalized):
        email = normalize_email_candidate(match)
        if not email or "@" not in email or not site_domain:
            continue
        domain = email.split("@", 1)[1]
        if domain != site_domain and not domain.endswith(f".{site_domain}"):
            continue
        if email not in found:
            found.append(email)
    return found


def _prune_non_company_emails(website: str, emails: list[str]) -> list[str]:
    site_domain = extract_registrable_domain(website)
    if not site_domain:
        return list(emails)
    same_domain: list[str] = []
    free_mail: list[str] = []
    others: list[str] = []
    for email in emails:
        if "@" not in email:
            continue
        email_domain = email.split("@", 1)[1].strip().lower()
        registrable = extract_registrable_domain(email_domain)
        if registrable == site_domain or email_domain.endswith(f".{site_domain}"):
            same_domain.append(email)
            continue
        if registrable in _FREE_MAIL_DOMAINS:
            free_mail.append(email)
            continue
        others.append(email)
    if same_domain:
        return [*same_domain, *[email for email in free_mail if email not in same_domain]]
    if free_mail:
        return free_mail
    other_domains = {extract_registrable_domain(email.split("@", 1)[1]) for email in others if "@" in email}
    if len(other_domains) >= 2:
        return []
    return others
