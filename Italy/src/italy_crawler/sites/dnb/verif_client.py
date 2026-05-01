"""Verif 浏览器补充客户端。"""

from __future__ import annotations

import logging
import re
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote
from urllib.parse import urljoin
from urllib.parse import urlparse

from playwright.sync_api import BrowserContext
from playwright.sync_api import Page
from playwright.sync_api import sync_playwright


LOGGER = logging.getLogger(__name__)
_CHALLENGE_TITLE_HINTS = ("just a moment", "请稍候")
_CHALLENGE_TEXT_HINTS = (
    "performing security verification",
    "正在进行安全验证",
    "enable javascript and cookies to continue",
)
_RESULT_READY_HINTS = (
    "results displayed",
    "0 results displayed",
    "sort by",
)
_WEBSITE_LABELS = ("website", "web site")
_REPRESENTATIVE_LABELS = (
    "most senior leader",
    "managing partner",
    "chief executive officer",
    "legal representative",
    "president",
    "owner",
    "director",
)


class VerifChallengeError(RuntimeError):
    """Verif challenge 未通过。"""


@dataclass(slots=True)
class VerifCompanyMatch:
    company_name: str
    representative: str
    website: str
    company_url: str
    search_url: str


def normalize_company_key(value: str) -> str:
    return "".join(ch.lower() for ch in str(value or "") if ch.isalnum())


def pick_best_company_link(items: list[tuple[str, str]], target_company_name: str) -> tuple[str, str] | None:
    target_key = normalize_company_key(target_company_name)
    best_item: tuple[str, str] | None = None
    best_score = -1
    for label, href in items:
        label_key = normalize_company_key(label)
        if not href:
            continue
        score = 0
        if label_key == target_key and target_key:
            score += 1000
        if target_key and target_key in label_key:
            score += 200
        if label_key and label_key in target_key:
            score += 100
        shared = len(set(_tokenize_name(label_key)) & set(_tokenize_name(target_key)))
        score += shared * 10
        if score > best_score:
            best_score = score
            best_item = (label.strip(), href.strip())
    return best_item


def extract_company_fields_from_text(page_text: str) -> tuple[str, str]:
    lines = [line.strip(" \t\r\n:|") for line in str(page_text or "").splitlines() if line.strip(" \t\r\n:|")]
    website = _extract_value_after_labels(lines, _WEBSITE_LABELS, normalize=_normalize_website)
    representative = _extract_value_after_labels(lines, _REPRESENTATIVE_LABELS, normalize=_normalize_person)
    return website, representative


class VerifClient:
    """使用持久浏览器 profile 访问 Verif。"""

    def __init__(
        self,
        *,
        profile_dir: Path,
        proxy_url: str,
        timeout_seconds: float = 180.0,
        headless: bool = False,
    ) -> None:
        self._profile_dir = profile_dir
        self._proxy_url = proxy_url
        self._timeout_seconds = max(float(timeout_seconds or 0.0), 30.0)
        self._headless = bool(headless)
        self._lock = threading.RLock()
        self._playwright = None
        self._context: BrowserContext | None = None
        self._page: Page | None = None

    def close(self) -> None:
        with self._lock:
            if self._context is not None:
                self._context.close()
                self._context = None
            if self._playwright is not None:
                self._playwright.stop()
                self._playwright = None
            self._page = None

    def search_company(self, company_name: str) -> VerifCompanyMatch | None:
        query = str(company_name or "").strip()
        if not query:
            return None
        with self._lock:
            page = self._ensure_page()
            search_url = f"https://www.verif.com/en/searchResult/?search={quote(query)}&country=IT"
            self._goto_and_wait_ready(page, search_url, expect_company_page=False)
            items = self._collect_search_links(page)
            if not items:
                return None
            picked = pick_best_company_link(items, query)
            if picked is None:
                return None
            picked_name, company_url = picked
            self._goto_and_wait_ready(page, company_url, expect_company_page=True)
            page_text = page.locator("body").inner_text(timeout=5000)
            website, representative = extract_company_fields_from_text(page_text)
            return VerifCompanyMatch(
                company_name=picked_name or query,
                representative=representative,
                website=website,
                company_url=page.url,
                search_url=search_url,
            )

    def _ensure_page(self) -> Page:
        if self._page is not None:
            return self._page
        self._profile_dir.mkdir(parents=True, exist_ok=True)
        self._playwright = sync_playwright().start()
        launch_kwargs = {
            "user_data_dir": str(self._profile_dir),
            "headless": self._headless,
            "channel": "chrome",
            "ignore_https_errors": True,
            "viewport": {"width": 1440, "height": 960},
        }
        if self._proxy_url:
            launch_kwargs["proxy"] = {"server": self._proxy_url}
        self._context = self._playwright.chromium.launch_persistent_context(**launch_kwargs)
        self._page = self._context.new_page()
        return self._page

    def _goto_and_wait_ready(self, page: Page, url: str, *, expect_company_page: bool) -> None:
        page.goto(url, wait_until="domcontentloaded", timeout=60000)
        deadline = time.monotonic() + self._timeout_seconds
        last_title = ""
        last_text = ""
        while time.monotonic() < deadline:
            time.sleep(4)
            last_title = page.title()
            last_text = page.locator("body").inner_text(timeout=5000)
            if _looks_like_verif_challenge(last_title, last_text):
                continue
            if expect_company_page:
                if _company_page_ready(last_text):
                    return None
            elif _search_page_ready(page, last_text):
                return None
        raise VerifChallengeError(
            f"Verif challenge 未在 {int(self._timeout_seconds)}s 内通过：title={last_title!r} url={page.url}"
        )

    def _collect_search_links(self, page: Page) -> list[tuple[str, str]]:
        raw_items = page.locator("a[href*='/en/company/']").evaluate_all(
            """nodes => nodes.map(node => ({
                href: node.href || '',
                text: (node.innerText || node.textContent || '').trim()
            }))"""
        )
        items: list[tuple[str, str]] = []
        for item in raw_items or []:
            href = str(item.get("href", "") or "").strip()
            text = str(item.get("text", "") or "").strip()
            if not href:
                continue
            items.append((text, _normalize_verif_url(href)))
        return items


def _looks_like_verif_challenge(title: str, body_text: str) -> bool:
    lowered_title = str(title or "").strip().lower()
    lowered_body = str(body_text or "").strip().lower()
    if any(hint in lowered_title for hint in _CHALLENGE_TITLE_HINTS):
        return True
    return any(hint in lowered_body for hint in _CHALLENGE_TEXT_HINTS)


def _search_page_ready(page: Page, body_text: str) -> bool:
    lowered = str(body_text or "").strip().lower()
    if any(hint in lowered for hint in _RESULT_READY_HINTS):
        return True
    return page.locator("a[href*='/en/company/']").count() > 0


def _company_page_ready(body_text: str) -> bool:
    lowered = str(body_text or "").strip().lower()
    return any(label in lowered for label in (*_WEBSITE_LABELS, *_REPRESENTATIVE_LABELS))


def _extract_value_after_labels(
    lines: list[str],
    labels: tuple[str, ...],
    *,
    normalize,
) -> str:
    lowered_labels = tuple(label.lower() for label in labels)
    for index, line in enumerate(lines):
        lowered = line.lower()
        for label in lowered_labels:
            if lowered == label:
                for offset in range(1, 4):
                    if index + offset >= len(lines):
                        break
                    candidate = normalize(lines[index + offset])
                    if candidate:
                        return candidate
            if lowered.startswith(label):
                tail = line[len(label):].strip(" :-|")
                candidate = normalize(tail)
                if candidate:
                    return candidate
    return ""


def _normalize_verif_url(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    return urljoin("https://www.verif.com/", text)


def _normalize_website(value: str) -> str:
    text = str(value or "").strip()
    if not text:
        return ""
    if not text.startswith(("http://", "https://")):
        text = f"https://{text}"
    parsed = urlparse(text)
    if not parsed.netloc:
        return ""
    return f"https://{parsed.netloc.lower()}{parsed.path or ''}".rstrip("/")


def _normalize_person(value: str) -> str:
    text = re.sub(r"\s+", " ", str(value or "").strip())
    if not text or len(text) > 160:
        return ""
    if any(ch.isdigit() for ch in text):
        return ""
    return text


def _tokenize_name(value: str) -> list[str]:
    return [item for item in re.split(r"[^a-z0-9]+", str(value or "").lower()) if item]
