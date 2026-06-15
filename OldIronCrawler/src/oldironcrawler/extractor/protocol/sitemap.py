from __future__ import annotations

import gzip
import re
from collections.abc import Callable
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree

from oldironcrawler.extractor.protocol.content import decode_bytes
from oldironcrawler.extractor.protocol_discovery import (
    is_supported_url,
    prioritize_discovery_urls,
)
from oldironcrawler.extractor.protocol_runtime import request_slot

_ROBOTS_SITEMAP_RE = re.compile(r"^Sitemap:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}


def discover_sitemap_urls(base_url: str, *, limit: int, fetch_text: Callable[[str], str]) -> list[str]:
    locations = _find_sitemap_locations(base_url, fetch_text)
    if not locations:
        locations = [urljoin(base_url, "/sitemap.xml")]
    scan_limit = min(max(limit * 4, limit), 400)
    urls: list[str] = []
    visited: set[str] = set()
    base_host = (urlparse(base_url).netloc or "").strip().lower()
    for location in locations:
        if len(urls) >= scan_limit:
            break
        _parse_sitemap_recursive(
            location,
            urls,
            visited,
            base_host=base_host,
            limit=scan_limit,
            depth=0,
            fetch_text=fetch_text,
        )
    return prioritize_discovery_urls(base_url, urls, limit=limit)


def fetch_sitemap_text(
    session: object,
    url: str,
    *,
    deadline_monotonic: float | None,
    request_timeout: Callable[..., float],
    request_slot_wait_timeout: Callable[..., float],
) -> str:
    try:
        timeout_seconds = request_timeout(deadline_monotonic=deadline_monotonic)
        with request_slot(
            timeout_seconds=timeout_seconds,
            wait_timeout_seconds=request_slot_wait_timeout(
                timeout_seconds,
                deadline_monotonic=deadline_monotonic,
            ),
        ):
            timeout_seconds = request_timeout(deadline_monotonic=deadline_monotonic)
            response = session.get(url, timeout=timeout_seconds)
        if int(response.status_code) != 200:
            return ""
        content = response.content or b""
        if url.endswith(".gz") or content[:2] == b"\x1f\x8b":
            try:
                content = gzip.decompress(content)
            except Exception:  # noqa: BLE001
                pass
        return decode_bytes(content, str(response.headers.get("Content-Type", "") or ""))
    except Exception:  # noqa: BLE001
        return ""


def _find_sitemap_locations(base_url: str, fetch_text: Callable[[str], str]) -> list[str]:
    text = fetch_text(urljoin(base_url, "/robots.txt"))
    return [item.strip() for item in _ROBOTS_SITEMAP_RE.findall(text) if item.strip()]


def _parse_sitemap_recursive(
    sitemap_url: str,
    result: list[str],
    visited: set[str],
    *,
    base_host: str,
    limit: int,
    depth: int,
    fetch_text: Callable[[str], str],
) -> None:
    if depth > 3 or sitemap_url in visited or len(result) >= limit:
        return
    visited.add(sitemap_url)
    xml_text = fetch_text(sitemap_url)
    if not xml_text:
        return
    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError:
        return
    tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag
    if tag == "sitemapindex":
        _parse_sitemap_children(root, result, visited, base_host=base_host, limit=limit, depth=depth, fetch_text=fetch_text)
        return
    _append_sitemap_page_urls(root, result, visited, base_host=base_host, limit=limit)


def _parse_sitemap_children(
    root: ElementTree.Element,
    result: list[str],
    visited: set[str],
    *,
    base_host: str,
    limit: int,
    depth: int,
    fetch_text: Callable[[str], str],
) -> None:
    for child_loc in root.findall(".//sm:sitemap/sm:loc", _NS):
        child_url = str(child_loc.text or "").strip()
        if child_url:
            _parse_sitemap_recursive(
                child_url,
                result,
                visited,
                base_host=base_host,
                limit=limit,
                depth=depth + 1,
                fetch_text=fetch_text,
            )


def _append_sitemap_page_urls(
    root: ElementTree.Element,
    result: list[str],
    visited: set[str],
    *,
    base_host: str,
    limit: int,
) -> None:
    for loc in root.findall(".//sm:url/sm:loc", _NS):
        page_url = str(loc.text or "").strip()
        if not page_url or page_url in visited or not is_supported_url(page_url):
            continue
        host = (urlparse(page_url).netloc or "").strip().lower()
        if host == base_host or host.endswith(f".{base_host}") or base_host.endswith(f".{host}"):
            visited.add(page_url)
            result.append(page_url)
            if len(result) >= limit:
                return
