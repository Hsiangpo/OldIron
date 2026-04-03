"""Sitemap 解析：从 robots.txt / sitemap.xml 中提取 URL 列表。"""

from __future__ import annotations

import gzip
import logging
import re
from urllib.parse import urljoin, urlparse
from xml.etree import ElementTree

from curl_cffi import requests as cffi_requests

LOGGER = logging.getLogger(__name__)

# sitemap XML 命名空间
_NS = {"sm": "http://www.sitemaps.org/schemas/sitemap/0.9"}

# robots.txt 中 Sitemap 声明的正则
_ROBOTS_SITEMAP_RE = re.compile(r"^Sitemap:\s*(.+)$", re.IGNORECASE | re.MULTILINE)
_SKIP_EXTENSIONS = frozenset({
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".webp",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
    ".mp3", ".mp4", ".avi", ".mov", ".wmv",
    ".pdf", ".doc", ".docx", ".xls", ".xlsx", ".ppt", ".pptx",
    ".zip", ".tar", ".gz", ".rar", ".7z",
    ".exe", ".dmg", ".apk",
})


def discover_sitemap_urls(
    session: cffi_requests.Session,
    base_url: str,
    *,
    limit: int = 200,
    timeout: float = 20.0,
    include_subdomains: bool = False,
) -> list[str]:
    """从目标站点发现 sitemap 并解析出 URL 列表。

    流程：robots.txt → sitemap 声明 → 解析 sitemap（支持 index 递归）。
    如果 robots.txt 没有声明，尝试默认 /sitemap.xml。
    """
    sitemap_locations = _find_sitemap_locations(session, base_url, timeout=timeout)
    if not sitemap_locations:
        # 尝试默认路径
        sitemap_locations = [urljoin(base_url, "/sitemap.xml")]

    urls: list[str] = []
    visited: set[str] = set()
    base_host = (urlparse(base_url).netloc or "").strip().lower()
    for loc in sitemap_locations:
        if len(urls) >= limit:
            break
        _parse_sitemap_recursive(
            session, loc, urls, visited,
            limit=limit, timeout=timeout, depth=0,
            base_host=base_host,
            include_subdomains=include_subdomains,
        )
    return urls[:limit]


def _find_sitemap_locations(
    session: cffi_requests.Session,
    base_url: str,
    *,
    timeout: float,
) -> list[str]:
    """从 robots.txt 提取 Sitemap 声明。"""
    robots_url = urljoin(base_url, "/robots.txt")
    try:
        resp = session.get(robots_url, timeout=timeout)
        if resp.status_code != 200:
            return []
        text = resp.text or ""
        matches = _ROBOTS_SITEMAP_RE.findall(text)
        return [m.strip() for m in matches if m.strip()]
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("robots.txt 请求失败：%s — %s", robots_url, exc)
        return []


def _parse_sitemap_recursive(
    session: cffi_requests.Session,
    sitemap_url: str,
    result: list[str],
    visited: set[str],
    *,
    limit: int,
    timeout: float,
    depth: int,
    base_host: str,
    include_subdomains: bool,
) -> None:
    """递归解析 sitemap（含 sitemapindex）。"""
    if depth > 3 or sitemap_url in visited or len(result) >= limit:
        return
    visited.add(sitemap_url)

    xml_text = _fetch_sitemap_text(session, sitemap_url, timeout=timeout)
    if not xml_text:
        return

    try:
        root = ElementTree.fromstring(xml_text)
    except ElementTree.ParseError as exc:
        LOGGER.debug("sitemap XML 解析失败：%s — %s", sitemap_url, exc)
        return

    # 检测是 sitemapindex 还是普通 urlset
    tag = root.tag.split("}")[-1] if "}" in root.tag else root.tag

    if tag == "sitemapindex":
        for child_loc in root.findall(".//sm:sitemap/sm:loc", _NS):
            if len(result) >= limit:
                return
            child_url = (child_loc.text or "").strip()
            if child_url:
                _parse_sitemap_recursive(
                    session, child_url, result, visited,
                    limit=limit, timeout=timeout, depth=depth + 1,
                    base_host=base_host,
                    include_subdomains=include_subdomains,
                )
    else:
        for loc in root.findall(".//sm:url/sm:loc", _NS):
            url = (loc.text or "").strip()
            if (
                url
                and url not in visited
                and not _should_skip_candidate_url(url)
                and _is_allowed_host(base_host, url, include_subdomains=include_subdomains)
            ):
                visited.add(url)
                result.append(url)
                if len(result) >= limit:
                    return


def _should_skip_candidate_url(candidate_url: str) -> bool:
    path = (urlparse(candidate_url).path or "").lower()
    return any(path.endswith(ext) for ext in _SKIP_EXTENSIONS)


def _is_allowed_host(base_host: str, candidate_url: str, *, include_subdomains: bool) -> bool:
    link_host = (urlparse(candidate_url).netloc or "").strip().lower()
    if not base_host or not link_host:
        return False
    if base_host == link_host:
        return True
    bare = base_host[4:] if base_host.startswith("www.") else base_host
    if link_host == bare or link_host == f"www.{bare}":
        return True
    if include_subdomains and link_host.endswith(f".{bare}"):
        return True
    return False


def _fetch_sitemap_text(
    session: cffi_requests.Session,
    url: str,
    *,
    timeout: float,
) -> str:
    """获取 sitemap 内容，支持 .gz 压缩。"""
    try:
        resp = session.get(url, timeout=timeout)
        if resp.status_code != 200:
            return ""
        content = resp.content or b""
        # 尝试 gzip 解压
        if url.endswith(".gz") or content[:2] == b"\x1f\x8b":
            try:
                content = gzip.decompress(content)
            except Exception:  # noqa: BLE001
                pass
        return content.decode("utf-8", errors="replace")
    except Exception as exc:  # noqa: BLE001
        LOGGER.debug("sitemap 获取失败：%s — %s", url, exc)
        return ""
