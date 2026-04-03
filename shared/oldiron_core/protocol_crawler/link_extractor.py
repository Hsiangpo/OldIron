"""HTML 页面站内链接提取（sitemap 不存在时的 fallback）。"""

from __future__ import annotations

import logging
import re
from urllib.parse import urljoin, urlparse

LOGGER = logging.getLogger(__name__)

# 简单的 <a href="..."> 正则，避免依赖 BeautifulSoup
_HREF_RE = re.compile(r'<a\s[^>]*href=["\']([^"\']+)["\']', re.IGNORECASE)

# 跳过的文件扩展名
_SKIP_EXTENSIONS = frozenset({
    ".jpg", ".jpeg", ".png", ".gif", ".svg", ".ico", ".webp",
    ".css", ".js", ".woff", ".woff2", ".ttf", ".eot",
    ".mp3", ".mp4", ".avi", ".mov", ".wmv",
    ".zip", ".tar", ".gz", ".rar", ".7z",
    ".exe", ".dmg", ".apk",
})


def extract_same_site_links(
    html: str,
    page_url: str,
    *,
    limit: int = 200,
    include_subdomains: bool = False,
) -> list[str]:
    """从 HTML 中提取同站链接，去重和归一化。

    Args:
        html: 页面 HTML 内容
        page_url: 当前页面 URL（用于解析相对链接和同站判断）
        limit: 最多返回的链接数

    Returns:
        去重后的同站 URL 列表
    """
    base_host = urlparse(page_url).netloc.lower()
    if not base_host:
        return []

    seen: set[str] = set()
    result: list[str] = []

    for raw_href in _HREF_RE.findall(html):
        href = raw_href.strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue

        # 解析为绝对 URL
        try:
            absolute = urljoin(page_url, href)
            parsed = urlparse(absolute)
        except ValueError:
            LOGGER.debug("跳过异常 href：page=%s href=%s", page_url, href)
            continue

        # 只保留同站链接
        link_host = parsed.netloc.lower()
        if not _is_same_site(base_host, link_host, include_subdomains=include_subdomains):
            continue

        # 跳过静态资源
        path_lower = parsed.path.lower()
        if any(path_lower.endswith(ext) for ext in _SKIP_EXTENSIONS):
            continue

        # 去掉 fragment，保留 query
        normalized = f"{parsed.scheme}://{parsed.netloc}{parsed.path}"
        if parsed.query:
            normalized = f"{normalized}?{parsed.query}"

        if normalized not in seen:
            seen.add(normalized)
            result.append(normalized)
            if len(result) >= limit:
                break

    return result


def _is_same_site(base_host: str, link_host: str, *, include_subdomains: bool) -> bool:
    """判断两个 host 是否属于同一站点。"""
    if not link_host:
        return False
    if base_host == link_host:
        return True
    # www.example.com 和 example.com 视为同站
    if base_host.startswith("www."):
        bare = base_host[4:]
    else:
        bare = base_host
    if link_host == bare or link_host == f"www.{bare}":
        return True
    if include_subdomains and link_host.endswith(f".{bare}"):
        return True
    return False
