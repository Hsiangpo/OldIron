from __future__ import annotations

import html
import re
from urllib.parse import urljoin, urlparse

_ANCHOR_TAG_RE = re.compile(r"<a\b(?P<attrs>[^>]*)>", re.IGNORECASE | re.DOTALL)
_BASE_TAG_RE = re.compile(r"<base\b(?P<attrs>[^>]*)>", re.IGNORECASE | re.DOTALL)
_LINK_TAG_RE = re.compile(r"<link\b(?P<attrs>[^>]*)>", re.IGNORECASE | re.DOTALL)
_HREF_ATTR_RE = re.compile(r"\bhref\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)
_REL_ATTR_RE = re.compile(r"\brel\s*=\s*[\"']([^\"']+)[\"']", re.IGNORECASE)


def pick_relative_link_base_url(html_text: str, page_url: str) -> str:
    declared_base_url = _extract_declared_link_base_url(html_text, page_url)
    if declared_base_url:
        return declared_base_url
    directory_base_url = _ensure_directory_base_url(page_url)
    parsed = urlparse(directory_base_url)
    base_host = (parsed.netloc or "").strip().lower()
    dominant_host = _pick_dominant_www_pair_host(html_text, base_host)
    if not dominant_host:
        return directory_base_url
    return parsed._replace(netloc=dominant_host).geturl()


def allowed_link_hosts(base_host: str, join_base: str) -> list[str]:
    hosts = [str(base_host or "").strip().lower()]
    join_host = (urlparse(str(join_base or "")).netloc or "").strip().lower()
    if join_host and join_host not in hosts:
        hosts.append(join_host)
    return [host for host in hosts if host]


def link_host_allowed(link_host: str, allowed_hosts: list[str]) -> bool:
    for host in allowed_hosts:
        if link_host == host or link_host.endswith(f".{host}") or host.endswith(f".{link_host}"):
            return True
    return False


def _extract_declared_link_base_url(html_text: str, page_url: str) -> str:
    for tag_match in _BASE_TAG_RE.finditer(str(html_text or "")):
        value = _extract_href_from_attrs(tag_match.group("attrs") or "", page_url)
        if value:
            return _ensure_directory_base_url(value)
    for tag_match in _LINK_TAG_RE.finditer(str(html_text or "")):
        attrs = tag_match.group("attrs") or ""
        rel_match = _REL_ATTR_RE.search(attrs)
        if rel_match is None:
            continue
        rel_tokens = {token.strip().lower() for token in re.split(r"\s+", rel_match.group(1) or "") if token.strip()}
        if "canonical" not in rel_tokens:
            continue
        value = _extract_href_from_attrs(attrs, page_url)
        if value:
            return _ensure_directory_base_url(value)
    return ""


def _extract_href_from_attrs(attrs: str, page_url: str) -> str:
    href_match = _HREF_ATTR_RE.search(str(attrs or ""))
    if href_match is None:
        return ""
    value = html.unescape(str(href_match.group(1) or "")).strip()
    if not value or value.startswith(("#", "javascript:", "mailto:", "tel:")):
        return ""
    parsed = urlparse(urljoin(page_url, value))
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return ""
    return parsed._replace(query="", fragment="").geturl()


def _ensure_directory_base_url(page_url: str) -> str:
    parsed = urlparse(page_url)
    path = str(parsed.path or "")
    if not path or path.endswith("/"):
        return page_url
    last_segment = path.rsplit("/", 1)[-1]
    if "." in last_segment:
        return page_url
    return parsed._replace(path=f"{path}/").geturl()


def _pick_dominant_www_pair_host(html_text: str, base_host: str) -> str:
    counts: dict[str, int] = {}
    for raw_href in _iter_anchor_hrefs(html_text):
        parsed = urlparse(str(raw_href or "").strip())
        host = (parsed.netloc or "").strip().lower()
        if not host or not _is_www_pair(base_host, host):
            continue
        counts[host] = counts.get(host, 0) + 1
    if not counts:
        return ""
    host, count = max(counts.items(), key=lambda item: (item[1], item[0]))
    return host if count >= 2 else ""


def _iter_anchor_hrefs(html_text: str) -> list[str]:
    values: list[str] = []
    for match in _ANCHOR_TAG_RE.finditer(str(html_text or "")):
        href_match = _HREF_ATTR_RE.search(match.group("attrs") or "")
        if href_match is not None:
            values.append(html.unescape(href_match.group(1) or ""))
    return values


def _is_www_pair(left: str, right: str) -> bool:
    if not left or not right or left == right:
        return False
    return left == f"www.{right}" or right == f"www.{left}"
