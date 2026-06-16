from __future__ import annotations

import html
import re
import threading
from collections.abc import Callable
from urllib.parse import urlparse

from oldironcrawler.extractor.protocol.content import (
    decode_response_text as _decode_response_text,
    extract_meta_refresh_url as _extract_meta_refresh_url,
    extract_same_site_html_redirect_url as _extract_same_site_html_redirect_url,
    raise_if_challenge_page as _raise_if_challenge_page,
    truncate_html as _truncate_html,
)
from oldironcrawler.extractor.protocol.errors import ProtocolPermanentError, ProtocolTemporaryError
from oldironcrawler.extractor.protocol.fallbacks import (
    build_host_fallback_urls as _build_host_fallback_urls,
    is_supported_response as _is_supported_response,
)

_HEAD_OPEN_RE = re.compile(r"(<head\b[^>]*>)", re.IGNORECASE)


def fetch_discovery_homepage_with_host_fallback(
    start_url: str,
    timeout_seconds: float,
    fetch_homepage: Callable[[str, float], str],
) -> str:
    try:
        return fetch_homepage(start_url, timeout_seconds)
    except (ProtocolPermanentError, ProtocolTemporaryError) as exc:
        for fallback_url in _build_host_fallback_urls(start_url, str(exc or "").lower()):
            try:
                return fetch_homepage(fallback_url, timeout_seconds)
            except Exception:  # noqa: BLE001
                continue
        raise


def fetch_discovery_homepage_httpx(
    start_url: str,
    timeout_seconds: float,
    *,
    fetch_direct: Callable[[str, float], object],
    normalize_response: Callable[[str, object], str],
) -> str:
    result: dict[str, object] = {}

    def worker() -> None:
        try:
            result["html"] = _fetch_normalized_discovery_homepage(
                start_url,
                timeout_seconds,
                fetch_direct=fetch_direct,
                normalize_response=normalize_response,
            )
        except BaseException as exc:  # noqa: BLE001
            result["error"] = exc

    thread = threading.Thread(target=worker, name="oldiron-discovery-homepage", daemon=True)
    thread.start()
    thread.join(timeout=max(timeout_seconds, 0.01))
    if thread.is_alive():
        raise ProtocolTemporaryError(f"site_open_timeout: {start_url}")
    error = result.get("error")
    if isinstance(error, (ProtocolPermanentError, ProtocolTemporaryError)):
        raise error
    if isinstance(error, BaseException):
        raise ProtocolTemporaryError(str(error)) from error
    return str(result.get("html") or "")


def _fetch_normalized_discovery_homepage(
    start_url: str,
    timeout_seconds: float,
    *,
    fetch_direct: Callable[[str, float], object],
    normalize_response: Callable[[str, object], str],
) -> str:
    response = fetch_direct(start_url, timeout_seconds)
    html_text = normalize_response(start_url, response)
    target_url = _extract_meta_refresh_url(html_text, start_url, allow_cross_site=True)
    if not target_url:
        target_url = _extract_same_site_html_redirect_url(html_text, start_url)
    if not target_url or target_url == start_url:
        return html_text
    redirected = normalize_response(start_url, fetch_direct(target_url, timeout_seconds))
    return redirected if redirected.strip() else html_text


def normalize_discovery_homepage_response(start_url: str, response: object, *, max_html_chars: int) -> str:
    if response is None:
        raise ProtocolTemporaryError(f"site_open_timeout: {start_url}")
    status = int(getattr(response, "status_code", 0) or 0)
    headers = getattr(response, "headers", {}) or {}
    response_text = _truncate_html(_response_text_for_discovery(response), max_html_chars)
    if status in {429, 500, 502, 503, 504}:
        raise ProtocolTemporaryError(f"temporary_http_{status}: {start_url}")
    if status == 403:
        _raise_if_challenge_page(start_url, response_text)
        raise ProtocolPermanentError(f"http_403: {start_url}")
    if status == 404:
        return ""
    if status != 200:
        raise ProtocolPermanentError(f"http_{status}: {start_url}")
    content_type = str(headers.get("Content-Type", "") or "").lower()
    if not _is_supported_response(start_url, content_type):
        return ""
    _raise_if_challenge_page(start_url, response_text)
    return _inject_final_url_base(response_text, start_url, response)


def _response_text_for_discovery(response: object) -> str:
    content = getattr(response, "content", None)
    if content is not None:
        return _decode_response_text(response)
    return str(getattr(response, "text", "") or "")


def _inject_final_url_base(html_text: str, start_url: str, response: object) -> str:
    final_url = str(getattr(response, "url", "") or "").strip()
    if not final_url or final_url == start_url:
        return html_text
    parsed = urlparse(final_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        return html_text
    base_tag = f'<base href="{html.escape(final_url, quote=True)}">'
    lowered_head = html_text[:500].lower()
    if "<head" in lowered_head:
        return _HEAD_OPEN_RE.sub(lambda match: f"{match.group(1)}{base_tag}", html_text, count=1)
    return f"{base_tag}{html_text}"
