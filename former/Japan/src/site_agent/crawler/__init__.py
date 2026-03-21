from __future__ import annotations

import asyncio
import codecs
import io
import re
from html.parser import HTMLParser
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping
from urllib.parse import urljoin, urlparse

from ..firecrawl_client import FirecrawlClient, FirecrawlConfig, FirecrawlError
from ..firecrawl_key_pool import KeyPool, KeyPoolConfig
from ..models import LinkItem, PageContent

_FIT_MARKDOWN_QUERY = (
    "会社概要 会社案内 会社情報 企業情報 企業概要 会社紹介 企業紹介 会社データ "
    "代表 代表取締役 代表者 社長 役員 役員紹介 役員一覧 経営陣 代表挨拶 会社沿革 沿革 アクセス"
)
_NAV_ANCHOR_TOKENS = (
    "nav",
    "menu",
    "gnav",
    "global",
    "header",
    "navbar",
    "top",
    "main-nav",
    "breadcrumb",
    "breadcrumbs",
    "pankuzu",
    "topicpath",
    "trail",
)


class CrawlerClient:
    def __init__(
        self,
        semaphore: asyncio.Semaphore,
        page_timeout: int = 60000,
        keys_path: str | Path | None = None,
        base_url: str | None = None,
        per_key_limit: int = 2,
        wait_seconds: int = 20,
        proxy: str | None = None,
    ) -> None:
        self._sem = semaphore
        self._page_timeout = page_timeout
        self._reset_requested = False
        self._active_fetches = 0
        self._proxy = proxy
        path = Path(keys_path) if keys_path else Path("output") / "firecrawl_keys.txt"
        keys = KeyPool.load_keys(path)
        pool_config = KeyPoolConfig(
            per_key_limit=per_key_limit, wait_seconds=wait_seconds
        )
        key_pool = KeyPool(keys, pool_config, key_file_path=path)
        fc_config = FirecrawlConfig(
            base_url=(base_url or "https://api.firecrawl.dev/v2/"),
            timeout_ms=page_timeout,
        )
        self._client = FirecrawlClient(key_pool, fc_config)

    async def __aenter__(self) -> "CrawlerClient":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        return None

    async def fetch_page(self, url: str) -> PageContent:
        async with self._sem:
            self._active_fetches += 1
            try:
                data = await self._scrape_with_key_wait(
                    url, timeout_ms=self._page_timeout, rendered=False
                )
            except FirecrawlError as exc:
                return PageContent(url=url, markdown="", success=False, error=exc.code)
            finally:
                self._active_fetches = max(0, self._active_fetches - 1)

        payload = _extract_firecrawl_payload(data)
        if not payload:
            return PageContent(
                url=url, markdown="", success=False, error="firecrawl_empty"
            )

        markdown = payload.get("markdown") or ""
        raw_html = payload.get("raw_html")
        title = _choose_best_title(payload.get("title"), raw_html, None)
        fit_markdown = _build_fit_markdown(markdown)
        links = _build_links_from_payload(payload, url)

        if not (markdown.strip() or (fit_markdown or "").strip() or raw_html):
            return PageContent(
                url=url, markdown="", success=False, error="empty_render"
            )

        return PageContent(
            url=payload.get("final_url") or url,
            markdown=markdown,
            fit_markdown=fit_markdown,
            raw_html=raw_html,
            title=title,
            links=links,
            success=True,
        )

    async def fetch_page_rendered(self, url: str) -> PageContent:
        async with self._sem:
            self._active_fetches += 1
            try:
                data = await self._scrape_with_key_wait(
                    url, timeout_ms=max(self._page_timeout, 60000), rendered=True
                )
            except FirecrawlError as exc:
                return PageContent(url=url, markdown="", success=False, error=exc.code)
            finally:
                self._active_fetches = max(0, self._active_fetches - 1)

        payload = _extract_firecrawl_payload(data)
        if not payload:
            return PageContent(
                url=url, markdown="", success=False, error="firecrawl_empty"
            )

        markdown = payload.get("markdown") or ""
        raw_html = payload.get("raw_html")
        title = _choose_best_title(payload.get("title"), raw_html, None)
        fit_markdown = _build_fit_markdown(markdown)
        links = _build_links_from_payload(payload, url)

        if not (markdown.strip() or (fit_markdown or "").strip() or raw_html):
            return PageContent(
                url=url, markdown="", success=False, error="empty_render"
            )

        return PageContent(
            url=payload.get("final_url") or url,
            markdown=markdown,
            fit_markdown=fit_markdown,
            raw_html=raw_html,
            title=title,
            links=links,
            success=True,
        )

    async def fetch_pages(self, urls: Iterable[str]) -> list[PageContent]:
        url_list = list(urls)
        tasks = [self.fetch_page(url) for url in url_list]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        pages: list[PageContent] = []
        for url, result in zip(url_list, results):
            if isinstance(result, PageContent):
                pages.append(result)
                continue
            if isinstance(result, asyncio.CancelledError):
                current = asyncio.current_task()
                if current is not None and current.cancelled():
                    raise result
                pages.append(
                    PageContent(url=url, markdown="", success=False, error="cancelled")
                )
                continue
            if isinstance(result, Exception):
                pages.append(
                    PageContent(
                        url=url,
                        markdown="",
                        success=False,
                        error=_summarize_crawl_error(result),
                    )
                )
                continue
            pages.append(
                PageContent(url=url, markdown="", success=False, error="crawl_failed")
            )
        return pages

    async def _scrape_with_key_wait(
        self, url: str, *, timeout_ms: int, rendered: bool
    ) -> dict[str, Any]:
        attempts = 4
        for attempt in range(attempts):
            try:
                return await self._client.scrape(
                    url, timeout_ms=timeout_ms, rendered=rendered
                )
            except RuntimeError as exc:
                message = str(exc).lower()
                if "no available firecrawl key" in message:
                    if attempt < attempts - 1:
                        await asyncio.sleep(2 * (attempt + 1))
                        continue
                    raise FirecrawlError("no available firecrawl key") from exc
                raise

    async def extract_fields(
        self,
        urls: list[str],
        *,
        prompt: str,
        schema: dict[str, Any],
    ) -> dict[str, Any]:
        async with self._sem:
            self._active_fetches += 1
            try:
                return await self._client.extract(urls, prompt=prompt, schema=schema)
            except FirecrawlError as exc:
                return {"error": exc.code}
            finally:
                self._active_fetches = max(0, self._active_fetches - 1)

    async def _reset_crawler(self) -> bool:
        return True

    async def _try_reset_if_idle(self) -> bool:
        if not self._reset_requested:
            return False
        if self._active_fetches > 0:
            return False
        ok = await self._reset_crawler()
        if ok:
            self._reset_requested = False
        return ok


def _extract_firecrawl_payload(
    data: Dict[str, Any] | None,
) -> Dict[str, Any] | None:
    if not isinstance(data, Mapping):
        return None
    payload = data.get("data") if isinstance(data.get("data"), Mapping) else data
    if not isinstance(payload, Mapping):
        return None
    markdown = (
        payload.get("markdown") if isinstance(payload.get("markdown"), str) else ""
    )
    raw_html = (
        payload.get("rawHtml") if isinstance(payload.get("rawHtml"), str) else None
    )
    if raw_html is None and isinstance(payload.get("html"), str):
        raw_html = payload.get("html")
    metadata_raw = payload.get("metadata")
    metadata = metadata_raw if isinstance(metadata_raw, Mapping) else {}
    title = metadata.get("title") if isinstance(metadata.get("title"), str) else None
    final_url = (
        metadata.get("sourceURL")
        if isinstance(metadata.get("sourceURL"), str)
        else None
    )
    return {
        "markdown": markdown,
        "raw_html": raw_html,
        "title": title,
        "final_url": final_url,
        "links": payload.get("links"),
    }


def _build_fit_markdown(markdown: str) -> str | None:
    if not isinstance(markdown, str) or not markdown.strip():
        return None
    keywords = [k for k in _FIT_MARKDOWN_QUERY.split() if k]
    if not keywords:
        return None
    picked: list[str] = []
    for line in markdown.splitlines():
        if any(k in line for k in keywords):
            picked.append(line)
    if not picked:
        return None
    text = "\n".join(picked).strip()
    return text[:4000] if text else None


def _build_links_from_payload(payload: Dict[str, Any], base_url: str) -> list[LinkItem]:
    links_obj = payload.get("links")
    links: list[LinkItem] = []
    if isinstance(links_obj, list):
        if links_obj and isinstance(links_obj[0], Mapping):
            links = _extract_links(links_obj)  # type: ignore[arg-type]
        else:
            links = _normalize_and_filter_links(
                [str(item) for item in links_obj], base_url
            )
    raw_html = payload.get("raw_html")
    if isinstance(raw_html, str) and raw_html.strip():
        html_links, context_map = _extract_links_with_nav(raw_html, base_url)
        if not links and html_links:
            links = html_links
        elif links and context_map:
            _merge_link_context(links, context_map)
    return links


def _normalize_url_for_match(url: str) -> str:
    if not url:
        return ""
    normalized = url.split("#", 1)[0].strip()
    if normalized.endswith("/") and len(normalized) > 1:
        normalized = normalized.rstrip("/")
    return normalized


def _normalize_internal_url(base_url: str, href: str) -> str | None:
    href = (href or "").strip()
    if not href:
        return None
    resolved = urljoin(base_url, href)
    parsed = urlparse(resolved)
    if not parsed.scheme.startswith("http"):
        return None
    if parsed.netloc != urlparse(base_url).netloc:
        return None
    return _normalize_url_for_match(resolved)


def _extract_links_with_nav(
    html: str, base_url: str
) -> tuple[list[LinkItem], Dict[str, LinkItem]]:
    if not html:
        return [], {}
    parser = _HTMLNavLinkParser()
    try:
        parser.feed(html)
    except Exception:
        return [], {}
    items: list[LinkItem] = []
    context_map: Dict[str, LinkItem] = {}
    seen: set[str] = set()
    for href, text, is_nav in parser.links:
        normalized = _normalize_internal_url(base_url, href)
        if not normalized:
            continue
        if normalized in seen:
            continue
        seen.add(normalized)
        item = LinkItem(
            url=normalized,
            text=text.strip() if isinstance(text, str) and text.strip() else None,
            is_nav=is_nav,
        )
        items.append(item)
        context_map[normalized] = item
    return items, context_map


def _merge_link_context(
    links: list[LinkItem], context_map: Dict[str, LinkItem]
) -> None:
    if not links or not context_map:
        return
    for item in links:
        key = _normalize_url_for_match(item.url or "")
        if not key:
            continue
        ctx = context_map.get(key)
        if not ctx:
            continue
        if not item.text and ctx.text:
            item.text = ctx.text
        if getattr(ctx, "is_nav", False):
            item.is_nav = True


def _extract_links(raw_links: list[Dict[str, Any]] | None) -> list[LinkItem]:
    items: list[LinkItem] = []
    for item in raw_links or []:
        href = item.get("href") if isinstance(item, Mapping) else None
        text = item.get("text") if isinstance(item, Mapping) else None
        if isinstance(href, str) and href.strip():
            items.append(
                LinkItem(
                    url=href.strip(),
                    text=text.strip() if isinstance(text, str) else None,
                )
            )
    return items


def _needs_render_fallback(page: PageContent) -> bool:
    content_len = len((page.markdown or "").strip())
    if _looks_like_cloudflare_html(page.raw_html or ""):
        return True
    if page.links:
        return False
    return content_len < 80


_META_CHARSET_RE = re.compile(r"<meta[^>]+charset=[\"']?([^\"'>\s;]+)", re.IGNORECASE)
_META_HTTP_EQUIV_RE = re.compile(
    r"<meta[^>]+http-equiv=[\"']content-type[\"'][^>]+content=[\"'][^\"']*charset=([^\"'>\s;]+)",
    re.IGNORECASE,
)


def _normalize_encoding_name(name: str | None) -> str | None:
    if not name:
        return None
    cleaned = name.strip().strip(";").strip().lower()
    if not cleaned:
        return None
    aliases = {
        "shift_jis": "cp932",
        "shift-jis": "cp932",
        "sjis": "cp932",
        "x-sjis": "cp932",
        "windows-31j": "cp932",
    }
    return aliases.get(cleaned, cleaned)


def _detect_html_charset(raw: bytes) -> str | None:
    if not raw:
        return None
    if raw.startswith(codecs.BOM_UTF8):
        return "utf-8-sig"
    head = raw[:4096].decode("ascii", errors="ignore")
    for regex in (_META_CHARSET_RE, _META_HTTP_EQUIV_RE):
        match = regex.search(head)
        if match:
            return match.group(1)
    return None


def _choose_best_title(
    title: str | None, raw_html: str | None, fallback_title: str | None
) -> str | None:
    def clean(value: str | None) -> str | None:
        if not isinstance(value, str):
            return None
        text = value.strip()
        return text if text else None

    def ok(value: str | None) -> bool:
        return bool(value) and not _looks_mojibake_text(value or "")

    primary = clean(title)
    fallback = clean(fallback_title)
    parsed = (
        _extract_title_from_html(raw_html)
        if isinstance(raw_html, str) and raw_html.strip()
        else None
    )

    if ok(primary):
        return primary
    if ok(parsed):
        return parsed
    if ok(fallback):
        return fallback

    for candidate in (primary, parsed, fallback):
        if not candidate:
            continue
        fixed = _repair_mojibake_text(candidate)
        if ok(fixed):
            return fixed
    return primary or parsed or fallback


def _extract_title_from_html(raw_html: str | None) -> str | None:
    if not isinstance(raw_html, str) or not raw_html.strip():
        return None
    parser = _HTMLPreviewParser()
    try:
        parser.feed(raw_html)
    except Exception:
        return None
    title = parser.title.strip() if parser.title else None
    return title if title else None


def _looks_mojibake_text(text: str) -> bool:
    if not text:
        return False
    if "�" in text:
        return True
    cjk = sum(1 for ch in text if _is_cjk_char(ch))
    latin1 = sum(1 for ch in text if 0xC0 <= ord(ch) <= 0xFF)
    if cjk == 0 and latin1 >= 2:
        return True
    markers = sum(1 for ch in text if ch in ("Ã", "Â", "â", "ä", "å", "æ", "ç"))
    if cjk == 0 and markers >= 2:
        return True
    return False


def _repair_mojibake_text(text: str) -> str:
    if not text:
        return text
    try:
        raw = text.encode("latin1")
    except Exception:
        return text

    candidates: list[str] = []
    for enc in ("utf-8", "cp932", "cp949"):
        try:
            candidates.append(raw.decode(enc))
        except Exception:
            continue
    if not candidates:
        return text

    def score(val: str) -> int:
        return sum(1 for ch in val if _is_cjk_char(ch))

    best = max(candidates, key=score)
    return best if score(best) > score(text) else text


def _is_cjk_char(ch: str) -> bool:
    code = ord(ch)
    return (
        0x4E00 <= code <= 0x9FFF or 0x3040 <= code <= 0x30FF or 0xAC00 <= code <= 0xD7A3
    )


def _swap_scheme(url: str) -> str | None:
    if url.startswith("http://"):
        return "https://" + url[len("http://") :]
    if url.startswith("https://"):
        return "http://" + url[len("https://") :]
    return None


def _looks_like_pdf_url(url: str) -> bool:
    parsed = urlparse(url or "")
    path = (parsed.path or "").lower()
    return path.endswith(".pdf")


def _looks_like_image_url(url: str) -> bool:
    parsed = urlparse(url or "")
    path = (parsed.path or "").lower()
    return path.endswith((".png", ".jpg", ".jpeg", ".webp", ".gif", ".bmp", ".svg"))


def _looks_like_cloudflare_html(text: str) -> bool:
    lower = (text or "").lower()
    return "cloudflare" in lower and (
        "cf-ray" in lower or "cf-chl" in lower or "checking your browser" in lower
    )


def _extract_pdf_text(data: bytes, max_chars: int = 40000) -> str:
    if not data:
        return ""
    text = _extract_pdf_text_pymupdf(data)
    if not text:
        text = _extract_pdf_text_pdfminer(data)
    if not text:
        return ""
    cleaned = re.sub(r"\s+", " ", text).strip()
    if max_chars > 0 and len(cleaned) > max_chars:
        cleaned = cleaned[:max_chars] + "…"
    return cleaned


def _extract_pdf_text_pymupdf(data: bytes) -> str:
    try:
        import fitz  # type: ignore
    except Exception:
        return ""
    try:
        doc = fitz.open(stream=data, filetype="pdf")
    except Exception:
        return ""
    parts: list[str] = []
    try:
        for page in doc:
            text = page.get_text("text")
            if not isinstance(text, str):
                text = str(text)
            parts.append(text or "")
    finally:
        doc.close()
    return "\n".join(parts).strip()


def _extract_pdf_text_pdfminer(data: bytes) -> str:
    try:
        from pdfminer.high_level import extract_text  # type: ignore
    except Exception:
        return ""
    try:
        return extract_text(io.BytesIO(data)) or ""
    except Exception:
        return ""


def _normalize_and_filter_links(raw_links: list[str], base_url: str) -> list[LinkItem]:
    base = urlparse(base_url)
    seen: set[str] = set()
    items: list[LinkItem] = []
    for href in raw_links:
        href = (href or "").strip()
        if not href:
            continue
        lowered = href.lower()
        if lowered.startswith(("mailto:", "tel:", "javascript:")):
            continue
        if href.startswith("#"):
            continue
        resolved = urljoin(base_url, href)
        parsed = urlparse(resolved)
        if not parsed.scheme.startswith("http"):
            continue
        if parsed.netloc != base.netloc:
            continue
        normalized = resolved.split("#", 1)[0]
        if normalized in seen:
            continue
        seen.add(normalized)
        items.append(LinkItem(url=normalized, text=None))
    return items


class _HTMLPreviewParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []
        self.description: str | None = None
        self._title_parts: list[str] = []
        self._body_parts: list[str] = []
        self._in_title = False
        self._in_body = False
        self._skip_text = False

    @property
    def title(self) -> str:
        return " ".join(
            part.strip() for part in self._title_parts if part.strip()
        ).strip()

    @property
    def body_text(self) -> str:
        text = " ".join(
            part.strip() for part in self._body_parts if part.strip()
        ).strip()
        return text[:4000]

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]):
        t = tag.lower()
        attrs_dict = {k.lower(): v for k, v in attrs if k}
        if t == "title":
            self._in_title = True
            return
        if t == "body":
            self._in_body = True
            return
        if t in ("script", "style", "noscript"):
            self._skip_text = True
            return
        if t == "a":
            href = attrs_dict.get("href")
            if href:
                self.links.append(href)
            return
        if t == "meta":
            name = (attrs_dict.get("name") or attrs_dict.get("property") or "").lower()
            if name in ("description", "og:description") and not self.description:
                content = attrs_dict.get("content")
                if content:
                    self.description = content
            return

    def handle_endtag(self, tag: str):
        t = tag.lower()
        if t == "title":
            self._in_title = False
            return
        if t == "body":
            self._in_body = False
            return
        if t in ("script", "style", "noscript"):
            self._skip_text = False
            return

    def handle_data(self, data: str):
        if not data:
            return
        if self._in_title:
            self._title_parts.append(data)
            return
        if self._in_body and not self._skip_text:
            chunk = data.strip()
            if chunk:
                self._body_parts.append(chunk)
            return


class _HTMLNavLinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[tuple[str, str, bool]] = []
        self._current_href: str | None = None
        self._current_text: list[str] = []
        self._current_is_nav = False
        self._nav_depth = 0
        self._header_depth = 0
        self._footer_depth = 0
        self._nav_like_depth = 0
        self._nav_like_stack: list[bool] = []

    def _is_nav_like_anchor(self, attrs: dict[str, str]) -> bool:
        blob = " ".join([attrs.get("class", ""), attrs.get("id", "")]).lower()
        return any(token in blob for token in _NAV_ANCHOR_TOKENS)

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]):
        t = tag.lower()
        attrs_dict = {k.lower(): (v or "") for k, v in attrs if k}
        if t == "nav":
            self._nav_depth += 1
        elif t == "header":
            self._header_depth += 1
        elif t == "footer":
            self._footer_depth += 1
        elif t in ("div", "ul", "ol", "section", "aside"):
            is_nav_like = self._is_nav_like_anchor(attrs_dict)
            self._nav_like_stack.append(is_nav_like)
            if is_nav_like:
                self._nav_like_depth += 1
        if t == "a":
            href = attrs_dict.get("href", "").strip()
            if href:
                self._current_href = href
                self._current_text = []
                label = (
                    attrs_dict.get("aria-label")
                    or attrs_dict.get("title")
                    or attrs_dict.get("data-label")
                    or attrs_dict.get("data-title")
                )
                if label:
                    self._current_text.append(label)
                nav_like = self._is_nav_like_anchor(attrs_dict)
                in_nav = (
                    self._nav_depth > 0
                    or self._header_depth > 0
                    or self._nav_like_depth > 0
                )
                in_footer = self._footer_depth > 0
                self._current_is_nav = (in_nav or nav_like) and not in_footer
        elif t == "img" and self._current_href is not None:
            alt = attrs_dict.get("alt")
            if alt:
                self._current_text.append(alt)

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            self._current_text.append(data)

    def handle_endtag(self, tag: str) -> None:
        t = tag.lower()
        if t == "a" and self._current_href is not None:
            text = "".join(self._current_text).strip()
            self.links.append((self._current_href, text, self._current_is_nav))
            self._current_href = None
            self._current_text = []
            self._current_is_nav = False
            return
        if t == "nav" and self._nav_depth > 0:
            self._nav_depth -= 1
        elif t == "header" and self._header_depth > 0:
            self._header_depth -= 1
        elif t == "footer" and self._footer_depth > 0:
            self._footer_depth -= 1
        elif t in ("div", "ul", "ol", "section", "aside") and self._nav_like_stack:
            was_nav_like = self._nav_like_stack.pop()
            if was_nav_like and self._nav_like_depth > 0:
                self._nav_like_depth -= 1


class _ChineseLogger:
    def debug(self, message: str, tag: str = "DEBUG", **kwargs):
        return

    def info(self, message: str, tag: str = "INFO", **kwargs):
        return

    def success(self, message: str, tag: str = "SUCCESS", **kwargs):
        return

    def warning(self, message: str, tag: str = "WARNING", **kwargs):
        return

    def error(self, message: str, tag: str = "ERROR", **kwargs):
        return

    def url_status(
        self,
        url: str,
        success: bool,
        timing: float,
        tag: str = "FETCH",
        url_length: int = 100,
    ):
        return

    def error_status(
        self, url: str, error: str, tag: str = "ERROR", url_length: int = 100
    ):
        return


def _summarize_crawl_error(exc: Exception) -> str:
    return _summarize_error_message(str(exc) or repr(exc))


def _summarize_error_message(message: str) -> str:
    msg = (message or "").strip()
    if not msg:
        return "crawl_failed"

    lines = [line.strip() for line in msg.splitlines() if line.strip()]

    for line in lines:
        if "net::" in line:
            return line

    for line in lines:
        if "timeout" in line.lower():
            return line

    head = " | ".join(lines[:3]) if lines else msg
    if len(head) > 400:
        head = head[:400] + "…"
    return head


def _should_reset_crawler_error(exc: Exception) -> bool:
    msg = (str(exc) or "").lower()
    patterns = (
        "connection closed",
        "pipe is closing",
        "pipe was closed",
        "timeout",
    )
    return any(p in msg for p in patterns)
