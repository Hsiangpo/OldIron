"""Wiza Italy Pipeline 1。"""

from __future__ import annotations

import html
import json
import logging
import math
import re
import time
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from .client import WizaClient
from .store import ItalyWizaStore


LOGGER = logging.getLogger("italy.wiza.pipeline")
CHECKPOINT_NAME = "list_checkpoint.json"
PAGE_SIZE = 100
EXPORT_INTERVAL_PAGES = 10
USAGE_LIMIT_WAIT_SECONDS = 60
TRANSIENT_WAIT_SECONDS = 30
CHALLENGE_WAIT_SECONDS = 300
_BAD_WEBSITE_HOSTS = {
    "share.google",
    "facebook.com",
    "www.facebook.com",
    "instagram.com",
    "www.instagram.com",
    "twitter.com",
    "www.twitter.com",
    "x.com",
    "www.x.com",
    "linkedin.com",
    "www.linkedin.com",
    "maps.app.goo.gl",
}


def run_pipeline_list(
    *,
    output_dir: Path,
    request_delay: float = 1.0,
    proxy: str = "",
    max_pages: int = 0,
    concurrency: int = 8,
) -> dict[str, int]:
    """抓取 Wiza Italy 公司列表，仅保留网站。"""
    del concurrency
    output_dir.mkdir(parents=True, exist_ok=True)
    store = ItalyWizaStore(output_dir / "companies.db")
    checkpoint = _load_checkpoint(output_dir)
    if checkpoint.get("status") == "done" and max_pages <= 0:
        _export_websites(output_dir, store)
        return {"pages": 0, "new_companies": 0, "total_companies": store.get_company_count()}
    client = WizaClient(output_dir, proxy)
    page_number = int(checkpoint.get("page") or 0) + 1
    search_after = checkpoint.get("search_after")
    processed_pages = 0
    new_companies = 0
    try:
        while True:
            try:
                page = client.search_companies(search_after=search_after, page_size=PAGE_SIZE)
            except Exception as exc:  # noqa: BLE001
                if _handle_retryable_error(
                    output_dir=output_dir,
                    store=store,
                    exc=exc,
                    page_number=page_number,
                    search_after=search_after,
                ):
                    continue
                raise
            if not page.items:
                _save_checkpoint(output_dir, page_number - 1, [], "done")
                store.update_checkpoint("list", page_number - 1, "done")
                break
            companies = _build_company_records(page.items)
            new_companies += store.upsert_companies(companies)
            processed_pages += 1
            total_pages = _estimate_total_pages(page.total, page.total_relation, page.page_size)
            _save_checkpoint(output_dir, page_number, page.last_sort, "running")
            store.update_checkpoint("list", page_number, "running")
            LOGGER.info("Wiza 页 %d/%s：解析 %d 家", page_number, total_pages or "?", len(companies))
            if _should_export_websites(processed_pages):
                _export_websites(output_dir, store)
            if max_pages > 0 and processed_pages >= max_pages:
                break
            if not page.last_sort:
                _save_checkpoint(output_dir, page_number, [], "done")
                store.update_checkpoint("list", page_number, "done")
                break
            search_after = page.last_sort
            page_number += 1
            time.sleep(max(request_delay, 0.0))
    finally:
        client.close()
        _export_websites(output_dir, store)
    return {
        "pages": processed_pages,
        "new_companies": new_companies,
        "total_companies": store.get_company_count(),
    }


def _build_company_records(items: list[dict[str, Any]]) -> list[dict[str, str]]:
    results: list[dict[str, str]] = []
    for item in items:
        company_name = str(item.get("name") or "").strip()
        website = _normalize_company_website(str(item.get("website") or "").strip())
        if company_name:
            results.append({"company_name": company_name, "website": website})
    return results


def _normalize_company_website(value: str) -> str:
    text = str(value or "").strip()
    if text and "://" not in text:
        text = f"https://{text}"
    return _normalize_website_url(text)


def _normalize_website_url(value: str) -> str:
    text = html.unescape(str(value or "")).strip(" \t\r\n,;|<>[](){}'\"")
    if not text:
        return ""
    matched = re.search(r"https?://[^\s<>'\"]+", text, flags=re.I)
    if matched is not None:
        text = matched.group(0)
    text = text.rstrip(".,;:)")
    parsed = urlparse(text)
    if parsed.scheme not in {"http", "https"}:
        return ""
    host = str(parsed.netloc or "").strip().lower()
    if not host or "+" in host or "." not in host or host in _BAD_WEBSITE_HOSTS:
        return ""
    suffix = host.rsplit(".", 1)[-1]
    if not re.fullmatch(r"[a-z]{2,24}", suffix):
        return ""
    normalized = f"{parsed.scheme}://{host}{parsed.path or ''}"
    if parsed.query:
        normalized = f"{normalized}?{parsed.query}"
    return normalized


def _estimate_total_pages(total: int, total_relation: str, page_size: int) -> int:
    if total <= 0 or page_size <= 0:
        return 0
    if str(total_relation or "").lower() != "eq":
        return 0
    return max(math.ceil(total / page_size), 1)


def _load_checkpoint(output_dir: Path) -> dict[str, Any]:
    checkpoint_path = output_dir / CHECKPOINT_NAME
    if not checkpoint_path.exists():
        return {}
    try:
        payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except Exception:
        LOGGER.warning("Wiza checkpoint 解析失败：%s", checkpoint_path)
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_checkpoint(output_dir: Path, page: int, search_after: list[Any], status: str) -> None:
    payload = {
        "page": int(page),
        "search_after": list(search_after or []),
        "status": str(status or "running"),
    }
    (output_dir / CHECKPOINT_NAME).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _export_websites(output_dir: Path, store: ItalyWizaStore) -> None:
    (output_dir / "websites.txt").write_text("\n".join(store.export_websites()), encoding="utf-8")


def _should_export_websites(processed_pages: int) -> bool:
    return processed_pages <= 1 or processed_pages % EXPORT_INTERVAL_PAGES == 0


def _handle_retryable_error(
    *,
    output_dir: Path,
    store: ItalyWizaStore,
    exc: Exception,
    page_number: int,
    search_after: list[Any] | None,
) -> bool:
    wait_seconds = _resolve_retry_wait_seconds(exc)
    if wait_seconds <= 0:
        return False
    checkpoint_page = max(page_number - 1, 0)
    checkpoint_sort = list(search_after or [])
    _save_checkpoint(output_dir, checkpoint_page, checkpoint_sort, "running")
    store.update_checkpoint("list", checkpoint_page, "running")
    _export_websites(output_dir, store)
    LOGGER.warning(
        "Wiza 列表命中%s，等待 %ds 后自动继续：page=%d error=%s",
        _describe_retryable_error(exc),
        wait_seconds,
        page_number,
        exc,
    )
    time.sleep(wait_seconds)
    return True


def _resolve_retry_wait_seconds(exc: Exception) -> int:
    if _looks_like_usage_limit_error(exc):
        return USAGE_LIMIT_WAIT_SECONDS
    if _looks_like_challenge_hold_error(exc):
        return CHALLENGE_WAIT_SECONDS
    if _looks_like_transient_p1_error(exc):
        return TRANSIENT_WAIT_SECONDS
    return 0


def _describe_retryable_error(exc: Exception) -> str:
    if _looks_like_usage_limit_error(exc):
        return "站点额度限制"
    if _looks_like_challenge_hold_error(exc):
        return "站点验证拦截"
    return "临时上游异常"


def _looks_like_usage_limit_error(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    message = str(exc or "").lower()
    return "usagelimit" in name or "usage limit" in message or "额度已用尽" in message


def _looks_like_transient_p1_error(exc: Exception) -> bool:
    name = exc.__class__.__name__.lower()
    message = str(exc or "").lower()
    if name in {"jsondecodeerror", "sslerror", "connectionerror", "toomanyredirects", "timeout", "timeouterror"}:
        return True
    transient_markers = (
        "expecting value: line 1 column 1",
        "connection timed out",
        "operation timed out",
        "empty reply from server",
        "maximum (30) redirects followed",
        "tls connect error",
        "invalid library",
        "curl: (28)",
        "curl: (35)",
        "curl: (47)",
        "curl: (52)",
    )
    return any(marker in message for marker in transient_markers)


def _looks_like_challenge_hold_error(exc: Exception) -> bool:
    message = str(exc or "").lower()
    markers = (
        "cloudflare",
        "captcha",
        "attention required",
        "cf-chl",
        "forbidden by robots",
    )
    return any(marker in message for marker in markers)
