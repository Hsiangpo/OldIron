"""Kompass Germany Pipeline 1。"""

from __future__ import annotations

import html
import json
import logging
import re
import time
from pathlib import Path

from .client import KompassClient
from .store import GermanyKompassStore


LOGGER = logging.getLogger("germany.kompass.pipeline")
CHECKPOINT_NAME = "list_checkpoint.json"
COMPANY_LINK_RE = re.compile(
    r"<a\b[^>]*href=(['\"])(?P<href>/c/[^\"']+)\1[^>]*>(?P<label>.*?)</a>",
    re.I | re.S,
)
EXTERNAL_LINK_RE = re.compile(
    r"<a\b[^>]*href=(['\"])(?P<href>https?://[^\"']+)\1[^>]*>(?P<label>.*?)</a>",
    re.I | re.S,
)
RAW_URL_RE = re.compile(r"https?://[^\s<>'\"]+", re.I)
BAD_WEBSITE_HOSTS = {
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
    "youtube.com",
    "www.youtube.com",
    "kompass.com",
    "us.kompass.com",
    "mise-en-relation.svaplus.fr",
    "geo.captcha-delivery.com",
    "ct.captcha-delivery.com",
}


def run_pipeline_list(
    *,
    output_dir: Path,
    request_delay: float = 1.0,
    proxy: str = "",
    max_pages: int = 0,
    concurrency: int = 1,
) -> dict[str, int]:
    """抓取 Kompass Germany 列表页，仅保留公司名与官网。"""
    del concurrency
    output_dir.mkdir(parents=True, exist_ok=True)
    store = GermanyKompassStore(output_dir / "companies.db")
    checkpoint = _load_checkpoint(output_dir)
    if checkpoint.get("status") == "done" and max_pages <= 0:
        _export_websites(output_dir, store)
        return {"pages": 0, "new_companies": 0, "total_companies": store.get_company_count()}
    client = KompassClient(output_dir, proxy)
    page_number = int(checkpoint.get("page") or 0) + 1
    processed_pages = 0
    new_companies = 0
    try:
        while True:
            page_html = client.fetch_list_page(page_number)
            companies = parse_companies_from_html(page_html)
            if not companies:
                _save_checkpoint(output_dir, page_number - 1, "done")
                store.update_checkpoint("list", page_number - 1, "done")
                break
            total_before = store.get_company_count()
            store.upsert_companies(companies)
            inserted = store.get_company_count() - total_before
            if inserted <= 0:
                LOGGER.warning("Kompass 页 %d 未新增任何公司，疑似分页回卷，停止续跑。", page_number)
                _save_checkpoint(output_dir, page_number - 1, "done")
                store.update_checkpoint("list", page_number - 1, "done")
                break
            new_companies += inserted
            processed_pages += 1
            _save_checkpoint(output_dir, page_number, "running")
            store.update_checkpoint("list", page_number, "running")
            LOGGER.info("Kompass 页 %d：解析 %d 家", page_number, len(companies))
            if max_pages > 0 and processed_pages >= max_pages:
                break
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


def parse_companies_from_html(page_html: str) -> list[dict[str, str]]:
    """从 Kompass 列表页 HTML 提取公司名与官网。"""
    html_text = str(page_html or "")
    company_matches = [
        matched
        for matched in COMPANY_LINK_RE.finditer(html_text)
        if not str(matched.group("href") or "").strip().lower().startswith("/c/p/")
        and not _clean_text(matched.group("label")).lower().startswith("see the ")
    ]
    results: list[dict[str, str]] = []
    seen_keys: set[str] = set()
    for index, matched in enumerate(company_matches):
        company_name = _clean_text(matched.group("label"))
        if not company_name:
            continue
        block_start = matched.end()
        next_start = company_matches[index + 1].start() if index + 1 < len(company_matches) else len(html_text)
        block_html = html_text[block_start:next_start]
        website = _extract_website_from_block(block_html)
        if not website:
            continue
        company_key = "".join(ch.lower() for ch in company_name if ch.isalnum())
        if company_key in seen_keys:
            continue
        seen_keys.add(company_key)
        results.append({"company_name": company_name, "website": website})
    return results


def _extract_website_from_block(block_html: str) -> str:
    for matched in EXTERNAL_LINK_RE.finditer(block_html):
        website = _normalize_website_url(matched.group("href"))
        if website:
            return website
    for matched in RAW_URL_RE.finditer(_clean_text(block_html)):
        website = _normalize_website_url(matched.group(0))
        if website:
            return website
    return ""


def _normalize_website_url(value: str) -> str:
    text = html.unescape(str(value or "")).strip(" \t\r\n,;|<>[](){}'\"")
    if not text:
        return ""
    if "://" not in text and re.fullmatch(r"[a-z0-9][a-z0-9.-]+\.[a-z]{2,24}(/[^\s]*)?", text, flags=re.I):
        text = f"https://{text}"
    matched = RAW_URL_RE.search(text)
    if matched is not None:
        text = matched.group(0)
    text = text.rstrip(".,;:)")
    parsed = re.match(r"^(https?)://([^/]+)(?P<rest>/?.*)$", text, flags=re.I)
    if parsed is None:
        return ""
    host = str(parsed.group(2) or "").strip().lower()
    if not host or host in BAD_WEBSITE_HOSTS or host.endswith(".kompass.com"):
        return ""
    if "." not in host or "+" in host:
        return ""
    suffix = host.rsplit(".", 1)[-1]
    if not re.fullmatch(r"[a-z]{2,24}", suffix):
        return ""
    return f"{parsed.group(1).lower()}://{host}{parsed.group('rest') or ''}"


def _clean_text(value: str) -> str:
    text = re.sub(r"<[^>]+>", " ", str(value or ""))
    text = html.unescape(text)
    return re.sub(r"\s+", " ", text).strip()


def _load_checkpoint(output_dir: Path) -> dict[str, int | str]:
    checkpoint_path = output_dir / CHECKPOINT_NAME
    if not checkpoint_path.exists():
        return {}
    try:
        payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except Exception:
        LOGGER.warning("Kompass checkpoint 解析失败：%s", checkpoint_path)
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_checkpoint(output_dir: Path, page: int, status: str) -> None:
    payload = {"page": int(page), "status": str(status or "running")}
    (output_dir / CHECKPOINT_NAME).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _export_websites(output_dir: Path, store: GermanyKompassStore) -> None:
    (output_dir / "websites.txt").write_text("\n".join(store.export_websites()), encoding="utf-8")
