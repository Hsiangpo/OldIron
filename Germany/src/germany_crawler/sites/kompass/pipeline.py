"""Kompass Germany Pipeline 1。"""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import html
import json
import logging
import re
import threading
import time
from pathlib import Path

from .client import KompassClient
from .client import build_seed_page_url
from .store import GermanyKompassStore


LOGGER = logging.getLogger("germany.kompass.pipeline")
CHECKPOINT_NAME = "list_checkpoint.json"
REGION_LINK_RE = re.compile(r'href=(["\'])(?P<href>/z/de/r/[^"\']+)\1', re.I)
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
    """抓取 Kompass Germany 多地区入口，仅保留公司名与官网。"""
    output_dir.mkdir(parents=True, exist_ok=True)
    store = GermanyKompassStore(output_dir / "companies.db")
    checkpoint = _load_checkpoint(output_dir)
    if checkpoint.get("status") == "done" and max_pages <= 0 and checkpoint.get("seeds"):
        _export_websites(output_dir, store)
        return {"pages": 0, "new_companies": 0, "total_companies": store.get_company_count()}
    state = _load_or_build_state(checkpoint, output_dir, proxy)
    worker_count = _resolve_worker_count(concurrency, max_pages)
    if worker_count <= 1:
        processed_pages, new_companies, finished = _run_serial(
            output_dir=output_dir,
            store=store,
            state=state,
            request_delay=request_delay,
            proxy=proxy,
            max_pages=max_pages,
        )
    else:
        processed_pages, new_companies, finished = _run_concurrent(
            output_dir=output_dir,
            store=store,
            state=state,
            request_delay=request_delay,
            proxy=proxy,
            concurrency=worker_count,
        )
    _finalize_state(output_dir, store, state, processed_pages, finished)
    _export_websites(output_dir, store)
    return {
        "pages": processed_pages,
        "new_companies": new_companies,
        "total_companies": store.get_company_count(),
    }


def _load_or_build_state(checkpoint: dict[str, object], output_dir: Path, proxy: str) -> dict[str, object]:
    if checkpoint.get("seeds"):
        return checkpoint
    client = KompassClient(output_dir, proxy)
    try:
        return _build_initial_checkpoint(client, output_dir)
    finally:
        client.close()


def _resolve_worker_count(concurrency: int, max_pages: int) -> int:
    if max_pages > 0:
        return 1
    return max(int(concurrency or 1), 1)


def _run_serial(
    *,
    output_dir: Path,
    store: GermanyKompassStore,
    state: dict[str, object],
    request_delay: float,
    proxy: str,
    max_pages: int,
) -> tuple[int, int, bool]:
    processed_pages = 0
    new_companies = 0
    client = KompassClient(output_dir, proxy)
    try:
        while True:
            seed = _next_pending_seed(state)
            if seed is None:
                return processed_pages, new_companies, True
            page_result = _fetch_seed_page(client, seed)
            if page_result is None:
                _mark_seed_done(output_dir, state, seed)
                continue
            page_number, companies = page_result
            inserted = store.upsert_companies(companies)
            if inserted <= 0:
                LOGGER.warning("Kompass 种子 %s 第 %d 页未新增任何公司，疑似分页回卷，停止该种子。", seed.get("url"), page_number)
                _mark_seed_done(output_dir, state, seed)
                continue
            processed_pages += 1
            new_companies += inserted
            _advance_seed(output_dir, state, seed, page_number)
            store.update_checkpoint("list", processed_pages, "running")
            LOGGER.info("Kompass 种子 %s 第 %d 页：解析 %d 家，新增 %d 家", seed.get("label"), page_number, len(companies), inserted)
            if max_pages > 0 and processed_pages >= max_pages:
                return processed_pages, new_companies, False
            time.sleep(max(request_delay, 0.0))
    finally:
        client.close()


def _run_concurrent(
    *,
    output_dir: Path,
    store: GermanyKompassStore,
    state: dict[str, object],
    request_delay: float,
    proxy: str,
    concurrency: int,
) -> tuple[int, int, bool]:
    progress = {"pages": 0, "new_companies": 0}
    state_lock = threading.Lock()
    progress_lock = threading.Lock()
    stop_event = threading.Event()

    def worker() -> None:
        client = KompassClient(output_dir, proxy)
        try:
            while not stop_event.is_set():
                with state_lock:
                    seed = _claim_pending_seed(state)
                if seed is None:
                    return
                try:
                    _process_seed_until_done(
                        client=client,
                        output_dir=output_dir,
                        store=store,
                        state=state,
                        seed=seed,
                        request_delay=request_delay,
                        progress=progress,
                        state_lock=state_lock,
                        progress_lock=progress_lock,
                        stop_event=stop_event,
                    )
                except Exception:
                    with state_lock:
                        seed["status"] = "pending"
                        _save_checkpoint(output_dir, state)
                    stop_event.set()
                    raise
        finally:
            client.close()

    with ThreadPoolExecutor(max_workers=concurrency) as executor:
        futures = [executor.submit(worker) for _ in range(concurrency)]
        for future in futures:
            future.result()
    return int(progress["pages"]), int(progress["new_companies"]), True


def _process_seed_until_done(
    *,
    client: KompassClient,
    output_dir: Path,
    store: GermanyKompassStore,
    state: dict[str, object],
    seed: dict[str, object],
    request_delay: float,
    progress: dict[str, int],
    state_lock: threading.Lock,
    progress_lock: threading.Lock,
    stop_event: threading.Event,
) -> None:
    while not stop_event.is_set():
        page_result = _fetch_seed_page(client, seed)
        if page_result is None:
            with state_lock:
                _mark_seed_done(output_dir, state, seed)
            return
        page_number, companies = page_result
        inserted = store.upsert_companies(companies)
        if inserted <= 0:
            LOGGER.warning("Kompass 种子 %s 第 %d 页未新增任何公司，疑似分页回卷，停止该种子。", seed.get("url"), page_number)
            with state_lock:
                _mark_seed_done(output_dir, state, seed)
            return
        with progress_lock:
            progress["pages"] += 1
            progress["new_companies"] += inserted
            total_pages = int(progress["pages"])
        with state_lock:
            _advance_seed(output_dir, state, seed, page_number)
        store.update_checkpoint("list", total_pages, "running")
        LOGGER.info("Kompass 种子 %s 第 %d 页：解析 %d 家，新增 %d 家", seed.get("label"), page_number, len(companies), inserted)
        time.sleep(max(request_delay, 0.0))


def _fetch_seed_page(client: KompassClient, seed: dict[str, object]) -> tuple[int, list[dict[str, str]]] | None:
    page_number = int(seed.get("page") or 0) + 1
    page_url = build_seed_page_url(str(seed.get("url") or ""), page_number)
    previous_url = build_seed_page_url(str(seed.get("url") or ""), max(page_number - 1, 1))
    page_html = client.fetch_page(page_url, referer=previous_url)
    companies = parse_companies_from_html(page_html)
    if not companies:
        return None
    return page_number, companies


def _claim_pending_seed(state: dict[str, object]) -> dict[str, object] | None:
    for seed in list(state.get("seeds") or []):
        status = str(seed.get("status") or "pending")
        if status in {"done", "working"}:
            continue
        seed["status"] = "working"
        return seed
    return None


def _advance_seed(output_dir: Path, state: dict[str, object], seed: dict[str, object], page_number: int) -> None:
    seed["page"] = page_number
    seed["status"] = "working" if str(seed.get("status") or "") == "working" else "pending"
    _save_checkpoint(output_dir, state)


def _mark_seed_done(output_dir: Path, state: dict[str, object], seed: dict[str, object]) -> None:
    seed["status"] = "done"
    _save_checkpoint(output_dir, state)


def _finalize_state(
    output_dir: Path,
    store: GermanyKompassStore,
    state: dict[str, object],
    processed_pages: int,
    finished: bool,
) -> None:
    state["status"] = "done" if finished else "running"
    _save_checkpoint(output_dir, state)
    store.update_checkpoint("list", processed_pages, "done" if finished else "running")


def extract_seed_urls(page_html: str, *, country_code: str) -> list[str]:
    """从页面里抽取地区种子 URL。"""
    results: list[str] = []
    seen: set[str] = set()
    for matched in REGION_LINK_RE.finditer(str(page_html or "")):
        href = str(matched.group("href") or "").strip()
        absolute = f"https://us.kompass.com{href}" if href.startswith("/") else href
        if f"/z/{country_code}/" not in absolute:
            continue
        normalized = absolute if absolute.endswith("/") else f"{absolute}/"
        if normalized in seen:
            continue
        seen.add(normalized)
        results.append(normalized)
    return results


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


def _load_checkpoint(output_dir: Path) -> dict[str, object]:
    checkpoint_path = output_dir / CHECKPOINT_NAME
    if not checkpoint_path.exists():
        return {}
    try:
        payload = json.loads(checkpoint_path.read_text(encoding="utf-8"))
    except Exception:
        LOGGER.warning("Kompass checkpoint 解析失败：%s", checkpoint_path)
        return {}
    return payload if isinstance(payload, dict) else {}


def _save_checkpoint(output_dir: Path, payload: dict[str, object]) -> None:
    (output_dir / CHECKPOINT_NAME).write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _export_websites(output_dir: Path, store: GermanyKompassStore) -> None:
    (output_dir / "websites.txt").write_text("\n".join(store.export_websites()), encoding="utf-8")


def _build_initial_checkpoint(client: KompassClient, output_dir: Path) -> dict[str, object]:
    root_url = "https://us.kompass.com/businessplace/z/de/"
    root_html = client.fetch_page(root_url, referer=root_url)
    seeds = [{"url": url, "label": _seed_label(url), "page": 0, "status": "pending"} for url in extract_seed_urls(root_html, country_code="de")]
    state = {"status": "running", "seeds": _dedupe_seed_entries(seeds)}
    _save_checkpoint(output_dir, state)
    return state


def _dedupe_seed_entries(seeds: list[dict[str, object]]) -> list[dict[str, object]]:
    results: list[dict[str, object]] = []
    seen: set[str] = set()
    for seed in seeds:
        url = str(seed.get("url") or "").strip()
        if not url or url in seen:
            continue
        seen.add(url)
        results.append(seed)
    return results


def _next_pending_seed(state: dict[str, object]) -> dict[str, object] | None:
    for seed in list(state.get("seeds") or []):
        if str(seed.get("status") or "pending") not in {"done", "working"}:
            return seed
    return None


def _seed_label(seed_url: str) -> str:
    parts = [part for part in str(seed_url or "").rstrip("/").split("/") if part]
    return parts[-2] if len(parts) >= 2 and parts[-1].startswith("de_") else parts[-1] if parts else "seed"
