"""dsnuri.com 爬虫入口 — 四阶段: 列表→详情→Google Maps→Snov。"""

from __future__ import annotations

import argparse
import html
import json
import logging
import random
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse

from curl_cffi import requests as cffi_requests
from lxml import html as lxml_html

from korea_crawler.dedup import deduplicate_by_domain
from korea_crawler.google_maps import GoogleMapsClient, GoogleMapsConfig
from korea_crawler.models import CompanyRecord
from korea_crawler.snov.pipeline import run_snov_pipeline

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent.parent.parent.parent

LIST_URL = "https://www.dsnuri.com/dsnuri/biz/usr/entryList.do"
DETAIL_URL = "https://www.dsnuri.com/dsnuri/biz/openBizInfo.do"
LIST_HEADERS = {
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "referer": LIST_URL,
}
DEFAULT_DETAIL_CONCURRENCY = 4
DEFAULT_GMAP_CONCURRENCY = 3
WRAP_TEXT_RE = re.compile(r"\s+")
CORP_ID_RE = re.compile(r"fn_openCorpInfo\('([^']+)'\)")
PAGE_INFO_RE = re.compile(r"페이지\s*<strong>(\d+)</strong>\s*/\s*(\d+)")
TOTAL_COUNT_RE = re.compile(r"총\s*게시물\s*<strong>(\d+)</strong>")
DOMAIN_HINT_RE = re.compile(r"^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$")
BLOCKED_GMAP_HOSTS = (
    'wikipedia.org',
    'dsnuri.com',
    'sejong.go.kr',
    'sjepa.or.kr',
    'coop.go.kr',
    'mss.go.kr',
    'kotra.or.kr',
    'blog.naver.com',
    'cafe.naver.com',
    'map.naver.com',
    'facebook.com',
    'instagram.com',
    'youtube.com',
)

_thread_local = threading.local()
_gmap_thread_local = threading.local()


@dataclass(slots=True)
class RateLimitConfig:
    min_delay: float = 0.15
    max_delay: float = 0.45
    long_rest_interval: int = 300
    long_rest_seconds: float = 5.0


class DsnuriClient:
    def __init__(self, rate_config: RateLimitConfig | None = None) -> None:
        self.rate_config = rate_config or RateLimitConfig()
        self._request_count = 0
        self.session = self._build_session()

    def _build_session(self) -> cffi_requests.Session:
        return cffi_requests.Session(impersonate="chrome110")

    def _reset_session(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        self.session = self._build_session()

    def _sleep(self) -> None:
        time.sleep(random.uniform(self.rate_config.min_delay, self.rate_config.max_delay))
        self._request_count += 1
        if self.rate_config.long_rest_interval > 0 and self._request_count % self.rate_config.long_rest_interval == 0:
            logger.info("DSNURI 已请求 %d 次，休息 %.0fs", self._request_count, self.rate_config.long_rest_seconds)
            time.sleep(self.rate_config.long_rest_seconds)

    def _request(self, url: str, method: str = "GET", data: dict[str, Any] | None = None, max_retries: int = 4) -> str:
        data = data or {}
        for attempt in range(1, max_retries + 1):
            self._sleep()
            try:
                if method == "POST":
                    resp = self.session.post(url, data=data, headers=LIST_HEADERS, timeout=30)
                else:
                    resp = self.session.get(url, headers=LIST_HEADERS, timeout=30)
            except Exception as exc:
                err_text = str(exc)
                logger.warning("DSNURI 请求异常 (第%d次): %s — %s", attempt, url, err_text)
                if re.search(r"curl: \((28|35|56)\)", err_text):
                    self._reset_session()
                if attempt == max_retries:
                    raise RuntimeError(f"请求失败，已重试 {max_retries} 次: {url}") from exc
                time.sleep(min((2 ** attempt) + random.uniform(0, 1.0), 20))
                continue
            if resp.status_code == 429:
                wait = 2 ** attempt * 5
                logger.warning("DSNURI 429 限流，等待 %ds (第%d次)", wait, attempt)
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                raise RuntimeError(f"DSNURI 403 Forbidden: {url}")
            if resp.status_code >= 500:
                if attempt == max_retries:
                    raise RuntimeError(f"DSNURI 服务端错误 {resp.status_code}: {url}")
                time.sleep(min((2 ** attempt) + random.uniform(0, 1.0), 20))
                continue
            resp.raise_for_status()
            return resp.text
        raise RuntimeError(f"请求失败，已重试 {max_retries} 次: {url}")

    def get_list_html(self, page: int = 1, corp_emd: str = "") -> str:
        return self._request(
            LIST_URL,
            method="POST",
            data={
                "page": str(page),
                "corpEmd": corp_emd,
                "targetNumber": "",
                "searchKrwd": "",
                "searchCnd": "all",
            },
        )

    def get_detail_html(self, corp_id: str) -> str:
        return self._request(f"{DETAIL_URL}?corpId={corp_id}")


def _get_client() -> DsnuriClient:
    if not hasattr(_thread_local, "client"):
        _thread_local.client = DsnuriClient()
    return _thread_local.client


def _get_gmap_client(search_pb: str, hl: str, gl: str) -> GoogleMapsClient:
    pb_template = search_pb.strip() if search_pb.strip() else GoogleMapsConfig().pb_template
    if not hasattr(_gmap_thread_local, "client"):
        _gmap_thread_local.client = GoogleMapsClient(
            GoogleMapsConfig(
                hl=hl,
                gl=gl,
                pb_template=pb_template,
                min_delay=0.4,
                max_delay=0.9,
                long_rest_interval=150,
                long_rest_seconds=5.0,
            )
        )
    return _gmap_thread_local.client


def _normalize_text(text: str) -> str:
    return WRAP_TEXT_RE.sub(" ", (text or "").strip())


def _extract_first_ceo(raw_ceo: str) -> str:
    if not raw_ceo:
        return ""
    return re.split(r"[/,·、]| 외 ", raw_ceo)[0].strip()


def _clean_homepage(raw_url: str) -> str:
    url = _normalize_text(html.unescape(raw_url or ""))
    if not url or url == "-":
        return ""
    url = re.sub(r"\s+", "", url)
    if url.startswith("mailto:"):
        return ""
    if url.startswith("//"):
        url = f"https:{url}"
    if url.startswith("www."):
        url = f"https://{url}"
    if not url.startswith(("http://", "https://")):
        if DOMAIN_HINT_RE.fullmatch(url):
            url = f"https://{url}"
        else:
            return ""
    return url


def _is_rejected_gmap_homepage(homepage: str) -> bool:
    if not homepage:
        return True
    try:
        host = urlparse(homepage).netloc.lower()
    except ValueError:
        return True
    if host.startswith('www.'):
        host = host[4:]
    if not host:
        return True
    return any(host == blocked or host.endswith('.' + blocked) for blocked in BLOCKED_GMAP_HOSTS)


def _extract_total_pages(tree: lxml_html.HtmlElement) -> int:
    page_links: list[int] = []
    for anchor in tree.xpath('//a[contains(@onclick, "fn_select_linkPage")]'):
        onclick = anchor.get('onclick', '')
        match = re.search(r'fn_select_linkPage\((\d+)\)', onclick)
        if not match:
            continue
        page_no = int(match.group(1))
        label = _normalize_text(anchor.text_content())
        if '마지막페이지' in label:
            return page_no
        page_links.append(page_no)
    page_info = _normalize_text(tree.text_content())
    match = re.search(r'페이지\s*\d+\s*/\s*(\d+)', page_info)
    if match:
        return int(match.group(1))
    return max(page_links) if page_links else 1


def _extract_total_count(tree: lxml_html.HtmlElement) -> int:
    page_info = _normalize_text(tree.text_content())
    match = re.search(r'총\s*게시물\s*(\d+)', page_info)
    return int(match.group(1)) if match else 0


def _parse_list_row(tr: lxml_html.HtmlElement) -> dict[str, str] | None:
    company_anchor = tr.xpath('.//a[contains(@onclick, "fn_openCorpInfo")][1]')
    if not company_anchor:
        return None
    onclick = company_anchor[0].get('onclick', '')
    match = CORP_ID_RE.search(onclick)
    if not match:
        return None
    cells = [td for td in tr.xpath('./td')]
    if len(cells) < 7:
        return None
    return {
        'comp_id': f"DS_{match.group(1)}",
        'corp_id': match.group(1),
        'corp_type': _normalize_text(cells[1].text_content()),
        'district': _normalize_text(cells[2].text_content()),
        'company_name': _normalize_text(company_anchor[0].text_content()),
        'ceo': _extract_first_ceo(_normalize_text(cells[4].text_content())),
        'certified_at': _normalize_text(cells[5].text_content()),
        'main_business': _normalize_text(cells[6].text_content()),
    }


def _parse_list_page(html_text: str) -> tuple[list[dict[str, str]], int, int]:
    try:
        tree = lxml_html.fromstring(html_text)
    except Exception:
        return [], 1, 0
    rows: list[dict[str, str]] = []
    for tr in tree.xpath('//table//tbody/tr'):
        row = _parse_list_row(tr)
        if row:
            rows.append(row)
    return rows, _extract_total_pages(tree), _extract_total_count(tree)


def _parse_detail_page(html_text: str) -> dict[str, str]:
    try:
        tree = lxml_html.fromstring(html_text)
    except Exception:
        return {'phone': '', 'address': '', 'business_type': '', 'item': ''}
    values: dict[str, str] = {}
    for li in tree.xpath('//article[@id="companyinfo"]//li'):
        label = _normalize_text(''.join(li.xpath('./strong/text()')))
        value = _normalize_text(''.join(li.xpath('./span/text()')))
        if label:
            values[label] = value
    return {
        'phone': values.get('대표번호', ''),
        'address': values.get('주소', ''),
        'business_type': values.get('업태', ''),
        'item': values.get('종목', ''),
    }


def _load_jsonl_records(filepath: Path) -> list[dict]:
    if not filepath.exists():
        return []
    rows: list[dict] = []
    with filepath.open('r', encoding='utf-8') as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except Exception:
                continue
            if isinstance(row, dict):
                rows.append(row)
    return rows


def _load_existing_ids(filepath: Path) -> set[str]:
    return {str(row.get('comp_id', '')).strip() for row in _load_jsonl_records(filepath) if str(row.get('comp_id', '')).strip()}


def _load_checkpoint_ids(filepath: Path) -> set[str]:
    if not filepath.exists():
        return set()
    try:
        payload = json.loads(filepath.read_text(encoding='utf-8'))
    except Exception:
        return set()
    return {str(x).strip() for x in payload.get('processed_ids', []) if str(x).strip()}


def _save_checkpoint_ids(filepath: Path, processed_ids: set[str]) -> None:
    filepath.write_text(json.dumps({'processed_ids': sorted(processed_ids)}, ensure_ascii=False), encoding='utf-8')


def _atomic_write_jsonl(filepath: Path, records: list[dict]) -> None:
    tmp_path = filepath.with_suffix(filepath.suffix + '.tmp')
    with tmp_path.open('w', encoding='utf-8') as fp:
        for row in records:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")
    tmp_path.replace(filepath)


def _merge_detail_rows(source_rows: list[dict], detail_rows: list[dict]) -> list[dict]:
    detail_map = {str(row.get('comp_id', '')): row for row in detail_rows if row.get('comp_id')}
    merged: list[dict] = []
    seen: set[str] = set()
    for src in source_rows:
        comp_id = str(src.get('comp_id', '')).strip()
        if not comp_id or comp_id in seen:
            continue
        seen.add(comp_id)
        base = dict(src)
        old = detail_map.get(comp_id, {})
        for key in ('phone', 'address', 'business_type', 'item'):
            if old.get(key):
                base[key] = old.get(key)
        merged.append(base)
    return merged


def _merge_gmap_rows(source_rows: list[dict], enriched_rows: list[dict]) -> list[dict]:
    enriched_map = {str(row.get('comp_id', '')): row for row in enriched_rows if row.get('comp_id')}
    merged: list[dict] = []
    seen: set[str] = set()
    for src in source_rows:
        comp_id = str(src.get('comp_id', '')).strip()
        if not comp_id or comp_id in seen:
            continue
        seen.add(comp_id)
        base = dict(src)
        old = enriched_map.get(comp_id, {})
        if old.get('homepage'):
            base['homepage'] = old.get('homepage')
        merged.append(base)
    return merged


def _build_gmap_queries(record: dict) -> list[str]:
    company_name = _normalize_text(str(record.get('company_name', '')))
    address = _normalize_text(str(record.get('address', '')))
    phone = _normalize_text(str(record.get('phone', '')))
    queries: list[str] = []
    for parts in [
        [company_name, address, phone],
        [company_name, address],
        [company_name, phone],
        [company_name],
    ]:
        query = _normalize_text(' '.join(x for x in parts if x))
        if query and query not in queries:
            queries.append(query)
    return queries


def _refresh_snov_state(output_dir: Path) -> int:
    input_file = output_dir / 'companies_enriched.jsonl'
    output_file = output_dir / 'companies_with_emails.jsonl'
    checkpoint_file = output_dir / 'checkpoint_snov.json'
    if not input_file.exists() or not output_file.exists() or not checkpoint_file.exists():
        return 0
    input_rows = {str(row.get('comp_id', '')): row for row in _load_jsonl_records(input_file) if row.get('comp_id')}
    output_rows = _load_jsonl_records(output_file)
    processed_ids = _load_checkpoint_ids(checkpoint_file)
    kept_rows: list[dict] = []
    stale = 0
    for row in output_rows:
        comp_id = str(row.get('comp_id', '')).strip()
        input_row = input_rows.get(comp_id, {})
        input_homepage = _normalize_text(str(input_row.get('homepage', '')))
        output_homepage = _normalize_text(str(row.get('homepage', '')))
        has_email = any(str(x).strip() for x in (row.get('emails') or []))
        if input_homepage and input_homepage != output_homepage and not has_email:
            processed_ids.discard(comp_id)
            stale += 1
            continue
        kept_rows.append(row)
    if stale > 0:
        _atomic_write_jsonl(output_file, kept_rows)
        _save_checkpoint_ids(checkpoint_file, processed_ids)
    return stale


def crawl_list(output_dir: Path, max_pages: int = 0) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / 'company_ids.jsonl'
    checkpoint_file = output_dir / 'checkpoint_list.json'
    client = _get_client()
    last_page = 0
    checkpoint_total_pages = 0
    if checkpoint_file.exists():
        try:
            payload = json.loads(checkpoint_file.read_text(encoding='utf-8'))
            last_page = int(payload.get('last_page', 0))
            checkpoint_total_pages = int(payload.get('total_pages', 0))
        except Exception:
            last_page = 0
            checkpoint_total_pages = 0
    current_page = last_page + 1
    total_pages = max_pages if max_pages > 0 else max(current_page, checkpoint_total_pages, 1)
    mode = 'a' if last_page > 0 and output_file.exists() else 'w'
    seen_ids = _load_existing_ids(output_file) if mode == 'a' else set()
    total_written = 0
    logger.info('DSNURI 列表爬虫: 从第 %d 页开始', current_page)
    try:
        with output_file.open(mode, encoding='utf-8') as fp:
            while current_page <= total_pages:
                html_text = client.get_list_html(page=current_page)
                rows, detected_pages, total_count = _parse_list_page(html_text)
                if max_pages <= 0:
                    total_pages = max(total_pages, detected_pages)
                if current_page == last_page + 1:
                    logger.info('DSNURI 列表总页数: %d | 总量: %d', total_pages, total_count)
                if not rows:
                    logger.info('第 %d 页无数据，列表阶段结束', current_page)
                    break
                page_written = 0
                for row in rows:
                    if row['comp_id'] in seen_ids:
                        continue
                    fp.write(json.dumps(row, ensure_ascii=False) + "\n")
                    seen_ids.add(row['comp_id'])
                    total_written += 1
                    page_written += 1
                fp.flush()
                checkpoint_file.write_text(json.dumps({'last_page': current_page, 'total_pages': total_pages}, ensure_ascii=False), encoding='utf-8')
                if current_page <= 3 or current_page % 10 == 0 or current_page == total_pages:
                    pct = current_page / total_pages * 100 if total_pages else 0
                    logger.info('第 %d/%d 页: 新增 %d / 解析 %d | 累计 %d | %.1f%%', current_page, total_pages, page_written, len(rows), total_written, pct)
                current_page += 1
                if max_pages > 0 and current_page > max_pages:
                    break
    except Exception:
        checkpoint_file.write_text(json.dumps({'last_page': max(0, current_page - 1), 'total_pages': total_pages}, ensure_ascii=False), encoding='utf-8')
        raise
    logger.info('DSNURI 列表完成: 新增 %d 条公司', total_written)
    return total_written


def crawl_details(output_dir: Path, max_items: int = 0, detail_concurrency: int = DEFAULT_DETAIL_CONCURRENCY) -> int:
    ids_file = output_dir / 'company_ids.jsonl'
    output_file = output_dir / 'companies.jsonl'
    checkpoint_file = output_dir / 'checkpoint_detail.json'
    source_rows = _load_jsonl_records(ids_file)
    if not source_rows:
        return 0
    merged_rows = _merge_detail_rows(source_rows, _load_jsonl_records(output_file))
    processed_ids = _load_checkpoint_ids(checkpoint_file)
    pending = [row for row in merged_rows if row.get('comp_id', '') not in processed_ids]
    if max_items > 0:
        pending = pending[:max_items]
    if not pending:
        if not output_file.exists():
            _atomic_write_jsonl(output_file, merged_rows)
        return 0
    logger.info('DSNURI 详情补齐: 待处理 %d 条, 并发=%d', len(pending), detail_concurrency)
    merged_map = {str(row.get('comp_id')): row for row in merged_rows}
    processed = 0
    failed = 0
    lock = threading.Lock()

    def _worker(raw_record: dict) -> tuple[str, dict[str, str]]:
        comp_id = str(raw_record.get('comp_id', ''))
        corp_id = str(raw_record.get('corp_id', '')).strip() or comp_id.replace('DS_', '', 1)
        details = _parse_detail_page(_get_client().get_detail_html(corp_id))
        return comp_id, details

    try:
        with ThreadPoolExecutor(max_workers=detail_concurrency) as executor:
            futures = {executor.submit(_worker, row): row for row in pending}
            for fut in as_completed(futures):
                original = futures[fut]
                comp_id = str(original.get('comp_id', ''))
                try:
                    result_comp_id, details = fut.result()
                    with lock:
                        row = merged_map.get(result_comp_id)
                        if row is not None:
                            row.update(details)
                        processed_ids.add(comp_id)
                        processed += 1
                        if processed <= 5 or processed % 20 == 0:
                            pct = processed / len(pending) * 100
                            logger.info('[DETAIL %d/%d] %.1f%% %s | TEL=%s | ADDR=%s', processed, len(pending), pct, original.get('company_name', ''), details.get('phone', '-') or '-', (details.get('address', '-') or '-')[:40])
                except Exception as exc:
                    failed += 1
                    processed_ids.add(comp_id)
                    logger.warning('DSNURI 详情失败 (%s): %s', comp_id, exc)
    finally:
        _save_checkpoint_ids(checkpoint_file, processed_ids)
        _atomic_write_jsonl(output_file, list(merged_map.values()))
    logger.info('DSNURI 详情完成: 处理 %d 条 | 失败 %d 条', processed, failed)
    return processed


def enrich_homepage_with_gmap(output_dir: Path, max_items: int = 0, gmap_concurrency: int = DEFAULT_GMAP_CONCURRENCY, gmap_search_pb: str = '', gmap_hl: str = 'ko', gmap_gl: str = 'kr') -> tuple[int, int]:
    source_file = output_dir / 'companies.jsonl'
    enriched_file = output_dir / 'companies_enriched.jsonl'
    checkpoint_file = output_dir / 'checkpoint_gmap.json'
    source_rows = _load_jsonl_records(source_file)
    if not source_rows:
        return 0, 0
    merged_rows = _merge_gmap_rows(source_rows, _load_jsonl_records(enriched_file))
    processed_ids = _load_checkpoint_ids(checkpoint_file)
    pending = [row for row in merged_rows if not row.get('homepage') and row.get('comp_id', '') not in processed_ids]
    if max_items > 0:
        pending = pending[:max_items]
    if not pending:
        if not enriched_file.exists():
            _atomic_write_jsonl(enriched_file, merged_rows)
        return 0, 0
    logger.info('DSNURI Google Maps 补官网: 待处理 %d 条, 并发=%d', len(pending), gmap_concurrency)
    merged_map = {str(row.get('comp_id')): row for row in merged_rows}
    processed = 0
    found = 0
    failed = 0
    lock = threading.Lock()

    def _worker(raw_record: dict) -> tuple[str, str]:
        queries = _build_gmap_queries(raw_record)
        homepage = ''
        client = _get_gmap_client(gmap_search_pb, gmap_hl, gmap_gl)
        for query in queries:
            candidate = _clean_homepage(client.search_official_website(query))
            if not candidate or _is_rejected_gmap_homepage(candidate):
                continue
            homepage = candidate
            break
        return str(raw_record.get('comp_id', '')), homepage

    try:
        with ThreadPoolExecutor(max_workers=gmap_concurrency) as executor:
            futures = {executor.submit(_worker, row): row for row in pending}
            for fut in as_completed(futures):
                original = futures[fut]
                comp_id = str(original.get('comp_id', ''))
                try:
                    result_comp_id, homepage = fut.result()
                    with lock:
                        row = merged_map.get(result_comp_id)
                        if row is not None and homepage:
                            row['homepage'] = homepage
                            found += 1
                        processed_ids.add(comp_id)
                        processed += 1
                        if processed <= 5 or processed % 20 == 0:
                            pct = processed / len(pending) * 100
                            logger.info('[GMAP %d/%d] %.1f%% %s | HP=%s', processed, len(pending), pct, original.get('company_name', ''), homepage[:60] if homepage else '-')
                except Exception as exc:
                    failed += 1
                    processed_ids.add(comp_id)
                    logger.warning('DSNURI Google Maps 查询失败 (%s): %s', comp_id, exc)
    finally:
        _save_checkpoint_ids(checkpoint_file, processed_ids)
        _atomic_write_jsonl(enriched_file, list(merged_map.values()))
    logger.info('DSNURI Google Maps 完成: 处理 %d 条 | 找到官网 %d 条 | 失败 %d 条', processed, found, failed)
    return processed, found


def run_dsnuri(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description='따사누리 기업현황 크롤링')
    parser.add_argument('--max-pages', type=int, default=0, help='列表最大页数')
    parser.add_argument('--max-items', type=int, default=0, help='详情/GMap/Snov 最大条数')
    parser.add_argument('--skip-list', action='store_true', help='跳过列表阶段')
    parser.add_argument('--skip-detail', action='store_true', help='跳过详情阶段')
    parser.add_argument('--skip-gmap', action='store_true', help='跳过 Google Maps 官网补齐阶段')
    parser.add_argument('--skip-snov', action='store_true', help='跳过 Snov 阶段')
    parser.add_argument('--detail-concurrency', type=int, default=DEFAULT_DETAIL_CONCURRENCY, help='详情阶段并发数')
    parser.add_argument('--gmap-concurrency', type=int, default=DEFAULT_GMAP_CONCURRENCY, help='Google Maps 阶段并发数')
    parser.add_argument('--gmap-hl', default='ko', help='Google Maps 语言参数 hl')
    parser.add_argument('--gmap-gl', default='kr', help='Google Maps 地区参数 gl')
    parser.add_argument('--gmap-search-pb', default='', help='Google Maps 搜索 pb 参数')
    parser.add_argument('--snov-concurrency', type=int, default=2, help='Snov 阶段并发数')
    parser.add_argument('--snov-delay', type=float, default=1.0, help='Snov 单条查询后等待秒数')
    parser.add_argument('--log-level', default='INFO', help='日志级别')
    args = parser.parse_args(argv)
    args.detail_concurrency = max(1, args.detail_concurrency)
    args.gmap_concurrency = max(1, args.gmap_concurrency)
    args.snov_concurrency = max(1, args.snov_concurrency)
    args.snov_delay = max(0.0, args.snov_delay)

    logging.basicConfig(level=getattr(logging, args.log_level.upper(), logging.INFO), format='%(asctime)s [%(levelname)s] %(name)s: %(message)s', datefmt='%H:%M:%S')

    output_dir = ROOT / 'output' / 'dsnuri'
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info('=== dsnuri.com 爬虫启动 ===')

    try:
        if not args.skip_list:
            crawl_list(output_dir=output_dir, max_pages=args.max_pages)
        if not args.skip_detail:
            crawl_details(output_dir=output_dir, max_items=args.max_items, detail_concurrency=args.detail_concurrency)
        if not args.skip_gmap:
            enrich_homepage_with_gmap(
                output_dir=output_dir,
                max_items=args.max_items,
                gmap_concurrency=args.gmap_concurrency,
                gmap_search_pb=args.gmap_search_pb,
                gmap_hl=args.gmap_hl,
                gmap_gl=args.gmap_gl,
            )
        if not args.skip_snov:
            stale_count = _refresh_snov_state(output_dir)
            if stale_count > 0:
                logger.info('DSNURI Snov 状态刷新: 回收 %d 条待重试记录', stale_count)
            run_snov_pipeline(
                output_dir=output_dir,
                max_items=args.max_items,
                concurrency=args.snov_concurrency,
                request_delay=args.snov_delay,
                input_filename='companies_enriched.jsonl',
                output_filename='companies_with_emails.jsonl',
                checkpoint_filename='checkpoint_snov.json',
            )
        final_file = output_dir / 'companies_with_emails.jsonl'
        if final_file.exists():
            logger.info('--- 域名去重 ---')
            deduped = deduplicate_by_domain(final_file)
            logger.info('去重完成: %d 条', deduped)
    except KeyboardInterrupt:
        logger.warning('用户中断，已保存断点。')
        return 130

    logger.info('=== dsnuri.com 爬虫完毕 ===')
    return 0
