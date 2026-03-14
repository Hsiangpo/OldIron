"""gpsc.or.kr 爬虫入口 — 三阶段: 列表→Google Maps→Snov。"""

from __future__ import annotations

import argparse
import json
import logging
import random
import re
import shutil
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from curl_cffi import requests as cffi_requests
from lxml import html as lxml_html

from korea_crawler.dedup import deduplicate_by_domain
from korea_crawler.google_maps import GoogleMapsClient, GoogleMapsConfig
from korea_crawler.models import CompanyRecord
from korea_crawler.snov.pipeline import run_snov_pipeline

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent.parent.parent.parent

BASE_HEADERS = {
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "referer": "https://gpsc.or.kr/company/",
}

HTML_HEADERS = {
    **BASE_HEADERS,
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

ROW_ID_PATTERN = re.compile(r"ninja_table_row_\d+\s+nt_row_id_(\d+)")
DOMAIN_HINT_PATTERN = re.compile(r"^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$")
DEFAULT_GMAP_CONCURRENCY = 3
BLOCKED_HOMEPAGE_HOST_HINTS = (
    'wikipedia.org',
    'wikidata.org',
    'namu.wiki',
    'ko.wikipedia.org',
)

_gmap_thread_local = threading.local()


@dataclass(slots=True)
class RateLimitConfig:
    min_delay: float = 0.2
    max_delay: float = 0.6
    long_rest_interval: int = 300
    long_rest_seconds: float = 5.0


class GpscClient:
    """gpsc.or.kr 列表页客户端。"""

    BASE_URL = "https://gpsc.or.kr"

    def __init__(self, rate_config: RateLimitConfig | None = None) -> None:
        self.rate_config = rate_config or RateLimitConfig()
        self._request_count = 0
        self.session = cffi_requests.Session(impersonate="chrome110")

    def _reset_session(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        self.session = cffi_requests.Session(impersonate="chrome110")

    def _sleep(self) -> None:
        time.sleep(random.uniform(self.rate_config.min_delay, self.rate_config.max_delay))
        self._request_count += 1
        if self.rate_config.long_rest_interval > 0 and self._request_count % self.rate_config.long_rest_interval == 0:
            logger.info("GPSC 已请求 %d 次，休息 %.0fs", self._request_count, self.rate_config.long_rest_seconds)
            time.sleep(self.rate_config.long_rest_seconds)

    def get_company_html(self, max_retries: int = 4) -> str:
        url = f"{self.BASE_URL}/company/"
        for attempt in range(1, max_retries + 1):
            self._sleep()
            try:
                resp = self.session.get(url, headers=HTML_HEADERS, timeout=30)
            except Exception as exc:
                err_text = str(exc)
                logger.warning("GPSC 请求异常 (第%d次): %s — %s", attempt, url, err_text)
                if re.search(r"curl: \((28|35|56)\)", err_text):
                    self._reset_session()
                if attempt == max_retries:
                    raise RuntimeError(f"请求失败，已重试 {max_retries} 次: {url}") from exc
                time.sleep(min((2 ** attempt) + random.uniform(0, 1.0), 20))
                continue
            if resp.status_code == 429:
                wait = 2 ** attempt * 5
                logger.warning("GPSC 429 限流，等待 %ds (第%d次)", wait, attempt)
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                raise RuntimeError(f"GPSC 403 Forbidden: {resp.url}")
            if resp.status_code >= 500:
                if attempt == max_retries:
                    raise RuntimeError(f"GPSC 服务端错误 {resp.status_code}: {resp.url}")
                time.sleep(min((2 ** attempt) + random.uniform(0, 1.0), 20))
                continue
            resp.raise_for_status()
            return resp.text
        raise RuntimeError(f"请求失败，已重试 {max_retries} 次: {url}")


def _normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", (text or "").strip())


def _extract_first_ceo(raw_ceo: str) -> str:
    if not raw_ceo:
        return ""
    parts = re.split(r"[/,·、]| 외 |ㆍ", raw_ceo)
    return parts[0].strip()


def _clean_homepage(raw_url: str) -> str:
    url = _normalize_text(raw_url)
    if not url:
        return ""
    url = re.sub(r"\s+", "", url)
    if url.startswith("mailto:"):
        return ""
    if url.startswith("//"):
        url = f"https:{url}"
    if url.startswith("www."):
        url = f"https://{url}"
    if not url.startswith(("http://", "https://")):
        if DOMAIN_HINT_PATTERN.fullmatch(url):
            url = f"https://{url}"
        else:
            return ""
    try:
        parsed = urlparse(url)
    except ValueError:
        return ""
    host = (parsed.netloc or parsed.path).strip().lower()
    if host.startswith("www."):
        host = host[4:]
    if not host:
        return ""
    if any(hint in host for hint in BLOCKED_HOMEPAGE_HOST_HINTS):
        return ""
    return url


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
            if isinstance(row, dict) and row.get('comp_id'):
                rows.append(row)
    return rows


def _atomic_write_jsonl(filepath: Path, records: list[dict]) -> None:
    tmp_path = filepath.with_suffix(filepath.suffix + '.tmp')
    with tmp_path.open('w', encoding='utf-8') as fp:
        for row in records:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")
    tmp_path.replace(filepath)


def _load_checkpoint_ids(filepath: Path) -> set[str]:
    if not filepath.exists():
        return set()
    try:
        payload = json.loads(filepath.read_text(encoding='utf-8'))
    except Exception:
        return set()
    return {str(x).strip() for x in payload.get('processed_ids', []) if str(x).strip()}


def _save_checkpoint_ids(filepath: Path, processed_ids: set[str]) -> None:
    filepath.write_text(
        json.dumps({'processed_ids': sorted(processed_ids)}, ensure_ascii=False),
        encoding='utf-8',
    )


def _parse_table_rows(html_text: str) -> list[dict[str, str]]:
    try:
        tree = lxml_html.fromstring(html_text)
    except Exception:
        return []
    rows: list[dict[str, str]] = []
    for tr in tree.xpath('//table[@id="footable_800"]//tbody/tr'):
        row_id = str(tr.get('data-row_id', '')).strip()
        if not row_id:
            classes = ' '.join(tr.get('class', '').split())
            match = ROW_ID_PATTERN.search(classes)
            row_id = match.group(1) if match else ''
        if not row_id:
            continue
        cols = [_normalize_text(td.text_content()) for td in tr.xpath('./td')]
        if len(cols) < 6:
            continue
        rows.append({
            'comp_id': f'GG_{row_id}',
            'region': cols[0],
            'company_name': cols[1],
            'phone': cols[2],
            'ceo': _extract_first_ceo(cols[3]),
            'industry': cols[4],
            'address': cols[5],
            'homepage': '',
        })
    return rows


def _merge_companies_for_gmap(source_rows: list[dict], enriched_rows: list[dict]) -> list[dict]:
    enriched_map = {str(row.get('comp_id', '')): row for row in enriched_rows if row.get('comp_id')}
    merged: list[dict] = []
    seen: set[str] = set()
    for src in source_rows:
        comp_id = str(src.get('comp_id', '')).strip()
        if not comp_id or comp_id in seen:
            continue
        seen.add(comp_id)
        out = dict(src)
        old = enriched_map.get(comp_id, {})
        if not out.get('homepage') and old.get('homepage'):
            out['homepage'] = old.get('homepage')
        merged.append(out)
    return merged


def _build_gmap_queries(row: dict) -> list[str]:
    company_name = str(row.get('company_name', '')).strip()
    address = str(row.get('address', '')).strip()
    phone = str(row.get('phone', '')).strip()
    queries = [
        company_name,
        f"{company_name} {address}".strip(),
        f"{company_name} {phone}".strip(),
    ]
    seen: set[str] = set()
    out: list[str] = []
    for query in queries:
        query = _normalize_text(query)
        if query and query not in seen:
            seen.add(query)
            out.append(query)
    return out


def _get_gmap_client(search_pb: str, hl: str, gl: str) -> GoogleMapsClient:
    pb_template = search_pb.strip() if search_pb.strip() else GoogleMapsConfig().pb_template
    if not hasattr(_gmap_thread_local, 'client'):
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


def crawl_list(output_dir: Path) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    html_text = GpscClient().get_company_html()
    rows = _parse_table_rows(html_text)
    output_file = output_dir / 'companies.jsonl'
    checkpoint_file = output_dir / 'checkpoint_list.json'

    payloads: list[dict] = []
    for row in rows:
        record = CompanyRecord(
            comp_id=row['comp_id'],
            company_name=row['company_name'],
            ceo=row['ceo'],
            homepage=row.get('homepage', ''),
        )
        payload = json.loads(record.to_json_line())
        payload.update({
            'region': row['region'],
            'phone': row['phone'],
            'industry': row['industry'],
            'address': row['address'],
        })
        payloads.append(payload)

    _atomic_write_jsonl(output_file, payloads)
    checkpoint_file.write_text(
        json.dumps({'completed': True, 'row_count': len(payloads)}, ensure_ascii=False),
        encoding='utf-8',
    )
    logger.info('GPSC 列表完成: %d 条公司', len(payloads))
    return len(payloads)


def enrich_homepage_with_gmap(
    output_dir: Path,
    max_items: int = 0,
    gmap_concurrency: int = DEFAULT_GMAP_CONCURRENCY,
    gmap_search_pb: str = '',
    gmap_hl: str = 'ko',
    gmap_gl: str = 'kr',
) -> tuple[int, int]:
    source_file = output_dir / 'companies.jsonl'
    enriched_file = output_dir / 'companies_enriched.jsonl'
    checkpoint_file = output_dir / 'checkpoint_gmap.json'

    source_rows = _load_jsonl_records(source_file)
    if not source_rows:
        return 0, 0
    merged_rows = _merge_companies_for_gmap(source_rows, _load_jsonl_records(enriched_file))
    processed_ids = _load_checkpoint_ids(checkpoint_file)
    pending = [r for r in merged_rows if not r.get('homepage') and r.get('comp_id', '') not in processed_ids]
    if max_items > 0:
        pending = pending[:max_items]

    if not pending:
        if not enriched_file.exists():
            _atomic_write_jsonl(enriched_file, merged_rows)
        return 0, sum(1 for row in merged_rows if str(row.get('homepage', '')).strip())

    logger.info('GPSC Google Maps 补官网: 待处理 %d 条, 并发=%d', len(pending), gmap_concurrency)

    merged_map = {str(row.get('comp_id', '')): row for row in merged_rows}
    processed = 0
    found = 0
    failed = 0
    lock = threading.Lock()

    def _worker(raw_record: dict) -> tuple[str, str]:
        comp_id = str(raw_record.get('comp_id', ''))
        homepage = ''
        client = _get_gmap_client(gmap_search_pb, gmap_hl, gmap_gl)
        for query in _build_gmap_queries(raw_record):
            homepage = _clean_homepage(client.search_official_website(query))
            if homepage:
                break
        return comp_id, homepage

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
                    logger.warning('GPSC Google Maps 查询失败 (%s): %s', comp_id, exc)
    finally:
        _save_checkpoint_ids(checkpoint_file, processed_ids)
        _atomic_write_jsonl(enriched_file, list(merged_map.values()))

    logger.info('GPSC Google Maps 完成: 处理 %d 条 | 找到官网 %d 条 | 失败 %d 条', processed, found, failed)
    return processed, found


def _refresh_snov_enriched_state(output_dir: Path) -> int:
    input_file = output_dir / 'companies_enriched.jsonl'
    output_file = output_dir / 'companies_with_emails_enriched.jsonl'
    checkpoint_file = output_dir / 'checkpoint_snov_enriched.json'
    if not input_file.exists():
        return 0

    source_rows = _load_jsonl_records(input_file)
    source_map = {str(row.get('comp_id', '')): row for row in source_rows if row.get('comp_id')}
    output_rows = _load_jsonl_records(output_file)
    output_map = {str(row.get('comp_id', '')): row for row in output_rows if row.get('comp_id')}

    stale_ids: set[str] = set()
    for comp_id, src in source_map.items():
        src_homepage = str(src.get('homepage', '')).strip()
        if not src_homepage:
            continue
        out = output_map.get(comp_id)
        if out is None or str(out.get('homepage', '')).strip() != src_homepage:
            stale_ids.add(comp_id)

    if not stale_ids:
        return 0

    kept_rows = [row for row in output_rows if str(row.get('comp_id', '')) not in stale_ids]
    _atomic_write_jsonl(output_file, kept_rows)
    processed_ids = {cid for cid in _load_checkpoint_ids(checkpoint_file) if cid not in stale_ids}
    _save_checkpoint_ids(checkpoint_file, processed_ids)
    return len(stale_ids)


def _sync_snov_output_if_needed(output_dir: Path, skip_gmap: bool) -> None:
    if skip_gmap:
        return
    src_file = output_dir / 'companies_with_emails_enriched.jsonl'
    dst_file = output_dir / 'companies_with_emails.jsonl'
    if src_file.exists():
        shutil.copyfile(src_file, dst_file)


def run_gpsc(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description='GPSC 자활기업 列表爬取')
    parser.add_argument('--max-items', type=int, default=0, help='GMap/Snov 最大条数')
    parser.add_argument('--skip-list', action='store_true', help='跳过列表阶段')
    parser.add_argument('--skip-detail', action='store_true', help='跳过详情阶段（GPSC 无详情阶段）')
    parser.add_argument('--skip-gmap', action='store_true', help='跳过 Google Maps 官网补齐阶段')
    parser.add_argument('--skip-snov', action='store_true', help='跳过 Snov 阶段')
    parser.add_argument('--gmap-concurrency', type=int, default=DEFAULT_GMAP_CONCURRENCY, help='Google Maps 阶段并发数')
    parser.add_argument('--gmap-hl', default='ko', help='Google Maps 语言参数 hl')
    parser.add_argument('--gmap-gl', default='kr', help='Google Maps 地区参数 gl')
    parser.add_argument('--gmap-search-pb', default='', help='Google Maps 搜索 pb 参数')
    parser.add_argument('--snov-concurrency', type=int, default=2, help='Snov 阶段并发数')
    parser.add_argument('--snov-delay', type=float, default=1.0, help='Snov 单条查询后等待秒数')
    parser.add_argument('--serial', action='store_true', help='保留参数，GPSC 实际按串行执行')
    parser.add_argument('--log-level', default='INFO', help='日志级别')
    args = parser.parse_args(argv)
    args.gmap_concurrency = max(1, args.gmap_concurrency)
    args.snov_concurrency = max(1, args.snov_concurrency)
    args.snov_delay = max(0.0, args.snov_delay)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%H:%M:%S',
    )

    output_dir = ROOT / 'output' / 'gpsc'
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info('=== gpsc.or.kr 爬虫启动 ===')

    if args.skip_detail:
        logger.info('GPSC 无详情阶段，忽略 --skip-detail')

    try:
        if not args.skip_list:
            crawl_list(output_dir=output_dir)
        if not args.skip_gmap:
            enrich_homepage_with_gmap(
                output_dir=output_dir,
                max_items=args.max_items,
                gmap_concurrency=args.gmap_concurrency,
                gmap_search_pb=args.gmap_search_pb,
                gmap_hl=args.gmap_hl,
                gmap_gl=args.gmap_gl,
            )
            stale_count = _refresh_snov_enriched_state(output_dir)
            if stale_count > 0:
                logger.info('GPSC Snov 状态刷新: %d 条官网更新待重查', stale_count)
        if not args.skip_snov:
            input_filename = 'companies.jsonl' if args.skip_gmap else 'companies_enriched.jsonl'
            output_filename = 'companies_with_emails.jsonl' if args.skip_gmap else 'companies_with_emails_enriched.jsonl'
            checkpoint_filename = 'checkpoint_snov.json' if args.skip_gmap else 'checkpoint_snov_enriched.json'
            run_snov_pipeline(
                output_dir=output_dir,
                max_items=args.max_items,
                concurrency=args.snov_concurrency,
                request_delay=args.snov_delay,
                input_filename=input_filename,
                output_filename=output_filename,
                checkpoint_filename=checkpoint_filename,
            )
            _sync_snov_output_if_needed(output_dir, args.skip_gmap)
        final_input = output_dir / ('companies_with_emails.jsonl' if (output_dir / 'companies_with_emails.jsonl').exists() else 'companies_with_emails_enriched.jsonl')
        if final_input.exists():
            logger.info('--- 域名去重 ---')
            deduped = deduplicate_by_domain(final_input)
            logger.info('去重完成: %d 条', deduped)
    except KeyboardInterrupt:
        logger.warning('用户中断，已保存断点。')
        return 130

    logger.info('=== gpsc.or.kr 爬虫完毕 ===')
    return 0
