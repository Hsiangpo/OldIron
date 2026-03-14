"""kssba.or.kr 爬虫入口 — 两阶段: 列表→Snov。"""

from __future__ import annotations

import argparse
import html
import json
import logging
import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from curl_cffi import requests as cffi_requests
from lxml import html as lxml_html

from korea_crawler.dedup import deduplicate_by_domain
from korea_crawler.models import CompanyRecord
from korea_crawler.snov.pipeline import run_snov_pipeline

logger = logging.getLogger(__name__)
ROOT = Path(__file__).resolve().parent.parent.parent.parent

BASE_HEADERS = {
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "referer": "https://www.kssba.or.kr/bbs/board.php?bo_table=21&page=1",
}

HTML_HEADERS = {
    **BASE_HEADERS,
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

WR_ID_PATTERN = re.compile(r"wr_id=(\d+)")
PAGE_PATTERN = re.compile(r"page=(\d+)")
DOMAIN_HINT_PATTERN = re.compile(r"^[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}(/.*)?$")


@dataclass(slots=True)
class RateLimitConfig:
    min_delay: float = 0.2
    max_delay: float = 0.6
    long_rest_interval: int = 300
    long_rest_seconds: float = 5.0


class KssbaClient:
    """kssba.or.kr 列表页客户端。"""

    BASE_URL = "https://www.kssba.or.kr"

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
            logger.info("KSSBA 已请求 %d 次，休息 %.0fs", self._request_count, self.rate_config.long_rest_seconds)
            time.sleep(self.rate_config.long_rest_seconds)

    def get_list_html(self, page: int, max_retries: int = 4) -> str:
        url = f"{self.BASE_URL}/bbs/board.php"
        params: dict[str, Any] = {"bo_table": "21", "page": str(page)}
        for attempt in range(1, max_retries + 1):
            self._sleep()
            try:
                resp = self.session.get(url, params=params, headers=HTML_HEADERS, timeout=30)
            except Exception as exc:
                err_text = str(exc)
                logger.warning("KSSBA 请求异常 (第%d次): %s — %s", attempt, url, err_text)
                if re.search(r"curl: \((28|35|56)\)", err_text):
                    self._reset_session()
                if attempt == max_retries:
                    raise RuntimeError(f"请求失败，已重试 {max_retries} 次: {url}") from exc
                time.sleep(min((2 ** attempt) + random.uniform(0, 1.0), 20))
                continue
            if resp.status_code == 429:
                wait = 2 ** attempt * 5
                logger.warning("KSSBA 429 限流，等待 %ds (第%d次)", wait, attempt)
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                raise RuntimeError(f"KSSBA 403 Forbidden: {resp.url}")
            if resp.status_code >= 500:
                if attempt == max_retries:
                    raise RuntimeError(f"KSSBA 服务端错误 {resp.status_code}: {resp.url}")
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
    parts = re.split(r"[/,·、]| 외 ", raw_ceo)
    return parts[0].strip()


def _clean_homepage(raw_url: str) -> str:
    url = html.unescape(raw_url or "").strip()
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
    return url


def _extract_wr_id(raw_href: str) -> str:
    match = WR_ID_PATTERN.search(raw_href or "")
    return match.group(1) if match else ""


def _extract_total_pages(tree: lxml_html.HtmlElement) -> int:
    page_values: list[int] = []
    for href in tree.xpath('//a[contains(@href, "bo_table=21")]/@href'):
        match = PAGE_PATTERN.search(href)
        if match:
            page_values.append(int(match.group(1)))
    return max(page_values) if page_values else 1


def _extract_item_value(items: list[str], label: str) -> str:
    prefix = f"{label} :"
    for item in items:
        if item.startswith(prefix):
            return item.replace(prefix, "", 1).strip()
    return ""


def _parse_list_page(html_text: str) -> tuple[list[dict[str, str]], int]:
    try:
        tree = lxml_html.fromstring(html_text)
    except Exception:
        return [], 1
    rows: list[dict[str, str]] = []
    for anchor in tree.xpath('//ul[contains(@class,"board_gallery")]/li/a[contains(@href,"wr_id=") and contains(@class,"photo")]'):
        wr_id = _extract_wr_id(anchor.get("href", ""))
        if not wr_id:
            continue
        info_node = anchor.getnext()
        if info_node is None or info_node.tag.lower() != "ul":
            candidates = anchor.xpath('following-sibling::ul[contains(@class,"gellery_info")][1]')
            info_node = candidates[0] if candidates else None
        if info_node is None:
            continue
        items = [_normalize_text(li.text_content()) for li in info_node.xpath('./li') if _normalize_text(li.text_content())]
        homepage_href = info_node.xpath('.//a[contains(., "웹사이트")][1]/@href')
        homepage_raw = homepage_href[0] if homepage_href else _extract_item_value(items, "웹사이트")
        rows.append({
            "comp_id": f"KS_{wr_id}",
            "company_name": _extract_item_value(items, "회사명"),
            "ceo": _extract_first_ceo(_extract_item_value(items, "대표자명")),
            "homepage": _clean_homepage(homepage_raw),
        })
    return rows, _extract_total_pages(tree)


def _load_existing_ids(filepath: Path) -> set[str]:
    if not filepath.exists():
        return set()
    existing: set[str] = set()
    with filepath.open('r', encoding='utf-8') as fp:
        for line in fp:
            line = line.strip()
            if not line:
                continue
            try:
                comp_id = json.loads(line).get('comp_id', '')
            except Exception:
                continue
            if comp_id:
                existing.add(comp_id)
    return existing


def crawl_list(output_dir: Path, max_pages: int = 0) -> int:
    output_dir.mkdir(parents=True, exist_ok=True)
    output_file = output_dir / 'companies.jsonl'
    checkpoint_file = output_dir / 'checkpoint_list.json'
    client = KssbaClient()

    last_page = 0
    checkpoint_total_pages = 0
    if checkpoint_file.exists():
        try:
            checkpoint_data = json.loads(checkpoint_file.read_text(encoding='utf-8'))
            last_page = int(checkpoint_data.get('last_page', 0))
            checkpoint_total_pages = int(checkpoint_data.get('total_pages', 0))
        except Exception:
            last_page = 0
            checkpoint_total_pages = 0

    current_page = last_page + 1
    mode = 'a' if last_page > 0 and output_file.exists() else 'w'
    seen_ids = _load_existing_ids(output_file) if mode == 'a' else set()
    total_written = 0
    total_pages = max_pages if max_pages > 0 else max(current_page, checkpoint_total_pages, 1)

    logger.info('KSSBA 列表爬虫: 从第 %d 页开始', current_page)

    try:
        with output_file.open(mode, encoding='utf-8') as fp:
            while current_page <= total_pages:
                html_text = client.get_list_html(current_page)
                rows, detected_pages = _parse_list_page(html_text)
                if current_page == last_page + 1 and max_pages <= 0:
                    total_pages = detected_pages
                    logger.info('KSSBA 列表总页数: %d', total_pages)
                if not rows:
                    logger.info('第 %d 页无数据，列表阶段结束', current_page)
                    break
                written_this_page = 0
                for row in rows:
                    if row['comp_id'] in seen_ids:
                        continue
                    record = CompanyRecord(
                        comp_id=row['comp_id'],
                        company_name=row['company_name'],
                        ceo=row['ceo'],
                        homepage=row['homepage'],
                    )
                    fp.write(record.to_json_line() + "\n")
                    seen_ids.add(record.comp_id)
                    total_written += 1
                    written_this_page += 1
                fp.flush()
                checkpoint_file.write_text(
                    json.dumps({'last_page': current_page, 'total_pages': total_pages}, ensure_ascii=False),
                    encoding='utf-8',
                )
                if current_page <= 3 or current_page % 10 == 0 or current_page == total_pages:
                    pct = current_page / total_pages * 100 if total_pages else 0
                    logger.info('第 %d/%d 页: 新增 %d / 解析 %d | 累计 %d | %.1f%%', current_page, total_pages, written_this_page, len(rows), total_written, pct)
                current_page += 1
                if max_pages > 0 and current_page > max_pages:
                    break
    except Exception:
        checkpoint_file.write_text(
            json.dumps({'last_page': max(0, current_page - 1), 'total_pages': total_pages}, ensure_ascii=False),
            encoding='utf-8',
        )
        raise

    logger.info('KSSBA 列表完成: 新增 %d 条公司', total_written)
    return total_written


def run_kssba(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description='KSSBA 회원사 소개 列表爬取')
    parser.add_argument('--max-pages', type=int, default=0, help='列表最大页数')
    parser.add_argument('--max-items', type=int, default=0, help='Snov 最大条数')
    parser.add_argument('--skip-list', action='store_true', help='跳过列表阶段')
    parser.add_argument('--skip-detail', action='store_true', help='跳过详情阶段（KSSBA 无详情阶段）')
    parser.add_argument('--skip-snov', action='store_true', help='跳过 Snov 阶段')
    parser.add_argument('--snov-concurrency', type=int, default=2, help='Snov 阶段并发数')
    parser.add_argument('--snov-delay', type=float, default=1.0, help='Snov 单条查询后等待秒数')
    parser.add_argument('--serial', action='store_true', help='保留参数，KSSBA 实际按串行执行')
    parser.add_argument('--log-level', default='INFO', help='日志级别')
    args = parser.parse_args(argv)
    args.snov_concurrency = max(1, args.snov_concurrency)
    args.snov_delay = max(0.0, args.snov_delay)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
        datefmt='%H:%M:%S',
    )

    output_dir = ROOT / 'output' / 'kssba'
    output_dir.mkdir(parents=True, exist_ok=True)
    logger.info('=== kssba.or.kr 爬虫启动 ===')

    if args.skip_detail:
        logger.info('KSSBA 无详情阶段，忽略 --skip-detail')

    try:
        if not args.skip_list:
            crawl_list(output_dir=output_dir, max_pages=args.max_pages)
        if not args.skip_snov:
            run_snov_pipeline(
                output_dir=output_dir,
                max_items=args.max_items,
                concurrency=args.snov_concurrency,
                request_delay=args.snov_delay,
            )
        final_file = output_dir / 'companies_with_emails.jsonl'
        if final_file.exists():
            logger.info('--- 域名去重 ---')
            deduped = deduplicate_by_domain(final_file)
            logger.info('去重完成: %d 条', deduped)
    except KeyboardInterrupt:
        logger.warning('用户中断，已保存断点。')
        return 130

    logger.info('=== kssba.or.kr 爬虫完毕 ===')
    return 0
