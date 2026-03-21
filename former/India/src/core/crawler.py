from __future__ import annotations

import json
import logging
import queue
import random
import threading
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

from src.core.config import (
    CrawlerConfig,
    DEFAULT_LISTING_FIRST,
    DEFAULT_LISTING_TEMPLATE,
)
from src.core.fetcher import (
    CloudflareChallengeError,
    FetchError,
    Fetcher,
    TooManyRequestsError,
)
from src.core.parser import extract_total_pages, parse_detail_page, parse_list_page
from src.utils.cookies import cookies_to_dict, load_cookies
from src.utils.progress_db import ProgressDB
from src.utils.writer import OutputWriter


class ZaubaCrawler:
    def __init__(self, config: CrawlerConfig) -> None:
        self.config = config
        self.logger = logging.getLogger('zaubacorp')
        self._throttle_lock = threading.Lock()
        self._challenge_lock = threading.Lock()
        self._challenge_failures = 0

        cookies = {}
        cookies_file = config.cookies_file
        if not cookies_file and Path('cookies.json').exists():
            cookies_file = 'cookies.json'
        if cookies_file:
            cookies = cookies_to_dict(load_cookies(cookies_file))
        self._fetcher = Fetcher(
            cookies=cookies,
            user_agent=config.user_agent,
            timeout=config.timeout,
            min_delay=config.min_delay,
            max_delay=config.max_delay,
            max_retries=config.max_retries,
        )

    def run(self) -> None:
        output_dir = self._prepare_output_dir()
        self._setup_logging(output_dir)
        db = ProgressDB(str(output_dir / 'checkpoint.sqlite3'))
        writer = OutputWriter(str(output_dir))

        try:
            start_page, end_page = self._resolve_page_range()
            self.logger.info('抓取页码范围: %s - %s', start_page, end_page)
            self._save_run_metadata(output_dir, start_page, end_page)

            detail_queue = self._build_detail_queue()
            stop_token = object()
            company_since_commit = 0
            company_lock = threading.Lock()

            def mark_company(cin: str) -> None:
                nonlocal company_since_commit
                if not cin:
                    return
                db.mark_company_done(cin)
                with company_lock:
                    company_since_commit += 1
                    if company_since_commit >= self.config.commit_every:
                        db.commit()
                        company_since_commit = 0

            def process_company(company: Dict[str, str]) -> None:
                cin = company.get('cin', '')
                if self.config.resume and cin and db.is_company_done(cin):
                    return
                url = company.get('detail_url')
                if not url:
                    return
                try:
                    html = self._fetcher.get(url)
                except TooManyRequestsError:
                    detail_queue.put(company)
                    self._sleep_429()
                    return
                except CloudflareChallengeError:
                    detail_queue.put(company)
                    self._record_challenge()
                    self._refresh_cookies()
                    self._sleep_cf()
                    return
                except FetchError as exc:
                    self.logger.error('详情页获取失败: %s (%s)', url, exc)
                    return
                self._clear_challenge()
                basic_info, contact_details, current_director = parse_detail_page(html)
                writer.write({
                    **company,
                    'basic_info': basic_info,
                    'contact_details': contact_details,
                    'current_director': current_director,
                })
                mark_company(cin)

            def worker() -> None:
                while True:
                    company = detail_queue.get()
                    if company is stop_token:
                        detail_queue.task_done()
                        return
                    try:
                        process_company(company)
                    finally:
                        detail_queue.task_done()

            with ThreadPoolExecutor(max_workers=self.config.concurrency) as executor:
                futures = [executor.submit(worker) for _ in range(self.config.concurrency)]

                page_queue = deque(range(start_page, end_page + 1))
                challenge_failures = 0
                while page_queue:
                    page = page_queue.popleft()
                    if self.config.resume and db.is_page_done(page):
                        continue
                    list_url = self._list_url(page)
                    self.logger.info('抓取列表页: %s', list_url)

                    try:
                        html = self._fetcher.get(list_url)
                    except TooManyRequestsError:
                        page_queue.append(page)
                        self._sleep_429()
                        continue
                    except CloudflareChallengeError:
                        page_queue.append(page)
                        self._record_challenge()
                        self._refresh_cookies()
                        self._sleep_cf()
                        continue
                    except FetchError as exc:
                        self.logger.error('列表页获取失败: %s', exc)
                        continue

                    self._clear_challenge()
                    companies = parse_list_page(html)
                    if not companies:
                        self.logger.warning('列表页解析为空，跳过: %s', list_url)
                        continue

                    for company in companies:
                        cin = company.get('cin', '')
                        if not cin:
                            continue
                        if self.config.resume and db.is_company_done(cin):
                            continue
                        detail_queue.put(company)

                    db.mark_page_done(page)
                    db.commit()

                detail_queue.join()
                for _ in futures:
                    detail_queue.put(stop_token)
                for future in futures:
                    future.result()

            db.commit()
        finally:
            writer.close()
            db.close()

    def _sleep_429(self) -> None:
        delay = random.uniform(self.config.backoff_429_min, self.config.backoff_429_max)
        with self._throttle_lock:
            self.logger.warning('触发 429，休息 %.1f 秒', delay)
            time.sleep(delay)

    def _sleep_cf(self) -> None:
        delay = random.uniform(self.config.backoff_cf_min, self.config.backoff_cf_max)
        with self._throttle_lock:
            self.logger.warning('触发 Cloudflare 验证，休息 %.1f 秒', delay)
            time.sleep(delay)

    def _record_challenge(self) -> None:
        with self._challenge_lock:
            self._challenge_failures += 1
            if self._challenge_failures == self.config.max_challenge_failures:
                self.logger.warning('Cloudflare 验证连续出现，建议更新 cookies.json 后继续')

    def _clear_challenge(self) -> None:
        with self._challenge_lock:
            self._challenge_failures = 0

    def _refresh_cookies(self) -> None:
        cookies_file = self.config.cookies_file
        if not cookies_file and Path('cookies.json').exists():
            cookies_file = 'cookies.json'
        if not cookies_file:
            return
        cookies = cookies_to_dict(load_cookies(cookies_file))
        if not cookies:
            return
        self._fetcher.update_cookies(cookies)

    def _build_detail_queue(self) -> queue.Queue:
        maxsize = max(1000, self.config.concurrency * 200)
        return queue.Queue(maxsize=maxsize)

    def _resolve_page_range(self) -> tuple[int, int]:
        start_page = max(self.config.start_page, 1)
        if self.config.end_page:
            return start_page, self.config.end_page
        attempts = 0
        while True:
            try:
                html = self._fetcher.get(self._list_url(1))
                self._clear_challenge()
                total_pages = extract_total_pages(html) or start_page
                return start_page, total_pages
            except TooManyRequestsError:
                self._sleep_429()
            except CloudflareChallengeError:
                attempts += 1
                self._record_challenge()
                self._refresh_cookies()
                self._sleep_cf()
                if attempts >= self.config.max_challenge_failures:
                    raise

    def _list_url(self, page: int) -> str:
        if page <= 1:
            return DEFAULT_LISTING_FIRST
        return DEFAULT_LISTING_TEMPLATE.format(page=page)

    def _prepare_output_dir(self) -> Path:
        base = Path(self.config.output_dir)
        if base.name != 'output':
            base.mkdir(parents=True, exist_ok=True)
            return base
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        output_dir = base / timestamp
        output_dir.mkdir(parents=True, exist_ok=True)
        return output_dir

    def _save_run_metadata(self, output_dir: Path, start_page: int, end_page: int) -> None:
        meta = {
            'start_page': start_page,
            'end_page': end_page,
            'concurrency': self.config.concurrency,
            'min_delay': self.config.min_delay,
            'max_delay': self.config.max_delay,
            'user_agent': self.config.user_agent,
            'timestamp': datetime.now().isoformat(timespec='seconds'),
        }
        (output_dir / 'run.json').write_text(
            json.dumps(meta, ensure_ascii=False, indent=2), encoding='utf-8'
        )

    def _setup_logging(self, output_dir: Path) -> None:
        log_path = output_dir / 'run.log'
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s %(levelname)s %(message)s',
            handlers=[
                logging.StreamHandler(),
                logging.FileHandler(log_path, encoding='utf-8'),
            ],
        )
