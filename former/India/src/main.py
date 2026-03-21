from __future__ import annotations

import argparse
import os
import sys

from src.core.config import CrawlerConfig, DEFAULT_USER_AGENT
from src.core.crawler import ZaubaCrawler


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='ZaubaCorp Active Companies Crawler')
    parser.add_argument('--start-page', type=int, default=1, help='起始页码')
    parser.add_argument('--end-page', type=int, default=None, help='结束页码')
    parser.add_argument('--concurrency', type=int, default=24, help='详情页并发数')
    parser.add_argument('--timeout', type=int, default=30, help='请求超时时间(秒)')
    parser.add_argument('--min-delay', type=float, default=0.1, help='请求最小延迟(秒)')
    parser.add_argument('--max-delay', type=float, default=0.3, help='请求最大延迟(秒)')
    parser.add_argument('--max-retries', type=int, default=3, help='请求重试次数')
    parser.add_argument('--output-dir', type=str, default='output', help='输出目录')
    parser.add_argument('--cookies', type=str, default=None, help='cookies.json 路径')
    parser.add_argument('--user-agent', type=str, default=DEFAULT_USER_AGENT, help='自定义 UA')
    parser.add_argument('--no-resume', action='store_true', help='禁用断点续跑')
    return parser.parse_args()


def _ensure_utf8_console() -> None:
    if os.name == 'nt':
        try:
            os.system('chcp 65001 > NUL')
        except Exception:
            pass
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding='utf-8', errors='replace')
        except Exception:
            pass


def main() -> None:
    _ensure_utf8_console()
    args = parse_args()
    config = CrawlerConfig(
        start_page=args.start_page,
        end_page=args.end_page,
        concurrency=args.concurrency,
        timeout=args.timeout,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        max_retries=args.max_retries,
        output_dir=args.output_dir,
        cookies_file=args.cookies,
        user_agent=args.user_agent,
        resume=not args.no_resume,
    )
    crawler = ZaubaCrawler(config)
    crawler.run()


if __name__ == '__main__':
    main()
