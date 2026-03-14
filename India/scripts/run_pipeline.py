from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

try:
    import browser_cookie3
except Exception as exc:
    print(f"browser_cookie3 未安装或不可用: {exc}")
    print("请先安装依赖: pip install browser-cookie3")
    sys.exit(1)

from src.core.config import CrawlerConfig, DEFAULT_USER_AGENT
from src.core.crawler import ZaubaCrawler


def _ensure_utf8_console() -> None:
    if os.name == "nt":
        try:
            os.system("chcp 65001 > NUL")
        except Exception:
            pass
    for stream in (sys.stdout, sys.stderr):
        try:
            stream.reconfigure(encoding="utf-8", errors="replace")
        except Exception:
            pass


def _find_local_state(cookie_file: str | None) -> str | None:
    if not cookie_file:
        return None
    path = Path(cookie_file)
    for parent in path.parents:
        candidate = parent / "Local State"
        if candidate.exists():
            return str(candidate)
    return None


def _open_chrome(chrome_path: str, profile_dir: str, url: str) -> None:
    exe = Path(chrome_path)
    command = [
        str(exe) if exe.exists() else chrome_path,
        f"--user-data-dir={profile_dir}",
        url,
    ]
    subprocess.Popen(command, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)


def _export_cookies(
    domain: str,
    browser: str,
    cookie_file: str | None,
    key_file: str | None,
    output: str,
) -> int:
    loader = browser_cookie3.chrome if browser == "chrome" else browser_cookie3.edge
    kwargs = {"domain_name": domain}
    if cookie_file:
        kwargs["cookie_file"] = cookie_file
    if key_file:
        kwargs["key_file"] = key_file

    jar = loader(**kwargs)
    cookies = [
        {
            "name": cookie.name,
            "value": cookie.value,
            "domain": cookie.domain,
            "path": cookie.path,
            "secure": cookie.secure,
        }
        for cookie in jar
    ]
    Path(output).write_text(
        json.dumps({"cookies": cookies}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    print(f"已导出 {len(cookies)} 条 cookie 到 {output}")
    return len(cookies)


def _has_cf_clearance(path: str) -> bool:
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except Exception:
        return False
    cookies = data.get("cookies", data)
    if not isinstance(cookies, list):
        return False
    return any(isinstance(c, dict) and c.get("name") == "cf_clearance" for c in cookies)


def main() -> None:
    _ensure_utf8_console()
    parser = argparse.ArgumentParser(description="一键获取 cookies 并续跑爬虫")
    parser.add_argument("--chrome-path", default=r"C:\Program Files\Google\Chrome\Application\chrome.exe")
    parser.add_argument("--profile-dir", default=str(Path(".chrome-profile").resolve()))
    parser.add_argument("--domain", default="zaubacorp.com")
    parser.add_argument("--browser", default="chrome", choices=["chrome", "edge"])
    parser.add_argument("--profile", default="Default")
    parser.add_argument("--cookie-file", default=None)
    parser.add_argument("--key-file", default=None)
    parser.add_argument("--output-cookies", default="cookies.json")
    parser.add_argument("--start-page", type=int, default=1)
    parser.add_argument("--end-page", type=int, default=None)
    parser.add_argument("--concurrency", type=int, default=8)
    parser.add_argument("--min-delay", type=float, default=0.3)
    parser.add_argument("--max-delay", type=float, default=0.8)
    parser.add_argument("--timeout", type=int, default=30)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
    parser.add_argument("--output-dir", default=str(Path("output/zauba_active")))
    parser.add_argument("--skip-open", action="store_true", help="跳过打开 Chrome")
    parser.add_argument("--skip-run", action="store_true", help="只导出 cookies，不启动爬虫")
    parser.add_argument("--force", action="store_true", help="即使没有 cf_clearance 也继续")
    args = parser.parse_args()

    if not args.skip_open:
        target_url = f"https://www.{args.domain}/companies-list/status-Active-company.html"
        print("正在打开 Chrome，请通过 Cloudflare 验证后关闭浏览器。")
        _open_chrome(args.chrome_path, args.profile_dir, target_url)
        input("完成验证并关闭 Chrome 后，按回车继续导出 cookies...")

    cookie_file = args.cookie_file
    if not cookie_file:
        cookie_file = str(Path(args.profile_dir) / args.profile / "Network" / "Cookies")
    key_file = args.key_file or _find_local_state(cookie_file)

    count = _export_cookies(
        domain=args.domain,
        browser=args.browser,
        cookie_file=cookie_file,
        key_file=key_file,
        output=args.output_cookies,
    )

    if count == 0:
        print("未导出 cookies，请确认已通过 Cloudflare 并关闭 Chrome。")
        if not args.force:
            sys.exit(2)

    if not _has_cf_clearance(args.output_cookies):
        print("未检测到 cf_clearance，建议重新通过 Cloudflare 后再导出。")
        if not args.force:
            sys.exit(2)

    if args.skip_run:
        return

    config = CrawlerConfig(
        start_page=args.start_page,
        end_page=args.end_page,
        concurrency=args.concurrency,
        timeout=args.timeout,
        min_delay=args.min_delay,
        max_delay=args.max_delay,
        max_retries=args.max_retries,
        output_dir=args.output_dir,
        cookies_file=args.output_cookies,
        user_agent=args.user_agent,
        resume=True,
    )
    crawler = ZaubaCrawler(config)
    crawler.run()


if __name__ == "__main__":
    main()
