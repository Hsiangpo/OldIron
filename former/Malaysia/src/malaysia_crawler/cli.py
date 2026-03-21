"""命令行入口。"""

from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path
from typing import Any
from typing import Sequence
from urllib.parse import urlsplit
from urllib.parse import urlunsplit

from malaysia_crawler.businesslist.cookie_sync import DEFAULT_TARGET_URL
from malaysia_crawler.businesslist.cookie_sync import DEFAULT_LOGIN_PROBE_COMPANY_ID
from malaysia_crawler.businesslist.cookie_sync import probe_businesslist_login_status
from malaysia_crawler.businesslist.cookie_sync import sync_cookie_from_cdp
from malaysia_crawler.businesslist.cf_crawler import BusinessListCFCrawler
from malaysia_crawler.businesslist.crawler import BusinessListCrawler
from malaysia_crawler.common.env_loader import load_dotenv
from malaysia_crawler.common.io_utils import ensure_dir
from malaysia_crawler.ctos_directory.crawler import CTOSDirectoryCrawler
from malaysia_crawler.snov.client import SnovClient
from malaysia_crawler.snov.client import SnovConfig
from malaysia_crawler.snov.pipeline import CtosBusinessListSnovPipeline
from malaysia_crawler.manager_agent import ManagerAgentConfig
from malaysia_crawler.manager_agent import ManagerAgentService
from malaysia_crawler.streaming.pipeline import MalaysiaStreamingPipeline
from malaysia_crawler.streaming.pipeline import StreamingPipelineConfig
from malaysia_crawler.streaming.pipeline import build_businesslist_source

DEFAULT_BUSINESSLIST_CF_COOKIE_FILE = "cookies/businesslist.cf.cookie.txt"
DEFAULT_BUSINESSLIST_CF_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/145.0.0.0 Safari/537.36"
)


def _project_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _env_bool(name: str, default: bool = False) -> bool:
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "y", "on"}


def _has_system_proxy_env() -> bool:
    for key in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY"):
        if os.getenv(key, "").strip():
            return True
    return False


def _mask_proxy_url(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    try:
        parts = urlsplit(value)
    except Exception:  # noqa: BLE001
        return value
    netloc = parts.netloc
    if "@" in netloc:
        _, host = netloc.rsplit("@", 1)
        netloc = f"***@{host}"
    return urlunsplit((parts.scheme, netloc, parts.path, parts.query, parts.fragment))


def _cmd_sync_businesslist_cookie(args: argparse.Namespace) -> int:
    result = sync_cookie_from_cdp(
        cdp_url=args.businesslist_cdp_url,
        output_file=args.businesslist_cf_cookies_file,
        target_url=args.target_url,
        wait_seconds=args.wait_seconds,
        poll_seconds=args.poll_seconds,
        require_login=args.businesslist_require_login,
        login_probe_company_id=args.businesslist_login_probe_company_id,
    )
    has_cf = "是" if result.get("cf_clearance", "") else "否"
    has_cakephp = "是" if result.get("cakephp", "") else "否"
    login_verified = "是" if result.get("login_verified", "0") == "1" else "否"
    print(
        "完成：已写入 Cookie 文件 {cookie_file}，cf_clearance={has_cf}，CAKEPHP={has_cakephp}，登录校验={login_verified}".format(
            cookie_file=result.get("cookie_file", args.businesslist_cf_cookies_file),
            has_cf=has_cf,
            has_cakephp=has_cakephp,
            login_verified=login_verified,
        )
    )
    return 0


def _resolve_snov_credentials(args: argparse.Namespace) -> tuple[str, str]:
    client_id = (getattr(args, "snov_client_id", "") or os.getenv("SNOV_CLIENT_ID", "")).strip()
    client_secret = (getattr(args, "snov_client_secret", "") or os.getenv("SNOV_CLIENT_SECRET", "")).strip()
    if not client_id or not client_secret:
        raise ValueError("缺少 Snov 凭据，请在 .env 中设置 SNOV_CLIENT_ID/SNOV_CLIENT_SECRET。")
    return client_id, client_secret


def _resolve_manager_agent_config() -> ManagerAgentConfig:
    config = ManagerAgentConfig.from_env(_project_root())
    missing: list[str] = []
    if not config.llm_api_key:
        missing.append("LLM_API_KEY")
    if not config.llm_base_url:
        missing.append("LLM_BASE_URL")
    if not config.llm_model:
        missing.append("LLM_MODEL")
    if missing:
        raise ValueError(
            "缺少管理人补全 LLM 配置，请在 .env 设置："
            + ", ".join(missing)
        )
    return config


def _prepare_firecrawl_keys(config: ManagerAgentConfig) -> None:
    seed_raw = os.getenv("FIRECRAWL_KEYS_SEED_FILE", "").strip()
    if seed_raw:
        seed_path = Path(seed_raw)
        if not seed_path.is_absolute():
            seed_path = _project_root() / seed_path
    else:
        seed_path = _project_root().parent / "wikipedia" / "output" / "firecrawl_keys.txt"
    ManagerAgentService.ensure_keys_file(config.firecrawl_keys_file, seed_path)


def _ensure_cf_cookie_file(args: argparse.Namespace) -> None:
    if getattr(args, "businesslist_source", "") != "cf":
        return
    cookie_file = Path(getattr(args, "businesslist_cf_cookies_file", "")).expanduser()
    if not cookie_file.exists():
        raise ValueError(f"缺少 BusinessList cf cookie 文件：{cookie_file}")
    content = cookie_file.read_text(encoding="utf-8").strip()
    if not content:
        raise ValueError(f"BusinessList cf cookie 文件为空：{cookie_file}")
    if not bool(getattr(args, "businesslist_require_login", True)):
        return
    login_ok, reason, probe_url = probe_businesslist_login_status(
        content,
        login_probe_company_id=int(
            getattr(args, "businesslist_login_probe_company_id", DEFAULT_LOGIN_PROBE_COMPANY_ID)
        ),
        timeout=min(max(float(getattr(args, "timeout", 30.0)), 3.0), 20.0),
    )
    if not login_ok:
        raise ValueError(
            "BusinessList Cookie 未处于登录态，"
            f"原因={reason} url={probe_url or '-'}。"
            "请先在 9222 浏览器登录后执行："
            "python -m malaysia_crawler.cli sync-businesslist-cookie "
            "--businesslist-require-login"
        )


def _load_historical_businesslist_max_non_miss(db_path: Path) -> int | None:
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(str(db_path), timeout=5)
    except sqlite3.Error:
        return None
    try:
        row = conn.execute(
            "SELECT MAX(company_id) FROM businesslist_scan WHERE status <> 'miss'"
        ).fetchone()
    except sqlite3.Error:
        conn.close()
        return None
    conn.close()
    if row is None:
        return None
    value = row[0]
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _load_historical_businesslist_min_non_miss(db_path: Path) -> int | None:
    if not db_path.exists():
        return None
    try:
        conn = sqlite3.connect(str(db_path), timeout=5)
    except sqlite3.Error:
        return None
    try:
        row = conn.execute(
            "SELECT MIN(company_id) FROM businesslist_scan WHERE status <> 'miss'"
        ).fetchone()
    except sqlite3.Error:
        conn.close()
        return None
    conn.close()
    if row is None:
        return None
    value = row[0]
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _probe_first_businesslist_hit_id(
    *,
    start_id: int,
    end_id: int,
    cookies_file: str,
    user_agent: str,
    proxy_url: str,
    use_system_proxy: bool,
    timeout: float,
    delay_min: float,
    delay_max: float,
) -> int | None:
    probe_end = min(end_id, 200000)
    if probe_end < start_id:
        return None

    crawler = BusinessListCFCrawler(
        timeout=timeout,
        delay_min=delay_min,
        delay_max=delay_max,
        max_retries=2,
        backoff_base=1.3,
        user_agent=user_agent,
        cookies_file=cookies_file,
        proxy_url=proxy_url,
        use_system_proxy=use_system_proxy,
    )
    hit_cache: dict[int, bool] = {}

    def is_hit(company_id: int) -> bool:
        if company_id in hit_cache:
            return hit_cache[company_id]
        try:
            profile = crawler.fetch_company_profile(company_id)
            result = profile is not None
        except Exception:  # noqa: BLE001
            result = False
        hit_cache[company_id] = result
        return result

    try:
        coarse_hit: int | None = None
        for company_id in range(start_id, probe_end + 1, 5000):
            if is_hit(company_id):
                coarse_hit = company_id
                break
        if coarse_hit is None:
            return None

        left = max(start_id, coarse_hit - 5000)
        right = coarse_hit
        for step in (500, 50, 5, 1):
            candidate: int | None = None
            for company_id in range(left, right + 1, step):
                if is_hit(company_id):
                    candidate = company_id
                    break
            if candidate is None:
                continue
            right = candidate
            left = max(start_id, candidate - step)
        return right
    finally:
        crawler.close()


def _resolve_streaming_businesslist_start_id(args: argparse.Namespace, db_path: Path) -> int:
    raw_start = max(int(args.businesslist_start_id), 1)
    if raw_start > 1:
        return raw_start

    historical_min = _load_historical_businesslist_min_non_miss(db_path)
    if historical_min is not None and historical_min > raw_start:
        tail_window = max(int(args.businesslist_resume_tail_window), 0)
        rewind = min(tail_window, 50000)
        adjusted = max(raw_start, historical_min - rewind)
        if adjusted > raw_start:
            print(
                f"[BusinessList] 检测到历史有效ID下限={historical_min}，"
                f"自动将起始ID提升为 {adjusted}（原 {raw_start}）。"
            )
            return adjusted

    source = str(getattr(args, "businesslist_source", "")).strip().lower()
    if source != "cf":
        return raw_start

    cookie_file = Path(getattr(args, "businesslist_cf_cookies_file", "")).expanduser()
    if not cookie_file.exists():
        return raw_start

    raw_end = max(int(args.businesslist_end_id), raw_start)
    try:
        first_hit = _probe_first_businesslist_hit_id(
            start_id=raw_start,
            end_id=raw_end,
            cookies_file=str(cookie_file),
            user_agent=str(getattr(args, "businesslist_cf_user_agent", "")).strip(),
            proxy_url=str(getattr(args, "businesslist_proxy_url", "")).strip(),
            use_system_proxy=bool(getattr(args, "businesslist_use_system_proxy", False)),
            timeout=float(getattr(args, "timeout", 30.0)),
            delay_min=float(getattr(args, "delay_min", 0.1)),
            delay_max=float(getattr(args, "delay_max", 0.3)),
        )
    except Exception as exc:  # noqa: BLE001
        print(f"[BusinessList] 有效ID探测失败，维持起始ID={raw_start}：{type(exc).__name__}: {exc}")
        return raw_start
    if first_hit is None or first_hit <= raw_start:
        return raw_start
    print(
        f"[BusinessList] 探测到低位大量空页，首个有效ID约为 {first_hit}，"
        f"自动将起始ID提升为 {first_hit}。"
    )
    return first_hit


def _resolve_streaming_businesslist_end_id(
    args: argparse.Namespace,
    db_path: Path,
    *,
    start_id: int | None = None,
) -> int:
    raw_start = int(start_id if start_id is not None else args.businesslist_start_id)
    raw_end = int(args.businesslist_end_id)
    start_id = max(raw_start, 1)
    end_id = max(raw_end, start_id)
    tail_window = max(int(args.businesslist_resume_tail_window), 0)
    historical_max = _load_historical_businesslist_max_non_miss(db_path)
    if historical_max is None:
        return end_id
    if historical_max < start_id:
        return end_id
    cap_end = max(historical_max + tail_window, start_id)
    if cap_end >= end_id:
        return end_id
    print(
        f"[BusinessList] 检测到历史有效ID上限={historical_max}，"
        f"自动将结束ID收敛为 {cap_end}（原 {end_id}）。"
    )
    return cap_end


def _cmd_ctos_directory_crawl(args: argparse.Namespace) -> int:
    crawler = CTOSDirectoryCrawler(
        timeout=args.timeout,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        verify_ssl=not args.skip_tls_verify,
    )
    stats = crawler.crawl(
        output_dir=args.output_dir,
        prefixes=args.prefixes,
        start_page=args.start_page,
        max_pages_per_prefix=args.max_pages_per_prefix,
        max_prefixes=args.max_prefixes,
        with_detail=args.with_detail,
        state_file=args.state_file,
    )
    print(
        "完成：前缀 {prefixes_done}，页面 {pages_done}，公司 {companies_done}，详情 {details_done}".format(
            **stats
        )
    )
    return 0


def _cmd_businesslist_crawl(args: argparse.Namespace) -> int:
    crawler = BusinessListCrawler(
        timeout=args.timeout,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        verify_ssl=not args.skip_tls_verify,
        proxy_url=args.businesslist_proxy_url,
        use_system_proxy=args.businesslist_use_system_proxy,
    )
    stats = crawler.crawl(
        output_dir=args.output_dir,
        start_id=args.start_id,
        end_id=args.end_id,
        max_companies=args.max_companies,
        state_file=args.state_file,
    )
    print(
        "完成：扫描ID {scanned_ids}，命中公司 {companies_done}，下次起点 {next_id}".format(
            **stats
        )
    )
    return 0


def _cmd_ctos_businesslist_snov(args: argparse.Namespace) -> int:
    client_id, client_secret = _resolve_snov_credentials(args)
    _ensure_cf_cookie_file(args)
    manager_config = _resolve_manager_agent_config()
    _prepare_firecrawl_keys(manager_config)
    manager_agent = ManagerAgentService.from_config(manager_config)

    businesslist_source: Any
    if args.businesslist_source == "cf":
        businesslist_source = BusinessListCFCrawler(
            timeout=args.timeout,
            delay_min=args.delay_min,
            delay_max=args.delay_max,
            max_retries=args.businesslist_cf_max_retries,
            backoff_base=args.businesslist_cf_backoff_base,
            user_agent=args.businesslist_cf_user_agent,
            cookies_file=args.businesslist_cf_cookies_file,
            proxy_url=args.businesslist_proxy_url,
            use_system_proxy=args.businesslist_use_system_proxy,
        )
    else:
        businesslist_source = BusinessListCrawler(
            timeout=args.timeout,
            delay_min=args.delay_min,
            delay_max=args.delay_max,
            verify_ssl=not args.skip_tls_verify,
            proxy_url=args.businesslist_proxy_url,
            use_system_proxy=args.businesslist_use_system_proxy,
        )

    pipeline = CtosBusinessListSnovPipeline(
        ctos_crawler=CTOSDirectoryCrawler(
            timeout=args.timeout,
            delay_min=args.delay_min,
            delay_max=args.delay_max,
            verify_ssl=not args.skip_tls_verify,
        ),
        businesslist_crawler=businesslist_source,
        snov_client=SnovClient(
            SnovConfig(
                client_id=client_id,
                client_secret=client_secret,
                timeout=args.timeout,
            )
        ),
        manager_agent=manager_agent,
        manager_enrich_max_rounds=manager_config.max_rounds,
        manager_enrich_workers=args.manager_enrich_workers,
    )
    stats = pipeline.run(
        output_dir=args.output_dir,
        target_companies=args.target_companies,
        ctos_prefixes=args.ctos_prefixes,
        ctos_max_pages_per_prefix=args.ctos_max_pages_per_prefix,
        businesslist_start_id=args.businesslist_start_id,
        businesslist_end_id=args.businesslist_end_id,
        require_ctos_match=args.require_ctos_match,
    )
    print(
        "完成：CTOS名池 {ctos_name_pool}，扫描ID {scanned_ids}，命中公司 {matched_companies}，"
        "Snov补全 {snov_enriched}，管理人来源 BL={manager_from_businesslist}，"
        "管理人来源 Firecrawl+LLM={manager_from_firecrawl_llm}，缓存命中成功={manager_from_cache_success}，"
        "缓存命中失败={manager_from_cache_miss}，"
        "管理人补全尝试={manager_fallback_attempted}，管理人仍缺失={manager_missing_after_fallback}，"
        "耗时 {elapsed_seconds} 秒".format(**stats)
    )
    return 0


def _cmd_streaming_run(args: argparse.Namespace) -> int:
    client_id, client_secret = _resolve_snov_credentials(args)
    _ensure_cf_cookie_file(args)
    manager_agent: Any = None
    manager_max_rounds = 3
    manager_retry_backoff = 30.0
    if not args.allow_no_manager_output:
        manager_config = _resolve_manager_agent_config()
        _prepare_firecrawl_keys(manager_config)
        manager_agent = ManagerAgentService.from_config(manager_config)
        manager_max_rounds = manager_config.max_rounds
        manager_retry_backoff = manager_config.retry_backoff_seconds

    ctos_worker_count = max(int(args.ctos_worker_count), 1)
    businesslist_worker_count = max(int(args.businesslist_worker_count), 1)
    snov_worker_count = max(int(args.snov_worker_count), 1)
    manager_worker_count = max(int(args.manager_worker_count), 1)
    proxy_url_masked = _mask_proxy_url(str(args.businesslist_proxy_url))
    proxy_mode = "系统代理" if args.businesslist_use_system_proxy else "直连"
    if proxy_url_masked:
        proxy_mode = f"显式代理({proxy_url_masked})"
    print(f"[Pipeline] BusinessList 出口：{proxy_mode}")

    ctos_crawlers = [
        CTOSDirectoryCrawler(
            timeout=args.timeout,
            delay_min=args.delay_min,
            delay_max=args.delay_max,
            verify_ssl=not args.skip_tls_verify,
        )
        for _ in range(ctos_worker_count)
    ]
    businesslist_sources = [
        build_businesslist_source(
            source=args.businesslist_source,
            cf_cookies_file=args.businesslist_cf_cookies_file,
            cf_user_agent=args.businesslist_cf_user_agent,
            cf_max_retries=args.businesslist_cf_max_retries,
            cf_backoff_base=args.businesslist_cf_backoff_base,
            proxy_url=args.businesslist_proxy_url,
            use_system_proxy=args.businesslist_use_system_proxy,
            timeout=args.timeout,
            delay_min=args.delay_min,
            delay_max=args.delay_max,
            verify_ssl=not args.skip_tls_verify,
        )
        for _ in range(businesslist_worker_count)
    ]
    snov_clients = [
        SnovClient(
            SnovConfig(
                client_id=client_id,
                client_secret=client_secret,
                timeout=args.timeout,
            )
        )
        for _ in range(snov_worker_count)
    ]
    db_path = Path(args.db_path)
    effective_businesslist_start_id = _resolve_streaming_businesslist_start_id(args, db_path)
    effective_businesslist_end_id = _resolve_streaming_businesslist_end_id(
        args,
        db_path,
        start_id=effective_businesslist_start_id,
    )

    pipeline = MalaysiaStreamingPipeline(
        config=StreamingPipelineConfig(
            db_path=db_path,
            ctos_prefixes=args.ctos_prefixes,
            businesslist_start_id=effective_businesslist_start_id,
            businesslist_end_id=effective_businesslist_end_id,
            log_interval_seconds=args.log_interval_seconds,
            retry_sleep_seconds=args.retry_sleep_seconds,
            snov_max_retries=args.snov_max_retries,
            ctos_transient_retry_limit=args.ctos_transient_retry_limit,
            businesslist_transient_retry_limit=args.businesslist_transient_retry_limit,
            businesslist_cf_block_retry_limit=args.businesslist_cf_block_retry_limit,
            businesslist_cf_backoff_base_seconds=args.businesslist_cf_backoff_base_seconds,
            businesslist_cf_backoff_cap_seconds=args.businesslist_cf_backoff_cap_seconds,
            backoff_cap_seconds=args.backoff_cap_seconds,
            zero_queue_guard_min_hits=args.zero_queue_guard_min_hits,
            strict_ctos_match=args.strict_ctos_match,
            contact_email_fast_path=not args.disable_contact_email_fast_path,
            stale_running_requeue_seconds=args.stale_running_requeue_seconds,
            ctos_worker_count=ctos_worker_count,
            businesslist_worker_count=businesslist_worker_count,
            snov_worker_count=snov_worker_count,
            manager_worker_count=manager_worker_count,
            require_manager_for_output=not args.allow_no_manager_output,
            manager_enrich_max_rounds=manager_max_rounds,
            manager_enrich_retry_backoff_seconds=manager_retry_backoff,
            businesslist_require_login=bool(args.businesslist_require_login),
            businesslist_login_probe_company_id=max(int(args.businesslist_login_probe_company_id), 1),
        ),
        ctos_crawler=ctos_crawlers,
        businesslist_crawler=businesslist_sources,
        snov_client=snov_clients,
        manager_agent=manager_agent,
    )
    pipeline.run_forever()
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="马来西亚公司主流程抓取工具")
    parser.add_argument("--timeout", type=float, default=30.0, help="请求超时秒数")
    parser.add_argument("--skip-tls-verify", action="store_true", help="跳过 TLS 证书校验")
    sub = parser.add_subparsers(dest="command", required=True)

    ctos = sub.add_parser("ctos-directory-crawl", help="抓取 CTOS 公共公司目录")
    ctos.add_argument("--output-dir", default="output/ctos_directory")
    ctos.add_argument("--prefixes", default="0123456789abcdefghijklmnopqrstuvwxyz")
    ctos.add_argument("--start-page", type=int, default=1)
    ctos.add_argument("--max-pages-per-prefix", type=int)
    ctos.add_argument("--max-prefixes", type=int)
    ctos.add_argument("--with-detail", action="store_true")
    ctos.add_argument("--delay-min", type=float, default=0.3)
    ctos.add_argument("--delay-max", type=float, default=0.8)
    ctos.add_argument("--state-file")
    ctos.set_defaults(func=_cmd_ctos_directory_crawl)

    businesslist = sub.add_parser("businesslist-crawl", help="按 company_id 抓取 BusinessList 公司档案")
    businesslist.add_argument("--output-dir", default="output/businesslist")
    businesslist.add_argument("--start-id", type=int, default=1)
    businesslist.add_argument("--end-id", type=int, default=500000)
    businesslist.add_argument("--max-companies", type=int)
    businesslist.add_argument("--delay-min", type=float, default=0.3)
    businesslist.add_argument("--delay-max", type=float, default=0.8)
    businesslist.add_argument(
        "--businesslist-proxy-url",
        default=os.getenv("BUSINESSLIST_PROXY_URL", "").strip(),
        help="BusinessList 抓取代理（如 http://user:pass@host:port）",
    )
    businesslist.add_argument(
        "--businesslist-use-system-proxy",
        action="store_true",
        default=_env_bool("BUSINESSLIST_USE_SYSTEM_PROXY", _has_system_proxy_env()),
        help="启用系统代理环境变量（HTTP_PROXY/HTTPS_PROXY）",
    )
    businesslist.add_argument("--state-file")
    businesslist.set_defaults(func=_cmd_businesslist_crawl)

    snov_pipe = sub.add_parser(
        "ctos-businesslist-snov",
        help="CTOS 公司名 -> BusinessList 官网 -> Snov 域名邮箱",
    )
    snov_pipe.add_argument("--output-dir", default="output/ctos_businesslist_snov")
    snov_pipe.add_argument("--target-companies", type=int, default=30)
    snov_pipe.add_argument("--ctos-prefixes", default="0123456789abcdefghijklmnopqrstuvwxyz")
    snov_pipe.add_argument("--ctos-max-pages-per-prefix", type=int, default=20)
    snov_pipe.add_argument("--businesslist-start-id", type=int, default=381000)
    snov_pipe.add_argument("--businesslist-end-id", type=int, default=500000)
    snov_pipe.add_argument("--businesslist-source", choices=["cf", "requests"], default="cf")
    snov_pipe.add_argument("--businesslist-cf-cookies-file", default=DEFAULT_BUSINESSLIST_CF_COOKIE_FILE)
    snov_pipe.add_argument("--businesslist-cf-user-agent", default=DEFAULT_BUSINESSLIST_CF_USER_AGENT)
    snov_pipe.add_argument("--businesslist-cf-max-retries", type=int, default=3)
    snov_pipe.add_argument("--businesslist-cf-backoff-base", type=float, default=1.7)
    snov_pipe.add_argument(
        "--businesslist-require-login",
        action="store_true",
        default=_env_bool("BUSINESSLIST_REQUIRE_LOGIN", True),
        help="启动前强制校验 BusinessList Cookie 为登录态",
    )
    snov_pipe.add_argument(
        "--businesslist-login-probe-company-id",
        type=int,
        default=int(os.getenv("BUSINESSLIST_LOGIN_PROBE_COMPANY_ID", str(DEFAULT_LOGIN_PROBE_COMPANY_ID))),
        help="登录态探针用的 company_id",
    )
    snov_pipe.add_argument(
        "--businesslist-proxy-url",
        default=os.getenv("BUSINESSLIST_PROXY_URL", "").strip(),
    )
    snov_pipe.add_argument(
        "--businesslist-use-system-proxy",
        action="store_true",
        default=_env_bool("BUSINESSLIST_USE_SYSTEM_PROXY", _has_system_proxy_env()),
    )
    snov_pipe.add_argument("--require-ctos-match", action="store_true")
    snov_pipe.add_argument("--delay-min", type=float, default=0.3)
    snov_pipe.add_argument("--delay-max", type=float, default=0.8)
    snov_pipe.add_argument("--manager-enrich-workers", type=int, default=40)
    snov_pipe.add_argument("--snov-client-id", default="")
    snov_pipe.add_argument("--snov-client-secret", default="")
    snov_pipe.set_defaults(func=_cmd_ctos_businesslist_snov)

    stream = sub.add_parser("streaming-run", help="三线并发全量流式主流程（CTOS + BusinessList + Snov）")
    stream.add_argument("--db-path", default="output/runtime/malaysia_pipeline.db")
    stream.add_argument("--ctos-prefixes", default="0123456789abcdefghijklmnopqrstuvwxyz")
    stream.add_argument("--businesslist-start-id", type=int, default=1)
    stream.add_argument("--businesslist-end-id", type=int, default=900000)
    stream.add_argument("--businesslist-resume-tail-window", type=int, default=900000)
    stream.add_argument("--businesslist-source", choices=["cf", "requests"], default="cf")
    stream.add_argument("--businesslist-cf-cookies-file", default=DEFAULT_BUSINESSLIST_CF_COOKIE_FILE)
    stream.add_argument("--businesslist-cf-user-agent", default=DEFAULT_BUSINESSLIST_CF_USER_AGENT)
    stream.add_argument("--businesslist-cf-max-retries", type=int, default=3)
    stream.add_argument("--businesslist-cf-backoff-base", type=float, default=1.7)
    stream.add_argument(
        "--businesslist-require-login",
        action="store_true",
        default=_env_bool("BUSINESSLIST_REQUIRE_LOGIN", True),
        help="启动前强制校验 BusinessList Cookie 为登录态",
    )
    stream.add_argument(
        "--businesslist-login-probe-company-id",
        type=int,
        default=int(os.getenv("BUSINESSLIST_LOGIN_PROBE_COMPANY_ID", str(DEFAULT_LOGIN_PROBE_COMPANY_ID))),
        help="登录态探针用的 company_id",
    )
    stream.add_argument(
        "--businesslist-proxy-url",
        default=os.getenv("BUSINESSLIST_PROXY_URL", "").strip(),
    )
    stream.add_argument(
        "--businesslist-use-system-proxy",
        action="store_true",
        default=_env_bool("BUSINESSLIST_USE_SYSTEM_PROXY", _has_system_proxy_env()),
    )
    stream.add_argument("--delay-min", type=float, default=0.1)
    stream.add_argument("--delay-max", type=float, default=0.3)
    stream.add_argument("--log-interval-seconds", type=float, default=20.0)
    stream.add_argument("--retry-sleep-seconds", type=float, default=3.0)
    stream.add_argument("--snov-max-retries", type=int, default=3)
    stream.add_argument("--ctos-transient-retry-limit", type=int, default=8)
    stream.add_argument("--businesslist-transient-retry-limit", type=int, default=8)
    stream.add_argument("--businesslist-cf-block-retry-limit", type=int, default=4)
    stream.add_argument("--businesslist-cf-backoff-base-seconds", type=float, default=3.0)
    stream.add_argument("--businesslist-cf-backoff-cap-seconds", type=float, default=30.0)
    stream.add_argument("--backoff-cap-seconds", type=float, default=120.0)
    stream.add_argument("--zero-queue-guard-min-hits", type=int, default=200)
    stream.add_argument("--strict-ctos-match", action="store_true")
    stream.add_argument("--disable-contact-email-fast-path", action="store_true")
    stream.add_argument("--stale-running-requeue-seconds", type=int, default=600)
    stream.add_argument("--ctos-worker-count", type=int, default=4)
    stream.add_argument("--businesslist-worker-count", type=int, default=4)
    stream.add_argument("--snov-worker-count", type=int, default=4)
    stream.add_argument("--manager-worker-count", type=int, default=32)
    stream.add_argument("--allow-no-manager-output", action="store_true")
    stream.add_argument("--snov-client-id", default="")
    stream.add_argument("--snov-client-secret", default="")
    stream.set_defaults(func=_cmd_streaming_run)

    sync_cookie = sub.add_parser(
        "sync-businesslist-cookie",
        help="连接 9222 浏览器，手动通过 cf 后同步运行期 Cookie",
    )
    sync_cookie.add_argument("--businesslist-cdp-url", default="http://127.0.0.1:9222")
    sync_cookie.add_argument("--businesslist-cf-cookies-file", default=DEFAULT_BUSINESSLIST_CF_COOKIE_FILE)
    sync_cookie.add_argument("--target-url", default=DEFAULT_TARGET_URL)
    sync_cookie.add_argument("--wait-seconds", type=int, default=600)
    sync_cookie.add_argument("--poll-seconds", type=float, default=2.0)
    sync_cookie.add_argument(
        "--businesslist-require-login",
        action="store_true",
        default=_env_bool("BUSINESSLIST_REQUIRE_LOGIN", True),
        help="同步 Cookie 时要求同时检测到登录态",
    )
    sync_cookie.add_argument(
        "--businesslist-login-probe-company-id",
        type=int,
        default=int(os.getenv("BUSINESSLIST_LOGIN_PROBE_COMPANY_ID", str(DEFAULT_LOGIN_PROBE_COMPANY_ID))),
        help="登录态探针用的 company_id",
    )
    sync_cookie.set_defaults(func=_cmd_sync_businesslist_cookie)
    return parser


def run_cli(argv: Sequence[str] | None = None) -> int:
    load_dotenv(".env")
    parser = _build_parser()
    args = parser.parse_args(argv)
    ensure_dir("output")
    try:
        return args.func(args)
    except Exception as exc:  # noqa: BLE001
        print(f"执行失败：{exc}")
        return 1
