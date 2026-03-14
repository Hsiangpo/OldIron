from __future__ import annotations

import argparse
import os
from datetime import datetime
from pathlib import Path

from ..config import PipelineSettings
from ..pipeline import run_pipeline


DEFAULT_OUTPUT_DIR = "output"
DEFAULT_LLM_BASE_URL = "https://api.gpteamservices.com/v1"
DEFAULT_LLM_MODEL = "gpt-5.1-codex-mini"
DEFAULT_LLM_REASONING_EFFORT = "medium"


def main() -> None:
    args = _parse_args()
    input_path = _resolve_input_path(args)
    output_base = Path(args.output_dir)
    run_dir = Path(args.run_dir) if args.run_dir else _build_run_dir(output_base)

    llm_api_key = args.llm_api_key or os.environ.get("LLM_API_KEY")
    llm_base_url = (
        args.llm_base_url or os.environ.get("LLM_BASE_URL") or DEFAULT_LLM_BASE_URL
    )
    llm_model = args.llm_model or os.environ.get("LLM_MODEL") or DEFAULT_LLM_MODEL
    llm_reasoning_effort = (
        args.llm_reasoning_effort
        or os.environ.get("LLM_REASONING_EFFORT")
        or DEFAULT_LLM_REASONING_EFFORT
    )
    if args.use_llm is None:
        use_llm = _env_bool("SITE_USE_LLM", False)
    else:
        use_llm = bool(args.use_llm)
    if use_llm and not llm_api_key:
        raise SystemExit("已启用 LLM 模式，需要配置 LLM_API_KEY")
    if not llm_api_key:
        llm_api_key = ""
    snov_extension_selector = args.snov_extension_selector or os.environ.get(
        "SNOV_EXTENSION_SELECTOR"
    )
    snov_extension_token = args.snov_extension_token or os.environ.get(
        "SNOV_EXTENSION_TOKEN"
    )
    snov_extension_fingerprint = args.snov_extension_fingerprint or os.environ.get(
        "SNOV_EXTENSION_FINGERPRINT"
    )
    snov_extension_cdp_host = args.snov_extension_cdp_host or os.environ.get(
        "SNOV_EXTENSION_CDP_HOST"
    )
    snov_extension_cdp_port = args.snov_extension_cdp_port or os.environ.get(
        "SNOV_EXTENSION_CDP_PORT"
    )
    if args.snov_extension_only is None:
        snov_extension_only = _env_bool("SNOV_EXTENSION_ONLY", True)
    else:
        snov_extension_only = bool(args.snov_extension_only)
    extension_ready = bool(
        (snov_extension_selector and snov_extension_token) or snov_extension_cdp_port
    )
    if not extension_ready:
        print("警告：未找到 Snov 扩展配置，邮箱将跳过 Snov 预取。")  # pragma: no cover

    llm_concurrency = max(1, int(args.llm_concurrency))
    keyword = args.keyword or os.environ.get("SITE_KEYWORD")
    keyword = keyword.strip() if isinstance(keyword, str) and keyword.strip() else None
    keyword_filter_enabled = (
        bool(args.keyword_filter) if args.keyword_filter is not None else bool(keyword)
    )
    firecrawl_extract_enabled = (
        bool(args.firecrawl_extract)
        if args.firecrawl_extract is not None
        else _env_bool("FIRECRAWL_EXTRACT", False)
    )
    if not use_llm:
        keyword_filter_enabled = False
        firecrawl_extract_enabled = False
    firecrawl_keys_path = (
        args.firecrawl_keys_path
        or os.environ.get("FIRECRAWL_KEYS_PATH")
        or str(Path(DEFAULT_OUTPUT_DIR) / "firecrawl_keys.txt")
    )
    firecrawl_base_url = args.firecrawl_base_url or os.environ.get("FIRECRAWL_BASE_URL")
    firecrawl_extract_max_urls = int(
        args.firecrawl_extract_max_urls
        if args.firecrawl_extract_max_urls is not None
        else _env_int("FIRECRAWL_EXTRACT_MAX_URLS", 6)
    )
    firecrawl_key_per_limit = int(
        args.firecrawl_key_per_limit
        if args.firecrawl_key_per_limit is not None
        else _env_int("FIRECRAWL_KEY_PER_LIMIT", 2)
    )
    firecrawl_key_wait_seconds = int(
        args.firecrawl_key_wait_seconds
        if args.firecrawl_key_wait_seconds is not None
        else _env_int("FIRECRAWL_KEY_WAIT_SECONDS", 20)
    )
    settings = PipelineSettings(
        input_path=input_path,
        output_base_dir=output_base,
        run_dir=run_dir,
        concurrency=args.concurrency,
        llm_concurrency=llm_concurrency,
        max_pages=args.max_pages,
        max_rounds=args.max_rounds,
        max_sites=args.max_sites,
        page_timeout=args.page_timeout,
        site_timeout_seconds=(args.site_timeout if args.site_timeout > 0 else None),
        max_content_chars=args.max_content_chars,
        save_pages=args.save_pages,
        resume=args.resume,
        llm_api_key=llm_api_key,
        llm_base_url=llm_base_url,
        llm_model=llm_model,
        llm_temperature=args.llm_temperature,
        llm_max_output_tokens=args.llm_max_output_tokens,
        llm_reasoning_effort=llm_reasoning_effort,
        use_llm=use_llm,
        crawler_reset_every=args.crawler_reset_every,
        snov_extension_selector=snov_extension_selector,
        snov_extension_token=snov_extension_token,
        snov_extension_fingerprint=snov_extension_fingerprint,
        snov_extension_cdp_host=snov_extension_cdp_host,
        snov_extension_cdp_port=int(snov_extension_cdp_port)
        if snov_extension_cdp_port
        else None,
        snov_extension_only=snov_extension_only,
        keyword=keyword,
        keyword_filter_enabled=keyword_filter_enabled,
        keyword_min_confidence=float(args.keyword_min_confidence),
        email_max_per_domain=args.email_max_per_domain,
        email_details_limit=args.email_details_limit,
        pdf_max_pages=args.pdf_max_pages,
        firecrawl_keys_path=Path(firecrawl_keys_path) if firecrawl_keys_path else None,
        firecrawl_base_url=firecrawl_base_url,
        firecrawl_extract_enabled=firecrawl_extract_enabled,
        firecrawl_extract_max_urls=firecrawl_extract_max_urls,
        firecrawl_key_per_limit=firecrawl_key_per_limit,
        firecrawl_key_wait_seconds=firecrawl_key_wait_seconds,
    )

    import asyncio

    asyncio.run(run_pipeline(settings))


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Official site extraction pipeline")
    parser.add_argument("--input", help="Input file (csv/json/jsonl) with website URLs")
    parser.add_argument(
        "--google-map-output",
        help="Optional google_map output dir to search for inputs",
    )
    parser.add_argument("--output-dir", default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--run-dir", help="Use an existing run dir (for resume)")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--concurrency", type=int, default=10)
    parser.add_argument(
        "--llm-concurrency",
        type=int,
        default=16,
        help="LLM 并发（建议 1-16；过大可能触发限流）",
    )
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--max-rounds", type=int, default=3)
    parser.add_argument("--max-sites", type=int, default=None)
    parser.add_argument("--page-timeout", type=int, default=20000)
    parser.add_argument(
        "--site-timeout", type=float, default=_env_float("SITE_TIMEOUT_SECONDS", 0)
    )
    parser.add_argument(
        "--crawler-reset-every", type=int, default=_env_int("CRAWLER_RESET_EVERY", 0)
    )
    parser.add_argument("--max-content-chars", type=int, default=20000)
    parser.add_argument("--save-pages", action="store_true")
    parser.add_argument("--email-max-per-domain", type=int, default=0)
    parser.add_argument("--email-details-limit", type=int, default=80)
    parser.add_argument("--pdf-max-pages", type=int, default=4)
    parser.add_argument("--keyword", help="关键词过滤：仅处理与关键词匹配的站点")
    parser.add_argument(
        "--keyword-filter",
        dest="keyword_filter",
        action="store_true",
        help="启用关键词过滤",
    )
    parser.add_argument(
        "--no-keyword-filter",
        dest="keyword_filter",
        action="store_false",
        help="禁用关键词过滤",
    )
    parser.set_defaults(keyword_filter=None)
    parser.add_argument(
        "--keyword-min-confidence",
        type=float,
        default=_env_float("KEYWORD_MIN_CONFIDENCE", 0.6),
    )

    parser.add_argument("--firecrawl-keys-path")
    parser.add_argument("--firecrawl-base-url")
    parser.add_argument(
        "--firecrawl-extract", dest="firecrawl_extract", action="store_true"
    )
    parser.add_argument(
        "--no-firecrawl-extract", dest="firecrawl_extract", action="store_false"
    )
    parser.set_defaults(firecrawl_extract=None)
    parser.add_argument("--firecrawl-extract-max-urls", type=int)
    parser.add_argument("--firecrawl-key-per-limit", type=int)
    parser.add_argument("--firecrawl-key-wait-seconds", type=int)

    parser.add_argument("--use-llm", dest="use_llm", action="store_true", help="启用 LLM 辅助（默认关闭，仅规则提取）")
    parser.add_argument("--rules-only", dest="use_llm", action="store_false", help="仅规则提取，不调用 LLM")
    parser.set_defaults(use_llm=None)
    parser.add_argument("--llm-api-key")
    parser.add_argument("--llm-base-url")
    parser.add_argument("--llm-model")
    parser.add_argument(
        "--llm-temperature", type=float, default=_env_float("LLM_TEMPERATURE", 0.0)
    )
    parser.add_argument(
        "--llm-max-output-tokens",
        type=int,
        default=_env_int("LLM_MAX_OUTPUT_TOKENS", 1200),
    )
    parser.add_argument(
        "--llm-reasoning-effort", default=os.environ.get("LLM_REASONING_EFFORT")
    )
    parser.add_argument("--snov-extension-selector", dest="snov_extension_selector")
    parser.add_argument("--snov-extension-token", dest="snov_extension_token")
    parser.add_argument(
        "--snov-extension-fingerprint", dest="snov_extension_fingerprint"
    )
    parser.add_argument("--snov-extension-cdp-host", dest="snov_extension_cdp_host")
    parser.add_argument("--snov-extension-cdp-port", dest="snov_extension_cdp_port")
    parser.add_argument(
        "--snov-extension-only", dest="snov_extension_only", action="store_true"
    )
    parser.add_argument("--snov-api", dest="snov_extension_only", action="store_false")
    parser.set_defaults(snov_extension_only=None)

    return parser.parse_args()


def _build_run_dir(base_dir: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return base_dir / stamp


def _resolve_input_path(args: argparse.Namespace) -> Path:
    if args.input:
        return Path(args.input)
    if args.google_map_output:
        candidate = _find_input_from_google_map(Path(args.google_map_output))
        if candidate:
            return candidate
    raise SystemExit("需要提供 --input，或使用 --google-map-output 指定有效文件")


def _find_input_from_google_map(root: Path) -> Path | None:
    if not root.exists():
        return None
    preferred = [
        "places_with_websites.jsonl",
        "places_with_websites.json",
        "places_with_websites.csv",
        "places.json",
        "places_new.json",
    ]
    for name in preferred:
        path = root / name
        if path.exists():
            return path
    latest_run = _find_latest_run_dir(root)
    if latest_run:
        for name in preferred:
            path = latest_run / name
            if path.exists():
                return path
    return None


def _find_latest_run_dir(root: Path) -> Path | None:
    run_dirs = [p for p in root.iterdir() if p.is_dir()]
    if not run_dirs:
        return None
    return max(run_dirs, key=lambda p: p.stat().st_mtime)


def _env_int(key: str, default: int) -> int:
    value = os.environ.get(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        return default


def _env_float(key: str, default: float) -> float:
    value = os.environ.get(key)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        return default


def _env_bool(key: str, default: bool) -> bool:
    value = os.environ.get(key)
    if value is None:
        return default
    return str(value).strip().lower() in {"1", "true", "yes", "on"}


if __name__ == "__main__":
    main()
