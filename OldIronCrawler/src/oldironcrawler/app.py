from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import sys
import time

from oldironcrawler.bootstrap import raise_nofile_soft_limit
from oldironcrawler import console as console_module
from oldironcrawler.config import AppConfig, persist_llm_key, read_saved_llm_key
from oldironcrawler.extractor.llm_client import LlmConfigurationError, LlmTemporaryError, WebsiteLlmClient
from oldironcrawler.importer import choose_input_file, compute_rows_fingerprint, load_websites
from oldironcrawler.llm_errors import classify_llm_exception
from oldironcrawler.runner import run_crawl_session
from oldironcrawler.runtime.store import RuntimeStore


DEFAULT_SITE_CONCURRENCY = 64
DEFAULT_SITE_TIMEOUT_SECONDS = 180


@dataclass
class CrawlRunResult:
    exit_code: int
    delivery_path: Path
    failed_path: Path
    effective_key: str
    llm_base_url: str = ""


@dataclass(frozen=True)
class RuntimeConcurrencyBudget:
    site_concurrency: int
    llm_concurrency: int
    ai_email_concurrency: int
    page_concurrency: int
    page_worker_count: int
    page_host_limit: int


def run_interactive(project_root: Path | None = None, llm_key_override: str | None = None) -> int:
    project_root = (project_root or Path(__file__).resolve().parents[2]).resolve()
    _raise_nofile_soft_limit()
    current_key = _resolve_initial_llm_key(project_root, llm_key_override)
    config = _ensure_runtime_key_ready(project_root, current_key)
    input_path = choose_input_file(config.websites_dir)
    result = run_selected_input(project_root, config.llm_key, input_path, llm_base_url=config.llm_base_url)
    return result.exit_code


def run_selected_input(
    project_root: Path,
    current_key: str,
    input_path: Path,
    *,
    concurrency: int = DEFAULT_SITE_CONCURRENCY,
    site_timeout_seconds: int = DEFAULT_SITE_TIMEOUT_SECONDS,
    llm_base_url: str = "",
    collect_email_enabled: bool = True,
    collect_phone_enabled: bool = True,
    collect_company_name_enabled: bool = False,
    extract_representative_enabled: bool = False,
    search_representative_enabled: bool = False,
) -> CrawlRunResult:
    config, rows, current_key = _load_rows_with_llm_recovery(
        project_root,
        input_path,
        current_key,
        concurrency=concurrency,
        site_timeout_seconds=site_timeout_seconds,
        llm_base_url=llm_base_url,
        collect_email_enabled=collect_email_enabled,
        collect_phone_enabled=collect_phone_enabled,
        collect_company_name_enabled=collect_company_name_enabled,
        extract_representative_enabled=extract_representative_enabled,
        search_representative_enabled=search_representative_enabled,
    )
    artifact_stem = _build_artifact_stem(input_path)
    db_path = config.runtime_dir / f"{artifact_stem}.sqlite3"
    snapshot_path = config.runtime_dir / f"{artifact_stem}.snapshot.csv"
    success_path = config.delivery_dir / f"{artifact_stem}_success.csv"
    failed_path = config.delivery_dir / f"{artifact_stem}_failed.csv"
    store = RuntimeStore(db_path)
    try:
        exit_code, effective_key = _run_session_with_llm_recovery(
            project_root=project_root,
            input_path=input_path,
            rows=rows,
            config=config,
            store=store,
            delivery_path=snapshot_path,
            concurrency=concurrency,
            site_timeout_seconds=site_timeout_seconds,
            collect_email_enabled=collect_email_enabled,
            collect_phone_enabled=collect_phone_enabled,
            collect_company_name_enabled=collect_company_name_enabled,
            extract_representative_enabled=extract_representative_enabled,
            search_representative_enabled=search_representative_enabled,
        )
        from oldironcrawler.reporter import write_delivery_reports

        write_delivery_reports(
            store=store,
            success_path=success_path,
            failed_path=failed_path,
            include_company_name=collect_company_name_enabled,
            include_email=collect_email_enabled,
            include_phone=collect_phone_enabled,
            include_representative=extract_representative_enabled,
            include_searched_representative=search_representative_enabled,
        )
        return CrawlRunResult(
            exit_code=exit_code,
            delivery_path=success_path,
            failed_path=failed_path,
            effective_key=effective_key,
            llm_base_url=config.llm_base_url,
        )
    finally:
        store.close()


def _resolve_initial_llm_key(project_root: Path, llm_key_override: str | None) -> str:
    if str(llm_key_override or "").strip():
        return str(llm_key_override).strip()
    return read_saved_llm_key(project_root)


def _build_artifact_stem(input_path: Path) -> str:
    suffix = str(input_path.suffix or "").strip().lower().lstrip(".") or "txt"
    return f"{input_path.stem}-{suffix}"


def _load_runtime_config(project_root: Path, llm_key_override: str, *, llm_base_url: str = "") -> AppConfig:
    config = AppConfig.load(project_root, llm_key_override=llm_key_override)
    if str(llm_base_url or "").strip():
        config.llm_base_url = str(llm_base_url or "").strip()
    config.ensure_directories()
    config.validate()
    return config


def _ensure_runtime_key_ready(project_root: Path, current_key: str) -> AppConfig:
    while True:
        if not str(current_key or "").strip():
            current_key = console_module.prompt_runtime_llm_key()
        config = _load_runtime_config(project_root, current_key)
        _apply_runtime_preferences(
            config,
            concurrency=DEFAULT_SITE_CONCURRENCY,
            site_timeout_seconds=DEFAULT_SITE_TIMEOUT_SECONDS,
            collect_email_enabled=True,
            collect_phone_enabled=True,
        )
        try:
            _validate_llm_runtime(config)
            _persist_runtime_llm_key(project_root, config.llm_key)
            return config
        except (LlmConfigurationError, LlmTemporaryError) as exc:
            current_key = _recover_runtime_llm_key(current_key, exc)


def _load_rows_with_llm_recovery(
    project_root: Path,
    input_path: Path,
    current_key: str,
    *,
    concurrency: int,
    site_timeout_seconds: int,
    llm_base_url: str = "",
    collect_email_enabled: bool = True,
    collect_phone_enabled: bool = True,
    collect_company_name_enabled: bool = False,
    extract_representative_enabled: bool = False,
    search_representative_enabled: bool = False,
) -> tuple[AppConfig, list, str]:
    selected_llm_base_url = str(llm_base_url or "").strip()
    while True:
        config = _load_runtime_config(project_root, current_key, llm_base_url=selected_llm_base_url)
        _apply_runtime_preferences(
            config,
            concurrency=concurrency,
            site_timeout_seconds=site_timeout_seconds,
            collect_email_enabled=collect_email_enabled,
            collect_phone_enabled=collect_phone_enabled,
            collect_company_name_enabled=collect_company_name_enabled,
            extract_representative_enabled=extract_representative_enabled,
            search_representative_enabled=search_representative_enabled,
        )
        try:
            rows = _load_input_rows(config, input_path)
            if not rows:
                raise RuntimeError(f"输入文件没有识别到任何有效网站：{input_path.name}")
            return config, rows, current_key
        except (LlmConfigurationError, LlmTemporaryError) as exc:
            current_key = _recover_runtime_llm_key(current_key, exc)


def _run_session_with_llm_recovery(
    *,
    project_root: Path,
    input_path: Path,
    rows: list,
    config: AppConfig,
    store: RuntimeStore,
    delivery_path: Path,
    concurrency: int,
    site_timeout_seconds: int,
    collect_email_enabled: bool = True,
    collect_phone_enabled: bool = True,
    collect_company_name_enabled: bool = False,
    extract_representative_enabled: bool = False,
    search_representative_enabled: bool = False,
) -> tuple[int, str]:
    current_key = config.llm_key
    selected_llm_base_url = config.llm_base_url
    fingerprint = compute_rows_fingerprint(rows)
    key_already_validated = True
    while True:
        config = _load_runtime_config(project_root, current_key, llm_base_url=selected_llm_base_url)
        _apply_runtime_preferences(
            config,
            concurrency=concurrency,
            site_timeout_seconds=site_timeout_seconds,
            collect_email_enabled=collect_email_enabled,
            collect_phone_enabled=collect_phone_enabled,
            collect_company_name_enabled=collect_company_name_enabled,
            extract_representative_enabled=extract_representative_enabled,
            search_representative_enabled=search_representative_enabled,
        )
        if not key_already_validated:
            try:
                _validate_llm_runtime(config)
                selected_llm_base_url = config.llm_base_url
            except (LlmConfigurationError, LlmTemporaryError) as exc:
                current_key = _recover_runtime_llm_key(current_key, exc)
                continue
        key_already_validated = True
        _persist_runtime_llm_key(project_root, current_key)
        store.prepare_job(
            input_name=input_path.name,
            fingerprint=fingerprint,
            rows=rows,
        )
        store.reset_running_tasks()
        rerun_reset = store.reset_completed_job_for_rerun()
        progress = store.progress()
        if rerun_reset:
            print(f"检测到 {input_path.name} 上次已经跑完，本次已重置后重新跑。", flush=True)
        db_path = getattr(store, "_db_path", getattr(store, "db_path", ""))
        print(f"开始任务：file={input_path.name} total={progress['total']} db={db_path}", flush=True)
        print(_format_runtime_budget(config), flush=True)
        try:
            from oldironcrawler.ui.console import get_console
            from oldironcrawler.ui.crawl_view import CrawlView

            with CrawlView(get_console()):
                run_crawl_session(config, store, delivery_path)
            print(f"交付完成：{delivery_path}", flush=True)
            return 0, current_key
        except (LlmConfigurationError, LlmTemporaryError) as exc:
            current_key = _recover_runtime_llm_key(current_key, exc)
            key_already_validated = False


def _recover_runtime_llm_key(current_key: str, exc: Exception) -> str:
    failure = classify_llm_exception(exc)
    if failure is None:
        raise exc
    if failure.prompt_mode == "new_key":
        try:
            return console_module.prompt_runtime_llm_key(notice=failure.user_message)
        except TypeError:
            print(failure.user_message, flush=True)
            return console_module.prompt_runtime_llm_key()
    print(f"{failure.user_message} 程序将自动重试。", flush=True)
    time.sleep(_retry_wait_seconds(failure.retry_after_seconds))
    return current_key


def _retry_wait_seconds(retry_after_seconds: int | None) -> int:
    if retry_after_seconds is None:
        return 3
    return min(max(int(retry_after_seconds), 1), 30)


def _persist_runtime_llm_key(project_root: Path, llm_key: str) -> None:
    if _is_packaged_runtime():
        return
    persist_llm_key(project_root, llm_key)


def _is_packaged_runtime() -> bool:
    return bool(getattr(sys, "frozen", False))


def _validate_llm_runtime(config: AppConfig) -> None:
    # 只有一个固定入口，不再测速选节点：对该入口做一次 ping，校验 Key 与连通性即可。
    client = _build_llm_client(config)
    try:
        client.ping()
    except Exception as exc:  # noqa: BLE001
        _raise_classified_llm_validation_error(exc)
    finally:
        client.close()
    print(f"LLM API 入口已就绪：{config.llm_base_url}", flush=True)


def _raise_classified_llm_validation_error(exc: Exception) -> None:
    failure = classify_llm_exception(exc)
    if failure is None:
        raise exc
    if failure.prompt_mode == "new_key":
        raise LlmConfigurationError(failure.user_message, failure=failure) from exc
    raise LlmTemporaryError(failure.user_message, failure=failure) from exc


def _build_llm_client(config: AppConfig) -> WebsiteLlmClient:
    return WebsiteLlmClient(
        api_key=config.llm_key,
        base_url=config.llm_base_url,
        model=config.llm_model,
        api_style=config.llm_api_style,
        reasoning_effort=config.llm_reasoning_effort,
        proxy_url=config.proxy_url,
        timeout_seconds=config.request_timeout_seconds * 2,
        concurrency_limit=config.llm_concurrency,
    )


def _apply_runtime_preferences(
    config: AppConfig,
    *,
    concurrency: int,
    site_timeout_seconds: int,
    collect_email_enabled: bool | None = None,
    collect_phone_enabled: bool | None = None,
    collect_company_name_enabled: bool | None = None,
    extract_representative_enabled: bool | None = None,
    search_representative_enabled: bool | None = None,
) -> None:
    budget = _derive_runtime_concurrency_budget(concurrency)
    bounded_timeout = min(max(int(site_timeout_seconds), 60), 600)
    config.llm_concurrency = budget.llm_concurrency
    config.ai_email_concurrency = max(int(getattr(config, "ai_email_concurrency", budget.ai_email_concurrency) or 1), 1)
    config.site_concurrency = budget.site_concurrency
    config.page_concurrency = budget.page_concurrency
    config.page_worker_count = budget.page_worker_count
    config.page_host_limit = budget.page_host_limit
    # 全局唯一并发：AI 搜法人池也跟随同一个并发数，
    # 避免出现“站点并发设了，但搜法人池还停在独立的小值”这种隐藏瓶颈。
    config.search_representative_concurrency = budget.site_concurrency
    config.total_wait_seconds = float(bounded_timeout)
    # None 表示沿用 .env 里的默认值；菜单跑任务时才用勾选值覆盖这些开关。
    if collect_email_enabled is not None:
        config.collect_email_enabled = bool(collect_email_enabled)
    if collect_phone_enabled is not None:
        config.collect_phone_enabled = bool(collect_phone_enabled)
    if collect_company_name_enabled is not None:
        config.collect_company_name_enabled = bool(collect_company_name_enabled)
    if extract_representative_enabled is not None:
        config.extract_representative_enabled = bool(extract_representative_enabled)
    if search_representative_enabled is not None:
        config.search_representative_enabled = bool(search_representative_enabled)


def _derive_runtime_concurrency_budget(concurrency: int) -> RuntimeConcurrencyBudget:
    runtime_concurrency = min(max(int(concurrency), 1), 64)
    return RuntimeConcurrencyBudget(
        site_concurrency=runtime_concurrency,
        # 大模型并发按站点并发放大 3 倍（每家约调 3 次大模型），上限 64；
        # 否则 AI 搜法人的大模型调用会排在站点抽取后面、排队超时被丢弃。
        llm_concurrency=min(runtime_concurrency * 3, 64),
        ai_email_concurrency=8,
        page_concurrency=runtime_concurrency,
        page_worker_count=runtime_concurrency,
        page_host_limit=runtime_concurrency,
    )


def _format_runtime_budget(config: AppConfig) -> str:
    return (
        "运行预算："
        f"站点并发={config.site_concurrency}，"
        f"LLM并发={config.llm_concurrency}，"
        f"AI邮箱并发={config.ai_email_concurrency}，"
        f"公共探测批量={config.page_concurrency}，"
        f"全局抓页线程={config.page_worker_count}，"
        f"同主机抓页上限={config.page_host_limit}，"
        f"单站超时={int(config.total_wait_seconds)}秒"
    )


def _load_input_rows(config: AppConfig, input_path: Path):
    if input_path.suffix.lower() not in {".csv", ".xlsx"}:
        return load_websites(
            input_path,
            collect_company_name_enabled=bool(getattr(config, "collect_company_name_enabled", False)),
        )
    llm = _build_llm_client(config)
    try:
        collect_company = bool(getattr(config, "collect_company_name_enabled", False))
        return load_websites(
            input_path,
            website_column_picker=llm.pick_website_column,
            collect_company_name_enabled=collect_company,
            company_column_picker=llm.pick_company_name_column if collect_company else None,
        )
    finally:
        llm.close()


def _raise_nofile_soft_limit(target: int = 65536) -> None:
    raise_nofile_soft_limit(target)
