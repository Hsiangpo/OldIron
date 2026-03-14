"""三线并发流式主流程。"""

from __future__ import annotations

import re
import threading
import time
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol
from typing import Sequence

import requests

from malaysia_crawler.businesslist.cdp_crawler import BusinessListBlockedError
from malaysia_crawler.businesslist.cf_crawler import BusinessListCFCrawler
from malaysia_crawler.businesslist.cookie_sync import DEFAULT_TARGET_URL
from malaysia_crawler.businesslist.cookie_sync import sync_cookie_from_cdp
from malaysia_crawler.businesslist.crawler import BusinessListCrawler
from malaysia_crawler.businesslist.models import BusinessListCompany
from malaysia_crawler.ctos_directory.crawler import CTOSDirectoryCrawler
from malaysia_crawler.snov.client import SnovClient
from malaysia_crawler.snov.client import extract_domain
from malaysia_crawler.streaming.store import PipelineStore
from malaysia_crawler.streaming.store_manager import ManagerTask
from malaysia_crawler.streaming.store import SnovTask


def _normalize_company_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _normalize_registration_code(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", value.lower())


def _normalize_prefixes(raw: str) -> list[str]:
    cleaned = [ch.lower() for ch in raw if ch.isalnum()]
    return list(dict.fromkeys(cleaned))


def _merge_emails(contact_email: str, snov_emails: list[str]) -> list[str]:
    merged: list[str] = []
    head = contact_email.strip().lower()
    if head:
        merged.append(head)
    for item in snov_emails:
        value = item.strip().lower()
        if value:
            merged.append(value)
    # 中文注释：去重并保持顺序。
    return list(dict.fromkeys(merged))


def _pick_contact_phone(company: BusinessListCompany) -> str:
    for raw in company.contact_numbers:
        value = str(raw).strip()
        if value:
            return value
    for member in company.employees:
        role = str(member.get("role", "")).strip().upper().replace(" ", "")
        if role != "DIRECTOR":
            continue
        phone = str(member.get("phone", "")).strip()
        if phone:
            return phone
    return ""


class _BusinessListSource(Protocol):
    def fetch_company_profile(self, company_id: int) -> BusinessListCompany | None: ...

    def close(self) -> None: ...


class _ManagerAgentSource(Protocol):
    def enrich_manager(
        self,
        *,
        company_name: str,
        domain: str,
        candidate_pool: list[str],
        tried_urls: list[str],
    ) -> "_ManagerResult": ...


@dataclass(slots=True)
class _ManagerResult:
    success: bool
    manager_name: str
    manager_role: str
    evidence_url: str
    evidence_quote: str
    candidate_pool: list[str]
    tried_urls: list[str]
    error_code: str
    error_text: str
    retry_after: float


@dataclass(slots=True)
class StreamingPipelineConfig:
    db_path: Path
    ctos_prefixes: str = "0123456789abcdefghijklmnopqrstuvwxyz"
    businesslist_start_id: int = 1
    businesslist_end_id: int = 500000
    log_interval_seconds: float = 20.0
    retry_sleep_seconds: float = 2.0
    snov_max_retries: int = 3
    ctos_transient_retry_limit: int = 8
    businesslist_transient_retry_limit: int = 8
    businesslist_cf_block_retry_limit: int = 4
    businesslist_cf_backoff_base_seconds: float = 3.0
    businesslist_cf_backoff_cap_seconds: float = 30.0
    backoff_cap_seconds: float = 120.0
    zero_queue_guard_min_hits: int = 200
    strict_ctos_match: bool = False
    contact_email_fast_path: bool = True
    stale_running_requeue_seconds: int = 600
    require_manager_for_output: bool = True
    manager_enrich_max_rounds: int = 3
    manager_enrich_retry_backoff_seconds: float = 30.0
    ctos_worker_count: int = 4
    businesslist_worker_count: int = 4
    snov_worker_count: int = 4
    manager_worker_count: int = 32
    businesslist_require_login: bool = True
    businesslist_login_probe_company_id: int = 62731


def _extract_http_status(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    if response is None:
        return None
    status = getattr(response, "status_code", None)
    if isinstance(status, int):
        return status
    return None


def _extract_retry_after_seconds(exc: Exception) -> int | None:
    response = getattr(exc, "response", None)
    if response is None:
        return None
    headers = getattr(response, "headers", None)
    if headers is None:
        return None
    raw = str(headers.get("Retry-After", "")).strip()
    if raw.isdigit():
        return max(int(raw), 1)
    return None


def _is_transient_status(status_code: int) -> bool:
    return status_code in {403, 408, 409, 425, 429, 500, 502, 503, 504}


def _is_transient_error(exc: Exception) -> bool:
    if isinstance(exc, BusinessListBlockedError):
        return True
    if isinstance(exc, (requests.exceptions.Timeout, requests.exceptions.ConnectionError)):
        return True
    status_code = _extract_http_status(exc)
    if status_code is None:
        return False
    return _is_transient_status(status_code)


def _backoff_seconds(*, base: float, attempt: int, cap: float) -> float:
    n = max(attempt, 1)
    return min(base * (2 ** (n - 1)), cap)


def _is_transient_store_error(exc: Exception) -> bool:
    if not isinstance(exc, sqlite3.OperationalError):
        return False
    message = str(exc).strip().lower()
    transient_tokens = (
        "disk i/o error",
        "database is locked",
        "database table is locked",
        "database schema is locked",
        "database is busy",
        "unable to open database file",
    )
    return any(token in message for token in transient_tokens)


class MalaysiaStreamingPipeline:
    """CTOS -> BusinessList -> Snov 并发流式运行。"""

    def __init__(
        self,
        *,
        config: StreamingPipelineConfig,
        ctos_crawler: CTOSDirectoryCrawler | Sequence[CTOSDirectoryCrawler],
        businesslist_crawler: _BusinessListSource | Sequence[_BusinessListSource],
        snov_client: SnovClient | Sequence[SnovClient],
        manager_agent: _ManagerAgentSource | None = None,
    ) -> None:
        self.config = config
        self._ctos_crawlers = self._normalize_workers(ctos_crawler)
        self._businesslist_crawlers = self._normalize_workers(businesslist_crawler)
        self._snov_clients = self._normalize_workers(snov_client)
        self.ctos_crawler = self._ctos_crawlers[0]
        self.businesslist_crawler = self._businesslist_crawlers[0]
        self.snov_client = self._snov_clients[0]
        self.manager_agent = manager_agent
        self.store = PipelineStore(config.db_path)
        self.stop_event = threading.Event()
        self.shutdown_event = threading.Event()
        self._threads: list[threading.Thread] = []
        self._fatal_lock = threading.Lock()
        self._fatal_error: str = ""
        self._last_stats: dict[str, int | str] | None = None
        self._bl_cf_wait_event = threading.Event()
        self._instance_lock_path: Path | None = None
        self._last_guard_hint_hits = -1
        self._last_manager_stale_requeue_ts = 0.0
        self._manager_infra_lock = threading.Lock()
        self._manager_infra_consecutive = 0
        self._manager_infra_pause_until_ts = 0.0
        self._manager_infra_last_notice_ts = 0.0
        self._manager_infra_last_reason = ""
        self._manager_infra_last_source = ""
        self._last_cookie_auto_sync_ts = 0.0
        self._last_cookie_auto_sync_error_ts = 0.0

    def _normalize_workers(self, source: object) -> list:
        if isinstance(source, Sequence) and not isinstance(source, (str, bytes)):
            items = [item for item in source if item is not None]
            if items:
                return list(items)
        return [source]

    def _is_pid_running(self, pid: int) -> bool:
        if pid <= 0:
            return False
        try:
            os.kill(pid, 0)
            return True
        except PermissionError:
            return True
        except OSError:
            return False

    def _acquire_instance_lock(self) -> None:
        lock_path = self.config.db_path.with_suffix(".pipeline.lock")
        current_pid = os.getpid()
        if lock_path.exists():
            raw = lock_path.read_text(encoding="utf-8").strip()
            old_pid = int(raw) if raw.isdigit() else 0
            if self._is_pid_running(old_pid) and old_pid != current_pid:
                raise RuntimeError(f"检测到已有主流程进程在运行（PID={old_pid}），请先停止旧进程。")
            lock_path.unlink(missing_ok=True)
        lock_path.write_text(str(current_pid), encoding="utf-8")
        self._instance_lock_path = lock_path

    def _release_instance_lock(self) -> None:
        if self._instance_lock_path is None:
            return
        self._instance_lock_path.unlink(missing_ok=True)
        self._instance_lock_path = None

    def _set_fatal(self, message: str) -> None:
        if self.shutdown_event.is_set():
            return
        with self._fatal_lock:
            if self._fatal_error:
                return
            self._fatal_error = message
            print(f"[FATAL] {message}")
        self.stop_event.set()

    def _get_fatal(self) -> str:
        with self._fatal_lock:
            return self._fatal_error

    def _run_worker_guard(self, worker_name: str, fn, *fn_args: object) -> None:
        store_failures = 0
        while not self.stop_event.is_set():
            try:
                fn(*fn_args)
                return
            except Exception as exc:  # noqa: BLE001
                if self.stop_event.is_set():
                    return
                if _is_transient_store_error(exc):
                    store_failures += 1
                    delay = _backoff_seconds(
                        base=max(self.config.retry_sleep_seconds, 2.0),
                        attempt=store_failures,
                        cap=self.config.backoff_cap_seconds,
                    )
                    try:
                        self.store.reconnect()
                        reconnect_msg = "已重连数据库"
                    except Exception as reconnect_exc:  # noqa: BLE001
                        reconnect_msg = f"重连失败：{type(reconnect_exc).__name__}: {reconnect_exc}"
                    print(
                        f"[{worker_name}] 存储暂时异常({store_failures})，"
                        f"{delay:.1f}s 后重试：{type(exc).__name__}: {exc}；{reconnect_msg}"
                    )
                    time.sleep(delay)
                    continue
                self._set_fatal(f"{worker_name} 未处理异常：{type(exc).__name__}: {exc}")
                return

    def _ctos_worker(self, worker_index: int = 0, worker_count: int = 1) -> None:
        prefixes_all = _normalize_prefixes(self.config.ctos_prefixes)
        if not prefixes_all:
            raise ValueError("ctos_prefixes 为空。")
        current_count = max(worker_count, 1)
        current_index = max(min(worker_index, current_count - 1), 0)
        prefixes = [
            prefix
            for idx, prefix in enumerate(prefixes_all)
            if idx % current_count == current_index
        ]
        if not prefixes:
            return
        cursor_key = f"ctos_w{current_index + 1}_of_{current_count}"
        prefix_key = "".join(prefixes_all)
        prefix_index, next_page = self.store.load_ctos_cursor(prefix_key, cursor_key)
        crawler = self._ctos_crawlers[current_index % len(self._ctos_crawlers)]
        failures = 0
        paused = False
        while not self.stop_event.is_set():
            if self._bl_cf_wait_event.is_set():
                if not paused:
                    print("[CTOS] 暂停抓取：等待 BusinessList 通过 cf 验证。")
                    paused = True
                time.sleep(max(self.config.retry_sleep_seconds, 2.0))
                continue
            if paused:
                print("[CTOS] 恢复抓取：BusinessList 已解除 cf 拦截。")
                paused = False
            if prefix_index >= len(prefixes):
                prefix_index = 0
                next_page = 1
            prefix = prefixes[prefix_index]
            try:
                page_data = crawler.fetch_list_page(prefix, next_page)
            except Exception as exc:  # noqa: BLE001
                if self.stop_event.is_set():
                    return
                if not _is_transient_error(exc):
                    self._set_fatal(f"CTOS 不可恢复异常：{type(exc).__name__}: {exc}")
                    return
                failures += 1
                if failures >= self.config.ctos_transient_retry_limit:
                    self._set_fatal(f"CTOS 连续失败 {failures} 次：{type(exc).__name__}: {exc}")
                    return
                delay = _backoff_seconds(
                    base=self.config.retry_sleep_seconds,
                    attempt=failures,
                    cap=self.config.backoff_cap_seconds,
                )
                print(f"[CTOS] 暂时失败({failures})，{delay:.1f}s 后重试：{type(exc).__name__}: {exc}")
                time.sleep(delay)
                continue
            failures = 0
            if not page_data.companies:
                prefix_index += 1
                next_page = 1
                self.store.save_ctos_cursor(prefix_index, next_page, cursor_key)
                continue
            for item in page_data.companies:
                normalized = _normalize_company_name(item.company_name)
                if not normalized:
                    continue
                self.store.upsert_ctos_company(
                    normalized_name=normalized,
                    company_name=item.company_name,
                    registration_no=item.registration_no,
                    prefix=prefix,
                    page=next_page,
                )
                self.store.enqueue_from_businesslist_if_ready(normalized)
            next_page += 1
            self.store.save_ctos_cursor(prefix_index, next_page, cursor_key)

    def _businesslist_worker(self, worker_index: int = 0) -> None:
        if worker_index == 0 and not self.config.strict_ctos_match:
            backfilled = self.store.backfill_unmatched_businesslist_to_queue(batch_size=2000)
            if backfilled > 0:
                print(f"[回补] 已将历史未命中CTOS但可用的 {backfilled} 条记录补入 Snov 队列。")
        if worker_index == 0 and not self.config.require_manager_for_output:
            backfilled_no_manager = self.store.backfill_no_manager_to_queue(batch_size=2000)
            if backfilled_no_manager > 0:
                print(f"[回补] 已将历史无管理人但有域名的 {backfilled_no_manager} 条记录补入 Snov 队列。")
        crawler = self._businesslist_crawlers[worker_index % len(self._businesslist_crawlers)]
        idle_notice_ts = 0.0
        while not self.stop_event.is_set():
            company_id = self.store.claim_next_businesslist_id(
                start_id=self.config.businesslist_start_id,
                end_id=self.config.businesslist_end_id,
            )
            if company_id is None:
                now_ts = time.time()
                if worker_index == 0 and now_ts - idle_notice_ts >= 60.0:
                    idle_notice_ts = now_ts
                    next_id = self.store.get_next_businesslist_id(self.config.businesslist_start_id)
                    print(
                        f"[BusinessList] 已到扫描上限，当前游标={next_id} "
                        f"结束ID={self.config.businesslist_end_id}，worker 空闲。"
                    )
                time.sleep(self.config.retry_sleep_seconds)
                continue
            if self.store.is_businesslist_scanned(company_id):
                continue

            failures = 0
            cf_retries = 0
            cf_blocked_skip = False
            profile: BusinessListCompany | None = None
            while not self.stop_event.is_set():
                try:
                    profile = crawler.fetch_company_profile(company_id)
                    break
                except BusinessListBlockedError as exc:
                    if self.stop_event.is_set():
                        return
                    cf_retries += 1
                    blocked_reason = str(exc)
                    asn_blocked = "error_1005_asn_blocked" in blocked_reason
                    self._refresh_businesslist_cookies()
                    if cf_retries >= max(self.config.businesslist_cf_block_retry_limit, 1):
                        self.store.mark_businesslist_scan(
                            company_id=company_id,
                            normalized_name="",
                            company_name="",
                            domain="",
                            company_manager="",
                            contact_email="",
                            status="error:cf_blocked",
                            contact_phone="",
                        )
                        print(
                            f"[BusinessList][cf] id={company_id} 连续拦截 {cf_retries} 次，"
                            f"标记 error:cf_blocked 并跳过。原因={blocked_reason}"
                        )
                        cf_blocked_skip = True
                        break
                    delay = _backoff_seconds(
                        base=max(
                            self.config.businesslist_cf_backoff_base_seconds,
                            self.config.retry_sleep_seconds,
                            1.0,
                        ),
                        attempt=cf_retries,
                        cap=max(self.config.businesslist_cf_backoff_cap_seconds, 1.0),
                    )
                    if cf_retries == 1 or cf_retries % 3 == 0:
                        if asn_blocked:
                            print(
                                f"[BusinessList][cf] id={company_id} 命中 Cloudflare 1005（出口 ASN 被封），"
                                "请切换代理/出口后重试。"
                            )
                        else:
                            print(
                                f"[需要人工过cf] BusinessList id={company_id} 被 cf 拦截。"
                                f"请在浏览器打开 https://www.businesslist.my/company/{company_id} 完成验证，"
                                "然后执行 python run.py Cookie 自动更新 cookies/businesslist.cf.cookie.txt。"
                            )
                    print(
                        f"[BusinessList][cf] 第{cf_retries}次拦截，{delay:.1f}s 后重试 id={company_id} "
                        f"原因={blocked_reason}"
                    )
                    time.sleep(delay)
                    continue
                except Exception as exc:  # noqa: BLE001
                    if self.stop_event.is_set():
                        return
                    if not _is_transient_error(exc):
                        self._set_fatal(f"BusinessList 不可恢复异常：id={company_id} {type(exc).__name__}: {exc}")
                        return
                    failures += 1
                    if failures >= self.config.businesslist_transient_retry_limit:
                        self._set_fatal(
                            f"BusinessList 连续失败 {failures} 次：id={company_id} {type(exc).__name__}: {exc}"
                        )
                        return
                    delay = _backoff_seconds(
                        base=self.config.retry_sleep_seconds,
                        attempt=failures,
                        cap=self.config.backoff_cap_seconds,
                    )
                    print(
                        f"[BusinessList] 暂时失败({failures}) id={company_id}，"
                        f"{delay:.1f}s 后重试：{type(exc).__name__}: {exc}"
                    )
                    time.sleep(delay)
                    continue
            if self.stop_event.is_set():
                return

            if cf_blocked_skip:
                continue

            if profile is None:
                self.store.mark_businesslist_scan(
                    company_id=company_id,
                    normalized_name="",
                    company_name="",
                    domain="",
                    company_manager="",
                    contact_email="",
                    status="miss",
                    contact_phone="",
                )
                continue

            normalized = _normalize_company_name(profile.company_name)
            registration_norm = _normalize_registration_code(profile.registration_code)
            domain = extract_domain(profile.website_url)
            manager = profile.company_manager.strip()
            contact_email = profile.contact_email.strip().lower()
            contact_phone = _pick_contact_phone(profile)
            ctos_matched = self.store.has_ctos_name(normalized)
            if not ctos_matched and registration_norm:
                ctos_matched = self.store.has_ctos_registration(registration_norm)
            status = "queued"
            if not normalized:
                status = "invalid_name"
            elif not ctos_matched and self.config.strict_ctos_match:
                status = "not_in_ctos"
            elif not domain:
                status = "no_domain"
            elif not manager and self.config.require_manager_for_output:
                if self.manager_agent is not None:
                    status = "queued_manager_enrich"
                    self.store.enqueue_manager_task(
                        normalized_name=normalized,
                        company_name=profile.company_name.strip(),
                        domain=domain,
                        contact_email=contact_email,
                        company_id=company_id,
                        contact_phone=contact_phone,
                    )
                else:
                    status = "no_manager"
            else:
                if not ctos_matched:
                    status = "queued_without_ctos"
                elif not manager:
                    status = "queued_no_manager"
                self.store.enqueue_snov_task(
                    normalized_name=normalized,
                    company_name=profile.company_name.strip(),
                    domain=domain,
                    company_manager=manager,
                    contact_email=contact_email,
                    company_id=company_id,
                    contact_phone=contact_phone,
                )
            self.store.mark_businesslist_scan(
                company_id=company_id,
                normalized_name=normalized,
                company_name=profile.company_name.strip(),
                domain=domain,
                company_manager=manager,
                contact_email=contact_email,
                status=status,
                contact_phone=contact_phone,
            )

    def _refresh_businesslist_cookies(self) -> None:
        self._auto_sync_businesslist_cookie_if_possible()
        changed_any = False
        for crawler in self._businesslist_crawlers:
            refresher = getattr(crawler, "refresh_cookies_from_file", None)
            if not callable(refresher):
                continue
            try:
                changed = bool(refresher(force=False))
            except Exception as exc:  # noqa: BLE001
                print(f"[BusinessList][cf] 刷新 cookie 失败：{type(exc).__name__}: {exc}")
                continue
            if changed:
                changed_any = True
        if changed_any:
            print("[BusinessList][cf] 已从 cookie 文件刷新登录态。")

    def _auto_sync_businesslist_cookie_if_possible(self) -> None:
        now_ts = time.time()
        # 中文注释：避免每次拦截都连接 CDP，限制最小尝试间隔。
        if now_ts - self._last_cookie_auto_sync_ts < 30.0:
            return
        self._last_cookie_auto_sync_ts = now_ts
        cookie_file = ""
        for crawler in self._businesslist_crawlers:
            value = str(getattr(crawler, "cookies_file", "")).strip()
            if value:
                cookie_file = value
                break
        if not cookie_file:
            return
        try:
            sync_cookie_from_cdp(
                cdp_url="http://127.0.0.1:9222",
                output_file=cookie_file,
                target_url=DEFAULT_TARGET_URL,
                wait_seconds=2,
                poll_seconds=0.5,
                require_login=self.config.businesslist_require_login,
                login_probe_company_id=self.config.businesslist_login_probe_company_id,
            )
            print("[BusinessList][cf] 已自动从 9222 浏览器同步 Cookie。")
        except Exception as exc:  # noqa: BLE001
            # 中文注释：CDP 不可用时降噪，避免刷屏。
            if now_ts - self._last_cookie_auto_sync_error_ts >= 120.0:
                self._last_cookie_auto_sync_error_ts = now_ts
                print(f"[BusinessList][cf] 自动同步 Cookie 失败：{type(exc).__name__}: {exc}")

    def _manager_worker(self) -> None:
        if self.manager_agent is None:
            return
        self._try_requeue_stale_manager_tasks(force=True)
        while not self.stop_event.is_set():
            self._try_requeue_stale_manager_tasks(force=False)
            task = self.store.claim_manager_task()
            if task is None:
                time.sleep(1.0)
                continue
            self._process_manager_task(task)

    def _manager_global_pause_left_seconds(self) -> float:
        with self._manager_infra_lock:
            now_ts = time.time()
            if self._manager_infra_pause_until_ts <= now_ts:
                return 0.0
            return self._manager_infra_pause_until_ts - now_ts

    def _print_manager_global_pause_notice(self, pause_left: float) -> None:
        now_ts = time.time()
        source = ""
        reason = ""
        consecutive = 0
        with self._manager_infra_lock:
            if now_ts - self._manager_infra_last_notice_ts < 15.0:
                return
            self._manager_infra_last_notice_ts = now_ts
            source = self._manager_infra_last_source or "unknown"
            reason = self._manager_infra_last_reason or "unknown"
            consecutive = self._manager_infra_consecutive
        print(
            f"[Manager] 服务退避中：来源={source} 最近错误={reason} "
            f"连续异常={consecutive} 预计 {pause_left:.1f}s 后恢复。"
        )

    def _infer_manager_infra_source(self, reason_code: str) -> str:
        code = (reason_code or "").strip().lower()
        if code.startswith("firecrawl_") or code == "firecrawl_key_unavailable":
            return "firecrawl"
        if code in {"internalservererror", "apiconnectionerror", "apitimeouterror", "serviceunavailableerror"}:
            return "llm_gateway"
        return "unknown"

    def _register_manager_infra_error(self, reason_code: str) -> float:
        with self._manager_infra_lock:
            self._manager_infra_consecutive += 1
            self._manager_infra_last_reason = (reason_code or "unknown").strip().lower()
            self._manager_infra_last_source = self._infer_manager_infra_source(reason_code)
            base = max(self.config.manager_enrich_retry_backoff_seconds, 15.0)
            cap = max(self.config.backoff_cap_seconds, 120.0)
            delay = _backoff_seconds(
                base=base,
                attempt=self._manager_infra_consecutive,
                cap=cap,
            )
            self._manager_infra_pause_until_ts = max(
                self._manager_infra_pause_until_ts,
                time.time() + delay,
            )
            return delay

    def _clear_manager_infra_backoff(self) -> None:
        with self._manager_infra_lock:
            self._manager_infra_consecutive = 0
            self._manager_infra_pause_until_ts = 0.0
            self._manager_infra_last_reason = ""
            self._manager_infra_last_source = ""

    def _try_requeue_stale_manager_tasks(self, *, force: bool) -> None:
        if self.manager_agent is None:
            return
        now_ts = time.time()
        if not force and now_ts - self._last_manager_stale_requeue_ts < 30.0:
            return
        self._last_manager_stale_requeue_ts = now_ts
        recovered = self.store.requeue_stale_running_manager_tasks(
            older_than_seconds=self.config.stale_running_requeue_seconds
        )
        if recovered > 0:
            print(f"[Manager] 已回收 {recovered} 条超时 running 任务并重新入队。")

    def _default_manager_candidates(self, domain: str) -> list[str]:
        base = f"https://{domain.strip().lower()}"
        return [
            base + "/about",
            base + "/team",
            base + "/management",
            base + "/leadership",
            base + "/directors",
            base + "/board",
            base + "/company",
        ]

    def _is_manager_non_retryable(self, error_code: str) -> bool:
        code = (error_code or "").strip().lower()
        return code in {
            "no_candidate",
            "manager_not_found",
            "empty_pages",
            "invalid_domain",
            "firecrawl_401",
            "firecrawl_402",
        }

    def _normalize_manager_error_code(self, error_code: str, error_text: str) -> str:
        code = (error_code or "").strip()
        text = (error_text or "").strip().lower()
        if code.lower() == "runtimeerror":
            if "没有可用 firecrawl key" in text or "no available firecrawl key" in text:
                return "firecrawl_key_unavailable"
        return code

    def _is_manager_infra_retryable(self, error_code: str) -> bool:
        code = (error_code or "").strip().lower()
        return code in {
            "internalservererror",
            "apiconnectionerror",
            "apitimeouterror",
            "serviceunavailableerror",
            "firecrawl_key_unavailable",
            "firecrawl_429",
            "firecrawl_5xx",
            "firecrawl_request_failed",
        }

    def _mark_manager_task_failed(
        self,
        *,
        task: ManagerTask,
        retries: int,
        round_index: int,
        result: _ManagerResult,
        reason_code: str,
    ) -> None:
        self.store.mark_manager_failed(
            normalized_name=task.normalized_name,
            retries=retries,
            round_index=round_index,
            candidate_pool=result.candidate_pool,
            tried_urls=result.tried_urls,
            error_text=result.error_text or reason_code,
        )
        self.store.mark_businesslist_scan(
            company_id=task.company_id,
            normalized_name=task.normalized_name,
            company_name=task.company_name,
            domain=task.domain,
            company_manager="",
            contact_email=task.contact_email,
            status="no_manager",
            contact_phone=task.contact_phone,
        )
        print(
            f"[Manager] 失败终止 id={task.company_id} 公司={task.company_name} "
            f"原因={reason_code or 'manager_not_found'}"
        )

    def _process_manager_task(self, task: ManagerTask) -> None:
        if self.manager_agent is None:
            return
        current_retry = max(task.retries, 0) + 1
        current_round = max(task.round_index, 0) + 1
        candidate_pool = list(task.candidate_pool) if task.candidate_pool else self._default_manager_candidates(task.domain)
        tried_urls = list(task.tried_urls)
        try:
            result = self.manager_agent.enrich_manager(
                company_name=task.company_name,
                domain=task.domain,
                candidate_pool=candidate_pool,
                tried_urls=tried_urls,
            )
        except Exception as exc:  # noqa: BLE001
            error_text = str(exc)
            error_code = self._normalize_manager_error_code(type(exc).__name__, error_text)
            result = _ManagerResult(
                success=False,
                manager_name="",
                manager_role="",
                evidence_url="",
                evidence_quote="",
                candidate_pool=candidate_pool,
                tried_urls=tried_urls,
                error_code=error_code,
                error_text=error_text,
                retry_after=0.0,
            )
        if result.success:
            manager = result.manager_name.strip()
            self._clear_manager_infra_backoff()
            self.store.mark_manager_done(
                normalized_name=task.normalized_name,
                retries=current_retry,
                round_index=current_round,
                candidate_pool=result.candidate_pool,
                tried_urls=result.tried_urls,
            )
            self.store.mark_businesslist_scan(
                company_id=task.company_id,
                normalized_name=task.normalized_name,
                company_name=task.company_name,
                domain=task.domain,
                company_manager=manager,
                contact_email=task.contact_email,
                status="queued",
                contact_phone=task.contact_phone,
            )
            self.store.enqueue_snov_task(
                normalized_name=task.normalized_name,
                company_name=task.company_name,
                domain=task.domain,
                company_manager=manager,
                contact_email=task.contact_email,
                company_id=task.company_id,
                contact_phone=task.contact_phone,
            )
            print(
                f"[Manager] 补全成功 id={task.company_id} 公司={task.company_name} "
                f"manager={manager} 证据={result.evidence_url or '-'}"
            )
            return

        reason_code = result.error_code or "manager_not_found"
        if self._is_manager_non_retryable(reason_code):
            self._clear_manager_infra_backoff()
            self._mark_manager_task_failed(
                task=task,
                retries=current_retry,
                round_index=current_round,
                result=result,
                reason_code=reason_code,
            )
            return

        if self._is_manager_infra_retryable(reason_code):
            infra_backoff_delay = self._register_manager_infra_error(reason_code)
            if result.error_code == "firecrawl_429" and result.retry_after > 0:
                delay = min(max(result.retry_after, 5.0), 120.0)
            else:
                delay = _backoff_seconds(
                    base=max(self.config.retry_sleep_seconds, 3.0),
                    attempt=max(task.retries + 1, 1),
                    cap=self.config.backoff_cap_seconds,
                )
            delay = max(delay, min(infra_backoff_delay, self.config.backoff_cap_seconds))
            source = self._infer_manager_infra_source(reason_code)
            global_pause_delay = 0.0
            self.store.defer_manager_task(
                normalized_name=task.normalized_name,
                delay_seconds=delay,
                retries=max(task.retries, 0),
                round_index=max(task.round_index, 0),
                candidate_pool=result.candidate_pool,
                tried_urls=result.tried_urls,
                error_text=result.error_text or reason_code,
            )
            if reason_code == "firecrawl_key_unavailable":
                print(
                    f"[Manager] Firecrawl key 暂不可用 id={task.company_id} 公司={task.company_name} "
                    f"{delay:.1f}s 后重试（不计轮次）。"
                )
                return
            print(
                f"[Manager] 基础服务异常重试 id={task.company_id} 公司={task.company_name} "
                f"{delay:.1f}s 后重试（不计轮次）。来源={source} 原因={reason_code} "
                f"全局退避=关闭({global_pause_delay:.1f}s)"
            )
            return

        self._clear_manager_infra_backoff()
        if current_retry >= self.config.manager_enrich_max_rounds:
            self._mark_manager_task_failed(
                task=task,
                retries=current_retry,
                round_index=current_round,
                result=result,
                reason_code=reason_code,
            )
            return

        if result.error_code == "firecrawl_429" and result.retry_after > 0:
            delay = min(max(result.retry_after, 5.0), 120.0)
        else:
            delay = _backoff_seconds(
                base=max(self.config.manager_enrich_retry_backoff_seconds, 5.0),
                attempt=current_retry,
                cap=self.config.backoff_cap_seconds,
            )
        self.store.defer_manager_task(
            normalized_name=task.normalized_name,
            delay_seconds=delay,
            retries=current_retry,
            round_index=current_round,
            candidate_pool=result.candidate_pool,
            tried_urls=result.tried_urls,
            error_text=result.error_text or result.error_code,
        )
        print(
            f"[Manager] 待重试 id={task.company_id} 公司={task.company_name} "
            f"第{current_round}轮失败，{delay:.1f}s 后重试。原因={result.error_code or 'unknown'}"
        )

    def _snov_worker(self, worker_index: int = 0) -> None:
        snov_client = self._snov_clients[worker_index % len(self._snov_clients)]
        if worker_index == 0:
            revived = self.store.requeue_rate_limited_failed_tasks()
            if revived > 0:
                print(f"[Snov] 已恢复 {revived} 条历史 429 失败任务，重新入队。")
            recovered = self.store.requeue_stale_running_tasks(
                older_than_seconds=self.config.stale_running_requeue_seconds
            )
            if recovered > 0:
                print(f"[Snov] 已回收 {recovered} 条超时 running 任务并重新入队。")
        last_requeue_check = time.monotonic()
        while not self.stop_event.is_set():
            if worker_index == 0 and time.monotonic() - last_requeue_check >= 60:
                recovered = self.store.requeue_stale_running_tasks(
                    older_than_seconds=self.config.stale_running_requeue_seconds
                )
                if recovered > 0:
                    print(f"[Snov] 已回收 {recovered} 条超时 running 任务并重新入队。")
                last_requeue_check = time.monotonic()
            task = self.store.claim_snov_task()
            if task is None:
                time.sleep(1.0)
                continue
            self._process_snov_task(task, snov_client=snov_client)

    def _process_snov_task(self, task: SnovTask, *, snov_client: SnovClient | None = None) -> None:
        client = snov_client or self._snov_clients[0]
        if self.config.contact_email_fast_path and task.contact_email.strip():
            merged = _merge_emails(task.contact_email, [])
            inserted = self.store.mark_snov_done(
                normalized_name=task.normalized_name,
                final_status="done",
                contact_eamils=merged,
                company_name=task.company_name,
                domain=task.domain,
                company_manager=task.company_manager,
                company_id=task.company_id,
                phone=task.contact_phone,
            )
            if inserted:
                print(
                    f"[快速落盘] id={task.company_id} 公司={task.company_name} "
                    f"域名={task.domain} 邮箱数={len(merged)}（来源=BusinessList）"
                )
            return
        try:
            snov_emails = client.get_domain_emails(task.domain)
            merged = _merge_emails(task.contact_email, snov_emails)
            status = "done" if merged else "no_email"
            inserted = self.store.mark_snov_done(
                normalized_name=task.normalized_name,
                final_status=status,
                contact_eamils=merged,
                company_name=task.company_name,
                domain=task.domain,
                company_manager=task.company_manager,
                company_id=task.company_id,
                phone=task.contact_phone,
            )
            if status == "done" and inserted:
                print(
                    f"[落盘] id={task.company_id} 公司={task.company_name} "
                    f"域名={task.domain} 邮箱数={len(merged)}"
                )
            if status == "no_email" and not inserted:
                print(
                    f"[未落盘-无邮箱] id={task.company_id} 公司={task.company_name} "
                    f"域名={task.domain} 邮箱数=0"
                )
        except Exception as exc:  # noqa: BLE001
            if self.stop_event.is_set():
                return
            status_code = _extract_http_status(exc)
            if status_code in {400, 404, 422}:
                # 中文注释：Snov 对无效或不支持域名会返回 4xx，此类不应中断全局流程。
                merged = _merge_emails(task.contact_email, [])
                status = "done" if merged else "no_email"
                self.store.mark_snov_done(
                    normalized_name=task.normalized_name,
                    final_status=status,
                    contact_eamils=merged,
                    company_name=task.company_name,
                    domain=task.domain,
                    company_manager=task.company_manager,
                    company_id=task.company_id,
                    phone=task.contact_phone,
                )
                print(
                    f"[Snov] 跳过域名={task.domain}，状态码={status_code}，"
                    f"按现有邮箱结果记为 {status}。"
                )
                return
            if status_code == 429:
                # 中文注释：限流不应判失败，延迟后重试同一任务。
                retry_after = _extract_retry_after_seconds(exc)
                delay = float(retry_after) if retry_after is not None else 65.0
                delay = min(max(delay, 30.0), 300.0)
                self.store.defer_snov_task(
                    normalized_name=task.normalized_name,
                    delay_seconds=delay,
                    error_text=str(exc),
                )
                print(f"[Snov] 触发限流 429，domain={task.domain}，{delay:.1f}s 后自动重试。")
                return
            if not _is_transient_error(exc):
                self._set_fatal(f"Snov 不可恢复异常：domain={task.domain} {type(exc).__name__}: {exc}")
                return
            self.store.mark_snov_failed(
                normalized_name=task.normalized_name,
                error_text=str(exc),
                max_retries=self.config.snov_max_retries,
            )
            retries = task.retries + 1
            if retries >= self.config.snov_max_retries:
                print(
                    f"[Snov] 域名={task.domain} 连续失败达到上限，"
                    "已标记 failed，主流程继续跑其他公司。"
                )
                return
            delay = _backoff_seconds(
                base=self.config.retry_sleep_seconds,
                attempt=retries,
                cap=self.config.backoff_cap_seconds,
            )
            print(f"[Snov] 暂时失败({retries}) domain={task.domain}，{delay:.1f}s 后重试：{type(exc).__name__}: {exc}")
            time.sleep(delay)

    def _int_stat(self, stats: dict[str, int | str], key: str) -> int:
        value = stats.get(key, 0)
        return int(value) if isinstance(value, int) else 0

    def _delta(self, current: dict[str, int | str], key: str) -> int:
        now_value = self._int_stat(current, key)
        if self._last_stats is None:
            return 0
        prev_value = self._int_stat(self._last_stats, key)
        return now_value - prev_value

    def _print_progress(self, stats: dict[str, int | str]) -> None:
        ctos_pool = self._int_stat(stats, "ctos_pool")
        businesslist_scanned = self._int_stat(stats, "businesslist_scanned")
        businesslist_hit = self._int_stat(stats, "businesslist_hit")
        businesslist_queued = self._int_stat(stats, "businesslist_queued")
        businesslist_queued_no_manager = self._int_stat(stats, "businesslist_queued_no_manager")
        businesslist_queued_without_ctos = self._int_stat(stats, "businesslist_queued_without_ctos")
        businesslist_queued_late = self._int_stat(stats, "businesslist_queued_late")
        businesslist_queued_manager_enrich = self._int_stat(stats, "businesslist_queued_manager_enrich")
        businesslist_miss = self._int_stat(stats, "businesslist_miss")
        businesslist_error = self._int_stat(stats, "businesslist_error")
        businesslist_no_domain = self._int_stat(stats, "businesslist_no_domain")
        businesslist_no_manager = self._int_stat(stats, "businesslist_no_manager")
        businesslist_not_in_ctos = self._int_stat(stats, "businesslist_not_in_ctos")
        manager_pending = self._int_stat(stats, "manager_queue_pending")
        manager_running = self._int_stat(stats, "manager_queue_running")
        manager_done = self._int_stat(stats, "manager_queue_done")
        manager_failed = self._int_stat(stats, "manager_queue_failed")
        recent_window_size = self._int_stat(stats, "recent_window_size")
        recent_with_domain = self._int_stat(stats, "recent_with_domain")
        recent_with_manager = self._int_stat(stats, "recent_with_manager")
        recent_with_email = self._int_stat(stats, "recent_with_email")
        queue_pending = self._int_stat(stats, "queue_pending")
        queue_running = self._int_stat(stats, "queue_running")
        queue_done = self._int_stat(stats, "queue_done")
        queue_no_email = self._int_stat(stats, "queue_no_email")
        queue_failed = self._int_stat(stats, "queue_failed")
        final_companies = self._int_stat(stats, "final_companies")
        businesslist_next_id = self._int_stat(stats, "businesslist_next_id")
        print(
            "[进度] CTOS公司池={ctos}(+{ctos_d}) | "
            "BusinessList已扫描={bl}(+{bl_d}) 详情命中={hit} "
            "入Snov队列={queued}(+{queued_d}) 无管理人入队={queued_no_manager} "
            "待补管理人累计={queued_manager_enrich} "
            "未命中CTOS入队={queued_relaxed} "
            "延迟入队={queued_late}(+{queued_late_d}) "
            "404空页={miss} 异常={bl_error} 无官网域名={no_domain} "
            "无管理人={no_manager} 未命中CTOS={not_in_ctos} | "
            "Manager待补={manager_pending} 处理中={manager_running} "
            "完成={manager_done} 失败={manager_failed} | "
            "近{recent_n}条(BL原始):域名={recent_domain} 管理人={recent_manager} 邮箱={recent_email} | "
            "Snov待处理={pending} 处理中={running} 已完成={done}(+{done_d}) "
            "无邮箱={no_email}(+{no_email_d}) 失败={failed} | "
            "成品落盘={final}(+{final_d}) BusinessList游标={cursor}".format(
                ctos=ctos_pool,
                ctos_d=self._delta(stats, "ctos_pool"),
                bl=businesslist_scanned,
                bl_d=self._delta(stats, "businesslist_scanned"),
                hit=businesslist_hit,
                queued=businesslist_queued,
                queued_d=self._delta(stats, "businesslist_queued"),
                queued_no_manager=businesslist_queued_no_manager,
                queued_manager_enrich=businesslist_queued_manager_enrich,
                queued_relaxed=businesslist_queued_without_ctos,
                queued_late=businesslist_queued_late,
                queued_late_d=self._delta(stats, "businesslist_queued_late"),
                miss=businesslist_miss,
                bl_error=businesslist_error,
                no_domain=businesslist_no_domain,
                no_manager=businesslist_no_manager,
                not_in_ctos=businesslist_not_in_ctos,
                manager_pending=manager_pending,
                manager_running=manager_running,
                manager_done=manager_done,
                manager_failed=manager_failed,
                recent_n=recent_window_size,
                recent_domain=recent_with_domain,
                recent_manager=recent_with_manager,
                recent_email=recent_with_email,
                pending=queue_pending,
                running=queue_running,
                done=queue_done,
                done_d=self._delta(stats, "queue_done"),
                no_email=queue_no_email,
                no_email_d=self._delta(stats, "queue_no_email"),
                failed=queue_failed,
                final=final_companies,
                final_d=self._delta(stats, "final_companies"),
                cursor=businesslist_next_id,
            )
        )
        if self._delta(stats, "final_companies") > 0:
            company_id = self._int_stat(stats, "last_success_company_id")
            company_name = str(stats.get("last_success_company_name", ""))
            domain = str(stats.get("last_success_domain", ""))
            updated_at = str(stats.get("last_success_updated_at", ""))
            email_count = self._int_stat(stats, "last_success_email_count")
            print(
                f"[进度-落盘] 最新成品 id={company_id} 公司={company_name} "
                f"域名={domain} 邮箱数={email_count} 时间={updated_at}"
            )

    def _check_zero_queue_guard(self, stats: dict[str, int | str]) -> None:
        if not self.config.strict_ctos_match:
            return
        hit = self._int_stat(stats, "businesslist_hit")
        if hit < self.config.zero_queue_guard_min_hits:
            return
        queued = self._int_stat(stats, "businesslist_queued")
        queued_late = self._int_stat(stats, "businesslist_queued_late")
        done = self._int_stat(stats, "queue_done")
        no_email = self._int_stat(stats, "queue_no_email")
        final_count = self._int_stat(stats, "final_companies")
        manager_pending = self._int_stat(stats, "manager_queue_pending")
        manager_running = self._int_stat(stats, "manager_queue_running")
        manager_done = self._int_stat(stats, "manager_queue_done")
        not_in_ctos = self._int_stat(stats, "businesslist_not_in_ctos")
        if (
            queued > 0
            or queued_late > 0
            or done > 0
            or no_email > 0
            or final_count > 0
            or manager_pending > 0
            or manager_running > 0
            or manager_done > 0
        ):
            return
        if not_in_ctos < hit:
            return
        if hit != self._last_guard_hint_hits:
            self._last_guard_hint_hits = hit
            print(
                f"[告警] 已命中 BusinessList 详情页 {hit} 条，但入Snov队列仍为 0。"
                "当前全部卡在“未命中CTOS”，已自动停机避免空跑。"
            )
        self._set_fatal("零入队保护触发：请先确认 CTOS 与 BusinessList 的匹配策略。")

    def _start_threads(self) -> None:
        threads: list[threading.Thread] = []
        ctos_worker_count = max(self.config.ctos_worker_count, 1)
        for idx in range(ctos_worker_count):
            worker_name = f"ctos-worker-{idx + 1}"
            threads.append(
                threading.Thread(
                    target=self._run_worker_guard,
                    args=(worker_name, self._ctos_worker, idx, ctos_worker_count),
                    name=worker_name,
                    daemon=True,
                )
            )
        businesslist_worker_count = max(self.config.businesslist_worker_count, 1)
        for idx in range(businesslist_worker_count):
            worker_name = f"businesslist-worker-{idx + 1}"
            threads.append(
                threading.Thread(
                    target=self._run_worker_guard,
                    args=(worker_name, self._businesslist_worker, idx),
                    name=worker_name,
                    daemon=True,
                )
            )
        if self.manager_agent is not None:
            worker_count = max(self.config.manager_worker_count, 1)
            for idx in range(worker_count):
                worker_name = f"manager-worker-{idx + 1}"
                threads.append(
                    threading.Thread(
                        target=self._run_worker_guard,
                        args=(worker_name, self._manager_worker),
                        name=worker_name,
                        daemon=True,
                    )
                )
        snov_worker_count = max(self.config.snov_worker_count, 1)
        for idx in range(snov_worker_count):
            worker_name = f"snov-worker-{idx + 1}"
            threads.append(
                threading.Thread(
                    target=self._run_worker_guard,
                    args=(worker_name, self._snov_worker, idx),
                    name=worker_name,
                    daemon=True,
                ),
            )
        self._threads = threads
        for thread in self._threads:
            thread.start()

    def run_forever(self) -> None:
        self._acquire_instance_lock()
        try:
            self._start_threads()
            manager_worker_count = max(self.config.manager_worker_count, 0) if self.manager_agent is not None else 0
            print("[Pipeline] 已启动：CTOS + BusinessList + Snov 三线并发。")
            print(
                f"[Pipeline] worker：CTOS={max(self.config.ctos_worker_count, 1)} "
                f"BusinessList={max(self.config.businesslist_worker_count, 1)} "
                f"Snov={max(self.config.snov_worker_count, 1)} "
                f"Manager={manager_worker_count}"
            )
            print(
                f"[Pipeline] Manager补全：{'开启' if self.manager_agent is not None else '关闭'}，"
                f"worker={manager_worker_count}，"
                f"每轮重试上限={self.config.manager_enrich_max_rounds}"
            )
            try:
                while not self.stop_event.is_set():
                    stats = self.store.get_stats()
                    self._print_progress(stats)
                    self._check_zero_queue_guard(stats)
                    self._last_stats = stats
                    time.sleep(self.config.log_interval_seconds)
            except KeyboardInterrupt:
                print("[Pipeline] 收到停止信号，准备退出。")
                self.shutdown_event.set()
            finally:
                self.stop_event.set()
                for thread in self._threads:
                    thread.join(timeout=5)
                closed_ids: set[int] = set()
                for crawler in self._businesslist_crawlers:
                    close_fn = getattr(crawler, "close", None)
                    if not callable(close_fn):
                        continue
                    object_id = id(crawler)
                    if object_id in closed_ids:
                        continue
                    close_fn()
                    closed_ids.add(object_id)
                self.store.close()
        finally:
            self._release_instance_lock()
        fatal = self._get_fatal()
        if fatal:
            raise RuntimeError(fatal)


def build_businesslist_source(
    *,
    source: str,
    cf_cookies_file: str,
    cf_user_agent: str,
    cf_max_retries: int,
    cf_backoff_base: float,
    proxy_url: str,
    use_system_proxy: bool,
    timeout: float,
    delay_min: float,
    delay_max: float,
    verify_ssl: bool,
) -> _BusinessListSource:
    if source == "cf":
        return BusinessListCFCrawler(
            timeout=timeout,
            delay_min=delay_min,
            delay_max=delay_max,
            max_retries=cf_max_retries,
            backoff_base=cf_backoff_base,
            user_agent=cf_user_agent,
            cookies_file=cf_cookies_file,
            proxy_url=proxy_url,
            use_system_proxy=use_system_proxy,
        )
    return BusinessListCrawler(
        timeout=timeout,
        delay_min=delay_min,
        delay_max=delay_max,
        verify_ssl=verify_ssl,
        proxy_url=proxy_url,
        use_system_proxy=use_system_proxy,
    )
