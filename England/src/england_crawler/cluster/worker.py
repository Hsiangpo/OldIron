"""England 集群 worker。"""

from __future__ import annotations

import logging
import json
import platform
import random
import re
import socket
import subprocess
import sys
import threading
import time
from dataclasses import dataclass
from urllib.parse import urlparse

import requests

from england_crawler.cluster.config import ClusterConfig
from england_crawler.companies_house.client import CompaniesHouseClient
from england_crawler.companies_house.client import select_best_candidate
from england_crawler.dnb.browser_cookie import DnbCookieProvider
from england_crawler.dnb.client import DnbClient
from england_crawler.dnb.client import extract_child_segments
from england_crawler.dnb.client import parse_company_listing
from england_crawler.dnb.client import parse_company_profile
from england_crawler.dnb.models import CompanyRecord
from england_crawler.dnb.models import Segment
from england_crawler.fc_email.client import FirecrawlClient
from england_crawler.fc_email.client import FirecrawlClientConfig
from england_crawler.fc_email.client import FirecrawlError
from england_crawler.fc_email.key_pool import KeyLease
from england_crawler.fc_email.llm_client import EmailUrlLlmClient
from england_crawler.google_maps import GoogleMapsClient
from england_crawler.google_maps import GoogleMapsConfig
from england_crawler.google_maps import GoogleMapsPlaceResult
from england_crawler.snov.client import extract_domain


logger = logging.getLogger(__name__)
URL_KEYWORDS = {
    "contact": 100,
    "support": 95,
    "help": 90,
    "customer": 85,
    "about": 80,
    "team": 78,
    "leadership": 76,
    "management": 74,
    "director": 72,
    "board": 70,
    "privacy": 68,
    "legal": 66,
    "imprint": 64,
    "career": 62,
    "job": 60,
    "press": 58,
    "media": 56,
    "terms": 54,
    "policy": 52,
    ".pdf": 50,
}
IGNORE_LOCAL_PARTS = {
    "x",
    "xx",
    "xxx",
    "test",
    "example",
    "sample",
    "yourname",
    "youremail",
    "email",
    "noreply",
    "no-reply",
    "donotreply",
    "do-not-reply",
}
CORP_SUFFIX_PATTERNS = (
    r"\bco\.?\s*,?\s*ltd\.?\b",
    r"\bcorporation\b",
    r"\bcorp\.?\b",
    r"\bcompany\b",
    r"\blimited\b",
    r"\bltd\.?\b",
)
WORKER_ROLE_CAPABILITIES = {
    "ch-lookup": ["ch_lookup"],
    "dnb-discovery": ["dnb_discovery", "dnb_list_segment"],
    "dnb-detail": ["dnb_detail"],
    "gmap": ["ch_gmap", "dnb_gmap"],
    "email-firecrawl": ["ch_firecrawl", "dnb_firecrawl"],
}

DNB_CLIENT_TASK_TYPES = {"dnb_discovery", "dnb_list_segment", "dnb_detail"}


def _normalize_text(value: str) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip())


def _strip_company_suffix(name: str) -> str:
    value = _normalize_text(name)
    lowered = value
    for pattern in CORP_SUFFIX_PATTERNS:
        lowered = re.sub(pattern, "", lowered, flags=re.I).strip(" ,()-")
    return _normalize_text(lowered)


def _build_gmap_queries(task: dict[str, object]) -> list[str]:
    parts = [str(task.get("city", "")), str(task.get("region", "")), str(task.get("country", "")) or "United Kingdom"]
    names = [str(task.get("company_name_en", "")), _strip_company_suffix(str(task.get("company_name_en", "")))]
    queries: list[str] = []
    for name in names:
        for suffix in (
            " ".join(part for part in parts if part),
            f"{task.get('region', '')} {task.get('country', '')}".strip(),
            str(task.get("country", "")),
        ):
            query = _normalize_text(" ".join(part for part in [name, suffix] if part))
            if query and query not in queries:
                queries.append(query)
    return queries


def _merge_place_results(current: GoogleMapsPlaceResult, incoming: GoogleMapsPlaceResult) -> GoogleMapsPlaceResult:
    return GoogleMapsPlaceResult(
        company_name=current.company_name or incoming.company_name,
        phone=current.phone or incoming.phone,
        website=current.website or incoming.website,
        score=max(int(current.score), int(incoming.score)),
    )


@dataclass(slots=True)
class ClaimedTaskPayload:
    task_id: str
    task_type: str
    retries: int
    payload: dict[str, object]


class ClusterApiClient:
    """协调器 HTTP 客户端。"""

    def __init__(self, config: ClusterConfig, *, worker_id: str) -> None:
        self._config = config
        self._worker_id = str(worker_id).strip()
        self._session = requests.Session()
        self._session.trust_env = False

    def register_worker(self, capabilities: list[str]) -> None:
        self._post(
            "/api/v1/workers/register",
            {
                "worker_id": self._worker_id,
                "host_name": socket.gethostname(),
                "platform": platform.platform(),
                "capabilities": capabilities,
                "git_commit": "",
                "python_version": sys.version.split()[0],
            },
        )

    def heartbeat(self) -> None:
        self._post("/api/v1/workers/heartbeat", {"worker_id": self._worker_id})

    def claim_task(self, capabilities: list[str]) -> ClaimedTaskPayload | None:
        payload = self._post(
            "/api/v1/tasks/claim",
            {"worker_id": self._worker_id, "capabilities": capabilities},
        )
        task = payload.get("task")
        if not isinstance(task, dict):
            return None
        return ClaimedTaskPayload(
            task_id=str(task["task_id"]),
            task_type=str(task["task_type"]),
            retries=int(task["retries"]),
            payload=dict(task.get("payload", {}) or {}),
        )

    def complete_task(self, task_id: str, result: dict[str, object]) -> None:
        self._post(
            f"/api/v1/tasks/{task_id}/complete",
            {"worker_id": self._worker_id, "result": result},
        )

    def renew_task_lease(self, task_id: str) -> None:
        self._post(
            f"/api/v1/tasks/{task_id}/renew",
            {"worker_id": self._worker_id},
        )

    def fail_task(self, task_id: str, *, error_text: str, retry_delay_seconds: float, fatal: bool) -> None:
        self._post(
            f"/api/v1/tasks/{task_id}/fail",
            {
                "worker_id": self._worker_id,
                "error_text": error_text,
                "retry_delay_seconds": retry_delay_seconds,
                "fatal": fatal,
            },
        )

    def acquire_firecrawl_key(self) -> dict[str, str]:
        payload = self._post("/api/v1/firecrawl/lease", {"worker_id": self._worker_id})
        return {"key_hash": str(payload["key_hash"]), "key_value": str(payload["key_value"])}

    def release_firecrawl_key(
        self,
        *,
        key_hash: str,
        outcome: str,
        retry_after_seconds: float = 0.0,
        reason: str = "",
    ) -> None:
        self._post(
            "/api/v1/firecrawl/release",
            {
                "key_hash": key_hash,
                "outcome": outcome,
                "retry_after_seconds": retry_after_seconds,
                "reason": reason,
            },
        )

    def _post(self, path: str, payload: dict[str, object]) -> dict[str, object]:
        headers = {"Content-Type": "application/json"}
        if self._config.cluster_token:
            headers["X-OldIron-Token"] = self._config.cluster_token
        if sys.platform == "darwin":
            return self._post_via_curl(path, headers, payload)
        response = self._session.post(
            self._config.coordinator_base_url + path,
            headers=headers,
            json=payload,
            timeout=30,
        )
        try:
            data = response.json()
        except Exception:  # noqa: BLE001
            data = {}
        if response.status_code >= 400:
            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(str(data["error"]))
            raise RuntimeError(f"协调器 HTTP {response.status_code}: {path}")
        if not isinstance(data, dict):
            raise RuntimeError(f"协调器返回非法响应：{path}")
        if data.get("error"):
            raise RuntimeError(str(data["error"]))
        return data

    def _post_via_curl(
        self,
        path: str,
        headers: dict[str, str],
        payload: dict[str, object],
    ) -> dict[str, object]:
        cmd = [
            "curl",
            "--silent",
            "--show-error",
            "--max-time",
            "30",
            "--noproxy",
            "*",
            "-X",
            "POST",
            "-w",
            "\n%{http_code}",
            self._config.coordinator_base_url + path,
        ]
        for key, value in headers.items():
            cmd.extend(["-H", f"{key}: {value}"])
        cmd.extend(["--data-binary", json.dumps(payload, ensure_ascii=False)])
        result = subprocess.run(
            cmd,
            check=False,
            capture_output=True,
            text=True,
            encoding="utf-8",
        )
        raw_output = str(result.stdout or "")
        raw_body, _, status_text = raw_output.rpartition("\n")
        if result.returncode != 0 and not status_text.strip():
            error_text = str(result.stderr or "").strip() or f"curl exit {result.returncode}"
            raise RuntimeError(error_text)
        try:
            status_code = int(status_text.strip() or "0")
        except ValueError as exc:
            raise RuntimeError(f"协调器返回非法状态码：{status_text}") from exc
        try:
            data = json.loads(raw_body or "{}")
        except json.JSONDecodeError:
            data = {}
        if status_code >= 400:
            if isinstance(data, dict) and data.get("error"):
                raise RuntimeError(str(data["error"]))
            error_text = str(result.stderr or "").strip()
            raise RuntimeError(error_text or f"协调器 HTTP {status_code}: {path}")
        if not isinstance(data, dict):
            raise RuntimeError(f"协调器返回非法响应：{path}")
        if data.get("error"):
            raise RuntimeError(str(data["error"]))
        return data


def get_worker_role_capabilities(role: str) -> list[str]:
    value = str(role or "").strip().lower()
    capabilities = WORKER_ROLE_CAPABILITIES.get(value)
    if capabilities is None:
        raise ValueError(f"不支持的 worker 角色：{role}")
    return list(capabilities)


class CoordinatorKeyPool:
    """借助协调器租约 Firecrawl key。"""

    def __init__(self, api: ClusterApiClient) -> None:
        self._api = api
        self._leases: dict[int, str] = {}
        self._lease_index = 0

    def acquire(self) -> KeyLease:
        try:
            key = self._api.acquire_firecrawl_key()
        except Exception as exc:  # noqa: BLE001
            raise RuntimeError(str(exc)) from exc
        self._lease_index += 1
        self._leases[self._lease_index] = key["key_hash"]
        return KeyLease(key=key["key_value"], index=self._lease_index)

    def release(self, lease: KeyLease) -> None:
        return None

    def mark_success(self, lease: KeyLease) -> None:
        key_hash = self._leases.pop(lease.index, "")
        if key_hash:
            self._api.release_firecrawl_key(key_hash=key_hash, outcome="success")

    def mark_rate_limited(self, lease: KeyLease, retry_after: float | None = None) -> None:
        key_hash = self._leases.pop(lease.index, "")
        if key_hash:
            self._api.release_firecrawl_key(
                key_hash=key_hash,
                outcome="rate_limited",
                retry_after_seconds=float(retry_after or 0.0),
            )

    def mark_failure(self, lease: KeyLease) -> None:
        key_hash = self._leases.pop(lease.index, "")
        if key_hash:
            self._api.release_firecrawl_key(key_hash=key_hash, outcome="failure")

    def disable(self, lease: KeyLease, reason: str) -> None:
        key_hash = self._leases.pop(lease.index, "")
        if key_hash:
            self._api.release_firecrawl_key(key_hash=key_hash, outcome="disable", reason=reason)


class ClusterEmailService:
    """复用 Firecrawl + LLM 的集群版邮箱发现。"""

    def __init__(self, config: ClusterConfig, api: ClusterApiClient) -> None:
        self._config = config
        self._firecrawl = FirecrawlClient(
            key_pool=CoordinatorKeyPool(api),
            config=FirecrawlClientConfig(
                timeout_seconds=config.firecrawl_timeout_seconds,
                max_retries=config.firecrawl_max_retries,
            ),
        )
        self._llm = EmailUrlLlmClient(
            api_key=config.llm_api_key,
            base_url=config.llm_base_url,
            model=config.llm_model,
            reasoning_effort=config.llm_reasoning_effort,
            timeout_seconds=config.llm_timeout_seconds,
        )

    def discover_emails(self, *, company_name: str, homepage: str, domain: str) -> list[str]:
        start_url = str(homepage or "").strip()
        if not start_url and domain:
            start_url = f"https://{domain.strip().lower()}"
        if not start_url:
            return []
        mapped_urls = self._firecrawl.map_site(start_url, limit=200)
        ranked_urls = self._prefilter_urls(start_url, mapped_urls)
        llm_urls = self._llm.pick_candidate_urls(
            company_name=company_name,
            domain=extract_domain(start_url),
            homepage=start_url,
            candidate_urls=ranked_urls,
            target_count=self._config.firecrawl_llm_pick_count,
        )
        final_urls = self._build_final_urls(start_url, llm_urls, ranked_urls)
        emails = self._extract_emails_with_fallback(final_urls)
        same_domain = self._filter_same_domain_emails(start_url, emails)
        return self._clean_emails(same_domain or emails)

    def _prefilter_urls(self, start_url: str, mapped_urls: list[str]) -> list[str]:
        host = urlparse(start_url).netloc.lower()
        ranked: list[tuple[int, str]] = []
        seen: set[str] = set()
        for raw in [start_url, *mapped_urls]:
            url = str(raw or "").strip()
            if not url or url in seen or not url.startswith("http"):
                continue
            if not self._same_host(host, url):
                continue
            seen.add(url)
            ranked.append((self._score_url(start_url, url), url))
        ranked.sort(key=lambda item: (-item[0], item[1]))
        return [url for _score, url in ranked[: max(self._config.firecrawl_prefilter_limit, 1)]]

    def _build_final_urls(self, start_url: str, llm_urls: list[str], ranked_urls: list[str]) -> list[str]:
        urls: list[str] = []
        for raw in [start_url, *llm_urls, *ranked_urls]:
            url = str(raw or "").strip()
            if url and url not in urls:
                urls.append(url)
            if len(urls) >= max(self._config.firecrawl_extract_max_urls, 1):
                break
        return urls

    def _extract_emails_with_fallback(self, urls: list[str]) -> list[str]:
        try:
            return self._firecrawl.extract_emails(urls).emails
        except FirecrawlError as exc:
            if exc.code not in {"firecrawl_extract_failed", "firecrawl_extract_timeout", "firecrawl_http_404"}:
                raise
        merged: list[str] = []
        for url in urls:
            try:
                result = self._firecrawl.extract_emails([url])
            except FirecrawlError as exc:
                if exc.code in {"firecrawl_http_404", "firecrawl_extract_failed", "firecrawl_extract_timeout"}:
                    continue
                raise
            for email in result.emails:
                value = str(email or "").strip().lower()
                if value and value not in merged:
                    merged.append(value)
        return merged

    def _filter_same_domain_emails(self, start_url: str, emails: list[str]) -> list[str]:
        domain = extract_domain(start_url)
        if not domain:
            return []
        matched: list[str] = []
        for email in emails:
            value = str(email or "").strip().lower()
            if "@" not in value:
                continue
            email_domain = value.split("@", 1)[1]
            if email_domain == domain or email_domain.endswith(f".{domain}"):
                if value not in matched:
                    matched.append(value)
        return matched

    def _clean_emails(self, emails: list[str]) -> list[str]:
        cleaned: list[str] = []
        for email in emails:
            value = str(email or "").strip().lower()
            if "@" not in value:
                continue
            if value.split("@", 1)[0] in IGNORE_LOCAL_PARTS:
                continue
            if value not in cleaned:
                cleaned.append(value)
        return cleaned

    def _score_url(self, start_url: str, url: str) -> int:
        if url == start_url:
            return 1000
        lowered = url.lower()
        score = sum(weight for keyword, weight in URL_KEYWORDS.items() if keyword in lowered)
        return score - min(lowered.count("/"), 10)

    def _same_host(self, host: str, url: str) -> bool:
        target = urlparse(url).netloc.lower()
        return bool(target and (target == host or target.endswith(f".{host}") or host.endswith(f".{target}")))


class ClusterWorkerRuntime:
    """England 集群 worker 主循环。"""

    def __init__(
        self,
        config: ClusterConfig,
        *,
        role: str,
        worker_index: int = 1,
        worker_id: str = "",
    ) -> None:
        self._config = config
        self._role = str(role or "").strip().lower()
        if not self._role:
            raise ValueError("worker 角色不能为空。")
        self._capabilities = get_worker_role_capabilities(self._role)
        resolved_worker_id = str(worker_id or "").strip()
        if not resolved_worker_id:
            resolved_worker_id = f"{socket.gethostname().strip().lower() or 'worker'}-{self._role}-{max(int(worker_index), 1)}"
        self._worker_id = resolved_worker_id
        self._api = ClusterApiClient(config, worker_id=self._worker_id)
        self._email_service = ClusterEmailService(config, self._api)
        self._gmap_client = GoogleMapsClient(GoogleMapsConfig(hl="en", gl="gb"))
        self._dnb_client = self._build_dnb_client()
        self._ch_client = CompaniesHouseClient(worker_label=self._worker_id)
        self._last_heartbeat = 0.0

    def _needs_dnb_client(self) -> bool:
        return any(item in DNB_CLIENT_TASK_TYPES for item in self._capabilities)

    def _build_dnb_client(self) -> DnbClient | None:
        if not self._needs_dnb_client():
            return None
        provider = DnbCookieProvider(
            project_root=self._config.project_root,
            logger=logger,
            allow_env_fallback=False,
        )
        cookie_header = provider.get(force_refresh=True)
        if not cookie_header:
            raise RuntimeError("DNB worker 启动失败：9222 浏览器未提供 DNB cookie。")
        return DnbClient(cookie_header=cookie_header, cookie_provider=provider)

    def _require_dnb_client(self) -> DnbClient:
        if self._dnb_client is None:
            raise RuntimeError("当前 worker 未初始化 DNB 客户端。")
        return self._dnb_client

    def run_forever(self) -> None:
        self._config.validate_worker_runtime()
        self._register_until_ready()
        while True:
            try:
                self._maybe_heartbeat()
                task = self._api.claim_task(self._capabilities)
            except Exception as exc:  # noqa: BLE001
                logger.warning("集群 worker 无法连接协调器，稍后重试：%s", exc)
                time.sleep(max(self._config.worker_poll_seconds, 2.0))
                continue
            if task is None:
                time.sleep(self._config.worker_poll_seconds)
                continue
            self._handle_task(task)

    def _register_until_ready(self) -> None:
        while True:
            try:
                self._api.register_worker(self._capabilities)
                self._last_heartbeat = time.monotonic()
                return
            except Exception as exc:  # noqa: BLE001
                logger.warning("集群 worker 注册失败，稍后重试：%s", exc)
                time.sleep(max(self._config.worker_poll_seconds, 2.0))

    def _maybe_heartbeat(self) -> None:
        now = time.monotonic()
        if now - self._last_heartbeat < self._config.worker_heartbeat_seconds:
            return
        self._api.heartbeat()
        self._last_heartbeat = now

    def _start_task_lease_renewer(self, task_id: str) -> tuple[threading.Event, threading.Thread]:
        stop_event = threading.Event()
        interval_seconds = max(float(self._config.task_lease_seconds) / 3.0, 15.0)

        def _runner() -> None:
            while not stop_event.wait(interval_seconds):
                try:
                    self._api.renew_task_lease(task_id)
                except Exception as exc:  # noqa: BLE001
                    logger.warning("任务租约续期失败：task=%s | 原因=%s", task_id, exc)

        thread = threading.Thread(
            target=_runner,
            name=f"LeaseRenew-{task_id}",
            daemon=True,
        )
        thread.start()
        return stop_event, thread

    def _handle_task(self, task: ClaimedTaskPayload) -> None:
        logger.info(
            "任务开始：worker=%s | role=%s | type=%s | entity=%s",
            self._worker_id,
            self._role,
            task.task_type,
            task.payload.get("comp_id", task.payload.get("duns", task.entity_id if hasattr(task, "entity_id") else "")),
        )
        renew_stop, renew_thread = self._start_task_lease_renewer(task.task_id)
        try:
            result = self._execute_task(task)
            if not self._report_complete(task.task_id, result):
                logger.warning("任务完成结果暂未回写，等待租约回收后重试：%s", task.task_id)
        except FirecrawlError as exc:
            fatal = exc.code in {"firecrawl_401", "firecrawl_402"}
            delay = max(float(exc.retry_after or 0.0), 5.0) if exc.code == "firecrawl_429" else 30.0
            if not self._report_failure(task.task_id, error_text=str(exc), retry_delay_seconds=delay, fatal=fatal):
                logger.warning("任务失败结果暂未回写，等待租约回收后重试：%s", task.task_id)
        except Exception as exc:  # noqa: BLE001
            delay = min((2 ** max(task.retries, 1)) * 5.0, 180.0)
            if not self._report_failure(task.task_id, error_text=str(exc), retry_delay_seconds=delay, fatal=False):
                logger.warning("任务异常结果暂未回写，等待租约回收后重试：%s", task.task_id)
        finally:
            renew_stop.set()
            renew_thread.join(timeout=1.0)

    def _report_complete(self, task_id: str, result: dict[str, object]) -> bool:
        for attempt in range(5):
            try:
                self._api.complete_task(task_id, result)
                return True
            except Exception as exc:  # noqa: BLE001
                logger.warning("回写完成结果失败：task=%s 第%d次 原因=%s", task_id, attempt + 1, exc)
                time.sleep(min(2 ** attempt, 15))
        return False

    def _report_failure(
        self,
        task_id: str,
        *,
        error_text: str,
        retry_delay_seconds: float,
        fatal: bool,
    ) -> bool:
        for attempt in range(5):
            try:
                self._api.fail_task(
                    task_id,
                    error_text=error_text,
                    retry_delay_seconds=retry_delay_seconds,
                    fatal=fatal,
                )
                return True
            except Exception as exc:  # noqa: BLE001
                logger.warning("回写失败结果失败：task=%s 第%d次 原因=%s", task_id, attempt + 1, exc)
                time.sleep(min(2 ** attempt, 15))
        return False

    def _execute_task(self, task: ClaimedTaskPayload) -> dict[str, object]:
        payload = task.payload
        if task.task_type == "dnb_discovery":
            dnb_client = self._require_dnb_client()
            segment = Segment.from_dict(payload)
            logger.info("DNB 探索开始：%s", segment.segment_id)
            result = dnb_client.fetch_company_listing_page(segment=segment, page_number=1)
            expected = int(result.get("candidatesMatchedQuantityInt", 0) or 0)
            logger.info("DNB 探索完成：%s | 预估=%d", segment.segment_id, expected)
            return {
                "expected_count": expected,
                "children": [item.to_dict() for item in extract_child_segments(segment.industry_path, result, segment.country_iso_two_code)],
            }
        if task.task_type == "dnb_list_segment":
            dnb_client = self._require_dnb_client()
            segment = Segment.from_dict(payload)
            page_number = int(payload.get("next_page", 1) or 1)
            logger.info("DNB 列表开始：%s | page=%d", segment.segment_id, page_number)
            raw = dnb_client.fetch_company_listing_page(segment=segment, page_number=page_number)
            rows = [item.to_dict() for item in parse_company_listing(raw)]
            page_count = int(raw.get("pageCount", 1) or 1)
            done = page_number >= page_count or not rows
            logger.info("DNB 列表完成：%s | page=%d | rows=%d | done=%s", segment.segment_id, page_number, len(rows), done)
            return {
                "rows": rows,
                "next_page": page_number + 1,
                "total_pages": page_count,
                "done": done,
            }
        if task.task_type == "dnb_detail":
            dnb_client = self._require_dnb_client()
            logger.info("DNB 详情开始：%s | %s", payload.get("duns", ""), payload.get("company_name_en_dnb", ""))
            company = parse_company_profile(
                record=CompanyRecord.from_dict(payload),
                payload=dnb_client.fetch_company_profile(str(payload.get("company_name_url", ""))),
            )
            logger.info("DNB 详情完成：%s | 负责人=%s | 官网=%s", company.duns, company.key_principal, company.dnb_website)
            return company.to_dict()
        if task.task_type == "dnb_gmap":
            logger.info("GMap 开始：%s | %s", payload.get("duns", ""), payload.get("company_name_en", ""))
            result = GoogleMapsPlaceResult()
            for query in _build_gmap_queries(payload):
                logger.info("GMap 查询：%s | %s", payload.get("duns", ""), query)
                candidate = self._gmap_client.search_company_profile(query, company_name=str(payload.get("company_name_en", "")))
                result = _merge_place_results(result, candidate)
                if result.website:
                    break
            logger.info("GMap 完成：%s | 官网=%s | 电话=%s", payload.get("duns", ""), result.website or payload.get("dnb_website", ""), result.phone)
            return {
                "website": result.website or str(payload.get("dnb_website", "")),
                "source": "gmap" if result.website else ("dnb" if str(payload.get("dnb_website", "")) else ""),
                "phone": result.phone,
                "company_name_local_gmap": result.company_name,
            }
        if task.task_type == "dnb_firecrawl":
            domain = str(payload.get("domain", "")).strip().lower() or extract_domain(str(payload.get("homepage", "")).strip())
            logger.info("Firecrawl 开始：%s | 域名=%s", payload.get("duns", ""), domain)
            emails = self._email_service.discover_emails(
                company_name=str(payload.get("company_name_en_dnb", "")),
                homepage=str(payload.get("homepage", "")),
                domain=str(payload.get("domain", "")),
            )
            logger.info("Firecrawl 完成：%s | 域名=%s | 邮箱=%d", payload.get("duns", ""), domain, len(emails))
            return {"emails": emails}
        if task.task_type == "ch_lookup":
            logger.info("CH 开始：%s | %s", payload.get("comp_id", ""), payload.get("company_name", ""))
            logger.info("CH 查询：%s | %s", payload.get("comp_id", ""), payload.get("company_name", ""))
            candidates = self._ch_client.search_companies(str(payload.get("company_name", "")))
            candidate = select_best_candidate(str(payload.get("company_name", "")), candidates)
            if candidate is None:
                logger.info("CH 未命中：%s | 公司号= | 代表人=", payload.get("comp_id", ""))
                return {"company_number": "", "company_status": "not_found", "ceo": ""}
            logger.info("CH 命中：%s | 公司号=%s | 状态=%s", payload.get("comp_id", ""), candidate.company_number, candidate.status_text)
            logger.info("CH Officers 查询：%s | 公司号=%s", payload.get("comp_id", ""), candidate.company_number)
            ceo = self._ch_client.fetch_first_active_director(candidate.company_number)
            logger.info("CH 完成：%s | 公司号=%s | 代表人=%s", payload.get("comp_id", ""), candidate.company_number, ceo)
            return {
                "company_number": candidate.company_number,
                "company_status": candidate.status_text,
                "ceo": ceo,
            }
        if task.task_type == "ch_gmap":
            logger.info("GMap 开始：%s | %s", payload.get("comp_id", ""), payload.get("company_name", ""))
            profile = self._gmap_client.search_company_profile(
                str(payload.get("company_name", "")),
                str(payload.get("company_name", "")),
            )
            logger.info("GMap 完成：%s | 官网=%s | 电话=%s", payload.get("comp_id", ""), profile.website, profile.phone)
            return {"homepage": profile.website, "phone": profile.phone}
        if task.task_type == "ch_firecrawl":
            domain = str(payload.get("domain", "")).strip().lower() or extract_domain(str(payload.get("homepage", "")).strip())
            logger.info("Firecrawl 开始：%s | 域名=%s", payload.get("comp_id", ""), domain)
            emails = self._email_service.discover_emails(
                company_name=str(payload.get("company_name", "")),
                homepage=str(payload.get("homepage", "")),
                domain=str(payload.get("domain", "")),
            )
            logger.info("Firecrawl 完成：%s | 域名=%s | 邮箱=%d", payload.get("comp_id", ""), domain, len(emails))
            return {"emails": emails}
        raise RuntimeError(f"未知任务类型：{task.task_type}")
