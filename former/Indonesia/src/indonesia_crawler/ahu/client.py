"""AHU 协议查询客户端。"""

from __future__ import annotations

import logging
import os
import re
import time
from dataclasses import dataclass
from urllib.parse import quote

from curl_cffi import requests as cffi_requests
from curl_cffi.const import CurlOpt

from .captcha_solver import CaptchaSolveError, TwoCaptchaConfig, TwoCaptchaSolver
from .parser import AhuDetail, AhuSearchResult, extract_form_token, parse_detail_payload, parse_search_results
from ..proxy import ProxyPool, build_proxy_pool_from_env

logger = logging.getLogger(__name__)


def _env_float(name: str, default: float) -> float:
    """读取浮点环境变量。"""
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        return default


def _env_int(name: str, default: int) -> int:
    """读取整型环境变量。"""
    raw = os.getenv(name, "").strip()
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        return default


def _env_bool(name: str, default: bool = False) -> bool:
    """读取布尔环境变量。"""
    raw = os.getenv(name, "").strip().lower()
    if not raw:
        return default
    return raw in {"1", "true", "yes", "on", "y"}


def normalize_ahu_query_name(company_name: str) -> str:
    """按 AHU 要求清洗公司名（去前后缀 PT/CV/TBK）。"""
    value = company_name.strip().upper()

    # 去掉常见前缀：PT/CV/全称。
    value = re.sub(r"^(PERSEROAN TERBATAS|PT)\.?\s+", "", value)
    value = re.sub(r"^(PERSEKUTUAN KOMANDITER|CV)\.?\s+", "", value)

    # 去掉常见后缀：PT/CV/TBK。
    value = re.sub(r"\s+(PERSEROAN TERBATAS|PT)\.?$", "", value)
    value = re.sub(r"\s+(PERSEKUTUAN KOMANDITER|CV)\.?$", "", value)
    value = re.sub(r"\s+TBK\.?$", "", value)

    # 清理尾部残留标点，避免造成 AHU 精确匹配失败。
    value = value.strip(" ,.-_/")
    value = re.sub(r"\s+", " ", value).strip()
    return value


@dataclass(slots=True)
class AhuRateLimitConfig:
    """AHU 限速配置。"""

    request_delay: float = 1.5
    timeout: float = 45.0
    max_retries: int = 3
    retry_backoff: float = 2.0
    rate_limit_wait: float = 8.0


def build_ahu_rate_config_from_env() -> AhuRateLimitConfig:
    """从环境变量构造 AHU 速率配置。"""
    return AhuRateLimitConfig(
        request_delay=_env_float("AHU_REQUEST_DELAY", 1.5),
        timeout=_env_float("AHU_TIMEOUT", 45.0),
        max_retries=_env_int("AHU_MAX_RETRIES", 3),
        retry_backoff=_env_float("AHU_RETRY_BACKOFF", 2.0),
        rate_limit_wait=_env_float("AHU_RATE_LIMIT_WAIT", 8.0),
    )


class AhuRateLimitError(RuntimeError):
    """AHU 全局限流异常。"""

    def __init__(self, retry_after: float, message: str = "HTTP Error 429: CHttpException") -> None:
        super().__init__(message)
        self.retry_after = retry_after


class AhuClient:
    """AHU 搜索与详情查询客户端。"""

    BASE_URL = "https://ahu.go.id"
    SEARCH_PATH = "/pencarian/profil-pemilik-manfaat/?tipe=bo"
    DETAIL_PATH = "/pencarian/detail-pemilik-manfaat"
    RECAPTCHA_SITE_KEY = "6LdvmHwsAAAAAEbQuvif9ubf1cfoHLkTXb859OTp"

    def __init__(self, rate_config: AhuRateLimitConfig | None = None) -> None:
        self.rate_config = rate_config or build_ahu_rate_config_from_env()
        self.session = cffi_requests.Session(impersonate="chrome110")
        self._last_request_at = 0.0
        self._form_token = ""
        self._recaptcha_token = ""
        self._recaptcha_token_at = 0.0
        self._recaptcha_token_ttl = 95.0
        self._rate_limited_until = 0.0
        self._rate_limit_cooldown = max(30.0, _env_float("AHU_RATE_LIMIT_COOLDOWN", 300.0))

        self._proxy_fail_streak = 0
        self._proxy_request_timeout = max(3.0, _env_float("AHU_PROXY_TIMEOUT", 8.0))
        self._proxy_healthcheck_enabled = _env_bool("AHU_PROXY_HEALTHCHECK_ENABLED", True)
        self._proxy_healthcheck_timeout = max(3.0, _env_float("AHU_PROXY_HEALTHCHECK_TIMEOUT", 5.0))
        self._proxy_healthcheck_probes = max(1, _env_int("AHU_PROXY_HEALTHCHECK_PROBES", 3))
        self._proxy_healthcheck_url = (
            os.getenv("AHU_PROXY_HEALTHCHECK_URL", "http://httpbin.org/ip").strip() or "http://httpbin.org/ip"
        )
        self._pre_proxy_url = os.getenv("AHU_PRE_PROXY_URL", "").strip()
        self.proxy_pool: ProxyPool | None = build_proxy_pool_from_env(prefix="AHU")
        self._proxy_disable_threshold = 0
        if self._pre_proxy_url:
            options = dict(self.session.curl_options or {})
            options[CurlOpt.PRE_PROXY] = self._pre_proxy_url
            self.session.curl_options = options
            logger.info("AHU 前置代理已启用：%s", self._pre_proxy_url)
        if self.proxy_pool and self.proxy_pool.enabled:
            configured = _env_int("AHU_PROXY_DISABLE_THRESHOLD", 0)
            self._proxy_disable_threshold = configured if configured > 0 else 3
            logger.info(
                "AHU 代理池已启用（异常熔断阈值=%d）",
                self._proxy_disable_threshold,
            )
            if self._proxy_healthcheck_enabled:
                self._run_proxy_healthcheck()

        api_key = os.getenv("TWO_CAPTCHA_API_KEY", "").strip()
        self.solver: TwoCaptchaSolver | None = None
        if api_key:
            solver_config = TwoCaptchaConfig(
                api_key=api_key,
                site_key=self.RECAPTCHA_SITE_KEY,
                page_url=f"{self.BASE_URL}{self.SEARCH_PATH}",
                action="cari",
            )
            self.solver = TwoCaptchaSolver(solver_config)

    def _throttle(self) -> None:
        """执行最小间隔限速。"""
        delta = time.time() - self._last_request_at
        wait = self.rate_config.request_delay - delta
        if wait > 0:
            time.sleep(wait)
        self._last_request_at = time.time()

    def _wait_global_rate_limit(self) -> None:
        """命中全局限流窗口时先等待。"""
        wait = self._rate_limited_until - time.time()
        if wait > 0:
            logger.warning("AHU 全局限流冷却中，等待 %.1fs", wait)
            time.sleep(wait)

    def _arm_global_rate_limit(self, wait_seconds: float) -> float:
        """设置全局限流冷却窗口。"""
        wait = max(self.rate_config.rate_limit_wait, float(wait_seconds))
        self._rate_limited_until = max(self._rate_limited_until, time.time() + wait)
        return wait

    def _request_with_retry(self, method: str, url: str, **kwargs) -> cffi_requests.Response:
        """请求重试：对超时与 429 限流重试，其他错误直接抛出。"""
        max_retries = max(1, int(self.rate_config.max_retries))
        for attempt in range(1, max_retries + 1):
            self._wait_global_rate_limit()
            self._throttle()
            lease = self.proxy_pool.acquire() if self.proxy_pool else None
            request_kwargs = dict(kwargs)
            if lease is not None:
                request_kwargs["proxy"] = lease.proxy_url
                timeout_value = request_kwargs.get("timeout", self.rate_config.timeout)
                if isinstance(timeout_value, (int, float)):
                    request_kwargs["timeout"] = max(1.0, min(float(timeout_value), self._proxy_request_timeout))
                else:
                    request_kwargs["timeout"] = self._proxy_request_timeout
            try:
                request_fn = getattr(self.session, method)
                response = request_fn(url, **request_kwargs)
                if response.status_code == 429:
                    raise RuntimeError("HTTP Error 429: CHttpException")
                response.raise_for_status()
                if self.proxy_pool and lease is not None:
                    self.proxy_pool.mark_success(lease.endpoint_id)
                self._proxy_fail_streak = 0
                return response
            except Exception as exc:  # noqa: BLE001
                message = str(exc).lower()
                is_timeout = "(28)" in message or "timed out" in message
                is_rate_limited = "429" in message
                is_proxy_error = (
                    "connect tunnel failed" in message
                    or "proxy" in message
                    or (lease is not None and "403" in message)
                )
                proxy_related = lease is not None and (is_proxy_error or is_timeout)
                if self.proxy_pool and lease is not None and proxy_related:
                    cooldown = self.proxy_pool.mark_failure(lease.endpoint_id)
                    logger.warning(
                        "AHU 代理节点失败 %s，冷却 %ds（第%d次）",
                        lease.label,
                        cooldown,
                        attempt,
                    )
                if proxy_related:
                    self._proxy_fail_streak += 1
                    if self._proxy_disable_threshold > 0 and self._proxy_fail_streak >= self._proxy_disable_threshold:
                        self._disable_proxy_pool(
                            f"连续代理异常 {self._proxy_fail_streak} 次，自动降级直连"
                        )
                retryable = is_timeout or is_rate_limited or is_proxy_error
                if is_rate_limited:
                    wait = self._arm_global_rate_limit(min(self.rate_config.rate_limit_wait * attempt, 90.0))
                    if attempt >= max_retries:
                        cooldown = self._arm_global_rate_limit(self._rate_limit_cooldown)
                        raise AhuRateLimitError(cooldown) from exc
                if not retryable or attempt >= max_retries:
                    raise
                if is_rate_limited:
                    logger.warning(
                        "AHU 请求触发 429，重试 %d/%d: %s (等待 %.1fs)",
                        attempt,
                        max_retries,
                        url,
                        wait,
                    )
                elif is_proxy_error:
                    wait = min(self.rate_config.retry_backoff * attempt, 8.0)
                    logger.warning(
                        "AHU 代理请求异常，重试 %d/%d: %s (等待 %.1fs)",
                        attempt,
                        max_retries,
                        url,
                        wait,
                    )
                else:
                    wait = min(self.rate_config.retry_backoff * attempt, 10.0)
                    logger.warning(
                        "AHU 请求超时，重试 %d/%d: %s (等待 %.1fs)",
                        attempt,
                        max_retries,
                        url,
                        wait,
                    )
                time.sleep(wait)
        raise RuntimeError(f"AHU 请求失败，超过最大重试次数: {url}")

    def _disable_proxy_pool(self, reason: str) -> None:
        """禁用代理池并降级直连。"""
        if not self.proxy_pool:
            return
        logger.error("AHU 代理池已禁用：%s", reason)
        self.proxy_pool = None
        self._proxy_disable_threshold = 0

    def _run_proxy_healthcheck(self) -> None:
        """启动时探测代理池可用性，全部失败时自动降级直连。"""
        if not self.proxy_pool or not self.proxy_pool.enabled:
            return

        total = self.proxy_pool.size
        max_probes = max(1, min(total, self._proxy_healthcheck_probes))
        tested_ids: set[int] = set()
        healthy = 0
        for _ in range(max_probes):
            lease = self.proxy_pool.acquire()
            if lease is None or lease.endpoint_id in tested_ids:
                continue
            tested_ids.add(lease.endpoint_id)
            try:
                response = self.session.get(
                    self._proxy_healthcheck_url,
                    proxy=lease.proxy_url,
                    timeout=self._proxy_healthcheck_timeout,
                    headers={"accept": "*/*"},
                )
                response.raise_for_status()
                healthy += 1
                self.proxy_pool.mark_success(lease.endpoint_id)
            except Exception as exc:  # noqa: BLE001
                cooldown = self.proxy_pool.mark_failure(lease.endpoint_id)
                logger.warning(
                    "AHU 代理健康检查失败 %s，冷却 %ds: %s",
                    lease.label,
                    cooldown,
                    exc,
                )

        if healthy <= 0:
            self._disable_proxy_pool(f"健康检查失败 0/{max(1, len(tested_ids))}")
        else:
            logger.info("AHU 代理健康检查通过：%d/%d", healthy, max(1, len(tested_ids)))

    def _load_form_token(self, force_refresh: bool = False) -> str:
        """从搜索页加载隐藏参数 `mxyplyzyk`。"""
        if self._form_token and not force_refresh:
            return self._form_token

        response = self._request_with_retry(
            "get",
            f"{self.BASE_URL}{self.SEARCH_PATH}",
            headers={
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                "accept-language": "zh-CN,zh;q=0.9,en-US;q=0.8,en;q=0.7",
            },
            timeout=self.rate_config.timeout,
        )
        token = extract_form_token(response.text)
        if not token:
            raise RuntimeError("AHU 搜索页未找到 mxyplyzyk")
        self._form_token = token
        return token

    def solve_recaptcha_token(self) -> str:
        """请求 reCAPTCHA v3 token。"""
        return self.get_recaptcha_token()

    def get_recaptcha_token(self, force_refresh: bool = False) -> str:
        """获取可复用的 reCAPTCHA token（过期后自动刷新）。"""
        if self.solver is None:
            raise CaptchaSolveError("缺少 TWO_CAPTCHA_API_KEY，无法获取 reCAPTCHA token")
        if not force_refresh and self._recaptcha_token:
            age = time.time() - self._recaptcha_token_at
            if age < self._recaptcha_token_ttl:
                return self._recaptcha_token
        token = self.solver.solve_token()
        self._recaptcha_token = token
        self._recaptcha_token_at = time.time()
        return token

    def _is_captcha_rejected(self, html: str) -> bool:
        """判断响应是否提示验证码失效。"""
        lower = " ".join(html.lower().split())
        # 仅匹配“明确验证码失败”文案，避免把页面正常 recaptcha 脚本当成失败信号。
        keywords = [
            "captcha tidak valid",
            "captcha salah",
            "gagal verifikasi captcha",
            "gagal verifikasi recaptcha",
            "token captcha tidak valid",
            "token recaptcha tidak valid",
            "invalid captcha",
            "invalid recaptcha",
            "captcha verification failed",
            "recaptcha verification failed",
            "silakan isi captcha",
            "harap isi captcha",
        ]
        return any(keyword in lower for keyword in keywords)

    def _post_search(self, query_name: str, recaptcha_token: str) -> str:
        """提交 AHU 搜索请求并返回原始 HTML。"""
        token = self._load_form_token()
        payload = {
            "recaptcha-version": "3",
            "g-recaptcha-response": recaptcha_token,
            "mxyplyzyk": token,
            "jenis_korporasi": "1",
            "nama": query_name,
        }
        response = self._request_with_retry(
            "post",
            f"{self.BASE_URL}{self.SEARCH_PATH}",
            data=payload,
            headers={
                "content-type": "application/x-www-form-urlencoded",
                "origin": self.BASE_URL,
                "referer": f"{self.BASE_URL}{self.SEARCH_PATH}",
                "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            },
            timeout=self.rate_config.timeout,
        )
        return response.text

    def search(self, company_name: str, recaptcha_token: str = "") -> list[AhuSearchResult]:
        """提交搜索请求并解析结果列表（自动复用/刷新验证码 token）。"""
        query_name = normalize_ahu_query_name(company_name)
        if not query_name:
            return []

        token = recaptcha_token.strip() if recaptcha_token else self.get_recaptcha_token()
        html = self._post_search(query_name, token)
        results = parse_search_results(html)
        if results:
            return results

        # 验证码过期或被拒时，强制刷新一次 token 重试。
        if self._is_captcha_rejected(html):
            token = self.get_recaptcha_token(force_refresh=True)
            html = self._post_search(query_name, token)
            return parse_search_results(html)

        return []

    def fetch_detail(self, detail_id: str) -> AhuDetail:
        """根据搜索结果中的 `detail_id` 获取法人详情。"""
        encoded = quote(detail_id, safe="%")
        response = self._request_with_retry(
            "get",
            f"{self.BASE_URL}{self.DETAIL_PATH}?id_korporasi={encoded}",
            headers={
                "accept": "application/json, text/javascript, */*; q=0.01",
                "referer": f"{self.BASE_URL}{self.SEARCH_PATH}",
                "x-requested-with": "XMLHttpRequest",
            },
            timeout=self.rate_config.timeout,
        )
        return parse_detail_payload(response.text)

    def close(self) -> None:
        """释放连接资源。"""
        self.session.close()
        if self.solver is not None:
            self.solver.close()

