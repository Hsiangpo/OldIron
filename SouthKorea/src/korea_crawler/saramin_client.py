"""saramin.co.kr HTTP 客户端。"""

from __future__ import annotations

import json
import logging
import random
import re
import time
from dataclasses import dataclass
from typing import Any

from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

LIST_PATH = "/zf_user/jobs/list/domestic"
COMPANY_PATH = "/zf_user/company-info/view"
HOMEPAGE_PATH = "/zf_user/track-apply-form/render-homepage"

DEFAULT_LOC_MCD = (
    "101000,102000,103000,104000,105000,106000,107000,108000,109000,"
    "110000,111000,112000,113000,114000,115000,116000,117000,118000"
)

BASE_HEADERS = {
    "accept-language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "referer": "https://www.saramin.co.kr/zf_user/jobs/list/domestic",
}

LIST_HEADERS = {
    **BASE_HEADERS,
    "accept": "text/html, */*; q=0.01",
    "x-requested-with": "XMLHttpRequest",
}

HTML_HEADERS = {
    **BASE_HEADERS,
    "accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}


@dataclass(slots=True)
class RateLimitConfig:
    min_delay: float = 0.2
    max_delay: float = 0.7
    long_rest_interval: int = 300
    long_rest_seconds: float = 5.0


class SaraminClient:
    """saramin.co.kr 客户端，封装列表和详情请求。"""

    BASE_URL = "https://www.saramin.co.kr"

    def __init__(self, rate_config: RateLimitConfig | None = None) -> None:
        self.rate_config = rate_config or RateLimitConfig()
        self._request_count = 0
        self.session = self._build_session()

    def _build_session(self) -> cffi_requests.Session:
        return cffi_requests.Session(impersonate="chrome110")

    def _reset_session(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
        self.session = self._build_session()

    def _sleep(self) -> None:
        delay = random.uniform(self.rate_config.min_delay, self.rate_config.max_delay)
        time.sleep(delay)
        self._request_count += 1
        if (
            self.rate_config.long_rest_interval > 0
            and self._request_count % self.rate_config.long_rest_interval == 0
        ):
            logger.info(
                "Saramin 已请求 %d 次，休息 %.0fs",
                self._request_count,
                self.rate_config.long_rest_seconds,
            )
            time.sleep(self.rate_config.long_rest_seconds)

    def _request(
        self,
        path: str,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        max_retries: int = 4,
    ) -> cffi_requests.Response:
        url = f"{self.BASE_URL}{path}"
        req_headers = headers or HTML_HEADERS
        params = params or {}

        for attempt in range(1, max_retries + 1):
            self._sleep()
            try:
                resp = self.session.get(
                    url,
                    params=params,
                    headers=req_headers,
                    timeout=30,
                )
            except Exception as exc:
                err_text = str(exc)
                logger.warning("Saramin 请求异常 (第%d次): %s — %s", attempt, url, err_text)
                if re.search(r"curl: \((28|35|56)\)", err_text):
                    self._reset_session()
                if attempt == max_retries:
                    raise RuntimeError(f"请求失败，已重试 {max_retries} 次: {url}") from exc
                backoff = min((2 ** attempt) + random.uniform(0, 1.0), 20)
                time.sleep(backoff)
                continue

            if resp.status_code == 429:
                wait = 2 ** attempt * 5
                logger.warning("Saramin 429 限流，等待 %ds (第%d次)", wait, attempt)
                time.sleep(wait)
                continue
            if resp.status_code == 403:
                raise RuntimeError(f"Saramin 403 Forbidden: {url}")
            if resp.status_code >= 500:
                if attempt == max_retries:
                    raise RuntimeError(f"Saramin 服务端错误 {resp.status_code}: {url}")
                backoff = min((2 ** attempt) + random.uniform(0, 1.0), 20)
                logger.warning(
                    "Saramin 服务端错误 %d，重试 (第%d次): %s",
                    resp.status_code,
                    attempt,
                    url,
                )
                time.sleep(backoff)
                continue

            resp.raise_for_status()
            return resp

        raise RuntimeError(f"请求失败，已重试 {max_retries} 次: {url}")

    def get_list_json(
        self,
        page: int,
        loc_mcd: str = DEFAULT_LOC_MCD,
        page_count: int = 100,
    ) -> dict[str, Any]:
        """获取列表接口响应。"""
        params = {
            "page": page,
            "loc_mcd": loc_mcd,
            "search_optional_item": "n",
            "search_done": "y",
            "panel_count": "y",
            "preview": "y",
            "isAjaxRequest": "1",
            "page_count": str(page_count),
            "sort": "RL",
            "type": "domestic",
            "is_param": "1",
            "isSearchResultEmpty": "1",
            "isSectionHome": "0",
            "searchParamCount": "1",
        }
        resp = self._request(LIST_PATH, params=params, headers=LIST_HEADERS)
        try:
            data = json.loads(resp.text)
        except json.JSONDecodeError as exc:
            raise RuntimeError(f"Saramin 列表响应不是 JSON: page={page}") from exc
        if not isinstance(data, dict):
            raise RuntimeError(f"Saramin 列表响应结构异常: page={page}")
        return data

    def get_company_html(self, csn: str) -> str:
        """获取公司详情页 HTML。"""
        resp = self._request(COMPANY_PATH, params={"csn": csn}, headers=HTML_HEADERS)
        return resp.text

    def get_homepage_from_rec_idx(self, rec_idx: str) -> str:
        """通过职位页兜底接口提取官网 URL。"""
        resp = self._request(HOMEPAGE_PATH, params={"rec_idx": rec_idx}, headers=HTML_HEADERS)
        text = resp.text or ""
        patterns = [
            r'document\.location\.replace\("([^"]+)"\)',
            r'location\.href\s*=\s*"([^"]+)"',
            r"location\.replace\('([^']+)'\)",
        ]
        for pattern in patterns:
            m = re.search(pattern, text, flags=re.IGNORECASE)
            if m and m.group(1).startswith(("http://", "https://")):
                return m.group(1).strip()
        return ""

    def close(self) -> None:
        self.session.close()

