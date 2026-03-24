"""Työmarkkinatori HTTP 客户端 — 公开 REST API，无需鉴权。"""

from __future__ import annotations

import logging
import re
import threading
from typing import Any

from curl_cffi import requests as cffi_requests

LOGGER = logging.getLogger(__name__)


class TmtClient:
    """Työmarkkinatori API 客户端。

    搜索接口：POST /api/jobpostingfulltext/search/v1/search
    详情接口：GET  /api/jobposting/v1/jobpostings/{id}
    """

    BASE_URL = "https://tyomarkkinatori.fi"

    def __init__(
        self,
        *,
        timeout_seconds: float = 30.0,
        proxy_url: str = "",
    ) -> None:
        self._timeout = timeout_seconds
        self._proxy = proxy_url
        self._session_local = threading.local()

    def _get_session(self) -> cffi_requests.Session:
        session = getattr(self._session_local, "session", None)
        if session is None:
            session = cffi_requests.Session(impersonate="chrome131")
            self._session_local.session = session
        return session

    def _request(self, method: str, path: str, **kwargs: Any) -> Any:
        session = self._get_session()
        url = f"{self.BASE_URL}{path}"
        headers = kwargs.pop("headers", {})
        headers.setdefault("Accept", "application/json, text/plain, */*")
        headers.setdefault("User-Agent", (
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/131.0.0.0 Safari/537.36"
        ))
        proxies = {"https": self._proxy, "http": self._proxy} if self._proxy else None
        resp = session.request(
            method, url,
            headers=headers,
            proxies=proxies,
            timeout=self._timeout,
            **kwargs,
        )
        resp.raise_for_status()
        return resp.json()

    # ---- 搜索 API ----

    def search_jobs(
        self,
        *,
        page_number: int = 0,
        page_size: int = 100,
        query: str = "",
    ) -> tuple[list[dict[str, Any]], int]:
        """搜索职位列表。

        返回 (职位列表, 总数)。
        """
        payload = {
            "query": query,
            "paging": {
                "pageSize": page_size,
                "pageNumber": page_number,
            },
            "filters": {
                "publishedAfter": None,
                "closesBefore": None,
            },
        }
        data = self._request(
            "POST",
            "/api/jobpostingfulltext/search/v1/search",
            json=payload,
            headers={"Content-Type": "application/json"},
        )
        # API 返回 content / totalElements
        jobs = data.get("content", []) or data.get("jobPostings", [])
        total = int(data.get("totalElements", 0) or data.get("totalCount", 0))
        return jobs, total

    # ---- 详情 API ----

    def fetch_job_detail(self, job_id: str) -> dict[str, Any]:
        """获取职位完整详情（含联系人信息）。"""
        return self._request("GET", f"/api/jobposting/v1/jobpostings/{job_id}")

    # ---- 解析 ----

    @staticmethod
    def parse_search_job(raw: dict[str, Any], page: int = 0):
        """将搜索结果解析为 TmtJobPosting。"""
        from finland_crawler.sites.tyomarkkinatori.models import TmtJobPosting

        title_vals = raw.get("title", {})
        if isinstance(title_vals, dict):
            title_vals = title_vals.get("values", title_vals)
        title = ""
        if isinstance(title_vals, dict):
            title = title_vals.get("fi") or title_vals.get("en") or next(iter(title_vals.values()), "")
        elif isinstance(title_vals, str):
            title = title_vals

        # 搜索结果公司名嵌套在 employer 下
        employer = raw.get("employer", {}) or {}
        biz_name_vals = employer.get("businessName", {}) or raw.get("businessName", {})
        if isinstance(biz_name_vals, dict):
            biz_name_vals = biz_name_vals.get("values", biz_name_vals)
        biz_name = ""
        if isinstance(biz_name_vals, dict):
            biz_name = biz_name_vals.get("fi") or next(iter(biz_name_vals.values()), "")
        elif isinstance(biz_name_vals, str):
            biz_name = biz_name_vals
        # employer.name 作为备用
        if not biz_name:
            biz_name = str(employer.get("name", ""))

        # businessId 可能在 employer 内
        biz_id = str(raw.get("businessId", "") or employer.get("businessId", ""))

        return TmtJobPosting(
            job_id=str(raw.get("id", "")),
            title=str(title),
            company_name=str(biz_name),
            business_id=biz_id,
            city=str((raw.get("location") or {}).get("municipality", "") or raw.get("postOffice", "")),
            source_page=page,
        )

    @staticmethod
    def enrich_with_detail(posting, detail: dict[str, Any]) -> None:
        """用详情 API 数据补全职位记录。"""
        # 联系人信息
        recruiting = detail.get("recruiting") or []
        if recruiting:
            first = recruiting[0]
            first_name = str(first.get("firstName", "")).strip()
            last_name = str(first.get("lastName", "")).strip()
            posting.representative = f"{first_name} {last_name}".strip()
            posting.email = str(first.get("email", "")).strip().lower()
            posting.phone = str(first.get("telephone", "")).strip()

        # 公司官网
        ext_links = detail.get("externalLinks") or []
        for link in ext_links:
            url = str(link.get("url", "")).strip()
            if url:
                posting.homepage = url
                break

        # 公司名
        biz_vals = detail.get("businessName", {})
        if isinstance(biz_vals, dict):
            biz_vals = biz_vals.get("values", biz_vals)
        if isinstance(biz_vals, dict):
            name = biz_vals.get("fi") or next(iter(biz_vals.values()), "")
            if name:
                posting.company_name = str(name)

        # 职位元数据
        posting.business_id = str(detail.get("businessId", posting.business_id))
        posting.address = str(detail.get("postalAddress", "")).strip()
        posting.postcode = str(detail.get("postalNumber", "")).strip()
        posting.city = str(detail.get("postOffice", posting.city)).strip()
        posting.work_time = str(detail.get("workTime", "")).strip()
        posting.employment_type = str(detail.get("employmentRelationship", "")).strip()
        posting.duration = str(detail.get("durationOfPosition", "")).strip()
        posting.publish_date = str(detail.get("publishDate", "")).strip()
        posting.end_date = str(detail.get("applicationPeriodEndDate", "")).strip()
        posting.industry_code = str(detail.get("industryCode", "")).strip()

        # 薪资
        wage_info = detail.get("wagePrincipleInfo", {})
        if isinstance(wage_info, dict):
            wage_vals = wage_info.get("values", {})
            if isinstance(wage_vals, dict):
                posting.salary_info = str(
                    wage_vals.get("fi") or next(iter(wage_vals.values()), "")
                )

        # 申请链接
        app_url = detail.get("applicationUrl", {})
        if isinstance(app_url, dict):
            app_vals = app_url.get("values", {})
            if isinstance(app_vals, dict):
                posting.application_url = str(
                    app_vals.get("fi") or next(iter(app_vals.values()), "")
                )

        # 描述
        desc = detail.get("jobDescription", {})
        if isinstance(desc, dict):
            desc_vals = desc.get("values", {})
            if isinstance(desc_vals, dict):
                posting.description = str(
                    desc_vals.get("fi") or next(iter(desc_vals.values()), "")
                )[:2000]

        # 区域
        regions = detail.get("regions") or []
        if regions:
            posting.region = ",".join(str(r) for r in regions)
