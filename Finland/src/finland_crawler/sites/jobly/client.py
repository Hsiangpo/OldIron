"""Jobly HTTP 客户端 — SSR HTML 解析。"""

from __future__ import annotations

import logging
import re
import threading
from typing import Any

from curl_cffi import requests as cffi_requests

LOGGER = logging.getLogger(__name__)

# 正则
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_MAILTO_RE = re.compile(r'mailto:([\w.+-]+@[\w-]+\.[\w.-]+)')
_PHONE_RE = re.compile(r'(?:Puhelin|puh\.?|p\.?)[:\s]*([\+\d\s\-]{7,})', re.IGNORECASE)
_FI_PHONE_RE = re.compile(r'(?:0\d{1,2}[\s\-]?\d{3,4}[\s\-]?\d{3,4}|\+358\s?\d[\s\-]?\d{3,4}[\s\-]?\d{3,4})')


class JoblyClient:
    """Jobly.fi SSR 爬虫客户端。"""

    BASE_URL = "https://www.jobly.fi"
    LIST_URL = "https://www.jobly.fi/tyopaikat"

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

    def _get_html(self, url: str) -> str:
        session = self._get_session()
        headers = {
            "Accept": "text/html,application/xhtml+xml",
            "Accept-Language": "fi-FI,fi;q=0.9,en;q=0.8",
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/131.0.0.0 Safari/537.36"
            ),
        }
        proxies = {"https": self._proxy, "http": self._proxy} if self._proxy else None
        resp = session.get(url, headers=headers, proxies=proxies, timeout=self._timeout)
        resp.raise_for_status()
        return resp.text

    # ---- 列表页 ----

    def fetch_list_page(self, page: int = 0) -> tuple[list[dict[str, str]], bool]:
        """爬取列表页。返回 (职位列表, 是否有下一页)。"""
        from bs4 import BeautifulSoup

        url = f"{self.LIST_URL}?page={page}" if page > 0 else self.LIST_URL
        html = self._get_html(url)
        soup = BeautifulSoup(html, "lxml")

        jobs: list[dict[str, str]] = []
        # Jobly 列表项在 views-row 下
        for row in soup.select(".views-row, .view-content .node"):
            link = row.select_one("a[href*='/tyopaikat/tyo/']") or row.select_one("a[href*='/tyopaikat/']")
            if not link:
                continue
            href = str(link.get("href", "")).strip()
            if not href:
                continue
            if not href.startswith("http"):
                href = f"{self.BASE_URL}{href}"
            # 提取唯一标识
            slug = href.rstrip("/").rsplit("/", 1)[-1]
            title = link.get_text(strip=True) or ""
            # 公司名
            company_el = row.select_one(".job-company, .field--name-field-company-name")
            company = company_el.get_text(strip=True) if company_el else ""
            # 地点
            location_el = row.select_one(".job-location, .field--name-field-job-location")
            city = location_el.get_text(strip=True) if location_el else ""

            jobs.append({
                "job_id": slug,
                "url": href,
                "title": title,
                "company_name": company,
                "city": city,
            })

        # 判断下一页
        next_link = soup.select_one(f'a[href*="page={page + 1}"]') or soup.select_one("li.pager__item--next a")
        has_next = bool(next_link)
        return jobs, has_next

    # ---- 详情页 ----

    def fetch_detail(self, url: str) -> dict[str, str]:
        html = self._get_html(url)
        return self.parse_detail_html(html)

    @staticmethod
    def parse_detail_html(html: str) -> dict[str, str]:
        """从详情页 HTML 提取联系人信息。"""
        result: dict[str, str] = {
            "email": "", "phone": "", "representative": "", "description": "",
        }
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")

        # 职位描述
        desc_el = soup.select_one(".field--name-body, .job-description, article .content")
        desc_text = desc_el.get_text("\n", strip=True) if desc_el else ""
        result["description"] = desc_text[:2000]

        # 邮箱
        mailto_matches = _MAILTO_RE.findall(html)
        if mailto_matches:
            result["email"] = mailto_matches[0].strip().lower()
        else:
            email_matches = _EMAIL_RE.findall(desc_text)
            for em in email_matches:
                if "@jobly" not in em.lower() and "@example" not in em.lower():
                    result["email"] = em.strip().lower()
                    break

        # 电话
        phone_matches = _PHONE_RE.findall(desc_text)
        if phone_matches:
            result["phone"] = re.sub(r"[^\d+]", "", phone_matches[0])
        else:
            fi_phones = _FI_PHONE_RE.findall(desc_text)
            if fi_phones:
                result["phone"] = re.sub(r"[^\d+]", "", fi_phones[0])

        # 联系人
        contact_patterns = [
            r'Lisätietoja[:\s]*(?:antaa|antavat)?\s*\n?\s*([A-ZÄÖÜ][a-zäöüå]+\s+[A-ZÄÖÜ][a-zäöüå]+)',
            r'yhteydessä[:\s]*\n?\s*([A-ZÄÖÜ][a-zäöüå]+\s+[A-ZÄÖÜ][a-zäöüå]+)',
            r'tiedustelut[:\s]*\n?\s*([A-ZÄÖÜ][a-zäöüå]+\s+[A-ZÄÖÜ][a-zäöüå]+)',
        ]
        for pattern in contact_patterns:
            match = re.search(pattern, desc_text)
            if match:
                result["representative"] = match.group(1).strip()
                break

        return result
