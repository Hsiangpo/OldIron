"""Duunitori HTTP 客户端 — SSR HTML 解析。"""

from __future__ import annotations

import logging
import re
import threading
from typing import Any

from curl_cffi import requests as cffi_requests

LOGGER = logging.getLogger(__name__)

# 邮箱/电话正则
_EMAIL_RE = re.compile(r"[\w.+-]+@[\w-]+\.[\w.-]+")
_MAILTO_RE = re.compile(r'mailto:([\w.+-]+@[\w-]+\.[\w.-]+)')
_PHONE_RE = re.compile(r'(?:Puhelin|puh\.?|p\.?)[:\s]*([\+\d\s\-]{7,})', re.IGNORECASE)
# 芬兰手机号直接匹配
_FI_PHONE_RE = re.compile(r'(?:0\d{1,2}[\s\-]?\d{3,4}[\s\-]?\d{3,4}|\+358\s?\d[\s\-]?\d{3,4}[\s\-]?\d{3,4})')


class DuunitoriClient:
    """Duunitori SSR 爬虫客户端。"""

    BASE_URL = "https://duunitori.fi"
    LIST_URL = "https://duunitori.fi/tyopaikat"

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
        """获取页面 HTML。"""
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
        resp = session.get(
            url, headers=headers, proxies=proxies, timeout=self._timeout,
        )
        resp.raise_for_status()
        return resp.text

    # ---- 列表页 ----

    def fetch_list_page(self, page: int = 1) -> tuple[list[dict[str, str]], bool]:
        """爬取列表页，返回 (职位列表, 是否有下一页)。

        每个职位：{url, title, company_name, city, salary}
        """
        from bs4 import BeautifulSoup

        url = f"{self.LIST_URL}?sivu={page}" if page > 1 else self.LIST_URL
        html = self._get_html(url)
        soup = BeautifulSoup(html, "lxml")

        jobs: list[dict[str, str]] = []
        # Duunitori 列表项在 <a> 标签中，class 包含 "job-box"
        for box in soup.select("div.job-box"):
            link = box.select_one("a.job-box__hover")
            if not link:
                continue
            href = str(link.get("href", "")).strip()
            if not href or "/tyopaikat/tyo/" not in href:
                continue
            if not href.startswith("http"):
                href = f"{self.BASE_URL}{href}"
            # 提取 slug 作为 job_id
            slug = href.rstrip("/").rsplit("/", 1)[-1]
            # 标题：在同级 .job-box__content 内的 h3
            title_el = box.select_one("h3.job-box__title, h3")
            title = title_el.get_text(strip=True) if title_el else ""
            # 公司名：优先 data-company 属性，备选 logo image alt
            company = str(link.get("data-company", "")).strip()
            if not company:
                logo_img = box.select_one("img.job-box__logo")
                if logo_img:
                    alt = str(logo_img.get("alt", "")).strip()
                    if alt.endswith(" logo"):
                        company = alt[:-5].strip()
            # 城市
            location_el = box.select_one(".job-box__job-location")
            city = location_el.get_text(strip=True) if location_el else ""
            # 清理城市文本（去掉末尾的 "–"）
            city = city.rstrip("– ").strip()

            jobs.append({
                "job_id": slug,
                "url": href,
                "title": title,
                "company_name": company,
                "city": city,
            })

        # 判断下一页
        has_next = bool(soup.select_one(f'a[href*="sivu={page + 1}"]'))
        return jobs, has_next

    # ---- 详情页 ----

    def fetch_detail(self, url: str) -> dict[str, str]:
        """爬取详情页，提取联系人信息。"""
        html = self._get_html(url)
        return self.parse_detail_html(html)

    @staticmethod
    def parse_detail_html(html: str) -> dict[str, str]:
        """从详情页 HTML 提取邮箱、电话、联系人。"""
        result: dict[str, str] = {
            "email": "",
            "phone": "",
            "representative": "",
            "description": "",
            "salary": "",
        }

        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "lxml")

        # 职位描述区域
        desc_el = soup.select_one(".job-post__description, .description-box")
        desc_text = desc_el.get_text("\n", strip=True) if desc_el else ""
        result["description"] = desc_text[:2000]

        # 邮箱：优先 mailto 链接
        mailto_matches = _MAILTO_RE.findall(html)
        if mailto_matches:
            result["email"] = mailto_matches[0].strip().lower()
        else:
            email_matches = _EMAIL_RE.findall(desc_text)
            # 过滤掉明显非业务邮箱
            for em in email_matches:
                if "@duunitori" not in em.lower() and "@example" not in em.lower():
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

        # 薪资
        salary_el = soup.select_one(".job-post__salary, .salary")
        if salary_el:
            result["salary"] = salary_el.get_text(strip=True)

        # 联系人名字（常出现在 "Lisätietoja" 后面）
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
