"""IEATPE 协议客户端。"""

from __future__ import annotations

import json
import re
import time
from typing import Any

from curl_cffi.requests import Session


LIST_ENDPOINT = "https://www.ieatpe.org.tw/MemberProvider.ashx"
DETAIL_PAGE_URL = "https://www.ieatpe.org.tw/qry/query.aspx"


class IeatpeClient:
    """IEATPE 会员列表与详情协议客户端。"""

    def __init__(
        self,
        *,
        session: Session | Any | None = None,
        timeout_seconds: float = 30.0,
        request_delay: float = 0.2,
        proxy_url: str = "",
    ) -> None:
        self._timeout = timeout_seconds
        self._delay = request_delay
        self._session = session or self._build_session(proxy_url)

    def fetch_company_list(self, *, letter: str, flow: str) -> list[dict[str, str]]:
        """按字母抓公司列表。"""
        payload = {"qry": json.dumps({"type": 1, "flow": flow, "input": letter}, ensure_ascii=False)}
        response = self._post_form(payload)
        data = response.json()
        records: list[dict[str, str]] = []
        for item in data:
            member_id = str(item.get("id", "")).strip()
            if not member_id:
                continue
            records.append(
                {
                    "member_id": member_id,
                    "company_name": str(item.get("Cname", "")).strip(),
                    "representative": str(item.get("Cowner", "")).strip(),
                    "address": str(item.get("Caddr", "")).strip(),
                    "capital": str(item.get("cpt", "")).strip(),
                    "query_letter": letter,
                    "flow": str(flow).strip(),
                    "detail_url": DETAIL_PAGE_URL,
                }
            )
        return records

    def fetch_company_detail(self, *, member_id: str, flow: str) -> dict[str, str]:
        """抓单个会员详情。"""
        payload = {"detl": json.dumps({"flow": flow, "input": member_id}, ensure_ascii=False)}
        response = self._post_form(payload)
        data = response.json()
        emails = self._merge_emails(data.get("email", ""), data.get("email2", ""))
        return {
            "member_id": str(data.get("id", member_id)).strip(),
            "company_name": str(data.get("Cname", "")).strip(),
            "representative": str(data.get("Cowner", "")).strip(),
            "website": self._normalize_website(str(data.get("url", "")).strip()),
            "phone": self._normalize_phone(str(data.get("tel", "")).strip()),
            "address": str(data.get("Caddr", "")).strip(),
            "emails": emails,
            "detail_url": DETAIL_PAGE_URL,
            "capital": str(data.get("cpt", "")).strip(),
            "english_name": str(data.get("Ename", "")).strip(),
            "english_representative": str(data.get("Eowner", "")).strip(),
        }

    def _post_form(self, data: dict[str, str]):
        time.sleep(self._delay)
        return self._session.post(
            LIST_ENDPOINT,
            data=data,
            timeout=self._timeout,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
        )

    def _build_session(self, proxy_url: str) -> Session:
        session = Session(impersonate="chrome110")
        if proxy_url:
            session.proxies = {"http": proxy_url, "https": proxy_url}
        return session

    def _merge_emails(self, *values: str) -> str:
        emails: list[str] = []
        for value in values:
            text = str(value or "").strip().lower()
            if not text or "@" not in text or text in emails:
                continue
            emails.append(text)
        return ";".join(emails)

    def _normalize_phone(self, raw: str) -> str:
        cleaned = re.sub(r"\s+", "", str(raw or ""))
        if not cleaned:
            return ""
        if cleaned.startswith("(") and ")" in cleaned:
            return cleaned
        return re.sub(r"[^\d()+-]", "", cleaned)

    def _normalize_website(self, raw: str) -> str:
        text = str(raw or "").strip()
        if text in {"", "http://", "https://"}:
            return ""
        if not text.startswith(("http://", "https://")):
            return f"https://{text}"
        return text
