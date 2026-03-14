"""BusinessList 公司档案抓取器。"""

from __future__ import annotations

import json
from dataclasses import replace
from pathlib import Path
from urllib.parse import urljoin

from malaysia_crawler.businesslist.models import BusinessListCompany
from malaysia_crawler.businesslist.parser import parse_company_page
from malaysia_crawler.businesslist.parser import parse_redir_target
from malaysia_crawler.common.http_client import HttpClient
from malaysia_crawler.common.http_client import HttpConfig
from malaysia_crawler.common.io_utils import CsvAppender
from malaysia_crawler.common.io_utils import append_jsonl
from malaysia_crawler.common.io_utils import ensure_dir

BASE_URL = "https://www.businesslist.my"


def _load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_state(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_website_url(raw: str) -> str:
    value = raw.strip()
    if not value:
        return ""
    if value.startswith("http://") or value.startswith("https://"):
        return value
    if value.startswith("//"):
        return f"https:{value}"
    if value.startswith("www."):
        return f"https://{value}"
    if value.startswith("/"):
        return urljoin(BASE_URL, value)
    return f"https://{value}"


class BusinessListCrawler:
    """按 company_id 区间抓取 BusinessList 公司档案。"""

    def __init__(
        self,
        *,
        timeout: float = 30.0,
        delay_min: float = 0.3,
        delay_max: float = 0.8,
        verify_ssl: bool = True,
        proxy_url: str = "",
        use_system_proxy: bool = False,
    ) -> None:
        proxy = proxy_url.strip()
        self.client = HttpClient(
            cookie_header="",
            config=HttpConfig(
                timeout=timeout,
                min_delay=delay_min,
                max_delay=delay_max,
                verify_ssl=verify_ssl,
                use_system_proxy=use_system_proxy,
            ),
        )
        if proxy:
            self.client.session.proxies.update({"http": proxy, "https": proxy})

    def _request(self, url: str, *, allow_redirects: bool = True):
        self.client.sleep_random()
        response = self.client.session.get(
            url,
            timeout=self.client.config.timeout,
            verify=self.client.config.verify_ssl,
            allow_redirects=allow_redirects,
        )
        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response

    def resolve_website_url(self, website_href: str) -> str:
        href = website_href.strip()
        if not href:
            return ""
        if href.startswith("/redir/"):
            redir_url = urljoin(BASE_URL, href)
            response = self._request(redir_url, allow_redirects=False)
            if response is None:
                return ""
            target = parse_redir_target(response.text)
            if not target:
                return ""
            return _normalize_website_url(target)
        return _normalize_website_url(href)

    def fetch_company_profile(self, company_id: int) -> BusinessListCompany | None:
        url = f"{BASE_URL}/company/{company_id}"
        response = self._request(url, allow_redirects=True)
        if response is None:
            return None
        parsed = parse_company_page(response.text, response_url=str(response.url))
        if parsed is None:
            return None
        resolved_website = self.resolve_website_url(parsed.website_href)
        return replace(parsed, website_url=resolved_website)

    def _build_row(self, company: BusinessListCompany) -> dict:
        return {
            "company_name": company.company_name,
            "contact_numbers": json.dumps(company.contact_numbers, ensure_ascii=False),
            "website_url": company.website_url,
            "contact_email": company.contact_email,
            "company_manager": company.company_manager,
            "employees": json.dumps(company.employees, ensure_ascii=False),
        }

    def crawl(
        self,
        *,
        output_dir: str | Path,
        start_id: int = 1,
        end_id: int = 500_000,
        max_companies: int | None = None,
        state_file: str | Path | None = None,
    ) -> dict[str, int]:
        if start_id <= 0 or end_id < start_id:
            raise ValueError("company_id 区间非法。")

        output = ensure_dir(output_dir)
        state_path = Path(state_file) if state_file else output / "state.businesslist.json"
        state = _load_state(state_path)
        if state.get("next_id") and start_id < int(state["next_id"]) <= end_id:
            start_id = int(state["next_id"])

        jsonl_path = output / "businesslist_companies.jsonl"
        csv_path = output / "businesslist_companies.csv"
        csv_writer = CsvAppender(
            csv_path,
            [
                "company_name",
                "contact_numbers",
                "website_url",
                "contact_email",
                "company_manager",
                "employees",
            ],
        )

        scanned_ids = 0
        companies_done = 0
        next_id = start_id
        try:
            for company_id in range(start_id, end_id + 1):
                if max_companies is not None and companies_done >= max_companies:
                    break
                next_id = company_id + 1
                scanned_ids += 1
                company = self.fetch_company_profile(company_id)
                if company is None:
                    _save_state(
                        state_path,
                        {
                            "next_id": next_id,
                            "scanned_ids": scanned_ids,
                            "companies_done": companies_done,
                            "last_company_id": company_id,
                        },
                    )
                    continue
                row = self._build_row(company)
                append_jsonl(jsonl_path, row)
                csv_writer.write_row(row)
                companies_done += 1
                _save_state(
                    state_path,
                    {
                        "next_id": next_id,
                        "scanned_ids": scanned_ids,
                        "companies_done": companies_done,
                        "last_company_id": company_id,
                    },
                )
        finally:
            csv_writer.close()

        return {
            "start_id": start_id,
            "next_id": next_id,
            "end_id": end_id,
            "scanned_ids": scanned_ids,
            "companies_done": companies_done,
        }
