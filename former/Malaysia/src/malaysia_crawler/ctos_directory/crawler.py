"""CTOS 公共目录抓取器。"""

from __future__ import annotations

import json
from pathlib import Path

from malaysia_crawler.common.http_client import HttpClient
from malaysia_crawler.common.http_client import HttpConfig
from malaysia_crawler.common.io_utils import CsvAppender
from malaysia_crawler.common.io_utils import append_jsonl
from malaysia_crawler.common.io_utils import ensure_dir
from malaysia_crawler.ctos_directory.models import CTOSCompanyDetail
from malaysia_crawler.ctos_directory.models import CTOSCompanyItem
from malaysia_crawler.ctos_directory.models import CTOSDirectoryPage
from malaysia_crawler.ctos_directory.parser import parse_company_detail_page
from malaysia_crawler.ctos_directory.parser import parse_directory_page

BASE_URL = "https://businessreport.ctoscredit.com.my"
DEFAULT_PREFIXES = "0123456789abcdefghijklmnopqrstuvwxyz"


def _load_state(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _save_state(path: Path, payload: dict) -> None:
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _normalize_prefixes(raw: str) -> list[str]:
    # 中文注释：只保留数字和英文字母，并保持去重后的顺序。
    cleaned = [ch.lower() for ch in raw if ch.isalnum()]
    return list(dict.fromkeys(cleaned))


class CTOSDirectoryCrawler:
    """抓取 CTOS 公共公司目录。"""

    def __init__(
        self,
        *,
        timeout: float = 30.0,
        delay_min: float = 0.3,
        delay_max: float = 0.8,
        verify_ssl: bool = True,
    ) -> None:
        self.client = HttpClient(
            cookie_header="",
            config=HttpConfig(
                timeout=timeout,
                min_delay=delay_min,
                max_delay=delay_max,
                verify_ssl=verify_ssl,
            ),
        )

    def _build_list_url(self, prefix: str, page: int) -> str:
        return f"{BASE_URL}/oneoffreport_api/malaysia-company-listing/{prefix}/{page}"

    def fetch_list_page(self, prefix: str, page: int) -> CTOSDirectoryPage:
        url = self._build_list_url(prefix, page)
        response = self.client.get(url)
        return parse_directory_page(response.text, response_url=str(response.url), base_url=BASE_URL)

    def fetch_detail(self, item: CTOSCompanyItem) -> CTOSCompanyDetail:
        response = self.client.get(item.detail_url)
        return parse_company_detail_page(response.text, item.detail_url)

    def _build_company_row(self, prefix: str, page: int, item: CTOSCompanyItem) -> dict:
        return {
            "source": "ctos_directory",
            "prefix": prefix,
            "page": page,
            "company_name": item.company_name,
            "registration_no": item.registration_no,
            "detail_url": item.detail_url,
            "detail_path": item.detail_path,
        }

    def _build_detail_row(
        self,
        *,
        prefix: str,
        page: int,
        item: CTOSCompanyItem,
        detail: CTOSCompanyDetail,
    ) -> dict:
        return {
            "source": "ctos_directory_detail",
            "prefix": prefix,
            "page": page,
            "company_name": detail.company_name or item.company_name,
            "registration_no": item.registration_no,
            "company_registration_no": detail.company_registration_no,
            "new_registration_no": detail.new_registration_no,
            "nature_of_business": detail.nature_of_business,
            "date_of_registration": detail.date_of_registration,
            "state": detail.state,
            "detail_url": detail.detail_url,
        }

    def crawl(
        self,
        *,
        output_dir: str | Path,
        prefixes: str = DEFAULT_PREFIXES,
        start_page: int = 1,
        max_pages_per_prefix: int | None = None,
        max_prefixes: int | None = None,
        with_detail: bool = False,
        state_file: str | Path | None = None,
    ) -> dict[str, int]:
        normalized_prefixes = _normalize_prefixes(prefixes)
        if not normalized_prefixes:
            raise ValueError("prefixes 为空，无法抓取。")

        output = ensure_dir(output_dir)
        state_path = Path(state_file) if state_file else output / "state.ctos_directory.json"
        state = _load_state(state_path)
        state_prefixes = state.get("prefixes", [])
        prefix_index = 0
        next_page = start_page
        if state_prefixes == normalized_prefixes:
            prefix_index = int(state.get("next_prefix_index", 0))
            next_page = int(state.get("next_page", start_page))

        companies_jsonl = output / "ctos_directory_companies.jsonl"
        companies_csv = output / "ctos_directory_companies.csv"
        company_fields = [
            "source",
            "prefix",
            "page",
            "company_name",
            "registration_no",
            "detail_url",
            "detail_path",
        ]
        company_writer = CsvAppender(companies_csv, company_fields)

        detail_writer: CsvAppender | None = None
        details_jsonl = output / "ctos_directory_details.jsonl"
        if with_detail:
            detail_writer = CsvAppender(
                output / "ctos_directory_details.csv",
                [
                    "source",
                    "prefix",
                    "page",
                    "company_name",
                    "registration_no",
                    "company_registration_no",
                    "new_registration_no",
                    "nature_of_business",
                    "date_of_registration",
                    "state",
                    "detail_url",
                ],
            )

        pages_done = 0
        companies_done = 0
        details_done = 0
        prefixes_done = 0
        try:
            for idx in range(prefix_index, len(normalized_prefixes)):
                if max_prefixes is not None and prefixes_done >= max_prefixes:
                    break
                prefix = normalized_prefixes[idx]
                page_no = next_page if idx == prefix_index else 1
                pages_this_prefix = 0
                while True:
                    if max_pages_per_prefix is not None and pages_this_prefix >= max_pages_per_prefix:
                        break
                    page_data = self.fetch_list_page(prefix, page_no)
                    if not page_data.companies:
                        break
                    for item in page_data.companies:
                        row = self._build_company_row(prefix, page_no, item)
                        append_jsonl(companies_jsonl, row)
                        company_writer.write_row(row)
                        companies_done += 1
                        if with_detail and detail_writer is not None:
                            detail = self.fetch_detail(item)
                            detail_row = self._build_detail_row(
                                prefix=prefix,
                                page=page_no,
                                item=item,
                                detail=detail,
                            )
                            append_jsonl(details_jsonl, detail_row)
                            detail_writer.write_row(detail_row)
                            details_done += 1

                    pages_done += 1
                    pages_this_prefix += 1
                    page_no += 1
                    _save_state(
                        state_path,
                        {
                            "prefixes": normalized_prefixes,
                            "next_prefix_index": idx,
                            "next_page": page_no,
                            "pages_done": pages_done,
                            "prefixes_done": prefixes_done,
                            "companies_done": companies_done,
                            "details_done": details_done,
                        },
                    )

                prefixes_done += 1
                next_page = 1
                _save_state(
                    state_path,
                    {
                        "prefixes": normalized_prefixes,
                        "next_prefix_index": idx + 1,
                        "next_page": 1,
                        "pages_done": pages_done,
                        "prefixes_done": prefixes_done,
                        "companies_done": companies_done,
                        "details_done": details_done,
                    },
                )
        finally:
            company_writer.close()
            if detail_writer is not None:
                detail_writer.close()

        return {
            "pages_done": pages_done,
            "prefixes_done": prefixes_done,
            "companies_done": companies_done,
            "details_done": details_done,
        }

