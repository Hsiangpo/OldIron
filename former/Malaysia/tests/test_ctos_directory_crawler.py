from pathlib import Path

from malaysia_crawler.ctos_directory.crawler import CTOSDirectoryCrawler
from malaysia_crawler.ctos_directory.models import CTOSCompanyDetail
from malaysia_crawler.ctos_directory.models import CTOSCompanyItem
from malaysia_crawler.ctos_directory.models import CTOSDirectoryPage


def test_crawl_stops_when_next_page_has_no_company(tmp_path: Path) -> None:
    crawler = CTOSDirectoryCrawler(delay_min=0.0, delay_max=0.0)

    def _fake_fetch(prefix: str, page: int) -> CTOSDirectoryPage:
        if prefix == "0" and page == 1:
            return CTOSDirectoryPage(
                prefix="0",
                current_page=1,
                next_page=2,
                companies=[
                    CTOSCompanyItem(
                        company_name="0 CALORIES HOLDINGS SDN BHD",
                        registration_no="0920539K",
                        detail_path="/oneoffreport_api/single-report/malaysia-company/0920539K/0-CALORIES-HOLDINGS-SDN-BHD",
                        detail_url=(
                            "https://businessreport.ctoscredit.com.my"
                            "/oneoffreport_api/single-report/malaysia-company/0920539K/0-CALORIES-HOLDINGS-SDN-BHD"
                        ),
                    )
                ],
            )
        return CTOSDirectoryPage(prefix=prefix, current_page=page, next_page=None, companies=[])

    crawler.fetch_list_page = _fake_fetch  # type: ignore[method-assign]
    stats = crawler.crawl(output_dir=tmp_path, prefixes="0")

    assert stats["prefixes_done"] == 1
    assert stats["pages_done"] == 1
    assert stats["companies_done"] == 1
    jsonl_path = tmp_path / "ctos_directory_companies.jsonl"
    assert jsonl_path.exists()
    assert "0 CALORIES HOLDINGS SDN BHD" in jsonl_path.read_text(encoding="utf-8")


def test_crawl_can_write_detail_rows(tmp_path: Path) -> None:
    crawler = CTOSDirectoryCrawler(delay_min=0.0, delay_max=0.0)
    item = CTOSCompanyItem(
        company_name="100 BASE TECHNOLOGY SDN BHD",
        registration_no="1180434X",
        detail_path="/oneoffreport_api/single-report/malaysia-company/1180434X/100-BASE-TECHNOLOGY-SDN-BHD",
        detail_url=(
            "https://businessreport.ctoscredit.com.my"
            "/oneoffreport_api/single-report/malaysia-company/1180434X/100-BASE-TECHNOLOGY-SDN-BHD"
        ),
    )

    def _fake_fetch(prefix: str, page: int) -> CTOSDirectoryPage:
        if page == 1:
            return CTOSDirectoryPage(prefix=prefix, current_page=1, next_page=2, companies=[item])
        return CTOSDirectoryPage(prefix=prefix, current_page=page, next_page=None, companies=[])

    def _fake_detail(_: CTOSCompanyItem) -> CTOSCompanyDetail:
        return CTOSCompanyDetail(
            detail_url=item.detail_url,
            company_name=item.company_name,
            company_registration_no="1180434X",
            new_registration_no="201601009506",
            nature_of_business="PROVISION OF SPECIALIZED TELECOMMUNICATIONS APPLICATIONS",
            date_of_registration="2016-03-22",
            state="SELANGOR",
        )

    crawler.fetch_list_page = _fake_fetch  # type: ignore[method-assign]
    crawler.fetch_detail = _fake_detail  # type: ignore[method-assign]
    stats = crawler.crawl(output_dir=tmp_path, prefixes="1", with_detail=True)

    assert stats["details_done"] == 1
    details_jsonl = (tmp_path / "ctos_directory_details.jsonl").read_text(encoding="utf-8")
    assert "201601009506" in details_jsonl

