from __future__ import annotations

import logging
import json
import threading
import time
from pathlib import Path

from thailand_crawler.models import Segment
from thailand_crawler.pipeline import discover_segments
from thailand_crawler.pipeline import run_gmap_enrichment
from thailand_crawler.pipeline import run_company_details
from thailand_crawler.pipeline import run_segment_discovery
from thailand_crawler.pipeline import run_snov_enrichment
from thailand_crawler.pipeline import _resolve_batch_limit
from thailand_crawler.pipeline import _atomic_write_jsonl


class FakeClient:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str, str, str]] = []
        self._payloads: dict[tuple[str, str, str, str], dict] = {
            ("construction", "th", "", ""): {
                "candidatesMatchedQuantityInt": 117129,
                "companyInformationGeos": [
                    {"href": "th.bangkok", "quantity": "25,411", "name": "Bangkok"},
                    {"href": "th.sing_buri", "quantity": "223", "name": "Sing Buri"},
                ],
                "relatedIndustries": {},
            },
            ("construction", "th", "bangkok", ""): {
                "candidatesMatchedQuantityInt": 25411,
                "companyInformationGeos": [
                    {"href": "th.bangkok.huai_khwang", "quantity": "744", "name": "Huai Khwang"},
                    {"href": "th.bangkok.khlong_sam_wa", "quantity": "1,723", "name": "Khlong Sam Wa"},
                ],
                "relatedIndustries": {},
            },
            ("construction", "th", "sing_buri", ""): {
                "candidatesMatchedQuantityInt": 223,
                "companyInformationGeos": [],
                "relatedIndustries": {},
            },
            ("construction", "th", "bangkok", "huai_khwang"): {
                "candidatesMatchedQuantityInt": 744,
                "companyInformationGeos": [],
                "relatedIndustries": {
                    "Residential Building Construction": "residential_building_construction"
                },
            },
            ("construction", "th", "bangkok", "khlong_sam_wa"): {
                "candidatesMatchedQuantityInt": 1723,
                "companyInformationGeos": [],
                "relatedIndustries": {
                    "Residential Building Construction": "residential_building_construction",
                    "Utility System Construction": "utility_system_construction",
                },
            },
            ("residential_building_construction", "th", "bangkok", "khlong_sam_wa"): {
                "candidatesMatchedQuantityInt": 923,
                "companyInformationGeos": [],
                "relatedIndustries": {},
            },
            ("utility_system_construction", "th", "bangkok", "khlong_sam_wa"): {
                "candidatesMatchedQuantityInt": 800,
                "companyInformationGeos": [],
                "relatedIndustries": {},
            },
        }

    def fetch_company_listing_page(self, segment: Segment, page_number: int = 1) -> dict:
        assert page_number == 1
        key = (
            segment.industry_path,
            segment.country_iso_two_code,
            segment.region_name,
            segment.city_name,
        )
        self.calls.append(key)
        return self._payloads[key]


class FakeDetailClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def fetch_company_profile(self, company_name_url: str) -> dict:
        self.calls.append(company_name_url)
        if company_name_url == "bad-company":
            raise RuntimeError("D&B 请求失败: https://www.dnb.com/business-directory/api/companyprofile")
        return {
            "overview": {
                "website": f"www.{company_name_url}.com",
                "keyPrincipal": f"{company_name_url}-principal",
                "phone": "021234567",
                "tradeStyleName": company_name_url.upper(),
                "formattedRevenue": "$1.00 million",
            }
        }


class SlowDetailClient:
    def __init__(self) -> None:
        self.active = 0
        self.max_active = 0
        self.lock = threading.Lock()

    def fetch_company_profile(self, company_name_url: str) -> dict:
        with self.lock:
            self.active += 1
            self.max_active = max(self.max_active, self.active)
        time.sleep(0.05)
        with self.lock:
            self.active -= 1
        return {
            "overview": {
                "website": f"www.{company_name_url}.com",
                "keyPrincipal": f"{company_name_url}-principal",
                "phone": "021234567",
                "tradeStyleName": company_name_url.upper(),
                "formattedRevenue": "$1.00 million",
            }
        }


class FakeGoogleMapsClient:
    def __init__(self, website_map: dict[str, str]) -> None:
        self.website_map = website_map

    def search_official_website(self, query: str) -> str:
        return self.website_map.get(query, "")


class FlakyGoogleMapsClient:
    def __init__(self, website_map: dict[str, str]) -> None:
        self.website_map = website_map

    def search_official_website(self, query: str) -> str:
        if query.startswith("BOOM"):
            raise RuntimeError("GMap timeout")
        return self.website_map.get(query, "")


class FakeSnovClient:
    def __init__(self, email_map: dict[str, list[str]]) -> None:
        self.email_map = email_map

    def get_domain_emails(self, domain: str) -> list[str]:
        return self.email_map.get(domain, [])


class FlakySnovClient:
    def __init__(self) -> None:
        self.calls: list[str] = []

    def get_domain_emails(self, domain: str) -> list[str]:
        self.calls.append(domain)
        if domain == "boom.com":
            raise RuntimeError("Snov timeout")
        return [f"hi@{domain}"]


def test_discover_segments_splits_geo_and_large_leaf_by_industry() -> None:
    client = FakeClient()
    root = Segment(
        industry_path="construction",
        country_iso_two_code="th",
        region_name="",
        city_name="",
        expected_count=117129,
        segment_type="country",
    )

    segments = discover_segments(client, root, max_leaf_records=1000)
    ids = {segment.segment_id for segment in segments}

    assert ids == {
        "construction|th|bangkok|huai_khwang",
        "construction|th|sing_buri|",
        "residential_building_construction|th|bangkok|khlong_sam_wa",
        "utility_system_construction|th|bangkok|khlong_sam_wa",
    }


def test_run_segment_discovery_emits_progress_logs(tmp_path, caplog) -> None:
    client = FakeClient()

    with caplog.at_level(logging.INFO):
        count = run_segment_discovery(tmp_path, client, max_segments=2)

    assert count == 2
    assert "切片发现开始" in caplog.text
    assert "切片发现完成" in caplog.text


def test_run_segment_discovery_respects_max_segments_early(tmp_path) -> None:
    client = FakeClient()

    count = run_segment_discovery(tmp_path, client, max_segments=1)

    assert count == 1
    assert client.calls == [
        ("construction", "th", "", ""),
        ("construction", "th", "bangkok", ""),
        ("construction", "th", "bangkok", "huai_khwang"),
    ]


def test_run_company_details_continues_after_single_timeout(tmp_path, caplog) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)
    ids_file = output_dir / "company_ids.jsonl"
    rows = [
        {"duns": "1", "company_name": "A", "company_name_url": "good-company"},
        {"duns": "2", "company_name": "B", "company_name_url": "bad-company"},
        {"duns": "3", "company_name": "C", "company_name_url": "good-company-2"},
    ]
    with ids_file.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")

    client = FakeDetailClient()

    with caplog.at_level(logging.WARNING):
        written = run_company_details(output_dir=output_dir, client=client, max_items=0)

    assert written == 2
    assert "详情失败" in caplog.text

    companies_file = output_dir / "companies.jsonl"
    with companies_file.open("r", encoding="utf-8") as fp:
        saved = [json.loads(line) for line in fp if line.strip()]

    assert sorted(row["duns"] for row in saved) == ["1", "3"]


def test_run_company_details_uses_four_workers(tmp_path) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)
    ids_file = output_dir / "company_ids.jsonl"
    rows = [
        {"duns": str(index), "company_name": f"C{index}", "company_name_url": f"company-{index}"}
        for index in range(8)
    ]
    with ids_file.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")

    client = SlowDetailClient()

    written = run_company_details(output_dir=output_dir, client=client, max_items=8, detail_concurrency=4)

    assert written == 8
    assert client.max_active == 4


def test_run_gmap_enrichment_skips_records_without_key_principal(tmp_path, monkeypatch) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)
    companies_file = output_dir / "companies.jsonl"
    rows = [
        {"duns": "1", "company_name": "A", "city": "Bangkok", "region": "Bangkok", "country": "Thailand", "key_principal": "", "website": "", "domain": "", "emails": []},
        {"duns": "2", "company_name": "B", "city": "Bangkok", "region": "Bangkok", "country": "Thailand", "key_principal": "Boss", "website": "", "domain": "", "emails": []},
    ]
    with companies_file.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")

    monkeypatch.setattr(
        "thailand_crawler.pipeline.GoogleMapsClient",
        lambda *args, **kwargs: FakeGoogleMapsClient({"B Bangkok Bangkok Thailand": "https://b.example.com"}),
    )

    updated = run_gmap_enrichment(output_dir=output_dir, max_items=0, gmap_concurrency=4)

    assert updated == 1
    enriched_file = output_dir / "companies_enriched.jsonl"
    with enriched_file.open("r", encoding="utf-8") as fp:
        saved = [json.loads(line) for line in fp if line.strip()]
    assert saved[0]["website"] == ""
    assert saved[1]["domain"] == "b.example.com"


def test_run_snov_enrichment_uses_existing_enriched_rows_and_requires_principal(tmp_path, monkeypatch) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)
    enriched_file = output_dir / "companies_enriched.jsonl"
    rows = [
        {"duns": "1", "company_name": "A", "key_principal": "", "domain": "a.com", "emails": []},
        {"duns": "2", "company_name": "B", "key_principal": "Boss", "domain": "b.com", "emails": []},
    ]
    with enriched_file.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")

    monkeypatch.setenv("SNOV_CLIENT_ID", "id")
    monkeypatch.setenv("SNOV_CLIENT_SECRET", "secret")
    monkeypatch.setattr(
        "thailand_crawler.pipeline.SnovClient",
        lambda *args, **kwargs: FakeSnovClient({"b.com": ["boss@b.com"]}),
    )

    updated = run_snov_enrichment(output_dir=output_dir, max_items=0, snov_concurrency=4)

    assert updated == 1
    emails_file = output_dir / "companies_with_emails.jsonl"
    with emails_file.open("r", encoding="utf-8") as fp:
        saved = [json.loads(line) for line in fp if line.strip()]
    assert saved[0]["emails"] == []
    assert saved[1]["emails"] == ["boss@b.com"]


def test_run_snov_enrichment_continues_after_single_worker_timeout(tmp_path, monkeypatch, caplog) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)
    enriched_file = output_dir / "companies_enriched.jsonl"
    rows = [
        {"duns": "1", "company_name": "A", "key_principal": "BossA", "domain": "ok.com", "emails": []},
        {"duns": "2", "company_name": "B", "key_principal": "BossB", "domain": "boom.com", "emails": []},
        {"duns": "3", "company_name": "C", "key_principal": "BossC", "domain": "ok2.com", "emails": []},
    ]
    with enriched_file.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")

    monkeypatch.setenv("SNOV_CLIENT_ID", "id")
    monkeypatch.setenv("SNOV_CLIENT_SECRET", "secret")
    monkeypatch.setattr("thailand_crawler.pipeline.SnovClient", lambda *args, **kwargs: FlakySnovClient())

    with caplog.at_level(logging.WARNING):
        updated = run_snov_enrichment(output_dir=output_dir, max_items=0, snov_concurrency=4)

    assert updated == 2
    assert "Snov 失败" in caplog.text
    emails_file = output_dir / "companies_with_emails.jsonl"
    with emails_file.open("r", encoding="utf-8") as fp:
        saved = [json.loads(line) for line in fp if line.strip()]
    saved_map = {row["duns"]: row for row in saved}
    assert saved_map["1"]["emails"] == ["hi@ok.com"]
    assert saved_map["2"]["emails"] == []
    assert saved_map["3"]["emails"] == ["hi@ok2.com"]


def test_run_gmap_enrichment_continues_after_single_worker_timeout(tmp_path, monkeypatch, caplog) -> None:
    output_dir = tmp_path / "output"
    output_dir.mkdir(parents=True)
    companies_file = output_dir / "companies.jsonl"
    rows = [
        {"duns": "1", "company_name": "OK ONE", "city": "Bangkok", "region": "Bangkok", "country": "Thailand", "key_principal": "Boss1", "website": "", "domain": "", "emails": []},
        {"duns": "2", "company_name": "BOOM TWO", "city": "Bangkok", "region": "Bangkok", "country": "Thailand", "key_principal": "Boss2", "website": "", "domain": "", "emails": []},
        {"duns": "3", "company_name": "OK THREE", "city": "Bangkok", "region": "Bangkok", "country": "Thailand", "key_principal": "Boss3", "website": "", "domain": "", "emails": []},
    ]
    with companies_file.open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")

    monkeypatch.setattr(
        "thailand_crawler.pipeline.GoogleMapsClient",
        lambda *args, **kwargs: FlakyGoogleMapsClient({
            "OK ONE Bangkok Bangkok Thailand": "https://one.example.com",
            "OK THREE Bangkok Bangkok Thailand": "https://three.example.com",
        }),
    )

    with caplog.at_level(logging.WARNING):
        updated = run_gmap_enrichment(output_dir=output_dir, max_items=0, gmap_concurrency=4)

    assert updated == 2
    assert "GMAP 失败" in caplog.text
    enriched_file = output_dir / "companies_enriched.jsonl"
    with enriched_file.open("r", encoding="utf-8") as fp:
        saved = [json.loads(line) for line in fp if line.strip()]
    saved_map = {row["duns"]: row for row in saved}
    assert saved_map["1"]["domain"] == "one.example.com"
    assert saved_map["2"]["domain"] == ""
    assert saved_map["3"]["domain"] == "three.example.com"


def test_atomic_write_jsonl_retries_after_transient_permission_error(tmp_path, monkeypatch) -> None:
    output_file = tmp_path / "companies_enriched.jsonl"
    attempts = {"count": 0}
    original_replace = Path.replace

    def flaky_replace(self: Path, target: Path) -> Path:
        if self.name.endswith(".tmp") and self.name.startswith("companies_enriched.jsonl.") and Path(target) == output_file and attempts["count"] == 0:
            attempts["count"] += 1
            raise PermissionError(13, "拒绝访问")
        return original_replace(self, target)

    monkeypatch.setattr(Path, "replace", flaky_replace)

    _atomic_write_jsonl(output_file, [{"duns": "1", "company_name": "A"}])

    assert attempts["count"] == 1
    with output_file.open("r", encoding="utf-8") as fp:
        saved = [json.loads(line) for line in fp if line.strip()]
    assert saved == [{"duns": "1", "company_name": "A"}]


def test_resolve_batch_limit_prefers_default_when_max_items_zero() -> None:
    assert _resolve_batch_limit(0, 100) == 100
    assert _resolve_batch_limit(50, 100) == 50
