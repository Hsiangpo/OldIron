from __future__ import annotations

import csv
import json

from thailand_crawler.delivery import build_delivery_bundle
from thailand_crawler.delivery import parse_day_label


def test_parse_day_label() -> None:
    assert parse_day_label("day1") == 1
    assert parse_day_label("DAY12") == 12


def test_build_delivery_bundle_outputs_incremental_csv(tmp_path) -> None:
    data_root = tmp_path / "output"
    site_dir = data_root / "dnb"
    delivery_root = data_root / "delivery"
    site_dir.mkdir(parents=True)

    rows = [
        {
            "duns": "1",
            "company_name": "ACME",
            "key_principal": "Alice",
            "emails": ["a@example.com"],
            "domain": "example.com",
            "phone": "021234567",
        },
        {
            "duns": "1",
            "company_name": "ACME",
            "key_principal": "Alice",
            "emails": ["a@example.com"],
            "domain": "example.com",
            "phone": "021234567",
        },
        {
            "duns": "2",
            "company_name": "NO MAIL",
            "key_principal": "Bob",
            "emails": [],
            "domain": "nomail.com",
            "phone": "",
        },
    ]
    with (site_dir / "final_companies.jsonl").open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = build_delivery_bundle(data_root=data_root, delivery_root=delivery_root, day_label="day1")

    assert summary["day"] == 1
    assert summary["baseline_day"] == 0
    assert summary["total_current_companies"] == 1
    assert summary["delta_companies"] == 1

    csv_path = delivery_root / "Thailand_day001" / "companies.csv"
    with csv_path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        rows = list(reader)

    assert reader.fieldnames == ["公司名", "代表人", "邮箱", "域名", "电话"]
    assert rows == [{"公司名": "ACME", "代表人": "Alice", "邮箱": "a@example.com", "域名": "example.com", "电话": "021234567"}]


def test_build_delivery_bundle_merges_live_email_results_over_stale_final(tmp_path) -> None:
    data_root = tmp_path / "output"
    site_dir = data_root / "dnb"
    delivery_root = data_root / "delivery"
    site_dir.mkdir(parents=True)

    stale_final = [
        {
            "duns": "1",
            "company_name": "OLD ONLY",
            "key_principal": "Alice",
            "emails": ["a@example.com"],
            "domain": "a.com",
            "phone": "",
        }
    ]
    with (site_dir / "final_companies.jsonl").open("w", encoding="utf-8") as fp:
        for row in stale_final:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")

    fresher_rows = [
        {
            "duns": "1",
            "company_name": "OLD ONLY",
            "key_principal": "Alice",
            "emails": ["a@example.com"],
            "domain": "a.com",
            "phone": "",
        },
        {
            "duns": "2",
            "company_name": "NEW LIVE",
            "key_principal": "Bob",
            "emails": ["b@example.com"],
            "domain": "b.com",
            "phone": "",
        },
    ]
    with (site_dir / "companies_with_emails.jsonl").open("w", encoding="utf-8") as fp:
        for row in fresher_rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = build_delivery_bundle(data_root=data_root, delivery_root=delivery_root, day_label="day1")

    assert summary["total_current_companies"] == 2
    assert summary["delta_companies"] == 2


def test_build_delivery_bundle_reads_sqlite_final_companies(tmp_path) -> None:
    from thailand_crawler.streaming.store import StreamStore

    data_root = tmp_path / "output"
    stream_dir = data_root / "dnb_stream"
    delivery_root = data_root / "delivery"
    stream_dir.mkdir(parents=True)

    store = StreamStore(stream_dir / "store.db")
    store.upsert_company(
        duns="1",
        company_name_en_dnb="ACME Co., Ltd.",
        company_name_url="acme",
        key_principal="Alice",
        phone="021234567",
        address="Bangkok",
        city="Bangkok",
        region="Bangkok",
        dnb_website="https://acme.example.com",
    )
    store.save_site_result(duns="1", company_name_th="บริษัท เอซีเอ็มอี จำกัด")
    store.save_snov_result(duns="1", emails=["sales@acme.example.com"])

    summary = build_delivery_bundle(data_root=data_root, delivery_root=delivery_root, day_label="day1")

    assert summary["total_current_companies"] == 1
    csv_path = delivery_root / "Thailand_day001" / "companies.csv"
    with csv_path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        rows = list(reader)

    assert rows == [{"公司名": "บริษัท เอซีเอ็มอี จำกัด", "代表人": "Alice", "邮箱": "sales@acme.example.com", "域名": "acme.example.com", "电话": "021234567"}]


def test_build_delivery_bundle_aggregates_multiple_site_directories(tmp_path) -> None:
    from thailand_crawler.streaming.store import StreamStore

    data_root = tmp_path / "output"
    stream_dir = data_root / "dnb_stream"
    extra_dir = data_root / "extra_site"
    delivery_root = data_root / "delivery"
    stream_dir.mkdir(parents=True)
    extra_dir.mkdir(parents=True)

    store = StreamStore(stream_dir / "store.db")
    store.upsert_company(
        duns="1",
        company_name_en_dnb="ACME Co., Ltd.",
        company_name_url="acme",
        key_principal="Alice",
        phone="021234567",
        address="Bangkok",
        city="Bangkok",
        region="Bangkok",
        dnb_website="https://acme.example.com",
    )
    store.save_site_result(duns="1", company_name_th="บริษัท เอซีเอ็มอี จำกัด")
    store.save_snov_result(duns="1", emails=["sales@acme.example.com"])

    with (extra_dir / "final_companies.jsonl").open("w", encoding="utf-8") as fp:
        fp.write(
            json.dumps(
                {
                    "duns": "2",
                    "company_name": "SECOND SITE",
                    "key_principal": "Bob",
                    "emails": ["bob@example.com"],
                    "domain": "second.example.com",
                    "phone": "029999999",
                },
                ensure_ascii=False,
            ) + "\n"
        )

    summary = build_delivery_bundle(data_root=data_root, delivery_root=delivery_root, day_label="day1")

    assert summary["total_current_companies"] == 2
    csv_path = delivery_root / "Thailand_day001" / "companies.csv"
    with csv_path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        rows = list(reader)

    assert {row["公司名"]: row for row in rows} == {
        "SECOND SITE": {"公司名": "SECOND SITE", "代表人": "Bob", "邮箱": "bob@example.com", "域名": "second.example.com", "电话": "029999999"},
        "บริษัท เอซีเอ็มอี จำกัด": {"公司名": "บริษัท เอซีเอ็มอี จำกัด", "代表人": "Alice", "邮箱": "sales@acme.example.com", "域名": "acme.example.com", "电话": "021234567"},
    }


def test_build_delivery_bundle_filters_shared_unrelated_domains(tmp_path) -> None:
    data_root = tmp_path / "output"
    site_dir = data_root / "dnb"
    delivery_root = data_root / "delivery"
    site_dir.mkdir(parents=True)

    rows = [
        {
            "duns": "1",
            "company_name": "BLUESKY ROYAL COMPANY LIMITED",
            "key_principal": "Alice",
            "emails": ["p@centarahotelsresorts.com"],
            "domain": "centarahotelsresorts.com",
            "phone": "021234567",
        },
        {
            "duns": "2",
            "company_name": "THONGTHARA GRAND COMPANY LIMITED",
            "key_principal": "Bob",
            "emails": ["p@centarahotelsresorts.com"],
            "domain": "centarahotelsresorts.com",
            "phone": "021234568",
        },
        {
            "duns": "3",
            "company_name": "GREAT GRAND LIMITED",
            "key_principal": "Carol",
            "emails": ["p@centarahotelsresorts.com"],
            "domain": "centarahotelsresorts.com",
            "phone": "021234569",
        },
        {
            "duns": "4",
            "company_name": "RITTA COMPANY LIMITED",
            "key_principal": "Boss",
            "emails": ["info@ritta.co.th"],
            "domain": "ritta.co.th",
            "phone": "021111111",
        },
    ]
    with (site_dir / "final_companies.jsonl").open("w", encoding="utf-8") as fp:
        for row in rows:
            fp.write(json.dumps(row, ensure_ascii=False) + "\n")

    summary = build_delivery_bundle(data_root=data_root, delivery_root=delivery_root, day_label="day1")

    assert summary["total_current_companies"] == 1
    csv_path = delivery_root / "Thailand_day001" / "companies.csv"
    with csv_path.open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        rows = list(reader)

    assert rows == [{"公司名": "RITTA COMPANY LIMITED", "代表人": "Boss", "邮箱": "info@ritta.co.th", "域名": "ritta.co.th", "电话": "021111111"}]
