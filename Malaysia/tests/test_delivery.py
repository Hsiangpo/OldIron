import sqlite3
from pathlib import Path

import pytest
import csv

from malaysia_crawler.delivery import build_delivery_bundle
from malaysia_crawler.delivery import parse_day_label


def _seed_final_companies(db_path: Path, rows: list[dict[str, str]]) -> None:
    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS final_companies (
            normalized_name TEXT PRIMARY KEY,
            company_name TEXT NOT NULL,
            domain TEXT NOT NULL,
            company_manager TEXT NOT NULL,
            contact_eamils TEXT NOT NULL,
            phone TEXT NOT NULL DEFAULT '',
            company_id INTEGER NOT NULL,
            updated_at TEXT NOT NULL
        )
        """
    )
    for row in rows:
        conn.execute(
            """
            INSERT INTO final_companies(
                normalized_name, company_name, domain, company_manager, contact_eamils, phone, company_id, updated_at
            ) VALUES(?, ?, ?, ?, ?, ?, ?, '2026-02-26T00:00:00Z')
            ON CONFLICT(normalized_name) DO UPDATE SET
                company_name = excluded.company_name,
                domain = excluded.domain,
                company_manager = excluded.company_manager,
                contact_eamils = excluded.contact_eamils,
                phone = excluded.phone,
                company_id = excluded.company_id,
                updated_at = excluded.updated_at
            """,
            (
                row["normalized_name"],
                row["company_name"],
                row["domain"],
                row["company_manager"],
                row["contact_eamils"],
                row.get("phone", ""),
                int(row["company_id"]),
            ),
        )
    conn.commit()
    conn.close()


def test_parse_day_label() -> None:
    assert parse_day_label("day1") == 1
    assert parse_day_label("DAY007") == 7
    with pytest.raises(ValueError):
        parse_day_label("d7")


def test_delivery_day_increment_and_rerun(tmp_path: Path) -> None:
    db_path = tmp_path / "runtime.db"
    delivery_root = tmp_path / "delivery"

    _seed_final_companies(
        db_path,
        [
            {
                "normalized_name": "a",
                "company_name": "A",
                "domain": "a.com",
                "contact_eamils": '["a@a.com"]',
                "company_manager": "MA",
                "phone": "+60111111111",
                "company_id": "1",
            },
            {
                "normalized_name": "b",
                "company_name": "B",
                "domain": "b.com",
                "contact_eamils": '["b@b.com"]',
                "company_manager": "MB",
                "phone": "",
                "company_id": "2",
            },
        ],
    )
    s1 = build_delivery_bundle(db_path=db_path, delivery_root=delivery_root, day_label="day1")
    assert s1["delta_companies"] == 2

    _seed_final_companies(
        db_path,
        [
            {
                "normalized_name": "c",
                "company_name": "C",
                "domain": "c.com",
                "contact_eamils": '["c@c.com"]',
                "company_manager": "MC",
                "phone": "+60322223333",
                "company_id": "3",
            }
        ],
    )
    s2 = build_delivery_bundle(db_path=db_path, delivery_root=delivery_root, day_label="day2")
    assert s2["delta_companies"] == 1

    _seed_final_companies(
        db_path,
        [
            {
                "normalized_name": "d",
                "company_name": "D",
                "domain": "d.com",
                "contact_eamils": '["d@d.com"]',
                "company_manager": "MD",
                "phone": "",
                "company_id": "4",
            }
        ],
    )
    s2_rerun = build_delivery_bundle(db_path=db_path, delivery_root=delivery_root, day_label="day2")
    assert s2_rerun["delta_companies"] == 2

    with (delivery_root / "Malaysia_day002" / "companies.csv").open("r", encoding="utf-8", newline="") as fp:
        reader = csv.DictReader(fp)
        header = reader.fieldnames or []
        assert "normalized_name" not in header
        assert "company_id" not in header
        assert "phone" in header

    with pytest.raises(ValueError):
        build_delivery_bundle(db_path=db_path, delivery_root=delivery_root, day_label="day1")
