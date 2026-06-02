from __future__ import annotations

import sys
from pathlib import Path

from openpyxl import Workbook

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from oldironcrawler.importer import load_websites


def test_xlsx_loader_keeps_company_name_with_website(tmp_path: Path) -> None:
    path = tmp_path / "companies.xlsx"
    wb = Workbook()
    ws = wb.active
    ws.append(["Company Name", "Company Website"])
    ws.append(["Acme Holdings Ltd", "https://acme.example"])
    ws.append(["Beta Group", "beta.example"])
    wb.save(path)

    rows = load_websites(path)

    assert [(row.company_name, row.website) for row in rows] == [
        ("Acme Holdings Ltd", "https://acme.example"),
        ("Beta Group", "https://beta.example"),
    ]


def test_txt_loader_has_empty_company_name(tmp_path: Path) -> None:
    path = tmp_path / "sites.txt"
    path.write_text("example.com\n", encoding="utf-8")

    rows = load_websites(path)

    assert rows[0].company_name == ""
    assert rows[0].website == "https://example.com"
