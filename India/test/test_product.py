import csv
from pathlib import Path

import product


def _write_csv(path: Path, rows: list[dict]) -> None:
    fieldnames = ["cin", "name", "status", "email", "director_name"]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def _read_csv(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader)


def test_day2_outputs_only_new_rows_and_overwrites(tmp_path, monkeypatch):
    monkeypatch.setattr(product, "BASE_DIR", tmp_path)

    total_rows = [
        {"cin": "AAA", "name": "A", "status": "Active", "email": "a@a.com", "director_name": "A"},
        {"cin": "BBB", "name": "B", "status": "Active", "email": "b@b.com", "director_name": "B"},
        {"cin": "CCC", "name": "C", "status": "Active", "email": "", "director_name": ""},
    ]
    _write_csv(tmp_path / "companies.csv", total_rows)
    _write_csv(tmp_path / "companies_001.csv", total_rows[:1])

    _write_csv(tmp_path / "companies_002.csv", [{"cin": "DUMMY", "name": "D", "status": "", "email": "", "director_name": ""}])

    monkeypatch.setattr("sys.argv", ["product.py", "day2", "--force"])
    product.main()

    rows = _read_csv(tmp_path / "companies_002.csv")
    assert [row["cin"] for row in rows] == ["BBB", "CCC"]


def test_day1_copies_total(tmp_path, monkeypatch):
    monkeypatch.setattr(product, "BASE_DIR", tmp_path)
    total_rows = [
        {"cin": "AAA", "name": "A", "status": "Active", "email": "a@a.com", "director_name": "A"},
        {"cin": "BBB", "name": "B", "status": "Active", "email": "b@b.com", "director_name": "B"},
    ]
    _write_csv(tmp_path / "companies.csv", total_rows)

    monkeypatch.setattr("sys.argv", ["product.py", "day1", "--force"])
    product.main()

    rows = _read_csv(tmp_path / "companies_001.csv")
    assert rows == total_rows
