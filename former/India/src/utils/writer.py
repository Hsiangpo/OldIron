from __future__ import annotations

import csv
import json
import threading
from pathlib import Path
from typing import Dict


class OutputWriter:
    def __init__(self, output_dir: str) -> None:
        self._output_dir = Path(output_dir)
        self._output_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

        self._jsonl_path = self._output_dir / "companies.jsonl"
        self._csv_path = self._output_dir / "companies.csv"

        self._jsonl_file = self._jsonl_path.open("a", encoding="utf-8")
        self._csv_file = self._csv_path.open("a", encoding="utf-8", newline="")
        self._csv_writer = None
        self._columns = []
        self._ensure_csv_header()

    @property
    def jsonl_path(self) -> Path:
        return self._jsonl_path

    @property
    def csv_path(self) -> Path:
        return self._csv_path

    def _ensure_csv_header(self) -> None:
        default_columns = [
            "cin",
            "name",
            "status",
            "email",
            "director_name",
        ]
        if self._csv_path.exists() and self._csv_path.stat().st_size > 0:
            with self._csv_path.open("r", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader, [])
            if header and header != default_columns:
                raise ValueError(
                    "companies.csv header mismatch. "
                    "Please delete existing CSV before re-crawling with the new schema."
                )
            columns = header if header else default_columns
            needs_header = not header
        else:
            columns = default_columns
            needs_header = True

        self._columns = columns
        self._csv_writer = csv.DictWriter(self._csv_file, fieldnames=columns)
        if needs_header:
            self._csv_writer.writeheader()
            self._csv_file.flush()

    def write(self, record: Dict[str, object]) -> None:
        payload = dict(record)
        contact_details = payload.get("contact_details") or {}
        current_director = payload.get("current_director") or {}
        if not isinstance(contact_details, dict):
            contact_details = {}
        if not isinstance(current_director, dict):
            current_director = {}

        email = contact_details.get("Email ID") or contact_details.get("email") or contact_details.get("Email") or ""
        if email is None:
            email = ""
        director_name = (
            current_director.get("Director Name")
            or current_director.get("director_name")
            or current_director.get("name")
            or ""
        )
        if director_name is None:
            director_name = ""
        row = {
            "cin": payload.get("cin", ""),
            "name": payload.get("name", ""),
            "status": payload.get("status", ""),
            "email": "" if email is None else str(email),
            "director_name": "" if director_name is None else str(director_name),
        }
        row = {key: row.get(key, "") for key in self._columns}
        with self._lock:
            self._jsonl_file.write(json.dumps(payload, ensure_ascii=False) + "\n")
            self._jsonl_file.flush()
            self._csv_writer.writerow(row)
            self._csv_file.flush()

    def close(self) -> None:
        with self._lock:
            self._jsonl_file.close()
            self._csv_file.close()
