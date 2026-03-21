"""文件输出工具。"""

from __future__ import annotations

import csv
import json
from pathlib import Path
from typing import Iterable


def ensure_dir(path: str | Path) -> Path:
    target = Path(path)
    target.mkdir(parents=True, exist_ok=True)
    return target


def append_jsonl(path: str | Path, payload: dict) -> None:
    target = Path(path)
    with target.open("a", encoding="utf-8") as fp:
        fp.write(json.dumps(payload, ensure_ascii=False) + "\n")


class CsvAppender:
    """增量写入 CSV。"""

    def __init__(self, path: str | Path, fieldnames: Iterable[str]) -> None:
        self.path = Path(path)
        self.fieldnames = list(fieldnames)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        file_exists = self.path.exists()
        self._fp = self.path.open("a", encoding="utf-8", newline="")
        self._writer = csv.DictWriter(self._fp, fieldnames=self.fieldnames)
        if not file_exists:
            self._writer.writeheader()

    def write_row(self, row: dict) -> None:
        clean = {name: row.get(name, "") for name in self.fieldnames}
        self._writer.writerow(clean)

    def close(self) -> None:
        self._fp.close()

