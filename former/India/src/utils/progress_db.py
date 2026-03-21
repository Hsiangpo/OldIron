from __future__ import annotations

import sqlite3
import threading
from pathlib import Path
from typing import Iterable


class ProgressDB:
    def __init__(self, path: str) -> None:
        self._path = Path(path)
        self._lock = threading.Lock()
        self._conn = sqlite3.connect(self._path, check_same_thread=False)
        self._conn.execute("PRAGMA journal_mode=WAL;")
        self._conn.execute("PRAGMA synchronous=NORMAL;")
        self._init_schema()

    def _init_schema(self) -> None:
        with self._lock:
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS pages (page INTEGER PRIMARY KEY)"
            )
            self._conn.execute(
                "CREATE TABLE IF NOT EXISTS companies (cin TEXT PRIMARY KEY)"
            )
            self._conn.commit()

    def is_page_done(self, page: int) -> bool:
        with self._lock:
            cur = self._conn.execute("SELECT 1 FROM pages WHERE page=?", (page,))
            return cur.fetchone() is not None

    def mark_page_done(self, page: int) -> None:
        with self._lock:
            self._conn.execute("INSERT OR IGNORE INTO pages(page) VALUES (?)", (page,))
            self._conn.commit()

    def is_company_done(self, cin: str) -> bool:
        with self._lock:
            cur = self._conn.execute("SELECT 1 FROM companies WHERE cin=?", (cin,))
            return cur.fetchone() is not None

    def mark_company_done(self, cin: str) -> None:
        with self._lock:
            self._conn.execute(
                "INSERT OR IGNORE INTO companies(cin) VALUES (?)", (cin,)
            )

    def mark_company_done_bulk(self, cins: Iterable[str]) -> None:
        with self._lock:
            self._conn.executemany(
                "INSERT OR IGNORE INTO companies(cin) VALUES (?)",
                [(cin,) for cin in cins],
            )

    def commit(self) -> None:
        with self._lock:
            self._conn.commit()

    def close(self) -> None:
        with self._lock:
            self._conn.commit()
            self._conn.close()
