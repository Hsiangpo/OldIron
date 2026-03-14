"""SQLite 锁冲突重试。"""

from __future__ import annotations

import sqlite3
import time
from typing import Callable
from typing import TypeVar


T = TypeVar("T")


def run_with_sqlite_retry(
    conn: sqlite3.Connection,
    operation: Callable[[], T],
    *,
    attempts: int = 6,
    base_delay: float = 0.05,
    cap_delay: float = 0.5,
) -> T:
    """遇到 database is locked 时短退避重试。"""
    for attempt in range(max(int(attempts), 1)):
        try:
            return operation()
        except sqlite3.OperationalError as exc:
            if "locked" not in str(exc).lower() or attempt + 1 >= max(int(attempts), 1):
                raise
            try:
                conn.rollback()
            except sqlite3.Error:
                pass
            time.sleep(min(base_delay * (2**attempt), cap_delay))
    raise RuntimeError("sqlite retry unreachable")
