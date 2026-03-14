"""England 集群 Postgres 连接工具。"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

import psycopg
from psycopg.rows import dict_row


class ClusterDb:
    """轻量 Postgres 连接工厂。"""

    def __init__(self, dsn: str) -> None:
        self._dsn = str(dsn or "").strip()
        if not self._dsn:
            raise ValueError("Postgres DSN 不能为空。")

    @contextmanager
    def connect(self) -> Iterator[psycopg.Connection]:
        conn = psycopg.connect(self._dsn, row_factory=dict_row)
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def transaction(self) -> Iterator[psycopg.Connection]:
        with self.connect() as conn:
            with conn.transaction():
                yield conn

    def test_connection(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
