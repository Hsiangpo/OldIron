"""England 集群 Postgres 连接工具。"""

from __future__ import annotations

from contextlib import contextmanager
from typing import Iterator

from psycopg.rows import dict_row
from psycopg_pool import ConnectionPool


class ClusterDb:
    """基于连接池的 Postgres 工厂。"""

    def __init__(
        self,
        dsn: str,
        *,
        min_size: int = 1,
        max_size: int = 8,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._dsn = str(dsn or "").strip()
        if not self._dsn:
            raise ValueError("Postgres DSN 不能为空。")
        self._pool = ConnectionPool(
            conninfo=self._dsn,
            min_size=max(int(min_size), 1),
            max_size=max(int(max_size), max(int(min_size), 1)),
            timeout=max(float(timeout_seconds), 1.0),
            kwargs={"row_factory": dict_row},
            open=True,
        )
        self._pool.wait()

    @contextmanager
    def connect(self) -> Iterator:
        with self._pool.connection() as conn:
            yield conn

    @contextmanager
    def transaction(self) -> Iterator:
        with self.connect() as conn:
            with conn.transaction():
                yield conn

    def test_connection(self) -> None:
        with self.connect() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")

    def close(self) -> None:
        self._pool.close()
