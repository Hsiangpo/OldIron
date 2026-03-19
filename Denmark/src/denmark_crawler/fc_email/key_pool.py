"""Firecrawl Key 池。"""

from __future__ import annotations

import sqlite3
import threading
import time
from dataclasses import dataclass
from pathlib import Path


@dataclass(slots=True)
class KeyPoolConfig:
    per_key_limit: int = 2
    wait_seconds: int = 20
    cooldown_seconds: int = 90
    failure_threshold: int = 5


@dataclass(slots=True)
class KeyLease:
    key: str
    index: int


def _utc_now_unix() -> float:
    return time.time()


def _should_remove_key(reason: str) -> bool:
    code = str(reason or "").strip().lower()
    return code in {"payment_required", "insufficient_credits", "insufficient_tokens"}


def _run_with_retry(conn: sqlite3.Connection, operation, attempts: int = 6):
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
            time.sleep(min(0.05 * (2**attempt), 0.5))
    raise RuntimeError("sqlite retry unreachable")


class FirecrawlKeyPool:
    """基于 sqlite 的 Firecrawl Key 池。"""

    def __init__(self, *, keys: list[str], key_file: Path, db_path: Path, config: KeyPoolConfig | None = None) -> None:
        cleaned = [item.strip() for item in keys if item.strip()]
        if not cleaned:
            raise ValueError("firecrawl keys 为空。")
        self._keys = cleaned
        self._key_file = key_file
        self._db_path = db_path
        self._config = config or KeyPoolConfig()
        self._lock = threading.Lock()
        self._conn = self._connect()
        self._init_db()

    @staticmethod
    def load_keys(path: Path) -> list[str]:
        if not path.exists():
            raise FileNotFoundError(f"firecrawl key 文件不存在：{path}")
        keys: list[str] = []
        for raw in path.read_text(encoding="utf-8").splitlines():
            line = raw.strip()
            if not line or line.startswith("#"):
                continue
            keys.append(line)
        if not keys:
            raise ValueError(f"firecrawl key 文件为空：{path}")
        return keys

    def _connect(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self._db_path), timeout=30)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute("PRAGMA busy_timeout = 30000;")
        return conn

    def _init_db(self) -> None:
        with self._lock:
            def _op() -> None:
                self._conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS keys (
                        key TEXT PRIMARY KEY,
                        idx INTEGER NOT NULL,
                        state TEXT NOT NULL,
                        cooldown_until REAL,
                        failure_count INTEGER NOT NULL DEFAULT 0,
                        in_flight INTEGER NOT NULL DEFAULT 0,
                        disabled_reason TEXT,
                        last_used REAL
                    )
                    """
                )
                for idx, key in enumerate(self._keys):
                    self._conn.execute(
                        """
                        INSERT INTO keys(key, idx, state, cooldown_until, failure_count, in_flight, disabled_reason, last_used)
                        VALUES(?, ?, 'active', NULL, 0, 0, NULL, NULL)
                        ON CONFLICT(key) DO UPDATE SET idx = excluded.idx
                        """,
                        (key, idx),
                    )
                placeholders = ",".join("?" for _ in self._keys)
                self._conn.execute(
                    f"DELETE FROM keys WHERE key NOT IN ({placeholders})",
                    tuple(self._keys),
                )
                self._conn.execute("UPDATE keys SET in_flight = 0")
                self._conn.commit()

            _run_with_retry(self._conn, _op)

    def acquire(self) -> KeyLease:
        while True:
            lease = self._try_acquire_once()
            if lease is not None:
                return lease
            if not self._has_enabled_key():
                raise RuntimeError("没有可用 firecrawl key。")
            time.sleep(0.2)

    def _has_enabled_key(self) -> bool:
        with self._lock:
            row = self._conn.execute(
                "SELECT 1 FROM keys WHERE state != 'disabled' LIMIT 1"
            ).fetchone()
            return row is not None

    def _try_acquire_once(self) -> KeyLease | None:
        with self._lock:
            now = _utc_now_unix()
            holder: dict[str, KeyLease | None] = {"lease": None}

            def _op() -> None:
                self._conn.execute(
                    """
                    UPDATE keys
                    SET state = 'active', cooldown_until = NULL
                    WHERE state = 'cooldown' AND cooldown_until IS NOT NULL AND cooldown_until <= ?
                    """,
                    (now,),
                )
                row = self._conn.execute(
                    """
                    SELECT key, idx
                    FROM keys
                    WHERE state != 'disabled'
                      AND (state != 'cooldown' OR cooldown_until IS NULL OR cooldown_until <= ?)
                      AND in_flight < ?
                    ORDER BY in_flight ASC, COALESCE(last_used, 0) ASC, idx ASC
                    LIMIT 1
                    """,
                    (now, max(self._config.per_key_limit, 1)),
                ).fetchone()
                if row is None:
                    self._conn.commit()
                    holder["lease"] = None
                    return
                key = str(row["key"])
                idx = int(row["idx"])
                self._conn.execute(
                    "UPDATE keys SET in_flight = in_flight + 1, last_used = ? WHERE key = ?",
                    (now, key),
                )
                self._conn.commit()
                holder["lease"] = KeyLease(key=key, index=idx)

            _run_with_retry(self._conn, _op)
            return holder["lease"]

    def release(self, lease: KeyLease) -> None:
        with self._lock:
            def _op() -> None:
                self._conn.execute(
                    "UPDATE keys SET in_flight = CASE WHEN in_flight > 0 THEN in_flight - 1 ELSE 0 END WHERE key = ?",
                    (lease.key,),
                )
                self._conn.commit()

            _run_with_retry(self._conn, _op)

    def mark_success(self, lease: KeyLease) -> None:
        with self._lock:
            def _op() -> None:
                self._conn.execute(
                    "UPDATE keys SET failure_count = 0, state = 'active', cooldown_until = NULL, disabled_reason = NULL WHERE key = ?",
                    (lease.key,),
                )
                self._conn.commit()

            _run_with_retry(self._conn, _op)

    def mark_rate_limited(self, lease: KeyLease, retry_after: float | None = None) -> None:
        _ = lease, retry_after
        # Firecrawl 官方 429 不做本地 key 惩罚，只交给任务级退避。
        return None

    def mark_failure(self, lease: KeyLease) -> None:
        _ = lease
        # 普通失败不做 key 惩罚，只在 401/402 时禁用 key。
        return None

    def disable(self, lease: KeyLease, reason: str) -> None:
        with self._lock:
            def _op() -> None:
                self._conn.execute(
                    "UPDATE keys SET state = 'disabled', disabled_reason = ?, cooldown_until = NULL, in_flight = 0 WHERE key = ?",
                    (reason, lease.key),
                )
                self._conn.commit()

            _run_with_retry(self._conn, _op)
            if _should_remove_key(reason):
                self._remove_key_from_file(lease.key)

    def _remove_key_from_file(self, target_key: str) -> None:
        if not self._key_file.exists():
            return
        kept_lines: list[str] = []
        for raw in self._key_file.read_text(encoding="utf-8", errors="replace").splitlines():
            line = raw.strip()
            if not line:
                continue
            if line.startswith("#"):
                kept_lines.append(raw)
                continue
            if line == target_key:
                continue
            kept_lines.append(line)
        text = "\n".join(kept_lines).strip()
        payload = text + "\n" if text else ""
        self._key_file.write_text(payload, encoding="utf-8")

    def close(self) -> None:
        with self._lock:
            self._conn.close()

