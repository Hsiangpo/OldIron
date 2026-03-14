from __future__ import annotations

import asyncio
import os
import sqlite3
import time
import uuid
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Iterable


class KeyState(str, Enum):
    ACTIVE = "active"
    COOLDOWN = "cooldown"
    DISABLED = "disabled"


@dataclass
class KeyEntry:
    key: str
    index: int
    state: KeyState = KeyState.ACTIVE
    cooldown_until: float | None = None
    in_flight: int = 0
    failure_count: int = 0
    disabled_reason: str | None = None
    lease_id: str | None = None


@dataclass
class KeyPoolConfig:
    per_key_limit: int = 2
    cooldown_seconds: int = 0
    failure_threshold: int = 5
    wait_seconds: int = 20
    check_interval: float = 1.0
    shared_pool: bool = True
    shared_db_path: Path | None = None
    lease_ttl_seconds: int = 120
    busy_timeout_ms: int = 30000


class KeyLease:
    def __init__(self, pool: "KeyPool", entry: KeyEntry) -> None:
        self._pool = pool
        self.entry = entry

    async def release(self) -> None:
        await self._pool.release(self.entry.index, self.entry.lease_id)


class _LocalKeyPool:
    def __init__(
        self, keys: Iterable[str], config: KeyPoolConfig
    ) -> None:
        cleaned = [k.strip() for k in keys if isinstance(k, str) and k.strip()]
        if not cleaned:
            raise ValueError("no firecrawl keys provided")
        self._entries = [KeyEntry(key=k, index=i) for i, k in enumerate(cleaned)]
        self._config = config
        self._lock = asyncio.Lock()

    async def acquire(self) -> KeyLease:
        deadline = time.monotonic() + max(1, int(self._config.wait_seconds))
        while True:
            lease = await self._try_acquire()
            if lease is not None:
                return lease
            if time.monotonic() >= deadline:
                raise RuntimeError("no available firecrawl key")
            await asyncio.sleep(self._config.check_interval)

    async def _try_acquire(self) -> KeyLease | None:
        async with self._lock:
            now = time.monotonic()
            candidates = []
            for entry in self._entries:
                if entry.state == KeyState.DISABLED:
                    continue
                if entry.state == KeyState.COOLDOWN:
                    if entry.cooldown_until is not None and entry.cooldown_until > now:
                        continue
                    entry.state = KeyState.ACTIVE
                    entry.cooldown_until = None
                if entry.in_flight >= self._config.per_key_limit:
                    continue
                candidates.append(entry)

            if not candidates:
                return None

            candidates.sort(key=lambda e: (e.in_flight, e.index))
            picked = candidates[0]
            picked.in_flight += 1
            return KeyLease(self, picked)

    async def release(self, index: int, lease_id: str | None = None) -> None:
        async with self._lock:
            if 0 <= index < len(self._entries):
                entry = self._entries[index]
                entry.in_flight = max(0, entry.in_flight - 1)

    async def mark_success(self, index: int) -> None:
        async with self._lock:
            if 0 <= index < len(self._entries):
                entry = self._entries[index]
                entry.failure_count = 0
                if entry.state == KeyState.COOLDOWN:
                    entry.state = KeyState.ACTIVE
                    entry.cooldown_until = None

    async def mark_rate_limited(
        self, index: int, retry_after: float | None = None
    ) -> None:
        async with self._lock:
            if 0 <= index < len(self._entries):
                entry = self._entries[index]
                entry.failure_count += 1
                wait = (
                    retry_after
                    if retry_after is not None and retry_after > 0
                    else self._config.cooldown_seconds
                )
                entry.state = KeyState.COOLDOWN
                entry.cooldown_until = time.monotonic() + wait

    async def mark_failure(self, index: int) -> None:
        async with self._lock:
            if 0 <= index < len(self._entries):
                entry = self._entries[index]
                entry.failure_count += 1
                if entry.failure_count >= self._config.failure_threshold:
                    entry.state = KeyState.COOLDOWN
                    entry.cooldown_until = (
                        time.monotonic() + self._config.cooldown_seconds
                    )

    async def disable(self, index: int, reason: str) -> None:
        async with self._lock:
            if 0 <= index < len(self._entries):
                entry = self._entries[index]
                entry.state = KeyState.DISABLED
                entry.disabled_reason = reason

    async def snapshot(self) -> list[KeyEntry]:
        async with self._lock:
            return [KeyEntry(**entry.__dict__) for entry in self._entries]


class _SharedKeyPool:
    def __init__(self, keys: Iterable[str], config: KeyPoolConfig, db_path: Path) -> None:
        cleaned = [k.strip() for k in keys if isinstance(k, str) and k.strip()]
        if not cleaned:
            raise ValueError("no firecrawl keys provided")
        self._keys = cleaned
        self._config = config
        self._db_path = db_path
        self._key_by_index = {i: key for i, key in enumerate(cleaned)}
        self._index_by_key = {key: i for i, key in enumerate(cleaned)}
        self._init_db()

    def _connect(self) -> sqlite3.Connection:
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(
            str(self._db_path),
            timeout=max(1, self._config.busy_timeout_ms / 1000.0),
            isolation_level=None,
        )
        conn.execute("PRAGMA journal_mode=WAL;")
        conn.execute("PRAGMA synchronous=NORMAL;")
        conn.execute(f"PRAGMA busy_timeout={int(self._config.busy_timeout_ms)};")
        return conn

    def _init_db(self) -> None:
        conn = self._connect()
        try:
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS keys (
                    key TEXT PRIMARY KEY,
                    idx INTEGER,
                    state TEXT,
                    cooldown_until REAL,
                    failure_count INTEGER,
                    disabled_reason TEXT,
                    last_used REAL
                )
                """
            )
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS leases (
                    lease_id TEXT PRIMARY KEY,
                    key TEXT,
                    acquired_at REAL,
                    ttl REAL
                )
                """
            )
            for idx, key in enumerate(self._keys):
                conn.execute(
                    "INSERT OR IGNORE INTO keys (key, idx, state, cooldown_until, failure_count, disabled_reason, last_used) "
                    "VALUES (?, ?, ?, NULL, 0, NULL, NULL)",
                    (key, idx, KeyState.ACTIVE.value),
                )
                conn.execute("UPDATE keys SET idx=? WHERE key=?", (idx, key))
        finally:
            conn.close()

    def _cleanup_expired(self, conn: sqlite3.Connection, now: float) -> None:
        conn.execute(
            "DELETE FROM leases WHERE acquired_at + ttl < ?",
            (now,),
        )
        conn.execute(
            "UPDATE keys SET state=?, cooldown_until=NULL WHERE state=? AND cooldown_until IS NOT NULL AND cooldown_until <= ?",
            (KeyState.ACTIVE.value, KeyState.COOLDOWN.value, now),
        )

    async def acquire(self) -> KeyLease:
        deadline = time.time() + max(1, int(self._config.wait_seconds))
        while True:
            lease = await asyncio.to_thread(self._try_acquire_sync)
            if lease is not None:
                return lease
            if time.time() >= deadline:
                raise RuntimeError("no available firecrawl key")
            await asyncio.sleep(self._config.check_interval)

    def _try_acquire_sync(self) -> KeyLease | None:
        now = time.time()
        conn = self._connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            self._cleanup_expired(conn, now)
            if not self._keys:
                conn.execute("COMMIT")
                return None
            placeholders = ",".join(["?"] * len(self._keys))
            query = (
                "SELECT k.key, k.idx, COALESCE(l.cnt, 0) "
                "FROM keys k "
                "LEFT JOIN (SELECT key, COUNT(*) AS cnt FROM leases GROUP BY key) l ON k.key = l.key "
                "WHERE k.state != ? AND (k.state != ? OR k.cooldown_until IS NULL OR k.cooldown_until <= ?) "
                f"AND k.key IN ({placeholders}) "
                "AND COALESCE(l.cnt, 0) < ? "
                "ORDER BY COALESCE(l.cnt, 0) ASC, k.last_used ASC, k.idx ASC "
                "LIMIT 1"
            )
            params = [
                KeyState.DISABLED.value,
                KeyState.COOLDOWN.value,
                now,
                *self._keys,
                int(self._config.per_key_limit),
            ]
            row = conn.execute(query, params).fetchone()
            if not row:
                conn.execute("COMMIT")
                return None
            key, idx, _ = row
            lease_id = uuid.uuid4().hex
            conn.execute(
                "INSERT INTO leases (lease_id, key, acquired_at, ttl) VALUES (?, ?, ?, ?)",
                (lease_id, key, now, float(self._config.lease_ttl_seconds)),
            )
            conn.execute(
                "UPDATE keys SET last_used=? WHERE key=?",
                (now, key),
            )
            conn.execute("COMMIT")
            entry = KeyEntry(
                key=key,
                index=int(idx),
                state=KeyState.ACTIVE,
                in_flight=1,
                lease_id=lease_id,
            )
            return KeyLease(self, entry)
        finally:
            conn.close()

    async def release(self, index: int, lease_id: str | None = None) -> None:
        await asyncio.to_thread(self._release_sync, index, lease_id)

    def _release_sync(self, index: int, lease_id: str | None = None) -> None:
        key = self._key_by_index.get(index)
        if not key:
            return
        conn = self._connect()
        try:
            if lease_id:
                conn.execute("DELETE FROM leases WHERE lease_id=?", (lease_id,))
            else:
                conn.execute(
                    "DELETE FROM leases WHERE rowid IN (SELECT rowid FROM leases WHERE key=? ORDER BY acquired_at LIMIT 1)",
                    (key,),
                )
        finally:
            conn.close()

    async def mark_success(self, index: int) -> None:
        await asyncio.to_thread(self._mark_success_sync, index)

    def _mark_success_sync(self, index: int) -> None:
        key = self._key_by_index.get(index)
        if not key:
            return
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE keys SET failure_count=0, state=?, cooldown_until=NULL WHERE key=?",
                (KeyState.ACTIVE.value, key),
            )
        finally:
            conn.close()

    async def mark_rate_limited(self, index: int, retry_after: float | None = None) -> None:
        await asyncio.to_thread(self._mark_rate_limited_sync, index, retry_after)

    def _mark_rate_limited_sync(self, index: int, retry_after: float | None = None) -> None:
        key = self._key_by_index.get(index)
        if not key:
            return
        now = time.time()
        wait = retry_after if retry_after and retry_after > 0 else self._config.cooldown_seconds
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE keys SET failure_count=failure_count+1, state=?, cooldown_until=? WHERE key=?",
                (KeyState.COOLDOWN.value, now + wait, key),
            )
        finally:
            conn.close()

    async def mark_failure(self, index: int) -> None:
        await asyncio.to_thread(self._mark_failure_sync, index)

    def _mark_failure_sync(self, index: int) -> None:
        key = self._key_by_index.get(index)
        if not key:
            return
        now = time.time()
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT failure_count FROM keys WHERE key=?",
                (key,),
            ).fetchone()
            failure_count = int(row[0]) + 1 if row else 1
            if failure_count >= int(self._config.failure_threshold):
                conn.execute(
                    "UPDATE keys SET failure_count=?, state=?, cooldown_until=? WHERE key=?",
                    (failure_count, KeyState.COOLDOWN.value, now + self._config.cooldown_seconds, key),
                )
            else:
                conn.execute(
                    "UPDATE keys SET failure_count=? WHERE key=?",
                    (failure_count, key),
                )
        finally:
            conn.close()

    async def disable(self, index: int, reason: str) -> None:
        await asyncio.to_thread(self._disable_sync, index, reason)

    def _disable_sync(self, index: int, reason: str) -> None:
        key = self._key_by_index.get(index)
        if not key:
            return
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE keys SET state=?, disabled_reason=? WHERE key=?",
                (KeyState.DISABLED.value, reason, key),
            )
        finally:
            conn.close()

    async def snapshot(self) -> list[KeyEntry]:
        return await asyncio.to_thread(self._snapshot_sync)

    def _snapshot_sync(self) -> list[KeyEntry]:
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT k.key, k.idx, k.state, k.cooldown_until, k.failure_count, k.disabled_reason, "
                "COALESCE(l.cnt, 0) "
                "FROM keys k "
                "LEFT JOIN (SELECT key, COUNT(*) AS cnt FROM leases GROUP BY key) l ON k.key = l.key "
                "WHERE k.key IN ({})".format(",".join(["?"] * len(self._keys)))
            , self._keys).fetchall()
            entries: list[KeyEntry] = []
            for key, idx, state, cooldown_until, failure_count, disabled_reason, cnt in rows:
                entries.append(
                    KeyEntry(
                        key=key,
                        index=int(idx),
                        state=KeyState(state),
                        cooldown_until=cooldown_until,
                        in_flight=int(cnt or 0),
                        failure_count=int(failure_count or 0),
                        disabled_reason=disabled_reason,
                    )
                )
            return entries
        finally:
            conn.close()


class KeyPool:
    def __init__(
        self,
        keys: Iterable[str],
        config: KeyPoolConfig | None = None,
        *,
        key_file_path: Path | None = None,
    ) -> None:
        cleaned = [k.strip() for k in keys if isinstance(k, str) and k.strip()]
        if not cleaned:
            raise ValueError("no firecrawl keys provided")
        self._config = config or KeyPoolConfig()
        self._key_file_path = Path(key_file_path) if key_file_path else None
        self._keys = cleaned
        if self._config.shared_pool:
            db_path = self._config.shared_db_path
            if db_path is None:
                env_path = (os.environ.get("FIRECRAWL_KEY_POOL_DB") or "").strip()
                db_path = Path(env_path) if env_path else Path("output") / "cache" / "firecrawl_keys.db"
            self._backend = _SharedKeyPool(cleaned, self._config, db_path)
        else:
            self._backend = _LocalKeyPool(cleaned, self._config)

    @staticmethod
    def load_keys(path: Path) -> list[str]:
        if not path.exists():
            raise FileNotFoundError(f"firecrawl key file not found: {path}")
        text = path.read_text(encoding="utf-8")
        keys: list[str] = []
        for line in text.splitlines():
            cleaned = line.strip()
            if not cleaned:
                continue
            if cleaned.startswith("#"):
                continue
            keys.append(cleaned)
        if not keys:
            raise ValueError(f"firecrawl key file empty: {path}")
        return keys

    async def acquire(self) -> KeyLease:
        return await self._backend.acquire()

    async def release(self, index: int, lease_id: str | None = None) -> None:
        await self._backend.release(index, lease_id)

    async def mark_success(self, index: int) -> None:
        await self._backend.mark_success(index)

    async def mark_rate_limited(self, index: int, retry_after: float | None = None) -> None:
        await self._backend.mark_rate_limited(index, retry_after=retry_after)

    async def mark_failure(self, index: int) -> None:
        await self._backend.mark_failure(index)

    async def disable(self, index: int, reason: str) -> None:
        await self._backend.disable(index, reason)
        if _should_remove_key_from_file(reason):
            await asyncio.to_thread(self._remove_key_from_file_sync, index)

    async def snapshot(self) -> list[KeyEntry]:
        return await self._backend.snapshot()

    def _remove_key_from_file_sync(self, index: int) -> None:
        path = self._key_file_path
        if path is None or index < 0 or index >= len(self._keys):
            return
        if not path.exists():
            return
        target_key = self._keys[index]
        text = path.read_text(encoding="utf-8", errors="replace")
        kept_lines: list[str] = []
        for raw in text.splitlines():
            line = raw.strip()
            if not line:
                continue
            if line.startswith("#"):
                kept_lines.append(raw)
                continue
            if line == target_key:
                continue
            kept_lines.append(line)
        tmp_path = path.with_suffix(path.suffix + ".tmp")
        payload = ("\n".join(kept_lines)).strip()
        tmp_path.write_text((payload + "\n") if payload else "", encoding="utf-8")
        tmp_path.replace(path)


def _should_remove_key_from_file(reason: str) -> bool:
    text = (reason or "").strip().lower()
    return text in {"payment_required", "insufficient_credits", "insufficient_tokens"}
