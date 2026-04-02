"""IEATPE SQLite 存储。"""

from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


class IeatpeStore:
    """IEATPE 任务与公司存储。"""

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._local = threading.local()
        self._init_tables()

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(str(self._db_path), timeout=30.0)
            conn.row_factory = sqlite3.Row
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=60000")
            self._local.conn = conn
        return conn

    def _init_tables(self) -> None:
        conn = self._conn()
        conn.executescript(
            """
            CREATE TABLE IF NOT EXISTS letters (
                letter TEXT PRIMARY KEY,
                status TEXT DEFAULT 'pending',
                result_count INTEGER DEFAULT 0,
                updated_at REAL DEFAULT 0
            );
            CREATE TABLE IF NOT EXISTS companies (
                member_id TEXT PRIMARY KEY,
                company_name TEXT DEFAULT '',
                representative TEXT DEFAULT '',
                website TEXT DEFAULT '',
                phone TEXT DEFAULT '',
                address TEXT DEFAULT '',
                emails TEXT DEFAULT '',
                detail_url TEXT DEFAULT '',
                capital TEXT DEFAULT '',
                flow TEXT DEFAULT '12',
                source_letters TEXT DEFAULT '',
                detail_status TEXT DEFAULT 'pending',
                updated_at REAL DEFAULT 0
            );
            """
        )
        conn.commit()

    def seed_letters(self, letters: list[str] | tuple[str, ...]) -> None:
        conn = self._conn()
        for letter in letters:
            conn.execute(
                "INSERT OR IGNORE INTO letters(letter, updated_at) VALUES(?, ?)",
                (letter, time.time()),
            )
        conn.commit()

    def requeue_stale_running_tasks(self, *, older_than_seconds: float) -> dict[str, int]:
        """回收陈旧 running 任务，避免异常退出或双开导致挂死。"""
        conn = self._conn()
        deadline = time.time() - max(float(older_than_seconds), 1.0)
        letters = conn.execute(
            "UPDATE letters SET status='pending', updated_at=? WHERE status='running' AND updated_at < ?",
            (time.time(), deadline),
        ).rowcount
        details = conn.execute(
            "UPDATE companies SET detail_status='pending', updated_at=? WHERE detail_status='running' AND updated_at < ?",
            (time.time(), deadline),
        ).rowcount
        conn.commit()
        return {"letters": int(letters or 0), "details": int(details or 0)}

    def requeue_failed_detail_tasks(self) -> int:
        """将失败详情任务重新放回 pending，供下一次续跑重试。"""
        conn = self._conn()
        count = conn.execute(
            "UPDATE companies SET detail_status='pending', updated_at=? WHERE detail_status='failed'",
            (time.time(),),
        ).rowcount
        conn.commit()
        return int(count or 0)

    def claim_letter_task(self) -> dict[str, Any] | None:
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            "SELECT letter FROM letters WHERE status='pending' ORDER BY letter LIMIT 1"
        ).fetchone()
        if row is None:
            conn.commit()
            return None
        letter = str(row["letter"])
        updated = conn.execute(
            "UPDATE letters SET status='running', updated_at=? WHERE letter=? AND status='pending'",
            (time.time(), letter),
        ).rowcount
        conn.commit()
        if updated != 1:
            return None
        return {"letter": letter}

    def mark_letter_done(self, letter: str, *, result_count: int) -> None:
        conn = self._conn()
        conn.execute(
            "UPDATE letters SET status='done', result_count=?, updated_at=? WHERE letter=?",
            (result_count, time.time(), letter),
        )
        conn.commit()

    def mark_letter_failed(self, letter: str) -> None:
        conn = self._conn()
        conn.execute(
            "UPDATE letters SET status='failed', updated_at=? WHERE letter=?",
            (time.time(), letter),
        )
        conn.commit()

    def upsert_company_summary(self, company: dict[str, str], *, source_letter: str) -> None:
        conn = self._conn()
        member_id = str(company.get("member_id", "")).strip()
        if not member_id:
            return
        existing = conn.execute(
            "SELECT source_letters FROM companies WHERE member_id=?",
            (member_id,),
        ).fetchone()
        source_letters = {item for item in str(existing["source_letters"] or "").split(",") if item} if existing else set()
        source_letters.add(source_letter)
        conn.execute(
            """
            INSERT INTO companies (
                member_id, company_name, representative, address, capital,
                detail_url, flow, source_letters, detail_status, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'pending', ?)
            ON CONFLICT(member_id) DO UPDATE SET
                company_name=excluded.company_name,
                representative=excluded.representative,
                address=excluded.address,
                capital=excluded.capital,
                detail_url=excluded.detail_url,
                flow=excluded.flow,
                source_letters=excluded.source_letters,
                updated_at=excluded.updated_at
            """,
            (
                member_id,
                str(company.get("company_name", "")).strip(),
                str(company.get("representative", "")).strip(),
                str(company.get("address", "")).strip(),
                str(company.get("capital", "")).strip(),
                str(company.get("detail_url", "")).strip(),
                str(company.get("flow", "12")).strip() or "12",
                ",".join(sorted(source_letters)),
                time.time(),
            ),
        )
        conn.commit()

    def claim_detail_task(self) -> dict[str, Any] | None:
        conn = self._conn()
        conn.execute("BEGIN IMMEDIATE")
        row = conn.execute(
            """
            SELECT member_id, flow FROM companies
            WHERE detail_status='pending'
            ORDER BY member_id LIMIT 1
            """
        ).fetchone()
        if row is None:
            conn.commit()
            return None
        member_id = str(row["member_id"])
        updated = conn.execute(
            "UPDATE companies SET detail_status='running', updated_at=? WHERE member_id=? AND detail_status='pending'",
            (time.time(), member_id),
        ).rowcount
        conn.commit()
        if updated != 1:
            return None
        return {"member_id": member_id, "flow": str(row["flow"] or "12")}

    def save_detail_result(self, member_id: str, detail: dict[str, str]) -> None:
        conn = self._conn()
        conn.execute(
            """
            UPDATE companies
            SET company_name=?, representative=?, website=?, phone=?, address=?,
                emails=?, detail_url=?, capital=?, detail_status='done', updated_at=?
            WHERE member_id=?
            """,
            (
                str(detail.get("company_name", "")).strip(),
                str(detail.get("representative", "")).strip(),
                str(detail.get("website", "")).strip(),
                str(detail.get("phone", "")).strip(),
                str(detail.get("address", "")).strip(),
                str(detail.get("emails", "")).strip(),
                str(detail.get("detail_url", "")).strip(),
                str(detail.get("capital", "")).strip(),
                time.time(),
                member_id,
            ),
        )
        conn.commit()

    def mark_detail_failed(self, member_id: str) -> None:
        conn = self._conn()
        conn.execute(
            "UPDATE companies SET detail_status='failed', updated_at=? WHERE member_id=?",
            (time.time(), member_id),
        )
        conn.commit()

    def get_progress(self) -> dict[str, int]:
        conn = self._conn()
        return {
            "letters_done": conn.execute("SELECT COUNT(*) FROM letters WHERE status='done'").fetchone()[0],
            "letters_pending": conn.execute("SELECT COUNT(*) FROM letters WHERE status='pending'").fetchone()[0],
            "letters_running": conn.execute("SELECT COUNT(*) FROM letters WHERE status='running'").fetchone()[0],
            "details_pending": conn.execute("SELECT COUNT(*) FROM companies WHERE detail_status='pending'").fetchone()[0],
            "details_running": conn.execute("SELECT COUNT(*) FROM companies WHERE detail_status='running'").fetchone()[0],
            "companies_total": conn.execute("SELECT COUNT(*) FROM companies").fetchone()[0],
        }

    def count_details_done(self) -> int:
        conn = self._conn()
        return conn.execute("SELECT COUNT(*) FROM companies WHERE detail_status='done'").fetchone()[0]

    def get_company(self, member_id: str) -> dict[str, Any] | None:
        conn = self._conn()
        row = conn.execute("SELECT * FROM companies WHERE member_id=?", (member_id,)).fetchone()
        return dict(row) if row is not None else None

    def export_companies(self) -> list[dict[str, Any]]:
        conn = self._conn()
        rows = conn.execute("SELECT * FROM companies ORDER BY member_id").fetchall()
        return [dict(row) for row in rows]

    def close(self) -> None:
        conn = getattr(self._local, "conn", None)
        if conn is not None:
            conn.close()
            self._local.conn = None
