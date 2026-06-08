# -*- coding: utf-8 -*-
"""buffettcode 存储层：行业 + 公司两张表，SQLite WAL，支持断点续跑。

线程安全：每线程独立连接（threading.local），共享一个库文件；
WAL 模式允许多读单写，写入靠 SQLite 锁 + busy_timeout 串行化。
"""
from __future__ import annotations

import sqlite3
import threading
import time
from pathlib import Path


# 行业状态：pending=未列举, listing=翻页中, done=该行业公司链接已采全
# 公司状态：pending=待抓详情, fetched=已抓到, failed=多次失败放弃
_SCHEMA = """
CREATE TABLE IF NOT EXISTS industries (
    industry_id TEXT PRIMARY KEY,
    name        TEXT,
    cnt         INTEGER DEFAULT 0,
    status      TEXT DEFAULT 'pending',
    last_page   INTEGER DEFAULT 0,
    total_pages INTEGER DEFAULT 0,
    updated_at  REAL
);
CREATE TABLE IF NOT EXISTS companies (
    detail_path  TEXT PRIMARY KEY,
    industry_id  TEXT,
    company_name TEXT,
    representative TEXT,
    website      TEXT,
    address      TEXT,
    capital      TEXT,
    status       TEXT DEFAULT 'pending',
    attempts     INTEGER DEFAULT 0,
    updated_at   REAL
);
CREATE INDEX IF NOT EXISTS idx_comp_status ON companies(status);
CREATE INDEX IF NOT EXISTS idx_comp_industry ON companies(industry_id);
"""


class Store:
    """buffettcode 采集存储。"""

    def __init__(self, db_path: str):
        self._path = str(db_path)
        Path(self._path).parent.mkdir(parents=True, exist_ok=True)
        self._local = threading.local()
        self._init_lock = threading.Lock()
        with self._init_lock:
            conn = self._conn()
            conn.executescript(_SCHEMA)
            conn.commit()

    def _conn(self) -> sqlite3.Connection:
        conn = getattr(self._local, "conn", None)
        if conn is None:
            conn = sqlite3.connect(self._path, timeout=30, check_same_thread=False)
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA busy_timeout=30000")
            self._local.conn = conn
        return conn

    # ---------- 行业 ----------
    def upsert_industries(self, rows: list[tuple[str, str, int]]) -> int:
        """写入行业清单（id, name, cnt）；已存在则只更新名称/计数，不动进度。"""
        conn = self._conn()
        now = time.time()
        n = 0
        for iid, name, cnt in rows:
            conn.execute(
                "INSERT INTO industries(industry_id, name, cnt, status, updated_at) "
                "VALUES(?,?,?, 'pending', ?) "
                "ON CONFLICT(industry_id) DO UPDATE SET name=excluded.name, cnt=excluded.cnt",
                (iid, name, cnt, now),
            )
            n += 1
        conn.commit()
        return n

    def industries_count(self) -> int:
        return self._conn().execute("SELECT COUNT(*) FROM industries").fetchone()[0]

    def next_industry_to_list(self) -> tuple | None:
        """取下一个待翻页行业，按公司数从多到少（先跑量大的）。"""
        row = self._conn().execute(
            "SELECT industry_id, name, cnt, last_page, total_pages FROM industries "
            "WHERE status IN ('pending','listing') ORDER BY cnt DESC LIMIT 1"
        ).fetchone()
        return row

    def set_industry_listing(self, iid: str, total_pages: int) -> None:
        conn = self._conn()
        conn.execute(
            "UPDATE industries SET status='listing', total_pages=? , updated_at=? WHERE industry_id=?",
            (total_pages, time.time(), iid),
        )
        conn.commit()

    def advance_industry_page(self, iid: str, page: int) -> None:
        conn = self._conn()
        conn.execute(
            "UPDATE industries SET last_page=?, updated_at=? WHERE industry_id=?",
            (page, time.time(), iid),
        )
        conn.commit()

    def mark_industry_done(self, iid: str) -> None:
        conn = self._conn()
        conn.execute(
            "UPDATE industries SET status='done', updated_at=? WHERE industry_id=?",
            (time.time(), iid),
        )
        conn.commit()

    def iter_done_industries(self) -> list[tuple]:
        return self._conn().execute(
            "SELECT industry_id, name, cnt FROM industries ORDER BY cnt DESC"
        ).fetchall()

    # ---------- 公司 ----------
    def add_company_paths(self, industry_id: str, paths: list[str]) -> int:
        """批量登记公司详情链接（去重，已存在忽略）。返回新增数。"""
        conn = self._conn()
        now = time.time()
        before = conn.total_changes
        conn.executemany(
            "INSERT OR IGNORE INTO companies(detail_path, industry_id, status, updated_at) "
            "VALUES(?,?, 'pending', ?)",
            [(p, industry_id, now) for p in paths],
        )
        conn.commit()
        return conn.total_changes - before

    def claim_pending_companies(self, limit: int) -> list[str]:
        """取一批待抓公司并占位（status->fetching），避免多线程重复抓。"""
        conn = self._conn()
        rows = conn.execute(
            "SELECT detail_path FROM companies WHERE status='pending' ORDER BY rowid LIMIT ?", (limit,)
        ).fetchall()
        paths = [r[0] for r in rows]
        if paths:
            qs = ",".join("?" * len(paths))
            conn.execute(
                f"UPDATE companies SET status='fetching', updated_at=? WHERE detail_path IN ({qs})",
                [time.time(), *paths],
            )
            conn.commit()
        return paths

    def save_company(self, path: str, data: dict) -> None:
        conn = self._conn()
        conn.execute(
            "UPDATE companies SET company_name=?, representative=?, website=?, address=?, "
            "capital=?, status='fetched', updated_at=? WHERE detail_path=?",
            (data.get("company_name"), data.get("representative"), data.get("website"),
             data.get("address"), data.get("capital"), time.time(), path),
        )
        conn.commit()

    def fail_company(self, path: str, give_up: bool, max_attempts: int = 5) -> None:
        conn = self._conn()
        if give_up:
            conn.execute(
                "UPDATE companies SET status='failed', attempts=attempts+1, updated_at=? WHERE detail_path=?",
                (time.time(), path),
            )
        else:
            # 重试累计达上限自动判失败，否则回 pending 等待重抓
            conn.execute(
                "UPDATE companies SET attempts=attempts+1, updated_at=?, "
                "status=CASE WHEN attempts+1>=? THEN 'failed' ELSE 'pending' END WHERE detail_path=?",
                (time.time(), max_attempts, path),
            )
        conn.commit()

    def recover_stale(self) -> int:
        """启动时把卡在 fetching 的公司回退为 pending（崩溃恢复）。"""
        conn = self._conn()
        before = conn.total_changes
        conn.execute("UPDATE companies SET status='pending' WHERE status='fetching'")
        conn.commit()
        return conn.total_changes - before

    def requeue_failed(self) -> int:
        """把 failed 公司重置为 pending（attempts 清零），用于 CapSolver 续费后重爬。

        失败项 rowid 普遍较小（出自较早行业），claim 按 rowid 升序取 → 会被最先重抓。
        返回重排的公司数。
        """
        conn = self._conn()
        before = conn.total_changes
        conn.execute(
            "UPDATE companies SET status='pending', attempts=0, updated_at=? WHERE status='failed'",
            (time.time(),),
        )
        conn.commit()
        return conn.total_changes - before

    def counts(self) -> dict:
        conn = self._conn()
        out = {}
        for status, in conn.execute("SELECT DISTINCT status FROM companies"):
            out[status] = conn.execute(
                "SELECT COUNT(*) FROM companies WHERE status=?", (status,)
            ).fetchone()[0]
        return out

    def companies_for_industry(self, industry_id: str) -> list[tuple]:
        return self._conn().execute(
            "SELECT company_name, representative, website, address, capital, detail_path "
            "FROM companies WHERE industry_id=? AND status='fetched'",
            (industry_id,),
        ).fetchall()

    def pending_total(self) -> int:
        return self._conn().execute(
            "SELECT COUNT(*) FROM companies WHERE status IN ('pending','fetching')"
        ).fetchone()[0]
