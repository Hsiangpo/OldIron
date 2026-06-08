# -*- coding: utf-8 -*-
"""buffettcode 采集流程：行业总索引 → 行业翻页采公司链接 → 详情页抓三件套。

并发模型：
  - 1 个 lister 线程（独占一个日本 IP 会话）：按公司数从多到少遍历行业，逐页采公司链接入库；
  - N 个 detail worker 线程（各独占一个日本 IP 会话，互不抢锁）：轮询 pending 公司抓详情入库；
  - 任一会话摊上坏 IP 会自动换 IP，不拖累其他 worker；
  - lister 跑完置 list_done；worker 在无 pending 且 list_done 后退出。
全程断点续跑：行业 last_page、公司 status 都落库。
"""
from __future__ import annotations

import logging
import threading
import time

from . import parser
from .store import Store
from .waf import WafSession

log = logging.getLogger("buffettcode.pipeline")

_BASE = "https://www.buffett-code.com"


def bootstrap_industries(store: Store, session: WafSession) -> int:
    """首次运行：拉 /industries 解析顶层行业清单入库（排除 10 个不采分类）。"""
    if store.industries_count() > 0:
        log.info("行业清单已存在，跳过 bootstrap")
        return store.industries_count()
    log.info("拉取行业总索引 /industries ...")
    r = None
    for attempt in range(4):
        try:
            r = session.get(f"{_BASE}/industries")
            break
        except Exception as e:  # noqa: BLE001
            log.warning("行业索引抓取异常(%d/4)：%s，重试", attempt + 1, str(e)[:80])
            time.sleep(5)
    if r is None or r.status_code != 200:
        log.error("行业索引抓取失败 status=%s", getattr(r, "status_code", None))
        return 0
    rows = parser.parse_industry_index(r.text)
    n = store.upsert_industries(rows)
    total = sum(c for _, _, c in rows)
    log.info("行业清单写入 %d 个（剔除排除项后），预计公司总量 ≈ %s", n, f"{total:,}")
    return n


def crawl_industry_lists(store: Store, session: WafSession, list_done: threading.Event,
                         max_pages_per_industry: int, max_industries: int,
                         page_delay: float) -> None:
    """lister：遍历行业（公司数从多到少），逐页采公司链接。"""
    done_industries = 0
    handled: set[str] = set()
    try:
        while True:
            row = store.next_industry_to_list()
            if not row:
                log.info("所有行业已列举完毕")
                break
            iid, name, cnt, last_page, total_pages = row
            if iid in handled:
                log.info("无新行业可列举（其余被限页暂停，留待全量运行续跑）")
                break
            if max_industries and done_industries >= max_industries:
                log.info("达到 max_industries=%d，停止列举", max_industries)
                break
            handled.add(iid)
            log.info("列举行业 %s %s（公司数≈%s，从第 %d 页继续）", iid, name, f"{cnt:,}", last_page + 1)
            completed = _list_one_industry(store, session, iid, last_page, total_pages,
                                           max_pages_per_industry, page_delay)
            if completed:
                store.mark_industry_done(iid)
            done_industries += 1
    finally:
        list_done.set()
        log.info("lister 结束，本轮处理 %d 个行业", done_industries)


def _list_one_industry(store: Store, session: WafSession, iid: str, last_page: int,
                       total_pages: int, max_pages: int, page_delay: float) -> bool:
    """列举单个行业若干页。返回 True=已采到该行业末页(可标 done)，False=被限页暂停未完成。"""
    page = last_page + 1
    fails = 0
    while True:
        url = f"{_BASE}/industries/{iid}?page={page}"
        try:
            r = session.get(url)
        except Exception as e:  # noqa: BLE001
            fails += 1
            log.warning("列表页失败 %s：%s（第%d次）", url, str(e)[:60], fails)
            if fails >= 3:
                log.warning("行业 %s 第 %d 页连续失败（多 IP 均不通，疑似到达分页上限），本行业列举结束", iid, page)
                return True
            time.sleep(4)
            continue
        if r.status_code != 200:
            fails += 1
            log.warning("列表页失败 %s status=%s（第%d次）", url, r.status_code, fails)
            if fails >= 3:
                log.warning("行业 %s 第 %d 页连续 %s（疑似到达分页上限），本行业列举结束", iid, page, r.status_code)
                return True
            time.sleep(3)
            continue
        fails = 0
        paths, total = parser.parse_list_page(r.text)
        if total_pages == 0:
            total_pages = total
        store.set_industry_listing(iid, total_pages)
        added = store.add_company_paths(iid, paths)
        store.advance_industry_page(iid, page)
        log.info("  %s p%d/%d：本页公司 %d，新增 %d", iid, page, total_pages, len(paths), added)
        if not paths or page >= total_pages:
            return True  # 真正到末页
        if max_pages and page >= max_pages:
            return False  # 被测试限页暂停，留 'listing' 状态供续跑
        page += 1
        time.sleep(page_delay)


def detail_worker(store: Store, session: WafSession, list_done: threading.Event,
                  worker_delay: float, idle_limit: int = 3) -> None:
    """detail worker：抓公司详情页解析入库（失败退避，避免热循环）。"""
    idle = 0
    while True:
        paths = store.claim_pending_companies(8)
        if not paths:
            if list_done.is_set() and store.pending_total() == 0:
                idle += 1
                if idle >= idle_limit:
                    return
            time.sleep(2)
            continue
        idle = 0
        for path in paths:
            url = f"{_BASE}{path}"
            try:
                r = session.get(url)
                if r.status_code != 200:
                    store.fail_company(path, give_up=False)
                    time.sleep(1.0)
                    continue
                data = parser.parse_detail(r.text)
                if not data.get("company_name"):
                    store.fail_company(path, give_up=True)
                    continue
                store.save_company(path, data)
                log.debug("  详情 %s -> %s | %s", path, data.get("company_name"), data.get("representative"))
            except Exception as e:  # noqa: BLE001
                log.warning("详情抓取异常 %s: %s", path, str(e)[:80])
                store.fail_company(path, give_up=False)
                time.sleep(1.0)
            time.sleep(worker_delay)


def progress_reporter(store: Store, stop: threading.Event, interval: int = 20) -> None:
    """周期性打印采集进度（详情阶段很长，给实时反馈，避免"看着像卡死"）。"""
    last_fetched, last_t = 0, time.time()
    while not stop.wait(interval):
        c = store.counts()
        fetched = c.get("fetched", 0)
        now = time.time()
        rate = (fetched - last_fetched) / max(1e-6, now - last_t) * 60.0
        log.info("进度 | 已抓 %d | 待抓 %d | 失败 %d | 近 %ds 速率 %.0f 家/分",
                 fetched, c.get("pending", 0) + c.get("fetching", 0), c.get("failed", 0),
                 interval, rate)
        last_fetched, last_t = fetched, now


def run_pipeline(store: Store, sessions: list[WafSession], *, max_pages_per_industry: int,
                 max_industries: int, page_delay: float, worker_delay: float,
                 retry_failed: bool = False) -> None:
    """启动 lister + detail workers（各独占一个会话），阻塞直到全部完成。"""
    recovered = store.recover_stale()
    if recovered:
        log.info("崩溃恢复：%d 个公司从 fetching 回退为 pending", recovered)
    if retry_failed:
        n = store.requeue_failed()
        log.info("重爬失败项：%d 个 failed 公司重置为 pending（rowid 小，将被最先抓取）", n)
    bootstrap_industries(store, sessions[0])

    list_done = threading.Event()
    worker_sessions = sessions[1:] if len(sessions) > 1 else sessions
    threads = [threading.Thread(
        target=crawl_industry_lists, name="lister",
        args=(store, sessions[0], list_done, max_pages_per_industry, max_industries, page_delay),
        daemon=True,
    )]
    for i, sess in enumerate(worker_sessions):
        threads.append(threading.Thread(
            target=detail_worker, name=f"detail-{i}",
            args=(store, sess, list_done, worker_delay), daemon=True,
        ))
    stop_report = threading.Event()
    reporter = threading.Thread(target=progress_reporter, name="progress",
                                args=(store, stop_report), daemon=True)
    reporter.start()
    for t in threads:
        t.start()
    try:
        # 用可中断的 sleep 轮询代替 join：Windows 上对无超时 join 的 Ctrl+C 无效
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
    except KeyboardInterrupt:
        log.info("收到 Ctrl+C，停止采集（进度已落库，下次同命令自动续跑）")
        stop_report.set()
        raise
    stop_report.set()
    reporter.join(timeout=2)
    log.info("采集完成，公司状态统计：%s", store.counts())
