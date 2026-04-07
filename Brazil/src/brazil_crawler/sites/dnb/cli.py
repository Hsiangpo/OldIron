"""DNB 巴西 CLI。"""

from __future__ import annotations

import argparse
import logging
import threading
import time
from pathlib import Path

from oldiron_core.fc_email import FirecrawlEmailSettings

from .config import DnbBrConfig
from .pipeline import prepare_pipeline_list
from .pipeline import run_pipeline_list
from .pipeline2_gmap import run_pipeline_gmap
from .pipeline3_email import run_pipeline_email
from .store import DnbBrStore


ROOT = Path(__file__).resolve().parents[4]


def run_dnb(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(description="DNB 巴西企业采集")
    parser.add_argument("mode", nargs="?", default="all", choices=["all", "list", "gmap", "email"])
    parser.add_argument("--segment-workers", type=int, default=4)
    parser.add_argument("--detail-workers", type=int, default=4)
    parser.add_argument("--gmap-workers", type=int, default=16)
    parser.add_argument("--email-workers", type=int, default=16)
    parser.add_argument("--max-segments", type=int, default=0, help="P1 最多处理多少个小类切片（0=全部）")
    parser.add_argument("--max-pages-per-segment", type=int, default=20, help="P1 单切片最大页数（DNB 实际硬上限 20）")
    parser.add_argument("--industry-paths", type=str, default="", help="仅运行指定小类 slug，逗号分隔")
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    output_dir = ROOT / "output" / "dnb"
    output_dir.mkdir(parents=True, exist_ok=True)
    config = DnbBrConfig.from_env(
        project_root=ROOT,
        output_dir=output_dir,
        segment_workers=args.segment_workers,
        detail_workers=args.detail_workers,
        gmap_workers=args.gmap_workers,
        email_workers=args.email_workers,
        max_segments=args.max_segments,
        max_pages_per_segment=args.max_pages_per_segment,
        industry_paths=args.industry_paths,
    )
    config.validate(skip_email=args.mode in {"list", "gmap"})

    if args.mode == "list":
        print(run_pipeline_list(config=config))
        return 0

    store = DnbBrStore(output_dir / "dnb_store.db")
    recovered = store.requeue_stale_running_tasks()
    if recovered:
        logging.getLogger("brazil_crawler.sites.dnb.cli").info("DNB 启动回收僵住任务：%d", recovered)
    revived = store.requeue_failed_tasks()
    if revived:
        logging.getLogger("brazil_crawler.sites.dnb.cli").info("DNB 启动回收失败任务：%d", revived)
    cleaned = store.purge_bad_websites()
    if cleaned:
        logging.getLogger("brazil_crawler.sites.dnb.cli").info("DNB 启动清理脏官网：%d", cleaned)
    seeded = store.enqueue_site_for_ready_websites()
    if seeded:
        logging.getLogger("brazil_crawler.sites.dnb.cli").info("DNB 启动补种官网任务：%d", seeded)

    if args.mode == "gmap":
        _run_gmap_only(store=store, config=config)
        return 0

    if args.mode == "email":
        settings = _build_email_settings(config)
        _run_email_only(store=store, settings=settings, config=config)
        return 0

    results = _run_all_mode(store=store, config=config)
    print(results)
    return 0


def _build_email_settings(config: DnbBrConfig) -> FirecrawlEmailSettings:
    return FirecrawlEmailSettings(
        project_root=config.project_root,
        crawl_backend="protocol",
        llm_api_key=config.llm_api_key,
        llm_base_url=config.llm_base_url,
        llm_model=config.llm_model,
        llm_reasoning_effort=config.llm_reasoning_effort,
        llm_api_style=config.llm_api_style,
        llm_timeout_seconds=config.llm_timeout_seconds,
        prefilter_limit=12,
        llm_pick_count=5,
        extract_max_urls=5,
    )


def _run_all_mode(*, store: DnbBrStore, config: DnbBrConfig) -> dict[str, int]:
    prepare_pipeline_list(store=store, config=config)
    settings = _build_email_settings(config)
    stop_event = threading.Event()
    p1_done = threading.Event()
    list_results: dict[str, int] = {}
    logger = logging.getLogger("brazil_crawler.sites.dnb.cli")
    last_stale_recovery = 0.0

    def _p1_runner() -> None:
        try:
            list_results.update(
                run_pipeline_list(
                    config=config,
                    store=store,
                    auto_prepare=False,
                    close_store=False,
                )
            )
        finally:
            p1_done.set()

    threads = [
        threading.Thread(target=_p1_runner, name="dnb-p1", daemon=True),
        threading.Thread(
            target=run_pipeline_gmap,
            kwargs={
                "store": store,
                "workers": config.gmap_workers,
                "stop_event": stop_event,
                "queue_poll_interval": config.queue_poll_interval,
            },
            name="dnb-p2",
            daemon=True,
        ),
        threading.Thread(
            target=run_pipeline_email,
            kwargs={
                "store": store,
                "settings": settings,
                "workers": config.email_workers,
                "stop_event": stop_event,
            },
            name="dnb-p3",
            daemon=True,
        ),
    ]
    for thread in threads:
        thread.start()
    while True:
        last_stale_recovery = _recover_stale_tasks_if_needed(
            store=store,
            config=config,
            logger=logger,
            last_stale_recovery=last_stale_recovery,
        )
        progress = store.progress()
        if p1_done.is_set() and _pipelines_drained(progress):
            stop_event.set()
            break
        logger.info(
            "DNB all 进度：detail=%d/%d gmap=%d/%d site=%d/%d final=%d",
            progress.detail_running,
            progress.detail_pending,
            progress.gmap_running,
            progress.gmap_pending,
            progress.site_running,
            progress.site_pending,
            progress.final_total,
        )
        threading.Event().wait(config.queue_poll_interval)
    for thread in threads:
        thread.join(timeout=2)
    final_progress = store.progress()
    return {
        "companies": final_progress.companies_total,
        "final": final_progress.final_total,
        "gmap_pending": final_progress.gmap_pending,
        "site_pending": final_progress.site_pending,
        **list_results,
    }


def _run_gmap_only(*, store: DnbBrStore, config: DnbBrConfig) -> None:
    logger = logging.getLogger("brazil_crawler.sites.dnb.cli")
    store.requeue_running_tasks()
    store.enqueue_gmap_for_missing_websites()
    stop_event = threading.Event()
    last_stale_recovery = 0.0
    thread = threading.Thread(
        target=run_pipeline_gmap,
        kwargs={
            "store": store,
            "workers": config.gmap_workers,
            "stop_event": stop_event,
            "queue_poll_interval": config.queue_poll_interval,
        },
        name="dnb-gmap-only",
        daemon=True,
    )
    thread.start()
    while True:
        last_stale_recovery = _recover_stale_tasks_if_needed(
            store=store,
            config=config,
            logger=logger,
            last_stale_recovery=last_stale_recovery,
        )
        progress = store.progress()
        if progress.gmap_pending == 0 and progress.gmap_running == 0:
            stop_event.set()
            break
        logger.info("DNB gmap 进度：%d/%d", progress.gmap_running, progress.gmap_pending)
        threading.Event().wait(config.queue_poll_interval)
    thread.join(timeout=2)


def _run_email_only(*, store: DnbBrStore, settings: FirecrawlEmailSettings, config: DnbBrConfig) -> None:
    logger = logging.getLogger("brazil_crawler.sites.dnb.cli")
    store.requeue_running_tasks()
    stop_event = threading.Event()
    last_stale_recovery = 0.0
    thread = threading.Thread(
        target=run_pipeline_email,
        kwargs={
            "store": store,
            "settings": settings,
            "workers": config.email_workers,
            "stop_event": stop_event,
        },
        name="dnb-email-only",
        daemon=True,
    )
    thread.start()
    while True:
        last_stale_recovery = _recover_stale_tasks_if_needed(
            store=store,
            config=config,
            logger=logger,
            last_stale_recovery=last_stale_recovery,
        )
        progress = store.progress()
        if progress.site_pending == 0 and progress.site_running == 0:
            stop_event.set()
            break
        logger.info("DNB email 进度：%d/%d final=%d", progress.site_running, progress.site_pending, progress.final_total)
        threading.Event().wait(config.queue_poll_interval)
    thread.join(timeout=2)


def _pipelines_drained(progress) -> bool:
    return (
        progress.segment_pending == 0
        and progress.segment_running == 0
        and progress.detail_pending == 0
        and progress.detail_running == 0
        and progress.gmap_pending == 0
        and progress.gmap_running == 0
        and progress.site_pending == 0
        and progress.site_running == 0
    )


def _recover_stale_tasks_if_needed(*, store: DnbBrStore, config: DnbBrConfig, logger: logging.Logger, last_stale_recovery: float) -> float:
    now = time.monotonic()
    interval = min(max(config.stale_running_requeue_seconds / 3, 60.0), 300.0)
    if now - last_stale_recovery < interval:
        return last_stale_recovery
    recovered = store.requeue_stale_running_tasks(config.stale_running_requeue_seconds)
    if recovered:
        logger.warning("DNB 运行中回收僵住任务：%d", recovered)
    return now
