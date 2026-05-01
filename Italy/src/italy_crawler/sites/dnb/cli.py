"""Italy DNB CLI。"""

from __future__ import annotations

import argparse
import logging
import os
import threading
import time
from pathlib import Path

from .config import ItalyDnbConfig
from .pipeline import prepare_pipeline_list
from .pipeline import run_pipeline_email
from .pipeline import run_pipeline_list
from .pipeline import run_pipeline_verif
from .store import ItalyDnbStore


ROOT = Path(__file__).resolve().parents[4]


def _raise_nofile_limit() -> None:
    if os.name == "nt":
        return
    try:
        import resource

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = 65536
        if hard != resource.RLIM_INFINITY:
            target = min(target, hard)
        if soft < target:
            resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
    except Exception:  # noqa: BLE001
        return None


def run_site(argv: list[str]) -> int:
    _raise_nofile_limit()
    parser = argparse.ArgumentParser(description="DNB 意大利企业采集")
    parser.add_argument("mode", nargs="?", default="all", choices=["all", "list", "verif", "email"])
    parser.add_argument("--segment-workers", type=int, default=3)
    parser.add_argument("--verif-workers", type=int, default=1)
    parser.add_argument("--email-workers", type=int, default=8)
    parser.add_argument("--max-segments", type=int, default=0)
    parser.add_argument("--max-pages-per-segment", type=int, default=20)
    parser.add_argument("--industry-paths", type=str, default="", help="仅跑指定 subcategory slug，逗号分隔")
    parser.add_argument("--log-level", type=str, default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    output_dir = ROOT / "output" / "dnb"
    output_dir.mkdir(parents=True, exist_ok=True)
    config = ItalyDnbConfig.from_env(
        project_root=ROOT,
        output_dir=output_dir,
        segment_workers=args.segment_workers,
        verif_workers=args.verif_workers,
        email_workers=args.email_workers,
        max_segments=args.max_segments,
        max_pages_per_segment=args.max_pages_per_segment,
        industry_paths=args.industry_paths,
    )

    if args.mode == "list":
        print(run_pipeline_list(config=config))
        return 0

    store = ItalyDnbStore(output_dir / "dnb_store.db")
    recovered = store.requeue_stale_running_tasks(max_age_seconds=config.stale_running_requeue_seconds)
    if recovered:
        logging.getLogger(__name__).info("Italy DNB 启动回收僵住任务：%d", recovered)
    revived = store.requeue_failed_tasks()
    if revived:
        logging.getLogger(__name__).info("Italy DNB 启动回收失败任务：%d", revived)

    if args.mode == "verif":
        _run_verif_only(store=store, config=config)
        return 0
    if args.mode == "email":
        _run_email_only(store=store, config=config)
        return 0

    results = _run_all_mode(store=store, config=config)
    print(results)
    return 0


def _run_all_mode(*, store: ItalyDnbStore, config: ItalyDnbConfig) -> dict[str, int]:
    prepare_pipeline_list(store=store, config=config)
    stop_event = threading.Event()
    p1_done = threading.Event()
    list_results: dict[str, int] = {}
    logger = logging.getLogger(__name__)

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
        threading.Thread(target=_p1_runner, name="it-dnb-p1", daemon=True),
        threading.Thread(
            target=run_pipeline_verif,
            kwargs={"store": store, "config": config, "stop_event": stop_event},
            name="it-dnb-p2-verif",
            daemon=True,
        ),
        threading.Thread(
            target=run_pipeline_email,
            kwargs={"store": store, "config": config, "stop_event": stop_event},
            name="it-dnb-p3-email",
            daemon=True,
        ),
    ]
    for thread in threads:
        thread.start()
    while True:
        progress = store.progress()
        if p1_done.is_set() and _pipelines_drained(progress):
            stop_event.set()
            break
        logger.info(
            "Italy DNB all 进度：verif=%d/%d site=%d/%d companies=%d final=%d",
            progress.verif_running,
            progress.verif_pending,
            progress.site_running,
            progress.site_pending,
            progress.companies_total,
            progress.final_total,
        )
        time.sleep(config.queue_poll_interval)
    for thread in threads:
        thread.join(timeout=2)
    progress = store.progress()
    return {
        "companies": progress.companies_total,
        "final": progress.final_total,
        "verif_pending": progress.verif_pending,
        "site_pending": progress.site_pending,
        **list_results,
    }


def _run_verif_only(*, store: ItalyDnbStore, config: ItalyDnbConfig) -> None:
    stop_event = threading.Event()
    logger = logging.getLogger(__name__)
    thread = threading.Thread(
        target=run_pipeline_verif,
        kwargs={"store": store, "config": config, "stop_event": stop_event},
        name="it-dnb-verif-only",
        daemon=True,
    )
    thread.start()
    while True:
        progress = store.progress()
        if progress.verif_pending == 0 and progress.verif_running == 0:
            stop_event.set()
            break
        logger.info("Italy DNB Verif 进度：%d/%d", progress.verif_running, progress.verif_pending)
        time.sleep(config.queue_poll_interval)
    thread.join(timeout=2)


def _run_email_only(*, store: ItalyDnbStore, config: ItalyDnbConfig) -> None:
    stop_event = threading.Event()
    logger = logging.getLogger(__name__)
    thread = threading.Thread(
        target=run_pipeline_email,
        kwargs={"store": store, "config": config, "stop_event": stop_event},
        name="it-dnb-email-only",
        daemon=True,
    )
    thread.start()
    while True:
        progress = store.progress()
        if progress.site_pending == 0 and progress.site_running == 0:
            stop_event.set()
            break
        logger.info("Italy DNB Email 进度：%d/%d", progress.site_running, progress.site_pending)
        time.sleep(config.queue_poll_interval)
    thread.join(timeout=2)


def _pipelines_drained(progress) -> bool:
    return progress.verif_pending == 0 and progress.verif_running == 0 and progress.site_pending == 0 and progress.site_running == 0
