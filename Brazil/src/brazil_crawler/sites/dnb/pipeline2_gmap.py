"""DNB 巴西 Pipeline 2：GMap 补官网与电话。"""

from __future__ import annotations

import logging
import threading
import time

from oldiron_core.google_maps import GoogleMapsClient
from oldiron_core.google_maps import GoogleMapsConfig

from .store import DnbBrStore


LOGGER = logging.getLogger(__name__)


def run_pipeline_gmap(
    *,
    store: DnbBrStore,
    workers: int,
    stop_event,
    queue_poll_interval: float = 2.0,
) -> None:
    threads = [
        threading.Thread(
            target=_gmap_worker,
            args=(store, stop_event),
            name=f"dnb-gmap-{index + 1}",
            daemon=True,
        )
        for index in range(max(int(workers or 1), 1))
    ]
    for thread in threads:
        thread.start()
    try:
        while not stop_event.is_set():
            progress = store.progress()
            if progress.gmap_pending == 0:
                store.enqueue_gmap_for_missing_websites()
            time.sleep(queue_poll_interval)
    finally:
        for thread in threads:
            thread.join(timeout=2)


def _gmap_worker(store: DnbBrStore, stop_event) -> None:
    client = GoogleMapsClient(GoogleMapsConfig(hl="en", gl="br"))
    while not stop_event.is_set():
        task = store.claim_gmap_task()
        if task is None:
            time.sleep(1.0)
            continue
        try:
            result = client.search_company_profile(
                f"{task.company_name} {task.address} {task.city} {task.region} Brazil",
                task.company_name,
            )
            website = result.website if result else ""
            phone = result.phone if result else ""
            store.complete_gmap_task(task.duns, website, phone)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("DNB GMap 失败：%s | %s", task.duns, exc)
            try:
                store.fail_gmap_task(task.duns)
            except Exception as fail_exc:  # noqa: BLE001
                LOGGER.error("DNB GMap fail_gmap_task 也失败了：%s | %s", task.duns, fail_exc)
