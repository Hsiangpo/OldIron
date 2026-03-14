"""England 集群协调器。"""

from __future__ import annotations

import json
import logging
import threading
import time
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from urllib.parse import urlparse

from england_crawler.cluster.config import ClusterConfig
from england_crawler.cluster.db import ClusterDb
from england_crawler.cluster.export import export_cluster_snapshots
from england_crawler.cluster.repository import ClusterRepository
from england_crawler.cluster.schema import initialize_schema


logger = logging.getLogger(__name__)


class CoordinatorRuntime:
    """England 集群 HTTP 协调器。"""

    def __init__(self, config: ClusterConfig) -> None:
        self._config = config
        self._db = ClusterDb(config.postgres_dsn)
        self._repo = ClusterRepository(self._db, config)
        self._stop_event = threading.Event()
        self._last_export_at = 0.0

    def serve_forever(self) -> None:
        self._config.validate()
        initialize_schema(self._db)
        server = ThreadingHTTPServer(
            (self._config.coordinator_host, self._config.coordinator_port),
            self._build_handler(),
        )
        maintainer = threading.Thread(target=self._maintenance_loop, name="England-Cluster-Maintainer", daemon=True)
        maintainer.start()
        logger.info(
            "England 集群协调器启动：host=%s port=%d dsn=%s",
            self._config.coordinator_host,
            self._config.coordinator_port,
            self._config.postgres_dsn,
        )
        try:
            server.serve_forever()
        finally:
            self._stop_event.set()
            server.server_close()
            maintainer.join(timeout=3.0)

    def _maintenance_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                self._repo.requeue_expired_tasks()
                now = time.monotonic()
                if now - self._last_export_at >= self._config.snapshot_export_interval_seconds:
                    export_cluster_snapshots(self._db, self._config.output_root, include_delivery=True)
                    self._last_export_at = now
            except Exception as exc:  # noqa: BLE001
                logger.warning("England cluster 后台维护失败：%s", exc)
            self._stop_event.wait(2.0)

    def _build_handler(self):
        runtime = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:  # noqa: N802
                path = urlparse(self.path).path
                if path == "/healthz":
                    self._write_json(200, {"ok": True})
                    return
                self._write_json(404, {"error": "not_found"})

            def do_POST(self) -> None:  # noqa: N802
                if runtime._config.cluster_token:
                    token = self.headers.get("X-OldIron-Token", "").strip()
                    if token != runtime._config.cluster_token:
                        self._write_json(403, {"error": "forbidden"})
                        return
                body = self._read_json()
                if body is None:
                    self._write_json(400, {"error": "invalid_json"})
                    return
                path = urlparse(self.path).path
                try:
                    if path == "/api/v1/workers/register":
                        runtime._repo.register_worker(
                            worker_id=str(body.get("worker_id", "")).strip(),
                            host_name=str(body.get("host_name", "")).strip(),
                            platform=str(body.get("platform", "")).strip(),
                            capabilities=list(body.get("capabilities", []) or []),
                            git_commit=str(body.get("git_commit", "")).strip(),
                            python_version=str(body.get("python_version", "")).strip(),
                        )
                        self._write_json(200, {"ok": True})
                        return
                    if path == "/api/v1/workers/heartbeat":
                        runtime._repo.heartbeat(str(body.get("worker_id", "")).strip())
                        self._write_json(200, {"ok": True})
                        return
                    if path == "/api/v1/tasks/claim":
                        task = runtime._repo.claim_task(
                            str(body.get("worker_id", "")).strip(),
                            list(body.get("capabilities", []) or []),
                        )
                        if task is None:
                            self._write_json(200, {"task": None})
                            return
                        self._write_json(
                            200,
                            {
                                "task": {
                                    "task_id": task.task_id,
                                    "pipeline": task.pipeline,
                                    "task_type": task.task_type,
                                    "entity_id": task.entity_id,
                                    "retries": task.retries,
                                    "payload": task.payload,
                                }
                            },
                        )
                        return
                    if path.startswith("/api/v1/tasks/") and path.endswith("/complete"):
                        task_id = path.removeprefix("/api/v1/tasks/").removesuffix("/complete")
                        runtime._repo.complete_task(
                            task_id=task_id,
                            worker_id=str(body.get("worker_id", "")).strip(),
                            result=dict(body.get("result", {}) or {}),
                        )
                        self._write_json(200, {"ok": True})
                        return
                    if path.startswith("/api/v1/tasks/") and path.endswith("/fail"):
                        task_id = path.removeprefix("/api/v1/tasks/").removesuffix("/fail")
                        runtime._repo.fail_task(
                            task_id=task_id,
                            worker_id=str(body.get("worker_id", "")).strip(),
                            error_text=str(body.get("error_text", "")).strip(),
                            retry_delay_seconds=float(body.get("retry_delay_seconds", 0.0) or 0.0),
                            fatal=bool(body.get("fatal", False)),
                        )
                        self._write_json(200, {"ok": True})
                        return
                    if path == "/api/v1/firecrawl/lease":
                        lease = runtime._repo.acquire_firecrawl_key(str(body.get("worker_id", "")).strip())
                        self._write_json(200, {"key_hash": lease.key_hash, "key_value": lease.key_value})
                        return
                    if path == "/api/v1/firecrawl/release":
                        runtime._repo.release_firecrawl_key(
                            key_hash=str(body.get("key_hash", "")).strip(),
                            outcome=str(body.get("outcome", "")).strip(),
                            retry_after_seconds=float(body.get("retry_after_seconds", 0.0) or 0.0),
                            reason=str(body.get("reason", "")).strip(),
                        )
                        self._write_json(200, {"ok": True})
                        return
                except Exception as exc:  # noqa: BLE001
                    logger.warning("England 集群接口失败：path=%s error=%s", path, exc)
                    self._write_json(500, {"error": str(exc)})
                    return
                self._write_json(404, {"error": "not_found"})

            def log_message(self, fmt: str, *args) -> None:  # noqa: A003
                logger.debug("cluster-http " + fmt, *args)

            def _read_json(self) -> dict[str, object] | None:
                length = int(self.headers.get("Content-Length", "0") or 0)
                raw = self.rfile.read(length) if length > 0 else b"{}"
                try:
                    payload = json.loads(raw.decode("utf-8"))
                except json.JSONDecodeError:
                    return None
                return payload if isinstance(payload, dict) else None

            def _write_json(self, status_code: int, payload: dict[str, object]) -> None:
                data = json.dumps(payload, ensure_ascii=False).encode("utf-8")
                self.send_response(status_code)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.end_headers()
                self.wfile.write(data)

        return Handler
