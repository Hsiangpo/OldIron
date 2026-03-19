"""丹麦 Go 后端起停命令。"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from pathlib import Path
from urllib.request import urlopen


ROOT = Path(__file__).resolve().parents[3]
VERSATILE_ROOT = ROOT.parent / "VersatileBackend"
BACKEND_OUTPUT = ROOT / "output" / "backend"
SERVICE_DEFS = {
    "myip": {
        "cmd": ["go", "run", "./cmd/myip-service"],
        "addr": "http://127.0.0.1:17897",
        "log": "myip-service.log",
        "pid": "myip-service.pid",
        "env": {"MYIP_SERVICE_ADDR": ":17897"},
    },
    "gmap": {
        "cmd": ["go", "run", "./cmd/gmap-service"],
        "addr": "http://127.0.0.1:8082",
        "log": "gmap-service.log",
        "pid": "gmap-service.pid",
        "env": {"GMAP_SERVICE_ADDR": ":8082"},
    },
    "firecrawl": {
        "cmd": ["go", "run", "./cmd/firecrawl-service"],
        "addr": "http://127.0.0.1:8081",
        "log": "firecrawl-service.log",
        "pid": "firecrawl-service.pid",
        "env": {"FIRECRAWL_SERVICE_ADDR": ":8081"},
    },
}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="丹麦 Go 后端起停管理")
    parser.add_argument("action", choices=("start", "stop", "status"), help="执行动作")
    parser.add_argument(
        "--services",
        default="myip,gmap,firecrawl",
        help="服务列表，逗号分隔，可选 myip,gmap,firecrawl",
    )
    return parser


def run_backend(argv: list[str]) -> int:
    args = build_parser().parse_args(argv)
    services = parse_services(args.services)
    if args.action == "start":
        return start_services(services)
    if args.action == "stop":
        return stop_services(services)
    return status_services(services)


def parse_services(raw: str) -> list[str]:
    values: list[str] = []
    for chunk in str(raw or "").split(","):
        value = chunk.strip().lower()
        if value in SERVICE_DEFS and value not in values:
            values.append(value)
    return values or ["myip", "gmap", "firecrawl"]


def start_services(services: list[str]) -> int:
    ensure_services_started(services, quiet=False)
    return 0


def ensure_services_started(services: list[str], *, quiet: bool) -> list[str]:
    BACKEND_OUTPUT.mkdir(parents=True, exist_ok=True)
    started_now: list[str] = []
    for name in services:
        if is_service_running(name):
            if not quiet:
                print(f"{name} 已在运行")
            continue
        definition = SERVICE_DEFS[name]
        log_path = BACKEND_OUTPUT / definition["log"]
        pid_path = BACKEND_OUTPUT / definition["pid"]
        env = build_service_env(definition["env"])
        if name == "myip" and not myip_service_should_start(env):
            if not quiet:
                print("myip 已跳过：未启用或未配置上游代理列表")
            continue
        with log_path.open("ab") as log_fp:
            process = subprocess.Popen(  # noqa: S603
                definition["cmd"],
                cwd=VERSATILE_ROOT,
                env=env,
                stdout=log_fp,
                stderr=log_fp,
                creationflags=_creation_flags(),
            )
        pid_path.write_text(json.dumps({"pid": process.pid}, ensure_ascii=False), encoding="utf-8")
        wait_for_health(name, timeout_seconds=15.0)
        started_now.append(name)
        if not quiet:
            print(f"{name} 已启动")
    return started_now


def stop_services(services: list[str]) -> int:
    stop_service_names(services, quiet=False)
    return 0


def stop_service_names(services: list[str], *, quiet: bool) -> None:
    for name in services:
        definition = SERVICE_DEFS[name]
        pid_path = BACKEND_OUTPUT / definition["pid"]
        payload = read_pid_payload(pid_path)
        pid = int(payload.get("pid", 0) or 0)
        if pid <= 0 or not pid_exists(pid):
            if not quiet:
                print(f"{name} 未运行")
            if pid_path.exists():
                pid_path.unlink()
            continue
        terminate_pid(pid)
        if pid_path.exists():
            pid_path.unlink()
        if not quiet:
            print(f"{name} 已停止")


def status_services(services: list[str]) -> int:
    for name in services:
        running = is_service_running(name)
        health = check_health(name) if running else False
        print(f"{name}: running={str(running).lower()} health={str(health).lower()}")
    return 0


def build_service_env(extra_env: dict[str, str]) -> dict[str, str]:
    env = os.environ.copy()
    for key, value in load_env_file(VERSATILE_ROOT / ".env").items():
        env.setdefault(key, value)
    outbound_proxy = resolve_outbound_proxy(env)
    env.setdefault("HTTP_PROXY", outbound_proxy)
    env.setdefault("HTTPS_PROXY", outbound_proxy)
    for key, value in extra_env.items():
        env[key] = value
    return env


def load_env_file(path: Path) -> dict[str, str]:
    if not path.exists():
        return {}
    values: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8").splitlines():
        text = line.strip()
        if not text or text.startswith("#") or "=" not in text:
            continue
        key, value = text.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def resolve_outbound_proxy(env: dict[str, str]) -> str:
    if env_flag(env, "MYIP_ENABLED"):
        return (
            env.get("MYIP_PROXY_URL", "").strip()
            or env.get("OUTBOUND_PROXY_URL", "").strip()
            or "http://127.0.0.1:17897"
        )
    return env.get("PROXY_URL", "").strip() or "http://127.0.0.1:7897"


def myip_service_should_start(env: dict[str, str]) -> bool:
    if not env_flag(env, "MYIP_ENABLED"):
        return False
    if env.get("MYIP_UPSTREAMS", "").strip() or env.get("PROXY_POOL_UPSTREAMS", "").strip():
        return True
    file_path = (
        env.get("MYIP_UPSTREAMS_FILE", "").strip()
        or env.get("PROXY_POOL_UPSTREAMS_FILE", "").strip()
    )
    return bool(file_path)


def env_flag(env: dict[str, str], key: str) -> bool:
    value = str(env.get(key, "")).strip().lower()
    return value in {"1", "true", "yes", "on"}


def _creation_flags() -> int:
    if os.name != "nt":
        return 0
    return getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0)


def is_service_running(name: str) -> bool:
    definition = SERVICE_DEFS[name]
    payload = read_pid_payload(BACKEND_OUTPUT / definition["pid"])
    pid = int(payload.get("pid", 0) or 0)
    return pid > 0 and pid_exists(pid)


def read_pid_payload(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def pid_exists(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def terminate_pid(pid: int) -> None:
    if os.name == "nt":
        subprocess.run(["taskkill", "/PID", str(pid), "/T", "/F"], check=False, capture_output=True)  # noqa: S603
        return
    os.kill(pid, signal.SIGTERM)


def check_health(name: str) -> bool:
    try:
        with urlopen(SERVICE_DEFS[name]["addr"] + "/healthz", timeout=3.0) as response:  # noqa: S310
            return response.status == 200
    except Exception:
        return False


def wait_for_health(name: str, timeout_seconds: float) -> None:
    deadline = time.time() + max(timeout_seconds, 1.0)
    while time.time() < deadline:
        if check_health(name):
            return
        time.sleep(0.5)
    raise RuntimeError(f"{name} 启动超时，请查看日志：{BACKEND_OUTPUT / SERVICE_DEFS[name]['log']}")
