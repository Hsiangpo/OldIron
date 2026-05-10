"""Clash unix-socket 控制器。"""

from __future__ import annotations

import json
import subprocess
from dataclasses import dataclass


@dataclass(slots=True)
class ClashSelectorState:
    name: str
    now: str
    all: list[str]


class ClashUnixController:
    """通过 mihomo unix socket 读写代理组。"""

    def __init__(self, unix_socket_path: str) -> None:
        self._unix_socket_path = unix_socket_path

    def get_selector(self, name: str) -> ClashSelectorState:
        payload = self._curl_json(f"/proxies/{name}")
        return ClashSelectorState(
            name=str(payload.get("name") or name),
            now=str(payload.get("now") or "").strip(),
            all=[str(item or "").strip() for item in list(payload.get("all") or []) if str(item or "").strip()],
        )

    def set_selector(self, name: str, choice: str) -> None:
        self._curl_json(
            f"/proxies/{name}",
            method="PUT",
            data={"name": choice},
        )

    def cycle_selector(self, name: str, *, ignore: set[str] | None = None) -> str:
        state = self.get_selector(name)
        candidates = [item for item in state.all if item and item not in (ignore or set())]
        if not candidates:
            return state.now
        if state.now not in candidates:
            self.set_selector(name, candidates[0])
            return candidates[0]
        index = candidates.index(state.now)
        picked = candidates[(index + 1) % len(candidates)]
        self.set_selector(name, picked)
        return picked

    def _curl_json(self, path: str, *, method: str = "GET", data: dict | None = None) -> dict:
        cmd = [
            "curl",
            "--unix-socket",
            self._unix_socket_path,
            "-s",
            "-X",
            method,
            "-H",
            "Content-Type: application/json",
            "http://localhost" + path,
        ]
        if data is not None:
            cmd.extend(["-d", json.dumps(data, ensure_ascii=False)])
        result = subprocess.run(  # noqa: S603
            cmd,
            capture_output=True,
            text=True,
            timeout=10,
            check=False,
        )
        if result.returncode != 0:
            raise RuntimeError(result.stderr.strip() or f"curl failed: {cmd}")
        text = str(result.stdout or "").strip()
        if not text:
            return {}
        return json.loads(text)
