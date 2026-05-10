"""CNPJ Biz 专用 Chrome 启动器。"""

from __future__ import annotations

import subprocess
from pathlib import Path


class CnpjBizChromeLauncher:
    """启动一个独立 profile 的 Chrome 调试实例。"""

    def __init__(self, *, debug_port: int, profile_dir: str, proxy_url: str, seed_url: str) -> None:
        self._debug_port = debug_port
        self._profile_dir = Path(profile_dir).expanduser()
        self._proxy_url = str(proxy_url or "").strip()
        self._seed_url = str(seed_url or "").strip()

    def launch(self) -> None:
        self._profile_dir.mkdir(parents=True, exist_ok=True)
        cmd = [
            "open",
            "-na",
            "Google Chrome",
            "--args",
            f"--user-data-dir={self._profile_dir}",
            f"--remote-debugging-port={self._debug_port}",
            "--remote-debugging-address=127.0.0.1",
        ]
        if self._proxy_url:
            cmd.append(f"--proxy-server={self._proxy_url}")
        if self._seed_url:
            cmd.append(self._seed_url)
        subprocess.Popen(cmd)  # noqa: S603
