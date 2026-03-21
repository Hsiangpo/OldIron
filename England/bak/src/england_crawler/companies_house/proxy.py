"""Companies House 代理配置。"""

from __future__ import annotations

from dataclasses import dataclass

from curl_cffi import CurlOpt


@dataclass(slots=True)
class BlurpathProxyConfig:
    enabled: bool
    host: str
    port: int
    username: str
    password: str
    region: str
    sticky_minutes: int
    preproxy_url: str = ""

    def build_username(self, session_id: str) -> str:
        return (
            f"{self.username}-zone-resi-region-{self.region}"
            f"-st--city--session-{session_id}-sessionTime-{self.sticky_minutes}"
        )

    def build_proxy_url(self, session_id: str) -> str:
        return (
            f"http://{self.build_username(session_id)}:{self.password}"
            f"@{self.host}:{self.port}"
        )

    def build_curl_options(self) -> dict[CurlOpt, str]:
        if not self.preproxy_url.strip():
            return {}
        return {CurlOpt.PRE_PROXY: self.preproxy_url.strip()}

    def describe_preproxy(self) -> str:
        return self.preproxy_url.strip() or "-"
