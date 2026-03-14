"""2Captcha reCAPTCHA v3 解码。"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass

from curl_cffi import requests as cffi_requests

logger = logging.getLogger(__name__)

IN_URL = "https://2captcha.com/in.php"
RES_URL = "https://2captcha.com/res.php"


class CaptchaSolveError(RuntimeError):
    """验证码求解失败。"""


@dataclass(slots=True)
class TwoCaptchaConfig:
    """2Captcha 配置。"""

    api_key: str
    site_key: str
    page_url: str
    action: str = "cari"
    min_score: float = 0.3
    timeout: float = 35.0
    poll_interval: float = 5.0
    poll_max_times: int = 24


class TwoCaptchaSolver:
    """2Captcha 客户端。"""

    def __init__(self, config: TwoCaptchaConfig) -> None:
        self.config = config
        self.session = cffi_requests.Session(impersonate="chrome110")

    def _post_task(self) -> str:
        """提交解码任务并返回 task_id。"""
        payload = {
            "key": self.config.api_key,
            "method": "userrecaptcha",
            "version": "v3",
            "googlekey": self.config.site_key,
            "pageurl": self.config.page_url,
            "action": self.config.action,
            "min_score": f"{self.config.min_score:.1f}",
            "json": "1",
        }
        response = self.session.post(IN_URL, data=payload, timeout=self.config.timeout)
        response.raise_for_status()
        data = response.json()
        if int(data.get("status", 0)) != 1:
            raise CaptchaSolveError(f"2Captcha 提交失败: {data.get('request', 'unknown')}")
        return str(data.get("request", "")).strip()

    def _poll_result(self, task_id: str) -> str:
        """轮询 task 直到拿到 token。"""
        params = {"key": self.config.api_key, "action": "get", "id": task_id, "json": "1"}
        for _ in range(self.config.poll_max_times):
            time.sleep(self.config.poll_interval)
            response = self.session.get(RES_URL, params=params, timeout=self.config.timeout)
            response.raise_for_status()
            data = response.json()
            if int(data.get("status", 0)) == 1:
                token = str(data.get("request", "")).strip()
                if token:
                    return token
                raise CaptchaSolveError("2Captcha 返回空 token")

            request_msg = str(data.get("request", "")).strip()
            if request_msg == "CAPCHA_NOT_READY":
                continue
            raise CaptchaSolveError(f"2Captcha 轮询失败: {request_msg}")
        raise CaptchaSolveError("2Captcha 超时，未获取到 token")

    def solve_token(self) -> str:
        """执行一次完整求解。"""
        task_id = self._post_task()
        logger.debug("2Captcha 任务已提交: %s", task_id)
        return self._poll_result(task_id)

    def close(self) -> None:
        """关闭连接。"""
        self.session.close()

