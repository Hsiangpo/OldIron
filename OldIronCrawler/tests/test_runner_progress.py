"""runner 进度快照单测：确保完成一批后能把存储进度正确翻译成底部计数器事件。"""

from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from oldironcrawler import runner as runner_module  # noqa: E402


class _FakeStore:
    def progress(self) -> dict[str, int]:
        return {"done": 7, "running": 50, "dropped": 3, "pending": 60, "failed_temp": 1}


def test_emit_progress_snapshot_translates_store_progress(monkeypatch) -> None:
    captured: dict[str, int] = {}
    monkeypatch.setattr(runner_module, "print_progress_heartbeat", lambda **kw: captured.update(kw))

    runner_module._emit_progress_snapshot(_FakeStore(), total=121)

    # 待处理 = 存储 pending(60) + failed_temp(1)，其余直传。
    assert captured == {"total": 121, "done": 7, "running": 50, "dropped": 3, "pending": 61}
