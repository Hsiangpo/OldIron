from __future__ import annotations

import sys
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SHARED_DIR = ROOT / "shared"
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from oldiron_core.fc_email.llm_client import EmailUrlLlmClient


class LlmRateLimitBackoffTests(unittest.TestCase):
    def test_429_wait_is_no_longer_30_to_60_seconds(self) -> None:
        client = object.__new__(EmailUrlLlmClient)
        client._client = SimpleNamespace(
            chat=SimpleNamespace(
                completions=SimpleNamespace(
                    create=lambda **kwargs: (_ for _ in ()).throw(Exception("429 rate_limit"))
                )
            ),
            responses=SimpleNamespace(
                create=lambda **kwargs: (_ for _ in ()).throw(Exception("429 rate_limit"))
            ),
        )

        sleeps: list[float] = []

        def fake_sleep(seconds: float) -> None:
            sleeps.append(seconds)
            raise RuntimeError("stop after first sleep")

        with patch("time.sleep", side_effect=fake_sleep), patch("random.random", return_value=0.0):
            with self.assertRaisesRegex(RuntimeError, "stop after first sleep"):
                client._call_api_with_retry(channel="chat", kwargs={})

        self.assertEqual([5.0], sleeps)


if __name__ == "__main__":
    unittest.main()
