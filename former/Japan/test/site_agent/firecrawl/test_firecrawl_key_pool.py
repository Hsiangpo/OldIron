import asyncio
import tempfile
import time
import unittest
from pathlib import Path

from site_agent.firecrawl_key_pool import KeyPool
from site_agent.firecrawl_key_pool import KeyPoolConfig
from site_agent.firecrawl_key_pool import KeyState


class TestKeyPool(unittest.TestCase):
    def test_load_keys_filters_comments(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            path = Path(tmpdir) / "keys.txt"
            path.write_text(
                """
            # comment line
            
            key_one
            key_two
            """.strip(),
                encoding="utf-8",
            )
            keys = KeyPool.load_keys(path)
            self.assertEqual(keys, ["key_one", "key_two"])

    def test_acquire_release_respects_limit(self) -> None:
        async def run() -> None:
            pool = KeyPool(
                ["k1"],
                KeyPoolConfig(
                    per_key_limit=1,
                    wait_seconds=0.2,
                    check_interval=0.01,
                    shared_pool=False,
                ),
            )
            lease1 = await pool.acquire()
            try:
                with self.assertRaises(RuntimeError):
                    await pool.acquire()
            finally:
                await lease1.release()

        asyncio.run(run())

    def test_rate_limit_and_disable(self) -> None:
        async def run() -> None:
            pool = KeyPool(
                ["k1"],
                KeyPoolConfig(per_key_limit=1, cooldown_seconds=5, shared_pool=False),
            )
            await pool.mark_rate_limited(0, retry_after=2.0)
            snapshot = await pool.snapshot()
            self.assertEqual(snapshot[0].state, KeyState.COOLDOWN)
            self.assertIsNotNone(snapshot[0].cooldown_until)
            cooldown_until = snapshot[0].cooldown_until
            self.assertIsNotNone(cooldown_until)
            if cooldown_until is not None:
                now = time.monotonic()
                self.assertTrue(cooldown_until > now)

            await pool.disable(0, "unauthorized")
            snapshot = await pool.snapshot()
            self.assertEqual(snapshot[0].state, KeyState.DISABLED)
            self.assertEqual(snapshot[0].disabled_reason, "unauthorized")

        asyncio.run(run())

    def test_payment_required_removes_key_from_file(self) -> None:
        async def run() -> None:
            with tempfile.TemporaryDirectory() as tmpdir:
                path = Path(tmpdir) / "keys.txt"
                path.write_text("k1\nk2\n", encoding="utf-8")
                pool = KeyPool(
                    ["k1", "k2"],
                    KeyPoolConfig(shared_pool=False),
                    key_file_path=path,
                )
                await pool.disable(0, "payment_required")
                keys = KeyPool.load_keys(path)
                self.assertEqual(keys, ["k2"])
                snapshot = await pool.snapshot()
                self.assertEqual(snapshot[0].state, KeyState.DISABLED)
                self.assertEqual(snapshot[0].disabled_reason, "payment_required")

        asyncio.run(run())


if __name__ == "__main__":
    unittest.main()
