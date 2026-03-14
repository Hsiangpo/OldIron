import asyncio

import pytest

from site_agent.crawler import CrawlerClient, _should_reset_crawler_error


def test_should_reset_on_missing_new_context():
    err = RuntimeError("connection closed")
    assert _should_reset_crawler_error(err) is True


@pytest.mark.asyncio
async def test_reset_request_persists_when_reset_fails():
    client = CrawlerClient(asyncio.Semaphore(1))
    client._reset_requested = True
    client._active_fetches = 0

    async def _fail_reset():
        return False

    client._reset_crawler = _fail_reset
    result = await client._try_reset_if_idle()

    assert result is False
    assert client._reset_requested is True
