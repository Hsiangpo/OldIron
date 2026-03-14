from __future__ import annotations

import os
from pathlib import Path

import pytest

from malaysia_crawler.manager_agent import ManagerAgentConfig
from malaysia_crawler.manager_agent import ManagerAgentService


def _can_run_live() -> bool:
    if os.getenv("RUN_LIVE_MANAGER_TEST", "").strip() != "1":
        return False
    if not os.getenv("LLM_API_KEY", "").strip():
        return False
    return True


@pytest.mark.skipif(not _can_run_live(), reason="未启用 live manager 测试")
def test_nomatech_manager_extraction_live() -> None:
    root = Path(__file__).resolve().parents[2]
    config = ManagerAgentConfig.from_env(root)
    if not config.llm_base_url:
        os.environ["LLM_BASE_URL"] = "https://api.gpteamservices.com/v1"
        config = ManagerAgentConfig.from_env(root)
    if not config.llm_model:
        os.environ["LLM_MODEL"] = "gpt-5.1-codex-mini"
        config = ManagerAgentConfig.from_env(root)

    seed = root.parent / "wikipedia" / "output" / "firecrawl_keys.txt"
    ManagerAgentService.ensure_keys_file(config.firecrawl_keys_file, seed)
    service = ManagerAgentService.from_config(config)
    result = service.enrich_manager(
        company_name="Nomatech",
        domain="www.nomatech.com.my",
        candidate_pool=[
            "https://www.nomatech.com.my/",
            "https://www.nomatech.com.my/about",
            "https://www.nomatech.com.my/about-us",
            "https://www.nomatech.com.my/company",
            "https://www.nomatech.com.my/management",
            "https://www.nomatech.com.my/team",
            "https://www.nomatech.com.my/directors",
            "https://www.nomatech.com.my/contact",
        ],
        tried_urls=[],
    )

    assert result.success is True
    assert result.manager_name
    assert result.manager_role
    role = result.manager_role.lower()
    assert ("manager" in role) or ("managing director" in role)
    assert result.evidence_url.startswith("https://www.nomatech.com.my/")
