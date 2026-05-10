"""CNPJ Biz 代表人选择测试。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[2]
SRC = ROOT / "src"
PROJECT_ROOT = ROOT.parent
SHARED_DIR = PROJECT_ROOT / "shared"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
if str(SHARED_DIR) not in sys.path:
    sys.path.insert(0, str(SHARED_DIR))

from brazil_crawler.sites.cnpjbiz.config import CnpjBizConfig
from brazil_crawler.sites.cnpjbiz.selector import CnpjBizRepresentativeSelector
from brazil_crawler.sites.cnpjbiz.selector import RepresentativeCandidate


class _FakeLlm:
    def __init__(self, payload):
        self.payload = payload

    def _call_json(self, prompt: str):  # noqa: SLF001
        _ = prompt
        return dict(self.payload)

    def close(self) -> None:
        return None


class _ErrorLlm:
    def __init__(self) -> None:
        self.calls = 0

    def _call_json(self, prompt: str):  # noqa: SLF001
        _ = prompt
        self.calls += 1
        raise RuntimeError("401 无效的令牌")

    def close(self) -> None:
        return None


class CnpjBizSelectorTests(unittest.TestCase):
    def test_fallback_prefers_socio_administrador(self) -> None:
        selector = CnpjBizRepresentativeSelector(CnpjBizConfig(project_root=Path("."), output_dir=Path(".")))
        try:
            picked = selector.choose(
                company_name="Acme",
                cnpj="1",
                candidates=[
                    RepresentativeCandidate(name="Maria Silva", role="Sócia"),
                    RepresentativeCandidate(name="Joao Souza", role="Sócio-Administrador"),
                ],
            )
        finally:
            selector.close()
        self.assertEqual("Joao Souza", picked)

    def test_llm_result_must_match_existing_candidate(self) -> None:
        selector = CnpjBizRepresentativeSelector(
            CnpjBizConfig(project_root=Path("."), output_dir=Path(".")),
            llm_client=_FakeLlm({"representative": "Maria Silva", "role": "Sócia"}),
        )
        try:
            picked = selector.choose(
                company_name="Acme",
                cnpj="1",
                candidates=[
                    RepresentativeCandidate(name="Maria Silva", role="Sócia"),
                    RepresentativeCandidate(name="Joao Souza", role="Sócio-Administrador"),
                ],
            )
        finally:
            selector.close()
        self.assertEqual("Maria Silva", picked)

    def test_auth_error_disables_llm_for_following_calls(self) -> None:
        llm = _ErrorLlm()
        selector = CnpjBizRepresentativeSelector(
            CnpjBizConfig(project_root=Path("."), output_dir=Path(".")),
            llm_client=llm,
        )
        candidates = [
            RepresentativeCandidate(name="Maria Silva", role="Sócia"),
            RepresentativeCandidate(name="Joao Souza", role="Sócio-Administrador"),
        ]
        try:
            first = selector.choose(company_name="Acme", cnpj="1", candidates=candidates)
            second = selector.choose(company_name="Acme", cnpj="1", candidates=candidates)
        finally:
            selector.close()
        self.assertEqual("Joao Souza", first)
        self.assertEqual("Joao Souza", second)
        self.assertEqual(1, llm.calls)


if __name__ == "__main__":
    unittest.main()
