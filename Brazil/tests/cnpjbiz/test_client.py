"""CNPJ Biz 客户端测试。"""

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

from brazil_crawler.sites.cnpjbiz.client import CnpjBizClient
from brazil_crawler.sites.cnpjbiz.client import _decrypt_gcm_b64
from brazil_crawler.sites.cnpjbiz.client import parse_list_page
from brazil_crawler.sites.cnpjbiz.config import CnpjBizConfig


LIST_HTML = """
<html>
  <head><link rel="next" href="https://cnpj.biz/empresas?id=34390518"></head>
  <body>
    <ul role="list">
      <li>
        <a href="https://cnpj.biz/44516693000145">
          <p>Antonio Jansen Farias de Arruda</p>
          <p>BAIXADA</p>
          <p>44.516.693/0001-45</p>
          <p>Florianópolis/SC</p>
          <p>Aberta em 07/12/2021</p>
        </a>
      </li>
      <li>
        <a href="https://cnpj.biz/19091843000179">
          <p>Forte Wash - Forte Wash - Lavagem e Manutencao de Veiculos LTDA</p>
          <p>BAIXADA</p>
          <p>19.091.843/0001-79</p>
          <p>Recife/PE</p>
          <p>Aberta em 17/10/2013</p>
        </a>
      </li>
    </ul>
  </body>
</html>
"""

DETAIL_HTML = """
<html>
  <body>
    <p>CNPJ: 45.652.925/0001-55 - 45652925000155</p>
    <p>Razão Social: 3A Contabilidade e Assessoria Empresarial LTDA</p>
    <p>Nome Fantasia: 3A Contabilidade</p>
    <p>Data da Abertura: 15/03/2022</p>
    <p>Situação: Ativa</p>
    <p>Logradouro: Avenida 03, Quadra-42, 7</p>
    <p>Bairro: Conjunto Habitacional Vinhais</p>
    <p>CEP: 65071-020</p>
    <p>Município: São Luís</p>
    <p>Estado: Maranhão</p>
    <span class="email-container" data-email-iv="fU98APUbAMlZjN85" data-email-ct="gvH/vUjn3R2wvzzQ6CCP8jxYHIY0MFm4Pdvmvz6GHEu6SlLBzsga2R0=" data-email-tag="GGgwDt1BqUHItS0xs/mw3Q=="></span>
    <span class="telefone-container" data-telefone-iv="nw4c7ZK9WFcwKR1S" data-telefone-ct="OtXDitoFwllm7WmZvIP1" data-telefone-tag="enDf8wdxcay9XuRMcD2MVg=="></span>
    <div>Quadro de Sócios e Administradores</div>
    <div>Roberto dos Santos da Cruz - Sócio-Administrador</div>
    <div>Maria Jose Silva - Sócia</div>
    <div>Qualificação do responsável pela empresa: Sócio-Administrador</div>
  </body>
</html>
"""


class CnpjBizClientTests(unittest.TestCase):
    def test_browser_fetch_client_is_enabled_by_config(self) -> None:
        config = CnpjBizConfig(
            project_root=Path("."),
            output_dir=Path("."),
            browser_fetch_enabled=True,
        )
        client = CnpjBizClient(config)
        try:
            self.assertIsNotNone(client._browser_fetch)  # noqa: SLF001
        finally:
            client.close()

    def test_active_proxy_uses_feed_pool_when_present(self) -> None:
        config = CnpjBizConfig(
            project_root=Path("."),
            output_dir=Path("."),
            proxy_feed_url="https://example.com/feed",
            proxy_feed_scheme="http",
        )
        client = CnpjBizClient(config)
        try:
            client._proxy_pool = type("Pool", (), {"current_proxy": lambda self: "http://1.1.1.1:80"})()  # noqa: SLF001
            self.assertEqual("http://1.1.1.1:80", client._active_proxy_url())  # noqa: SLF001
        finally:
            client.close()

    def test_active_proxy_prefers_fixed_proxy_over_pool(self) -> None:
        config = CnpjBizConfig(
            project_root=Path("."),
            output_dir=Path("."),
            proxy_url="http://127.0.0.1:7893",
            proxy_feed_url="https://example.com/feed",
            proxy_feed_scheme="http",
        )
        client = CnpjBizClient(config)
        try:
            client._proxy_pool = type("Pool", (), {"current_proxy": lambda self: "http://1.1.1.1:80"})()  # noqa: SLF001
            self.assertEqual("http://127.0.0.1:7893", client._active_proxy_url())  # noqa: SLF001
        finally:
            client.close()

    def test_parse_list_page_reads_records_and_next_url(self) -> None:
        parsed = parse_list_page(LIST_HTML, "https://cnpj.biz/empresas")

        self.assertEqual("https://cnpj.biz/empresas?id=34390518", parsed.next_url)
        self.assertEqual(2, len(parsed.records))
        self.assertEqual("44516693000145", parsed.records[0].cnpj)
        self.assertEqual("Florianópolis", parsed.records[0].city)
        self.assertEqual("SC", parsed.records[0].region)
        self.assertEqual("07/12/2021", parsed.records[0].opened_at)

    def test_parse_detail_profile_extracts_candidates_and_revealed_contacts(self) -> None:
        config = CnpjBizConfig(project_root=Path("."), output_dir=Path("."))
        client = CnpjBizClient(config)
        try:
            client._reveal_contacts = lambda **kwargs: {  # noqa: SLF001
                "emails": ["atendimento@3acontabilidadedigital.com.br"],
                "phone": "(98) 98439-8131",
            }
            profile = client._parse_detail_profile(DETAIL_HTML, "https://cnpj.biz/45652925000155")  # noqa: SLF001
        finally:
            client.close()

        self.assertEqual("45652925000155", profile.cnpj)
        self.assertEqual("3A Contabilidade e Assessoria Empresarial LTDA", profile.company_name)
        self.assertEqual("(98) 98439-8131", profile.phone)
        self.assertEqual(["atendimento@3acontabilidadedigital.com.br"], profile.emails)
        self.assertEqual(2, len(profile.representative_candidates))
        self.assertEqual("Roberto dos Santos da Cruz", profile.representative_candidates[0].name)

    def test_decrypt_gcm_b64_uses_site_vector(self) -> None:
        email = _decrypt_gcm_b64(
            key_b64="ghl/S580vCtFBOZUWrbxkXDj6pXHzvBWPp1d03P3t84=",
            iv_b64="fU98APUbAMlZjN85",
            ct_b64="gvH/vUjn3R2wvzzQ6CCP8jxYHIY0MFm4Pdvmvz6GHEu6SlLBzsga2R0=",
            tag_b64="GGgwDt1BqUHItS0xs/mw3Q==",
        )
        phone = _decrypt_gcm_b64(
            key_b64="ghl/S580vCtFBOZUWrbxkXDj6pXHzvBWPp1d03P3t84=",
            iv_b64="nw4c7ZK9WFcwKR1S",
            ct_b64="OtXDitoFwllm7WmZvIP1",
            tag_b64="enDf8wdxcay9XuRMcD2MVg==",
        )

        self.assertEqual("atendimento@3acontabilidadedigital.com.br", email)
        self.assertEqual("(98) 98439-8131", phone)


if __name__ == "__main__":
    unittest.main()
