"""ahu 响应解析测试。"""

from __future__ import annotations

import unittest

from indonesia_crawler.ahu.client import AhuClient, normalize_ahu_query_name
from indonesia_crawler.ahu.parser import (
    extract_form_token,
    parse_detail_payload,
    parse_search_results,
)


SEARCH_HTML = """
<html><body>
<form id="admin-ubah-form">
  <input type="hidden" name="mxyplyzyk" value="abc123token" />
</form>
<section id="hasil_cari">
  <div class="cl0">
    <strong class="judul">PERSEROAN TERBATAS WANACO INDO NIAGA</strong>
    <div class="alamat">Alamat A</div>
    <div class="detail_pemilik_manfaat" data-id="%7Eabc%3F">Detail</div>
  </div>
  <div class="cl0">
    <strong class="judul">PERSEROAN TERBATAS CONTOH MAJU</strong>
    <div class="alamat">Alamat B</div>
    <div class="detail_pemilik_manfaat" data-id="%7Exyz%3F">Detail</div>
  </div>
</section>
</body></html>
"""


DETAIL_JSON = """
{
  "status": 200,
  "message": "success",
  "value": [
    {
      "nama_korporasi": "WANACO INDO NIAGA",
      "data_pemilik_manfaat": [
        {"nama_lengkap": "ACHMAD SJAECHU NASLAN"},
        {"nama_lengkap": "HELLY FARADIS"}
      ]
    }
  ]
}
"""


class TestAhuParser(unittest.TestCase):
    """覆盖 AHU 搜索页与详情 JSON 解析。"""

    def test_extract_form_token(self) -> None:
        token = extract_form_token(SEARCH_HTML)
        self.assertEqual(token, "abc123token")

    def test_parse_search_results(self) -> None:
        results = parse_search_results(SEARCH_HTML)
        self.assertEqual(len(results), 2)
        self.assertEqual(results[0].nama_korporasi, "PERSEROAN TERBATAS WANACO INDO NIAGA")
        self.assertEqual(results[0].detail_id, "%7Eabc%3F")
        self.assertEqual(results[1].alamat, "Alamat B")

    def test_parse_detail_payload(self) -> None:
        detail = parse_detail_payload(DETAIL_JSON)
        self.assertEqual(detail.nama_korporasi, "WANACO INDO NIAGA")
        self.assertEqual(detail.pemilik_manfaat, ["ACHMAD SJAECHU NASLAN", "HELLY FARADIS"])

    def test_normalize_ahu_query_name_strip_prefix_and_suffix(self) -> None:
        self.assertEqual(normalize_ahu_query_name("PT. Trimitra Wisesa Abadi"), "TRIMITRA WISESA ABADI")
        self.assertEqual(normalize_ahu_query_name("Satria Baja Kumala PT"), "SATRIA BAJA KUMALA")
        self.assertEqual(normalize_ahu_query_name("CV. Maju Jaya Tbk."), "MAJU JAYA")

    def test_is_captcha_rejected_should_avoid_false_positive(self) -> None:
        client = AhuClient()
        try:
            normal_html = """
            <html><head>
            <script src="https://www.google.com/recaptcha/api.js?render=site-key"></script>
            </head><body>
            <div>Ditjen AHU tidak melakukan verifikasi terhadap data yang disampaikan.</div>
            </body></html>
            """
            self.assertFalse(client._is_captcha_rejected(normal_html))

            rejected_html = '<div class="alert-box">Captcha tidak valid, silakan ulangi.</div>'
            self.assertTrue(client._is_captcha_rejected(rejected_html))
        finally:
            client.close()


if __name__ == "__main__":
    unittest.main()
