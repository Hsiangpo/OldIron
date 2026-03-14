"""indonesiayp 解析器测试。"""

from __future__ import annotations

import unittest

from indonesia_crawler.indonesiayp.parser import (
    extract_total_pages,
    parse_detail_page,
    parse_list_page,
)


LIST_HTML = """
<html><body>
<a href="/company/837090/Wanaco_Indo_Niaga_PT">Wanaco Indo Niaga, PT</a>
<a href="/company/837090/Wanaco_Indo_Niaga_PT">View Profile</a>
<a href="/company/840179/Go_Bintan_Travel">Go Bintan Travel</a>
<a href="/company/840179/Go_Bintan_Travel">View Profile</a>
<a href="/category/general_business/2433">2433</a>
</body></html>
"""


DETAIL_HTML = """
<html><body>
<h1>Wanaco Indo Niaga, PT - Surabaya, Indonesia</h1>
<div>Company manager</div>
<div>Achmad Sjaechu Naslan</div>
<div>Website address</div>
<a href="/redir/837090?u=www.wanacoindoniaga.com">www.wanacoindoniaga.com</a>
<a href="mailto:sjaechu@gmail.com">sjaechu@gmail.com</a>
<a href="mailto:SALES@WANACO.COM">SALES@WANACO.COM</a>
</body></html>
"""


class TestIndonesiaypParser(unittest.TestCase):
    """覆盖列表/详情关键解析逻辑。"""

    def test_parse_list_page_extracts_company_links(self) -> None:
        records = parse_list_page(LIST_HTML)
        self.assertEqual(len(records), 2)
        self.assertEqual(records[0].comp_id, "IYP_837090")
        self.assertEqual(records[0].company_name, "Wanaco Indo Niaga, PT")
        self.assertEqual(records[0].detail_path, "/company/837090/Wanaco_Indo_Niaga_PT")
        self.assertEqual(records[1].comp_id, "IYP_840179")

    def test_extract_total_pages(self) -> None:
        total_pages = extract_total_pages(LIST_HTML)
        self.assertEqual(total_pages, 2433)

    def test_parse_detail_page_extracts_manager_homepage_and_emails(self) -> None:
        detail = parse_detail_page(DETAIL_HTML, "/company/837090/Wanaco_Indo_Niaga_PT")
        self.assertEqual(detail.company_name, "Wanaco Indo Niaga, PT")
        self.assertEqual(detail.ceo, "Achmad Sjaechu Naslan")
        self.assertEqual(detail.homepage, "https://www.wanacoindoniaga.com")
        self.assertEqual(detail.emails, ["sjaechu@gmail.com", "sales@wanaco.com"])


if __name__ == "__main__":
    unittest.main()
