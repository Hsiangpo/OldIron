import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DnbKoreaDomainQualityTests(unittest.TestCase):
    def test_normalize_website_url_removes_internal_spaces(self) -> None:
        from korea_crawler.dnb.domain_quality import normalize_website_url

        self.assertEqual("https://www.samson.co.kr", normalize_website_url("www. samson.co.kr"))

    def test_assess_company_domain_blocks_wikipedia(self) -> None:
        from korea_crawler.dnb.domain_quality import assess_company_domain

        result = assess_company_domain("Sujeong Industrial Development Co., Ltd.", "https://ko.wikipedia.org/wiki/%EC%88%98%EC%A0%95%EA%B5%AC", source="gmap")
        self.assertTrue(result.blocked)

    def test_assess_company_domain_blocks_recruit_domain(self) -> None:
        from korea_crawler.dnb.domain_quality import assess_company_domain

        result = assess_company_domain("Taekwang Heavy Industry Co., Ltd.", "https://tkgtaekwangvina.talent.vn", source="gmap")
        self.assertTrue(result.blocked)

    def test_assess_company_domain_blocks_low_match_gmap_domain(self) -> None:
        from korea_crawler.dnb.domain_quality import assess_company_domain

        result = assess_company_domain("HKENC Co., Ltd.", "http://www.hktdc.com/manufacturers-suppliers/NHK-Distribution-Co-Ltd/en/1X069JMT", source="gmap")
        self.assertTrue(result.blocked)

    def test_assess_company_domain_accepts_matching_domain(self) -> None:
        from korea_crawler.dnb.domain_quality import assess_company_domain

        result = assess_company_domain("Samsung C&T Corporation", "https://www.samsungcnt.com", source="dnb")
        self.assertFalse(result.blocked)


if __name__ == "__main__":
    unittest.main()
