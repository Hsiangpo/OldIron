import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DnbKoreaNameTests(unittest.TestCase):
    def test_has_korean_company_name_rejects_dirty_gmap_labels(self) -> None:
        from korea_crawler.dnb.naming import has_korean_company_name

        self.assertIs(False, has_korean_company_name("현재 게시가 사용 중지됨"))
        self.assertIs(False, has_korean_company_name("이름, 위치, 영업시간 등 수정"))
        self.assertIs(True, has_korean_company_name("삼성물산"))

    def test_resolve_company_name_prefers_korean_then_falls_back_to_dnb_english(self) -> None:
        from korea_crawler.dnb.naming import resolve_company_name

        self.assertEqual(
            "삼성물산",
            resolve_company_name(
                company_name_en_dnb="SAMSUNG C AND T CORP",
                company_name_local_gmap="삼성물산",
                company_name_local_site="삼성엔지니어링",
            ),
        )
        self.assertEqual(
            "현대건설",
            resolve_company_name(
                company_name_en_dnb="SAMSUNG C AND T CORP",
                company_name_local_gmap="",
                company_name_local_site="현대건설",
            ),
        )
        self.assertEqual(
            "SAMSUNG C AND T CORP",
            resolve_company_name(
                company_name_en_dnb="SAMSUNG C AND T CORP",
                company_name_local_gmap="",
                company_name_local_site="",
            ),
        )


if __name__ == "__main__":
    unittest.main()
