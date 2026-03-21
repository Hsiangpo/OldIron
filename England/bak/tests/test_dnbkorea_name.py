import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DnbEnglandNameTests(unittest.TestCase):
    def test_resolve_company_name_uses_dnb_english_for_uk(self) -> None:
        from england_crawler.dnb.naming import resolve_company_name

        self.assertEqual(
            "SAMSUNG C AND T CORP",
            resolve_company_name(
                company_name_en_dnb="SAMSUNG C AND T CORP",
                company_name_local_gmap="삼성물산",
                company_name_local_site="삼성엔지니어링",
            ),
        )
        self.assertEqual(
            "SAMSUNG C AND T CORP",
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
