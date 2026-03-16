import sys
import tempfile
import unittest
from pathlib import Path

from openpyxl import Workbook


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class CompaniesHouseInputTests(unittest.TestCase):
    def test_load_company_names_from_xlsx_skips_header_blank_and_duplicates(self) -> None:
        from england_crawler.companies_house.input_xlsx import load_company_names_from_xlsx

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "companies.xlsx"
            workbook = Workbook()
            sheet = workbook.active
            sheet.append(["CompanyName"])
            sheet.append(["TPL SERVICES LTD"])
            sheet.append(["  "])
            sheet.append(["TPL SERVICES LTD."])
            sheet.append(["ZZZ DEVELOPMENTS LTD"])
            workbook.save(path)

            names = load_company_names_from_xlsx(path)

        self.assertEqual(
            ["TPL SERVICES LTD", "ZZZ DEVELOPMENTS LTD"],
            names,
        )

    def test_load_company_names_from_text_skips_blank_and_duplicates(self) -> None:
        from england_crawler.companies_house.input_source import load_company_names_from_source

        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "companies.txt"
            path.write_text(
                "CompanyName\nTPL SERVICES LTD\n\nTPL SERVICES LTD.\nZZZ DEVELOPMENTS LTD\n",
                encoding="utf-8",
            )

            names = load_company_names_from_source(path)

        self.assertEqual(["TPL SERVICES LTD", "ZZZ DEVELOPMENTS LTD"], names)


if __name__ == "__main__":
    unittest.main()
