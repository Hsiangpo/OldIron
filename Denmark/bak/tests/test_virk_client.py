from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class VirkClientTests(unittest.TestCase):
    def test_parse_search_rows_extracts_email_and_phone(self) -> None:
        from denmark_crawler.virk.client import parse_search_rows

        payload = {
            "virksomhedTotal": 2,
            "enheder": [
                {
                    "cvr": "25297288",
                    "senesteNavn": "Fjordvejen ApS",
                    "beliggenhedsadresse": "Fabers Alle 24\n5300 Kerteminde",
                    "by": "Kerteminde",
                    "postnummer": "5300",
                    "status": "NORMAL",
                    "virksomhedsform": "Anpartsselskab",
                    "hovedbranche": "682040 Udlejning af erhvervsejendomme",
                    "startDato": "2000-03-31",
                    "telefonnummer": "22115015",
                    "email": "finn@munkebokro.dk",
                    "enhedstype": "virksomhed",
                }
            ],
        }

        rows, total = parse_search_rows(payload)
        self.assertEqual(2, total)
        self.assertEqual(1, len(rows))
        self.assertEqual("Fjordvejen ApS", rows[0].company_name)
        self.assertEqual("22115015", rows[0].phone)
        self.assertEqual(["finn@munkebokro.dk"], rows[0].emails)

    def test_merge_detail_extracts_representative_and_detail_emails(self) -> None:
        from denmark_crawler.virk.client import merge_detail
        from denmark_crawler.virk.models import VirkSearchCompany

        search_row = VirkSearchCompany(
            cvr="25297288",
            company_name="Fjordvejen ApS",
            emails=["finn@munkebokro.dk"],
            phone="22115015",
        )
        payload = {
            "stamdata": {
                "navn": "Fjordvejen ApS",
                "adresse": "Fabers Alle 24",
                "postnummerOgBy": "5300 Kerteminde",
                "status": "NORMAL",
                "virksomhedsform": "Anpartsselskab",
                "startdato": "2000-03-31",
            },
            "personkreds": {
                "personkredser": [
                    {
                        "rolleTekstnogle": "erstdist-organisation-rolle-direktion",
                        "personRoller": [
                            {
                                "senesteNavn": "Finn Egebjerg Rasmussen",
                                "enhedstype": "PERSON",
                                "titlePrefix": ["erstdist-organisation-rolle-adm_dir"],
                            }
                        ],
                    }
                ]
            },
            "ejerforhold": {
                "aktiveLegaleEjere": [
                    {"senesteNavn": "Egebjerg ApS"}
                ]
            },
            "produktionsenheder": {
                "aktiveProduktionsenheder": [
                    {
                        "stamdata": {
                            "email": "mail@lmeconsulting.dk",
                            "telefon": "22115015",
                        }
                    }
                ]
            },
        }

        record = merge_detail(search_row, payload)
        self.assertEqual("Finn Egebjerg Rasmussen", record.representative)
        self.assertEqual("Egebjerg ApS", record.legal_owner)
        self.assertIn("finn@munkebokro.dk", record.emails)
        self.assertIn("mail@lmeconsulting.dk", record.emails)


if __name__ == "__main__":
    unittest.main()
