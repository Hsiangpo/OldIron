from __future__ import annotations

import sys
import unittest
from pathlib import Path
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


from taiwan_crawler.sites.ieatpe.client import IeatpeClient  # noqa: E402


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload
        self.status_code = 200

    def json(self):
        return self._payload


class _FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def post(self, url, data=None, timeout=None, headers=None):
        self.calls.append({"url": url, "data": data, "timeout": timeout, "headers": headers})
        return _FakeResponse(self.responses.pop(0))


class IeatpeClientTests(unittest.TestCase):
    def test_fetch_company_list_posts_expected_qry_payload(self) -> None:
        session = _FakeSession(
            [[{"id": "00007", "Cname": "合發貿易股份有限公司", "Cowner": "徐季安", "Caddr": "臺北市", "cpt": "1"}]]
        )
        client = IeatpeClient(session=session)
        rows = client.fetch_company_list(letter="A", flow="12")
        self.assertEqual("00007", rows[0]["member_id"])
        self.assertEqual("A", rows[0]["query_letter"])
        self.assertEqual("12", rows[0]["flow"])
        self.assertEqual(
            '{"type": 1, "flow": "12", "input": "A"}',
            session.calls[0]["data"]["qry"],
        )

    def test_fetch_company_detail_normalizes_email_and_url(self) -> None:
        session = _FakeSession(
            [
                {
                    "id": "00007",
                    "Cname": "合發貿易股份有限公司",
                    "Cowner": "徐季安",
                    "Caddr": "臺北市大安區",
                    "tel": "(02                                                )27407278",
                    "email": "prgrtp@yahoo.com.tw",
                    "email2": "",
                    "url": "http://",
                }
            ]
        )
        client = IeatpeClient(session=session)
        row = client.fetch_company_detail(member_id="00007", flow="12")
        self.assertEqual("合發貿易股份有限公司", row["company_name"])
        self.assertEqual("徐季安", row["representative"])
        self.assertEqual("(02)27407278", row["phone"])
        self.assertEqual("prgrtp@yahoo.com.tw", row["emails"])
        self.assertEqual("", row["website"])


if __name__ == "__main__":
    unittest.main()
