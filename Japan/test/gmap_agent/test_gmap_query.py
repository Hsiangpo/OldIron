import unittest

from gmap_agent.parsing import extract_phone_from_preview_text, parse_places
from gmap_agent.query import split_queries
from gmap_agent.utils import normalize_url


class TestGmapQuery(unittest.TestCase):
    def test_split_queries_lines(self):
        raw = "alpha\nbeta\n\ngamma"
        self.assertEqual(split_queries(raw), ["alpha", "beta", "gamma"])

    def test_split_queries_separators(self):
        raw = "a|b,c"
        self.assertEqual(split_queries(raw), ["a", "b", "c"])

    def test_normalize_url_rejects_space(self):
        self.assertEqual(normalize_url("https://local guide program"), "")

    def test_parse_places_extracts_phone(self):
        payload = [
            _place_entry("0x1:0x1", "株式会社A", "电话: +81 6-6341-5340"),
            _place_entry("0x1:0x2", "株式会社B", "電話: 03-1234-5678"),
            _place_entry("0x1:0x3", "株式会社C", "tel:+81399998888"),
        ]
        records = parse_places(payload, source="google_maps")
        self.assertEqual(len(records), 3)
        self.assertEqual(records[0].phone, "+81 6-6341-5340")
        self.assertEqual(records[1].phone, "03-1234-5678")
        self.assertEqual(records[2].phone, "+81399998888")

    def test_parse_places_phone_none_when_missing(self):
        payload = [
            _place_entry("0x2:0x1", "株式会社A", "地址: 大阪市"),
            _place_entry("0x2:0x2", "株式会社B", "网站: example.co.jp"),
            _place_entry("0x2:0x3", "株式会社C", "营业中"),
        ]
        records = parse_places(payload, source="google_maps")
        self.assertEqual(len(records), 3)
        self.assertTrue(all(record.phone is None for record in records))

    def test_extract_phone_from_preview_text(self):
        text = ")]}'\n[null, [\"電話: +81 6-6341-5340\"], [\"tel:+81663415340\"]]"
        phone = extract_phone_from_preview_text(text)
        self.assertIn(phone, {"+81 6-6341-5340", "+81663415340"})

    def test_extract_phone_from_preview_structured_block(self):
        text = (
            ")]}'\n"
            "[null, [\"other\"], null, [[\"+81 45-443-9424\", [[\"045-443-9424\", 1], [\"+81 45-443-9424\", 2]], null, \"+81454439424\"]]]"
        )
        phone = extract_phone_from_preview_text(text)
        self.assertIn(phone, {"+81 45-443-9424", "045-443-9424"})

    def test_extract_phone_from_preview_text_invalid(self):
        self.assertIsNone(extract_phone_from_preview_text("not_json"))


def _place_entry(cid: str, name: str, extra_text: str) -> list:
    details = [None] * 12
    details[10] = cid
    details[11] = name
    details.append(extra_text)
    return [None, details]


if __name__ == "__main__":
    unittest.main()
