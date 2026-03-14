from __future__ import annotations

from thailand_crawler.gmap import build_gmap_query
from thailand_crawler.gmap import build_gmap_queries
from thailand_crawler.gmap import clean_homepage
from thailand_crawler.gmap import _extract_place_candidates
from thailand_crawler.gmap import _select_best_candidate
from thailand_crawler.models import CompanyRecord


def test_clean_homepage_filters_invalid_hosts() -> None:
    assert clean_homepage("mailto:test@example.com") == ""
    assert clean_homepage("https://facebook.com/acme") == ""
    assert clean_homepage("https://en.wikipedia.org/wiki/Saphan_Sung_district") == ""
    assert clean_homepage("https://www.kkmuni.go.th") == ""
    assert clean_homepage("https://booking.com/hotel/th/example") == ""
    assert clean_homepage("https://sutheethaiconstruction.wordpress.com") == ""
    assert clean_homepage("https://Traveloka.com") == ""
    assert clean_homepage("https://Trip.com") == ""
    assert clean_homepage("https://kr.bluepillow.com") == ""
    assert clean_homepage("https://LateRooms.com") == ""
    assert clean_homepage("https://fb.me/demo") == ""
    assert clean_homepage("https://centarahotelsresorts.com") == ""
    assert clean_homepage("https://expedia.com") == ""
    assert clean_homepage("https://agoda.com") == ""
    assert clean_homepage("https://bit.ly/demo") == ""
    assert clean_homepage("www.example.com") == "https://www.example.com"


def test_build_gmap_query_uses_company_and_location() -> None:
    record = CompanyRecord(
        duns="1",
        company_name="ACME CONSTRUCTION COMPANY LIMITED",
        city="Huai Khwang",
        region="Bangkok",
        country="Thailand",
    )

    assert build_gmap_query(record) == "ACME CONSTRUCTION Huai Khwang Bangkok Thailand"
    assert build_gmap_queries(record) == [
        "ACME CONSTRUCTION Huai Khwang Bangkok Thailand",
        "ACME CONSTRUCTION COMPANY LIMITED Huai Khwang Bangkok Thailand",
    ]


def test_extract_place_candidates_parses_nested_google_result() -> None:
    payload = [[
        "SEAFCO PUBLIC COMPANY LIMITED",
        [[
            None, None, None, None, None, None, None, None,
            "UcqsadaVMOWohbIP-96QiQY",
            "0ahUKEwiWgpqRjY-TAxVlVEEAHXsvJGEQmBkIAigA",
            None, None, None, None,
            [
                "UcqsadaVMOWohbIP-96QiQY",
                "0ahUKEwiWgpqRjY-TAxVlVEEAHXsvJGEQ8BcIAygA",
                ["144 Phraya Suren Rd", "Bang Chan, Khlong Sam Wa", "Bangkok 10510"],
                None,
                [None, None, None, None, None, None, None, 4.5],
                None,
                None,
                ["http://www.seafco.co.th/", "seafco.co.th", None, None],
                None,
                [None, None, 13.8197836, 100.6995054],
                "0x311d63786e9af629:0xd01054f27aaf89d8",
                "บริษัท ซีฟโก้ จำกัด (มหาชน) สำนักงานใหญ่",
                None,
                ["Construction company"],
                "Bang Chan",
                None,
                None,
                None,
                "บริษัท ซีฟโก้ จำกัด (มหาชน) สำนักงานใหญ่, 144 Phraya Suren Rd, Bang Chan, Khlong Sam Wa, Bangkok 10510",
            ],
        ]],
    ]]

    candidates = _extract_place_candidates(payload, "SEAFCO PUBLIC COMPANY LIMITED")

    assert candidates
    assert candidates[0]["website"] == "https://www.seafco.co.th"


def test_select_best_candidate_rejects_irrelevant_websites() -> None:
    candidates = [
        {"name": "Saphan Sung district", "website": "https://en.wikipedia.org/wiki/Saphan_Sung_district"},
        {"name": "Mistine", "website": "https://www.mistine.co.th"},
    ]

    picked = _select_best_candidate(candidates, "A C M E ENGINEERING COMPANY LIMITED")

    assert picked is None


def test_select_best_candidate_accepts_domain_match_for_localized_name() -> None:
    candidates = [
        {"name": "บริษัท ซีฟโก้ จำกัด (มหาชน) สำนักงานใหญ่", "website": "https://www.seafco.co.th"},
    ]

    picked = _select_best_candidate(candidates, "SEAFCO PUBLIC COMPANY LIMITED")

    assert picked is not None
    assert picked["website"] == "https://www.seafco.co.th"


def test_extract_candidate_phone_rejects_dense_garbage_numbers() -> None:
    from thailand_crawler.gmap import _extract_candidate_phone

    node = [
        '00090503746',
        '13300815157841731563',
        '0ahUKEwjw-dWlppGTAxX0WkEAHcX0OSAQ4cwMCCMoFw',
    ]

    assert _extract_candidate_phone(node) == ''


def test_extract_candidate_phone_prefers_formatted_thai_phone() -> None:
    from thailand_crawler.gmap import _extract_candidate_phone

    node = [
        '02 919 0090',
        '+66 2 919 0090',
        '029190090',
        'tel:029190090',
    ]

    assert _extract_candidate_phone(node) == '+66 2 919 0090'


def test_extract_candidate_thai_name_requires_corporate_shape() -> None:
    from thailand_crawler.gmap import _extract_candidate_thai_name

    assert _extract_candidate_thai_name(['แฟลต A5']) == ''
    assert _extract_candidate_thai_name(['ชุติกานต์']) == ''
    assert _extract_candidate_thai_name(['พระรามเก้า']) == ''
    assert _extract_candidate_thai_name(['บริษัท ซีฟโก้ จำกัด (มหาชน) สำนักงานใหญ่']) == 'บริษัท ซีฟโก้ จำกัด (มหาชน) สำนักงานใหญ่'
    assert _extract_candidate_thai_name(['บจก.อุณากรรณ']) == 'บจก.อุณากรรณ'


def test_select_best_candidate_rejects_non_corporate_thai_noise_even_with_website() -> None:
    candidates = [
        {"name": 'Huai Khwang', "company_name_th": 'ห้วยขวาง Huai Khwang', "phone": '+66 2 692 7534', "website": 'https://www.wayutech.com'},
        {"name": 'Rajthanee Home', "company_name_th": 'ภัตราคารหูฉลาม ไชน่าทาวน์สกาล่า 2', "phone": '+66 2 221 2121', "website": 'https://www.shanghaimansion.com'},
    ]

    picked = _select_best_candidate(candidates, 'BRIDGE GROUP (THAILAND) COMPANY LIMITED')

    assert picked is None
