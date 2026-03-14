from __future__ import annotations

import sys
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class DnbKoreaGMapTests(unittest.TestCase):
    def test_local_name_score_rejects_blocked_status_phrases(self) -> None:
        from korea_crawler.google_maps.client import _local_name_score

        self.assertLess(
            _local_name_score("휴업/폐업, 존재하지 않음 또는 중복으로 표시, 법적 문제 신고"),
            0,
        )

    def test_local_name_score_rejects_long_region_description(self) -> None:
        from korea_crawler.google_maps.client import _local_name_score

        self.assertLess(
            _local_name_score("문경시는 대한민국 경상북도 북서부에 있는 시이다. 과거 문경군과 점촌시가 합쳐진 도농복합시로, 문경시의 시내동구역은 과거 점촌시의 행정구역이다."),
            0,
        )

    def test_local_name_score_rejects_review_prompt_phrase(self) -> None:
        from korea_crawler.google_maps.client import _local_name_score

        self.assertLess(
            _local_name_score("다른 사용자에게 도움이 될 후기를 공유해 주세요."),
            0,
        )

    def test_local_name_score_rejects_claim_ownership_phrase(self) -> None:
        from korea_crawler.google_maps.client import _local_name_score

        self.assertLess(
            _local_name_score("비즈니스에 대한 소유권 주장"),
            0,
        )

    def test_local_name_score_rejects_edit_details_phrase(self) -> None:
        from korea_crawler.google_maps.client import _local_name_score

        self.assertLess(
            _local_name_score("이름 또는 기타 세부정보 변경"),
            0,
        )

    def test_local_name_score_rejects_short_disabled_phrase(self) -> None:
        from korea_crawler.google_maps.client import _local_name_score

        self.assertLess(
            _local_name_score("현재 게시가 사용 중지됨"),
            0,
        )

    def test_local_name_score_rejects_short_edit_phrase(self) -> None:
        from korea_crawler.google_maps.client import _local_name_score

        self.assertLess(
            _local_name_score("이름, 위치, 영업시간 등 수정"),
            0,
        )

    def test_local_name_score_rejects_marketing_sentence(self) -> None:
        from korea_crawler.google_maps.client import _local_name_score

        self.assertLess(
            _local_name_score("주식회사 로아스는 산업용 로봇 솔루션 및 ROS기반 연구개발용 로봇 솔루션, 서비스 로봇 솔루션을 산업 전반에 제공하는 로봇 전문 기업입니다."),
            0,
        )

    def test_local_name_score_rejects_facility_description(self) -> None:
        from korea_crawler.google_maps.client import _local_name_score

        self.assertLess(
            _local_name_score("포천천연가스발전소는 경기도 포천시 신북면 계류리에 위치한 복합화력발전소로, 포천민자발전에서 운영하고 있다."),
            0,
        )

    def test_extract_place_candidates_parses_korean_name(self) -> None:
        from korea_crawler.google_maps.client import _extract_place_candidates

        payload = [[
            "Samsung C&T Corporation",
            [[
                None, None, None, None, None, None, None, None,
                "UcqsadaVMOWohbIP-96QiQY",
                "0ahUKEwiWgpqRjY-TAxVlVEEAHXsvJGEQmBkIAigA",
                None, None, None, None,
                [
                    "UcqsadaVMOWohbIP-96QiQY",
                    "0ahUKEwiWgpqRjY-TAxVlVEEAHXsvJGEQ8BcIAygA",
                    ["26 Sangil-ro 6-gil", "Gangdong-gu", "Seoul 05288"],
                    None,
                    [None, None, None, None, None, None, None, 4.5],
                    None,
                    None,
                    ["http://www.samsungcnt.com/", "samsungcnt.com", None, None],
                    None,
                    [None, None, 37.5501, 127.1456],
                    "0x0:0x0",
                    "삼성물산",
                    None,
                    ["건설회사"],
                    "서울",
                    None,
                    None,
                    None,
                    "삼성물산, 26 Sangil-ro 6-gil, Gangdong-gu, Seoul 05288",
                ],
            ]],
        ]]

        candidates = _extract_place_candidates(payload, "Samsung C&T Corporation")

        self.assertTrue(candidates)
        self.assertEqual("삼성물산", candidates[0]["company_name_local"])
        self.assertEqual("https://www.samsungcnt.com", candidates[0]["website"])

    def test_select_best_candidate_rejects_non_corporate_korean_noise_even_with_website(self) -> None:
        from korea_crawler.google_maps.client import _pick_best_candidate

        candidates = [
            {
                "name": "Sujeong District",
                "company_name_local": "수정구",
                "phone": "",
                "website": "https://www.smartand.com",
            },
            {
                "name": "World Vision Hong Kong",
                "company_name_local": "월드비전 홍콩",
                "phone": "",
                "website": "http://www.worldvision.org.hk",
            },
        ]

        picked = _pick_best_candidate(candidates, "Sujeong Industrial Development Co., Ltd.")

        self.assertIsNone(picked)

    def test_extract_candidate_local_name_prefers_repeated_formal_name(self) -> None:
        from korea_crawler.google_maps.client import _extract_candidate_local_name

        node = [
            "L Hardware and Services",
            "주식회사 엘하드웨어앤서비스",
            "정보서비스",
            "주식회사 엘하드웨어앤서비스 (사업주)",
            "44 주식회사 엘하드웨어앤서비스",
            "사진",
            "주식회사 엘하드웨어앤서비스",
        ]

        picked = _extract_candidate_local_name(node)

        self.assertEqual("주식회사 엘하드웨어앤서비스", picked)


if __name__ == "__main__":
    unittest.main()
