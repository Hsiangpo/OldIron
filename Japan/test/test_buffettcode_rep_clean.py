# -*- coding: utf-8 -*-
"""buffettcode clean_representative 规则测试（防回归）。"""

from __future__ import annotations

import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

from japan_crawler.sites.buffettcode.parser import clean_representative


class TestWithoutTitleStrip(unittest.TestCase):
    """strip_title=False（DB 落库口径）：只去垃圾前缀/标签，保留职衔。"""

    def test_slash_prefix_removed(self):
        self.assertEqual(clean_representative("/竹村 洋平"), "竹村 洋平")

    def test_dot_prefix_removed(self):
        self.assertEqual(clean_representative("・会長 CEO 前田 秀作"), "会長 CEO 前田 秀作")  # strip_title=False 不去职衔

    def test_pure_label_blanked(self):
        self.assertEqual(clean_representative("設立"), "")

    def test_employee_count_blanked(self):
        self.assertEqual(clean_representative("従業員数"), "")

    def test_empty(self):
        self.assertEqual(clean_representative(None), "")
        self.assertEqual(clean_representative(""), "")

    def test_na_passthrough(self):
        """N/A 归一由上游 _clean/_na 完成，这里不做——传入时已是空或已归一。"""
        # 实流：parse_detail 走 _clean → 空串入；export 走 _na → 空串入
        self.assertEqual(clean_representative(""), "")
        self.assertEqual(clean_representative("N/A"), "N/A")  # 上游归一，不入此路径

    def test_title_preserved_no_strip(self):
        self.assertEqual(
            clean_representative("代表取締役社長 石堂 公彦", strip_title=False),
            "代表取締役社長 石堂 公彦",
        )


class TestWithTitleStrip(unittest.TestCase):
    """strip_title=True（导出 CSV 口径）：全量清洗，只留纯人名。"""

    # ---- 开头职衔剥离 ----
    def test_basic_leading_title(self):
        self.assertEqual(
            clean_representative("代表取締役社長 石堂 公彦", strip_title=True),
            "石堂 公彦",
        )

    def test_leading_representative_only(self):
        self.assertEqual(
            clean_representative("代表取締役 大越 昇", strip_title=True),
            "大越 昇",
        )

    def test_leading_ceo(self):
        self.assertEqual(
            clean_representative("執行役 CEO 大竹 伸明", strip_title=True),
            "大竹 伸明",
        )

    def test_leading_rijicho(self):
        self.assertEqual(
            clean_representative("理事長 瓦林 達比古", strip_title=True),
            "瓦林 達比古",
        )

    def test_leading_daihyo_shain(self):
        self.assertEqual(
            clean_representative("代表社員 松村 洋季", strip_title=True),
            "松村 洋季",
        )

    def test_leading_president_english(self):
        self.assertEqual(
            clean_representative("President 鈴木 太郎", strip_title=True),
            "鈴木 太郎",
        )

    def test_leading_kigyo_ka(self):
        self.assertEqual(
            clean_representative("創業者 田中 一郎", strip_title=True),
            "田中 一郎",
        )

    def test_leading_owner(self):
        self.assertEqual(
            clean_representative("オーナー 山田 花子", strip_title=True),
            "山田 花子",
        )

    # ---- 斜杠/标点前缀 + 职衔组合 ----
    def test_slash_prefix_then_title_then_name(self):
        self.assertEqual(
            clean_representative("/竹村 洋平", strip_title=True),
            "竹村 洋平",
        )

    def test_dot_prefix_with_multiple_leading(self):
        self.assertEqual(
            clean_representative("・所長 服部 峻介", strip_title=True),
            "服部 峻介",
        )

    # ---- 括注剥离 ----
    def test_title_in_parentheses_removed(self):
        self.assertEqual(
            clean_representative("岡澤隆弘(代表取締役社長)", strip_title=True),
            "岡澤隆弘",
        )

    def test_bracket_title_removed(self):
        self.assertEqual(
            clean_representative("雀部優(代表取締役社長)", strip_title=True),
            "雀部優",
        )

    def test_furigana_removed(self):
        self.assertEqual(
            clean_representative("白幡 晶彦(しらはた あきひこ)代表執行役員社長", strip_title=True),
            "白幡 晶彦",
        )

    def test_bracket_title_prefix_removed(self):
        self.assertEqual(
            clean_representative("[代表取締役社長] 田原 康博", strip_title=True),
            "田原 康博",
        )

    # ---- 结尾粘连职衔 ----
    def test_trailing_title_removed(self):
        self.assertEqual(
            clean_representative("北川博康(理事長)", strip_title=True),
            "北川博康",
        )

    # ---- 多段职衔叠加 ----
    def test_cascaded_titles(self):
        self.assertEqual(
            clean_representative("代表取締役会長 七尾 静也", strip_title=True),
            "七尾 静也",
        )

    def test_sousai_title_kept_name(self):
        self.assertEqual(
            clean_representative("代表取締役総裁 田中 一穂", strip_title=True),
            "田中 一穂",
        )

    # ---- 外国人名 / カタカナ名 ----
    def test_katakana_foreign_name_preserved(self):
        res = clean_representative("ジェローム・エティエンヌ・", strip_title=True)
        self.assertTrue(len(res) > 0)
        self.assertIn("ジェローム", res)

    # ---- 不会被误伤的边缘情况 ----
    def test_plain_name_no_title(self):
        self.assertEqual(
            clean_representative("塩原 正也", strip_title=True),
            "塩原 正也",
        )

    def test_plain_japanese_name_two_chars(self):
        self.assertEqual(
            clean_representative("曽根省吾", strip_title=True),
            "曽根省吾",
        )

    def test_plain_name_four_chars_no_space(self):
        self.assertEqual(
            clean_representative("板橋良夫", strip_title=True),
            "板橋良夫",
        )

    # ---- 含标签词的字段标签 ----
    def test_field_label_seturitu(self):
        self.assertEqual(clean_representative("設立", strip_title=True), "")

    def test_field_label_juugyouin(self):
        self.assertEqual(clean_representative("従業員数", strip_title=True), "")

    def test_field_label_denwa(self):
        self.assertEqual(clean_representative("電話番号", strip_title=True), "")

    def test_blank_after_strip(self):
        """纯职衔无名字 → 清空"""
        self.assertEqual(clean_representative("代表取締役", strip_title=True), "")

    # ---- 仅标点 / 空白 ----
    def test_punctuation_only(self):
        self.assertEqual(clean_representative("・", strip_title=True), "")
        self.assertEqual(clean_representative("---", strip_title=True), "")

    # ---- 不被当做职衔的正常词 ----
    def test_name_with_normal_kanji_untouched(self):
        """'代表'在名字中不是职衔时不被误剥"""
        self.assertEqual(
            clean_representative("中島 直人", strip_title=True),
            "中島 直人",
        )


if __name__ == "__main__":
    unittest.main()
