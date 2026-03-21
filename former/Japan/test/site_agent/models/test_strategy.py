"""最小单测/模拟测试：验证策略配置的正确性。

测试目标：
1. 全量跑策略：允许 LLM 选链，禁用关键词过滤
2. 代表人续跑策略：允许 LLM 选链与关键词过滤
"""

import unittest

from site_agent.config import get_strategy_for_mode


class TestRunStrategy(unittest.TestCase):
    """测试 RunStrategy 数据类和 get_strategy_for_mode 函数。"""

    def test_full_mode_strategy(self):
        """全量跑策略：允许 LLM 选链，禁用关键词过滤。"""
        strategy = get_strategy_for_mode(
            None, settings_max_rounds=2, settings_max_pages=4
        )

        self.assertEqual(strategy.mode, "full")
        self.assertTrue(strategy.allow_llm_link_select, "全量跑应允许 LLM 选链")
        self.assertFalse(
            strategy.allow_llm_keyword_filter, "全量跑不应允许 LLM 关键词过滤"
        )
        self.assertTrue(strategy.allow_snov_prefetch, "全量跑应允许 Snov 预取")
        self.assertFalse(
            strategy.allow_pdf_extract, "全量跑禁止 PDF 解析（当前无实际使用）"
        )
        self.assertEqual(strategy.max_rounds, 2, "全量跑最大轮次应为 2")

    def test_full_mode_with_explicit_full_string(self):
        """显式传入 'full' 字符串应得到全量跑策略。"""
        strategy = get_strategy_for_mode(
            "full", settings_max_rounds=5, settings_max_pages=8
        )

        self.assertEqual(strategy.mode, "full")
        self.assertTrue(strategy.allow_llm_link_select)
        # 全量跑尊重 settings_max_rounds
        self.assertEqual(strategy.max_rounds, 5)

    def test_representative_mode_strategy(self):
        """代表人续跑策略：允许 LLM 选链与关键词过滤。"""
        strategy = get_strategy_for_mode(
            "representative", settings_max_rounds=2, settings_max_pages=4
        )

        self.assertEqual(strategy.mode, "representative")
        self.assertTrue(strategy.allow_llm_link_select, "代表人续跑应允许 LLM 选链")
        self.assertTrue(
            strategy.allow_llm_keyword_filter, "代表人续跑应允许 LLM 关键词过滤"
        )
        self.assertTrue(strategy.allow_snov_prefetch, "代表人续跑应允许 Snov 预取")
        self.assertFalse(
            strategy.allow_pdf_extract, "代表人续跑禁止 PDF 解析（当前无实际使用）"
        )
        # 代表人续跑尊重 settings_max_rounds
        self.assertEqual(strategy.max_rounds, 2)

    def test_representative_mode_case_insensitive(self):
        """resume_mode 应该不区分大小写。"""
        strategy1 = get_strategy_for_mode("REPRESENTATIVE")
        strategy2 = get_strategy_for_mode("Representative")
        strategy3 = get_strategy_for_mode("  representative  ")

        for s in [strategy1, strategy2, strategy3]:
            self.assertEqual(s.mode, "representative")
            self.assertTrue(s.allow_llm_link_select)

    def test_unknown_mode_defaults_to_full(self):
        """未知模式应默认为全量跑。"""
        strategy = get_strategy_for_mode("unknown_mode")

        self.assertEqual(strategy.mode, "full")
        self.assertTrue(strategy.allow_llm_link_select)

    def test_empty_string_defaults_to_full(self):
        """空字符串应默认为全量跑。"""
        strategy = get_strategy_for_mode("")

        self.assertEqual(strategy.mode, "full")
        self.assertTrue(strategy.allow_llm_link_select)

    def test_max_pages_passthrough(self):
        """max_pages 应该正确传递。"""
        strategy1 = get_strategy_for_mode("full", settings_max_pages=10)
        strategy2 = get_strategy_for_mode("representative", settings_max_pages=20)

        self.assertEqual(strategy1.max_pages, 10)
        self.assertEqual(strategy2.max_pages, 20)


class TestStrategyBehaviorSimulation(unittest.TestCase):
    """模拟测试策略在流程中的行为表现。"""

    def test_full_mode_llm_behavior(self):
        """全量跑模式下，允许 LLM 选链，禁用关键词过滤。"""
        strategy = get_strategy_for_mode(None)

        # 模拟流程判断
        should_call_llm_select = (
            strategy.allow_llm_link_select and True
        )  # 假设有候选链接
        should_call_keyword_filter = strategy.allow_llm_keyword_filter and True

        self.assertTrue(should_call_llm_select, "全量跑应调用 LLM 选链")
        self.assertFalse(should_call_keyword_filter, "全量跑不应调用 LLM 关键词过滤")

    def test_representative_mode_allows_keyword_filter(self):
        """代表人续跑模式下，策略配置允许关键词过滤。"""
        strategy = get_strategy_for_mode("representative")

        should_call_llm_select = strategy.allow_llm_link_select and True
        should_call_keyword_filter = strategy.allow_llm_keyword_filter and True

        self.assertTrue(should_call_llm_select, "代表人续跑应调用 LLM 选链")
        self.assertTrue(should_call_keyword_filter, "代表人续跑应调用 LLM 关键词过滤")


if __name__ == "__main__":
    # 运行测试
    unittest.main(verbosity=2)
