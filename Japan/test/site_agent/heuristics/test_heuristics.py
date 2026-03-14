"""规则抽取模块的单元测试。"""
import unittest

from site_agent.heuristics import clean_company_value


class TestCleanCompanyValue(unittest.TestCase):
    """测试公司名称清理函数。"""

    def test_normalizes_whitespace(self):
        """应规范化空格。"""
        result = clean_company_value("株式会社　　テスト")
        self.assertIsNotNone(result)
        self.assertNotIn("　　", result)

    def test_rejects_too_short(self):
        """应拒绝过短的值。"""
        self.assertIsNone(clean_company_value("A"))

    def test_rejects_too_long(self):
        """应拒绝过长的值。"""
        long_value = "a" * 200
        self.assertIsNone(clean_company_value(long_value))

    def test_accepts_valid_company(self):
        """应接受有效的公司名称。"""
        result = clean_company_value("株式会社テスト")
        self.assertEqual(result, "株式会社テスト")


if __name__ == "__main__":
    unittest.main(verbosity=2)
