"""错误类型模块的单元测试。"""
import unittest

from site_agent.errors import (
    SiteAgentError,
    NetworkError,
    CrawlError,
    LLMError,
    ParseError,
    SnovError,
    SnovMaskedEmailError,
    ValidationError,
    ConfigError,
)


class TestErrorHierarchy(unittest.TestCase):
    """测试错误类型层次结构。"""

    def test_all_inherit_from_base(self):
        """所有错误类型应继承自 SiteAgentError。"""
        error_types = [
            NetworkError,
            CrawlError,
            LLMError,
            ParseError,
            SnovError,
            SnovMaskedEmailError,
            ValidationError,
            ConfigError,
        ]
        for error_type in error_types:
            with self.subTest(error_type=error_type.__name__):
                self.assertTrue(issubclass(error_type, SiteAgentError))

    def test_snov_errors_inherit_from_snov(self):
        """Snov 相关错误应继承自 SnovError。"""
        self.assertTrue(issubclass(SnovMaskedEmailError, SnovError))


class TestSiteAgentError(unittest.TestCase):
    """测试基础错误类。"""

    def test_basic_message(self):
        """应正确设置消息。"""
        error = SiteAgentError("Test error")
        self.assertEqual(error.message, "Test error")
        self.assertEqual(str(error), "Test error")

    def test_with_website(self):
        """带网站参数时应包含在字符串表示中。"""
        error = SiteAgentError("Test error", website="example.com")
        self.assertEqual(error.website, "example.com")
        self.assertIn("example.com", str(error))

    def test_with_details(self):
        """应正确存储详情字典。"""
        error = SiteAgentError("Test error", details={"key": "value"})
        self.assertEqual(error.details, {"key": "value"})


class TestCrawlError(unittest.TestCase):
    """测试抓取错误类。"""

    def test_with_status_code(self):
        """应正确存储状态码。"""
        error = CrawlError("Page not found", status_code=404, url="https://example.com/page")
        self.assertEqual(error.status_code, 404)
        self.assertEqual(error.url, "https://example.com/page")


class TestLLMError(unittest.TestCase):
    """测试 LLM 错误类。"""

    def test_retryable_flag(self):
        """应正确存储可重试标志。"""
        error1 = LLMError("Rate limited", is_retryable=True)
        self.assertTrue(error1.is_retryable)
        
        error2 = LLMError("Invalid API key", is_retryable=False)
        self.assertFalse(error2.is_retryable)

    def test_label(self):
        """应正确存储调用标签。"""
        error = LLMError("Timeout", label="抽取")
        self.assertEqual(error.label, "抽取")


class TestExceptionCatching(unittest.TestCase):
    """测试异常捕获能力。"""

    def test_catch_specific_then_general(self):
        """应能先捕获具体异常，再捕获通用异常。"""
        def raise_snov_masked():
            raise SnovMaskedEmailError("Masked email detected")

        try:
            raise_snov_masked()
        except SnovMaskedEmailError as e:
            self.assertIsInstance(e, SnovError)
            self.assertIsInstance(e, SiteAgentError)
        except SiteAgentError:
            self.fail("Should have caught SnovMaskedEmailError first")

    def test_catch_all_site_agent_errors(self):
        """应能用 SiteAgentError 捕获所有子类异常。"""
        errors = [
            NetworkError("Network failed"),
            CrawlError("Crawl failed"),
            LLMError("LLM failed"),
            ParseError("Parse failed"),
            SnovError("Snov failed"),
        ]
        for error in errors:
            with self.subTest(error_type=type(error).__name__):
                try:
                    raise error
                except SiteAgentError:
                    pass  # 应该被捕获
                except Exception:
                    self.fail(f"Should have caught {type(error).__name__} as SiteAgentError")


if __name__ == "__main__":
    unittest.main(verbosity=2)
