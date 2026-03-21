"""缓存模块的单元测试。"""
import unittest
import tempfile
import shutil
from pathlib import Path

from site_agent.cache import DomainEmailCache


class TestDomainEmailCache(unittest.TestCase):
    """测试域名邮箱缓存。"""

    def setUp(self):
        """创建临时目录用于测试。"""
        self.temp_dir = tempfile.mkdtemp()
        self.cache_path = Path(self.temp_dir) / "test_cache.jsonl"
        self.cache = DomainEmailCache(self.cache_path)

    def tearDown(self):
        """清理临时目录。"""
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_store_and_get(self):
        """存储后应能正确获取。"""
        self.cache.store("example.com", ["test@example.com", "info@example.com"])
        result = self.cache.get("example.com")
        self.assertIsNotNone(result)
        self.assertEqual(len(result), 2)
        self.assertIn("test@example.com", result)

    def test_get_nonexistent(self):
        """获取不存在的域名应返回 None。"""
        result = self.cache.get("notexist.com")
        self.assertIsNone(result)

    def test_case_insensitive(self):
        """域名查询应不区分大小写。"""
        self.cache.store("Example.COM", ["test@example.com"])
        result = self.cache.get("example.com")
        self.assertIsNotNone(result)
        result2 = self.cache.get("EXAMPLE.COM")
        self.assertIsNotNone(result2)

    def test_no_duplicate_store(self):
        """重复存储同一域名不应覆盖。"""
        self.cache.store("example.com", ["first@example.com"])
        self.cache.store("example.com", ["second@example.com"])
        result = self.cache.get("example.com")
        self.assertEqual(result, ["first@example.com"])

    def test_empty_emails_not_stored(self):
        """空邮箱列表不应被存储。"""
        self.cache.store("example.com", [])
        result = self.cache.get("example.com")
        self.assertIsNone(result)

    def test_invalid_domain_not_stored(self):
        """无效域名不应被存储。"""
        self.cache.store("", ["test@example.com"])
        self.cache.store("   ", ["test@example.com"])
        # 不应抛出异常

    def test_persistence(self):
        """缓存应持久化到文件。"""
        self.cache.store("example.com", ["test@example.com"])
        
        # 创建新的缓存实例，从同一文件加载
        cache2 = DomainEmailCache(self.cache_path)
        result = cache2.get("example.com")
        self.assertIsNotNone(result)
        self.assertIn("test@example.com", result)

    def test_contains(self):
        """测试 __contains__ 方法。"""
        self.cache.store("example.com", ["test@example.com"])
        self.assertIn("example.com", self.cache)
        self.assertNotIn("notexist.com", self.cache)


if __name__ == "__main__":
    unittest.main(verbosity=2)
