# 关键技术方案

- Cloudflare 防护：优先使用 cookies；可通过 `browser_cookie3` 或 CDP 导出后复用
- CDP 导出：已通过验证的 9222 浏览器可用 `scripts/export_cookies_cdp.py` 直接导出
- 断点续跑：使用 SQLite 记录已完成页与公司（`checkpoint.sqlite3`）
- 大规模输出：JSONL/CSV 边爬边写，避免内存堆积
