# KSSBA List Crawler Design

**Goal:** 从 `bo_table=21` 公开列表抓取 `公司名 + CEO + 官网`，再走 `Snov` 获取邮箱并接入现有交付。

**Architecture:** 采用两阶段 pc：Phase 1 列表分页直接解析并写入 `companies.jsonl`；Phase 2 复用现有 `Snov` 管道写入 `companies_with_emails.jsonl`，最后走现有域名去重与 `product.py`。

**Key Constraints:** 详情页需要登录，不能作为主数据源；列表页公开字段完整；请求必须使用 `curl_cffi` 浏览器指纹。
