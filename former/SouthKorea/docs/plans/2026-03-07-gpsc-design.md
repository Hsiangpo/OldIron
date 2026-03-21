# GPSC List + GMap + Snov Design

**Goal:** 从 `https://gpsc.or.kr/company/` 抓取 `公司名 + CEO`，再通过 `GMap` 补官网，最后用 `Snov` 查邮箱并接入现有交付。

**Architecture:** 使用三阶段 pc。Phase 1 解析公开表格源码直接生成 `companies.jsonl`，保留 `地址/电话/业种` 作为官网补全线索；Phase 2 复用现有 `GoogleMapsClient`，以 `公司名 + 地址` 为主查询官网，输出 `companies_enriched.jsonl`；Phase 3 复用现有 `Snov` 管道查邮箱，并同步到标准 `companies_with_emails.jsonl` 供 `product.py` 使用。

**Constraints:** 该站公开页没有公司官网、没有邮箱、没有详情链接，只有 `172` 条表格行；分页为前端表格分页，源码一次性包含全部行。
