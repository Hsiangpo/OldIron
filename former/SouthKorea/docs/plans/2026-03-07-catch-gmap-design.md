# Catch GMap Enrichment Design

**Goal:** 对 `catch` 已完成详情解析但无官网的公司，新增 `GMap` 官网补齐层，并只对新补到官网的记录续跑 `Snov`。

**Architecture:** 保持原 `列表 -> 详情 -> Snov` 不变，在详情和 `Snov` 之间新增可选 `GMap` 阶段。`GMap` 只处理 `homepage=''` 且存在 `company_name+ceo` 的记录，输出 `companies_enriched.jsonl`。随后刷新 `Snov` 状态，只让新补到官网的 `comp_id` 重新进入邮箱查询，最后同步到 `companies_with_emails.jsonl` 供现有 `product.py` 使用。

**Constraints:** `catch` 规模很大，`GMap` 必须可断点续跑、可单独启动；不能重跑已经写入 `companies_with_emails.jsonl` 的老记录；需要沿用现有 `GoogleMapsClient` 和 `Snov` 断点规则。
