# Saramin Dart Incheon GMap Enrichment Design

**Goal:** 为 `saramin`、`dart`、`incheon` 新增“无官网 -> GMap 补官网 -> 增量 Snov”链路，并在现有产出上直接补量。

**Architecture:** 复用 `GoogleMapsClient` 和 `run_snov_pipeline`，抽出通用 `GMap` 增量模块。每个站点只负责把 `companies.jsonl` 交给通用模块，并按站点需要定义日志标签与阶段顺序。`Snov` 只消费 `GMap` 新补到官网的队列，不重跑已有邮箱记录。

**Constraints:** `saramin`、`dart`、`incheon` 当前都已存在完整 `companies.jsonl`，因此本次不需要重跑列表或详情；重点是补官网、增量邮箱和刷新 `product.py day2`。
