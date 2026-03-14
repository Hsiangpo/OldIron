# Catch GMap Enrichment Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 为 `catch` 新增 `GMap` 官网补齐与增量 `Snov` 续跑链路，并启动后台补跑。

**Architecture:** 新增 `src/korea_crawler/sites/catch_gmap.py` 负责 `companies.jsonl -> companies_enriched.jsonl -> checkpoint_gmap.json`；`src/korea_crawler/sites/catch.py` 接入 `--skip-gmap`、`--gmap-concurrency` 等参数；补一个 `catch` 专用的 `Snov` 状态刷新函数，只回收“新补到官网”的 `comp_id`。

**Tech Stack:** `curl_cffi`, `GoogleMapsClient`, `run_snov_pipeline`, 现有 `dedup`、现有 `product.py`。

---

### Task 1: 写失败测试
- 文件: `tmp/analysis/test_catch_gmap_refresh.py`
- 验证 `Snov` 状态刷新只移除官网新增的记录。

### Task 2: 实现 GMap 补官网模块
- 文件: `src/korea_crawler/sites/catch_gmap.py`
- 内容: 读 `companies.jsonl`，筛选无官网但有公司名+CEO 的记录，补官网并写断点。

### Task 3: 接入 catch 入口
- 文件: `src/korea_crawler/sites/catch.py`, `run.py`
- 内容: 增加 `GMap` 阶段、参数和同步逻辑。

### Task 4: 验证并启动补跑
- 命令: 失败测试、`py_compile`、小批量冒烟、后台启动全量 `catch` 补官网与增量 `Snov`。
