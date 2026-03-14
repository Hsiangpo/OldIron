# GPSC Crawler Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 新增 `GPSC` 三阶段站点爬虫并全量跑完后重打 `day2`。

**Architecture:** 新增 `src/korea_crawler/sites/gpsc.py`，列表解析直接抓公开表格；中间态复用 `companies_enriched.jsonl`；最后走 `Snov` 和现有去重/交付。

**Tech Stack:** `curl_cffi`, `lxml`, `GoogleMapsClient`, `run_snov_pipeline`, `product.py`。

---

### Task 1: 写失败解析测试
- 文件: `tmp/analysis/test_gpsc_parser.py`
- 校验总行数、首行字段、末行字段。

### Task 2: 实现 `GPSC` 站点
- 文件: `src/korea_crawler/sites/gpsc.py`
- 内容: 客户端、表格解析、GMap 官网补全、Snov 联动。

### Task 3: 接入 CLI
- 文件: `run.py`
- 内容: 新增 `gpsc` 分发与帮助文案。

### Task 4: 验证与跑数
- 命令: 解析测试、`python run.py gpsc --skip-snov` 冒烟、`python run.py gpsc` 全量、`python product.py day2`。
