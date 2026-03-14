# KSSBA Crawler Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 为 `bo_table=21` 新增 `KSSBA` 列表协议爬虫，抓取公司名、CEO、官网并接入 `Snov` 与交付。

**Architecture:** 新增 `src/korea_crawler/sites/kssba.py`，内置协议客户端与列表解析；Phase 1 直接从公开列表写入 `companies.jsonl`；Phase 2 复用 `run_snov_pipeline` 生成 `companies_with_emails.jsonl` 并做域名去重。

**Tech Stack:** `curl_cffi`, `lxml`, 现有 `CompanyRecord`, `Snov` pipeline, 现有 `product.py`。

---

### Task 1: 解析页样本
- 文件: `tmp/analysis/test_kssba_parser.py`
- 步骤: 先让解析测试失败，再实现最小解析逻辑。

### Task 2: 新增站点入口
- 文件: `src/korea_crawler/sites/kssba.py`
- 步骤: 实现客户端、列表分页、断点续跑、Snov、去重。

### Task 3: 接入 CLI
- 文件: `run.py`
- 步骤: 注册 `kssba` 站点和帮助文案。

### Task 4: 验证
- 文件: `tmp/analysis/test_kssba_parser.py`
- 步骤: 运行解析测试，再跑 `python run.py kssba --max-pages 1 --skip-snov` 冒烟。
