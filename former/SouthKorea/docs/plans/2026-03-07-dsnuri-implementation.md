# DSNURI Crawler Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 新增 `DSNURI` 四阶段站点爬虫并接入 `day2` 交付。

**Architecture:** 单文件站点入口 `src/korea_crawler/sites/dsnuri.py`，内含 pc 客户端、列表解析、详情补齐、`Google Maps` 官网补齐与 `Snov` 管道调用；CLI 通过 `run.py` 暴露 `dsnuri` 命令。

**Tech Stack:** `curl_cffi`, `lxml`, 现有 `CompanyRecord`, `GoogleMapsClient`, `run_snov_pipeline`, `deduplicate_by_domain`.

---

### Task 1: 准备红测
- 文件: `tmp/analysis/test_dsnuri_parser.py`
- 步骤: 用已保存的列表/详情样本验证分页总数、首行公司、详情字段解析。

### Task 2: 实现站点入口
- 文件: `src/korea_crawler/sites/dsnuri.py`
- 步骤: 完成列表、详情、gmap、snov 四阶段与断点续跑。

### Task 3: 接入 CLI
- 文件: `run.py`
- 步骤: 注册 `dsnuri` 站点及帮助文本。

### Task 4: 验证与交付
- 文件: `tmp/analysis/test_dsnuri_parser.py`
- 步骤: 跑解析测试，再做 `--max-pages 1 --skip-gmap --skip-snov` 冒烟，最后全量跑并重打 `day2`。
