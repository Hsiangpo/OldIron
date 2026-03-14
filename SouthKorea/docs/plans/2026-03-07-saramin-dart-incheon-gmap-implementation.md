# Saramin Dart Incheon GMap Enrichment Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 为 `saramin`、`dart`、`incheon` 接入通用 `GMap` 官网补齐与增量 `Snov`，并跑完当前积压数据。

**Architecture:** 新增 `src/korea_crawler/google_maps/pipeline.py` 作为通用补官网/增量 `Snov` 模块；`saramin.py`、`dart.py`、`incheon.py` 接入 `--skip-gmap` 等参数和调用点；最后分别启动补跑并统一重打 `day2`。

**Tech Stack:** `curl_cffi`, `GoogleMapsClient`, `run_snov_pipeline`, 现有 JSONL 断点模式。

---

### Task 1: 通用模块测试
- 文件: `tmp/analysis/test_gmap_pipeline_merge.py`
- 校验增量 `Snov` 合并逻辑。

### Task 2: 新增通用 GMap 模块
- 文件: `src/korea_crawler/google_maps/pipeline.py`
- 内容: 补官网、队列、增量 `Snov`、标准文件合并。

### Task 3: 接入三站入口
- 文件: `src/korea_crawler/sites/saramin.py`, `src/korea_crawler/sites/dart.py`, `src/korea_crawler/sites/incheon.py`, `run.py`
- 内容: 参数、阶段调用、日志。

### Task 4: 验证和补跑
- 命令: 测试、编译、三站 `--skip-list --skip-detail` 或等效补跑、重打 `day2`。
