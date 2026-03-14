# Firecrawl 邮箱默认路线 Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 将 England 默认邮箱阶段从 Snov 切换到 Firecrawl + 外部 LLM，并保持 `emails` 下游出口不变。

**Architecture:** 新增通用 `england_crawler.firecrawl` 模块，复用 sibling 项目的 Firecrawl key pool、同步 client 和 LLM 范式。`companies_house` 与 `dnb` 分别把第三阶段替换为 `firecrawl_queue/firecrawl_status`，统一通过 `FirecrawlEmailService` 完成域名级邮箱提取。

**Tech Stack:** Python 3.11, sqlite, curl_cffi/requests, OpenAI Python SDK, unittest

---

### Task 1: 补齐环境变量与依赖

**Files:**
- Modify: `England/.env`
- Modify: `England/.env.example`
- Modify: `England/requirements.txt`

**Step 1: 补 Firecrawl / LLM 环境变量**

- 从 sibling 项目复用 `FIRECRAWL_*` 与 `LLM_*` 口径
- 保留现有 `SNOV_*`，但 England 默认路线不再依赖它们

**Step 2: 补依赖**

- 增加 `openai` 依赖

**Step 3: 验证配置文件格式**

Run: `python -c "from pathlib import Path; print(Path('England/.env.example').read_text(encoding='utf-8'))"`

Expected: 新变量存在且编码正常

### Task 2: 新增 Firecrawl 基础模块

**Files:**
- Create: `England/src/england_crawler/firecrawl/__init__.py`
- Create: `England/src/england_crawler/firecrawl/key_pool.py`
- Create: `England/src/england_crawler/firecrawl/client.py`
- Create: `England/src/england_crawler/firecrawl/domain_cache.py`
- Create: `England/src/england_crawler/firecrawl/llm_client.py`
- Create: `England/src/england_crawler/firecrawl/email_service.py`
- Test: `England/tests/test_firecrawl_key_pool.py`
- Test: `England/tests/test_firecrawl_client.py`
- Test: `England/tests/test_firecrawl_email_service.py`

**Step 1: 先写失败测试**

- 覆盖 key pool 获取/冷却/禁用
- 覆盖 client 对 `401/402/429/5xx/request_failed`
- 覆盖 email service 的候选页选择、结果合并、无邮箱返回

**Step 2: 实现最小代码**

- key pool 采用同步 sqlite 模式
- client 至少支持 `map_urls()` 与 `scrape_json()`
- llm client 支持 `pick_urls()` 与 `merge_email_results()`
- email service 封装完整流程

**Step 3: 运行局部测试**

Run: `python -m unittest England.tests.test_firecrawl_key_pool England.tests.test_firecrawl_client England.tests.test_firecrawl_email_service`

Expected: PASS

### Task 3: 替换 Companies House 默认邮箱阶段

**Files:**
- Modify: `England/src/england_crawler/companies_house/store.py`
- Modify: `England/src/england_crawler/companies_house/pipeline.py`
- Modify: `England/src/england_crawler/companies_house/config.py`
- Modify: `England/src/england_crawler/companies_house/cli.py`
- Test: `England/tests/test_companies_house_store.py`
- Test: `England/tests/test_companies_house_pipeline.py`
- Test: `England/tests/test_companies_house_cli.py`

**Step 1: 先写失败测试**

- 断言第三阶段变为 `firecrawl_queue`
- 断言 stats / 进度 / 完成判定看 `firecrawl`
- 断言校验依赖改为 `FIRECRAWL_* / LLM_*`

**Step 2: 实现最小替换**

- store 增加 `firecrawl_status/firecrawl_queue`
- pipeline 改成 `FirecrawlEmailService`
- CLI 增加 `--skip-firecrawl/--firecrawl-workers`
- 兼容旧 `--skip-snov/--snov-workers` 作为别名

**Step 3: 运行 CH 测试**

Run: `python -m unittest England.tests.test_companies_house_store England.tests.test_companies_house_pipeline England.tests.test_companies_house_cli`

Expected: PASS

### Task 4: 替换 DNB 默认邮箱阶段

**Files:**
- Modify: `England/src/england_crawler/dnb/store.py`
- Modify: `England/src/england_crawler/dnb/pipeline.py`
- Modify: `England/src/england_crawler/dnb/config.py`
- Modify: `England/src/england_crawler/dnb/cli.py`
- Test: `England/tests/test_dnbkorea_store.py`
- Test: `England/tests/test_dnbkorea_pipeline.py`
- Test: `England/tests/test_dnbkorea_cli.py`

**Step 1: 先写失败测试**

- 断言第三阶段改为 `firecrawl_queue`
- 断言默认 worker / stats / 完成判定看 `firecrawl`

**Step 2: 实现最小替换**

- store 增加 `firecrawl_status/firecrawl_queue`
- pipeline 默认不再创建 `SnovClient`
- 使用 `FirecrawlEmailService`

**Step 3: 运行 DNB 测试**

Run: `python -m unittest England.tests.test_dnbkorea_store England.tests.test_dnbkorea_pipeline England.tests.test_dnbkorea_cli`

Expected: PASS

### Task 5: 回归验证与清理

**Files:**
- Modify: `England/run.py`（仅在需要时）
- Verify: `England/tests/*`

**Step 1: 运行全量单测**

Run: `python -m unittest discover England/tests`

Expected: PASS

**Step 2: 运行诊断**

Run: `python -m compileall England/src`

Expected: PASS

**Step 3: 人工检查**

- 日志口径全部改成 `Firecrawl`
- 下游 `delivery.py` 无需改即可消费 `emails`
- `.env.example` 完整

**Step 4: 架构复核**

- 让 architect agent 审核最终实现与错误处理策略
