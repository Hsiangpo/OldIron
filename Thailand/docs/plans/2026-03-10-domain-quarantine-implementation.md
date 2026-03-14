# Domain Quarantine Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 阻止共享域名、平台域名、品牌不匹配域名继续污染官网、邮箱和交付结果。

**Architecture:** 在 `GMAP -> store -> Snov -> delivery` 四层增加统一域名信任判定。先做硬拦截，再加共享域名隔离与交付审计，避免后续重复人工清洗。

**Tech Stack:** Python, sqlite3, pytest

---

### Task 1: 建立域名审计模型

**Files:**
- Create: `src/thailand_crawler/domain_quality.py`
- Test: `tests/test_domain_quality.py`

**Step 1: Write the failing test**
- 覆盖硬黑名单、品牌匹配、共享域名阈值、审计标签输出。

**Step 2: Run test to verify it fails**
- Run: `pytest tests/test_domain_quality.py -v`

**Step 3: Write minimal implementation**
- 提供域名规范化、硬黑名单判断、品牌匹配评分、共享域名隔离判定。

**Step 4: Run test to verify it passes**
- Run: `pytest tests/test_domain_quality.py -v`

### Task 2: 接入 GMAP 与 store

**Files:**
- Modify: `src/thailand_crawler/gmap.py`
- Modify: `src/thailand_crawler/streaming/store.py`
- Test: `tests/test_gmap.py`
- Test: `tests/test_streaming.py`

**Step 1: Write the failing test**
- 共享/平台/品牌不匹配域名不得进入 `website/domain/site/snov`。

**Step 2: Run test to verify it fails**
- Run: `pytest tests/test_gmap.py tests/test_streaming.py -v`

**Step 3: Write minimal implementation**
- 在官网接纳前调用统一判定。

**Step 4: Run test to verify it passes**
- Run: `pytest tests/test_gmap.py tests/test_streaming.py -v`

### Task 3: 接入 Snov 与 delivery 审计

**Files:**
- Modify: `src/thailand_crawler/snov.py`
- Modify: `src/thailand_crawler/delivery.py`
- Test: `tests/test_snov.py`
- Test: `tests/test_delivery.py`

**Step 1: Write the failing test**
- 隔离域名不得查邮箱，不得进入交付 CSV。

**Step 2: Run test to verify it fails**
- Run: `pytest tests/test_snov.py tests/test_delivery.py -v`

**Step 3: Write minimal implementation**
- `delivery` 输出前再做一次域名信任过滤，并生成审计摘要。

**Step 4: Run test to verify it passes**
- Run: `pytest tests/test_snov.py tests/test_delivery.py -v`

### Task 4: 数据清洗与重建交付

**Files:**
- Modify: `output/dnb_stream/store.db`（运行时数据）
- Output: `output/delivery/Thailand_day001/companies.csv`

**Step 1: 备份数据库**
- 复制 `store.db` 到带时间戳备份文件。

**Step 2: 清理隔离域名历史污染**
- 清空污染公司 `website/domain/emails/final_companies`，回退到 `website_queue`。

**Step 3: 重建交付**
- Run: `python product.py day1`

**Step 4: 验证结果**
- 复查 `companies.csv` 中不再含隔离域名。
