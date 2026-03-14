# DNB Thailand Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 在 Thailand 目录实现 D&B 建筑行业主体抓取、GMAP 官网补齐、Snov 邮箱补齐与日交付脚本。

**Architecture:** 使用 `curl_cffi` 驱动 D&B 页面和接口，先按地理与子行业切片稳定拿到主体清单，再用详情接口补齐官网、代表人和电话，最后串联 GMAP 与 Snov，并输出日增量交付包。

**Tech Stack:** Python、curl_cffi、python-dotenv、pytest。

---

### Task 1: 初始化项目骨架

**Files:**
- Create: `requirements.txt`
- Create: `.gitignore`
- Create: `.env.example`
- Create: `run.py`
- Create: `product.py`
- Create: `src/thailand_crawler/__init__.py`
- Create: `src/thailand_crawler/cli.py`
- Create: `src/thailand_crawler/config.py`
- Create: `src/thailand_crawler/models.py`

**Step 1: 写最小测试约束**
- 测 `run.py` 和 `product.py` 可以导入 CLI 与交付入口。

**Step 2: 通过最小实现让入口可运行**
- 补齐包路径、`ROOT/SRC` 注入、中文帮助文本。

### Task 2: 实现 D&B 客户端

**Files:**
- Create: `src/thailand_crawler/client.py`
- Test: `tests/test_client.py`

**Step 1: 先写失败测试**
- 测列表 URL、详情 URL、请求 body 构造。
- 测公司列表响应解析为统一字典。
- 测详情响应解析为 `website/key_principal/phone`。

**Step 2: 写最小实现**
- `curl_cffi.Session(impersonate="chrome110")`
- 页面预热
- 列表接口
- 详情接口
- 基础重试

### Task 3: 实现切片发现与主体抓取

**Files:**
- Create: `src/thailand_crawler/pipeline.py`
- Test: `tests/test_pipeline.py`

**Step 1: 先写失败测试**
- 测国家 -> 省 -> 区切片发现。
- 测 `count > 1000` 时改走 `relatedIndustries` 拆桶。
- 测公司主体写出与 `duns` 去重。

**Step 2: 写最小实现**
- `discover_segments`
- `crawl_segment_companies`
- `crawl_company_details`
- 断点文件持久化

### Task 4: 实现 GMAP 官网补齐

**Files:**
- Create: `src/thailand_crawler/gmap.py`
- Test: `tests/test_gmap.py`

**Step 1: 先写失败测试**
- 测官网清洗。
- 测无官网记录的查询词构造。

**Step 2: 写最小实现**
- 复用 Google Maps 协议查询思路。
- 仅更新缺官网记录。

### Task 5: 实现 Snov 邮箱补齐

**Files:**
- Create: `src/thailand_crawler/snov.py`
- Modify: `src/thailand_crawler/pipeline.py`

**Step 1: 先写失败测试**
- 测域名提取。
- 测邮箱合并去重。

**Step 2: 写最小实现**
- `SnovClient`
- `run_snov_enrichment`

### Task 6: 实现日交付脚本

**Files:**
- Create: `src/thailand_crawler/delivery.py`
- Test: `tests/test_delivery.py`
- Modify: `product.py`

**Step 1: 先写失败测试**
- 测 `dayN` 解析。
- 测增量 key 计算。
- 测 CSV 只落 `公司名/代表人/邮箱/域名/电话`。

**Step 2: 写最小实现**
- `build_delivery_bundle`
- `product.py` CLI 入口

### Task 7: 冒烟验证

**Files:**
- Modify: `run.py`
- Modify: `src/thailand_crawler/cli.py`

**Step 1: 跑单测**
- `pytest tests -q`

**Step 2: 跑小样本命令**
- `python run.py dnb --max-segments 1 --max-pages-per-segment 1 --max-items 5 --skip-gmap --skip-snov`

**Step 3: 检查输出**
- 确认 `output/dnb/` 下生成标准 JSONL 与 checkpoint。

