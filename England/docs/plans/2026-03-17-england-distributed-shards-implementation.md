# England Distributed Shards Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 把 England 主执行链路切换为静态切片、各机独立执行、主机集中合并，同时保留现有单机 DNB / CH pipeline。

**Architecture:** 不删除现有单机 pipeline，只给它们补“外部输入 + 独立输出目录”能力；新增 `dist` 命令负责切片和合并；交付继续使用 `product.py` / `delivery.py`。

**Tech Stack:** Python 3.11, argparse, sqlite, jsonl, openpyxl, 现有 England pipeline 与 delivery 模块

---

### Task 1: 新增分布式命令骨架

**Files:**
- Create: `src/england_crawler/distributed/__init__.py`
- Create: `src/england_crawler/distributed/cli.py`
- Test: `tests/test_distributed_cli.py`
- Modify: `run.py`

**Step 1: 写失败测试**

- 验证 `python run.py dist --help` 可用
- 验证存在 `plan-ch`、`plan-dnb`、`merge-site` 这三类命令

**Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_distributed_cli -v`

**Step 3: 写最小实现**

- 在 `run.py` 增加 `dist` 入口
- 在 `distributed/cli.py` 增加 argparse 骨架

**Step 4: 运行测试确认通过**

Run: `python -m unittest tests.test_distributed_cli -v`

**Step 5: Commit**

```bash
git add run.py src/england_crawler/distributed/__init__.py src/england_crawler/distributed/cli.py tests/test_distributed_cli.py
git commit -m "feat: add england distributed cli skeleton"
```

### Task 2: 给 Companies House 增加切片输入能力

**Files:**
- Create: `src/england_crawler/companies_house/input_source.py`
- Modify: `src/england_crawler/companies_house/cli.py`
- Modify: `src/england_crawler/companies_house/config.py`
- Modify: `src/england_crawler/companies_house/pipeline.py`
- Test: `tests/test_companies_house_input.py`
- Test: `tests/test_companies_house_cli.py`

**Step 1: 写失败测试**

- 验证支持从文本清单读取公司名
- 验证 `--output-dir` 能覆盖默认目录

**Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_companies_house_input tests.test_companies_house_cli -v`

**Step 3: 写最小实现**

- 把公司名输入抽象成统一 loader
- `companies-house` CLI 增加 `--input-file`、`--output-dir`
- config 接收新的输入与输出目录

**Step 4: 运行测试确认通过**

Run: `python -m unittest tests.test_companies_house_input tests.test_companies_house_cli -v`

**Step 5: Commit**

```bash
git add src/england_crawler/companies_house/input_source.py src/england_crawler/companies_house/cli.py src/england_crawler/companies_house/config.py src/england_crawler/companies_house/pipeline.py tests.test_companies_house_input.py tests.test_companies_house_cli.py
git commit -m "feat: support shard inputs for england companies house"
```

### Task 3: 给 DNB 增加种子切片输入能力

**Files:**
- Create: `src/england_crawler/dnb/seed_segments.py`
- Modify: `src/england_crawler/dnb/cli.py`
- Modify: `src/england_crawler/dnb/config.py`
- Modify: `src/england_crawler/dnb/pipeline.py`
- Test: `tests/test_dnbkorea_pipeline.py`
- Test: `tests/test_dnbkorea_cli.py`

**Step 1: 写失败测试**

- 验证 DNB 可从外部 seed JSONL 装载 discovery seeds
- 验证 `--output-dir` 能隔离本地 sqlite 和快照目录

**Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_dnbkorea_pipeline tests.test_dnbkorea_cli -v`

**Step 3: 写最小实现**

- `dnb` CLI 增加 `--seed-file`、`--output-dir`
- pipeline 在有 seed 文件时不再强制用全量 catalog

**Step 4: 运行测试确认通过**

Run: `python -m unittest tests.test_dnbkorea_pipeline tests.test_dnbkorea_cli -v`

**Step 5: Commit**

```bash
git add src/england_crawler/dnb/seed_segments.py src/england_crawler/dnb/cli.py src/england_crawler/dnb/config.py src/england_crawler/dnb/pipeline.py tests/test_dnbkorea_pipeline.py tests/test_dnbkorea_cli.py
git commit -m "feat: support shard seeds for england dnb"
```

### Task 4: 实现 CH 静态切片规划

**Files:**
- Create: `src/england_crawler/distributed/ch_planner.py`
- Modify: `src/england_crawler/distributed/cli.py`
- Test: `tests/test_distributed_ch_planner.py`

**Step 1: 写失败测试**

- 验证按稳定哈希分成 N 个 shard
- 验证输出 manifest 和 shard 文本文件
- 验证重复执行结果稳定

**Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_distributed_ch_planner -v`

**Step 3: 写最小实现**

- 从 xlsx 读取公司名
- 标准化后按哈希分桶
- 写 `manifest.json` 和 `shard-XXX.txt`

**Step 4: 运行测试确认通过**

Run: `python -m unittest tests.test_distributed_ch_planner -v`

**Step 5: Commit**

```bash
git add src/england_crawler/distributed/ch_planner.py src/england_crawler/distributed/cli.py tests/test_distributed_ch_planner.py
git commit -m "feat: add england companies house shard planner"
```

### Task 5: 实现 DNB 静态切片规划

**Files:**
- Create: `src/england_crawler/distributed/dnb_planner.py`
- Modify: `src/england_crawler/distributed/cli.py`
- Test: `tests/test_distributed_dnb_planner.py`

**Step 1: 写失败测试**

- 验证默认只输出叶子行业切片
- 验证 N 个 shard 分桶稳定
- 验证 manifest 和 `segments.jsonl` 正确

**Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_distributed_dnb_planner -v`

**Step 3: 写最小实现**

- 从 catalog 生成叶子行业路径
- 构造 `Segment` JSONL
- 写 manifest 和 shard 文件

**Step 4: 运行测试确认通过**

Run: `python -m unittest tests.test_distributed_dnb_planner -v`

**Step 5: Commit**

```bash
git add src/england_crawler/distributed/dnb_planner.py src/england_crawler/distributed/cli.py tests/test_distributed_dnb_planner.py
git commit -m "feat: add england dnb shard planner"
```

### Task 6: 实现站点级合并

**Files:**
- Create: `src/england_crawler/distributed/site_merge.py`
- Modify: `src/england_crawler/distributed/cli.py`
- Test: `tests/test_distributed_site_merge.py`

**Step 1: 写失败测试**

- 验证多个 run 目录能合并成标准站点目录
- 验证优先读取 `final_companies.jsonl`
- 验证去重和原子替换

**Step 2: 运行测试确认失败**

Run: `python -m unittest tests.test_distributed_site_merge -v`

**Step 3: 写最小实现**

- 读取多个 run 目录
- 汇总成标准 `companies.jsonl` / `companies_enriched.jsonl` / `companies_with_emails.jsonl` / `final_companies.jsonl`
- 目标目录原子替换

**Step 4: 运行测试确认通过**

Run: `python -m unittest tests.test_distributed_site_merge -v`

**Step 5: Commit**

```bash
git add src/england_crawler/distributed/site_merge.py src/england_crawler/distributed/cli.py tests/test_distributed_site_merge.py
git commit -m "feat: add england distributed site merge"
```

### Task 7: 补文档和端到端命令说明

**Files:**
- Modify: `README.md`
- Modify: `docs/plans/2026-03-17-england-distributed-shards-design.md`

**Step 1: 写最小文档更新**

- 增加新的推荐执行流
- 写清楚主机和子机分别执行什么命令

**Step 2: 验证命令示例与代码一致**

- 手工核对 `run.py` 用法

**Step 3: Commit**

```bash
git add README.md docs/plans/2026-03-17-england-distributed-shards-design.md
git commit -m "docs: document england distributed shard workflow"
```

### Task 8: 运行关键测试并做冒烟验证

**Files:**
- Modify: `tests/...` 仅在发现缺口时补测

**Step 1: 跑单测**

Run:

```bash
cd E:/Develop/Masterpiece/Spider/Website/OldIron/England
python -m unittest ^
  tests.test_distributed_cli ^
  tests.test_distributed_ch_planner ^
  tests.test_distributed_dnb_planner ^
  tests.test_distributed_site_merge ^
  tests.test_companies_house_input ^
  tests.test_companies_house_cli ^
  tests.test_dnbkorea_pipeline ^
  tests.test_dnbkorea_cli -v
```

**Step 2: 跑本地命令冒烟**

Run:

```bash
cd E:/Develop/Masterpiece/Spider/Website/OldIron/England
python run.py dist plan-ch --input-xlsx docs/英国.xlsx --shards 2
python run.py dist plan-dnb --shards 2
```

**Step 3: 记录输出路径和样例结果**

**Step 4: Commit**

```bash
git add .
git commit -m "feat: finish england distributed shard workflow"
```
