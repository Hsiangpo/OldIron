# Product Delta Generation Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** 恢复 `product.py` 的增量生成逻辑（day2 仅输出相对 day1 的新增），并支持覆盖已存在的 day 文件以便“重新生成”。

**Architecture:** 在 `product.py` 中按天读取历史 day1..dayN-1 的 CSV 生成去重键集合；遍历 `companies.csv`，输出未出现的记录到目标 day 文件。若目标文件已存在则允许覆盖。

**Tech Stack:** Python 3, csv, pathlib

### Task 1: Add failing tests for incremental generation

**Files:**
- Create: `test/test_product.py`

**Step 1: Write the failing test**

```python
def test_day2_outputs_only_new_rows_and_overwrites(tmp_path, monkeypatch):
    ...
```

**Step 2: Run test to verify it fails**

Run: `pytest test/test_product.py::test_day2_outputs_only_new_rows_and_overwrites -q`
Expected: FAIL (functionality not implemented / CLI lacks overwrite)

**Step 3: Write minimal implementation**

Implement incremental logic + overwrite behavior in `product.py`.

**Step 4: Run test to verify it passes**

Run: `pytest test/test_product.py::test_day2_outputs_only_new_rows_and_overwrites -q`
Expected: PASS

**Step 5: Commit**

Skip (not a git repo).

### Task 2: Add basic day1 copy test (safety)

**Files:**
- Modify: `test/test_product.py`

**Step 1: Write the failing test**

```python
def test_day1_copies_total(tmp_path, monkeypatch):
    ...
```

**Step 2: Run test to verify it fails**

Run: `pytest test/test_product.py::test_day1_copies_total -q`
Expected: FAIL (behavior not aligned after refactor)

**Step 3: Write minimal implementation**

Adjust `product.py` to keep day1 full snapshot behavior.

**Step 4: Run test to verify it passes**

Run: `pytest test/test_product.py::test_day1_copies_total -q`
Expected: PASS

**Step 5: Commit**

Skip (not a git repo).

### Task 3: Full test run and regenerate day2

**Files:**
- Modify: `product.py`

**Step 1: Run full tests**

Run: `pytest -q`
Expected: PASS

**Step 2: Regenerate day2 output**

Run: `python product.py day2 --force`
Expected: outputs updated `companies_002.csv`

