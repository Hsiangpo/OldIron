# England Germany Wiza Websites Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add England/Germany Wiza website-only collection and `product.py <Country> websites dayN` delivery.

**Architecture:** Keep country-specific Wiza clients/pipelines thin, remove accidental post-list execution paths, and add a separate websites delivery mode that uses independent `dayN` baselines. Reuse each country's existing normalization and checkpoint patterns while exporting a flat `websites.txt` result.

**Tech Stack:** Python 3.10+, `curl_cffi`, SQLite, unittest, existing OldIron delivery helpers

---

## Chunk 1: Tests First

### Task 1: Germany website-only behavior

**Files:**
- Modify: `Germany/tests/test_wiza_basic.py`

- [ ] **Step 1: Write failing tests for website-only filtering/export**
- [ ] **Step 2: Run the Germany Wiza tests and verify the new tests fail**
- [ ] **Step 3: Implement the minimal Germany code to pass**
- [ ] **Step 4: Re-run the Germany Wiza tests**

### Task 2: England new Wiza site behavior

**Files:**
- Create: `England/tests/test_wiza_basic.py`

- [ ] **Step 1: Write failing tests for United Kingdom filter and website export**
- [ ] **Step 2: Run the England Wiza tests and verify they fail**
- [ ] **Step 3: Implement the minimal England code to pass**
- [ ] **Step 4: Re-run the England Wiza tests**

## Chunk 2: Runtime Implementation

### Task 3: Germany list-only Wiza runtime

**Files:**
- Modify: `Germany/src/germany_crawler/sites/wiza/cli.py`
- Modify: `Germany/src/germany_crawler/sites/wiza/pipeline.py`
- Modify: `Germany/README.md`

- [ ] **Step 1: Make runtime commands list-only**
- [ ] **Step 2: Keep only website URLs as effective output**
- [ ] **Step 3: Export `output/wiza/websites.txt` after list runs**
- [ ] **Step 4: Document the new runtime semantics**

### Task 4: England new Wiza runtime

**Files:**
- Modify: `England/run.py`
- Create: `England/src/england_crawler/sites/wiza/__init__.py`
- Create: `England/src/england_crawler/sites/wiza/client.py`
- Create: `England/src/england_crawler/sites/wiza/cli.py`
- Create: `England/src/england_crawler/sites/wiza/pipeline.py`
- Modify: `England/README.md`

- [ ] **Step 1: Add the Wiza site entry**
- [ ] **Step 2: Add the United Kingdom Wiza list client/pipeline**
- [ ] **Step 3: Export `output/wiza/websites.txt`**
- [ ] **Step 4: Document runtime requirements and commands**

## Chunk 3: Websites Delivery

### Task 5: Country delivery support

**Files:**
- Modify: `England/src/england_crawler/delivery.py`
- Modify: `Germany/src/germany_crawler/delivery.py`
- Modify: `product.py`

- [ ] **Step 1: Add websites day-package builders in both country delivery modules**
- [ ] **Step 2: Add root `product.py <Country> websites dayN` dispatch**
- [ ] **Step 3: Keep company delivery behavior unchanged**
- [ ] **Step 4: Verify websites day baseline logic is independent**

## Chunk 4: Verification

### Task 6: Run focused tests

**Files:**
- Test: `Germany/tests/test_wiza_basic.py`
- Test: `England/tests/test_wiza_basic.py`

- [ ] **Step 1: Run Germany focused tests**
- [ ] **Step 2: Run England focused tests**
- [ ] **Step 3: Run any additional impacted delivery tests if needed**
- [ ] **Step 4: Fix failures until green**

### Task 7: Finalize

**Files:**
- Modify: `coordination/active_tasks.json`
- Modify: `coordination/shared_locks.json`

- [ ] **Step 1: Review resulting files and summaries**
- [ ] **Step 2: Commit the verified change set**
- [ ] **Step 3: Push to `main`**
- [ ] **Step 4: Release the coordination task and locks**
