# Denmark Virk Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a new `python run.py virk` route in `Denmark` that directly collects Danish companies from Virk APIs and only uses GMap/Firecrawl for email backfill.

**Architecture:** Add a new `denmark_crawler.virk` module with browser-cookie sync, API client, sqlite store, queue-based pipeline, and snapshot export. Reuse existing Denmark `google_maps` and `fc_email` modules. Keep delivery unchanged by writing standard output JSONL files into `output/virk`.

**Tech Stack:** Python 3.11, requests, websocket-client, curl_cffi, sqlite, OpenAI API, Firecrawl API.

---

### Task 1: Create Virk module skeleton

**Files:**
- Create: `Denmark/src/denmark_crawler/virk/__init__.py`
- Create: `Denmark/src/denmark_crawler/virk/browser_cookie.py`
- Create: `Denmark/src/denmark_crawler/virk/client.py`
- Create: `Denmark/src/denmark_crawler/virk/config.py`
- Create: `Denmark/src/denmark_crawler/virk/models.py`
- Create: `Denmark/src/denmark_crawler/virk/store.py`
- Create: `Denmark/src/denmark_crawler/virk/pipeline.py`
- Create: `Denmark/src/denmark_crawler/virk/cli.py`

### Task 2: Add Denmark run.py dispatch

**Files:**
- Modify: `Denmark/run.py`
- Modify: `Denmark/README.md`

Add a new site:

- `virk`

### Task 3: Implement protocol client

**Files:**
- Modify: `Denmark/src/denmark_crawler/virk/client.py`
- Modify: `Denmark/src/denmark_crawler/virk/browser_cookie.py`

Support:

- fetch browser cookies for `datacvr.virk.dk`
- search POST request
- detail GET request
- simple pagination

### Task 4: Implement sqlite runtime

**Files:**
- Modify: `Denmark/src/denmark_crawler/virk/store.py`

Need:

- page queue
- company table
- detail queue
- gmap queue
- firecrawl queue
- final output refresh

### Task 5: Implement pipeline rules

**Files:**
- Modify: `Denmark/src/denmark_crawler/virk/pipeline.py`

Rules:

- search imports list data
- detail enriches representative and legal owner
- if `name + representative + email` is already complete, skip GMap and Firecrawl
- only missing-email companies go to GMap
- only missing-email companies with website go to Firecrawl

### Task 6: Implement snapshot export

**Files:**
- Modify: `Denmark/src/denmark_crawler/virk/store.py`

Write standard files into `Denmark/output/virk`.

### Task 7: Add minimum tests

**Files:**
- Create: `Denmark/tests/test_virk_client.py`
- Create: `Denmark/tests/test_virk_store.py`
- Create: `Denmark/tests/test_virk_cli.py`

### Task 8: Run smoke verification

**Files:**
- Modify if needed after smoke

Smoke:

- `python run.py virk --max-companies 20`
- verify `output/virk/store.db`
- verify snapshot files exist
- verify at least some records stop before GMap/Firecrawl when email already exists
