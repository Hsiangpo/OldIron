# Denmark DNB Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a standalone Denmark DNB project that supports crawling, sharding, merge, and `product.py dayN` delivery with the same operational model as England.

**Architecture:** Copy the England DNB runtime into `Denmark`, rename the package to `denmark_crawler`, remove Companies House dependencies, and replace UK hardcoding with Denmark defaults. Keep the distributed and delivery flow aligned with England so later Spain and Turkey can be cloned from Denmark.

**Tech Stack:** Python 3.11, sqlite, curl_cffi, requests, websocket-client, OpenAI API, Firecrawl API.

---

### Task 1: Create Denmark project skeleton

**Files:**
- Create: `Denmark/run.py`
- Create: `Denmark/product.py`
- Create: `Denmark/README.md`
- Create: `Denmark/requirements.txt`
- Create: `Denmark/.env.example`
- Create: `Denmark/src/denmark_crawler/...`

**Step 1: Copy required England files**

Copy only DNB-related modules and shared delivery/distributed utilities.

**Step 2: Remove UK-only modules**

Do not copy `companies_house`, `cluster`, or England-only entry wiring.

**Step 3: Verify skeleton exists**

Check that `Denmark/src/denmark_crawler/dnb`, `distributed`, `fc_email`, `google_maps`, and `delivery.py` exist.

### Task 2: Rename package and country defaults

**Files:**
- Modify: `Denmark/run.py`
- Modify: `Denmark/product.py`
- Modify: `Denmark/src/denmark_crawler/dnb/*.py`
- Modify: `Denmark/src/denmark_crawler/distributed/*.py`
- Modify: `Denmark/src/denmark_crawler/delivery.py`

**Step 1: Rename imports**

Replace `england_crawler` with `denmark_crawler`.

**Step 2: Rename country names**

Replace `England` / `英国` / `United Kingdom` with `Denmark` / `丹麦` where they are defaults or user-facing logs.

**Step 3: Rename runtime defaults**

Replace default country ISO from `gb` to `dk`.

### Task 3: Strip Denmark project to DNB-only distributed flow

**Files:**
- Modify: `Denmark/run.py`
- Modify: `Denmark/src/denmark_crawler/distributed/cli.py`
- Modify: `Denmark/src/denmark_crawler/distributed/bootstrap.py`
- Modify: `Denmark/src/denmark_crawler/distributed/site_merge.py`

**Step 1: Keep only DNB dist commands**

Support `plan-dnb`, `bootstrap-dnb`, `merge-site dnb`.

**Step 2: Remove Companies House imports**

Use local key normalization instead of CH helpers.

**Step 3: Make merge output Denmark-compatible**

Write the same snapshot files expected by `product.py`.

### Task 4: Adapt runtime details to Denmark

**Files:**
- Modify: `Denmark/src/denmark_crawler/dnb/config.py`
- Modify: `Denmark/src/denmark_crawler/dnb/pipeline.py`
- Modify: `Denmark/src/denmark_crawler/dnb/store.py`
- Modify: `Denmark/src/denmark_crawler/google_maps/client.py`

**Step 1: Switch country-specific defaults**

- country code `dk`
- country display name `Denmark`
- Google Maps `gl=dk`

**Step 2: Keep Firecrawl + LLM behavior aligned with latest England fixes**

Include the zero-email retry logic and current concurrency defaults.

**Step 3: Keep resume and shard bootstrap behavior**

Ensure reruns keep using the same output dir and sqlite state.

### Task 5: Add Denmark delivery packaging

**Files:**
- Modify: `Denmark/product.py`
- Modify: `Denmark/src/denmark_crawler/delivery.py`

**Step 1: Change delivery directory naming**

Use `Denmark_dayNNN`.

**Step 2: Keep dayN baseline logic**

Reuse England daily delivery behavior.

**Step 3: Keep export shape**

Output `companies.csv`, `keys.txt`, `summary.json`.

### Task 6: Add Denmark smoke tests

**Files:**
- Create: `Denmark/tests/test_run_dispatch.py`
- Create: `Denmark/tests/test_dist_cli.py`
- Create: `Denmark/tests/test_delivery.py`

**Step 1: Verify run.py dispatch**

Test `dnb` and `dist`.

**Step 2: Verify DNB shard planner defaults**

Test default country code is `dk`.

**Step 3: Verify delivery day directory naming**

Test output uses `Denmark_day001`.

### Task 7: Verify with real commands

**Files:**
- Modify if needed after verification

**Step 1: Run unit tests**

Run Denmark tests.

**Step 2: Run CLI help / planning smoke**

Run:

```bash
cd Denmark
python run.py --help
python run.py dist plan-dnb --shards 2
python product.py day1
```

Expected:

- help output is correct
- shard files are generated
- `product.py day1` fails gracefully if no data, or succeeds if seeded data exists

**Step 3: Fix any smoke issues**

Patch logs, imports, or defaults until the commands are stable.
