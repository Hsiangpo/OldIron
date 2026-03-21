# OldIron Shared Delivery And Denmark Proff Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a real root `product.py`, extract England and Denmark into a shared delivery engine, add Denmark `proff`, and create the initial Go backend skeleton.

**Architecture:** Shared Python delivery logic lives under `shared/oldiron_core/delivery`, country wrappers stay thin, new Denmark sites live under `denmark_crawler/sites/`, and `VersatileBackend` becomes a real Go codebase skeleton instead of documentation only.

**Tech Stack:** Python 3.11, unittest, requests, sqlite3, Go 1.22 skeleton

---

### Task 1: Replace Example Root Delivery Entry

**Files:**
- Create: `product.py`
- Delete: `Product.py`
- Modify: `README.md`
- Modify: `AGENTS.md`

**Step 1: Write the failing behavior test mentally**

The current root entry is only a subprocess shell and the documented lowercase path does not truly exist as the canonical file.

**Step 2: Implement the real root entry**

- add lowercase `product.py`
- support England and Denmark through the shared engine path
- keep legacy subprocess fallback for non-migrated countries

**Step 3: Align docs**

- update command examples to lowercase `product.py`
- remove wording that conflicts with shared reusable Python core

**Step 4: Verify**

Run targeted import checks for the root entry.

### Task 2: Extract Shared Delivery Core

**Files:**
- Create: `shared/oldiron_core/__init__.py`
- Create: `shared/oldiron_core/delivery/__init__.py`
- Create: `shared/oldiron_core/delivery/spec.py`
- Create: `shared/oldiron_core/delivery/engine.py`
- Create: `England/src/england_crawler/country_spec.py`
- Create: `Denmark/src/denmark_crawler/country_spec.py`
- Modify: `England/src/england_crawler/delivery.py`
- Modify: `Denmark/src/denmark_crawler/delivery.py`

**Step 1: Move generic delivery behavior**

Put parsing, loading, baseline, dedupe, delta, and writing into the shared engine.

**Step 2: Keep country rules small**

Each country only provides a `DeliverySpec`.

**Step 3: Keep compatibility**

Country-local `build_delivery_bundle(...)` wrappers should keep the same function signature so current tests keep working.

**Step 4: Verify**

Run the existing England and Denmark delivery tests.

### Task 3: Add Denmark Proff Site Package

**Files:**
- Create: `Denmark/src/denmark_crawler/sites/__init__.py`
- Create: `Denmark/src/denmark_crawler/sites/proff/__init__.py`
- Create: `Denmark/src/denmark_crawler/sites/proff/models.py`
- Create: `Denmark/src/denmark_crawler/sites/proff/config.py`
- Create: `Denmark/src/denmark_crawler/sites/proff/client.py`
- Create: `Denmark/src/denmark_crawler/sites/proff/store.py`
- Create: `Denmark/src/denmark_crawler/sites/proff/pipeline.py`
- Create: `Denmark/src/denmark_crawler/sites/proff/cli.py`
- Modify: `Denmark/run.py`
- Modify: `Denmark/README.md`

**Step 1: Implement parser/client**

Parse Proff search pages from `__NEXT_DATA__` and map rows to a normalized model.

**Step 2: Implement SQLite resume store**

Seed query/page tasks, resume unfinished tasks, dedupe companies by `orgnr`, and export JSONL snapshots.

**Step 3: Implement pipeline and CLI**

Support direct run plus future custom query-file based runs.

**Step 4: Wire run entry**

Add `python run.py proff`.

**Step 5: Verify**

Run unit tests and a small live smoke run against one query with a low page cap.

### Task 4: Add Denmark Proff Tests

**Files:**
- Create: `Denmark/tests/test_proff_client.py`
- Create: `Denmark/tests/test_proff_store.py`
- Modify: `Denmark/tests/test_run_dispatch.py`

**Step 1: Test parser**

Use fixture-like inline HTML/JSON snippets to verify `__NEXT_DATA__` parsing.

**Step 2: Test store**

Verify task seeding, company dedupe, and final snapshot export.

**Step 3: Test run dispatch**

Verify `run.py proff` dispatches to the new CLI.

**Step 4: Verify**

Run the Denmark test subset.

### Task 5: Add Go Backend Skeleton

**Files:**
- Create: `VersatileBackend/go.mod`
- Create: `VersatileBackend/cmd/firecrawl-service/main.go`
- Create: `VersatileBackend/cmd/gmap-service/main.go`
- Create: `VersatileBackend/cmd/snov-service/main.go`
- Create: `VersatileBackend/internal/app/server.go`
- Modify: `VersatileBackend/README.MD`

**Step 1: Build minimal health-check services**

Each service should compile and expose a minimal HTTP health endpoint.

**Step 2: Document intent clearly**

State that the Go backend is now the real concurrency landing zone, while Python crawlers remain site-specific.

**Step 3: Verify**

Run `go test ./...` or `go build ./...` under `VersatileBackend`.

### Task 6: Final Verification

**Files:**
- None

**Step 1: Run Python tests**

Run focused England and Denmark tests that cover delivery and Denmark entrypoints.

**Step 2: Run Proff smoke**

Run a low-volume Proff command using proxy `7897`.

**Step 3: Run Go build**

Verify `VersatileBackend` compiles.

**Step 4: Update summary docs if needed**

Make sure the root README and Denmark README describe the new entrypoints accurately.
