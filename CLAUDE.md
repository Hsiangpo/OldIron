# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this repo is

OldIron is a **multi-country company-data collection monorepo**. Each top-level country folder
(`Brazil/`, `Denmark/`, `England/`, `Finland/`, `Germany/`, `Italy/`, `Japan/`, `Taiwan/`,
`UnitedArabEmirates/`, `UnitedStates/` — 10 countries) is a near-independent Python project with its own
`run.py`, `src/<country>_crawler/`, `output/`, and `tests/`. Cross-country capabilities live in
`shared/oldiron_core/`. `VersatileBackend/` is a separate **Go** service tree for high-concurrency
generic work (Gmap/Snov/MyIP/Firecrawl). `OldIronCrawler/` is a distinct, packaged desktop tool
(generic website-list → CSV), not a country crawler.

**Core architecture principle:** all site/web crawlers are written in **Python**; high-concurrency
generic backend services are written in **Go**. Do not add new Go dependencies for a new
country/site crawler.

## Read these first — they are the source of truth

- **`AGENTS.md`** (repo root) — the authoritative, exhaustive ruleset: delivery rules,
  per-country overrides, coordination protocol, sync rules, machine registry. When in doubt,
  it wins over this file.
- **`.claude/commands/oldiron-crawler.md`** — the canonical workflow for crawler work
  (also mirrored as the `oldiron-crawler` skill). Invoke it whenever the task is: add a country,
  add a site, import Excel/CSV, or change delivery/`product.py`.
- **`README.md`** — current country/site coverage table and the email-extraction pipeline detail.

Priority when sources conflict: **current code > root `AGENTS.md` > current user task > skill/docs.**
Never infer one country's strategy from another — country-specific overrides in `AGENTS.md` always
beat generic defaults.

## Behavioral guardrails (these cause rework or data loss if ignored)

- **Before writing delivery code for a country whose strategy is not already in `AGENTS.md`, ask
  the user** for (1) delivery mode — merge-and-dedupe vs per-site, and (2) email policy — none /
  allowlist / denylist. Then record the confirmed choice in `AGENTS.md`.
- **Do not run `python product.py <Country> dayN` unless the user explicitly asked/approved** it
  this session. It writes real delivery packages.
- **Do not delete delivery files.** When replacing an existing day package, move the old directory
  to the OS recycle bin/trash (see `shared/oldiron_core/delivery/trash.py`), never hard-delete.
- **Real validation is required** for crawler/pipeline/resume/delivery changes — run an actual
  crawl/checkpoint/delivery, not just mock tests, before claiming success.
- Code/docs/tests sync **only via Git**; `.env`, SQLite DBs, and `output/delivery/` may move by
  SSH/scp. Never SSH/scp whole-project overwrites or `tmp/`.

## Common commands

Scrapers run from inside the country dir; delivery always runs from the repo root.

```bash
# install + run a site (one entry form everywhere: python run.py <site>)
cd Denmark && python -m pip install -r requirements.txt
cd Denmark && python run.py proff        # also: python run.py virk
cd England && python run.py companyname
cd Germany && python run.py wiza

# delivery (root entry; delegates to <country>_crawler.delivery.build_delivery_bundle)
python product.py Denmark day1
python product.py Germany websites day1   # "websites"-only delivery variant (per-site)

# OldIronCrawler desktop tool (separate product)
cd OldIronCrawler && python run.py        # launches the dashboard TUI
```

Day labels are strictly sequential: first delivery must be `day1`; thereafter only `dayN` (rerun)
or `dayN+1` is accepted (`shared/oldiron_core/delivery/engine.py`).

### Tests (runner differs per project — match the target)

```bash
cd England && python -m unittest tests -v          # England/Taiwan/UnitedStates/Italy use unittest
cd Japan && python -m pytest test -v               # Japan uses pytest
cd OldIronCrawler && python -m pytest tests -v
python -m pytest tests                              # root tests: shared delivery/coordination logic
# single test: python -m pytest tests/test_delivery_engine.py::TestName::test_case
```

Python 3.10+, 4-space indent, `snake_case`/`PascalCase`.

## Crawler architecture: the P1/P2/P3 pipeline model

A site under `<Country>/src/<country>_crawler/sites/<site>/` follows a standard layout
(`cli.py`, `client.py`, `parser.py`, `pipeline.py`, `pipeline2_gmap.py`, `pipeline3_email.py`,
`store.py`). The three pipelines run **concurrently**, not serially:

- **P1 — site collection:** pull company subjects + detail fields from the source site into SQLite.
  Full-site coverage is the default goal (segmentation, category recursion, full pagination).
- **P2 — Google Maps completion:** fill missing `website`/`phone` via the protocol Gmap path
  (`shared/oldiron_core/google_maps/`), scoring candidate matches and rejecting weak/non-official ones.
- **P3 — contact extraction:** crawl the official website with the protocol crawler, convert
  HTML→Markdown, then extract. **Emails are rule-based only; the LLM is used only for the
  representative.**

P2/P3 continuously poll storage for new work — P1 finishing must not block them, and each pipeline
keeps its own retry/rate-limit logic. **Resume/checkpoint is mandatory** (SQLite WAL; restart
continues from saved progress; recover stale `running` rows to pending).

`cli.py` auto-manages the Go backends a site needs (e.g. starts/stops `gmap-service` via PID +
healthcheck) so `python run.py <site>` works without manually launching backends. Country `.env`
is loaded with `override=True` so it beats stray host env vars.

### Shared core (`shared/oldiron_core/`)

- `protocol_crawler/` — `curl_cffi`-based link discovery (`map_site`: sitemap then homepage links)
  + HTML scraping (`scrape_html`). The zero-cost Firecrawl replacement, used when
  `CRAWL_BACKEND=protocol`. The email route was migrated **from Firecrawl → protocol crawler + LLM**.
- `fc_email/` — email/representative extraction service, LLM client, key pool, and a **domain cache**
  (when many companies share a domain, the first task does the work and the rest reuse the cached
  result; no email found → mark done, don't retry forever).
- `google_maps/`, `snov/`, `delivery/`, `dnb_cookie_cache.py`.

Modules imported by **multiple countries** must live in `shared/`; modules shared by **multiple
sites of one country** live in `<Country>/shared/`. **Never** import/symlink/copy a module from one
country's (or site's) tree into another — no cross-country/cross-site imports, no symlink sharing.

### HTML → Markdown → LLM (exact order)

Fetch raw HTML → strip `script/style/img/svg/video/audio/canvas/iframe/noscript` → markdownify →
collapse blank lines → truncate a single page to **80,000 chars** (symmetric) → truncate the final
combined prompt to **250,000 chars** (`_MAX_PROMPT_CHARS`, under the model's ~272k token limit). Rule-based email extraction uses the **full** page content
(never truncate the rule path); truncation applies only to what is sent to the LLM. On LLM 429:
wait-and-retry forever (do not consume retry budget); ordinary transient failures use bounded
exponential backoff.

### Representative extraction rules

Accept only principal roles (CEO, Managing Director, Director, Chairman, Founder, Owner, Partner,
President/VP, Chief-prefixed officers). Reject Manager/Coordinator/Consultant/Advisor/Assistant/
Secretary/Clerk/etc. The name must appear **verbatim** in page content (never inferred from the
company name), must come with an `evidence_quote`, and code must verify ≥50% of name tokens appear
in that quote or the representative is cleared.

## Delivery model

- Default: merge all of a country's site outputs and **dedupe by normalized company name** into one
  `companies.csv` + `keys.txt`. Many countries override this to **per-site** delivery
  (Japan, Brazil, Germany, UnitedStates, UAE…) — check `AGENTS.md`.
- **Delivery gate (default):** a record ships only when `company_name` **and** `representative`
  **and** `emails` are all present. Some countries (e.g. UAE) define their own gate — check `AGENTS.md`.
- Unified CSV column order (do not add columns):
  `company_name, representative, emails, website, phone, evidence_url` (`emails` semicolon-joined).
- Output path: `<Country>/output/delivery/<Country>_dayNNN/`.

## Dual-agent coordination (two machines/agents may run in parallel)

Substantial work starts with: `git pull` → read `AGENTS.md` → read `coordination/active_tasks.json`
and `coordination/shared_locks.json` → classify the task:

- **`site_local`** — touches one country/site only: register the task, work directly on `main`
  (no branch/PR unless the user asks), commit+push after verification.
- **`shared_zone`** — touches any high-risk shared path: register the task **and** claim a lease lock
  (`expires_at` + `heartbeat_at`), push the lock before editing, release it with the completion push.
  High-risk zones: `shared/`, root `product.py`/`AGENTS.md`/`README.md`, `.github/`, `coordination/`,
  any `<Country>/shared/`, any `<Country>/src/*/delivery.py`.

Use the CLI rather than hand-editing the JSON:

```bash
python coordination/coord_cli.py begin --task-id ... --change-class site_local|shared_zone --machine ... --scope ...
python coordination/coord_cli.py heartbeat --task-id ... --lease-minutes 20
python coordination/coord_cli.py finish --task-id ... --notes "done"
python coordination/preflight.py --change-class shared_zone --scope ... --lock-path ...
python coordination/lease_doctor.py            # inspect stale/expired locks before takeover
```

If a planned path is already owned/locked by another active task, stop and report the conflict.

## Hard conventions

- Function ≤ **200 lines**, source file ≤ **1000 lines**, directory normally ≤ **10 files** — split
  when exceeded. No `_v2`/`_old`/version-suffixed files; replace old code by deleting it, not by
  commenting it out.
- UTF-8 everywhere. **Code comments and user-facing frontend text in Chinese**; technical docs
  (`AGENTS.md`, design notes) in English.
- Outbound (non-China) traffic uses proxy port **7897** by default; probe for the real port if it's down.
- LLM config via country `.env`: `LLM_API_KEY`, `LLM_BASE_URL`, `LLM_MODEL`, `CRAWL_BACKEND`.
  The active provider is whatever the user last confirmed + that machine's local `.env` — **not**
  committed docs/`.env.example`/code defaults. Keep API keys only in local `.env`; never commit them.
- `AGENTS.md` designates `<Country>/bak/` or `former/` as the legacy-archive location (reference-only,
  never extend). Note: neither path currently exists — all 10 countries above are on the active framework.
- Put temporary artifacts in a local `tmp/` and clean them up; never scatter debug files in the repo root.
