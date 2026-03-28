---
name: oldiron-crawler
description: Use for OldIron multi-country company data collection work: adding a new country, adding a new site to an existing country, importing Excel/CSV sources, or changing delivery/product.py logic. Covers site exploration, protocol analysis, pipeline design, implementation, smoke tests, git/deploy, and delivery integration.
---

# OldIron crawler skill for Codex

Use this skill for repository-local OldIron crawling work.

When this skill is activated, state clearly:

> I am using the `oldiron-crawler` skill for this OldIron task.

## 1. Core project rules

- New country/site crawler work must use **Python**.
- Do **not** introduce new Go backend dependencies for new country/site crawler implementations.
- Prefer shared modules under `shared/oldiron_core/`.
- Do not copy or symlink shared business modules into country folders.
- Keep all files UTF-8.
- Code comments must be Chinese.
- User-facing frontend text must be Chinese.
- Technical docs may be written in English.
- Backend function length must stay within **200 lines**.
- Backend source file length must stay within **1000 lines**.
- A single directory should normally stay within **10 files**.
- Do not create `_v2`, `_old`, or similar version-suffixed files.
- Do not leave temporary artifacts scattered in the repo root. Put temporary files under a local `tmp/` folder and clean them up after smoke tests.

## 2. Before implementation: required alignment questions

Before writing delivery code, ask the user these two questions if the answers are not already explicit:

1. **Delivery mode**
   - Merge mode: all site outputs merge into one deduplicated `companies.csv` + `keys.txt`
   - Per-site mode: each site produces its own CSV, but day2+ still excludes already delivered records

2. **Email delivery policy**
   - Default: no filtering
   - Allowlist mode: keep only specified domains
   - Denylist mode: exclude specified domains

Also confirm where the code should run:
- local machine
- or the other machine registered in `AGENTS.md`

## 3. Environment self-check

At the start of substantial work:

1. Detect current machine from `AGENTS.md` and current OS.
2. Confirm whether the target run is local or remote.
3. Check whether required env/config exists:
   - `.env`
   - `LLM_API_KEY`
   - `LLM_BASE_URL`
   - `LLM_MODEL`
   - proxy settings (`7897` is the default outbound proxy port in this project)
4. If protocol exploration is needed, ensure the workflow follows the repository's crawler tooling expectations.

## 4. Trigger routing

### A. New country / new site from a website URL

Follow this route:
1. Capture requirements
2. Explore the site / protocol
3. Produce a structured exploration report
4. Wait for user confirmation
5. Implement crawler + pipelines
6. Run smoke tests
7. Integrate delivery
8. Commit/push only when user explicitly asks for git actions in this session

### B. Data import from Excel/CSV

Follow this route:
1. Inspect source columns
2. Decide which pipelines are still needed
3. Implement import route in the country/site structure
4. Run smoke tests
5. Integrate delivery
6. Commit/push only when user explicitly asks for git actions in this session

## 5. Site exploration requirements

When exploring a site, determine:

- whether these fields are available and where:
  - `company_name`
  - `representative`
  - `emails`
  - `phone`
  - `website`
  - `address`
- coverage rate for representative/email/website/phone when the site exposes them directly
- full-coverage collection strategy:
  - region/category/letter segmentation
  - pagination strategy
  - single-query result limits
  - anti-bot behavior
- protocol details:
  - endpoint
  - method
  - required headers
  - required params
  - pagination params
  - response structure

Before implementation, output a structured exploration report with:
- field availability
- sampled coverage
- full-collection strategy
- anti-bot notes
- endpoint details
- P1/P2/P3 recommendation

Do not skip this report for a new site flow.

## 6. Required pipeline architecture

Use the standard 3-pipeline model unless the task is purely delivery-only:

- **P1**: site collection
- **P2**: Google Maps completion
- **P3**: protocol + LLM extraction for emails / representative

Required runtime behavior:

- Pipelines run in parallel, not fully serial.
- P2/P3 should continuously poll for new work from storage.
- P1 completion should not block P2/P3 from processing already discovered records.
- Each pipeline should keep its own retry / rate-limit logic.

## 7. Delivery gate and CSV schema

A record may be delivered only when all three are present:

- `company_name`
- `representative`
- `emails`

Missing any of the three means **do not deliver** the record.

Unified CSV column order:

```csv
company_name, representative, emails, website, phone, evidence_url
```

Rules:
- `emails` should use semicolon separation when multiple values exist
- do not add extra columns to delivery CSV
- deduplicate by normalized company name at delivery time
- daily delivery output must stay under `output/delivery/<Country>_dayNNN/`
- use root `product.py` for delivery entry, not a country-local replacement flow

## 8. Email handling rules

- During crawling/storage: save all discovered emails to SQLite
- During delivery: apply the user-selected filter policy
- Never silently hardcode a new email filtering rule

## 9. HTML -> Markdown -> LLM flow

Before sending website content to the LLM, follow this exact order:

1. Fetch raw HTML
2. Remove `script`, `style`, `img`, `svg`, `video`, `audio`, `canvas`, `iframe`, `noscript`
3. Convert to Markdown
4. Collapse excessive blank lines
5. Truncate a single page to **80,000 chars** using symmetric truncation
6. Truncate the final combined prompt to **272,000 chars**

## 10. Representative extraction rules

Only accept representative-like roles such as:
- CEO
- Managing Director
- Director
- Chairman
- Founder
- Owner
- Partner
- President
- Vice President
- Chief Officer

Reject low-authority or non-principal roles such as:
- Manager
- Coordinator
- Consultant
- Advisor
- Employee
- Assistant
- Secretary
- Accountant
- Receptionist
- Clerk
- Officer without a Chief prefix

Hard rules:
- The person name must appear verbatim in page content.
- Never infer a person name from the company name.
- Require an `evidence_quote`.
- After LLM extraction, validate in code that at least 50% of representative name tokens appear in `evidence_quote`.
- If validation fails, clear representative.

## 11. Google Maps protocol rules

Use the repository's Google Maps protocol approach, not the official API integration pattern.

Expected behavior:
- query via web/protocol crawling pattern
- use SOCKS5 proxy for the Google Maps protocol path when the project requires it
- score candidate matches by company/domain/location quality
- reject weak matches below the project threshold
- filter non-official domains such as social/wiki/directory sites

## 12. Domain cache rule for P3

When multiple companies share the same domain:

- the first task claims the domain work
- later tasks wait and reuse the cached result
- cache results in dedicated SQLite storage
- if no email is found, mark done and do not endlessly retry the same domain

## 13. Retry policy

For LLM 429 rate limits:
- do not consume retry budget
- wait and retry until success

For ordinary transient failures:
- use bounded exponential backoff

## 14. Resume / checkpoint rules

Resume support is mandatory.

Acceptable implementations include:
- page-based checkpointing
- queue/status tables
- status fields on company rows

Common requirements:
- restart must continue from saved progress
- avoid repeating completed work
- prefer SQLite WAL mode
- avoid fragile single-connection cross-thread designs
- recover stale `running` jobs back to pending when appropriate

## 15. Full coverage is the default goal

For P1 design, actively pursue full-site coverage using the best available strategy:
- segmentation
- recursive category expansion
- complete pagination
- multiple search keywords
- breaking through per-query result limits by narrowing segments

Do not stop at a shallow sample unless the user explicitly asks for sampling only.

## 16. Implementation structure expectations

For a new country, follow the repository's standard structure:

```text
{Country}/
  run.py
  requirements.txt
  src/{country}_crawler/
    delivery.py
    sites/{site_name}/
      cli.py
      client.py
      parser.py
      pipeline.py
      pipeline2_gmap.py
      pipeline3_email.py
      store.py
  output/
  tests/
```

For an existing country, add the new site under that country's `sites/` directory and reuse country-level delivery integration.

## 17. Smoke-test requirement

Do not stop at static code changes. Run relevant smoke checks for the changed path:

- P1 can collect and persist records
- P2 can fill website/phone when applicable
- P3 can produce valid representative/email results when applicable
- at least one complete record can satisfy the delivery gate when the target source makes that possible

If smoke tests fail, fix the cause before claiming the task is done.

## 18. Git / deployment behavior

Follow repository expectations from `AGENTS.md`, but do not create commits or push unless the user explicitly asks for git actions in the current session.

If the task includes deployment to the remote Windows machine, use the machine record in `AGENTS.md` as the source of truth.

## 19. Priority of truth

When this skill conflicts with stale assumptions, prefer current repository files in this order:
1. current code
2. root `AGENTS.md`
3. current task requirements from the user
4. this skill

## 20. Source mapping

This Codex skill is the Codex-native counterpart of the Claude Code command file:

- `.claude/commands/oldiron-crawler.md`

If behavior needs to be updated in the future, keep the Codex skill aligned with that project workflow while preserving Codex-native structure (`.agents/skills/<name>/SKILL.md`).
