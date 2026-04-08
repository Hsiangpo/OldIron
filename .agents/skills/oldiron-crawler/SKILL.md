---
name: oldiron-crawler
description: "Use when working on OldIron multi-country company collection tasks such as adding a country, adding a site, importing Excel/CSV sources, or changing delivery and product.py flows."
---

# OldIron crawler skill for Codex

Use this skill for repository-local OldIron crawling work.

When this skill is activated, state clearly:

> I am using the `oldiron-crawler` skill for this OldIron task.

## 1. Core project rules

- New country/site crawler work must use **Python**.
- Do **not** introduce new Go backend dependencies for new country/site crawler implementations.
- Shared-module placement hard rule:
  - If a module is shared across different countries, place it under repo-level `shared/` in the appropriate shared location such as `shared/oldiron_core/`.
  - If a module is shared only by multiple sites inside one country, place it under `<Country>/shared/`.
  - Never put reusable modules inside one country's site tree and then import/symlink/copy them into another country.
  - Never create cross-country imports or cross-site imports from one site's source tree into another site's source tree.
- Do not copy or symlink shared business modules into country folders as a long-term architecture pattern.
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

Before writing delivery code, do not assume another country's strategy applies here.
If the current country's delivery mode or email policy is not already explicitly recorded in `AGENTS.md` or the user message, you must ask the user first before implementation.
Do not run `product.py` delivery packaging unless the user has explicitly asked for or approved that delivery run in the current task.
Do not delete existing delivery files unless the user has explicitly asked for that deletion.

Before writing delivery code, ask the user these two questions if the answers are not already explicit:

Before applying any generic delivery/email rule, check `AGENTS.md` for country-specific overrides. Country-specific overrides always beat the generic default.

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

Current country overrides already fixed by repository rule:
- `England`: representative comes from Companies House `officers` pages (current officers only, semicolon-joined). England website email extraction is rule-based only; do not use website LLM extraction for representative or email.
- `Japan`: per-site delivery
- `Japan`: multi-machine same-day delivery is allowed only by site split. Different machines may run the same `dayN` only when each machine owns different Japan sites. Never let two machines generate the same Japan site package for the same `dayN`. Final Japan `summary.json` must be regenerated on one designated assembler machine after collecting all per-site CSV/keys files.
- `Brazil`: per-site delivery
- `UnitedStates`: per-site delivery
- For future countries, never infer strategy from Japan/Brazil/UnitedStates/Denmark/England/Finland. Ask first, then record the confirmed override in `AGENTS.md`.

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
   - if the user has switched the approved LLM provider, update the local runtime `.env` first and verify a real API call before restarting crawler processes
   - do not infer the current provider from committed docs, `.env.example`, README snippets, or code fallback defaults
   - for a machine that is actually running, the source of truth is:
     - the user's latest explicit provider-switch instruction
     - that machine's current local country `.env`
   - if repository docs/examples/defaults disagree, do not present any one of them as the active provider until the user confirms the intended one
   - current active approved provider in this repo is:
     - `LLM_BASE_URL=https://gpt-agent.cc/v1`
     - `LLM_MODEL=claude-sonnet-4-6`
     - `LLM_REASONING_EFFORT=`
     - `LLM_API_STYLE=chat`
   - do not write `LLM_API_KEY` into tracked files; keep it only in local `.env`
4. If protocol exploration is needed, ensure the workflow follows the repository's crawler tooling expectations.
5. Read `coordination/active_tasks.json` and `coordination/shared_locks.json` before substantial edits.
6. Classify the task first:
   - `site_local` if it stays inside one country/site-local scope
   - `shared_zone` if it touches any high-risk shared zone in `AGENTS.md`
7. If the task is `site_local`, register the task and, unless the user explicitly asked for branch/PR flow, plan to commit and push the verified change directly to `main`.
8. If the task is `shared_zone`, register the task, claim a shared lease lock, set `expires_at` + `heartbeat_at`, and push the lock to the remote before editing the shared-zone files.
9. If another active task already owns or locks the same scope, stop and report the conflict instead of editing through it.
10. Prefer `python coordination/coord_cli.py ...` for begin / heartbeat / finish / takeover / render instead of hand-editing coordination JSON.
11. Prefer `python coordination/preflight.py ...` for a quick start check and `python coordination/lease_doctor.py` for stale-lock inspection before manually taking over an expired shared lock.
8. If SQLite databases are being moved across machines:
   - stop the source process first
   - sync `.db`, `-wal`, and `-shm` together when the sidecar files exist
   - verify file size / timestamp / openability on the target machine
   - for resume-critical or delivery-critical DBs, run at least `PRAGMA quick_check` or `PRAGMA integrity_check`
   - if the target DB shows `database disk image is malformed`, do not blame SSH/scp first; check live-copy timing, WAL completeness, and whether the source DB was already broken

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
8. If the user explicitly asks for git actions in this session, commit and push after verification; do not leave the verified change set only in the local worktree

### B. Data import from Excel/CSV

Follow this route:
1. Inspect source columns
2. Decide which pipelines are still needed
3. Implement import route in the country/site structure
4. Run smoke tests
5. Integrate delivery
6. If the user explicitly asks for git actions in this session, commit and push after verification; do not leave the verified change set only in the local worktree

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
- **P3**: protocol rule extraction for emails + LLM extraction for representative

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
- default delivery mode deduplicates by normalized company name at delivery time
- daily delivery output must stay under `output/delivery/<Country>_dayNNN/`
- use root `product.py` for delivery entry, not a country-local replacement flow
- if `AGENTS.md` declares a country-specific per-site delivery override, follow that override instead of the default merge+dedup rule
- if `AGENTS.md` declares a country-specific email delivery override, follow that override instead of the default/no-filter assumption
- if re-running the same day and replacing an existing day package, move the old day directory to recycle bin / trash first; do not hard-delete it

Japan multi-machine delivery checklist:
- split by site only; never split one Japan site across multiple machines
- different machines may each run `python product.py Japan dayN` for their owned Japan sites
- choose one final assembler machine for `Japan_dayNNN`
- copy only per-site delivery files from the other machine: `<site>.csv` and `<site>.keys.txt`
- do not copy the other machine's `summary.json` into the final package
- stop the remote Japan site processes before copying delivery files out
- regenerate the final Japan `summary.json` on the assembler after all site files are present

## 8. Email handling rules

- During crawling/storage: save all discovered emails to SQLite
- During delivery: apply the user-selected filter policy or the country-specific override from `AGENTS.md`
- For official company websites, keep every real email discovered from the site
- Do not filter by mailbox type: personal/free mailbox domains and corporate mailbox domains must all be kept when the email is real
- Do not cap the number of kept emails
- Only remove clearly fake / invalid / placeholder emails, or obvious directory-style noise that does not belong to the company itself
- Never silently hardcode a new email filtering rule
- Website email extraction is rule-based only; do not use LLM to extract website emails
- If only emails are missing, run rule email extraction only
- If only representative is missing, use LLM for representative only
- If both emails and representative are missing, use rule email extraction for emails and LLM for representative only

## 8A. Real validation requirement

- For crawler, pipeline, queue/resume, and delivery changes, mock tests are supplementary only.
- Before claiming success, run at least one real validation using actual crawler tasks, actual runtime databases/checkpoints, or an actual delivery run approved by the user.
- Do not treat pure mock-data tests as final validation for crawler correctness.

## 9. HTML -> Markdown -> LLM flow

Before sending website content to the LLM, follow this exact order:

- Rule-based email extraction must use full page content; do not truncate page content for the rule path
- Markdown truncation applies only to the content that is actually sent to LLM

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

When the user has explicitly asked for git actions in the current session:
- treat `git add` + `git commit` + `git push` as required completion steps
- do not stop after local edits or local verification
- do not report the task as complete while verified tracked changes are still only local
- for `shared_zone` tasks, push the lease lock first, then edit the shared files, then release the lock in the completion push
- for `site_local` tasks, direct-to-`main` is the default workflow; only use a task branch when the user explicitly asked for branch/PR flow

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
