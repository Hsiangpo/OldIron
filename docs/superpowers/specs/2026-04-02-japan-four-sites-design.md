# Japan Four New Sites Design

## Scope

Add four new Japan sites under `Japan/src/japan_crawler/sites/`:

- `mynavi`
- `pasonacareer`
- `openwork`
- `onecareer`

Japan keeps per-site day delivery and personal-email-only delivery policy.

## Site Classification

### Job-to-company sites

- `mynavi`
- `pasonacareer`

These sites are treated as job listing sources. P1 extracts company clues from search/list pages and job detail pages, then deduplicates to company-level records.

### Company-directory sites

- `openwork`
- `onecareer`

These sites are treated as company listing sources. P1 extracts company detail pages directly.

## Pipeline Model

All four sites follow the same runtime model:

1. `P1`
   - Crawl site list/detail pages
   - Write company-level rows into SQLite
   - Save source detail URL for traceability
2. `P2`
   - Only for rows with empty `website`
   - Use Google Maps to enrich missing official site URL
3. `P3`
   - Crawl official site through protocol crawler
   - Rule-first email extraction
   - LLM fallback for representative/email/company-name recovery

Pipelines run in parallel. P2/P3 poll storage and consume newly discovered rows continuously.

## Per-Site Field Strategy

### mynavi

- Main path: search/list page -> job detail page
- P1 target fields:
  - `company_name`
  - `website`
  - `emails` when directly visible
  - `source_job_url`
- Representative mainly comes from official site in P3

### pasonacareer

- Main path: search result pagination -> job detail page
- Explicitly do not use `/company/<id>/` as primary path because of JS/robot verification
- P1 target fields:
  - `company_name`
  - `website`
  - `address`
  - `source_job_url`
- Representative and emails come from official site in P3

### openwork

- Main path: company list page -> company detail page
- P1 target fields:
  - `company_name`
  - `representative`
  - `website`
  - `address`
  - optional public company metadata when cheap to capture
- P3 mainly fills emails

### onecareer

- Main path: company category page pagination -> company detail page
- P1 target fields:
  - `company_name`
  - `representative`
  - `website`
  - `address`
- P3 mainly fills emails

## Storage Strategy

Each new site owns its own SQLite store under its site output directory.

Recommended common fields:

- `company_name`
- `representative`
- `website`
- `address`
- `industry`
- `phone`
- `emails`
- `detail_url`
- `source_job_url`
- `gmap_status`
- `email_status`

Deduplication strategy:

- Prefer company-level dedupe within each site
- For job-to-company sites, dedupe by:
  - normalized `company_name + website`
  - fallback to normalized `company_name + address`

## Integration Points

### `Japan/run.py`

Add dispatch branches and help text for:

- `mynavi`
- `pasonacareer`
- `openwork`
- `onecareer`

### `Japan/src/japan_crawler/delivery.py`

Add site loaders for the four new site DBs so `product.py Japan dayN` includes them in per-site packaging.

## Validation

Before completion:

- unit tests for parser/store/client behavior
- run dispatch tests for new site names
- delivery test coverage updated if delivery integration changes
- at least one real crawl validation per newly added site
