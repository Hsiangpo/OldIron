# OldIronCrawler Implementation Plan

## Goal

Build a lightweight global official-website crawler that:

- loads one website list from `/websites`
- resumes from a per-file SQLite database under `/output/runtime`
- extracts company name and representative with shared-style `LLM` rules
- searches the active highest representative with Tavily + `LLM` when input company name exists
- extracts emails with shared-style rule logic
- writes a CSV under `/output/delivery`

## Task Breakdown

### Task 1. Project Skeleton

- create runtime package layout
- add interactive entrypoint
- add requirements and `.env.example`

### Task 2. Input Import

- support `txt/csv/xlsx`
- auto-detect website column
- preserve optional company-name column from structured files
- compute file fingerprint
- create one runtime database per input file

### Task 3. State Store

- store imported rows in input order
- deduplicate by website or domain
- support `pending/running/done/failed_temp/dropped`
- support queue-tail retry for `failed_temp`

### Task 4. Protocol Crawl Layer

- adapt shared `pc` crawler behavior into the new project
- fetch sitemap or homepage links
- fetch pages concurrently inside one website
- auto-use proxy when available

### Task 5. Extraction Layer

- email rule extraction
- representative/company-name `LLM` extraction
- active representative Tavily search loop
- representative top-5 page selection
- email all-matched-page selection
- weighted learned token storage

### Task 6. Runtime Orchestration

- website-level concurrency `64`
- page-level concurrency `8`
- terminal progress output in agreed format

### Task 7. Delivery

- output CSV in input order
- keep `company_name, representative, emails, searched_representative, phones, website`

### Active Representative Search

- Preserve optional `company_name` from CSV/XLSX inputs.
- When company name is present, run a Tavily-backed AI search loop in parallel with website extraction.
- Store and deliver `searched_representative`.
- Tavily failure must not fail the website crawl.

### Task 8. Validation

- run unit tests for import/store/core rule logic
- run at least one real crawl validation on live websites
