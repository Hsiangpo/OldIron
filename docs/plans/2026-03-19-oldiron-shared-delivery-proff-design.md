# OldIron Shared Delivery And Denmark Proff Design

## Goal

Introduce a real root-level `product.py` delivery entry, extract England and Denmark delivery logic into one shared Python delivery core, and start Denmark `proff` under a cleaner new-site structure that does not copy the older DNB/Virk layout.

## Why This Change

The repository currently has three structural problems:

1. The documented root delivery entry exists only as an example shell and is not the real system entry.
2. England and Denmark delivery logic are nearly duplicated.
3. New sites still risk repeating the old “copy one country, rename a few files, keep growing” pattern.

This design fixes the highest-value shared layer first: delivery. It also introduces Denmark `proff` as the first site implemented under the newer structure.

## Scope

### In Scope

- Replace the example root `Product.py` with a real lower-case `product.py`.
- Add a shared Python delivery core for country-level packaging.
- Add England and Denmark country delivery specs on top of the shared core.
- Keep legacy countries working through a temporary fallback path.
- Add Denmark `proff` crawler code under a cleaner site directory.
- Add Denmark `proff` resume/checkpoint storage.
- Update root and Denmark docs to match the new layout.
- Add a minimal Go backend skeleton under `VersatileBackend` so the concurrency direction is no longer only a README idea.

### Out Of Scope

- Rewriting old England/Denmark DNB runtime into the new site structure.
- Moving Firecrawl/GMap/Snov production traffic to Go in this same change.
- Solving deep Proff segmentation beyond the currently verified keyword route.

## Architecture

## 1. Shared Delivery Core

Add a root Python package under `shared/oldiron_core/delivery/`.

This core owns:

- day label parsing
- site output discovery
- current-record loading
- historical baseline reconstruction
- deduplication
- delta computation
- CSV / summary / keys writing

Country-specific behavior is provided by a small `DeliverySpec` object instead of copy-pasting entire delivery modules.

## 2. Country Specs

England and Denmark each expose a very thin `country_spec.py`.

The country spec defines:

- country name / delivery directory prefix
- suspicious-record filtering rules
- output directory behavior

The existing `england_crawler.delivery` and `denmark_crawler.delivery` modules become thin wrappers around the shared delivery engine so existing country tests and country-local scripts keep working.

## 3. Root Delivery Entry

The root `product.py` becomes the real entrypoint.

Behavior:

- for migrated countries such as England and Denmark, load the shared delivery path directly
- for non-migrated countries, temporarily fall back to the country-local `product.py`

This gives immediate consistency without forcing a full multi-country migration in one patch.

## 4. Denmark New-Site Structure

Denmark `proff` should not be placed beside the old site modules as another flat sibling with old patterns repeated again.

Instead:

- keep old sites (`dnb`, `virk`) untouched
- create `denmark_crawler/sites/proff/` as the first “new style” site package

This package owns:

- config
- models
- HTTP client
- SQLite store
- pipeline
- CLI

## 5. Denmark Proff Runtime Model

The first Proff implementation will use the verified search results page data embedded in `__NEXT_DATA__`.

Verified live facts on 2026-03-19:

- search page returns HTTP 200 through proxy `7897`
- `__NEXT_DATA__` is present
- `searchStore.companies` exposes `hits`, `pages`, `currentPage`
- each company row can directly contain `name`, `orgnr`, `email`, `homePage`, `phone`, `contactPerson`

Initial runtime strategy:

1. seed search tasks from a query list
2. fetch search pages
3. parse company rows from `__NEXT_DATA__`
4. dedupe by `orgnr`
5. directly emit final companies when `company_name + representative + email` are present
6. keep homepage/no-email rows in snapshot data for future enrichment

This means the first version already produces deliverable rows without needing GMap as a mandatory first step.

## 6. Proff Coverage Strategy

Because one raw query is capped at 400 pages, the crawler must not hardcode a single keyword forever.

The first version will support:

- default legal-form query list
- custom query-file input for future segmentation experiments
- page cap per query

That keeps the crawler operational now and makes deeper segmentation a planner problem later, not a parser rewrite.

## 7. Go Backend Direction

`VersatileBackend` currently documents an idea but does not contain a usable Go codebase.

This change should add a minimal Go service skeleton:

- `go.mod`
- `cmd/<service>/main.go`
- `internal/...`

The goal is not to move traffic immediately. The goal is to make the “concurrent backend must be Go” rule real in repository structure and future integration points.

## Data Flow

### Root Delivery

`python product.py England day4`

-> root entry resolves country
-> shared delivery engine loads England spec
-> engine reads all England site outputs
-> engine reconstructs baseline
-> engine deduplicates by company name/domain key
-> engine writes `England/output/delivery/England_day004/`

### Denmark Proff

`python run.py proff`

-> CLI loads config
-> pipeline seeds search tasks
-> client fetches Proff search pages
-> parser extracts company rows
-> store deduplicates by `orgnr`
-> store exports `companies.jsonl`, `companies_enriched.jsonl`, `companies_with_emails.jsonl`, `final_companies.jsonl`
-> root `product.py Denmark dayN` merges `dnb + virk + proff`

## Error Handling

- Root delivery keeps the current locked-directory safe overwrite behavior.
- Proff search tasks use SQLite-backed task states for resume.
- Search page failures are retried through task requeue rather than losing the page forever.
- Proxy defaults to `127.0.0.1:7897` but remains configurable.

## Testing

- Keep existing England and Denmark delivery tests passing through the wrapper modules.
- Add tests for root delivery entry routing.
- Add Denmark Proff parser/store/dispatch tests.
- Run focused country suites for modified areas.

## Migration Notes

- Old country-local `product.py` scripts remain for compatibility.
- Root `product.py` becomes the documented default immediately.
- Legacy countries can be moved one by one into the shared delivery core later.
