# Denmark DNB Design

## Goal

Build a Denmark project that follows the England execution model for DNB collection:

- Pipeline 1: DNB list/detail collects company name, key principal, address, phone, and website
- Pipeline 2: Google Maps fills missing website and phone when DNB detail has no reliable website
- Pipeline 3: Firecrawl + LLM extracts emails from company websites
- Delivery rule: only records with company name + key principal + email are deliverable

## Scope

This first implementation targets `Denmark/` only. It must include:

- standalone project structure
- DNB runtime
- distributed shard planning / bootstrap / merge for DNB
- `product.py dayN` delivery packaging

It must not include England-only `companies-house` or old multi-machine coordinator code.

## Reuse Strategy

Reuse England DNB architecture because the site structure is confirmed compatible:

- Denmark DNB country code: `dk`
- list pages and detail pages follow the same URL pattern as England
- detail pages expose the same core fields: `Website`, `Key Principal`, `Address`, `Phone`

The implementation will copy and rename the England DNB stack, then strip UK-only pieces.

## Data Flow

1. `run.py dnb`
   loads `.env`, refreshes DNB cookie from 9222 browser, validates Firecrawl + LLM config, and starts the pipeline
2. DNB discovery queue
   seeds root industry segments for Denmark and walks list pages
3. DNB detail queue
   parses company profiles and writes normalized company records into sqlite
4. Google Maps queue
   runs only for companies missing reliable website / phone
5. Firecrawl queue
   runs only for companies with website/domain candidates
6. snapshot export
   continuously writes `companies.jsonl`, `companies_enriched.jsonl`, `companies_with_emails.jsonl`, and `final_companies.jsonl`
7. distributed flow
   supports `plan-dnb`, `bootstrap-dnb`, `merge-site`
8. delivery flow
   `product.py dayN` reads merged output and writes daily delivery bundle

## Delivery Rules

Same as England:

- `day1` creates first package
- `dayN` uses `day(N-1)` as baseline
- only records with company name + key principal + email are exported
- output contains `companies.csv`, `keys.txt`, `summary.json`

For Denmark first version, suspicious foreign-domain filtering will stay conservative and minimal to avoid over-filtering valid records.

## Execution Model

- single-machine execution first
- same-pipeline concurrency
- multi-pipeline parallelism
- static shard split for multi-machine follow-up
- host-side merge before final `product.py`

## Risks

- England code contains hardcoded UK strings, `gb`, and `United Kingdom`
- England distributed module mixes DNB and Companies House logic
- some utility imports still point to `snov.client.extract_domain`

## Decisions

- build `Denmark` as an independent project, not as an import wrapper around `England`
- keep only DNB-related modules plus shared runtime utilities
- simplify distributed module to DNB-only for Denmark first version
