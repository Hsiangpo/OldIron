# Denmark Virk Design

## Goal

Add a new Denmark data source based on `datacvr.virk.dk`, with a direct protocol-first pipeline:

- Pipeline 1: Virk search + detail APIs
- Pipeline 2: Google Maps only for companies still missing email
- Pipeline 3: Firecrawl + LLM only for companies still missing email after GMap

The main rule is:

- If pipeline 1 already has `company_name + representative + email`, the company is immediately deliverable.
- Do not send such records to GMap or Firecrawl.

## Why A New Route

The Virk site exposes far more structured fields than DNB:

- company name
- CVR number
- phone
- email
- status
- company form
- legal owner
- person/management data

This makes a DNB-style website-first pipeline wasteful.

## Confirmed API Surface

- Search list:
  - `POST /gateway/soeg/fritekst`
- Company detail:
  - `GET /gateway/virksomhed/hentVirksomhed?cvrnummer=...&locale=da`

The site is behind Cloudflare, so runtime must reuse cookies from the user’s 9222 browser.

## Field Strategy

### Search API

Primary fields available directly in list rows:

- `senesteNavn`
- `cvr`
- `email`
- `telefonnummer`
- `status`
- `virksomhedsform`
- `beliggenhedsadresse`
- `hovedbranche`

### Detail API

Primary fields available in detail response:

- `personkreds.personkredser[*].personRoller[*].senesteNavn`
- `personkreds.personkredser[*].rolleTekstnogle`
- `ejerforhold.aktiveLegaleEjere[*].senesteNavn`
- `produktionsenheder.aktiveProduktionsenheder[*].stamdata.email`
- `produktionsenheder.aktiveProduktionsenheder[*].stamdata.telefon`

### Representative Selection

Representative priority:

1. natural person from `personkreds` in leadership roles
2. fallback from registration history when leadership link text exists
3. if still missing, leave empty and do not force GMap/Firecrawl to fix it

### Website Strategy

No stable website field is currently visible in Virk response.

So:

- if email already exists, skip website enrichment
- if email is missing, try GMap for website
- if GMap finds website, run Firecrawl

## Storage And Output

Add a new Denmark site output folder:

- `Denmark/output/virk`

Snapshots should match current Denmark delivery expectations:

- `companies.jsonl`
- `companies_enriched.jsonl`
- `companies_with_emails.jsonl`
- `final_companies.jsonl`

Delivery remains:

- `python product.py dayN`
- rule: `company_name + ceo + emails`

## Runtime Model

- single-machine first
- same-pipeline concurrency
- multi-pipeline parallelism
- resume from sqlite

Distributed sharding can be added later after protocol pipeline is stable.
