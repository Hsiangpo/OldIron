# England Germany Wiza Websites Design

**Goal:** Add website-only Wiza list collection for England and Germany, and support `python product.py <Country> websites dayN` website delivery.

## Scope

- `Germany/wiza` becomes a list-only site in practice.
- `England` adds a new `wiza` site.
- Both countries only keep non-empty website URLs from Wiza list results.
- Website deduplication uses the full normalized URL string, not domain folding.
- Delivery adds a separate websites day package and does not change existing company day delivery semantics.

## Runtime Behavior

- `python run.py wiza`
- `python run.py wiza list`

Both commands execute Pipeline 1 list collection only.

- No detail crawling
- No GMap
- No P3/email pipeline

Each run keeps checkpoint state under `output/wiza/` and exports a flat website list to `output/wiza/websites.txt`.

## Country Filters

- Germany Wiza filter remains `HQ Location = Germany`
- England Wiza filter uses `HQ Location = United Kingdom`

The England site is stored under the `England` country directory, but the Wiza query scope is the whole United Kingdom.

## Data Shape

Crawler persistence remains checkpoint-friendly, but the user-facing output only cares about website URLs.

- Skip rows with empty website
- Normalize URLs with existing country website normalization helpers
- Deduplicate by normalized full URL
- Export one URL per line in sorted order

## Delivery

New command shape:

- `python product.py England websites dayN`
- `python product.py Germany websites dayN`

Separate day directories:

- `England/output/delivery/England_websites_day001/`
- `Germany/output/delivery/Germany_websites_day001/`

Files in each package:

- `websites.txt`
- `keys.txt`
- `summary.json`

Website day packages use their own baseline sequence and do not mix with company day packages.

## Testing

- Germany tests cover website-only filtering/export and new websites delivery
- England tests cover the new Wiza filter, website-only pipeline behavior, and websites delivery
- Root `product.py` tests cover the new three-argument websites entry path
