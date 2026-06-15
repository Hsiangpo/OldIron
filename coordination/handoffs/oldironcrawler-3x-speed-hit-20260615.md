# OldIronCrawler Brazil Speed/Hit Handoff

Task: `oldironcrawler-3x-speed-hit-20260615`

Status as of 2026-06-15:
- Desktop pre-change Brazil output: 69/121 email hits, about 639s wall time.
- Best accepted current validation: 70/121 email hits, about 234s wall time, run dir `OldIronCrawler/tmp/brazil_full_validation_ai01_20260615_151140`.
- Unit suite passed: `390 passed`.
- Package rebuilt and copied to `C:\Users\Administrator\Desktop\OldIronCrawler`; desktop Excel files were preserved.

Implemented and kept:
- Brazil/Turkey/Japan value page keyword and common probe improvements from the prior task state.
- AI email still starts, but main merge wait is capped at 0.1s to avoid long stalls.
- Protocol fallback requests now inherit page batch deadlines.
- Large non-shell pages avoid full BeautifulSoup parsing.
- Packaged `.env` forces runtime concurrency fields to 32 even if local `.env` has higher values.
- `protocol_client.py` was reduced under the 1000-line gate by moving sitemap fetch text logic to `extractor/protocol/sitemap.py`.

Tried and rejected:
- Reducing common probe batch/target to 4: speed improved slightly but hits dropped to 64/121.
- Skipping extra fetches when homepage already had enough emails: no stable speed gain and did not improve validation.
- Bypassing shared page pool for 1-2 target URLs: worsened wall time in Brazil validation.

Remaining bottleneck:
- Full-run target page fetching can still show 60s+ outliers even when the same site is fast alone. Example: `shoppinguberaba.com.br` single-site trace was about 1.5s, but full-run fetch stage reached 135s. This points to global request/fetch pool contention and lingering slow network tasks rather than country-specific URL scoring.
