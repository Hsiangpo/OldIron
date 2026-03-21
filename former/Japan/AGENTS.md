# Repository Guidelines

## Project Structure & Module Organization
- `src/` contains the Python packages: `web_agent/` (UI + job runner), `site_agent/` (site extraction), `gmap_agent/` (Google Maps), `corp_agent/` and related utilities.
- `test/` holds pytest tests (naming pattern: `test_*.py`).
- `docs/` includes working notes and handoff reports.
- `output/` is runtime data: `web_jobs/` for job runs, `delivery/` for exported CSVs, and caches under `output/cache/`.

## Build, Test, and Development Commands
- `pip install -r requirements.txt` installs runtime dependencies.
- `pip install -e .` installs the package in editable mode for local development.
- `python -m web_agent <prefecture>` runs the end-to-end flow (registry + site enrichment). Example: `python -m web_agent Tokyo`.
- `python -m site_agent --input docs/websites.csv --concurrency 16` runs the site extractor only.
- `python -m gmap_agent --query "official site" --concurrency 16` runs Google Maps discovery only.
- `python -m pytest test -v` runs the test suite.

## Coding Style & Naming Conventions
- Python 3.10+, 4-space indentation, `snake_case` for functions/variables, `PascalCase` for classes.
- Keep output filenames stable (e.g., `output.success.csv`, `output.partial.csv`, `checkpoint.json`).
- Prefer small, composable helpers in `site_agent/` over large monolithic functions.

## Testing Guidelines
- Framework: pytest.
- Unit tests live in `test/` and should target parsing, caching, and strategy logic.
- Use `test_*.py` and aim to cover any new extraction rules or error handling changes.

## Commit & Pull Request Guidelines
- Git history does not show a strict convention; use Conventional Commits (`feat:`, `fix:`, `chore:`) going forward.
- PRs should describe the change, link related issues (if any), and include log snippets or CSV examples when behavior changes.

## Configuration & Security
- Required: `LLM_API_KEY` (or `docs/llm_key.txt` first line).
- Optional Snov extension vars: `SNOV_EXTENSION_SELECTOR`, `SNOV_EXTENSION_TOKEN`, `SNOV_EXTENSION_CDP_PORT`.
- Do not commit secrets or files under `output/`.
