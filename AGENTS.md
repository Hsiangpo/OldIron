# Repository Guidelines

## Project Structure & Module Organization

This repository is a multi-country company data collection workspace. Each country folder is a mostly independent project with its own runtime, output, and delivery flow: `England/`, `SouthKorea/`, `Japan/`, `Indonesia/`, `Malaysia/`, `Thailand/`, and `India/`. In each country project, keep source code under `src/`, tests under `tests/` or `test/`, docs under `docs/`, runtime artifacts under `output/`, and entry scripts such as `run.py` and `product.py` at the project root.

## Build, Test, and Development Commands

There is no single root build step. Work inside the target country directory.

- `cd England && python -m pip install -r requirements.txt`: install England dependencies.
- `cd England && python run.py dnb`: run the England DNB pipeline.
- `cd England && python run.py companies-house`: run the England Companies House pipeline.
- `cd England && python product.py day2`: build a daily delivery package.
- `cd Japan && python -m pytest test -v`: run Japan tests.
- `cd Thailand && pytest tests -q`: run Thailand tests.

## Coding Style & Naming Conventions

Use Python 3.10+ with 4-space indentation. Use `snake_case` for functions, variables, and modules, and `PascalCase` for classes. Keep functions under 200 lines and files under 1000 lines. All files must be UTF-8. Code comments should be in Chinese. Keep country-specific logic inside the matching country folder; shared patterns should be copied deliberately, not hidden in ad hoc cross-country imports.

## Testing Guidelines

Match the existing test runner in the target project: England mainly uses `unittest`, while Japan, Malaysia, and Thailand use `pytest`. Name tests `test_*.py`. Add or update tests whenever parsing, deduplication, checkpoint, delivery, or email extraction logic changes. Run the relevant country suite before submitting changes.

## Commit & Pull Request Guidelines

This root folder is a coordination workspace; some country folders have their own Git history. Where Git is used, current history already follows short Conventional Commit subjects such as `feat: add manager enrichment via firecrawl and llm fallback`. Keep using `feat:`, `fix:`, `refactor:`, and `docs:`. PRs should state the target country, affected pipeline, sample command used for verification, and any output or schema changes.

## Security & Configuration Tips

Do not commit `.env`, cookies, API keys, or anything under `output/`. Keep credentials isolated per country project. When adding a new country or source, document the entry command, required env vars, output path, and delivery format in that country’s `README.md` or `docs/`.

## Remote Machines

The first non-local Mac used for this workspace is `macbook-air-england`:

- Host: `192.168.0.103`
- User: `Zhuanz1`
- Password：`520526`
- Role: secondary execution machine for distributed runs and result collection

Do not store plaintext passwords, cookies, API keys, or other secrets in `AGENTS.md`. Use SSH keys or a local credential store instead.
