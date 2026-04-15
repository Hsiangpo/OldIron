# Global Collaboration Rules

## Communication

- Do not end responses with invitation-style phrases such as "如果你要", "如果你需要", "要不要我".
- Give the conclusion, the action taken, and the result directly.
- Explain things in plain language because the user is not highly technical. When technical terms are necessary, add a short plain explanation.
- Before doing substantial implementation, align on the desired effect when the requirement is ambiguous enough to cause rework.
- Do not assume one country's delivery policy, email policy, or data-quality policy applies to another country.
- For any new country, or any country whose strategy is not already explicitly written in `AGENTS.md`, confirm the country-specific strategy with the user before implementation.

## Code Limits & CI Gates (protocol-crawler 门禁)

- 严格遵守 `protocol-crawler` 技能的 CI 门禁规范：
  - Backend functions must stay within 200 lines. (单函数不超过 200 行，超过必须拆分)
  - Backend source files must stay within 1000 lines. (单文件不超过 1000 行，超过必须拆分为多模块)
  - A single directory should normally stay within 10 files. Split by domain when it grows beyond that.
- 严禁代码带版本后缀（如 `_v2`, `_old`），旧方案被替换后必须干净彻底地上演“物理删除”，禁止通过注释屏蔽来堆积屎山代码。

## Encoding And Language

- All files must use UTF-8.
- Code comments must be written in Chinese.
- User-facing frontend copy must be Chinese.
- Technical docs such as `AGENTS.md`, internal design notes, and interface docs should preferably be written in English.
- User-facing docs such as `README.md` and `PRD.md` may stay in Chinese when that is easier for the user.

## Network And Secrets

- For non-China outbound access, use proxy port `7897` by default. If that port is unavailable, probe the actual working outbound proxy port first.
- Do not commit `.env`, cookies, API keys, or anything under `output/`.
- LLM provider switching rule:
  - this repo may switch between approved LLM providers over time; do not hardcode the assumption that only one provider exists
  - the active provider must be recorded in both `AGENTS.md` and the relevant skill doc when the user explicitly confirms a repo-wide switch
  - API keys must stay only in local `.env` files; never write LLM API keys into tracked docs, code comments, commits, or delivery outputs
  - current user-confirmed repo-wide provider target is:
    - `LLM_BASE_URL=https://gpt-agent.cc/v1`
    - `LLM_MODEL=claude-sonnet-4-6`
    - `LLM_REASONING_EFFORT=` (empty)
    - `LLM_API_STYLE=auto`
  - live validation status:
    - the provider path is connected in code
    - UnitedStates local `.env` was switched and returned a real success payload
    - Brazil local `.env` was switched and returned a real success payload
  - do not treat committed docs, `.env.example`, README snippets, or code fallback defaults as the source of truth for the current runtime provider
  - for a machine that is actually running crawler processes, the source of truth is:
    - the user's latest explicit provider-switch instruction
    - that machine's current local country `.env`
  - if docs/examples/defaults disagree, stop claiming a repo-wide active provider until the user confirms the intended one
  - current active provider on this repo is:
    - `LLM_BASE_URL=https://gpt-agent.cc/v1`
    - `LLM_MODEL=claude-sonnet-4-6`
    - `LLM_REASONING_EFFORT=`
    - `LLM_API_STYLE=chat`
  - after a provider switch, update all affected runtime `.env` files on every runtime machine before resuming processes
- Cross-machine sync rule:
  - Code, docs, tests, and normal source changes must be synced by Git only.
  - `.env` files, SQLite databases, checkpoint/state files, and delivery outputs may be synced by SSH/scp because they are untracked, large, sensitive, or are core runtime/delivery assets.
  - Do not use SSH/scp to sync normal code files, test files, docs, `tmp/`, cache directories, smoke-test artifacts, one-off debug scripts, or ad hoc full-sync packages as the default workflow.
  - Do not blindly wholesale sync `output/` to another machine. Sync the explicitly needed runtime databases, checkpoint/state files, and delivery outputs; do not treat the whole output tree as disposable.
  - `output/` is not a one-rule directory:
    - resumable runtime databases may be synced when needed
    - delivery outputs are core assets and should be synced when cross-machine continuity or archive completeness requires them
    - caches and debug artifacts must not be blindly synced or overwritten
  - SQLite transfer checklist:
    - stop the source site process before copying the database
    - copy the full SQLite state as one consistent set: main db + matching `-wal` + matching `-shm` when they exist
    - do not copy only the main `.db` file when a live WAL pair exists
    - after transfer, verify file size / timestamp / openability on the target machine before resume or delivery
    - if the database is delivery-critical or resume-critical, run at least `PRAGMA quick_check` or `PRAGMA integrity_check`
  - Root-cause rule for malformed SQLite after transfer:
    - do not assume SSH/scp itself is the default root cause
    - first check whether the source process was still writing
    - then check whether `.db`, `-wal`, and `-shm` were copied as a matching snapshot
    - then check whether the source database was already damaged before transfer

## Runtime Model

- The current execution model is multi-machine site-level execution with centralized merge.
- Do not reintroduce the old coordinator-based multi-machine cluster flow unless explicitly requested.
- Do not reintroduce shard-based multi-machine execution for new work.
- Multi-machine work should be done by assigning different sites (or different whole pipelines) to different machines, then pulling back site outputs and merging at the country level.
- Example: Mac runs `Denmark proff` + `Denmark virk`, Windows runs `England companyname` + `Finland tmt/duunitori/jobly`. This is the preferred model.

## Dual Codex Coordination Protocol

- This protocol applies whenever two or more Codex/AI agents may work in parallel without direct chat communication.
- Use a dual channel:
  - repo-local coordination files under `coordination/` for machine-readable real-time state
  - GitHub issue / PR updates for human-visible audit history
- Every substantial task must start with:
  1. `git pull`
  2. read `AGENTS.md`
  3. read `coordination/active_tasks.json`
  4. read `coordination/shared_locks.json`
  5. check whether the planned scope is already owned or locked
- Every task must be classified before implementation:
  - `site_local`: only touches one country/site-local scope and does not modify the high-risk shared zone
  - `shared_zone`: touches any high-risk shared zone path listed below
- Default ownership rule:
  - one active Codex owns one country/site scope at a time
  - do not let two active Codex agents edit the same site scope simultaneously
- Git push rule:
  - verified code changes should be committed and pushed directly to `main` by default
  - do not create or use a task branch unless the user explicitly asks for a branch, PR, or branch-based review flow
  - coordination still uses `coordination/active_tasks.json` and `coordination/shared_locks.json`; direct-to-`main` does not remove the lock/task requirements
- High-risk shared zone rule:
  - the following paths are high-risk shared zones:
    - `shared/`
    - repo-root `product.py`
    - repo-root `AGENTS.md`
    - repo-root `README.md`
    - `.github/`
    - `coordination/`
    - any `<Country>/shared/`
    - any `<Country>/src/*/delivery.py`
  - shared-zone work uses a lease lock, not an indefinite lock
  - before editing any high-risk shared zone, the agent must:
    1. register or update its task in `coordination/active_tasks.json`
    2. claim the exact path(s) in `coordination/shared_locks.json`
    3. set `expires_at` and `heartbeat_at` on the lock entry
    4. record the related GitHub issue/PR reference in the task/lock entry
    5. push the lock state to the remote before editing the shared-zone files
- Site-local changes outside the high-risk shared zone still require an active task entry, but do not require a shared lock.
- For site-local work:
  - update `coordination/active_tasks.json`
  - work directly on `main` by default
  - after verification, commit and push directly to `main`
  - a separate shared lock is not required
- If a planned path is already locked by another active task, do not edit it. Stop, sync the latest state, and ask the user to reassign or sequence the work.
- Keep lock scope small. Lock exact files or narrow directories; do not lock an entire country unless the whole country truly needs exclusive ownership.
- Lease expiry rule:
  - shared locks must always include `expires_at`
  - refresh `heartbeat_at` while the shared-zone task is still active
  - if a lock is expired and there has been no recent heartbeat, another agent may take over after pulling latest state and writing a takeover note
- Release rule:
  - after push or when handing work back, update `coordination/active_tasks.json`
  - release any shared lock in `coordination/shared_locks.json`
  - if the work is partial, add a handoff note under `coordination/handoffs/`
  - shared lock release should travel with the completion push; do not leave an already-finished shared task locked on the remote
- Preferred tooling:
  - use `python coordination/coord_cli.py begin ...` to create task entries
  - use `python coordination/coord_cli.py heartbeat ...` to refresh a shared lease
  - use `python coordination/coord_cli.py finish ...` to complete a task and release locks
  - use `python coordination/coord_cli.py takeover ...` only when a shared lock is expired and takeover is justified
  - use `python coordination/preflight.py ...` before starting a task when you want a fast conflict check
  - use `python coordination/lease_doctor.py` to inspect expired locks, orphan locks, or malformed shared-zone state
- Machine roles in the `Machines` section are default runtime responsibilities, not permanent exclusive development locks. Real-time ownership is defined by the coordination files and current task assignment.

## Country Delivery Rules

- Daily delivery files use a unified entry at the project root. Do not use `<Country>/product.py` directly for execution.
- Run command example: `python product.py England dayN` (root script delegates to the respective country module).
- 默认交付规范：单个国家，所有站点，通过 `product.py` 打包落盘的时候，**默认**将所有站点的输出数据进行归并，按公司名严格**去重后才落盘生成最终交付文档**。
- 文件存放规范：生成的最终交付文档必须放在各个国家内部的 `output/delivery/<国家英文名>_dayN(从001开始)` 文件夹下。例如：`SouthKorea/output/delivery/SouthKorea_day001/`。
- Global website email rule:
  - for every country, if an email is a real email found from the company's official website, keep it
  - do not filter by mailbox type; personal/free mailbox domains and corporate mailbox domains are both allowed
  - do not cap the number of kept emails
  - only remove clearly fake / invalid / placeholder emails, or obvious directory-style noise that does not belong to the company itself
  - website email extraction is rule-based only for all countries
  - if only emails are missing, run rule email extraction only; do not use LLM for emails
  - if only representative is missing, use LLM for representative only
  - if both emails and representative are missing, run rule email extraction for emails and use LLM for representative only
  - when extracting emails by rules, do not truncate page content for the rule path
  - truncate Markdown / prompt content only for the content that is actually sent to LLM
- Country-specific overrides always win over the default delivery rule.
- Do not extrapolate these overrides to future countries by yourself.
- For future countries, if delivery mode or email policy is not explicitly recorded here, ask the user first and write the confirmed strategy into `AGENTS.md` before implementing.
- Do not run `python product.py <Country> dayN` unless the user has explicitly requested or approved that delivery run in the current task.
- Do not delete existing delivery files or delivery directories unless the user has explicitly requested that deletion.
- When re-running the same day delivery and replacing an existing day directory, do not hard-delete it. Move the old day directory to the OS recycle bin / trash first, then build the new delivery directory.
- The same recycle-bin rule also applies to manual operator actions. When replacing an existing day package, move the old day directory to recycle bin / trash instead of physical deletion.
- Country-specific delivery overrides:
  - `Japan`: per-site day delivery. Write one CSV + one keys file per site under `Japan/output/delivery/Japan_dayNNN/`. Do not merge sites into one country-level `companies.csv`.
  - `Brazil`: per-site day delivery. Write one CSV + one keys file per site under `Brazil/output/delivery/Brazil_dayNNN/`. Do not merge sites into one country-level `companies.csv`.
  - `Germany`: per-site day delivery. Write one CSV + one keys file per site under `Germany/output/delivery/Germany_dayNNN/`. Do not merge sites into one country-level `companies.csv`.
  - `UnitedStates`: per-site day delivery. Write one CSV + one keys file per site under `UnitedStates/output/delivery/UnitedStates_dayNNN/`. Do not merge sites into one country-level `companies.csv`.
  - `UnitedArabEmirates`: per-site day delivery. Write one CSV + one keys file per site under `UnitedArabEmirates/output/delivery/UnitedArabEmirates_dayNNN/`. Do not merge sites into one country-level `companies.csv`.
- Country-specific source overrides:
  - `Germany`: delivery keeps one CSV + one keys file per site. Same-site dedupe uses `company_name` only.
  - `Germany`: cross-site duplicates are allowed to appear in different site delivery files.
  - `Germany`: `wiza` 的 `P1` 不抓站内联系人，代表人只来自 `P3` 官网 LLM。
  - `UnitedArabEmirates`: delivery keeps one CSV + one keys file per site. Same-site dedupe uses `company_name` only.
  - `UnitedArabEmirates`: cross-site duplicates are allowed to appear in different site delivery files.
  - `UnitedArabEmirates`: representative output keeps `P1;P3` order. If `P1` is empty, keep only `P3`.
  - `UnitedArabEmirates`: `P1` representative comes from site-native contact/contact-person fields when present, but output should keep only person names.
  - `UnitedArabEmirates`: delivery gate is country-specific. A record is deliverable when `company_name` and `website` are present and the post-list pipelines have finished for that record (`gmap_status='done'` and `email_status='done'`). Representative and emails may be empty for delivery.
- Japan multi-machine same-day rule:
  - different machines may run the same `Japan dayN` only when the split is by site ownership
  - never let two machines produce the same Japan site package for the same `dayN`
  - one designated machine must be the final Japan day assembler
  - merge only per-site delivery assets from the other machine: `<site>.csv` + `<site>.keys.txt`
  - do not copy the other machine's `summary.json` as the final summary
  - regenerate the final `summary.json` on the designated assembler after all site packages are collected
  - before copying Japan delivery assets from another machine, stop the corresponding remote Japan site processes so the copied day package is complete and stable
  - `England`: representative source is Companies House `officers` pages, using current officers only. Join multiple names with semicolons in page order. Prefer human names; if no human names exist, use current company-entity officers.
  - `England`: website email extraction is rule-based only. Do not use the website LLM path for England representative extraction or England website email extraction.
- Never package delivery site by site when multiple sites belong to the same country, unless that country is explicitly listed in the country-specific overrides above.
- If the same country is being run on multiple machines, the split must be by site or by whole pipeline, not by shard.

### England DayN Flow

1. Stop active England site processes that affect delivery on every machine.
2. Pull back the remote England site outputs to the local machine.
3. Merge the England site outputs back into one England country output tree.
4. Run `python product.py England dayN` from the project root.
5. The final day package must be deduplicated by company name across all England sites together.

### Denmark DayN Flow

1. Stop active Denmark site processes that affect delivery on every machine.
2. Pull back the remote Denmark site outputs to the local machine.
3. Merge Denmark site outputs into one Denmark country output tree.
4. Run `python product.py Denmark dayN` from the project root.
5. The final day package must be deduplicated by company name across all Denmark sites together.

### Finland DayN Flow

1. Stop active Finland site processes on the machine running them.
2. If Finland runs on a remote machine, pull back the Finland site outputs to the local machine.
3. Merge Finland site outputs into one Finland country output tree.
4. Run `python product.py Finland dayN` from the project root.
5. The final day package must be deduplicated by company name across all Finland sites (TMT + Duunitori + Jobly) together.

### Japan Same-Day Multi-Machine Flow

1. Split Japan strictly by site ownership before running `dayN`.
2. Each machine may run `python product.py Japan dayN` only for the Japan sites it owns.
3. Never allow two machines to generate the same Japan site package for the same `dayN`.
4. Choose one machine as the final `Japan_dayNNN` assembler.
5. On the non-assembler machine, stop the Japan site processes for the owned sites before copying out the delivery assets.
6. Copy only the owned site delivery files to the assembler:
   - `<site>.csv`
   - `<site>.keys.txt`
7. Do not copy or keep the other machine's `summary.json` as the final summary.
8. On the assembler machine, rebuild the final `summary.json` after all Japan site packages are present under the same `Japan_dayNNN/`.
9. Treat the assembler's `Japan_dayNNN/` as the only final authoritative day package.

## Temporary Artifact Cleanup & File Organization

- **注意文件规范，绝不能随意堆放文件**。产生的任何调试请求响应、DevTools 截图、冒烟测试结果只能专门放在各目录的 `tmp/` 下，绝不允许散落在项目根目录。
- 必须做到**主动清理**：冒烟测试（Smoke-test）、一次性单元测试结束后，必须主动删除 `tmp_*`, `*_smoke*` 或其他生成的调试缓存物。
- Never delete active run directories, live checkpoint databases, or logs still needed for resume. (处于续跑状态的状态库、运行日志禁止删除)。
- Temporary artifacts are local-only by default. Do not distribute `tmp/`, cache directories, transient archives, or one-off debug files to other machines as part of normal sync.

## Project Structure And Module Organization

**核心架构原则 (Core Architecture Principle):** 所有国家、所有站点的具体网页爬虫（Crawler）抽取部分，必须使用 **Python** 编写；所有通用后端服务（Backend），尤其是涉及大并发、高吞吐的综合调度模块等，必须严格使用 **Go** 语言编写。

This repository is a multi-country company data collection workspace. Each country folder is a mostly independent project with its own runtime, output, and delivery flow. Additionally, there is a global generic backend for high concurrency generic tasks:

- `Denmark/`
- `Brazil/`
- `England/`
- `Germany/`
- `India/`
- `Indonesia/`
- `Japan/`
- `UnitedArabEmirates/`
- `Taiwan/`
- `UnitedStates/`
- `Malaysia/`
- `SouthKorea/`
- `Spain/`
- `Thailand/`
- `Turkey/`
- `VersatileBackend/` (Go-based generic backend for high-concurrency tasks like Firecrawl, Gmap, Snov)

Legacy implementations that are no longer the active development path must be archived under either:

- `<Country>/bak/` for country-local historical code and output
- `former/` for countries or modules that have not yet been migrated to the new framework

New development must not continue inside archived implementations. Archived code is reference-only.

In each country project, keep source code under `src/`, tests under `tests/` or `test/`, docs under `docs/`, runtime artifacts under `output/`, and entry scripts such as `run.py` and `product.py` at the project root.

## Build, Test, And Development Commands

There is no single root build step. Work inside the target country directory for scrapers, but run delivery from the root.

- `cd England && python -m pip install -r requirements.txt`
- `cd England && python run.py companyname`
- `python product.py England day1`
- `cd Germany && python -m pip install -r requirements.txt`
- `cd Germany && python run.py wiza`
- `python product.py Germany day1`
- `cd Brazil && python -m pip install -r requirements.txt`
- `cd Brazil && python run.py dnb`
- `python product.py Brazil day1`
- `cd Denmark && python -m pip install -r requirements.txt`
- `cd Denmark && python run.py proff`
- `cd Denmark && python run.py virk`
- `python product.py Denmark day1`
- `cd Finland && python -m pip install -r requirements.txt`
- `cd Finland && python run.py tmt`
- `cd Finland && python run.py duunitori`
- `cd Finland && python run.py jobly`
- `python product.py Finland day1`
- `cd Japan && python -m pip install -r requirements.txt`
- `cd Japan && python run.py bizmaps`
- `cd Japan && python run.py hellowork`
- `cd Japan && python run.py xlsximport`
- `python product.py Japan day1`
- `cd Taiwan && python -m pip install -r requirements.txt`
- `cd Taiwan && python run.py ieatpe`
- `python product.py Taiwan day1`
- `cd UnitedArabEmirates && python -m pip install -r requirements.txt`
- `cd UnitedArabEmirates && python run.py dubaibusinessdirectory`
- `cd UnitedArabEmirates && python run.py hidubai`
- `cd UnitedArabEmirates && python run.py dayofdubai`
- `cd UnitedArabEmirates && python run.py dubaibizdirectory`
- `cd UnitedArabEmirates && python run.py wiza`
- `python product.py UnitedArabEmirates day1`
- `cd UnitedStates && python -m pip install -r requirements.txt`
- `cd UnitedStates && python run.py dnb`
- `python product.py UnitedStates day1`
- `cd Japan && python -m pytest test -v`
- `cd Taiwan && python -m unittest tests -v`
- `cd UnitedStates && python -m unittest tests -v`
- `cd Thailand && pytest tests -q`

## Coding Style And Naming Conventions

- Use Python 3.10+ with 4-space indentation.
- Use `snake_case` for functions, variables, and modules.
- Use `PascalCase` for classes.
- Keep country-specific logic inside the matching country folder.
- Old country projects may keep local isolation for stability, but newly extracted reusable cores must follow this hard rule:
  - If a module is shared across different countries, it must live under `shared/` in an appropriate shared location such as `shared/oldiron_core/`.
  - If a module is shared only by multiple sites inside the same country, it must live under `<Country>/shared/`.
  - Never place reusable modules inside one country's site tree and then import/symlink/copy them into other countries.
  - Never create ad hoc cross-country imports or cross-site imports from one site's source tree into another site's source tree.
  - Never use symlinks as a shortcut for cross-country or cross-site sharing in active code.
  - If a shared-module cleanup claims to have removed cross-country/cross-site reuse, do not trust the first pass. Run independent audit passes first:
    - scan formal imports
    - scan `sys.path` injection and packaging entrypoints
    - scan symlinks and wrapper modules for reverse dependency
  - Only after those audit passes are clean may the cleanup be considered complete.
- `shared/oldiron_core/protocol_crawler/` is the shared protocol crawler module (curl_cffi-based site link discovery + HTML scraping). It replaces Firecrawl when `CRAWL_BACKEND=protocol` is set in a country's `.env`.
- New active work for rewritten countries/sites must target the new framework only. Do not extend archived code under `bak/`.

## Site Runtime Rules

- New sites must expose a single entry form: `python run.py <site>`.
- A site CLI must auto-manage the shared backends it depends on. Users should not need to manually start MyIP / Firecrawl / Gmap just to run a site normally.
- Shared backend manual commands may remain for debugging, but they are not the primary runtime model.
- `MyIP` is optional and site-strategy-specific. Do not force every site to use `MyIP`.
- A site should enable `MyIP` only when that site's anti-bot situation benefits from residential IP rotation.

## Testing Guidelines

- Match the existing test runner in the target project.
- England mainly uses `unittest`.
- Japan, Malaysia, and Thailand mainly use `pytest`.
- Name tests `test_*.py`.
- Add or update tests whenever parsing, deduplication, checkpoint, delivery, or email extraction logic changes.
- Run the relevant country suite before claiming completion.
- For crawler, queue/resume, and delivery changes, mock/unit tests are only supplementary evidence.
- Before claiming completion, you must also run at least one real validation using actual crawler tasks, actual runtime databases/checkpoints, or an actual delivery run approved by the user.
- Do not treat pure mock-data tests as final proof that a crawler or delivery change is safe.

## Documentation Rules

- When adding a new country, site, shard flow, delivery rule, or resume rule, update the relevant `README.md` or docs in the same change.
- Keep runtime commands, required env vars, output paths, and delivery behavior documented.
- When the execution model changes (for example shard -> site split, archived `bak/` policy, or `MyIP` strategy), update the root `AGENTS.md` and root `README.md` in the same change.

## Code Sync Rules

- Every code change must be committed and pushed to GitHub immediately after verification.
- This applies to code, tests, docs, coordination files, `AGENTS.md`, and skill files as well; do not leave verified tracked changes only in the local worktree.
- If the user explicitly asks for Git actions in the current task, treat `git add` + `git commit` + `git push` as required completion steps, not optional cleanup.
- Every time code changes are deployed or verified against a running site process, stop the old process first, then restart it on the new code. Never leave an old process running on stale code after a code change.
- Normal machine-to-machine code sync must use Git:
  - Mac pushes verified code to GitHub.
  - Other machines receive code changes via `git pull`.
  - Do not use SSH/scp to push normal code files, test files, temp scripts, or cache files to another machine as the default workflow.
- `coordination/` and `.github/` are normal repo files. They must stay in Git, not in ad hoc SSH/scp sync.
- Only the following untracked or special data may be synced by SSH/scp when needed:
  - `.env`
  - SQLite databases / checkpoint databases
  - delivery outputs under `output/delivery/`
  - other explicitly approved large or sensitive runtime state files
- Never use SSH/scp full-project overwrite as the routine sync mechanism.
- Never sync temporary files, cache files, smoke-test artifacts, or debug helper scripts to another machine as part of code deployment.
- When syncing non-git runtime files to another machine:
  1. stop the affected process first
  2. sync only the required files
  3. for SQLite, sync the complete snapshot set (`.db`, `-wal`, `-shm`) when those sidecar files exist
  4. verify file size / timestamp / openability on the target machine
  5. if the DB is critical for resume or delivery, run `PRAGMA quick_check` or `PRAGMA integrity_check`
  6. restart on the target machine only after verification
- Before editing a high-risk shared zone, sync the latest Git state first and then re-check `coordination/shared_locks.json`.
- England exception rule:
  - if Windows is the active England runtime machine, do not overwrite the Windows England database or England output tree from another machine unless the task explicitly says to do so
- After pushing, **all machines** must be updated to the latest code before restarting any process.
  - Machine 2 (Mac): `git pull`
  - Machine 1 (Windows): `git pull` on the E: drive project.
- Never run a process on stale code. If in doubt, `git pull` first.
- The `.env` files are not tracked by git. When `.env` changes, manually sync to all machines.

### Import Path Rule

- `run.py` and tests may adjust `sys.path` locally so Python can import `src/` and `shared/`.
- This is a local runtime/import rule only.
- It does not change the sync rule:
  - code distribution still uses Git
  - `.env` and resume databases still use SSH/scp when manual sync is required

## Machines

When a new machine joins the project, it **must** be registered here with at least:
- Machine number (e.g. Machine 3)
- OS
- User name
- LAN IP (note it may change)
- Project path
- Role (which country/site it runs)
- Password (for SSH automation)

### Machine 1 — Windows (secondary)

- IP: `192.168.0.102` (LAN, may change — verify before use)
- User: `Administrator`
- Password: `deadman`
- Project path: `E:\Develop\Masterpiece\Spider\Website\OldIron`
- Role: runs England CompanyName + Finland (TMT, Duunitori, Jobly) pipelines
- **Important**: cross-country shared modules must not rely on per-country symlinks or manual copy chains. Shared code belongs in `shared/`, and same-country multi-site shared code belongs in `<Country>/shared/`.

### Machine 2 — macOS (local, primary)

- User: `Zhuanz1`
- Project path: `/Users/Zhuanz1/Develop/Masterpiece/Spider/Website/OldIron`
- Role: primary development machine; runs Denmark Proff + Virk pipelines
- Also aliased as `macbook-air-england` on LAN (IP may change — verify before use).
