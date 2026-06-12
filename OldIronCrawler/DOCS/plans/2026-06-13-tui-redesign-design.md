# OldIronCrawler TUI Redesign — Design

Date: 2026-06-13
Status: approved (user picked direction interactively)

## Goal

Replace the plain ASCII-box / monochrome console UI with a restrained, premium
"hairline" terminal UI: lots of whitespace, thin Unicode rules, a single warm-amber
accent color, arrow-key navigation with number shortcuts, and a live crawl view with
scrolling result cards plus a sticky bottom progress bar. No flashy widgets.

User-confirmed decisions:
- Visual direction: **hairline / minimalist** (open-right layout, single accent).
- Interaction: **arrow-key navigation (↑↓ + Enter) with 1–N digit shortcuts**.
- Scope: **both** the static menus AND the live crawl process.
- Implementation: **`rich`** (one pure-Python dep; bundles fine with PyInstaller).
- Accent color: **warm amber** (`#d6943b`), single source of truth in `ui/theme.py`.

## Why rich

The hard part is "scrolling result feed + sticky progress footer" during the crawl.
`rich.Live` does exactly this (footer pinned, `live.console.print()` scrolls cards above
it) and auto-handles VT enabling, color downsampling, terminal resize, and CJK width.
`textual` (full async app) is rejected: conflicts with the blocking pipeline and is more
than "简约/不花里胡哨" wants.

## Architecture: new `oldironcrawler/ui/` layer

| File | Responsibility |
|------|----------------|
| `ui/theme.py` | Single source of truth: accent, palette styles, glyphs (`❯ ● ✓ ✗ · › ─`). |
| `ui/console.py` | Shared singleton `rich.Console` + primitives: `clear_screen`, `hairline`, `wordmark`, `kv_block`. |
| `ui/key_input.py` | Cross-platform single-key reader → semantic keys (UP/DOWN/ENTER/ESC/digits). Win `msvcrt` solid; POSIX best-effort. |
| `ui/menu.py` | Reusable arrow-key menu. `MenuController` is a **pure** state machine (unit-tested); `run_menu` renders via `Live(screen=True)`. |
| `ui/crawl_view.py` | Live crawl view. Pure `reduce(model, event)` reducer (unit-tested) + `CrawlView` context manager driving `rich.Live`. Module-global active sink with plain-print fallback. |
| `ui/masked_prompt.py` | Themed masked Key entry (`*` echo), port of `console._read_masked_line`. |

### Crawl data flow (all main-thread, serialized — no threads needed)

`run_crawl_session` main loop → `reporter.print_site_result` / `print_progress_heartbeat`
(signatures unchanged) → `crawl_view.emit_site_result` / `emit_progress`. If a `CrawlView`
is active: update model, `live.console.print(card)` for the scrolling card, refresh sticky
footer (counters + bar). If not active (non-TTY, piped, CI): fall back to the original
plain `print`. `runner.py:515` flush-error print is routed to `emit_log` too.

`CrawlView` wraps only the `run_crawl_session(...)` call inside
`app._run_session_with_llm_recovery`. Setup lines (开始任务 / 运行预算 / LLM 就绪) print
before Live starts; "交付完成" prints after it stops — no Live corruption.

### Menu flow

`dashboard.py` becomes thin: each screen is a `MenuSpec` (title, subtitle, status rows,
items); `run_menu` returns the chosen value; existing handlers stay. Numeric settings and
Key entry use `masked_prompt` / a line prompt instead of a menu. The old `_render_panel`
and width/pad/wrap helpers are deleted (rich owns width/wrap/CJK).

## Theme tokens

accent `#d6943b`; `wordmark`=bold accent; `cursor`=bold accent; `hair`=grey37;
`label`=grey58; `value`=grey85; `value.strong`=bold white; `hint`=grey42;
`ready`=muted green `#6db073` (only ● ready / ✓ done); `fail`=muted red `#c96a6a`
(only ✗). One accent; green/red are tiny semantic glyphs only.

## Edge cases

- Non-TTY / no VT: rich auto-detects; crawl emit falls back to plain print.
- Resize: rich re-renders to current width; CJK handled by rich.
- Ctrl+C: `read_key` maps `\x03`→raise KeyboardInterrupt; `Live`/alt-screen restored via
  context manager; `run.py` KeyboardInterrupt handling unchanged.
- Arrow keys vs Live: `Live` only writes output; input read separately via `msvcrt` —
  no conflict. `auto_refresh=False` + manual refresh for menus (no CPU spin while idle).
- PyInstaller: `rich` added to `requirements.txt`; verify frozen exe renders in conhost.

## Testing

- Unit (pure logic, TDD): `MenuController` key→action transitions; `crawl_view.reduce`
  event→model; theme/kv formatting via `Console(file=StringIO, force_terminal=True)`.
- Real validation (mandatory per CLAUDE.md): run a real small crawl through the new view
  and confirm the final success/failed CSVs still write correctly; then build the exe and
  launch it to confirm rendering + arrow keys in a real console window.

## Out of scope (YAGNI)

No mouse, no theme switching, no color config beyond the one accent, no full alt-buffer
app framework, no animations beyond the single progress bar.

## Deployment

`packaging/build_exe.ps1` → `dist/OldIronCrawler/OldIronCrawler.exe` (onefile). Desktop
copy at `%USERPROFILE%/Desktop/OldIronCrawler/` is the live working dir (real `.env`,
`websites/`, `output/`). Deploy = back up old exe to `OldIronCrawler.exe.bak-0613`, then
copy ONLY the new exe over it. Never touch `.env` / `websites/` / `output/`.
