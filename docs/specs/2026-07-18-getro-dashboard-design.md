# Getro Pipeline Dashboard — Design Spec

**Date:** 2026-07-18
**Status:** Approved by user (base scope: "visual and bones"; features layer on later)

## Purpose

Turn the existing Getro job-board scraping pipeline (`getro_scraper.py` +
`boards.csv` → `getro_results.csv`) into a local web dashboard: trigger scrapes
from the browser, watch progress, and browse/search/filter the results. This is
the **base** — visual quality and clean extension points matter more than
feature count. Per-job status tracking, notes, and board management are
explicitly deferred, but the base must make them easy to add.

## Scope decisions (made with user)

- **Scope:** Run + browse base. No status tracking, no notes, no board CRUD yet.
- **Stack:** Flask + vanilla JS single page. No build step, no Node toolchain.
- **Hosting:** Local only. `python server.py` → `http://localhost:5050`
  (port 5050 because macOS AirPlay Receiver occupies 5000).

## Architecture

```
jb/
├─ getro_scraper.py        ← minimally extended (see below), CLI unchanged
├─ discover_getro_companies.py  (untouched)
├─ boards.csv              ← input: one board URL per line
├─ getro_results.csv       ← output: shared by CLI and dashboard
├─ server.py               ← Flask app + API + background scrape thread
└─ static/
   ├─ index.html
   ├─ style.css
   └─ app.js
```

- `server.py` imports `getro_scraper` as a module; the scraper remains the
  single source of truth for fetching, filtering, de-duping, and CSV writing.
- **Targeted change to `getro_scraper.py`:** `run_scrape()` gains optional
  parameters `boards=None` (defaults to current behavior) and
  `on_progress=None` — a callback invoked with progress events
  (board started, board finished with count, board failed, run finished).
  With both left as defaults, CLI behavior is byte-for-byte identical.
- Both entry points write the same `getro_results.csv`; running the CLI still
  works and the dashboard picks up its output.

## API

| Method | Path                 | Behavior |
|--------|----------------------|----------|
| GET    | `/`                  | Serves `static/index.html` |
| GET    | `/api/jobs`          | Parses `getro_results.csv` → `{jobs: [...], last_scraped: <mtime ISO>}`. Missing file → `{jobs: [], last_scraped: null}` |
| GET    | `/api/boards`        | Active (uncommented) URLs from `boards.csv`; falls back to the scraper's built-in `BOARDS` if the file is absent |
| POST   | `/api/scrape`        | Starts a scrape in a background thread. `409` if one is running. `202` with initial status otherwise |
| GET    | `/api/scrape/status` | `{status, boards_done, total_boards, current_board, jobs_found, started_at, finished_at, error, log: [...]}` |

- `status ∈ idle | running | done | error`.
- One scrape at a time; scrape state is a single in-process dict guarded by a
  lock. No persistence of run history in the base.
- Frontend polls `/api/scrape/status` every ~1 s while `running`, then reloads
  `/api/jobs`.

## UI (single page, four zones)

1. **Header** — dashboard title; **Run Scrape** button that morphs into a
   progress indicator while running (boards done / total, current board name).
2. **Stat tiles** — total jobs, unique companies, active boards (from
   `/api/boards`), last-run time.
3. **Control bar** — free-text search over title + company, board filter
   dropdown (populated from results), clickable column headers to sort.
4. **Results table** — company, title, location, board (short host name);
   row links open the job URL in a new tab. All filtering/sorting is
   client-side over one state object.

- **Empty state:** no CSV yet → friendly "run your first scrape" panel, never a
  blank page.
- **Visual direction:** decided at implementation time via the
  `frontend-design` skill (and `dataviz` for stat tiles) — intentional and
  distinctive, not framework-default. Theming via CSS custom properties.

## Error handling

- Per-board fetch failures: recorded as log events via `on_progress`, shown in
  the UI after the run; the run continues past failed boards and still ends
  `done` (with failures visible in the log).
- Unhandled exception in the scrape thread: `status: error` + message; the Run
  button becomes available again.
- Malformed or missing CSV rows: skipped defensively; `/api/jobs` never 500s
  for content reasons.

## Extensibility bones (deliberate)

- Job identity is `(company, title, url)` — matches the scraper's de-dupe key;
  the future status-tracking table joins on it.
- All rendering flows from one client-side state object in `app.js`.
- Adding the tracker later = SQLite file + two endpoints + a status column;
  no rework of the base.

## Testing

- Flask test-client unit tests: `/api/jobs` CSV parsing (normal, missing,
  malformed), scrape-state transitions with `run_scrape` mocked, the `409`
  concurrent-scrape guard.
- End-to-end smoke: start server, run a real scrape, verify results render in
  the browser.

## Out of scope (deferred features)

Status tracking / notes, board management UI, run history, company discovery
integration, keyword-filter editing UI, deployment.
