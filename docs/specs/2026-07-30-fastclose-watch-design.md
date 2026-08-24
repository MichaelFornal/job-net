# Fast-Close Watch — same-day job detection and push alerting

**Date:** 2026-07-30
**Status:** approved, ready for implementation plan

## Problem

Roles on some boards open and close within a single day. The existing corpus
refresh (`run_all.py`) is a multi-hour full sweep, and the only diffing that
exists — `new_jobs_build.py` plus the per-family `*_posted_ledger.json` files —
records `first_seen` and nothing else. There is no `last_seen`, no
disappearance detection, and no polling faster than a full rebuild.

Consequence: a role posted at 09:00 and closed at 18:00 is either never seen at
all, or first surfaces in a rebuild after it is already dead.

A second, unproven possibility is folded into the problem statement
deliberately: some of what looks like same-day churn may be detection latency
rather than real churn. A job posted Tuesday 09:00 and first seen in Thursday's
rebuild is indistinguishable, from the outside, from a job that never opened.
The system is required to *measure* which of these is happening rather than
assume.

## Goal

Detect a fit-matching job within minutes of it appearing, and push it to the
phone with enough context to decide whether to open it. Applications are made
by hand. Nothing is auto-submitted.

## Decisions taken during design

| Question | Decision |
|---|---|
| Hot path | Push alert → user applies by hand. `apply/` engine untouched. |
| Watch scope | Two-tier: fit-first hot watchlist + wide sweep of all ATS-native boards |
| Alert channel | ntfy.sh phone push only. No email, no dashboard. |
| Trigger | Every new fit job pushes; urgency tiered by measured board churn |
| Runtime | launchd daemon on the Mac, `caffeinate` during 06:00–21:00 ET |
| Build approach | Standalone module with its own thin fetchers (approach A) |

### Why a standalone module and not a `--fast` flag on the existing fetchers

The root fetchers carry `*_fetch_state.json`, JSONL append semantics, and
posted-date ledgers documented as *must never be rotated*. Threading a fast
path through them places the nightly corpus — which works — inside the blast
radius of a loop that fires ~180 times a day. The duplicated code is roughly
120 lines across four families, and it is the easy 120 lines (list a board,
parse ids), not the hard parts (discovery, dedupe, canonical keying).

Rejected alternative: running `run_all.py --only <family>` on a short cycle.
One family's refresh is tens of minutes minimum and it rotates state files, so
it cannot reach the latency target and would damage the nightly pipeline.

## Scale

| Family | Boards | Endpoint shape |
|---|---:|---|
| Greenhouse | 8,376 | one request per board |
| Ashby | 3,624 | one request per board |
| Lever | 2,842 | one request per board |
| Getro | 838 | one request per board |
| **Total board universe** | **15,680** | ~5 min at 24-way concurrency |

The hot tier is derived from this universe (see below) and removed from it, so
hot + wide sums to 15,680 rather than exceeding it. Hot is expected in the low
thousands at ~20–50 s per sweep; wide is the remainder at ~5 min, run every
45 min.

## Architecture

New `fastclose/` package beside `apply/`. It reads from the repo root
(`all_jobs_master.csv`, the `*_job_boards.csv` files, the filter regexes) and
writes only to its own `fastclose.db` and log files.

| File | Responsibility |
|---|---|
| `sources.py` | `list_board(family, token) -> [Posting]`, one thin implementation per family |
| `db.py` | SQLite schema and accessors (WAL mode) |
| `watchlist.py` | Derives the hot list from `all_jobs_master.csv` |
| `fit.py` | Fit matching; imports regexes, does not copy them |
| `sweep.py` | Runs one tier: fetch → diff → persist → emit push events |
| `notify.py` | ntfy push and evening digest |
| `stats.py` | Churn statistics and `--report` |
| `watch.py` | Daemon entry point; decides which tier is due |
| `com.mf.fastclose.plist` | launchd agent |

`Posting` is the minimal record a listing endpoint reliably provides:
`job_id`, `title`, `location`, `url`. Nothing else is assumed present.

### Configuration

`fastclose/config.json` (chmod 600, never committed) holds the ntfy topic — a
private random string — and the watch-hour bounds. A `config.example.json` with
placeholder values is checked in beside it.

### What gets stored

**Every** posting on **every** swept board is stored in `sightings`, not only
fit-matching ones. Churn measurement needs the full population: a board's
same-day close rate is meaningless if computed from the handful of postings
that happened to match the fit filter. `fit_lane` gates *pushes*, never storage.

Expected volume is on the order of 1.3M rows across the four families — a few
hundred MB of SQLite, comfortably within a local file. Rows for jobs whose
`gone_ts` is older than 90 days are pruned by `stats.py`; the closure event has
already been folded into `churn` by then.

### Tier exclusivity

`tier` is exclusive. The wide sweep covers `tier = 'wide'` only; hot boards are
not re-swept by it. A board promoted to `hot` leaves the wide set, keeping total
requests per cycle bounded and preventing double-counting in churn.

## Data model

```sql
boards(
  board_id     TEXT PRIMARY KEY,   -- "greenhouse:001"
  family       TEXT,               -- greenhouse | lever | ashby | getro
  token        TEXT,               -- board slug / collection id
  company      TEXT,
  board_url    TEXT,
  tier         TEXT,               -- hot | wide | cold
  seeded_ts    TEXT,               -- NULL until first successful sweep
  last_ok_ts   TEXT,
  consec_fail  INTEGER DEFAULT 0
)

sightings(
  board_id     TEXT,
  job_id       TEXT,
  title        TEXT,
  location     TEXT,
  url          TEXT,
  job_key      TEXT,               -- canon_key() from all_jobs_build
  fit_lane     TEXT,               -- ai | finance | NULL
  first_seen_ts TEXT,
  last_seen_ts  TEXT,
  gone_ts      TEXT,               -- NULL while still listed
  pushed_ts    TEXT,               -- NULL, a timestamp, or 'seeded'
  PRIMARY KEY (board_id, job_id)
)

churn(
  board_id        TEXT PRIMARY KEY,
  n_closed        INTEGER,
  n_same_day      INTEGER,
  p25_lifetime_h  REAL,
  median_lifetime_h REAL,
  updated_ts      TEXT
)

pushes(
  job_key   TEXT PRIMARY KEY,
  sent_ts   TEXT,
  priority  TEXT                   -- urgent | default
)
```

## Correctness rules

These are the rules that make the difference between a working detector and one
that produces confident nonsense.

### R1 — `gone_ts` is written only after a trustworthy fetch

A board sweep may write `gone_ts` only when the fetch succeeded **and**
returned more than zero postings. A board that errors, times out, or drops from
`>0` postings to `0` is recorded as *no data* for that cycle: `last_seen_ts` is
left untouched and no closure is inferred.

Rationale: without this, a single 429 on a large board logs hundreds of phantom
closures and permanently poisons the churn statistics that drive the urgency
tier. Retired ATS tenants are known in this repo to answer 307/429/403 rather
than 404, so error-as-closure is a live hazard, not a theoretical one.

### R2 — Cold start seeds silently

A board's first successful sweep writes its sightings with
`pushed_ts = 'seeded'` and sets `boards.seeded_ts`. No push is emitted. In
addition, any job whose `job_key` already exists in `all_jobs_master.csv` is
marked seen-not-new on first encounter.

Rationale: the first wide sweep observes 100k+ postings, all of which look new.
Both guards are needed — the corpus check alone misses boards absent from the
master, and the seed check alone re-pushes the whole corpus if the DB is ever
rebuilt.

### R3 — Family-wide circuit breaker

If more than 30% of a family's boards fail within a single sweep, that family's
diff for that sweep is discarded entirely — no new-job events, no closures.

Rationale: that pattern is a bad network or a bad day at the ATS, not three
thousand companies acting at once.

### R4 — Absence means "gone from listing", never "closed"

Some boards keep filled roles listed; some 404 the detail page while the
listing still shows the job; a paginated board can drop a job off page one
without closing it. Every fetch must retrieve the board's complete listing, and
the recorded event and all user-facing language is *gone from listing*.

## Fit matching

`fit.py` imports rather than copies:

- AI lane — `ai_jobs_filter.CORE` and `ai_jobs_filter.ADJ`
- Finance lane — `apply.select_jobs.FINANCE`
- Geography — MA, or US-remote, using the same `is_remote` logic and state set
  as `select_jobs`

**Known limitation:** listing endpoints carry title and location but not a
skills list, so `ai_jobs_filter`'s Skills-Match tier cannot be evaluated in the
fast loop. Fast-loop matching is title + location only. Skills-based matches
continue to surface through the nightly pipeline as they do today.

## Hot watchlist derivation

`watchlist.py` streams `all_jobs_master.csv` and promotes a board to `hot` when
either holds:

1. It has ever posted a job passing the fit filter, or
2. Its measured `p25_lifetime_h < 24` with `n_closed >= 5` (churn feedback)

Rebuilt after each nightly corpus refresh. Everything else stays `wide`.

## Scheduling

A single launchd agent (`RunAtLoad`, `KeepAlive`) runs `watch.py --daemon`.
The daemon:

- loops every 5 minutes
- holds a `caffeinate -i` child process during 06:00–21:00 ET, releasing it
  outside those hours so the Mac sleeps normally
- runs the hot sweep every cycle
- runs the wide sweep when 45 minutes have passed since the last one
- emits the digest push at 20:00 ET

Missing cycles is harmless: the differ is snapshot-based, not event-based, so a
sleep or a crash costs detection latency and nothing else.

## Push rules

- **🔥 urgent** (ntfy high priority) when the board's `p25_lifetime_h < 24`
  **and** `n_closed >= 5`. Below that threshold the board is untiered and
  pushes at default priority — no urgency claims from three data points.
- Cap 25 pushes/day; overflow rolls into the 20:00 digest.
- A `job_key` is pushed at most once, ever.
- Body: `title — company · location · 🔥 · AI lane`, tapping opens the apply URL.

## Failure handling

| Condition | Behaviour |
|---|---|
| Per-board timeout | 8 s, board counted failed for the cycle |
| `consec_fail >= 10` | Board demoted to `cold`, retried once daily |
| >30% family failure | Family diff discarded (R3) |
| ntfy delivery failure | Queued, retried next cycle |
| DB locked | WAL mode; sweeps retry with backoff |

Concurrency is bounded at 24 with per-host politeness.

## Testing

- Unit: differ (new / gone / unchanged), R2 cold-start seeding, R1 zero-jobs
  guard, R3 circuit breaker, fit matcher against a fixture of known titles
- Snapshot replay — two synthetic board listings fed through the differ — lives
  in the differ's unit tests rather than behind a separate `--replay` flag. A
  CLI flag would be a second copy of the same harness with nothing testing it.
- `--dry-run` prints what would push without pushing
- Live smoke test against 20 boards sampled from the real board CSVs, five per
  family, so the check cannot go stale as individual boards retire

## Out of scope

No auto-submit. No packet pre-generation. No email. No dashboard. No
Workday/iCIMS/Oracle — their multi-step session handling is the wrong trade for
a five-minute loop.

## Expected outcome

Within roughly a week, `stats.py --report` ranks boards by observed same-day
close rate with observation counts attached. This settles empirically whether
same-day closing is real on these boards and where, or whether the original
observation was rebuild latency. Both answers are useful; the design does not
presuppose either.
