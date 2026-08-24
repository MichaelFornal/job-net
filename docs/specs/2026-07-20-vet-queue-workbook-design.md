# Vet Queue Workbook — Design

**Date:** 2026-07-20
**Goal:** A lean Excel workbook for filtering and vetting jobs from the Getro+Lever
universe, with vetting decisions that survive every re-crawl/rebuild.

## Problem

`Getro_Lever_Jobs_<date>.xlsx` holds 979k jobs in one read-only reference tab.
There is nowhere to record vetting decisions, and every rebuild produces a fresh
file, so any notes typed into a snapshot workbook are orphaned. The monolith's
Pipeline tab proved the flag/notes/dropdown pattern but has no carry-forward.

## Decisions (from brainstorm)

- **Vet surface:** pipeline-style slice — US-remote jobs plus on-site/hybrid in
  MA / RI / South-FL (~140k rows). Full universe stays in the existing
  `Getro_Lever_Jobs_<date>.xlsx`; not duplicated here.
- **Persistence:** automatic carry-forward. Vet state is harvested from the
  previous workbook into `vet_ledger.csv` (keyed on stable `job_key`) and
  re-applied to each new snapshot.
- **Vet fields (full CRM-ish):** `status`, `notes`, `applied_date`, `contact`,
  `next_action`, `follow_up_date`.
- **File weight:** lean workbook — vet queue + README + Summary only.
- **Ranking:** newest first (Remote bucket, then Local), no scoring column for
  now. An `ai_tier` column may be added later.
- **Remote scope:** US-remote only — remote rows tagged to a non-US country are
  dropped.

## Artifacts

| File | Role |
|---|---|
| `vet_build.py` | One command: harvest → rebuild → write workbook |
| `Vet_Queue_<date>.xlsx` | The vetting workbook (regenerated each run) |
| `vet_ledger.csv` | Persistent source of truth for all vet state |
| `vet_seen_keys.txt` | Every job_key ever shown in a queue (drives the `new` ★ marker) |

## Workbook layout

**Queue** (first sheet — the data-validation splice targets `sheet1.xml`, same
technique as `all_jobs_build.py`):

- Columns: `status`, `notes`, `applied_date`, `contact`, `next_action`,
  `follow_up_date`, `new`, `listing`, `bucket`, `region`, then the same 28 job
  columns as `Getro_Lever_Jobs` (monolith columns minus `city`/`state`).
- `status` dropdown: Interested / Maybe / No / Applied / Interviewing / Offer /
  Rejected. Blank = unvetted.
- Conditional row fills: Interested green, Maybe yellow, Applied/Interviewing
  blue, Offer gold, No/Rejected gray.
- `new` = ★ when the job_key has never appeared in a previous queue (blank on
  the very first build — no baseline to compare).
- `listing` = live | gone. `bucket` = Remote | Local | Gone. `region` = MA / RI /
  South FL / FL (statewide tag), from `all_jobs_build.regions_of`.
- Sort: Remote first, then Local (region rank), newest `posted` first within
  each; Gone rows last. Freeze panes, autofilter, no formulas (values only).

**README** — what it is, how carry-forward works, refresh command.
**Summary** — counts by status, bucket, region, new-this-snapshot, gone.

## Flow per run

1. **Harvest.** Find the newest existing `Vet_Queue_*.xlsx` (mtime, skipping
   `~$` lock files), or the path given by `--harvest-from`. Read the Queue tab
   headers by name; every row with any non-empty vet field is upserted into
   `vet_ledger.csv` by `job_key` (workbook wins — it is newer). The ledger also
   snapshots `company`, `title`, `apply_url`, `posted` so it stands alone, plus
   `first_seen` / `last_updated` dates.
2. **Rebuild.** Load `getro_jobs_master.csv` + `lever_jobs_master.csv` with
   `all_jobs_build` loaders and canonical-ATS dedupe (same as
   `getro_lever_build.py`). Apply the posted-date cutoff (default
   `--since 2026-05-01`, `--all-dates` to disable) — **except** rows whose
   job_key has vet state in the ledger, which never age out. Slice to
   US-remote + MA/RI/South-FL local.
3. **Merge.** Copy ledger vet fields onto matching rows. Ledger rows with an
   active status (anything except No / Rejected) that no longer match any
   crawled job are appended as synthetic rows with `listing=gone`, so an
   Applied job cannot silently vanish.
4. **Write** the workbook, splice the status dropdown into the Queue sheet XML,
   verify the splice, then append this queue's job_keys to `vet_seen_keys.txt`.

`--harvest-only` runs step 1 and stops (banks vet work without a rebuild).

## Error handling

- Missing master CSVs → exit with message (same as existing builds).
- No previous workbook / no ledger → first-run mode, harvest skipped.
- Splice anchor missing → hard exit before replacing the output (as monolith).
- Harvest upserts a row when any vet field is non-empty **or** the job_key is
  already in the ledger — so clearing every vet cell in the workbook clears the
  ledger entry too (blanked, not deleted) instead of resurrecting next build.

## Verification

1. Run `python3 vet_build.py`; compare slice counts against the monolith log
   (~156k on the bigger universe — expect same order of magnitude).
2. Inspect the workbook with openpyxl read-only: sheet order, headers, row
   count, dropdown XML present in `sheet1.xml`.
3. Round-trip: build a small synthetic "previous" workbook with two real
   job_keys (one vetted, one fake-key active status), run
   `vet_build.py --harvest-from <it>`, confirm carried status/notes on the live
   row and a `gone` row for the fake key; then remove test rows from the ledger
   and rebuild clean.
