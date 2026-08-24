# All-Jobs Monolith + Unified Orchestrator — Design

**Date:** 2026-07-19 · **Status:** approved for implementation (user exited plan mode to proceed)

## Problem

Four pipeline families now exist in `~/Documents/jb` — Getro (1.54M deduped jobs), Lever
(76k), the contract-gig multi-source harvest, and the FT+contract portals build (150k) —
but they produce six separate workbooks, share no cross-source dedupe, and rerunning them
means remembering ~15 script invocations, env flags (`ATS_KEEP_ALL=1`…), file rotations,
and a manual token merge. Goal: **one rerunable script** and **one monolithic workbook
holding all jobs**.

## Deliverables

1. **`all_jobs_build.py`** — merges the three master CSVs (`getro_jobs_master.csv`,
   `lever_jobs_master.csv`, `portals_combined_master.csv`) into:
   - `all_jobs_master.csv` — complete unified deduped table (~1.6–1.7M rows, 30 cols)
   - `All_Jobs_<date>.xlsx` — the monolith workbook
2. **`run_all.py`** — the single rerunable orchestrator for everything.
3. Minimal fix: `getro_check_live.py` `SRC` becomes an argv with a smart default
   (today it hardcodes `getro_collections.jsonl`; the real file is
   `getro_all_collections.jsonl`, so a bare rerun would crash).

## Unified schema (30 columns)

`company, title, employment_type, seniority, department, work_mode, remote, location,
city, state, country, posted, salary_min, salary_max, salary_currency, salary_period,
equity, skills, company_stage, company_size, company_industry, apply_url, job_url,
boards, board_count, source, source_type, sources, n_sources, job_key`

Mapping notes:
- Getro: `board_job_url`→`job_url`; `employment_type` = `contract_common.title_type(title)`
  or blank (blank = not stated); no city/state/country/department.
- Lever: `posting_url`→`job_url`; `department` = `department / team`;
  `employment_type` = `classify_any(commitment, title)` when commitment present, else
  `title_type(title)`; `region` dropped (country covers it).
- Portals: columns pass through; `board` merges into the `boards` union
  (`source:board` form); `sources`/`n_sources` union with the other pipelines.
- `job_key` = the dedupe key (below) — stable across snapshots, so flags can be
  re-applied to future monoliths by matching it.

## Cross-source dedupe (the new part)

Existing builders dedupe within a family; nothing dedupes across them, and raw apply
URLs differ cosmetically between sources (`/apply` vs `/application` suffixes,
`boards.` vs `job-boards.greenhouse.io`, `www.` vs `jobs.smartrecruiters.com`,
`-title-slug` tails). Three-tier key, strongest available wins:

1. **Canonical ATS key** parsed from `apply_url` (fall back to `job_url`):
   `(platform, org, job_id)` for lever, ashby, greenhouse (incl. `embed/job_app`
   query form), smartrecruiters, workable, recruitee, breezy, bamboohr, jazzhr
   (applytojob), personio, teamtailor, rippling, workday (`_REQID` tail), icims,
   wellfound, linkedin (`/jobs/view/{id}`), indeed (`jk=`).
2. `(company.lower, title.lower, apply_url.lower)` when the URL parses to no platform.
3. `(company.lower, title.lower, location.lower)` when there is no URL at all
   (e.g. Bullhorn agency portals).

Merge policy (same spirit as existing builders): union `sources`/`boards`/pipelines,
keep earliest non-empty `posted` (first-seen semantics), fill empty fields from the
other record. Ingest order getro → lever → portals so the richer Getro company
metadata forms the base and portals' `employment_type` fills the blanks.

## Workbook layout (`All_Jobs_<date>.xlsx`)

Tab order (Pipeline first — it must be `sheet1.xml` for the dropdown splice, and it's
the actionable tab):

1. **Pipeline** — every Remote role + On-site/Hybrid in MA / RI / South-FL metros,
   from the *unified* set (~100k rows est.); `flag/notes/bucket/region` + all 30 cols;
   Interested/Maybe/No dropdown spliced into sheet XML; Interested rows turn green.
   Region logic reused from `contract_pipeline.py` (word-boundary city regexes,
   state column support, non-US country never matches local).
2. **README** — counts, dedupe explanation, split explanation, column glossary, refresh
   instructions.
3. **All Jobs** — every unified job EXCEPT those seen only on the People Data Labs
   firehose (~780–900k rows, fits one tab).
4. **PDL Jobs** — jobs seen ONLY on People Data Labs (~829k). Splitting is what makes
   "all jobs in one workbook" physically possible under Excel's 1,048,576-row/tab limit.
5. **Summary** — by pipeline / source / employment_type / work_mode + overlap counts.
6. **Sources** — `build_sources.rows()` directory + synthesized Getro/Lever network rows.
7. **Boards** — (source, board, jobs, newest post), top 20k.

Any tab that still overflows the cap spills automatically to `"<name> 2"`, `"<name> 3"`…
(generic chunked writer) — nothing is ever silently truncated. `--csv-only` (skip
workbook) and `--no-pdl` (skip the PDL tab; CSV always complete) flags for iteration.
Memory: fixed-index row lists + selective string interning (the proven
`getro_build_sheet.py` approach that fit 2.15M rows on this 16GB machine).

## Orchestrator (`run_all.py`)

```
python3 run_all.py                    # default: refresh all jobs + rebuild everything
python3 run_all.py --build-only      # no network; rebuild masters + monolith from disk
python3 run_all.py --discover        # also re-run board/tenant discovery first
python3 run_all.py --resume          # continue an interrupted refresh (keep state/JSONLs)
python3 run_all.py --only getro      # or --skip lever,portals
python3 run_all.py --dry-run         # print the full plan, execute nothing
```

**Refresh rotation** (skipped with `--resume`/`--build-only`): each family's append-only
JSONLs rotate to `*.prev.jsonl` (one generation kept; 46GB free vs ~4GB transient) and
its `*_fetch_state.json` files are deleted so fetchers do a true re-fetch instead of
skipping "done" boards. Families: getro (`getro_all_jobs.jsonl` + 2 states), lever
(`lever_all_jobs.jsonl` + 1 state), portals (8 `contract_norm/*_all*/boards` JSONLs +
2 states). Mined intermediates (`contract_from_*.jsonl`, `contract_norm/{getro,lever}.jsonl`)
are regenerated; `normalize_mined.py` appends, so its two outputs are truncated before
every attempt.

**Execution DAG** — five parallel chains (different APIs, so concurrent is safe), then
joins:

- **getro**: `getro_check_live` → `getro_fetch_jobs` → `getro_fetch_pdl` →
  `contract_mine_getro` → `ATS_KEEP_ALL=1 ats_fetch` (needs the slugs the miner emits)
- **lever**: `lever_fetch_jobs` → `contract_mine_lever`
- **boards**: `boards_fetch` · **phenom**: `PHENOM_KEEP_ALL=1 phenom_fetch` ·
  **bullhorn**: ensure `bh_tokens_merged.txt` (regenerated from
  `bh_all_tokens.txt` + `bh_github_tokens.json` + `bh_final_tokens.txt` — today it has
  no producer script) → `BH_KEEP_ALL=1 bh_fetch`
- join(getro, lever) → `normalize_mined`
- **build** (sequential; memory-heavy): `build_sources` → `getro_build_sheet` →
  `lever_build_sheet` → `bh_build_combined` → `all_jobs_build`

`--discover` prepends per-chain discovery: getro enumerate (END = max known collection
id + 3000; ids are strings in the JSONL), lever wayback discover + probe, bullhorn
github/urlscan token hunts + token merge.

**Supervision**: each step re-launches up to 40× (port of `run_supervised.sh`), stdout
appended to `logs/<step>.log`, console gets start/finish lines + the step's final summary
line. Steps without state files (boards/phenom appenders) truncate their own output
before each attempt so retries can't duplicate rows. A failed family aborts its chain
but not the others; the build stage runs with whatever masters exist (warns if stale)
and the process exits non-zero.

## Verification

- `python3 -m py_compile` on both new scripts + the patched one.
- `run_all.py --dry-run` shows the full ordered plan.
- `all_jobs_build.py` runs against today's masters: row counts reconcile
  (unified ≤ sum of inputs; PDL + eco = total), cross-pipeline merges exist
  (rows with getro+lever/portals in `sources`), all tabs under the row cap,
  dropdown splice self-verifies, workbook opens (openpyxl read-back spot check).
- `run_all.py --build-only` end-to-end regenerates every master + the monolith.
- Full-refresh path validated by `--dry-run` inspection (a real refetch is ~3h of
  network; the state/rotation semantics are the same ones each fetcher already proved).

## Out of scope

The legacy per-family pipeline workbooks (`Getro_Pipeline_*.xlsx` etc.) keep working but
are not run by the orchestrator — the monolith's Pipeline tab supersedes them. Old dated
workbooks are left on disk. The contract-only crawl variants (`ats_*.jsonl`,
`phenom.jsonl`, `bullhorn.jsonl`, `contract_build.py`, `contract_pipeline.py`) are
superseded by the `*_all` FT-inclusive crawls and not refreshed by default.
