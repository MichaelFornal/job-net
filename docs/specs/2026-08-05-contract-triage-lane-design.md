# Contract-gig lane in triage — design

**Date:** 2026-08-05
**Goal:** a sixth tab in the triage app holding the 1,000 best contract gigs,
scored off a freshly refreshed contract pipe.

## Decisions taken during design

| Question | Answer |
|---|---|
| What "best" means | Blend: fit 60 / speed 25 / rate 15 |
| Geo scope | US-remote + MA + nearby (RI / southern NH / northern CT) |
| Refresh depth | `run_all.py --only portals` (~2h); Getro/Lever already Aug 3 |
| `contract_build.py` | Fix it in the same pass |

## The bug this work has to route around

`contract_build.py` decides what is contingent **by filename**: it skips any
`contract_norm/*.jsonl` whose basename contains `_all`. That rule was correct on
2026-07-21 and stopped being correct on 2026-07-29, when ~25 new pipes began
writing **full-employment-type** dumps into the same directory under names that
carry no `_all` suffix — `workday.jsonl` (248 MB), `careerjet.jsonl` (115 MB),
`oracle`, `icims`, `jobgether`, `themuse`, `intlmisc`, `intlboards`, `adzuna`,
`adp`, and more. Sampling every one of them returns `Full-time` on the first row.

So the filename rule now fails in both directions at once:

- it **admits** hundreds of thousands of Full-time rows into a master whose
  README calls it contingent-only, and
- it **excludes** the freshest contract data on disk, because the KEEP_ALL
  dumps (`ats_all_*`, `bullhorn_all`, `phenom_all`, `boards_all`) are exactly
  the files the `_all` guard throws away.

**Fix:** filter by type, not by name. A row is in the pool only when
`contract_common.classify(employment_type, title)` returns a contingent label.
Filenames stop carrying meaning, so the next new pipe cannot silently
re-introduce the same bug.

Generation selection is a separate concern from type: use one generation per
source — skip `*.prev.jsonl`, and where a source has both a Jul-19 contract-only
twin and an `_all` dump, keep only the `_all` one. Without this the pool
resurrects Jul-19 postings that have long since been filled.

## Components

### 1. `contract_pool.py` (new)

Reads `contract_norm/*.jsonl` → contingent-type filter → two-pass dedupe (same
keys as `contract_build`: `(source, job_id)`, then `(company, title, apply_url
| location)`) → geo filter → field filter → score → sort.

Geo and field filters are **imported from `contract_ma_fasthire`**, not
re-typed: `geo_of()`, `field_ok()`, `DROP_STEM`/`DROP_WORD`, `TYPE_SPEED`,
`URGENT_RE`, `DURATION_RE`, `parse_posted`. One definition of "commutable" and
one definition of "professional work" for both pipelines.

Outputs:
- `contract_pool.csv` — the full ranked pool, every scoring component kept as
  its own column so a rank can be audited.
- `Contract_Pool_<date>.csv` — top N (default 1000) in the exact column shape
  `triage_load.py` reads.

### 2. Scoring — blend 60/25/15

Contract rows carry **no `skills` field and no job description**, so fit is
title + company only. This is the same constraint triage already lives under
(the corpus has no JD either); it is not a gap to fix here.

**fit 0–60** — title tiers using `ai_jobs_filter`'s `CORE` and `ADJ` regexes:

| Tier | Points |
|---|---|
| Core AI building (`CORE`) | 60 |
| AI/ML adjacent (`ADJ`) | 45 |
| Software / platform / data engineering | 32 |
| Adjacent tech (PM, solutions, analytics) | 18 |
| Other professional | 6 |

**speed 0–25** — agency source, `TYPE_SPEED` by contingent label, urgency
wording in title, cross-posting, recency. Rescaled from `contract_ma_fasthire`'s
0–100 structural+urgency+recency.

**rate 0–15** — normalized to an hourly equivalent (annual ÷ 2080, daily ÷ 8).
Rows with no published pay score 0. Roughly 70% of the pool publishes no pay;
at a 15% weight they still rank on fit+speed rather than being buried, and the
zero is uniform so it never reorders rows against each other.

`score` = sum, 0–100. `score_why` = plain-English audit string.

### 3. Triage integration

The lane reuses the existing decisions / apply / carry-forward machine rather
than sitting beside it. Approvals flow to `/apply` unchanged, because that
queue keys off `decisions` and is feed-blind.

Schema — `jobs` gains three columns (ALTER + backfill, existing 178k rows
become `feed='main'`):

```sql
feed      TEXT DEFAULT 'main'   -- 'main' | 'contract'
score     REAL DEFAULT 0        -- blended 0-100, contract rows only
score_why TEXT DEFAULT ''       -- why it scored that
```

`triage_load.py`:
- new `--feed <name>` argument, default `main`
- **`retire()` becomes feed-scoped.** This is the load-bearing change: it
  currently sets `live=0` on every row the current load did not touch, so
  without scoping, each loader would retire the other's rows and the contract
  tab would empty itself on the next corpus rebuild — the same class of silent
  failure that `carry_forward` exists to prevent.

`triage/lanes.py`:
- the existing four lanes and `rest` are scoped to `feed='main'` — meanings
  unchanged, contract gigs simply cannot leak into the AI lane
- new `contract` lane: `feed='contract'`, ordered `score DESC, posted DESC`
  instead of the global `is_agg, rank_tier, posted` ordering
- `queue_sql`'s `order` parameter accepts a custom ORDER BY string

`triage/server.py` — `CARD_COLS` gains `score`, `score_why`, `feed`; card
queries use the lane's own ordering.

`triage/index.html` — contract cards show the score, the why line, and the
hourly rate; help text updated from `1`–`5` to `1`–`6`.

### 4. `contract_build.py` fix

Replace the filename guard with the same type-based filter, and apply the same
one-generation-per-source rule. `contract_master.csv` becomes contingent-only
again and picks up the fresh KEEP_ALL data it has been discarding.

## Found during implementation

Five things the design did not anticipate, all caught by looking at the ranked
output rather than at the counts:

1. **The geo filter asserted US from absence of evidence.** `geo_of()` treats a
   blank `country` as US and calls anything remote "Remote (US)". 71% of this
   pool has no country, and the boards that omit it are the international ones
   — himalayas rows reading `Spain`, `Serbia`, `Brazil`; remoteleaf rows reading
   `Japan, New Zealand, Singapore`; nofluffjobs, a Polish board whose listings
   all say just `Remote`. 39,760 non-US rows were ranking as US-remote.
   Replaced with `us_geo()`, which requires positive evidence and records which
   rule admitted each row in `geo_basis`.

2. **Dedupe barely bound on an aggregator-heavy pool.** The master's
   `(company, title, apply_url)` key leaves one gig posted eleven ways as
   eleven rows, because aggregator apply URLs are per-listing self-redirects.
   231 of the first 1,000 rows were exact company+title repeats and one
   AI-data-labelling farm held 164 of them. Collapse now ignores the URL, and
   ties go to the employer-direct copy.

3. **One employer could still own the list.** Added a `--per-company` cap
   (default 5) on the triage slice, with overflow left in `contract_pool.csv`.

4. **The blend ranks but does not compose.** Only 1,623 of 11,053 pooled rows
   reach tech-adjacent or better, so a pure blend filled the bottom 400 of the
   queue with maximally-fast, well-paid work that is not this user's — trial
   attorneys, HR interns, per-diem organ placement coordinators. Added
   `--min-fit` (default 18): the floor decides who is eligible, the blend still
   decides the order.

5. **`triage_export.py` would have dropped every approved contract gig.**
   `collect()` scanned one CSV for row bodies, so an approved contract row was
   approved in the app and absent from the workbook, silently. It now reads
   every feed's CSV and maps later files onto the first one's header.

Also: `job_key` must be a stable digest, not Python's `hash()` (salted per
process — every decision would re-queue on the next run), and its identity is
`url + company + title`, not URL alone (staffing boards put several distinct
gigs behind one URL; 20 of the first 1,000 rows collided).

## Addendum — the fast-money lane (2026-08-06)

A second tab was requested: quick-hire contract work, "a fast money tab".

**`money_score` = quick-hire 60 / pay 40, with no fit component at all.** A fit
floor here would just rebuild the contract lane. Speed comes from staffing-agency
source, a contingent type that hires on a rolling basis, urgency wording, defined
duration and recency; pay from the hourly equivalent. Eligible when the rate is
known **or** hire speed >= 22 — a staffing desk with an urgent req is fast money
even when the rate is only quoted on the phone. Result is ~86% "Other
professional", which is the tab working, not failing.

**One feed, two lanes, separated by `lane_tags`.** A gig can be both a good fit
and fast money. Giving each lane its own feed would need a second `job_key` for
the same job, so it would be judged twice and could carry two different verdicts.
Tagged (`,contract,fastmoney,`), it is one row: deciding it in either lane clears
it from both. ~14% of the fast-money slice is also in the contract slice. The
per-company cap applies per slice, so one employer can hold 5 in each.

Two more defects surfaced, both because this lane has no fit floor and therefore
puts at the top what the contract lane buried below its cut:

6. **Rate parsing was inventing numbers.** `salary_period='Hour'` on a 50,000
   annual figure, a 2,000,000 max beside a 20,000 min, a bare `15000` with no
   period — producing $50,000/hr, $962/hr, $1,875/hr. On a money-ranked lane a
   bad parse does not sit there being quietly wrong, it sorts straight to the
   top. `hourly()` now refuses: it returns None unless the result lands in
   $10–1000/hr, and distrusts a max more than 20x its min.

7. **Clinical roles were passing the field filter.** 5 of the first fast-money
   top 20 were `Telehealth MD`, `1099 Telehealth Gynecologist`,
   `All Remote Teleradiologist`, `APRNs`. `DROP_STEM` anchors on word
   *prefixes*, so `radiolog` never matches "Teleradiologist", and `\bmd\b` /
   `\bnp\b` are excluded on purpose (Maryland, Managing Director). Fixed at the
   source in `contract_ma_fasthire` — tele-prefixed forms plus specialties, and
   `DROP_WORD` gained aprn/crna/fnp/pmhnp/dnp/dds/dmd/lcsw/lmhc. Not `do`,
   which is the most common verb in English.

## Testing

`triage/selftest.py` gains cases for:
- feed-scoped retire: loading feed A does not set `live=0` on feed B's rows
- the `rest` lane does not swallow contract rows
- contract lane ordering is by score, not by `rank_tier`
- a contract row's decision still carries forward on `ct_key`

Plus a scoring unit check: a known Core-AI remote contract row outranks a known
generic on-site one.
