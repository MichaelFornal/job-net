# Startup job sheet — design and measured findings (2026-07-29)

`startup_boards.py` + `startup_build.py` → `Startup_Jobs_<date>.xlsx` / `.csv`.
Scope as chosen: **US + remote, last 30 days**, tiered so nothing is silently
mixed.

Everything below was measured against `all_jobs_master.csv` (2,392,615 rows), not
recalled. Where a first attempt was wrong, the correction is recorded so nobody
re-derives the optimistic version.

---

## 1. Three traps found while building this

**1. `company_stage` is partly poisoned.** `Pre Seed` is Getro's junk default:
46,204 of its 61,897 rows are 1000+ headcount employers — Walgreens (9,740),
AWS (5,118), JPMorganChase (4,722), Pratt & Whitney, Mayo Clinic, Duke
University. The named rounds are clean by comparison: `Seed` + hc=6 is only 316
rows (Cox Enterprises), `Series A` + hc=6 is **1** row. So `Pre Seed` is only
admitted as evidence at ≤500 staff *and* on a VC-classified board; every other
named round stands on its own.

**2. Getro boards are not one population.** The `boards` column puts a16z and Lux
Capital in the same field as chambers of commerce, county economic-development
corporations, university career fairs and a data vendor's marketing board.
**People Data Labs alone carries 181,487 candidate rows** — 21% of the whole
candidate set — and is the single largest source of the Pre-Seed-Walgreens
pollution. Board names cannot make the call: `8VC`, `Antler`, `Atomic`, `DCVC`,
`Costanoa` carry no VC token, while `Space Capital | Talent` is a talent network.

**3. Posted dates are not uniformly real.** Measured per source:

| quality | sources | evidence |
|---|---|---|
| real | getro, greenhouse, lever, ashby, teamtailor, careerjet … (50) | ~90 distinct dates, top date 3–8% |
| ingest-stamped | personio, rippling, icims, workday, taleo, jobvite … | 98% / 98% / 50% land on the run date |
| none | jazzhr, bamboohr, recruitee, phenom, powertofly … | 98–100% blank |

A plain `posted >= 30 days ago` would have **deleted 153k undated rows while
keeping every stale ingest-stamped one**. This is the §5 gap in
`startup_sources_2026-07-27.md`, hit in practice.

---

## 2. Board classification — measured, not named

`startup_boards.py` scores each of the 836 Getro boards from how its own
companies look, then lets names decide only the unmeasurable edges:

- `funded_share` — companies with a named round, of those with any stage
- `small_share` — companies at head_count ≤ 4 (~≤500 staff)
- `big_share` — companies at head_count = 6 (1000+ staff)

Verdicts: **566 vc · 193 ecosystem · 77 junk** → `startup_boards.csv`, with the
three shares and a reason per board. Edit that file and re-run the builder to
override any call.

**Stats outrank every name signal.** The first version let the allowlist win and
`Compa` — a comp-benchmarking vendor — was classified `vc` while measuring
`small=12% big=66%`. `MA AI Hub` (big=44%) and `Arist` (big=79%) were caught the
same way. A VC-sounding name now only rescues a board whose companies are
actually small.

## 3. ATS startup density — measured the same way

Joining each source's companies against the enriched population and scoring
`funded_share × small_share`:

| ATS | funded | small | score | placement |
|---|---|---|---|---|
| ashby | 73% | 86% | **0.63** | Tier 3 (dense) |
| yc | 94% | 44% | 0.41 | Tier 3 |
| lever | 47% | 78% | 0.37 | Tier 3 |
| greenhouse | 37% | 62% | 0.23 | Tier 4 (mixed SMB) |
| jazzhr | 26% | 90% | 0.23 | Tier 4 |
| **smartrecruiters** | 15% | 39% | **0.06** | **excluded** |
| workday / themuse / icims | ~8% | ~15% | 0.01 | excluded |

Caveat: teamtailor, recruitee, greenhouse and lever match only 2–7% of their
companies, and that sample is biased toward companies that also sit on a VC
board — so their true density is likely **lower** than measured, not higher.

`smartrecruiters` was in the startup list in the first build despite
`startup_sources_2026-07-27.md` row 8 saying its page-1 companies are "Accor,
Domino's, Experian, Equinox". **Domino's alone landed 19,997 rows — 13% of that
build.** Removed.

---

## 4. Tiers

| tier | rule | rows |
|---|---|---:|
| 1 Funded | named round Seed..Series H at ≤500 staff (tolerates 501–1000) | 14,790 |
| 2 VC-backed | on a `vc` board at ≤500 staff, round unknown | 5,403 |
| 3 Startup ATS | measurably startup-dense ATS, no enrichment either way | 7,361 |
| 4 SMB ATS | mixed-SMB ATS, no enrichment. Weakest — check it, don't trust it | 18,846 |
| Undated | any tier, no verifiable date; kept off Best Bets | 13,706 |

Excluded outright: IPO/acquired/PE (207,746), 1000+ staff (343,984), enterprise
and aggregator sources (865,430), junk/ecosystem-only boards (19,367), bulk
posters (130,597), synthetic listings (1,957), non-US non-remote (287,987),
outside the window (51,170).

**Bulk-poster cap.** No company at startup scale has 200 open requisitions, and
more than 4 per employee is a staffing agency or franchise. `Ethos` (481
openings at 1–10 staff, posting "Expert Opportunity – Full Professor" and
"Hedge Fund Analyst") ranked #4–18 before this existed.

**Cross-source dedupe.** `all_jobs_build.canon_key` cannot collapse a
Getro-hosted redirect page against the same job's real ATS URL, and the copies
disagree about location — Teragen Energy's Ashby copy says `Lehi, UT, USA` with
`city=Southborough` while its Getro copy says `Southborough, MA, USA`. So the key
is company + title, the best-placed copy is kept (MA wins, then direct apply URL,
then real date), and every other location goes to `alt_locations`. **11,353 rows
collapsed.** The per-city franchise repostings collapse too: one waste-removal
role covered 151 cities.

---

## 5. Derived columns

`department` and `seniority` in the master are unusable as filters — 75% and 62%
blank, and polluted (`'/ Global Elite Empire Consultants'`,
`'Engineering / Engineering'`, `'Entry Level'` and `'Entry level'` as separate
values). Everything below is derived from the title instead, at 100% coverage:

- `role_family` — 17 families. Adding **Science** matters for Boston biotech;
  Education / Retail-Food / Trades exist so they can be excluded. `Other` is 21%.
- `level`, `founding_role` — "Founding Engineer" is a startup-only title.
- `ai_role` (title/skills, reusing `ai_jobs_filter.py`) and `ai_company`
  (industry tags) are **separate columns** — an AI role at a non-AI company and a
  sales job at an AI startup are different things.
- `co_openings`, `co_openings_all`, `hiring_intensity` — openings ÷ headcount
  midpoint. A 20-person company with 15 open roles is the strongest
  "just raised and moving" signal in this data.
- `vc_boards`, `n_vc_boards` — makes "only Lux/Khosla/8VC-backed" a dropdown.
  Two independent funds listing a company is stronger than one.
- `evidence`, `flags`, `date_quality`, `days_old`, `dup_count`, `alt_locations`.
- `salary_usd_min/max` annualised at fixed FX (24.5% coverage), `equity_offered`.
- `geo_bucket`, `commute` (MA / nearby RI-NH-CT-ME-VT), `ats_family`,
  `apply_direct`.

Best Bets ranks by tier → geography → role band (technical, science, commercial,
frontline) → earlier round → founding → AI → hiring intensity → newest. The role
band only affects rank; nothing is hidden.

---

## 6. Open item

**19,367 Tier-1 rows are dropped for sitting only on a junk/ecosystem board** —
every one of them has a real named funding round, so the funding evidence and the
board evidence disagree. They are dropped per the "drop junk boards entirely"
decision; `--keep-funded-on-junk` keeps them. Worth an eyeball on the
`Excluded (sample)` tab before settling this permanently, because a genuinely
funded startup that also lists on its local chamber board is currently lost.

Runtime: classifier 27s, builder ~2:55 (two passes over 1.1 GB). Output 50.3 MB,
12 tabs.
