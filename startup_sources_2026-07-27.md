# Startup job sources — expansion briefing (2026-07-27)

Produced by a 6-lens fan-out (14 agents, every endpoint hit with a live request),
then adversarially spot-checked. **Numbers below are the post-critique values** —
where the discovery sweep and the verification pass disagreed, the measured
result wins and the correction is noted inline.

---

## 0. Baseline reality (measured, not recalled)

`all_jobs_master.csv` — 1,116,038 unique jobs (not 1.53M; that figure is stale).

| source | unique rows | raw pipe yield | survival after dedupe |
|---|---:|---:|---|
| getro | 978,547 | ~979k | ~100% |
| greenhouse | 77,050 | ~208k | **37%** |
| lever | 27,663 | ~76k | **36%** |
| ashby | 12,977 | ~60k | ~20% |
| phenom | 7,253 | — | — |
| freelancer | 4,966 | — | — |
| smartrecruiters | 4,119 | — | — |
| bullhorn | 2,044 | — | — |
| bamboohr / recruitee / breezy | 1,003 | — | — |
| **all 8 remote boards combined** | **256** | ~50k | **~0.5%** |

Three consequences that drive every ranking on this page:

1. **Getro is 87.7% of the corpus.** Every "we only hold N boards of this family"
   claim computed from a `*_job_boards.csv` is understated, because Getro
   syndicates other ATSes' postings as its own rows. Measured example: the sweep
   claimed we hold 17 Rippling boards; the master actually contains **340
   distinct `ats.rippling.com` boards / 2,586 rows**. Recompute overlap against
   the Getro row corpus, never against board lists.
2. **New ATS families mostly re-find held jobs.** Greenhouse contributed 208k
   rows and only 77k survived as unique. Assume ~35% survival for any new
   mainstream ATS pipe unless it serves a population Getro's 768 VC boards
   don't reach.
3. **The remote-board category is dead weight.** Eight integrations net 256
   unique jobs. Anything shaped like Remotive/RemoteOK/Jobicy should be ranked
   accordingly, and the aggregators (Simplify, TrueUp, Remote Rocketship) are
   the same trap one layer up.

The axis that matters is **companies reached that Getro doesn't**, not job count.

---

## 1. Verdict

Build in this order:

1. **Wayback CDX apex sweep** — one request per ATS apex, expands the slug space
   of pipes that already exist. Highest leverage in the sweep, S effort.
2. **Rippling** — a genuinely new ATS family, unauthenticated, whole board in one
   call. **~11k jobs** (not the 20–60k the sweep claimed — see §3), ~15% board
   overlap.
3. **YC Work at a Startup** — the only source here whose postings are structurally
   absent from every ATS. Small but ~0% overlap and exactly the startup segment.
4. **Personio** — the sweep marked it blocked; that was wrong (§3), it is an open
   S-effort pipe. Corpus value is high, MA/US-remote relevance is low.
5. **Dover** — smallest yield in the top tier, but sold almost exclusively to
   pre-seed→Series-B startups, i.e. the seed-stage AI roles `ai_jobs_filter.py`
   targets and the corpus barely has.

Drop everything Cloudflare-walled and everything aggregator-shaped. **Consider.co
was the sweep's #3 pick and does not survive verification as a discovery
multiplier** (§3).

---

## 2. All options — full ranked table

Verdicts: **build** / later / skip. Ranked by expected new *startup* jobs per unit
of effort. Nothing is omitted; a skip is a row with a reason.

| # | Source | Family | Access | Verified | Startup density | Est. new jobs | Overlap | Effort | Verdict |
|---|---|---|---|---|---|---|---|---|---|
| 1 | **Wayback CDX apex sweep** (`matchType=domain&collapse=urlkey:30`) | slug_discovery | open_api | ✅ | n/a (multiplier) | unlocks 40k–80k | <5% | **S** | **build first** — but every count is a floor, and one family failed to reproduce (§3) |
| 2 | **Rippling ATS** (`api.rippling.com/platform/api/ats/v1/board/{slug}/jobs`) | ats | open_api | ✅ | high | **~11k** (corrected) | **~15%** (corrected) | M | **build** — new ATS family, no pagination, cheap 404 signal |
| 3 | **YC Work at a Startup** (`ycombinator.com/companies/{slug}/jobs` data-page) | startup_board | html_scrape | ✅ | high | 5k–25k | ~5% | **S** | **build** — WaaS postings are not syndicated to any ATS |
| 4 | **Personio** (`{slug}.jobs.personio.de/search.json` \| `/xml`) | ats | open_api | ✅ (corrected) | med-high (DACH) | 8k–30k | <5% | **S** | **build** — sweep's "blocked" verdict was a caller-IP transient |
| 5 | **Recruitee** slug expansion (`{slug}.recruitee.com/api/offers/`) | ats | open_api | ✅ | med-high | 10k–15k | <5% | **S** | **build** — fetcher already exists in `ats_fetch.py` |
| 6 | **Dover** (`app.dover.com/api/v1/careers-page/{uuid}/jobs`) | ats | open_api | ✅ | **high** (pre-seed→B) | 1.5k–4k | 10–20% | **S** | **build** — smallest yield, best segment fit, <30 min pipe |
| 7 | **Workable per-account** (`apply.workable.com/api/v{1,2,3}/accounts/{slug}/jobs`) | ats | open_api | ✅ | med-high | 18k–28k | 10–25% | M | **build** — 14,382 slugs vs **478** held (not 560) |
| 8 | **SmartRecruiters global index** (`jobs.smartrecruiters.com/sr-jobs/search`) | ats | open_api | ✅ | low-med | company enumerator | — | **S** | **build** — critic's find; `totalFound=347,895`, no 10k window; **dominates the CDX plan for this family** |
| 9 | **Teamtailor** (`{slug}.teamtailor.com/jobs.json`) | ats | open_api | ✅ | med-high (Nordic/DACH) | **~8.5k** (corrected) | <5% | S+ | **build** — slug count is **1,164**, not 7,107 (§3); enrichment pass is mandatory |
| 10 | **SmartRecruiters** slug expansion | ats | open_api | ✅ | low-med | 8k–14k | ~15% | S | **build** — raise `SR_MAX_PAGES` 30→200; superseded in part by #8 |
| 11 | **YC OSS company API** (`yc-oss.github.io/api/companies/all.json`) | slug_discovery | open_api | ✅ | high | 0 direct; 1.5k–4k via resolver | n/a | **S** | **build** — 6,085 domains, daily refresh; durable seed asset |
| 12 | **Domain→ATS resolver** (`{domain}/careers` + regex) | company_signal | html_scrape | ✅ | inherits | 12k–25k boards | 65–75% | M | **build** — 42.5% measured on a cold YC sample; free BuiltWith replacement |
| 13 | **VC `jobs.*` CNAME sweep** (`dig +short CNAME jobs.{vc}`) | slug_discovery | open_api | ✅ | high | 0 direct | n/a | **S** | **build** — 54 domains → 17 Consider + 16 Getro; only enumeration path for Consider |
| 14 | **Common Crawl CDX** | slug_discovery | open_api | ✅ | high (Ashby) | 3k–8k | ~95% vs candidates | M | **build** (top-up) — corrects the repo's "CC is useless" note; true only for Lever (CCBot blocked) |
| 15 | **HN "Who is hiring?"** (Algolia) | company_signal | open_api | ✅ | **highest purity** | 45 slugs/month | 15–25% new | **S** | **build** as monthly cron — comment text is **double**-HTML-escaped (0 hits before, 45 after) |
| 16 | **urlscan.io search** | slug_discovery | open_api | ✅ | medium | 2k–6k | 80–85% | M | **build** (narrow) — only route to custom-domain boards; preserves case (Lever needs it) |
| 17 | **Built In / Built In Boston** | startup_board | jsonld_scrape | ✅ | **high + MA** | 10k–20k (2–6k Boston) | 50–60% | M | **build** — densest MA surface found; `api.builtin.com` is broken, scrape HTML |
| 18 | **Consider.co global board** (`board.id="consider"`) | vc_platform | open_api | ✅ | **low** (corrected) | 45k–60k gross | **93.6%** on held boards | S | **later** — see the overlap trap in §3; do not build as a multiplier |
| 19 | **Breezy HR** slug expansion | ats | open_api | ✅ | low-med | 5k–9k | <5% | S | later — 9,698 probes for SMB rows, no company metadata |
| 20 | **BambooHR** slug expansion | ats | open_api | ✅ | low-med | 25k–40k raw, mostly non-tech | ~5% | M | later — biggest raw haul, worst relevance; gate on a tech-keyword pass |
| 21 | **Pinpoint** (`{slug}.pinpointhq.com/postings.json`) | ats | open_api | ✅ | medium (UK) | 8k–20k | 10–15% | S | later — best structured salary data in the sweep |
| 22 | **Consider per-VC boards** | vc_platform | open_api | ✅ | high nominal | subset of #18 | 70–80% | M | later — build for `search-companies`, not jobs |
| 23 | **Consider `search-companies`** | company_signal | open_api | ✅ | high | 20k–40k companies | — | M | later — `board` key mandatory or HTTP 500 |
| 24 | **jobs.workable.com global index** | aggregator | open_api | ✅ | med-high | 10k–20k | ~70% vs #7 | S | later — freshness layer; cursor param name unproven |
| 25 | **LinkedIn guest jobs API** (`/jobs-guest/jobs/api/seeMoreJobPostings/search`) | aggregator | open_api | ✅ | medium | — | high | S | later — **the only surface in the sweep carrying a real posted date**; ToS-adverse, see §5 |
| 26 | **Torre.ai** (`search.torre.co/opportunities/_search/`) | intl | open_api | ✅ | high (LatAm) | 60k–150k | 20–35% | S | later — big and cheap, geographically irrelevant |
| 27 | **JazzHR** (`{slug}.applytojob.com/apply/`) | ats | html_scrape | ⚠️ | low | 10k–50k, non-tech | <5% | **L** | later — 13,774 slugs but HTML-only (`jobs.rss` 410, `jobs.json` 404) |
| 28 | **JOIN.com** hosted pages | ats | jsonld_scrape | ✅ | med-high (DACH) | 10k–40k (very soft) | <10% | M | later — slug space never enumerated; order-of-magnitude only |
| 29 | **NoFluffJobs** (POST `/api/search/posting`) | intl | open_api | ✅ | med-high (CEE) | 12k–18k | <5% | S | later |
| 30 | **Instahyre** (`/api/v1/job_search/`) | intl | open_api | ✅ | medium (IN) | 6k–7k | <10% | S | later |
| 31 | **GetOnBrd** (`/api/v0/categories/{cat}/jobs`) | intl | open_api | ✅ | med-high (LatAm) | 5k–15k (soft) | <5% | S | later — `total_pages=247` was measured at `per_page=1`; re-measure |
| 32 | **Cert Spotter** CT logs | slug_discovery | open_api | ✅ | medium | hundreds/mo | ~90% | M | later — freshness only; useless against wildcard certs |
| 33 | **SEC EDGAR Form D** (bulk + `efts`) | company_signal | open_api | ✅ | medium | 3k–10k/yr | 30–45% | M | later — catches startups *before* they have a board; no website field, join is fuzzy |
| 34 | **Clearbit/HubSpot autocomplete** | company_signal | open_api | ✅ | n/a | 0 direct | n/a | S | **build as library** — the only name→domain glue; accept only score ≥70 beating runner-up by ≥20 |
| 35 | **GitHub code search** (ATS tokens) | slug_discovery | key_required_free | ❌ 401 | high | 3k–10k | ~70% | M | later — **the only lane that can find Lever slugs CC structurally cannot see**; needs a free PAT |
| 36 | **GitHub org signal** | company_signal | open_api | ✅ | medium | 1k–3k | 60–70% | M | later — 60 req/hr anonymous is unusable; same PAT unlocks it |
| 37 | **Comeet / Spark Hire** | ats | gated | ❌ | high (Israeli VC) | 10k–25k | 20–30% | L | later — needs `{uid}:{token}` from each customer's JS; GitHub code search is the bulk route |
| 38 | **Welcome to the Jungle / Otta** | intl | gated | ❌ | med-high (EU) | 10k–60k | 20–40% | M | later — orgs endpoint open, jobs route 404s; one browser capture resolves the Algolia index name |
| 39 | **The Hub** (`thehub.io/api/jobs`) | startup_board | open_api | ✅ | high (Nordic) | ~1.1k | 15–25% | S | later — tiny; value is the company list |
| 40 | **Landing.jobs** (`/api/v1/jobs`) | intl | open_api | ✅ | med-high (EU) | 2k–5k | 10–20% | S | later — only 4 fields observed; dump a full record first |
| 41 | **PowerToFly** (`/api/v1/jobs/?page=`) | niche_board | open_api | ✅ | **low** | 3k–5k | 50–65% | S | later — easiest integration, worst fit |
| 42 | **Wantedly (JP)** / **Wanted.co.kr** | intl | open_api | ✅ | high (APAC) | unmeasured | <5% | S | later — critic's find; whole APAC region had zero coverage |
| 43 | **Product Hunt API v2** | startup_board | key_required_free | ❌ 401 | high | 1k–4k | 40–55% | M | later — free token, verifiable in minutes |
| 44 | **Berlin Startup Jobs** + WP `/feed/` pattern | startup_board | rss | ✅ | high | 300–1.5k/board | 20–40% | S | later — value is the pattern across ~20 regional boards |
| 45 | **Cryptocurrency Jobs RSS** | niche_board | rss | ✅ | high (crypto) | 500–2k | 50–70% | S | later |
| 46 | **VentureFizz** | niche_board | html_scrape | ⚠️ | high (Boston) | in #17 | — | M | later — pair with Built In as an MA roster |
| 47 | **Jobsoid** | ats | open_api | ⚠️ demo | **low** | 2k–8k | ~0% relevant | S | later (opt-in) — aviation/hospitality/staffing |
| 48 | **Jobvite / Gem / ClearCompany / Paylocity / ADP** | ats | gated | ❌ | low-med | — | — | L | later — critic's find; these are the actual composition of Consider's "startup" slice. Jobvite API 401, Gem 302→login |
| 49 | **SimplifyJobs GitHub lists** | aggregator | open_api | ✅ | medium | 3k–6k | 60–70% | S | **skip for you** — intern/new-grad lane |
| 50 | **Wellfound / AngelList** | startup_board | blocked | ❌ | high | 20k–40k | 40–60% | XL | **skip** — DataDome on `/jobs`, `/graphql`, even `sitemap.xml` |
| 51 | **workatastartup.com direct** | startup_board | blocked | ❌ | high | 0 over #3 | n/a | XL | **skip** — 406/302/422; superseded by the ycombinator.com mirror |
| 52 | **Startup.jobs** | startup_board | blocked | ❌ | high | unknown | unknown | XL | **skip** — Cloudflare domain-wide including `/feed` |
| 53 | **TrueUp.io** | aggregator | blocked | ❌ | med-high | unknown | >80% | XL | **skip** — blocked *and* near-total duplication |
| 54 | **Simplify.jobs** | aggregator | blocked | ❌ | medium | ~0 | 85%+ | L | **skip** — classic aggregator trap |
| 55 | **Remote Rocketship** | niche_board | blocked | ❌ | medium | <2k | 70–85% | L | **skip** — landing 200 but zero job hrefs |
| 56 | **Climatebase** | niche_board | blocked | ❌ | med-high | 3k–10k | 55–75% | L | **skip** — siblings dead too (workonclimate 404, terra.do 404) |
| 57 | **CryptoJobsList / web3.career / EU-Startups / SwissDevJobs** | niche_board | blocked | ❌ | high nominal | few k | 60–85% | M | **skip** — Cloudflare; aggregators over held ATS |
| 58 | **aijobs.net** | niche_board | html_scrape | ❌ | medium | 500–2k | 60–80% | M | **skip** — `ai_jobs_filter.py` already yields 13.2k |
| 59 | **Dice (DHI)** | aggregator | key_required_paid | ❌ | low | low | high | L | **skip** — public key dead |
| 60 | **Crunchbase v4** | company_signal | key_required_paid | ❌ | high | 50k+ if bought | 50–60% | XL | **skip** — paid; redistribution ToS risk |
| 61 | **OpenVC / Dealroom / Tracxn / PitchBook** | company_signal | gated | ❌ | high | 0 | n/a | XL | **skip** — 403/paywalled |
| 62 | **Zoho Recruit** | ats | html_scrape | ❌ | low | 5k–20k agency reposts | high | L | **skip** — per-tenant themes defeat one parser |
| 63 | **Workday CXS** | ats | open_api | ✅ | **near-zero** | 500k+ enterprise | dilutive | L | **skip** — 1,000+ employee floor; `total` caps at 2000 |
| 64 | **Softgarden** | ats | html_scrape | ❌ | low-med | 10k–30k | <5% | L | **skip** — every `/api/rest/v3/*` 404s; German-only |
| 65 | **Freshteam** | ats | key_required | ❌ | med (historic) | <2k | n/a | M | **skip** — EOL, tenants 404ing |
| 66 | **Getro fund boards** (Techstars, Antler, Menlo, SignalFire, Uncork, Khosla, GC, Insight, Craft, Thrive) | vc_platform | open_api | ✅ | high | **~0** | ~100% | S | **skip** — already in `getro_job_boards.csv`; write an audit script instead |
| 67 | **Getro `search/companies`** sub-surface | vc_platform | gated (cold) | ❌ | high | 10k–25k | 60–75% | M | later — Cloudflare from cold curl, but the repo's own pipeline gets past it: header problem, not a block |
| 68 | **Lever EU host** (`api.eu.lever.co`) | ats | open_api | ✅ neg | n/a | **0** | ~100% | S | **skip** — 28 slugs probed, zero EU-only boards |
| 69 | **Ashby EU host** | ats | — | ✅ neg | n/a | 0 | — | S | **skip** — `api.eu.ashbyhq.com` does not resolve |
| 70 | **crt.sh tenant enumeration** | slug_discovery | open_api | ✅ neg | n/a | ~0 | n/a | M | **skip** — wildcard certs hide every tenant; crt.sh 502'd 3 of 4 attempts |
| 71 | **Passive DNS** (subdomain.center, HackerTarget) | slug_discovery | open_api | ✅ | medium | <500/family | >95% | S | **skip** — 20–45× worse than one Wayback request |
| 72 | **Public SEO/search APIs** (SearXNG, PublicWWW, my.greenhouse.io) | slug_discovery | blocked | ❌ | n/a | **0** | n/a | M | **skip** — and no ATS publishes a customer sitemap (all four checked) |
| 73 | **schema.org JSON-LD broad sweep** | company_signal | jsonld_scrape | ❌ | high nominal | 2k–8k | 70–90% | XL | **skip as a sweep** — anthropic.com/jobs, ashbyhq/openai, notion.com/careers all returned **0** JobPosting blocks |
| 74 | **TechCrunch / Axios funding RSS** | company_signal | rss | ✅ | medium | 200–800/yr | ~90% | S | **skip** — two compounding fuzzy stages, negative ROI |
| 75 | **Wikidata SPARQL** | company_signal | open_api | ✅ | **low** | <500 | 95%+ | S | **skip** — notability-gated; returns Google/Intel/Apple |
| 76 | **NoDesk / Jobspresso RSS** | niche_board | rss | ✅ | medium | ~10/poll | 60–70% | S | **skip as volume** — freshness ping at best; Jobspresso already in the inventory |
| 77 | **Underdog.io** | startup_board | — | ❌ | none | **0** | n/a | S | **skip — verified dead.** Static Webflow page; a scraper would push marketing copy into the job schema and it would pass every downstream `title != ''` check |
| 78 | **Pallet** | vc_platform | — | ❌ | none | **0** | n/a | XL | **skip — verified dead.** Pivoted to AI logistics; `jobs.pallet.com` fails DNS |
| 79 | **TalentWall / Teamable / Vouch / Cardinal** | vc_platform | n/a | ❌ | n/a | **0** | n/a | S | **skip** — not board platforms at all (pipeline analytics, sourcing CRM, insurance co, fund site) |
| 80 | **Accel / Index / Founders Fund / Redpoint / Benchmark** vanity hosts | vc_platform | blocked | ❌ | high if real | **0** | n/a | L | **skip** — 4 of 5 fail DNS. But: `jobs.nea.com` does not resolve while Consider board id `nea` serves 1,082 jobs — **never gate slug validation on the hostname resolving** |
| 81 | **Key Values / Cord / Hired / Tech Ladies / Diversify Tech** | startup_board | blocked | ❌ | medium | 0 verified | unknown | XL | **skip** — 503/404/403/JS-shell |

---

## 3. Where the sweep was wrong (verification pass)

These corrections are already folded into the table above. Recorded so nobody
re-derives the optimistic version.

1. **Rippling is ~11k jobs, not 20–60k.** 120-slug random sample probed live:
   85 live / 29 empty / 6 dead, 582 deduped jobs → **6.8 jobs per live board ×
   2,287 slugs ≈ 11,092**. Endpoint itself confirmed (200, 256 KB on
   `/board/rippling/jobs`, duplicate-uuid behavior real).
2. **Rippling overlap is 14.9%, not ~0.7%.** Master already holds 340 distinct
   `ats.rippling.com` boards / 2,586 rows, syndicated via Getro. The "17 held"
   figure came from a board list, which cannot see Getro syndication.
3. **Personio is not blocked.** `1komma5grad.jobs.personio.de/search.json` →
   200, 182 KB of job JSON with no headers at all; `/xml` → 200, 2.0 MB. The
   "Vercel Security Checkpoint" body reproduced on an unrelated host, so it was
   a caller-IP transient. Kill the headless-browser spike; this is an S pipe.
4. **Teamtailor's slug count does not reproduce.** Running the sweep's own spec
   yields **1,164** slugs, not 7,107. 70-slug live sample → 36 boards with jobs,
   510 jobs → **~8,481 extrapolated**. Note the sweep mandated a "fail if not
   within 20% of baseline" gate that its own baseline would trip.
5. **Consider's staff-count filter does not carve startups.** Totals are real
   (744,430 unfiltered / 128,844 at 1–1000 / 28,428 at 1–100) but the field is
   junk: 24 rows in a 200-row "1–10 + 10–100 employees" sample are **Takeda with
   `companyStaffCount: 9`**, with `takeda.wd3.myworkdayjobs.com` apply URLs.
6. **The Workable v2/v3 discrepancy is a phantom.** v1, v2 and v3 return
   identical payloads on real slugs; `hotjar`/`deel` are simply empty accounts.
   Held count is 478, not 560.
7. **The YC gotchas were inverted.** `--compressed` is *not* mandatory
   (uncompressed → 200/240 KB). The real killer is the Accept header, which the
   sweep never mentioned: `Accept: application/json` → **404**;
   `Accept: text/html` → 200 with the data-page and 59 job postings.
   `props.jobsCount` was `null`, not the string `"thousands"`. And the master
   already holds 417 `workatastartup` + 859 `ycombinator.com` URLs, so "cannot
   already be held" is too strong.
8. **Held-slug baselines are inflated across the board** (measured from master):
   personio 257 (claimed 309) · bamboohr 631 (764) · breezy 269 (376) ·
   applytojob 433 (491) · recruitee 140 (154) · teamtailor 126 (137).

### The overlap traps

- **Consider is the trap.** Resolving all 400 jobs from both startup stage slices
  against the repo's real board lists: `gh_HELD=68, ashby_HELD=51, lever_HELD=12,
  ashby_NEW=9, other=260`. So **131 of 140 ATS-resolvable rows (93.6%) sit on
  boards already held**, and the new-mapping rate is **9/400 = 2.3%** — not the
  claimed 15k–25k new company→ATS maps. The sweep measured *"applyUrl hits a
  known ATS host"* (60%) and silently reused it as *"new"*. The other 65% is
  Workday / Jobvite / governmentjobs.com / ADP / Paylocity — dilutive enterprise
  rows.
- **Corpus mining is already exhausted for the big three.** Mining all 978,547
  Getro rows for embedded ATS slugs: greenhouse 2,084 seen / **6 new**, ashby
  1,746 / **41 new**, lever 695 / **4 new**. It works for Rippling; do not
  generalize it.
- **SmartRecruiters global index is itself a partial trap** — 347,895 jobs, but
  page-1 companies are Accor, Domino's, Experian, Equinox. Use it as a *company
  enumerator* replacing the CDX plan for that family, not as a job source.

---

## 4. Suggested pipes

House conventions: `<fam>_discover_boards.py` → `<fam>_probe_boards.py` →
`<fam>_fetch_jobs.py` → `<fam>_build_sheet.py`, resumable via
`<fam>_*_state.json`, rows through `contract_common.cc.row(...)`, wired into
`run_all.py` (`FAMILIES` + `rotate_family` + Step chain) and `all_jobs_build.py`
(a `load_<fam>` reader + registration in `main()`).

### Pipe 1 — `slug_discovery` (the multiplier, build first)

**Endpoint:** `http://web.archive.org/cdx/search/cdx?url={apex}&matchType=domain&fl=urlkey&collapse=urlkey:30&limit=300000`
(path-per-customer variant: `url=apply.workable.com*&collapse=urlkey:40`).

- **`slug_common.py`** (library) — `APEXES` table + SURT `urlkey` parser +
  blacklist. Verified config: `breezy.hr/sub/30`, `recruitee.com/sub/30`,
  `applytojob.com/sub/30`, `bamboohr.com/sub/30`, `teamtailor.com/sub/30`,
  `jobs.personio.de/sub/35`, `careers.smartrecruiters.com/path/45`,
  `ats.rippling.com/path/40`, `apply.workable.com/path/40`.
  urlkey is SURT (`hr,breezy,acme)/p/123`) — split on `)`, for `sub` mode require
  `labels[:n] == reversed(apex.split('.'))` and keep depth 0 only. Blacklist ~60
  tokens plus `re.fullmatch(r'[0-9a-f]{8,}', slug)` and `len < 2`.
- **`slug_discover_boards.py`** → `slug_candidates.csv`, `slug_raw/<fam>.urlkeys.txt`.
  Stream to disk (`stream=True`, `iter_lines()`, `os.replace` on clean EOF —
  breezy is 11.2 MB / 190,981 rows). On timeout/5xx: 3× with 30 s backoff, then
  year-chunk `&from=YYYY0101&to=YYYY1231` for 2013..2026. **Do not raise `limit`**
  — CDX silently truncates.
- **`slug_fanout.py`** → per-family `<fam>_candidates.csv` + `ats_slugs_expanded.json`,
  plus a 3-line patch to `ats_fetch.py` to deep-merge `*_ats_slugs.json` over
  `getro_ats_slugs.json`. The existing per-(platform,slug) state key makes the
  enlarged list resumable for free.

**Validation gate:** assert parsed counts within 20% of baselines and fail loudly
— a mis-tuned `collapse:N` silently *drops* slugs sharing a prefix. **Re-measure
the baselines first**: the teamtailor figure did not reproduce (7,107 → 1,164).

`run_all.py`: add `"slugs"` to FAMILIES as the **first** discover chain; the
ats/rippling/teamtailor chains wait on it via the `threading.Event` mechanism
`main()` already uses for the ats-after-getro dependency. Never delete
`slug_raw/*.urlkeys.txt` on rotate. `all_jobs_build.py`: no wiring — zero job rows.

**Runtime:** 15–25 min cold for all 9 apexes; <1 min warm reparse.

### Pipe 2 — `rippling`

**Endpoint:** `GET https://api.rippling.com/platform/api/ats/v1/board/{slug}/jobs`
— unauthenticated, flat JSON array, **no pagination**, whole board in one call.
Do not hit `ats.rippling.com` (Cloudflare-fronted HTML). Bad slug → 404
`{"error_code":"RESOURCE_NOT_FOUND"}`, so probing is cheap and unambiguous.

- **`rippling_discover_boards.py`** — CDX apex sweep (2,287 slugs) +
  `getro_ats_slugs.json['rippling']` + `grep -ohE 'ats\.rippling\.com/[A-Za-z0-9_-]+'`
  over `all_jobs_master.csv` (Getro syndicates Rippling apply URLs, and this
  source has **no recency bias** unlike CDX) + optional urlscan.
- **`rippling_probe_boards.py`** — `ThreadPoolExecutor(10)` + the
  `greenhouse_fetch_jobs.throttle()` token bucket at RPS=6. Classify from the
  **body, not the status**: `200 + non-empty` → live (`len({j['uuid'] …})` —
  the `/rippling/` board returns the same uuid twice); `200 + []` → **live-but-empty,
  never mark dead** (~32% land here and some refill); 404 → dead.
- **`rippling_fetch_jobs.py`** — verbatim `greenhouse_fetch_jobs.py` skeleton
  (8 workers, RPS=6, 6 retries, `_pause_until += 20s` on 429/403).
  **Run `--dump-sample <slug>` once before writing the mapper** — only `uuid`,
  `name`, `department{id}` are confirmed from live payloads; `workLocation`,
  `employmentType`, `createdAt` are best-effort.
- **`rippling_build_sheet.py`** —
  `apply_url = job.get('url') or f"https://ats.rippling.com/{slug}/jobs/{uuid}"`.
  **Load-bearing**: `all_jobs_build.canon_key` already has an
  `ats.rippling.com` branch, so tier-1 dedupe against Getro's 2,586 syndicated
  copies is free with zero parser changes.

`run_all.py`: rotate `rippling_all_jobs.jsonl` + `rippling_fetch_state.json`, but
**not** `rippling_probe_state.json` (probes are expensive, slugs churn slowly).
`all_jobs_build.py`: `load_rippling()` modeled on `load_greenhouse`,
`source='rippling'`, `source_type='ats'`.

**Runtime:** under 20 min cold. **Risk:** field-name risk, not access risk — a
naive parser yields thousands of rows with empty `location`, invisible to the
MA/remote and AI filters.

### Pipe 3 — `yc` (Y Combinator WaaS)

**Endpoint:** `GET https://www.ycombinator.com/companies/{slug}/jobs` with
**`Accept: text/html`** (`application/json` returns 404 — this is the gotcha that
kills a naive build). Job list is inline in the `data-page="…"` Inertia attribute:
`html.unescape()`, `json.loads`, read `props.jobPostings[]`. `props.signedIn` is
false — no login. 404 on some slugs is normal (`mercury`), treat as empty.

**Slug source:** `https://yc-oss.github.io/api/companies/hiring.json` (1,494
`isHiring`) or `all.json` (6,085). Hard-fail if `len(all) < 5000` — GitHub Pages
serves stubs. Filter `status == 'Active'` before probing.

Scripts: `yc_discover_boards.py` → `yc_probe_boards.py` (6 workers, RPS=4, cache
parsed `props` to `yc_pages.jsonl` so the fetcher never re-requests) →
`yc_fetch_jobs.py` → `yc_build_sheet.py`.

**`canon_key` patch required** so the two URL forms collapse:

```python
host == 'account.ycombinator.com'
    → jid = parse_qs(sp.query).get('signup_job_id', [''])[0]; plat = 'ycwaas'
host == 'ycombinator.com' and parts[:1] == ['companies'] and len(parts) >= 4 and parts[2] == 'jobs'
    → plat, org, jid = 'ycwaas', parts[1], parts[3].split('-')[0]
```

Keep the rich fields: `salaryRange`, `equityRange`, `minExperience`, `visa`,
`role`, `roleSpecificType`, `locationType`.

**Runtime:** ~10–35 min. **Risk:** single-vendor HTML surface with no contract —
make "count of slugs with no `data-page` attribute" a run-level assertion so an
Inertia version bump fails loudly instead of emitting zero. Volume extrapolated
from a 7-slug sample with huge variance (encord 59, gusto 0); floor ~5k.

### Pipe 4 — `personio`

**Endpoint:** `GET https://{slug}.jobs.personio.de/search.json` (JSON, verified
182 KB / 296 jobs) with `/xml` as fallback (2.0 MB). No headers required.
Slugs from the CDX apex sweep (`jobs.personio.de/sub/35`, ~6,249 candidates vs
257 held). Standard four-script shape.

Corpus value is high at <5% overlap, but the population is DACH — **tag the rows
so `ai_ma_remote.py` and `vet_build.py` exclude them by default**, or they inflate
headline counts without improving your vet queue.

### Pipe 5 — `dover`

**Endpoint:** `GET https://app.dover.com/api/v1/careers-page/{uuid}/jobs`,
DRF-paginated `{"count","next","previous","results"}`.

**The gotcha that defines the pipe:** you must send `Accept: application/json`.
Without it the same path returns the React SPA shell **with HTTP 200** — a
status-only health check reports success and yields zero jobs. Guard:
`if not ctype.startswith('application/json') or r.text.lstrip()[:1] == '<': treat as dead`.

**Discovery:** CDX prefix over **both** hosts (`app.dover.com`, `app.dover.io`),
regex `/[^/]+/careers/[0-9a-f-]{36}` → 165 + 127 = 292 UUIDs. There is **no
slug→uuid lookup**, so UUIDs must come from discovery. Always probe via
`app.dover.com` even for `.io`-discovered UUIDs. `count==0` after 4 consecutive
probes → mark `retired`.

Place `('dover', …)` **early** in `all_jobs_build`'s input list so its apply_url
wins first-writer on merged rows — Dover is the system of record, Getro mirrors.

**Runtime:** under 30 min cold. The cheapest pipe in the sweep.

### Pipe 6 — `ats_expand` (Recruitee + SmartRecruiters + Breezy)

No new fetcher — `ats_fetch.py` already owns all three endpoints. The entire
delta is slug coverage.

| Family | Endpoint | Slugs found | Held | Dead signal |
|---|---|---:|---:|---|
| Recruitee | `https://{slug}.recruitee.com/api/offers/` | 7,210 | 140 | 404 |
| SmartRecruiters | `https://api.smartrecruiters.com/v1/companies/{slug}/postings?limit=100&offset=` | 3,757 | 336 | 404 |
| Breezy | `https://{slug}.breezy.hr/json` | 9,698 | 269 | **302** (do not follow) |

**Two mandatory patches to `ats_fetch.py`:** merge `*_ats_slugs.json` over
`getro_ats_slugs.json`; **raise `SR_MAX_PAGES` 30→200** (SmartRecruiters
customers are larger and the cap silently truncates at 3,000 postings) *and* add
a per-platform throttle — `api.smartrecruiters.com` is the one host that 429s
under the current 14-worker fan-out.

**Risk:** `ats_fetch.py` is one script covering five platforms with one shared
state file. Adding ~20k slugs turns a 20-min step into a multi-hour one and one
bad platform stalls the rest. If that materializes, split by platform
(`env ATS_PLATFORMS=recruitee`) into parallel chains rather than raising `WORKERS`.

### Pipe 7 — `smartrecruiters_global` (replaces the CDX plan for this family)

`GET https://jobs.smartrecruiters.com/sr-jobs/search?limit=100&offset=` — 200,
`totalFound=347,895`, and **no 10k result window** (offset=20000 still returns
rows). Each page yields ~30 distinct `company.identifier` values plus `applyUrl`,
`releasedDate`, `location`. Use it as a **company enumerator** feeding
`ats_fetch.py`, not as a job source — page-1 companies are Accor, Domino's,
Experian, Equinox. Note `api.smartrecruiters.com/v1/postings` (the guessable
global) does **not** exist — 404.

### Supporting libraries worth writing once

- **`domain_ats_resolve.py`** — `GET {domain}/careers|/jobs|/company/careers` +
  regex for 12 ATS hosts. **42.5% measured** on a cold 40-company YC sample
  (uplane→ashby, instrumentl→lever, prolific→greenhouse, carbonfact→teamtailor).
  Adding `careers.{d}`/`jobs.{d}` subdomain probes should push past 60%.
- **`name_to_domain.py`** — `autocomplete.clearbit.com/v1/companies/suggest?query=`.
  The only name→domain glue, and the enabler for the SEC Form D chain. Fuzzy and
  unranked ("ramp" also returns rampant.tv, rampslogistics.com) — accept only
  when top score ≥70 **and** beats the runner-up by ≥20. Cache everything.
- **`hn_whoishiring.py`** — `hn.algolia.com/api/v1/search_by_date?tags=story,author_whoishiring`
  → `?tags=comment,story_{id}`. 45 distinct ATS slugs in the July 2026 thread
  (29 Ashby, 12 Greenhouse, 4 Lever). **Comment text is HTML-escaped twice** —
  `html.unescape()` twice or the regex returns literally zero (verified: 0 before,
  45 after). `/items/{id}` truncates (276 of 436 comments); use the paginated
  comment search.
- **`vcdns_sweep.py`** — `dig +short CNAME jobs.{vc}` over a VC domain list.
  54 domains → 17 Consider (`boards.consider.com`) + 16 Getro (`ns*.getro.com`,
  legacy `ns*.monday.vc`). The only enumeration path for Consider boards.
  Absence of a CNAME ≠ no board.
- **`getro_coverage_check.py`** — 2 minutes to write, prevents someone rebuilding
  the Techstars/Antler/Menlo/SignalFire boards already in `getro_job_boards.csv`.
  Always assert HTTP 200 before fingerprinting HTML.

---

## 5. Known gaps this sweep did not close

- **Posted dates.** Consider, YC WaaS and Torre carry no posted date, and undated
  rows survive `POSTED_SINCE` forever — they will silently pollute
  `ai_ma_remote.py` and `vet_build.py`. Either implement snapshot-diff freshness
  (first-seen date on ingest) or tag undated sources for exclusion. **Decide this
  before building YC or Consider**, not after.
  The only surface found that carries a real posted date for otherwise-invisible
  startups is the **LinkedIn guest endpoint** (row 25) — worth knowing it exists,
  but scraping it runs against LinkedIn's terms of service and they enforce
  actively, so it is not a sustainable pipeline dependency. Snapshot-diffing your
  own fetches is the durable answer.
- **APAC has zero coverage.** Wantedly (JP) and Wanted.co.kr both return clean
  JSON unauthenticated. Low relevance for a Boston/US-remote lens.
- **A free GitHub PAT unlocks three things at once** — code search (the only lane
  that can find Lever slugs, since CCBot is blocked on `jobs.lever.co`), the
  GitHub org signal, and the Comeet token harvest. Ten minutes of setup. Note
  `gh auth status` currently reports the token in default is invalid.

---

## 6. Sequencing

**Batch 1 — ~1 day, highest measured return**

| # | Work | Effort | Expected |
|---|---|---|---|
| 1 | `slug_common.py` + `slug_discover_boards.py` + `slug_fanout.py` + `ats_fetch.py` merge patch | S (~3 h build, 25 min run) | re-measured slug base for 9 families; unlocks everything below |
| 2 | `rippling` pipe (4 scripts cloned from `greenhouse_*.py`) | M (~4 h, 20 min run) | ~11k jobs, ~15% overlap → **~9k net new** |
| 3 | `yc` WaaS pipe + `yc-oss` seed + `canon_key` ycwaas patch | S (~3 h) | 5k–25k, ~0% overlap, exactly the target segment |
| 4 | Recruitee + SmartRecruiters discover/probe (+ `SR_MAX_PAGES` and throttle) | S (~2 h) | 18k–29k |

**Batch 2 — ~1 day, fills the startup-density gap**

`dover` · `personio` · `domain_ats_resolve.py` over YC's 6,085 domains ·
`teamtailor` with enrichment · `vcdns_sweep.py` · HN Algolia monthly cron ·
Workable per-account · Built In / Built In Boston.

**Maybe later**

Breezy + BambooHR + JazzHR slug expansion (40k–90k raw but overwhelmingly
SMB/non-tech; gate BambooHR behind a tech-keyword pass) · Pinpoint ·
jobs.workable.com global index · Common Crawl top-up · urlscan custom-domain pass ·
Cert Spotter freshness cron · SEC Form D + `name_to_domain.py` · a GitHub PAT ·
the intl batch (Torre, NoFluffJobs, Instahyre, GetOnBrd, The Hub, Landing.jobs,
Wantedly, Wanted.co.kr) — all cheap, all essentially net-new, all geographically
irrelevant to a Boston/US-remote lens, and Spanish/Polish/German titles will
silently defeat every English keyword filter in the repo.

**Do not build**

Wellfound · workatastartup.com direct · Startup.jobs · TrueUp · Simplify ·
Remote Rocketship · Climatebase · CryptoJobsList tier · aijobs.net · Dice ·
Crunchbase · OpenVC/Dealroom/Tracxn/PitchBook · Zoho Recruit · Softgarden ·
Freshteam · Workday CXS · Underdog.io · Pallet · TalentWall/Teamable/Vouch/Cardinal ·
Accel/Index/Founders Fund/Redpoint/Benchmark vanity hosts · Lever EU host ·
Ashby EU host · crt.sh · passive-DNS aggregators · SearXNG/PublicWWW · Wikidata ·
broad JSON-LD sweep · TechCrunch/Axios funding RSS · NoDesk/Jobspresso as volume.
