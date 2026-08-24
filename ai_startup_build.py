#!/usr/bin/env python3
"""
ai_startup_build.py — rank the US-remote AI/FDE startup lane.

  python3 ai_startup_build.py [--db triage.db] [--dry-run] [--show-killed]
                              [--show-dropped] [--top N] [--per-company N]

Writes `startup_score` / `startup_why` on feed='main', bucket='Remote' rows in
AI bands 1-2 whose company carries a startup tier.  triage/lanes.py has the
matching lane (`aistartup`) and sorts it on startup_score DESC.

WHY THE COMPANY TIER IS PROPAGATED, AND WHY THAT IS THE WHOLE POINT.
`startup_tier` is written by startup_build.py, which only ever reaches sources
it classifies — the employer-direct ATS families.  Aggregators never receive
one.  Measured on the 2026-08-15 corpus, live US-remote rows in AI bands 1-2:

    himalayas   810 rows, 0 tiered      getro   528 rows, 212 tiered
    jobgether   233 rows, 0 tiered      ashby   135 rows, 114 tiered
    torre       150 rows, 0 tiered      greenhouse 56 rows, 54 tiered
    remoteio     99 rows, 0 tiered      lever    43 rows,   9 tiered
    wttj         83 rows, 0 tiered      rippling 21 rows,  21 tiered

So the obvious lane — `bucket='Remote' AND rank_tier<=2 AND startup_tier<>''` —
is not "startup jobs".  It is "startup jobs that happen to have been posted on
an ATS somebody wrote a fetcher for", and it silently returns nothing at all
from the single largest source of remote AI roles in the corpus.  That is the
same shape as the EXCL_SRC mistake: a column that only DENIES a tier read as if
it were a statement about the job.

Startup-ness is a property of the COMPANY, not of the board the posting came
from.  8,520 companies carry a tier somewhere in the corpus, so a himalayas
posting from a company getro already tiered `1 Funded` is a funded-startup job
and is admitted as one.  That is 138 -> 349 undecided rows, 179 of the
recovered ones tier 1, and it is what puts fifteen sources in the lane instead
of eight.

INHERITED EVIDENCE IS SCORED LOWER THAN DIRECT, never treated as equal.  The
company matched by `company_norm`, which is a normalised name and not an
identifier: two small companies can share one.  Same principle as `basis_weak`
in lanes.ORDER_TERMS — the weaker evidence is admitted, labelled, and sorted
behind the proven rows rather than being thrown away or quietly mixed in.

HOW THIS DIFFERS FROM the per-metro lane builder, which it is otherwise modelled on.  The
`sfl` lane is a predicate over `bucket`, so rows the title filter kills stay IN
the lane at geo_score 0 and sort to the bottom.  This lane's predicate IS
`startup_score > 0`, so a killed row is not in it at all.  That is deliberate —
this lane promises AI/FDE startup jobs, not a full sweep of a metro — but it
means the kill filter is load-bearing here in a way it is not there, so every
run prints what it removed and --show-killed reads it back.

Every admitted row scores at least 25 (band-2 AI 18 + weakest inherited tier 3
+ inferred-US geo 4), so `startup_score > 0` and "admitted to this lane" are the
same statement and no row can land in the lane carrying no number and no reason.

RUN IT AFTER EVERY `triage_load.py --feed main`, for the same reason
lanes_build.py and the per-metro lane builder have to be: the load upserts startup_score from
MA_US_Remote_<date>.csv, which has no such column, so every main load blanks
this lane.  Order is: triage_load.py -> lanes_build.py -> the per-metro lane builder ->
ai_startup_build.py.
"""

import argparse
import datetime as dt
import sqlite3
import sys
from collections import Counter

import triage_load
from vocab import (DB, DOMAIN, comp_fit, company_fit, domain_fit,
                         geo_fit, killed_by, title_fit)
from vocab import fresh_wide

BUCKET = "Remote"

# AI bands 1-2 only: "1 Claude / agentic / FDE" and "2 AI engineer / LLM /
# GenAI".  Bands 3-5 ("Other core AI", "AI-ML adjacent", "AI tooling in
# skills") are what `ai_all` is for — band 4 alone is 1,862 remote rows of
# data/ML work that is not AI-building.
AI_MAX_TIER = 2

# What an inherited tier is worth against a direct one.  0.6 rather than a flat
# demotion by one tier: a `1 Funded` company seen through an aggregator posting
# (12.0) still outranks a directly-tiered `2 VC-backed` one (15.0)... it does
# not, and that is the intended answer.  Direct evidence about a weaker startup
# beats indirect evidence about a stronger one, because the failure mode being
# guarded against is a company_norm collision, which makes the inherited claim
# wrong rather than merely weak.
INHERIT = 0.6


# ---------------------------------------------------------------------- scoring
def ai_fit(ai_band):
    """0-30 for AI strength, read off the FIRST CHARACTER of `ai_band`.

    Deliberately NOT vocab.ai_fit, which spreads bands 1-5 over 15..2.
    This lane only admits bands 1 and 2, so that function's whole spread here
    would be 15 vs 12 — three points out of ~130, which cannot lift a Forward
    Deployed Engineer above a generically-titled LLM role with a fresher date.
    Band 1 IS the target role, so it gets a spread that can decide the sort.

    `ai_band` not `ai_tier`: ai_tier is a text label ("Core AI Building"), and
    reading it where a number belongs is the bug triage_load.py warns
    about — it silently ranks every AI role as the weakest one.
    """
    b = (ai_band or "").strip()[:1]
    if not b.isdigit():
        return 0, ""
    pts = {1: 30, 2: 18}.get(int(b), 0)
    return pts, {1: "FDE / agentic", 2: "AI engineer"}.get(int(b), "") \
        if pts else ""


def startup_fit(tier, inherited):
    """0-20 for startup evidence, halved-ish when the tier came from the company
    rather than from the posting.

    vocab.company_fit stops at tier 3 because `picks` never admits a
    tier-4 row.  This lane does — this lane's gate is "carries a tier", and a
    tier-4 row dropped here would be dropped for the very evidence that let it
    in — so tier 4 is added the way vocab.startup_fit adds it, by wrapping
    rather than by widening company_fit for the twelve lanes that import it.

    Rescaled above company_fit's 12/9/5 so the startup axis is not swamped by
    title_fit's 40: this is the lane where being a startup is half the ask.
    """
    pts, label = company_fit(tier or "")
    if pts:
        pts = {12: 20, 9: 15, 5: 9}[pts]
    elif (tier or "").startswith("4"):
        pts, label = 5, "startup 4"
    if not pts:
        return 0, ""
    if inherited:
        return round(pts * INHERIT, 1), label + " via company"
    return pts, label


def score_row(r, tier, inherited, today):
    """(score, why) — every point traceable to a phrase in `why`.

    geo_fit is IN, unlike the wide metro lane's score, and it is not the constant it looks
    like.  Every row here is bucket='Remote', so its first leg never fires — but
    its second one does: 27 of 339 admitted rows are `basis_weak`, meaning
    ma_remote_build could only INFER the US basis from a US-posting company or a
    USD salary rather than read it off the posting.  geo_fit scores those 4
    against 7 and, more importantly, LABELS them "remote (inferred US)" on the
    card.

    lanes.AISTARTUP_TERMS carries `basis_weak ASC` as well, and that is not a
    duplicate of this.  In a score-ordered lane the sort term is nearly dead —
    scores are near-unique, so it only breaks exact ties — which is why the
    distinction has to be IN the score to have any effect at all.  A reader who
    saw only the ORDER BY would reasonably conclude the lane puts proven rows
    ahead of inferred ones, and by itself it does not.
    """
    parts, total = [], 0.0
    dom = bool(DOMAIN.search(f"{r['title']} {r['skills'] or ''} "
                             f"{r['company_industry'] or ''}"))
    for pts, label in (
        title_fit(r["title"] or "", dom),
        domain_fit(r["title"] or "", r["skills"] or "",
                   r["company_industry"] or ""),
        ai_fit(r["ai_band"] or ""),
        startup_fit(tier, inherited),
        geo_fit(r["bucket"] or "", r["basis_weak"]),
        fresh_wide(r["posted"] or "", today),
        comp_fit(r["salary_max"]),
    ):
        if pts:
            total += pts
            parts.append(f"{label} +{pts:g}")
    return round(total, 1), " · ".join(parts)


# ------------------------------------------------------------------------ main
# MIN(startup_tier) is the STRONGEST tier, not the weakest: the values are
# '1 Funded' < '2 VC-backed' < '3 Startup ATS' < '4 SMB ATS' and they sort
# lexically in strength order because startup_build.py numbered them that way.
#
# Unscoped by feed and by `live` on purpose.  A retired posting still proves
# what the company was, and the contract feed's tiers are evidence about the
# same companies — this map answers "is this company a startup", a question
# that has nothing to do with which posting happens to be open today.
TIER_MAP_SQL = """
SELECT company_norm, MIN(startup_tier) AS tier
  FROM jobs
 WHERE startup_tier <> '' AND company_norm <> ''
 GROUP BY company_norm
"""

# Aggregators are NOT excluded — that is the entire point of the lane, and
# himalayas alone is 810 of the remote AI rows.  They sort behind
# employer-direct rows instead, on the is_agg term in lanes.AISTARTUP_TERMS.
CAND_SQL = f"""
SELECT job_key, ct_key, company, company_norm, title, skills, company_industry,
       startup_tier, ai_band, bucket, basis_weak, posted, salary_max, source,
       is_agg
  FROM jobs
 WHERE feed = 'main' AND live = 1 AND bucket = '{BUCKET}'
   AND rank_tier <= {AI_MAX_TIER}
   AND NOT EXISTS (SELECT 1 FROM decisions d WHERE d.job_key = jobs.job_key)
   AND NOT EXISTS (SELECT 1 FROM decisions d2
                    WHERE d2.ct_key = jobs.ct_key AND jobs.ct_key <> '')
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB)
    ap.add_argument("--top", type=int, default=0,
                    help="cap the lane (0 = no cap, the default)")
    ap.add_argument("--per-company", type=int, default=0,
                    help="cap rows per company (0 = no cap)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--show-killed", action="store_true",
                    help="print what the title filter excluded — read this "
                         "before trusting the filter")
    ap.add_argument("--show-dropped", action="store_true",
                    help="print the AI rows with no startup evidence at all")
    args = ap.parse_args()

    today = dt.date.today()
    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    # This builder introduced startup_score/startup_why, so unlike the per-metro lane builder it
    # cannot assume its column already exists: an existing triage.db only grows
    # them when triage_load.migrate() runs, and requiring a full main load
    # before the lane can be built for the first time would be a dependency
    # nothing states.  migrate() is idempotent and adds only what is missing.
    triage_load.migrate(con)

    tier_map = {r["company_norm"]: r["tier"]
                for r in con.execute(TIER_MAP_SQL)}
    rows = con.execute(CAND_SQL).fetchall()
    print(f"{len(tier_map):,} companies carry a startup tier somewhere in the "
          f"corpus")
    print(f"{len(rows):,} undecided, live US-remote rows in AI bands 1-"
          f"{AI_MAX_TIER}")

    scored, killed, dropped = [], [], []
    direct = inherited_n = 0
    for r in rows:
        tier, inherited = r["startup_tier"] or "", False
        if not tier:
            tier = tier_map.get(r["company_norm"] or "", "")
            inherited = bool(tier)
        if not tier:
            dropped.append(r)
            continue
        why_dead = killed_by(r["title"] or "", r["skills"] or "")
        if why_dead:
            killed.append((why_dead, r))
            continue
        s, why = score_row(r, tier, inherited, today)
        # The lane predicate is `startup_score > 0`, so a 0 here would admit a
        # row to the lane and then hide it.  Cannot happen — an admitted row
        # always carries an AI band and a tier — but the lane's definition
        # depends on it, so it is asserted rather than assumed.
        if s <= 0:
            sys.exit(f"scored 0 with evidence — {r['company']} / {r['title']}")
        scored.append((s, r, why))
        if inherited:
            inherited_n += 1
        else:
            direct += 1

    # Never a silent cap: say what came out and why, on every run.
    print(f"  {len(dropped):,} dropped — no startup tier on the posting and "
          f"none on the company")
    print(f"  {len(killed):,} excluded by title filter, "
          f"{len(scored):,} scored  "
          f"({direct:,} direct tier, {inherited_n:,} via company)")
    if killed:
        print("   " + ", ".join(f"{k}={v}" for k, v in
                                Counter(k for k, _ in killed).most_common()))
    if args.show_killed:
        for reason, r in sorted(killed, key=lambda x: x[0]):
            print(f"    [{reason}] {r['company']} — {r['title']}")
    if args.show_dropped:
        for r in dropped:
            print(f"    [no tier] {r['source']:14s} {r['company']} — "
                  f"{r['title']}")

    src = Counter(r["source"] for _, r, _ in scored)
    print("   sources: " + ", ".join(f"{k} {v}" for k, v in src.most_common(12)))

    if not scored:
        sys.exit("no rows scored — refusing to blank the lane")

    # Same order the lane itself uses (lanes.AISTARTUP_TERMS: startup_score
    # DESC, is_agg ASC, basis_weak ASC, posted DESC, job_key ASC), so the
    # preview below is the head of the real queue and not a second opinion about
    # it.  Built as stable sorts applied least-significant key first, because
    # `posted` sorts DESC and a string cannot be negated inside a composite key.
    scored.sort(key=lambda t: t[1]["job_key"])
    scored.sort(key=lambda t: t[1]["posted"] or "", reverse=True)
    scored.sort(key=lambda t: (-t[0], t[1]["is_agg"], t[1]["basis_weak"]))

    if args.per_company:
        seen, keep = Counter(), []
        for item in scored:
            co = item[1]["company_norm"] or item[1]["company"] or ""
            if seen[co] >= args.per_company:
                continue
            seen[co] += 1
            keep.append(item)
        print(f"  {len(scored) - len(keep):,} dropped by --per-company "
              f"{args.per_company}")
        scored = keep
    if args.top:
        print(f"  capped to top {args.top:,} of {len(scored):,}")
        scored = scored[:args.top]

    print(f"\ntop {min(40, len(scored))} of {len(scored):,}:")
    for s, r, why in scored[:40]:
        print(f"  {s:6.1f}  {(r['company'] or '')[:24]:24s} "
              f"{(r['title'] or '')[:42]:42s} {r['posted'] or '':10s}  {why}")

    if args.dry_run:
        print("\n--dry-run: nothing written")
        return

    with con:
        # Cleared on `startup_score <> 0` rather than on this lane's predicate.
        # A row that has since changed bucket, lost its AI band or had its
        # company's tier withdrawn no longer matches CAND_SQL, so a
        # predicate-scoped clear would leave it in the lane forever carrying the
        # score it earned under the old data.
        con.execute("UPDATE jobs SET startup_score = 0, startup_why = '' "
                    "WHERE feed = 'main' AND startup_score <> 0")
        con.executemany(
            "UPDATE jobs SET startup_score = ?, startup_why = ? "
            "WHERE job_key = ?",
            [(s, why, r["job_key"]) for s, r, why in scored])
    print(f"\nwrote startup_score on {len(scored):,} rows")

    # WITHOUT THIS THE LANE'S INDEX IS DEAD WEIGHT.  idx_jobs_aistartup is
    # partial (WHERE startup_score > 0), so it covers ~340 of 288,000 rows — but
    # with no sqlite_stat1 to read, SQLite cannot know that, falls back to a
    # default estimate for the `> 0` inequality, and picks idx_jobs_feed
    # instead, which matches every live main-feed row.  Measured on the
    # 2026-08-15 corpus: 352 ms for this lane's COUNT(*) before ANALYZE, 1 ms
    # after, and the whole 25-lane menu 6.6 s -> 4.9 s because the same missing
    # stats were costing the other lanes too.  The apply page moved the same
    # way (queue 63 -> 54 ms, facets 17 -> 13 ms): nothing measured got slower.
    #
    # Here rather than in triage_load.py because this is the builder that
    # creates the population the index describes, and it is the last step of
    # triage_chain.sh — so the stats are gathered once the table is in the shape
    # the server will actually read.  ~1 s on the current corpus.
    con.execute("ANALYZE")
    con.commit()


if __name__ == "__main__":
    main()
