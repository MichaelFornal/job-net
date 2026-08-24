#!/usr/bin/env python3
"""geo_lane.py — rank the commute-ring lane.

    python3 geo_lane.py [--db triage.db] [--dry-run] [--show-killed]
                        [--top N] [--per-company N] [--border]

Writes `local_score` / `local_why` on feed='main' rows whose `location` names a
town inside the ring configured in profile.json:geography.

WHY IT IS A BUILDER AND NOT A PREDICATE.  `bucket` collapses every in-state row
to a single value -- a suburban job and a downtown job are the same row to every
other lane in the app.  The city is not recoverable from a column either: the
master CSV carries city/state/country and the loader keeps none of them.  So the
town has to be read out of the free-text `location`, which is what this file is
for.

WHY ITS OWN COLUMN.  Two geographic lanes cannot share a score column: each
builder clears its own scope before writing, so the second run blanks the first
lane's rows.  Each geographic lane therefore owns a column.
"""

import argparse
import datetime as dt
import re
import sqlite3
import sys
from collections import Counter

import triage_load
from vocab import (DB, DOMAIN, PROFILE, ai_fit, comp_fit, domain_fit,
                   fresh_wide, killed_by, startup_fit, title_fit)

# ------------------------------------------------------------------ geography
# The rings are DATA (profile.json:geography); everything below them is
# mechanism.  ring1 is the towns the lane is named for and whatever abuts them;
# ring2 is the next band out; border is off by default and admitted with
# --border.
_GEO = PROFILE.get("geography", {})
RING1 = tuple(t.lower() for t in _GEO.get("ring1", ()))
RING2 = tuple(t.lower() for t in _GEO.get("ring2", ()))
BORDER = tuple(t.lower() for t in _GEO.get("border", ()))

# A town name only counts with a state marker in the SAME location segment, and
# that is not belt-and-braces -- the outer ring is unusable without it.  Place
# names collide hard across countries and states, and the bare tokens are some
# of the most common words in the corpus: measured here, `berlin` matched
# 16,178 rows (Germany), `auburn` 2,371, `sterling` 1,922 (Virginia), `oxford`
# 1,198 (UK), `hudson` 1,180 (Ohio / Wisconsin / New York), `milford` 1,249
# (Michigan / Connecticut).
#
# Match several marker forms, not one.  A pattern like `<state>|,\s*<abbr>\b`
# misses the `US-XX-TOWNNAME` facility codes, the reversed `XX - Townname`, the
# `State-Townname` form some ATSes emit, and the no-space `Townname,XX 01605`
# street form -- together about 7% of the in-state rows in this corpus.  A word
# boundary on the two-letter abbreviation covers all four, because `-` and `,`
# are both non-word characters.
MA_MARK = re.compile(_GEO.get("state_mark", r"(?!x)x"), re.I)

# Matching a bare token, so `\b` on BOTH sides: without it "Auburndale" scores
# as Auburn and "Hudson Yards" as Hudson.
_PAT = {t: re.compile(r"\b" + re.escape(t) + r"\b", re.I)
        for t in RING1 + RING2 + BORDER}

# A req that enumerates more than this many locations is a hospital-system or
# retail-chain posting listing every site it operates, not a job in any one of
# them.  Above the cap only the PRIMARY segment is read.
#
# This is the measured middle between the two rules that are both wrong here.
# Matching the PRIMARY segment only drops 268 real rows: a genuine suburban job
# is routinely written "<State>, USA; <Town>, <XX>, USA; <Metro>, <XX>, USA".
# Matching ANY segment instead admits 119 rows shaped like
# "<State>-<TownA>; <State>-<TownB>; ..." x47 -- one per site in a hospital
# system, naming ring towns and meaning none of them.
MAX_SEGMENTS = 6

# vocab.killed_by, WIDENED — wrapped rather than edited, the same call
# vocab.startup_fit makes about company_fit.  Twelve lanes import killed_by
# and none of them would benefit; this vocabulary simply does not occur in a
# lane pre-filtered to AI bands and startup tiers.
#
# It occurs constantly here.  This lane is the only one that admits a whole
# geography rather than a role population, and 1,459 of its rows are clinical,
# retail or food service — WRONG_ROLE has `\bnurse\b` and nothing for RN, LPN,
# CNA, medical assistant, phlebotomist, barista, cashier or dietary aide.
# Measured before this list existed: a Cityblock "Resource RN - Worcester, MA"
# scored 47.2 and sat sixteenth in the lane.
LOCAL_OFF = re.compile(r"""(?ix)
    \brn\b | \blpn\b | \bcna\b | \bnp\b | \bpa-?c\b | \baprn\b |
    medical\s+assistant | patient\s+(?:care|access|service|experience) |
    \bphlebotom | \bradiolog | \bsonograph | \bimaging\s+tech |
    \bsurg(?:eon|ical)\b | \bdental\b | \bhygienist\b | \bveterinar |
    \bmedical\s+science\s+liaison\b | \bper\s+diem\b | \burgent\s+care\b |
    \bhome\s+health\b | \bhospice\b | \bbehavioral\s+health\s+tech |
    \bbarista\b | \bcashier\b | \bstore\s+(?:manager|associate|lead)\b |
    \bsales\s+associate\b | \bretail\b | \bstocker\b | \bmerchandiser\b |
    \bfood\s+service\b | \bcook\b | \bserver\b | \bdishwash | \bbartender\b |
    \bhousekeep | \bdietary\b | \bcustodial\b | \bgroundskeep |
    \bmaintenance\s+tech | \bforklift\b | \bpicker\b | \bpacker\b |
    \bcdl\b | \bdelivery\b | \bcourier\b | \bcrew\s+member\b |
    \bcaregiver\b | \bdirect\s+support\b | \bteller\b | \bschool\b
""")

# A white-collar function word that title_fit does not reach.  This is the
# middle tier the lane needs and `picks` does not: there, a row is either one of
# the target titles or it is noise, because the lane was already filtered to AI
# roles.  Here the largest block — 2,773 rows — is ordinary professional work
# (finance, ops, program management, manufacturing engineering) that is neither
# a target title nor junk, and without a tier of its own it would score exactly
# what a barista scores.
PROF = re.compile(r"""(?ix)
    \bmanager\b | \bdirector\b | \bhead\s+of\b | \bprincipal\b | \blead\b |
    \bsupervisor\b | \banalyst\b | \bconsultant\b | \bstrategist\b |
    \bplanner\b | \badministrator\b | \bcoordinator\b | \bpartner\b |
    \bengineer\b | \bdeveloper\b | \barchitect\b | \bscientist\b |
    \bdesigner\b | \bcontroller\b | \bbuyer\b | \bunderwriter\b
""")


def role_fit(title, domain_hit):
    """0-40 for "is this on-target work" — title_fit with a floor tier.

    The floor is what keeps the lane honest at the bottom.  Freshness is worth
    25 here and comp 8, so an off-target posting from this morning would beat a
    three-week-old target title on recency alone: measured, that is exactly what
    put an in-ring "Medical Science Liaison" at 42.6 and a national "Client
    Partner - United States" at 45.0 above an in-ring full-stack role.
    Hence score_row gates those two axes on this returning something.
    """
    pts, label = title_fit(title, domain_hit)
    if pts:
        return pts, label
    if PROF.search(title or ""):
        return 8, "professional title"
    return 0, ""


def where_fit(location, border=False):
    """(points, label) for how local this posting is — or (0, "") if it is not.

    Proximity is scored here, where a single-metro lane would omit geography
    entirely: if every row in a lane is one metro the term is a constant.  Here
    ring 1 against ring 2 is the entire point of the lane -- it is what "lean
    toward the towns nearest home" actually means.
    """
    segs = [s.strip() for s in re.split(r"[;|]", location or "") if s.strip()]
    if not segs:
        return 0, ""
    if len(segs) > MAX_SEGMENTS:
        segs = segs[:1]

    best, town, tag = 0, "", ""
    for i, seg in enumerate(segs):
        if not MA_MARK.search(seg):
            continue
        for name, pat in _PAT.items():
            if not pat.search(seg):
                continue
            if name in RING1:
                ring, pts = "ring 1", (12 if i == 0 else 8)
            elif name in RING2:
                ring, pts = "ring 2", (6 if i == 0 else 4)
            elif border:
                ring, pts = "border", (4 if i == 0 else 2)
            else:
                continue
            if pts > best:
                # A town named in a LATER segment is a real signal and a weaker
                # one.  "<State>, USA; <Town>, <XX>, USA; <Metro>, <XX>, USA" is
                # a job in that town; "New York, New York; ...; <Town>, <XX>" is
                # a regional territory that merely includes it.  Scoring them
                # alike put a New York TAM req above an in-ring full-stack role,
                # so a secondary match is discounted and said
                # so in `why` — the card shows `location`, and a bare
                # "Worcester (ring 1)" beside a location reading New York looks
                # like the lane is broken.
                best, town = pts, name
                tag = ring if i == 0 else ring + ", also-listed"
    if not best:
        return 0, ""
    return best, f"{town.title()} ({tag})"


# --------------------------------------------------------------------- scoring
def local_killed_by(title, skills):
    """Reason this row is off-target, or None.  vocab.killed_by widened
    with LOCAL_OFF — one place, so --show-killed and the real run cannot
    disagree about what the filter did."""
    why = killed_by(title, skills)
    if why:
        return why
    m = LOCAL_OFF.search(title or "")
    return "local off-target: " + m.group(0).strip() if m else None


def score_row(r, today, border=False):
    """(score, why) — every point traceable to a phrase in `why`.

    THE FLOOR.  Proximity is never zero for an admitted row, and two things rest
    on that.  It makes the lane's SQL predicate `local_score > 0` mean exactly
    "the builder put it in the region", so no card can reach the page carrying
    no number and no reason — the invariant ai_startup_build.py states.
    And it is what lets this lane hold the whole local pool instead of a
    shortlist.

    A KILLED TITLE SCORES ITS PROXIMITY AND NOTHING ELSE.  sfl_build drops the
    security guards, nurses and delivery drivers from scoring and relies on its
    bucket predicate to keep them in the lane at 0.  This lane has no such
    predicate to fall back on — dropped here, they would vanish — so they are
    scored down instead of out: proximity only, capping at 12, below every row
    with even minimal title fit.  Freshness is withheld from them on purpose,
    because it is worth 25 and a same-day in-ring barista would otherwise
    outrank a three-week-old in-ring analyst.  Run --show-killed to read them.
    """
    gpts, gwhy = where_fit(r["location"] or "", border)
    if not gpts:
        return 0, ""

    parts, total = [f"{gwhy} +{gpts:g}"], float(gpts)
    if local_killed_by(r["title"] or "", r["skills"] or ""):
        return round(total, 1), " · ".join(parts)

    dom = bool(DOMAIN.search(f"{r['title']} {r['skills'] or ''} "
                             f"{r['company_industry'] or ''}"))
    role = role_fit(r["title"] or "", dom)
    dfit = domain_fit(r["title"] or "", r["skills"] or "",
                      r["company_industry"] or "")
    aifit = ai_fit(r["ai_band"] or "")
    axes = [role, dfit, aifit, startup_fit(r["startup_tier"] or "")]

    # THE GATE.  Freshness and comp are tie-breakers among on-target jobs, not
    # reasons to surface an off-target one.  `startup_tier` is deliberately NOT
    # part of the test even though it is scored: it is a property of the
    # employer, not of the role, and a well-funded startup hiring a phlebotomist
    # is still hiring a phlebotomist.
    if any(p for p, _ in (role, dfit, aifit)):
        axes += [fresh_wide(r["posted"] or "", today),
                 comp_fit(r["salary_max"])]

    for pts, label in axes:
        if pts:
            total += pts
            parts.append(f"{label} +{pts:g}")
    return round(total, 1), " · ".join(parts)


# ------------------------------------------------------------------------ main
# No posted cutoff and no work_mode term.  The 115 rows that name a local town
# and read Remote stay in: a posting anchored to a ring town is local whatever
# box the source ticked.  The alternative reading of "remote at a local
# employer" -- promote every remote row whose employer is proven local -- was
# measured at +4,743 rows whose top propagators are national staffing desks and
# enterprises with a single in-region posting (596 / 423 / 282 / 210 / 72 rows
# for the top five).  None of those is a job you could commute to.
#
# Aggregators are NOT excluded, for the reason sfl_build keeps them: half this
# lane is republish, and dropping careerjet would hide real local jobs from a
# lane that promised all of them.  They sort behind employer-direct rows on the
# is_agg term in lanes.LOCAL_TERMS.
#
# The `location LIKE '%, MA%'` prefilter is an optimisation, not the rule —
# where_fit() is.  It only has to be WIDER than the real test, and it is: every
# marker form MA_MARK accepts contains either "ma" or "mass".
CAND_SQL = """
SELECT job_key, ct_key, company, company_norm, title, skills, company_industry,
       startup_tier, ai_band, bucket, basis_weak, posted, salary_max, source,
       is_agg, location, work_mode
  FROM jobs
 WHERE feed = 'main' AND live = 1
   AND (location LIKE '%ma%' OR location LIKE '%Mass%')
   AND NOT EXISTS (SELECT 1 FROM decisions d WHERE d.job_key = jobs.job_key)
   AND NOT EXISTS (SELECT 1 FROM decisions d2
                    WHERE d2.ct_key = jobs.ct_key AND jobs.ct_key <> '')
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB)
    ap.add_argument("--top", type=int, default=0,
                    help="cap the lane (0 = no cap, the default: full sweep)")
    ap.add_argument("--per-company", type=int, default=0,
                    help="cap rows per company (0 = no cap)")
    ap.add_argument("--border", action="store_true",
                    help="also admit the border towns (Devens, Framingham, "
                         "Natick, Acton, Sudbury...) at 4 points")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--show-killed", action="store_true",
                    help="print the rows scored down to proximity only")
    args = ap.parse_args()

    today = dt.date.today()
    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    # This builder introduced local_score/local_why, so unlike sfl_build it
    # cannot assume its columns exist.  migrate() is idempotent.
    triage_load.migrate(con)
    con.executescript(triage_load.POST_MIGRATE_DDL)

    rows = con.execute(CAND_SQL).fetchall()
    print(f"{len(rows):,} undecided, live main-feed rows to test for locality"
          + (" (--border: border towns admitted)" if args.border else ""))

    scored, killed, rings = [], [], Counter()
    for r in rows:
        s, why = score_row(r, today, args.border)
        if not s:
            continue
        rings[why.split(" +")[0].split("(")[-1].rstrip(")")] += 1
        if local_killed_by(r["title"] or "", r["skills"] or ""):
            killed.append(r)
        scored.append((s, r, why))

    print(f"  {len(scored):,} in the ring "
          + ", ".join(f"{k}={v}" for k, v in rings.most_common()))
    # Never a silent cap: these are IN the lane, scored down, not removed.
    print(f"  {len(killed):,} of them scored to proximity only "
          f"(off-target title) — they sort last, they are not hidden")
    if args.show_killed:
        for r in killed:
            print(f"    [{local_killed_by(r['title'] or '', r['skills'] or '')}] "
                  f"{r['company']} — {r['title']}")

    if not scored:
        sys.exit("no local rows scored — refusing to blank the lane")

    # Same order the lane itself uses (lanes.LOCAL_TERMS: local_score DESC,
    # is_agg ASC, posted DESC, job_key ASC), so this preview is the head of the
    # real queue and not a second opinion about it.  Stable sorts applied
    # least-significant key first, because `posted` sorts DESC and a string
    # cannot be negated inside one composite key.
    scored.sort(key=lambda t: t[1]["job_key"])
    scored.sort(key=lambda t: t[1]["posted"] or "", reverse=True)
    scored.sort(key=lambda t: (-t[0], t[1]["is_agg"]))

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
              f"{(r['title'] or '')[:40]:40s} "
              f"{(r['location'] or '').split(';')[0][:24]:24s} {why}")

    if args.dry_run:
        print("\n--dry-run: nothing written")
        return

    with con:
        # Cleared on the VALUE, not on this lane's predicate.  A row that leaves
        # the ring — its location rewritten by a re-fetch, or the ring itself
        # edited — is not reachable by any predicate the new run computes, so a
        # predicate-scoped clear would leave it scored and in the lane forever.
        # Same lesson ai_startup_build.py records.
        con.execute("UPDATE jobs SET local_score = 0, local_why = '' "
                    "WHERE feed = 'main' AND local_score <> 0")
        con.executemany(
            "UPDATE jobs SET local_score = ?, local_why = ? WHERE job_key = ?",
            [(s, why, r["job_key"]) for s, r, why in scored])
    print(f"\nwrote local_score on {len(scored):,} rows")

    # No longer the last builder in triage_chain.sh -- level.py follows
    # it as of 2026-08-20 and carries the tail ANALYZE.  This call stays because
    # geo_lane is still run on its own, and running ANALYZE twice in a chain
    # costs one pass over sqlite_stat1, not one over the table.
    con.execute("ANALYZE")
    print("ANALYZE done")


if __name__ == "__main__":
    main()
