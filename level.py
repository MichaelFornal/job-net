#!/usr/bin/env python3
"""
level.py — rank the right-level lane: the same work one rung DOWN, plus
adjacent families at the same rung.

  python3 level.py [--db triage.db] [--dry-run] [--show-killed]
                         [--top N] [--per-company N] [--floor N]

Writes `level_score` / `level_why` on feed='main' rows whose title parses to a
rung at or below the target.  triage/lanes.py has the matching lane (`level`) and
sorts it on level_score DESC.

WHY THE LANE EXISTS.  Measured on the 2026-08-20 corpus: of 239 applications
sent between 08-08 and 08-20, 104 — 43% — carry an explicit senior-or-above
token in the title (51 "Senior", 19 "Lead", 17 "Manager", 11 "Director/Head of",
6 "Principal", 4 "Staff").  Five sit at Associate or below.  The profile behind
them is ~4 years post-grad, ~1.5 of it engineering-titled, with a 13-month gap,
against a $110-140k target (apply/profile.json comp.track_a).  The applications
are aimed a rung or two above the document.  There is no sideways spread either:
780 of the 821 approvals came from ai12 / aistartup / picks, all one family.

WHY A BUILDER AND NOT A PREDICATE.  `jobs.seniority` exists and cannot carry
this.  It is 78% blank, written by getro alone (72,686 of 78,339 populated rows;
personio contributes 59, every other source nothing), and its vocabulary has no
Staff and no Principal bucket at all — both fold into "Senior", which is why
"Staff AI Engineer" at Acquia and "Staff Forward Deployed Engineer" at Reltio
both read Senior, and "Lead AI Software Engineer" at Globalization Partners
reads *Cxo*.  The rung has to be parsed out of the title; the column is a
corroborator for unmarked titles and nothing more.

WHY ITS OWN COLUMN.  Same rule as local_score and desk_score: lanes_build.py
owns `score`/`lane_tags` on the main feed for all twelve tag lanes and blanks
the whole column, and every other score column belongs to a builder with a
different clear scope.  Cleared on the VALUE here, the ai_startup idiom.

RUN IT AFTER EVERY `triage_load.py --feed main` — the load upserts level_score
from MA_US_Remote_<date>.csv, which has no such column, so every main load
blanks this lane.  It is the sixth builder; see triage_load.py's docstring.

SCOPE.  All dates, no geography term, no cap.  The `l` chip on the card page
already filters MA / remote / South FL, and baking a location in here would make
the lane mean two things at once — the rule the `support` cut lane states.
"""

import argparse
import datetime as dt
import re
import sqlite3
import sys
from collections import Counter, defaultdict

import triage_load
import swath_vocab as V
from vocab import (DB, DOMAIN, comp_fit, company_fit, domain_fit,
                         geo_fit, killed_by, title_fit)
from vocab import PROFILE, fresh_wide, same_req, toks
# Imported, never re-transcribed.  `toks` already treats every seniority word as
# a stopword and `same_req` Jaccard-matches restated titles at 0.6, which is
# exactly the "same requisition, different rung" test this lane needs — it was
# written for direct_apply's dedupe, where collapsing levels is the point.
from geo_lane import LOCAL_OFF

# --------------------------------------------------------------------- the rung
# The rung the search is aimed at, from profile.json:level.  This is not a
# taste -- it is whatever the history behind the search actually supports, which
# is exactly why it is configuration and not a constant.
_LVL = PROFILE.get("level", {})
MY_RUNG = _LVL.get("target_rung", 2)

# The band the lane admits: at level, one notch up (II/III, still reachable),
# and everything below.  Above that is the mismatch this lane exists to correct,
# and the gated lanes already carry it.
MAX_RUNG = _LVL.get("max_rung", MY_RUNG + 1)

# TRAP 1 — `manager` is an IC word in exactly the families this lane is for.
# A Technical Account Manager, Customer Success Manager, Implementation Manager
# or Program Manager is an individual contributor; only a bare `manager` is a
# people-management rung.  Untreated, this one rule evicts the whole `support`
# (4,446 undecided) and `csam` (11,199) surface — the sideways pool the widening
# was asked for.  The compound is reduced to its head noun before laddering.
IC_MGR = re.compile(r"""(?ix)\b(
    technical\s+account | account | customer\s+success | success | engagement |
    implementation | onboarding | partner | program | project | product |
    community | territory | case
)\s+manager\b""")

# TRAP 2 — `lead generation` is a GTM desk noun, not a rung.  `team lead` and
# `technical lead` are.  Same shape as swath_vocab's rule that a domain word
# must never score as a job title.
LEAD_GEN = re.compile(r"(?i)\blead\s+gen(?:eration)?\b")

# TRAP 3 — `architect` is NOT in this ladder, deliberately.  "Solutions
# Architect" is F2's core presales title and reads mid-level in practice; a
# first prototype scored it as staff and would have excluded presales, which at
# 3,563 rows is the largest sideways family in the corpus.
#
# TRAP 4 — roman numerals only, no bare digits.  `\bII\b` is safe; `\b2\b`
# matches "Shift 2", "Tier 2" and stray years.
#
# Ordered alternation, first match wins.
_LADDER = [
    (7, r"\b(?:chief|cto|ceo|coo|cfo|cio|cpo|cro|svp|evp|vp|"
        r"vice\s+president)\b"),
    (6, r"\b(?:director|head\s+of|general\s+manager)\b"),
    (6, r"\b(?:manager|mgr|supervisor)\b"),
    (5, r"\b(?:staff|principal|distinguished|fellow)\b"),
    (5, r"\b(?:lead|leader)\b"),
    (4, r"\b(?:senior|sr\.?|snr|iv)\b"),
    (3, r"\b(?:ii|iii|mid[\s-]?level)\b"),
    (1, r"\b(?:associate|assistant|i)\b"),
    (0, r"\b(?:junior|jr\.?|entry[\s-]?level|new\s+grad|graduate|"
        r"early\s+career|apprentice|intern(?:ship)?|trainee|campus|student)\b"),
]
LADDER = [(n, re.compile(p, re.I | re.X)) for n, p in _LADDER]

# "Senior Associate" is a finance rung, not a senior one — vocab.py
# already records that those titles read that way and treats the word as noise.
# On the bare ladder it scores 4 and is rejected, which would drop the half of
# the finance pool that is genuinely at the target level.
#
# ADJACENT, not merely both present.  Matching the two words anywhere in the
# string admitted Accenture's "IFS Cloud Solution Architect Senior
# Manager/Associate Director" as a mid-level row: it contains "Senior" and it
# contains "Associate", and they belong to two different rungs, both of which
# are above this lane.  The pair only means the finance rung when it IS a pair.
SR_ASSOC = re.compile(r"(?i)\bsenior\s+associate\b|\bassociate\b,?\s+senior\b")

# getro's level tag, the ONLY populated source of `seniority`.  Consulted only
# when the title carries no marker of its own: it cannot represent Staff or
# Principal (both arrive as "Senior") and files Lead as "Cxo", so letting it
# override a parsed title would move rows the wrong way.
SEN_MAP = {"internship": 0, "student": 0, "entry level": 0, "associate": 1,
           "experienced": 2, "mid senior": 3, "senior": 4, "director": 6,
           "vice president": 7, "cxo": 7}


def rung_src(title, seniority=""):
    """(rung, source) — 0-7, and WHERE the number came from.

    The source is not bookkeeping.  5,862 rows in the candidate set land on rung
    3 with no roman numeral anywhere in the title: they are unmarked titles that
    getro tagged "Mid Senior", and labelling those "one notch up (II/III)" makes
    the card cite evidence that is not in the title it is printing beside.  Same
    failure geo_lane fixed by saying "also-listed" — a `why` that disagrees
    with what the reader can see reads as the lane being broken.
    """
    t = LEAD_GEN.sub(" ", title or "")
    t = IC_MGR.sub(lambda m: m.group(1), t)
    if SR_ASSOC.search(t):
        return 3, "title"
    for n, rx in LADDER:
        if rx.search(t):
            return n, "title"
    tag = (seniority or "").strip().lower()
    s = SEN_MAP.get(tag)
    if s is None:
        return MY_RUNG, "unmarked"
    return s, tag


def rung_of(title, seniority=""):
    """0-7 for how senior this title is.  2 = unmarked IC, the target rung."""
    return rung_src(title, seniority)[0]


def level_fit(rung, src="title"):
    """(points, label) for how well this rung matches the target, else (0, "").

    THE FLOOR.  Never zero for an admitted row, so the lane's SQL predicate
    `level_score > 0` means exactly "the builder put it here" — the invariant
    geo_lane.py and ai_startup_build.py both rest on, and what lets this lane
    hold the whole pool rather than a shortlist.

    THE TAPER.  It falls away below the target rung on purpose.  There are 11,267
    rung-0 rows in the live corpus against 108,195 unmarked ones; scored alike
    they would bury the at-level rows, which is the same shape as the defect
    geo_lane hit when fresh_wide (25) outranked relevance.  Entry level is
    admitted -- both depth options were chosen -- but it sits at the bottom of
    the lane, and --floor lifts it out if it proves to be noise.
    """
    pts, lab = {3: (15, "one notch up"),
                2: (18, "at level"),
                1: (12, "one notch down"),
                0: (6, "entry level")}.get(rung, (0, ""))
    if not pts:
        return 0, ""
    # Name the evidence, and name it accurately.  `unmarked` is the default that
    # put the row at the target rung because nothing in the title said otherwise;
    # anything else is getro's level tag, which is the ONLY populated source of
    # `seniority` and is worth flagging as the weaker signal it is -- it cannot
    # represent Staff or Principal and files Lead as "Cxo".
    if src == "title":
        return pts, lab
    if src == "unmarked":
        return pts, f"{lab} (unmarked title)"
    return pts, f"{lab} (tagged \u201c{src}\u201d)"


# ------------------------------------------------------------------- sideways
# The eleven F-swaths, split by how close each is to the work already being
# done.  Not a guess: presales/automation/aienable/aiexpert/gtm are the
# families the sent applications and the F1-F11 drafts already argue for, and
# the rest are real but a longer reach.
NEAR_SWATH = {"presales", "automation", "aienable", "aiexpert", "gtm"}

# Support / TAM / implementation.  swath_vocab.DECLINED carries this as a
# compiled regex that nothing has imported since it was written -- it was
# researched, priced and declined at $118k median, and the `support` cut lane
# then reopened it on 2026-08-18 after the decline turned out to be stale
# (re-measured: ~709 rows at a $136,912 mean disclosed floor).  Importing it is
# what keeps the pattern in one place; the cut lane's extra arms (Solutions
# Engineer, Implementation, Professional Services, Sales Engineer, Deployment
# Engineer, Customer Engineer) are all already in swath_vocab's F2.
SUPPORT = V.DECLINED["support"][1]

# Mirrors the `csam` cut lane's predicate (triage/lanes.py _CUT_DEFS), which is
# written as SQL LIKEs and cannot be imported as a pattern.  Kept literal and
# adjacent so the two can be read against each other.
CSAM = re.compile(r"""(?ix)
    customer\s+success | account\s+manager | account\s+executive |
    client\s+partner
""")


def family_fit(title, twin_of):
    """(points, label) for the sideways axis — which family this row is in.

    MAX, not a sum.  A row that is both a down-level twin and a swath match is
    one row about one job; adding the arms would put it 40 points clear of
    everything on a coincidence of vocabulary.
    """
    best = (0, "")
    if twin_of:
        best = max(best, (26, f"down-level twin of \"{twin_of[:44]}\""))
    for k in V.all_matching(title or ""):
        pts = 20 if k in NEAR_SWATH else 14
        cand = (pts, f"{V.code_of(k)} {k}" + ("" if k in NEAR_SWATH else " (reach)"))
        best = max(best, cand)
    if SUPPORT.search(title or ""):
        best = max(best, (18, "support / TAM / implementation"))
    if CSAM.search(title or ""):
        best = max(best, (16, "customer success / AM"))
    # The AI/FDE core stays in, but only at or below level -- rung is tested
    # before this function is ever called.  These are the rows ai12 and
    # aistartup carry at rung 4+; here they are the same work, one rung down.
    pts, lab = title_fit(title or "")
    if pts >= 32:
        best = max(best, (14, "AI/FDE core at level"))
    return best


# --------------------------------------------------------------------- scoring
def level_killed_by(title, skills):
    """Reason this row is off-target, or None.  vocab.killed_by widened
    with geo_lane.LOCAL_OFF — one place, so --show-killed and the real run
    cannot disagree about what the filter did.

    LOCAL_OFF is needed for the same reason `local` needed it: this lane admits
    a LEVEL BAND rather than a role population, so the corpus's 1,459 clinical
    and retail rows are inside its candidate set, and WRONG_ROLE has `\\bnurse\\b`
    and nothing for RN, LPN, CNA, phlebotomist, barista or cashier.  Widening
    killed_by itself is not an option — twelve lanes import it.
    """
    why = killed_by(title or "", skills or "")
    if why:
        return why
    m = LOCAL_OFF.search(title or "")
    return "off-target: " + m.group(0).strip() if m else None


def score_row(r, today, twin_of=None, floor=0):
    """(score, why) — every point traceable to a phrase in `why`."""
    rung, src = rung_src(r["title"] or "", r["seniority"] or "")
    if rung > MAX_RUNG or rung < floor:
        return 0, ""
    lpts, llab = level_fit(rung, src)
    if not lpts:
        return 0, ""
    if level_killed_by(r["title"] or "", r["skills"] or ""):
        return 0, ""

    fam = family_fit(r["title"] or "", twin_of)
    if not fam[0]:
        return 0, ""

    parts = [f"rung {rung} — {llab} +{lpts:g}", f"{fam[1]} +{fam[0]:g}"]
    total = float(lpts + fam[0])

    axes = [domain_fit(r["title"] or "", r["skills"] or "",
                       r["company_industry"] or ""),
            geo_fit(r["bucket"] or "", r["basis_weak"]),
            company_fit(r["startup_tier"] or ""),
            comp_fit(r["salary_max"])]
    # Freshness at half weight, and that is a lane weight rather than a second
    # definition of the curve.  fresh_wide is worth 25 against a 26-point family
    # axis, so at full weight a same-day far-reach row outranks a down-level
    # twin — the exact inversion geo_lane documents (a same-day Medical
    # Science Liaison at 42.6 over a Southborough full-stack role).  Gating it
    # behind a family hit is not enough here because every admitted row has one
    # by construction, so the weight has to come down instead.
    fpts, flab = fresh_wide(r["posted"] or "", today)
    if fpts:
        axes.append((round(fpts * 0.5, 1), flab))

    for pts, label in axes:
        if pts:
            total += pts
            parts.append(f"{label} +{pts:g}")
    return round(total, 1), " · ".join(parts)


# ------------------------------------------------------------------------ main
# The third NOT EXISTS is what no other builder has, and it is the point of the
# lane: `applications` holds what has already been sent, and this lane is
# defined as the correctly-levelled work that has NOT.  Keyed on ct_key rather
# than job_key because job_key is not stable across rebuilds (triage_load's
# docstring) — the same trap decisions carry-forward exists to solve.
#
# Aggregators are not excluded, for the reason the other geographic lanes keep
# them: 74% of the wide remote surface is republish, and dropping it would hide
# real jobs from a lane that promised the whole level band.  They sort behind
# employer-direct rows on the is_agg term in lanes.LEVEL_TERMS.
CAND_SQL = """
SELECT job_key, ct_key, company, company_norm, title, seniority, skills,
       company_industry, startup_tier, ai_band, bucket, basis_weak, posted,
       salary_max, source, is_agg, date_weak, location, work_mode
  FROM jobs
 WHERE feed = 'main' AND live = 1
   AND NOT EXISTS (SELECT 1 FROM decisions d WHERE d.job_key = jobs.job_key)
   AND NOT EXISTS (SELECT 1 FROM decisions d2
                    WHERE d2.ct_key = jobs.ct_key AND jobs.ct_key <> '')
   AND NOT EXISTS (SELECT 1 FROM applications a
                    WHERE a.ct_key = jobs.ct_key AND jobs.ct_key <> '')
"""

# The over-levelled applications — the rows this lane is correcting.  `applying`
# is included with `applied`: both are sent as far as level is concerned.
APPLIED_SQL = """
SELECT DISTINCT j.title, j.seniority
  FROM applications a JOIN jobs j ON j.job_key = a.job_key
 WHERE a.status IN ('applied', 'applying')
"""


def twin_index(con):
    """{title: tokens} for every application at rung 4+, plus a token->title
    postings list so the O(n*m) same_req sweep does not have to be one.

    Without the inverted index this is 68 applied titles x 145k candidates =
    9.9M Jaccard comparisons.  With it, only candidates sharing at least one
    content token are tested, because same_req cannot return True below 0.6
    overlap without one.
    """
    hi = []
    for r in con.execute(APPLIED_SQL):
        rung, src = rung_src(r["title"] or "", r["seniority"] or "")
        # TITLE-DERIVED ONLY, and this is the difference between the arm meaning
        # something and meaning nothing.  Admitting rungs that came from getro's
        # `seniority` tag put plain "Forward Deployed Engineer" into the
        # over-levelled set -- it was tagged "Senior" on the applied-to posting
        # -- and every plain "Forward Deployed Engineer" in the corpus then
        # matched it, printing `down-level twin of "Forward Deployed Engineer"`
        # beside an identical title.  A twin is the same requisition wearing a
        # seniority WORD this one does not, so the word has to be in the title.
        if rung >= 4 and src == "title":
            hi.append((r["title"], toks(r["title"])))
    post = defaultdict(list)
    for i, (t, tk) in enumerate(hi):
        for w in tk:
            post[w].append(i)
    return hi, post


def twin_of(title, hi, post):
    """The over-levelled application this row is the same requisition as, or
    None.  A row can mirror several sent applications and is still one job, so
    one is named — the CLOSEST, and closest is computed rather than picked.

    Iterating the candidate set directly returned an arbitrary match, and
    arbitrary here is not stable: PYTHONHASHSEED randomises string hashing, so
    the set's iteration order — and therefore every `level_why` this lane
    writes — changed between runs of the same builder against the same corpus.
    Highest token overlap, ties broken lexicographically, is reproducible.
    """
    ta = toks(title)
    if not ta:
        return None
    seen = set()
    for w in ta:
        seen.update(post.get(w, ()))
    best = None
    for i in seen:
        t, tb = hi[i]
        # Cheap prefilter off the PRECOMPUTED sets -- toks() on 90 applied
        # titles against 253k candidates is 23M redundant tokenisations, and it
        # is the whole cost of this pass.  same_req still decides: it is called
        # on the survivors, so the threshold stays in one place.
        inter = len(ta & tb)
        if not inter:
            continue
        j = inter / len(ta | tb)
        if j < 0.6 and not (inter >= 3 and inter == min(len(ta), len(tb))):
            continue
        if not same_req(t, title):
            continue
        # Highest overlap wins; the title breaks ties, so the choice does not
        # depend on set iteration order (PYTHONHASHSEED randomises it, which
        # made every `level_why` this lane writes differ between runs).
        key = (j, [-ord(c) for c in t])
        if best is None or key > best[0]:
            best = (key, t)
    return best[1] if best else None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB)
    ap.add_argument("--top", type=int, default=0,
                    help="cap the lane (0 = no cap, the default: full sweep)")
    ap.add_argument("--per-company", type=int, default=3,
                    help="cap rows per company (0 = no cap; default 3)")
    ap.add_argument("--floor", type=int, default=0,
                    help="lowest rung admitted (0 = entry level, the default; "
                         "1 drops new-grad, 2 drops associate too)")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--show-killed", action="store_true",
                    help="print the rows dropped by the off-target filter")
    ap.add_argument("--show-dropped", action="store_true",
                    help="print the rung histogram of everything rejected")
    args = ap.parse_args()

    today = dt.date.today()
    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    # This builder introduced level_score/level_why, so it cannot assume its
    # columns exist.  migrate() is idempotent.
    triage_load.migrate(con)

    hi, post = twin_index(con)
    print(f"{len(hi):,} distinct applications at rung 4+ — the mismatch this "
          f"lane corrects")

    rows = con.execute(CAND_SQL).fetchall()
    print(f"{len(rows):,} undecided, unapplied, live main-feed rows to level")

    scored, killed, rungs, fams, twins = [], [], Counter(), Counter(), 0
    for r in rows:
        title = r["title"] or ""
        rung = rung_of(title, r["seniority"] or "")
        rungs[rung] += 1
        if rung > MAX_RUNG or rung < args.floor:
            continue
        if level_killed_by(title, r["skills"] or ""):
            killed.append(r)
            continue
        t = twin_of(title, hi, post)
        s, why = score_row(r, today, t, args.floor)
        if not s:
            continue
        if t:
            twins += 1
        fams[why.split(" · ")[1].rsplit(" +", 1)[0]] += 1
        scored.append((s, r, why))

    print("  rung histogram (whole candidate set): "
          + ", ".join(f"{k}={rungs[k]:,}" for k in sorted(rungs)))
    # Never a silent cap: say what the filter removed, not just what survived.
    print(f"  {len(killed):,} dropped by the off-target filter "
          f"(anti-title / wrong role / clinical / retail)")
    if args.show_killed:
        for r in killed[:200]:
            print(f"    [{level_killed_by(r['title'] or '', r['skills'] or '')}] "
                  f"{r['company']} — {r['title']}")
        if len(killed) > 200:
            print(f"    ... and {len(killed) - 200:,} more")
    if args.show_dropped:
        over = sum(v for k, v in rungs.items() if k > MAX_RUNG)
        under = sum(v for k, v in rungs.items() if k < args.floor)
        print(f"  {over:,} rejected as over-levelled (rung > {MAX_RUNG}), "
              f"{under:,} below --floor {args.floor}")

    print(f"  {len(scored):,} in the lane — {twins:,} of them down-level twins")
    print("  by family: " + ", ".join(f"{k}={v:,}" for k, v in fams.most_common(8)))

    if not scored:
        sys.exit("no rows scored — refusing to blank the lane")

    # Same order the lane itself uses (lanes.LEVEL_TERMS: level_score DESC,
    # is_agg ASC, date_weak ASC, posted DESC, job_key ASC), so this preview is
    # the head of the real queue and not a second opinion about it.  Stable
    # sorts applied least-significant key first, because `posted` sorts DESC and
    # a string cannot be negated inside one composite key.
    scored.sort(key=lambda t: t[1]["job_key"])
    scored.sort(key=lambda t: t[1]["posted"] or "", reverse=True)
    scored.sort(key=lambda t: (-t[0], t[1]["is_agg"], t[1]["date_weak"]))

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
        print(f"  {s:6.1f}  {(r['company'] or '')[:22]:22s} "
              f"{(r['title'] or '')[:44]:44s} {why[:96]}")

    if args.dry_run:
        print("\n--dry-run: nothing written")
        return

    with con:
        # Cleared on the VALUE, not on this lane's predicate.  A row whose title
        # is rewritten by a re-fetch, or that leaves the band because the ladder
        # was edited, is not reachable by any predicate the new run computes, so
        # a predicate-scoped clear would leave it scored and in the lane forever.
        # Same lesson ai_startup_build.py records.
        con.execute("UPDATE jobs SET level_score = 0, level_why = '' "
                    "WHERE feed = 'main' AND level_score <> 0")
        con.executemany(
            "UPDATE jobs SET level_score = ?, level_why = ? WHERE job_key = ?",
            [(s, why, r["job_key"]) for s, r, why in scored])
    print(f"\nwrote level_score on {len(scored):,} rows")

    # This is now the last builder in triage_chain.sh, so it carries the
    # ANALYZE.  Without sqlite_stat1 SQLite ignores the partial indexes entirely
    # and every score-predicate lane's COUNT(*) falls back to a full scan on
    # each render of the 29-lane menu.
    con.execute("ANALYZE")
    print("ANALYZE done")


if __name__ == "__main__":
    main()
