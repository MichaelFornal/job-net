#!/usr/bin/env python3
"""
ai_agency_build.py — rank AI reqs that staffing desks are trying to fill.

  python3 ai_agency_build.py [--db triage.db] [--dry-run] [--show-killed]
                             [--show-dropped] [--top N] [--per-company N]

Writes `desk_score` / `desk_why` on feed='main' rows whose POSTER is a staffing
agency, recruiting firm or RPO and whose req is AI or AI-adjacent.
triage/lanes.py has the matching lane (`aiagency`) and sorts it on desk_score
DESC.

WHAT THIS LANE IS, AND WHY IT IS NOT THE ONE YOU WOULD GUESS.
It is NOT "jobs at recruiting companies".  It is client reqs: the placement is
at some other employer, and the agency is the counterparty you deal with.  That
is the white space — an agency with a GenAI req it cannot fill is demand-starved,
so an AI builder is scarce supply there rather than one resume in four hundred.

THE OBJECTION THIS REPO ALREADY RECORDED, AND WHY THE LANE STILL EARNS ITS PLACE.
the outreach ranker excludes this whole population on purpose — "the
hiring motion is a resume-forward keyword match to a client req ... there is no
hiring manager to build a relationship with" — and subtracts 60 points for it.
startup_build.py flags the same companies `agency?`.

That objection is right about the REQ and wrong about the DESK.  Measured on the
2026-08-16 corpus, live rows per agency:

    Diverse Lynx    365 reqs, 20 AI       Marathon TS      40 reqs, 4 AI
    PlanIT Group    226 reqs,  8 AI       US Tech Solutions 86 reqs, 3 AI
    ICONMA          164 reqs,  6 AI       TEKsystems       66 reqs, 2 AI

One recruiter relationship at Diverse Lynx addresses twenty open AI reqs and
every future one.  That IS a channel, and it is reusable in a way a single
application is not.  So the lane does not overrule the objection, it encodes it:
`desk_depth` is a scoring term, and the busiest desks sort to the top.  Ranking
these req-by-req the way every other lane does is what throws the advantage away.

TWO INDEPENDENT DETECTORS, AND THE WEAKER ONE IS LABELLED.
  * HARD — the apply/job URL's registrable domain is a staffing-ONLY ATS
    (JobDiva, Bullhorn, Ceipal, Crelate, Loxo, Avionté...).  Those vendors sell
    to agencies and to nobody else, so this is proof by construction, not a
    guess about a name.
  * NAME — the company name or industry reads as an agency.  A guess a
    normalised name can get wrong, so it scores lower and says so on the card.

They barely overlap: 387 main-feed rows are caught ONLY by the URL, where the
company name reads as an ordinary employer (NTT DATA, ICONMA, Metasys
Technologies, US Tech Solutions).  A name-only classifier — which is all
a hand-seeded staffing list is — misses those entirely.  Same asymmetry as
`basis_weak` and as ai_startup_build's inherited tier: admit the weaker
evidence, label it, sort it behind the proven rows.

WHY THE APEX DOMAIN AND NOT A SUBSTRING.  `apply_url LIKE '%bullhorn%'` admits
`bullhorn.wd1.myworkdayjobs.com` — which is Bullhorn Inc hiring for ITSELF on
Workday, the exact opposite of an agency-posted req.  Twelve such rows in the
corpus.  The ATS is the registrable domain; anything to the left of it is the
tenant, i.e. the employer.

WHY THE GATE IS NOT `ai_band`.  2,585 of the 2,683 agency rows are rank_tier 9.
The band classifier was fitted to employer-direct engineering titles and was
never asked this question — a "Senior GenAI / Python Engineer" from Diverse Lynx
reads as tier 9 about as often as not.  Gating on the band alone yields a tab you
burn in one sitting, so AI-adjacent TITLES are admitted too and scored lower.

THE SEED REGEXES ARE TIGHTENED, NEVER COPIED.  STAFFING and AGENCY_INDUSTRY were
both written to EXCLUDE this population, where over-matching costs nothing.
Flipped into an admission gate the over-match becomes the lane's contents — the
EXCL_SRC shape, where a rule that only ever denied something gets read as an
assertion.  `consultancy` and `consulting group` are dropped from the seed for
exactly this reason: Boston Consulting Group is not a staffing desk.

Every admitted row scores at least 12 (weakest AI evidence 8 + weakest agency
evidence 4), so `desk_score > 0` and "the builder put it here" are the same
statement and no row lands in the lane carrying no number and no reason.

RUN IT AFTER EVERY `triage_load.py --feed main`, for the same reason
lanes_build.py, the per-metro lane builder and ai_startup_build.py have to be: the load
upserts desk_score from MA_US_Remote_<date>.csv, which has no such column, so
every main load blanks this lane.
"""

import argparse
import datetime as dt
import re
import sqlite3
import sys
from collections import Counter
from urllib.parse import urlsplit

import triage_load
from vocab import (DB, DOMAIN, comp_fit, domain_fit, geo_fit, killed_by,
                         title_fit)
from vocab import fresh_wide

# Staffing-only ATS vendors, as REGISTRABLE DOMAINS (last two labels of the
# host).  Every one of these sells to staffing agencies, recruiting firms and
# RPOs and to nobody else, which is what makes a hit here proof rather than a
# guess.  See the module docstring for why this is not a substring test.
#
# A host under a multi-part public suffix ('agency.co.uk') reduces to 'co.uk',
# matches nothing here and falls through to the name arm — the safe direction,
# since the cost is a missed strong signal and not a false admission.
STAFFING_ATS = {
    "jobdiva.com",
    "bullhornstaffing.com", "bullhorncdn.com", "bhrms.com",
    "ceipal.com",
    "crelate.com",
    "loxo.co", "loxo.com",
    "avionte.com", "avionteaero.com", "aviontego.com",
    "jobadder.com",
    "vincere.io",
    "recruiterflow.com",
    "manatal.com",
    "trackerrms.com",
    "pcrecruiter.net",
    "tempworks.com",
    "erecruit.com",
    "jobscience.com",
}

# The generic agency-name test.  Word-anchored throughout: an unanchored
# `recruit` is fine (it only ever appears in agency names) but an unanchored
# `search` or `talent` is not — "Talent.com" is a job board and "Research
# Scientist" is not a desk.
#
# Deliberately ABSENT, against the a hand-seeded staffing list seed:
# `consultancy` and `consulting group`.  Those admit management consultancies,
# which hire directly and are not this lane.
AGENCY_NAME = re.compile(r"""(?ix)
    \bstaffing\b | \brecruit(?:ing|ment|er|ers)?\b | \bheadhunt |
    executive\s+search | \bsearch\s+(?:group|partners|firm|associates)\b |
    talent\s+(?:solutions?|acquisition|group|partners|network|services|advisors) |
    \bmanpower\b | workforce\s+solutions? | employment\s+agency |
    temp(?:orary)?\s+agency | \bpersonnel\b | \bplacements?\b |
    \bconsultants?\s+group\b
""")

# IT body shops and staffing firms whose names give nothing away.  A list, not a
# pattern, because there is no morphology to generalise — "Diverse Lynx" and
# "ICONMA" are agencies for no reason a regex could ever derive.  Seeded from
# a hand-seeded staffing list and extended with the desks this corpus actually
# carries; extending it is the intended way to grow the lane's name arm.
KNOWN_AGENCY = re.compile(r"""(?ix)
    diverse\s+lynx | insight\s+global | robert\s+half | teksystems | randstad |
    apex\s+systems | motion\s+recruitment | \bkforce\b | collabera | \bartech\b |
    mindlance | judge\s+group | akkodis | \baditi\b | cybercoders | \bjobot\b |
    \biconma\b | planit\s+group | us\s+tech\s+solutions | russell\s+tobin |
    marathon\s+ts | \bmetasys\b | \bkeylent\b | \bsaviance\b |
    veracity\s+software | resource\s+logistics | expedite\s+technology |
    \bperfict\b | spark\s+tek | \bmastech\b | \bnlb\s+services\b |
    \bcompunnel\b | \bnetpolarity\b | \beteam\b | \bintellectt\b |
    \bkellton\b | \bsunrise\s+systems\b | \bcybertec\b | \bvdart\b |
    \binfojini\b | \bakraya\b | \bmodis\b | \bcollabra\b
""")

# company_industry is a comma-joined multi-label ("Contingent Search, Contract
# Staffing, Retained Search, RPO") and is present on only ~21% of rows, so it is
# a third-string signal — but when it fires it is unambiguous.
#
# `human capital` was here and is deliberately gone.  It admitted YourCause —
# Blackbaud's philanthropy arm, posting on its own Workday — because PDL tagged
# it "Human Capital Services" among four other labels.  The phrase belongs to
# consultancies, HR software and corporate functions at least as much as to
# staffing desks, and one label in a five-label industry string is not evidence
# of what a company DOES.  Every term left here names the transaction.
AGENCY_INDUSTRY = re.compile(
    r"(?i)staffing|recruit|executive\s+search|\brpo\b|talent\s+solutions|"
    r"employment\s+agency|contingent\s+search|contract\s+staffing")

# Titles that ARE AI work even when the classifier scored them tier 9.
#
# EVERY TERM HERE MUST NAME AI CONTENT, and that is a narrower rule than it
# looks.  The first draft of this pattern also carried `solutions architect`,
# `platform engineer`, `integration engineer`, `automation engineer` and `data
# engineer` — the vocab T2 families — and it admitted "Windchill Solution
# Architect", "M365 Integration Engineer" and "Test Automation Engineer" to a
# lane called AI reqs, each one labelled "AI-adjacent title" on the card.  T2
# means "a doable job"; it does not mean "an AI job", and borrowing it here
# put roughly half the lane under a claim that was simply false.
#
# The general shape of that mistake is this repo's most repeated one: a pattern
# written for one question reused as the gate for another.  See the seed-regex
# note in the module docstring.
#
# Deliberately NOT including data science: vocab.ANTI kills "data
# scientist" as a profile anti-title, so admitting the family here would only
# manufacture rows for killed_by() to remove.
AI_ADJACENT = re.compile(r"""(?ix)
    \bai\b | \ba\.i\.| artificial\s+intelligence | \bagentic\b |
    machine\s+learning | \bml\s+engineer\b | \bmlops\b | \bnlp\b |
    deep\s+learning | computer\s+vision | \bgenerative\b |
    \bgenai\b | \bgen\s+ai\b | \bllms?\b | large\s+language |
    prompt\s+engineer | forward\s+deployed | \bfde\b | \brag\b |
    \bchatbot\b | \bcopilot\b | langchain | vector\s+(?:database|search)
""")

# Bands 1-4.  Wider than ai_startup_build's 1-2 because this population is
# classified badly (see the docstring) and band 4 here is not the 1,862 rows of
# generic ML work it is on the employer-direct side — it is 30 rows.
AI_MAX_TIER = 4

# What a name guess is worth against an ATS-domain proof.  Same shape as
# ai_startup_build.INHERIT and for the same reason: the failure mode being
# guarded against makes the weaker claim WRONG rather than merely weak — a
# company called "X Talent Partners" may be an employer with an unlucky name,
# where a jobdiva.com apply URL cannot be anything but an agency.
NAME_EVIDENCE = 4
HARD_EVIDENCE = 12


def apex_domain(url):
    """Registrable domain of a URL — the last two labels of the host.

    '' for anything without a host, so a blank apply_url falls through to the
    name arm instead of matching whatever the empty string would.
    """
    host = urlsplit(url or "").netloc.lower().split(":")[0]
    labels = [x for x in host.split(".") if x]
    return ".".join(labels[-2:]) if len(labels) >= 2 else ""


def agency_of(apply_url, job_url, company, industry):
    """(points, label) for the evidence that a staffing desk posted this row.

    (0, '') when nothing fires, which is how the caller drops the row.  The two
    arms are tested strongest-first and never summed: a JobDiva row from a
    company called "Acme Staffing" is one piece of proof, not two.
    """
    ats = apex_domain(apply_url) or apex_domain(job_url)
    if ats in STAFFING_ATS:
        return HARD_EVIDENCE, f"staffing ATS ({ats.split('.')[0]})"
    co = company or ""
    if KNOWN_AGENCY.search(co):
        return NAME_EVIDENCE, "known agency"
    if AGENCY_NAME.search(co):
        return NAME_EVIDENCE, "agency name"
    if AGENCY_INDUSTRY.search(industry or ""):
        return NAME_EVIDENCE, "agency industry"
    return 0, ""


def ai_fit(ai_band, title):
    """0-30 for AI strength, falling back to the title when the band is absent.

    The fallback is the whole reason this lane can exist at scale.  Reading the
    FIRST CHARACTER of `ai_band` rather than `ai_tier` is the trap
    triage_load.py documents — ai_tier is a text label and ranks every
    AI role as the weakest one.
    """
    b = (ai_band or "").strip()[:1]
    if b.isdigit():
        pts = {1: 30, 2: 22, 3: 14, 4: 12}.get(int(b), 0)
        label = {1: "FDE / agentic", 2: "AI engineer",
                 3: "core AI", 4: "AI/ML adjacent"}.get(int(b), "")
        if pts:
            return pts, label
    if AI_ADJACENT.search(title or ""):
        return 8, "AI-adjacent title"
    return 0, ""


def desk_fit(n_ai, n_reqs):
    """0-25 for how deep the desk is — the term that makes this a lane.

    Scored on the count of AI reqs the same agency has open, because that is
    what a recruiter relationship actually buys you.  Bucketed rather than
    linear: the difference between a 1-req and a 5-req desk is real, the
    difference between 18 and 20 is noise, and a linear term would let Diverse
    Lynx's 20 reqs swamp every fit signal in the lane.

    `n_reqs` rides along in the label only.  Total open reqs is a weaker proxy
    for the same thing and a busy generalist desk with one AI req is not what
    this rewards.
    """
    if n_ai >= 10:
        pts = 25
    elif n_ai >= 5:
        pts = 18
    elif n_ai >= 3:
        pts = 12
    elif n_ai >= 2:
        pts = 6
    else:
        return 0, ""
    return pts, f"desk: {n_ai} AI of {n_reqs} reqs"


def score_row(r, agency_pts, agency_label, n_ai, n_reqs, today):
    """(score, why) — every point traceable to a phrase in `why`."""
    parts, total = [], 0.0
    dom = bool(DOMAIN.search(f"{r['title']} {r['skills'] or ''} "
                             f"{r['company_industry'] or ''}"))
    for pts, label in (
        title_fit(r["title"] or "", dom),
        domain_fit(r["title"] or "", r["skills"] or "",
                   r["company_industry"] or ""),
        ai_fit(r["ai_band"] or "", r["title"] or ""),
        (agency_pts, agency_label),
        desk_fit(n_ai, n_reqs),
        geo_fit(r["bucket"] or "", r["basis_weak"]),
        fresh_wide(r["posted"] or "", today),
        comp_fit(r["salary_max"]),
    ):
        if pts:
            total += pts
            parts.append(f"{label} +{pts:g}")
    return round(total, 1), " · ".join(parts)


# ------------------------------------------------------------------------ main
# Aggregators are NOT excluded, and here that is not a tolerance — it is the
# point.  65 of the 86 AI agency rows are is_agg=1, because agency reqs reach
# this corpus as jobgether/careerjet spillover rather than through a fetcher of
# their own.  lanes.ORDER_TERMS leads with `is_agg ASC`, which is the exact
# mechanism burying them in every other lane, so AIAGENCY_TERMS drops that term
# and this query never had it.
CAND_SQL = f"""
SELECT job_key, ct_key, company, company_norm, title, skills, company_industry,
       startup_tier, ai_band, rank_tier, bucket, basis_weak, posted, salary_max,
       source, is_agg, apply_url, job_url
  FROM jobs
 WHERE feed = 'main' AND live = 1
   AND NOT EXISTS (SELECT 1 FROM decisions d WHERE d.job_key = jobs.job_key)
   AND NOT EXISTS (SELECT 1 FROM decisions d2
                    WHERE d2.ct_key = jobs.ct_key AND jobs.ct_key <> '')
"""

# Desk depth counts EVERY live req the agency has open, decided or not, and
# both feeds.  The count is a fact about the desk — how much work it is
# carrying — not about your queue, so filtering it to undecided rows would make
# a desk look thinner every time you triaged one of its jobs.
DESK_SQL = f"""
SELECT company_norm,
       COUNT(*) AS n_reqs,
       SUM(CASE WHEN rank_tier <= {AI_MAX_TIER} THEN 1 ELSE 0 END) AS n_ai
  FROM jobs
 WHERE live = 1 AND company_norm <> ''
 GROUP BY company_norm
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--db", default=DB)
    ap.add_argument("--top", type=int, default=0,
                    help="cap the lane (0 = no cap, the default)")
    ap.add_argument("--per-company", type=int, default=0,
                    help="cap rows per desk (0 = no cap).  Note this works "
                         "AGAINST the lane's thesis — the deep desks are the "
                         "point — so it is off by default.")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--show-killed", action="store_true",
                    help="print what the title filter excluded — read this "
                         "before trusting the filter")
    ap.add_argument("--show-dropped", action="store_true",
                    help="print agency rows that carried no AI evidence")
    args = ap.parse_args()

    today = dt.date.today()
    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    # This builder introduces desk_score/desk_why, so like ai_startup_build it
    # cannot assume its column exists: migrate() is idempotent and adds only
    # what is missing, so the lane can be built without a full main load first.
    triage_load.migrate(con)

    desks = {r["company_norm"]: (r["n_ai"], r["n_reqs"])
             for r in con.execute(DESK_SQL)}
    rows = con.execute(CAND_SQL).fetchall()
    print(f"{len(rows):,} undecided live main-feed rows")

    scored, killed, no_ai = [], [], []
    hard = named = 0
    for r in rows:
        ag_pts, ag_label = agency_of(r["apply_url"], r["job_url"],
                                     r["company"], r["company_industry"])
        if not ag_pts:
            continue
        ai_pts, _ = ai_fit(r["ai_band"] or "", r["title"] or "")
        if not ai_pts:
            no_ai.append(r)
            continue
        why_dead = killed_by(r["title"] or "", r["skills"] or "")
        if why_dead:
            killed.append((why_dead, r))
            continue
        n_ai, n_reqs = desks.get(r["company_norm"] or "", (0, 0))
        s, why = score_row(r, ag_pts, ag_label, n_ai, n_reqs, today)
        # The lane predicate is `desk_score > 0`, so a 0 here would admit a row
        # to the lane and then hide it.  Cannot happen — an admitted row always
        # carries AI evidence and agency evidence — but the lane's definition
        # depends on it, so it is asserted rather than assumed.
        if s <= 0:
            sys.exit(f"scored 0 with evidence — {r['company']} / {r['title']}")
        scored.append((s, r, why))
        if ag_pts == HARD_EVIDENCE:
            hard += 1
        else:
            named += 1

    # Never a silent cap: say what came out and why, on every run.
    print(f"  {len(no_ai):,} agency rows dropped — no AI band and no "
          f"AI-adjacent title")
    print(f"  {len(killed):,} excluded by title filter, {len(scored):,} scored "
          f"({hard:,} staffing-ATS proof, {named:,} name/industry guess)")
    if killed:
        print("   " + ", ".join(f"{k}={v}" for k, v in
                                Counter(k for k, _ in killed).most_common()))
    if args.show_killed:
        for reason, r in sorted(killed, key=lambda x: x[0]):
            print(f"    [{reason}] {r['company']} — {r['title']}")
    if args.show_dropped:
        for r in no_ai:
            print(f"    [no AI] {(r['company'] or '?')[:28]:28s} {r['title']}")

    if not scored:
        sys.exit("no rows scored — refusing to blank the lane")

    # The 19 rows this lane cannot reach, reported rather than silently missing.
    # lanes.py scopes every predicate lane to feed='main' on purpose ("Each
    # extra feed brings its own lane instead"), so contract-feed agency reqs are
    # out of scope here and this is the only place that says so.
    n_contract = con.execute(
        f"SELECT COUNT(*) FROM jobs WHERE feed <> 'main' AND live = 1 "
        f"AND rank_tier <= {AI_MAX_TIER}").fetchone()[0]
    print(f"   ({n_contract:,} AI rows sit in the contract feed and are out of "
          f"this lane's scope by design)")

    src = Counter(r["source"] for _, r, _ in scored)
    print("   sources: " + ", ".join(f"{k} {v}" for k, v in src.most_common(10)))

    # Same order the lane itself uses (lanes.AIAGENCY_TERMS: desk_score DESC,
    # basis_weak ASC, posted DESC, job_key ASC), so the preview below is the head
    # of the real queue and not a second opinion about it.  Built as stable sorts
    # applied least-significant key first, because `posted` sorts DESC and a
    # string cannot be negated inside a composite key.
    scored.sort(key=lambda t: t[1]["job_key"])
    scored.sort(key=lambda t: t[1]["posted"] or "", reverse=True)
    scored.sort(key=lambda t: (-t[0], t[1]["basis_weak"]))

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

    desk_n = Counter((r["company"] or "?") for _, r, _ in scored)
    print(f"\n{len(desk_n)} desks in the lane; busiest: "
          + ", ".join(f"{c} x{n}" for c, n in desk_n.most_common(6)))

    print(f"\ntop {min(40, len(scored))} of {len(scored):,}:")
    for s, r, why in scored[:40]:
        print(f"  {s:6.1f}  {(r['company'] or '')[:22]:22s} "
              f"{(r['title'] or '')[:44]:44s} {r['posted'] or '':10s}  {why}")

    if args.dry_run:
        print("\n--dry-run: nothing written")
        return

    with con:
        # Cleared on `desk_score <> 0` rather than on this lane's predicate.  A
        # row whose company has since been renamed, or whose apply_url changed
        # off a staffing ATS, no longer matches the candidate query — so a
        # predicate-scoped clear would leave it in the lane forever carrying the
        # score it earned under the old data.
        con.execute("UPDATE jobs SET desk_score = 0, desk_why = '' "
                    "WHERE feed = 'main' AND desk_score <> 0")
        con.executemany(
            "UPDATE jobs SET desk_score = ?, desk_why = ? WHERE job_key = ?",
            [(s, why, r["job_key"]) for s, r, why in scored])
    print(f"\nwrote desk_score on {len(scored):,} rows")

    # WITHOUT THIS THE LANE'S INDEX IS DEAD WEIGHT — see ai_startup_build.py's
    # note for the measurement.  idx_jobs_aiagency is partial (WHERE desk_score
    # > 0) and covers ~200 of 288,000 rows, but with no sqlite_stat1 to read
    # SQLite cannot know that and picks idx_jobs_feed instead, which matches
    # every live main-feed row.
    con.execute("ANALYZE")
    con.commit()


if __name__ == "__main__":
    main()
