#!/usr/bin/env python3
"""vocab.py — the role vocabulary, and the lane that ranks against it.

Two things live here, and the split is the point.

**Preference** — which titles are on target, which domain words make a posting
interesting, which titles are excluded outright — is DATA, and is read from
`profile.json` (falling back to the checked-in `profile.example.json`, so a
fresh clone runs with no setup).

**Gates** — WRONG_ROLE, JUNK_TITLE, the lab/defense test, COMMERCIAL — are
MECHANISM, and stay in code.  They are true of the job market regardless of who
is searching: a posting for a paralegal, a summer intern or a forklift operator
is not a software role for anybody, and nobody should have to re-derive that
list to use this tool.

Why a LANE and not a feed
-------------------------
These rows already exist in the database under their own job_keys.  Minting a
second key for the same posting is how a job gets judged twice and collects two
different verdicts — the same failure that made both contract lanes share one
feed.  Tagged instead, it stays one row: deciding it here clears it everywhere.

Operational caveat you will otherwise learn the hard way
--------------------------------------------------------
`triage_load.py` upserts lane_tags/score/score_why from the CSV, and the main
feed's CSV carries none of those columns.  So every `triage_load.py --feed main`
silently empties this lane.  Re-run this script after any main load.

Usage
-----
    python3 vocab.py --dry-run --show-killed   # read the kill list FIRST
    python3 vocab.py --top 250
"""
import argparse
import datetime as dt
import json
import math
import os
import re
import sqlite3
import sys
from collections import Counter

BASE = os.path.dirname(os.path.abspath(__file__))
if BASE not in sys.path:
    sys.path.insert(0, BASE)

DB = os.path.join(BASE, "triage.db")
PROFILE = os.path.join(BASE, "apply", "profile.json")
TAG = "picks"

# Imported, never copied: a second transcription of the AI vocabulary is a second
# thing to keep in sync, and these two have already drifted once (vet_build's
# bands vs ai_ma_remote's — see new_jobs_build.py).
from ai_jobs_filter import ADJ, CORE, SKILL          # noqa: E402
from startup_build import JUNK_TITLE                 # noqa: E402


# ------------------------------------------------------------------ vocabulary
# Three title tiers, built from apply/profile.json preferences.target_titles_*.
# The profile lists titles as prose ("Tokenization / RWA Consultant"); real
# postings never match that literally, so each is carried here as the pattern
# that actually finds it.

# Note what is NOT here: bare domain nouns.  "capital markets" / "private
# markets" / "digital assets" name a DESK, not a job, so scoring them as title
# fit made the top of the lane a Legal Counsel at O-I Glass, an Administrative
# Assistant at Bank of America (London) and a 2027 Summer Analyst — every one of
# them a 40-point "target title" because it sat on a capital-markets desk.  The
# domain words earn their 20 points in domain_fit() instead, where they belong;
# here the finance titles are anchored to the ROLE.
# --------------------------------------------------------------- the profile
BASE = os.path.dirname(os.path.abspath(__file__))


def _load_profile():
    """The tunable half, as data.  profile.json wins; the example is the
    fallback so a fresh clone is runnable before anyone configures anything."""
    for name in ("profile.json", "profile.example.json"):
        path = os.path.join(BASE, name)
        if os.path.exists(path):
            with open(path, encoding="utf-8") as fh:
                return json.load(fh), name
    raise SystemExit("vocab: no profile.json or profile.example.json beside me")


PROFILE, PROFILE_SRC = _load_profile()


def _rx(parts):
    """Compile a list of fragments as one alternation.

    VERBOSE, so fragments in the JSON read the way the literal patterns further
    down do: whitespace ignored, an explicit \\s+ required between words.

    An empty list compiles to a pattern that can never match, NOT to the empty
    pattern -- `re.compile("")` matches every string, so an unset preference
    would silently mark the entire corpus on target.
    """
    if not parts:
        return re.compile(r"(?!x)x")
    return re.compile("|".join(parts), re.I | re.X)


_TT = PROFILE.get("target_titles", {})
T1 = _rx(_TT.get("tier1", []))      # 40 pts — squarely the target
T2 = _rx(_TT.get("tier2", []))      # 32 pts — strong
T3 = _rx(_TT.get("tier3", []))      # 22 pts — adjacent, still a real fit

# The domain lane's own role gate.  Engineering role patterns (T1-T3) find
# almost none of the finance pool — those titles read "Senior Associate,
# Portfolio Operations (Private Markets)" — so gating the whole lane on them
# left 9 of ~80 finance rows standing.  A domain-matched row only has to look
# like a professional role; WRONG_ROLE has already removed the lawyers, admins,
# interns, accountants and recruiters that a desk name attracts.
FIN_ROLE = re.compile(r"""(?ix)
    \banalyst\b | \bassociate\b | \bconsultant\b | \bstrategist\b | \bstrategy\b |
    \bmanager\b | \bdirector\b | \bprincipal\b | \blead\b | \bhead\b | \bvp\b |
    vice\s+president | \bspecialist\b | \bproduct\b |
    business\s+development | \bsales\b | partnerships? | \bengineer\b |
    \barchitect\b | \bsolutions?\b | \badvisor\b
""")

# The differentiated half: what makes a posting specifically interesting
# rather than merely on-title.  Worth 20 points, and it opens the FIN_ROLE gate
# above for titles that do not look like an engineering ladder.
DOMAIN = _rx(PROFILE.get("domain_keywords", {}).get("patterns", []))

# Hard title exclusions.  Deliberately small -- a scalpel, where WRONG_ROLE
# below is the backstop.
ANTI = _rx(PROFILE.get("anti_titles", {}).get("patterns", []))

# Functions outside the target set, whatever desk they sit on.  This is the
# backstop for the domain lane: "Capital Markets" attracts lawyers, admins,
# summer analysts and controllers, and a role-anchored T1 alone still lets some
# of them through ("Legal Counsel - ... & Capital Markets" leads with a role
# word).  Cheap, and it fails safe — everything here is unambiguously not an
# on-target role.
WRONG_ROLE = re.compile(r"""(?ix)
    legal\s+counsel | \battorney\b | \bparalegal\b | \bcounsel\b |
    administrative\s+assistant | executive\s+assistant | \breceptionist\b |
    \bintern\b | \binternship\b | summer\s+analyst | \bco[\s-]?op\b |
    \bnurse\b | \bphysician\b | \bclinician\b | clinical\s+(?:research|trial) |
    \bpharmacist\b | \btherapist\b | \bcaregiver\b |
    \baccountant\b | \bcontroller\b | \bauditor\b | \btax\s+(?:manager|analyst) |
    \bbookkeep | accounts\s+(?:payable|receivable) |
    \brecruiter\b | talent\s+acquisition | \bhuman\s+resources\b |
    \bteacher\b | \bprofessor\b | \btutor\b |
    \bwarehouse\b | \bdriver\b | \bcustodian\b | \bjanitor\b |
    \bsecurity\s+officer\b | \bwelder\b | \bmachinist\b | \btechnician\b |
    # Back office.  DOMAIN matches company_industry, so at KKR, State Street and
    # Bain Capital EVERY posting reads as "private markets" — which admitted a
    # Head of Accounting SVP, an ETF Operations VP and a Risk & Controls manager.
    # The profile is front-office and technical: BD, strategy, consulting,
    # solutions, product, engineering.  None of this.
    \baccounting\b | fund\s+administration | \bcustody\b | \bsettlements?\b |
    reconciliation | \bcompliance\b | internal\s+audit | \bcontrols\b |
    \bunderwrit | \bactuar | middle\s+office | back\s+office |
    loan\s+servicing | \bcollections\b | \bpayroll\b | \bprocurement\b |
    risk\s+(?:manager|analyst|process) | portfolio\s+(?:accounting|operations)
""")

# A title whose FUNCTION is commercial.  When one of these appears at a robotics
# or hardware company, the lab/defense words land in `skills` because they
# describe the employer's product, not the work — Niantic Spatial's "Business
# Development Leader" and Velo3D's "BD Account Manager" are business-development
# jobs, and killing them on the company's domain is the blocklist failing in the
# direction that costs you real finds.
COMMERCIAL = re.compile(r"""(?ix)
    business\s+development | \bbd\b | account\s+(?:executive|manager|director) |
    \bsales\b | go[\s-]to[\s-]market | \bgtm\b | partnerships? |
    customer\s+success | solutions?\s+(?:engineer|consultant|architect|specialist) |
    technical\s+account | professional\s+services
""")

# The filter that decides whether this lane is usable at all.
#
# In MA, "Automation Engineer" is overwhelmingly wet-lab and manufacturing
# (CQV, "Platform Biology", SLA & SLS robotics, Insulet, Formlabs, Verista) and
# "Platform Engineer" pulls in defense (Pratt & Whitney C5I).  Without this the
# tab fills with jobs that are neither doable nor wanted.
#
# Precision matters more than recall.  A blunt list fails in BOTH directions —
# the same way the country-name blocklist did before ma_remote_build.py
# replaced it with an allowlist.  Two deliberate narrowings:
#   * `validation engineer` as a phrase, never bare `validation` (which would
#     eat "model validation", "data validation").
#   * `robotic` is handled separately below so that robotic PROCESS automation
#     (RPA) — which is squarely relevant — survives.
#
# Matched against title + skills only, NEVER company_industry: a full-stack
# engineer at a biotech is a software job, and killing on industry would drop
# every real role at every life-sciences employer in the state.
NEG = re.compile(r"""(?ix)
    \bcqv\b | \bgmp\b | \bcgmp\b | \bgxp\b | \bgcp\s+compliance\b |
    \bvalidation\s+engineer\b | \bcommissioning\b | qualification\s+engineer |
    bioprocess | biomanufactur | \bassay\b | wet[\s-]?lab | clean\s?room |
    chromatograph | \bhplc\b | \bsterile\b | \bbioreactor\b | \bupstream\s+process |
    platform\s+biology | cell\s+(?:culture|therapy|line) | \bvivarium\b |
    \bplc\b | \bscada\b | \bhmi\b | allen[\s-]bradley | siemens\s+s7 |
    ladder\s+logic | mechatronic | \bcnc\b | \bpackaging\s+line |
    instrumentation\s+(?:and|&)\s+controls | controls\s+engineer |
    calibration\s+(?:technician|engineer) | additive\s+manufactur |
    \bsla\s*&\s*sls\b | injection\s+molding |
    \bitar\b | security\s+clearance | \bts/sci\b | \bpolygraph\b |
    \bavionic | \bmissile\b | \bradar\b | \bmunition | \bwarfare\b |
    \bc5i\b | \bc4isr\b | \bnuclear\b
""")
ROBOT = re.compile(r"(?i)\brobotics?\b")
RPA = re.compile(r"(?i)robotic\s+process\s+automation|\brpa\b")


def _lab(text):
    m = NEG.search(text)
    if m:
        return re.sub(r"\s+", " ", m.group(0)).strip()
    if ROBOT.search(text) and not RPA.search(text):
        return "robotics"
    return None


def killed_by(title, skills):
    """Reason this row is excluded, or None.  One place, so --show-killed and
    the real run can never disagree about what the filter does.

    Title and skills are judged separately on purpose.  A lab/plant word in the
    TITLE is the job.  The same word in `skills` is usually the employer's
    product line, so it only condemns an engineering title — a commercial title
    survives it.
    """
    if ANTI.search(title):
        return "anti_title"
    if JUNK_TITLE.search(title):
        return "junk_title"
    if WRONG_ROLE.search(title):
        return "wrong_role"
    hit = _lab(title)
    if hit:
        return "lab/defense: " + hit
    if not COMMERCIAL.search(title):
        hit = _lab(skills)
        if hit:
            return "lab/defense (skills): " + hit
    return None


# ---------------------------------------------------------------------- scoring
def title_fit(title, domain_hit=False):
    if T1.search(title):
        return 40, "target title"
    if T2.search(title):
        return 32, "strong title"
    if T3.search(title):
        return 22, "adjacent title"
    # The domain gate.  A professional role at an employer whose whole product
    # is the target domain is a real fit even when the title looks nothing like
    # an engineering ladder -- gating the lane on T1-T3 alone left 9 of ~80 such
    # rows standing, because those titles read "Senior Associate, Platform
    # Operations" rather than "Engineer".
    if domain_hit and FIN_ROLE.search(title):
        return 22, "domain role"
    return 0, ""


def domain_fit(title, skills, industry):
    blob = f"{title} {skills} {industry}"
    if DOMAIN.search(blob):
        return 20, "finance domain"
    if CORE.search(title) or CORE.search(skills):
        return 20, "core AI"
    if ADJ.search(title) or SKILL.search(skills):
        return 10, "AI adjacent"
    return 0, ""


def company_fit(tier):
    if tier.startswith("1"):
        return 12, "startup 1"
    if tier.startswith("2"):
        return 9, "startup 2"
    if tier.startswith("3"):
        return 5, "startup 3"
    return 0, ""


def geo_fit(bucket, basis_weak):
    if bucket == "MA local":
        return 10, "MA local"
    # A named second market, so it must be tested before the remote legs: they
    # are a fallthrough, and a South FL row reaching them would be scored AND
    # LABELLED "remote (inferred US)", which is false on both counts.  This runs
    # well beyond the South FL lane — swath_build.py imports geo_fit and all
    # twelve lanes_build.py lanes score with it.
    if bucket == "South FL":
        return 8, "South FL"
    if not basis_weak:
        return 7, "US remote"
    return 4, "remote (inferred US)"


def fresh_fit(posted, today):
    """An exponential freshness decay — a 20-day-old posting
    is worth a fifth of a same-day one, not four fifths."""
    if not posted:
        return 0, ""
    try:
        d = (today - dt.date.fromisoformat(posted)).days
    except ValueError:
        return 0, ""
    d = max(d, 0)
    return round(10 * math.exp(-d / 5.0), 1), f"{d}d old"


def comp_fit(salary_max):
    try:
        v = float(salary_max)
    except (TypeError, ValueError):
        return 0, ""
    if v <= 0:
        return 0, ""
    # Yearly figures only; an hourly rate would score as "unknown" rather than
    # as a rounding error, which is the safe direction.
    if v < 1000:
        return 0, ""
    if v >= 150000:
        return 8, f"${v / 1000:.0f}k"
    if v >= 110000:
        return 6, f"${v / 1000:.0f}k"
    return 2, f"${v / 1000:.0f}k"


def score_row(r, today):
    """(score, why) — every point traceable to a phrase in `why`."""
    parts, total = [], 0.0
    dom = bool(DOMAIN.search(f"{r['title']} {r['skills'] or ''} "
                             f"{r['company_industry'] or ''}"))
    for pts, label in (
        title_fit(r["title"] or "", dom),
        domain_fit(r["title"] or "", r["skills"] or "", r["company_industry"] or ""),
        company_fit(r["startup_tier"] or ""),
        geo_fit(r["bucket"] or "", r["basis_weak"]),
        fresh_fit(r["posted"] or "", today),
        comp_fit(r["salary_max"]),
    ):
        if pts:
            total += pts
            parts.append(f"{label} +{pts:g}")
    return round(total, 1), " · ".join(parts)


# ------------------------------------------------------------------------ main
CAND_SQL = """
SELECT job_key, ct_key, company, company_norm, title, skills, company_industry,
       startup_tier, bucket, basis_weak, posted, salary_max, source
  FROM jobs
 WHERE feed = 'main' AND live = 1 AND is_agg = 0
   AND posted >= ?
   AND NOT EXISTS (SELECT 1 FROM decisions d WHERE d.job_key = jobs.job_key)
   AND NOT EXISTS (SELECT 1 FROM decisions d2
                    WHERE d2.ct_key = jobs.ct_key AND jobs.ct_key <> '')
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=250)
    ap.add_argument("--days", type=int, default=21)
    ap.add_argument("--per-company", type=int, default=0,
                    help="cap rows per company (0 = no cap)")
    ap.add_argument("--db", default=DB)
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--show-killed", action="store_true",
                    help="print what the lab/defense filter excluded — read this "
                         "before trusting the filter")
    args = ap.parse_args()

    today = dt.date.today()
    cutoff = (today - dt.timedelta(days=args.days)).isoformat()

    con = sqlite3.connect(args.db)
    con.row_factory = sqlite3.Row
    rows = con.execute(CAND_SQL, (cutoff,)).fetchall()
    print(f"{len(rows):,} undecided, live, non-aggregator rows since {cutoff}")

    scored, killed = [], []
    for r in rows:
        title = r["title"] or ""
        # Candidacy is a ROLE match, always.  A domain hit alone is not a ticket
        # in — that is what admitted the lawyers and the summer analysts — it is
        # worth 20 points to a row that already looks like an on-target role.
        dom = bool(DOMAIN.search(f"{title} {r['skills'] or ''} "
                                 f"{r['company_industry'] or ''}"))
        tpts, _ = title_fit(title, dom)
        if not tpts:
            continue
        why_kill = killed_by(title, r["skills"] or "")
        if why_kill:
            killed.append((why_kill, r["company"], title))
            continue
        s, why = score_row(r, today)
        scored.append((s, r, why))

    print(f"{len(scored) + len(killed):,} matched a target pool; "
          f"{len(killed):,} excluded by the lab/defense/anti filter")

    if args.show_killed:
        print("\n--- excluded (verify these are all genuinely wrong) ---")
        for reason, co, title in sorted(killed):
            print(f"  {reason:<34} {(co or '?')[:26]:<26} {title[:58]}")
        print(f"--- {len(killed)} excluded ---\n")
        print("exclusion reasons:")
        for reason, n in Counter(k[0] for k in killed).most_common():
            print(f"  {n:>4}  {reason}")
        print()

    scored.sort(key=lambda t: (-t[0], t[1]["posted"] or "", t[1]["job_key"]))

    if args.per_company:
        seen, capped = Counter(), []
        for s, r, why in scored:
            key = r["company_norm"] or r["job_key"]
            if seen[key] >= args.per_company:
                continue
            seen[key] += 1
            capped.append((s, r, why))
        print(f"per-company cap {args.per_company}: "
              f"{len(scored):,} -> {len(capped):,}")
        scored = capped

    picks = scored[:args.top]
    if not picks:
        sys.exit("no candidates matched — refusing to blank the lane")

    print(f"\nscore distribution of the {len(picks)} picks:")
    for lo in range(90, 19, -10):
        n = sum(1 for s, _, _ in picks if lo <= s < lo + 10)
        if n:
            print(f"  {lo:>3}-{lo + 9:<3} {'#' * min(n, 60)} {n}")
    print(f"  range {picks[-1][0]:g} .. {picks[0][0]:g}")

    print(f"\ntop 40 of {len(picks)}:")
    for s, r, why in picks[:40]:
        print(f"  {s:>5.1f}  {(r['company'] or '?')[:24]:<24} "
              f"{(r['title'] or '')[:52]:<52} {(r['bucket'] or '')[:9]:<9} "
              f"{r['posted'] or ''}")

    n_ma = sum(1 for _, r, _ in picks if r["bucket"] == "MA local")
    n_fin = sum(1 for _, r, _ in picks
                if DOMAIN.search(f"{r['title']} {r['skills'] or ''} "
                                 f"{r['company_industry'] or ''}"))
    top_co = Counter((r["company"] or "?") for _, r, _ in picks).most_common(5)
    print(f"\nMA local {n_ma} · remote {len(picks) - n_ma} · finance/domain {n_fin}")
    print("busiest companies: "
          + ", ".join(f"{c} x{n}" for c, n in top_co if n > 1) or "none repeated")

    if args.dry_run:
        print("\n--dry-run: nothing written")
        return

    # Feed scope on the clear is load-bearing, exactly as in triage_load.retire():
    # an unscoped wipe would strip ,contract,/,fastmoney, off the contract pool
    # and silently empty two other lanes.
    with con:
        con.execute(
            "UPDATE jobs SET lane_tags='', score=0, score_why='' "
            "WHERE feed='main' AND lane_tags LIKE ?", (f"%,{TAG},%",))
        con.executemany(
            "UPDATE jobs SET lane_tags=?, score=?, score_why=? WHERE job_key=?",
            [(f",{TAG},", s, why, r["job_key"]) for s, r, why in picks])

    n = con.execute("SELECT COUNT(*) FROM jobs WHERE feed='main' "
                    "AND lane_tags LIKE ?", (f"%,{TAG},%",)).fetchone()[0]
    print(f"\ntagged {n} rows into the `{TAG}` lane")
    print("restart triage/server.py to pick it up")


if __name__ == "__main__":
    main()


# ------------------------------------------------------- wide-lane variants
# These three are the "all dates, any band" counterparts of the gated scorers
# above.  They live here rather than in a lane module because more than one
# lane needs them and a second copy is how two lanes start disagreeing about
# what a tier-4 row is worth.

def ai_fit(ai_band):
    """0-15 for AI strength, read off the FIRST CHARACTER of `ai_band`.

    `title_fit` answers a different question -- "is this one of the roles being
    targeted" -- and a band-1 AI role with an off-target title scores 0 on it.
    On a lane named for AI that is the wrong answer, so AI strength gets its
    own axis.

    `ai_band` and NOT `ai_tier`: ai_tier is a text label ("Core AI Building"),
    and reading it where a number belongs silently ranks every AI role as the
    weakest one.
    """
    b = (ai_band or "").strip()[:1]
    if not b.isdigit():
        return 0, ""
    pts = {1: 15, 2: 12, 3: 8, 4: 5, 5: 2}.get(int(b), 0)
    return pts, f"AI band {b}" if pts else ""


def startup_fit(tier):
    """0-12 -- company_fit, plus the tier a wide lane admits and it does not.

    company_fit stops at tier 3 because the gated lane never admits a tier-4
    row.  A wide lane does, and without this such a row scores nothing for the
    very evidence that let it in: the weakest land on 0.0 with an empty `why`,
    a card carrying no number and no reason, which is the one thing the scoring
    exists to prevent.  Measured before the fix: 53 such rows.

    Wrapped rather than fixed in place -- twelve lanes import company_fit, and
    widening it there would move rankings for a tier they deliberately exclude.
    """
    pts, label = company_fit(tier)
    if pts:
        return pts, label
    if (tier or "").startswith("4"):
        return 3, "startup 4"
    return 0, ""


def fresh_wide(posted, today):
    """0-25 for recency on a ~21-day half-life, for all-dates lanes.

    Deliberately NOT `fresh_fit`, which decays on exp(-d/5).  That constant is
    right for a lane windowed to the last three weeks and wrong for one that is
    not: most rows in an all-dates lane are older than 21 days, and on a 5-day
    constant they all collapse to 0 and stop being ordered by recency at all --
    over exactly the range the lane spans.  exp(-d/30) is still worth ~1.2
    points at 90 days, the far end of the corpus.

    Fails soft: an unparseable date scores 0, and a future-dated posting is
    clamped to 0 days rather than scoring above today's.
    """
    if not posted:
        return 0, ""
    try:
        d = (today - dt.date.fromisoformat(posted)).days
    except ValueError:
        return 0, ""
    d = max(d, 0)
    return round(25 * math.exp(-d / 30.0), 1), f"{d}d old"


# ------------------------------------------------------------ title identity
# Seniority words are stopwords here on purpose: the whole point of same_req is
# to see through them.
_STOP = {"the", "a", "an", "of", "and", "for", "to", "in", "at", "on", "with", "or",
         "senior", "sr", "staff", "lead", "principal", "junior", "jr", "ii", "iii", "iv",
         "i", "remote", "us", "usa", "contract", "fulltime", "full", "time", "new"}


def toks(title):
    return {w for w in re.split(r"[^a-z0-9+#]+", (title or "").lower())
            if w and w not in _STOP}


def same_req(a, b):
    """True when two titles are the same requisition wearing different words.

    Jaccard over content tokens rather than string equality: aggregators
    routinely restate a title ("Senior AI Engineer, Platform" -> "AI Engineer -
    Platform Team") and an exact match finds almost none of them.  0.6 keeps
    those two together while separating "AI Engineer" from "AI Engineering
    Manager", which differ by one token in a set of three.
    """
    ta, tb = toks(a), toks(b)
    if not ta or not tb:
        return False
    inter = len(ta & tb)
    return (inter / max(1, len(ta | tb)) >= 0.6
            or (inter >= 3 and inter == min(len(ta), len(tb))))
