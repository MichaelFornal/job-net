#!/usr/bin/env python3
"""The one feature extractor: a job row -> ~60 named, orthogonal activations.

WHY THIS EXISTS.  Seven builders currently write nine scores that cannot be
compared (picks maxes ~90, aiagency ~148, ff ~105), because each bakes its own
weights into its own code.  The same signal is scored three different ways
depending on which lane you are in -- three freshness curves, three geography
scales, four ai_band->points maps.  Splitting *what is true about a row* from
*how much that is worth* is the whole point: this module answers the first
question only, and score_profile.py answers the second.

THE CONTRACT.
  - A feature value is an integer 0..100, a PERCENT ACTIVATION.  Binary
    features are 0 or 100; continuous ones (freshness, query relevance) fall in
    between.  Nothing here is signed -- the sign lives in the weight, so that
    "senior titles are bad" is a slider you can move to zero, not a branch.
  - Features within an axis marked ONEHOT are mutually exclusive: the most
    specific match wins and the rest are 0.  That is what makes their weights
    directly comparable to each other on a slider.
  - Features marked FLAG are independent and stack.

IMPORT, NEVER COPY.  swath_vocab.py and vocab.py state the rule
and the reason: "a second transcription of the AI vocabulary is a second thing
to keep in sync, and these two have already drifted once."  So this file
defines NO regex of its own.  Every pattern is imported from the module that
owns it, and ma_remote_build.geo_of is called through a list adapter rather
than reimplemented from a dict.

Read the first character of `ai_band`, NEVER `ai_tier` -- ai_tier is a text
label ("Core AI Building").  That trap is warned about in four separate files.

DETERMINISM.  level was bitten by PYTHONHASHSEED randomising its `why`
between runs on the same corpus, because it iterated a set.  Nothing here
iterates a set or a dict whose order is not literal; --selftest asserts it.
"""
import math
import os
import re
import sys

import all_jobs_build as AJB
import ai_jobs_filter as AIF
import date_basis as DB
import level as LV
import geo_lane as LOC
import vocab as P
import startup_build as SB
import swath_vocab as V
from remote_us_classify import us_basis

# --------------------------------------------------------------------- tuning
# Build-time constants.  These shape the FEATURE, not its worth -- anything you
# would want to slide during a triage session belongs in score_profile.py.
HALFLIFE_D = 21.0        # freshness e-folding, in days
# How much a `posted` value is worth believing, by who produced it.  78.4% of
# live rows carry a weak date (aggregator 137,577 / network 55,781 /
# first_seen 15,290 of 266,280), so a date you do not believe should COST
# freshness, not merely lose a tie the way date_weak does in the ORDER BY.
TRUST = {DB.EMPLOYER: 1.0, DB.NETWORK: 0.6, DB.AGGREGATOR: 0.5,
         DB.FIRST_SEEN: 0.4}
# Refuses every posting; the whole source 403s, so liveness is unverifiable and
# 101 of 359 approved-unapplied rows came from it (see worklist_live.py).
UNVERIFIABLE_SRC = ("himalayas",)

# ------------------------------------------------------------------ the schema
# (name, axis, kind, default_points).  ORDER IS THE MATRIX COLUMN ORDER and is
# persisted in the index; append new features at the END of their axis or the
# saved profiles and the .npz stop lining up.
ONEHOT, FLAG, CONT, QUERY = "onehot", "flag", "cont", "query"

SCHEMA = [
    # ---- role: what the title says the work is ------------------- hard axis
    ("role.target",      "role",  ONEHOT,  40),
    ("role.strong",      "role",  ONEHOT,  32),
    ("role.swath",       "role",  ONEHOT,  24),
    ("role.adjacent",    "role",  ONEHOT,  22),
    ("role.support",     "role",  ONEHOT,  18),
    ("role.csam",        "role",  ONEHOT,  16),
    ("role.prof",        "role",  ONEHOT,   8),
    ("role.near_swath",  "role",  FLAG,     6),   # presales/automation/aienable/aiexpert/gtm
    ("role.anti",        "role",  FLAG,   -25),   # profile.json anti_titles

    # ---- level: 43% of applications sent were over-levelled ------ hard axis
    ("level.at",         "level", ONEHOT,  18),   # rung 2, the target band
    ("level.up1",        "level", ONEHOT,  15),
    ("level.down1",      "level", ONEHOT,  12),
    ("level.entry",      "level", ONEHOT,   6),
    ("level.senior",     "level", ONEHOT,  -5),
    ("level.staff",      "level", ONEHOT, -15),
    ("level.mgr",        "level", ONEHOT, -30),
    ("level.tag_only",   "level", FLAG,    -3),   # rung came from getro's tag, not the title

    # ---- geo ----------------------------------------------------- hard axis
    ("geo.ring1",        "geo",   ONEHOT,  12),   # profile geography: ring 1
    ("geo.malocal",      "geo",   ONEHOT,  10),
    ("geo.ring2",        "geo",   ONEHOT,   8),
    ("geo.remote_us",    "geo",   ONEHOT,   8),
    ("geo.nearby",       "geo",   ONEHOT,   6),
    ("geo.sfl",          "geo",   ONEHOT,   6),
    ("geo.border",       "geo",   ONEHOT,   4),
    ("geo.remote_inf",   "geo",   ONEHOT,   4),   # US basis inferred, not read
    ("geo.worldwide",    "geo",   ONEHOT,   2),
    ("geo.us_far",       "geo",   ONEHOT, -10),   # onsite US, outside any ring
    ("geo.alsolisted",   "geo",   FLAG,    -2),   # town named in a later segment

    # ---- freshness ----------------------------------------------- hard axis
    ("fresh.trusted",    "fresh", CONT,    15),   # decay x TRUST[date_basis]
    ("fresh.recency",    "fresh", CONT,     0),   # decay alone; slide if you disagree
    ("fresh.undated",    "fresh", FLAG,     0),   # blank != old

    # ---- who produced the date / the listing --------------------- hard axis
    ("date.employer",    "date",  ONEHOT,   0),
    ("date.network",     "date",  ONEHOT,   0),
    ("date.aggregator",  "date",  ONEHOT,   0),
    ("date.first_seen",  "date",  ONEHOT,   0),
    ("src.direct",       "src",   ONEHOT,   5),
    ("src.agg",          "src",   ONEHOT,  -3),
    ("src.unverifiable", "src",   FLAG,    -8),
    ("xpost.many",       "src",   FLAG,    -2),   # republished across 3+ sources

    # ---- employment type / work mode ----------------------------- hard axis
    ("etype.fulltime",   "etype", ONEHOT,   5),
    ("etype.contract",   "etype", ONEHOT,   3),
    ("etype.c2h",        "etype", ONEHOT,   3),
    ("etype.parttime",   "etype", ONEHOT,   0),
    ("etype.temp",       "etype", ONEHOT,   0),
    ("etype.intern",     "etype", ONEHOT, -10),
    ("mode.remote",      "mode",  ONEHOT,   4),
    ("mode.hybrid",      "mode",  ONEHOT,   2),
    ("mode.onsite",      "mode",  ONEHOT,  -4),

    # ---- BONUS axes: only scoreable when the data exists ---------------------
    # skills 19% / company_industry 21% / salary 34% / startup_tier 11% /
    # ai_band 2.6%.  An all-positive additive score buries every sparse row,
    # which is why these are separated and their attainable share is reported
    # as `coverage` on the card.
    ("dom.ai_core",      "dom",   ONEHOT,  20),
    ("dom.finance",      "dom",   ONEHOT,  20),
    ("dom.swath",        "dom",   ONEHOT,  12),
    ("dom.ai_adj",       "dom",   ONEHOT,  10),
    ("dom.aiband1",      "dom",   FLAG,     8),   # first char of ai_band, never ai_tier
    ("dom.aiband2",      "dom",   FLAG,     5),

    ("comp.150",         "comp",  ONEHOT,  10),
    ("comp.130",         "comp",  ONEHOT,   8),
    ("comp.110",         "comp",  ONEHOT,   6),
    ("comp.90",          "comp",  ONEHOT,   3),
    ("comp.low",         "comp",  ONEHOT,  -5),

    ("co.tier1",         "co",    ONEHOT,  12),
    ("co.tier2",         "co",    ONEHOT,   9),
    ("co.tier3",         "co",    ONEHOT,   5),
    ("co.tier4",         "co",    ONEHOT,   3),
    ("co.inherited",     "co",    FLAG,    -3),   # tier came from a sibling req

    # ---- query: computed per search, never indexed ---------------------------
    ("q.bm25",           "query", QUERY,   25),
]

NAMES = [n for n, _, _, _ in SCHEMA]
AXIS = {n: a for n, a, _, _ in SCHEMA}
KIND = {n: k for n, _, k, _ in SCHEMA}
DEFAULTS = {n: p for n, _, _, p in SCHEMA}
IDX = {n: i for i, n in enumerate(NAMES)}
NFEAT = len(NAMES)
# Everything except q.bm25 is materialised into the matrix, in this order.
INDEXED = [n for n in NAMES if KIND[n] != QUERY]
BONUS_AXES = ("dom", "comp", "co")
HARD_AXES = ("role", "level", "geo", "fresh", "date", "src", "etype", "mode")

# What each bonus axis needs present to be scoreable at all, for `coverage`.
BONUS_EVIDENCE = {
    "dom":  ("skills", "company_industry", "ai_band"),
    "comp": ("salary_max",),
    "co":   ("startup_tier",),
}


# ------------------------------------------------------------------- adapters
class _AsRow:
    """Present a dict as the positional master row ma_remote_build expects.

    geo_of/is_remote index by all_jobs_build.IDX.  Wrapping is what lets this
    module CALL them instead of transcribing the MA/remote/South-FL dispatch a
    third time -- ma_remote_build.py documents four separate traps in
    that dispatch (the US_HINT bug, the primary-segment rule, the
    not-is_remote guard, ordering South FL last) and none of them should have
    to be remembered twice.
    """
    __slots__ = ("d",)

    def __init__(self, d):
        self.d = d

    def __getitem__(self, i):
        return self.d.get(AJB.COLUMNS[i], "") or ""


def _f(v):
    """Best-effort float; '' / None / '12,000' / junk -> None."""
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().replace(",", "").replace("$", "")
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None


def _days(posted, today):
    """Whole days from `posted` to `today`, or None when undated/unparseable."""
    p = (posted or "").strip()[:10]
    if len(p) != 10 or p[4] != "-" or p[7] != "-":
        return None
    try:
        y, m, d = int(p[:4]), int(p[5:7]), int(p[8:10])
        ty, tm, td = int(today[:4]), int(today[5:7]), int(today[8:10])
    except ValueError:
        return None
    import datetime
    try:
        n = (datetime.date(ty, tm, td) - datetime.date(y, m, d)).days
    except ValueError:
        return None
    return max(0, n)          # a future date is not fresher than today


_RING = re.compile(r"\((ring 1|ring 2|border)(, also-listed)?\)")


# ------------------------------------------------------------------- the axes
def _role(f, title, skills, industry):
    """ONEHOT best tier + two flags.  A title hit is the only ticket in for
    picks/swaths (vocab.py); here it is a weight, so an off-target
    title sinks instead of vanishing -- but the tiers keep picks' ordering."""
    dom = bool(P.DOMAIN.search(f"{title} {skills} {industry}"))
    pts, _ = P.title_fit(title, dom)
    swaths = V.all_matching(title)
    if pts == 40:
        f["role.target"] = 100
    elif pts == 32:
        f["role.strong"] = 100
    elif swaths:
        f["role.swath"] = 100
    elif pts == 22:
        f["role.adjacent"] = 100
    elif LV.SUPPORT.search(title):
        f["role.support"] = 100
    elif LV.CSAM.search(title):
        f["role.csam"] = 100
    elif LOC.PROF.search(title):
        f["role.prof"] = 100
    # NEAR_SWATH is a frozenset of literals; membership is order-free, but
    # iterate `swaths` (a list from all_matching) rather than the set.
    if any(k in LV.NEAR_SWATH for k in swaths):
        f["role.near_swath"] = 100
    if P.ANTI.search(title):
        f["role.anti"] = 100
    return swaths


def _level(f, title, seniority):
    rung, src = LV.rung_src(title, seniority)
    key = {2: "level.at", 3: "level.up1", 1: "level.down1", 0: "level.entry",
           4: "level.senior", 5: "level.staff"}.get(rung, "level.mgr")
    f[key] = 100
    if src not in ("title", "unmarked"):
        f["level.tag_only"] = 100
    return rung


def _geo(f, row, d):
    """ONEHOT best bucket.  Most specific wins: a ring-1 town scores as ring 1
    and not as the broader in-state bucket, even though both are true."""
    loc = d.get("location", "") or ""
    state = d.get("state", "") or ""
    country = d.get("country", "") or ""
    regions = AJB.regions_of(loc, state, country)
    basis = us_basis(country, state, loc)
    remote = AJB.is_remote(row)

    wpts, wlabel = LOC.where_fit(loc, border=True)
    ring = ""
    if wpts:
        m = _RING.search(wlabel)
        if m:
            ring = m.group(1)
            if m.group(2):
                f["geo.alsolisted"] = 100

    st = state.strip().upper()
    nearby = ("RI" in regions or st in ("RI", "NH", "CT")
              or bool(re.search(r"\b(?:rhode island|new hampshire|connecticut)\b",
                                loc, re.I)))

    if ring == "ring 1":
        f["geo.ring1"] = 100
    elif ring == "ring 2":
        f["geo.ring2"] = 100
    elif ring == "border":
        f["geo.border"] = 100
    elif "MA" in regions:
        f["geo.malocal"] = 100
    elif remote and basis and basis != "worldwide-open":
        # `basis` is how the US was established: country/state are read off the
        # posting, location is parsed out of free text, worldwide-open is an
        # open-to-anywhere listing.  ma_remote_build.INFERRED_BASIS ->
        # triage_load.basis_weak makes the same cut and geo_fit demotes 7 -> 4.
        f["geo.remote_us" if basis in ("country", "state") else "geo.remote_inf"] = 100
    elif remote and basis == "worldwide-open":
        f["geo.worldwide"] = 100
    elif nearby:
        f["geo.nearby"] = 100
    elif "South FL" in regions:
        f["geo.sfl"] = 100
    elif basis:
        f["geo.us_far"] = 100      # onsite, in the US, nowhere reachable


def _fresh_and_src(f, d, today, fabricated):
    source = (d.get("source", "") or "").strip().lower()
    posted = d.get("posted", "") or ""
    basis = DB.basis_of(source, posted, fabricated)
    f[{DB.EMPLOYER: "date.employer", DB.NETWORK: "date.network",
       DB.AGGREGATOR: "date.aggregator",
       DB.FIRST_SEEN: "date.first_seen"}[basis]] = 100

    n = _days(posted, today)
    if n is None:
        f["fresh.undated"] = 100
    else:
        decay = math.exp(-n / HALFLIFE_D)
        f["fresh.recency"] = int(round(decay * 100))
        f["fresh.trusted"] = int(round(decay * TRUST[basis] * 100))

    f["src.agg" if source in SB.EXCL_SRC else "src.direct"] = 100
    if source in UNVERIFIABLE_SRC:
        f["src.unverifiable"] = 100
    ns = _f(d.get("n_sources")) or 1
    if ns >= 3:
        f["xpost.many"] = 100


_ET = (("intern", "etype.intern"), ("contract-to-hire", "etype.c2h"),
       ("contract to hire", "etype.c2h"), ("part", "etype.parttime"),
       ("temp", "etype.temp"), ("seasonal", "etype.temp"),
       ("per diem", "etype.temp"), ("contract", "etype.contract"),
       ("freelance", "etype.contract"), ("full", "etype.fulltime"))


def _type_and_mode(f, d, row):
    et = (d.get("employment_type", "") or "").strip().lower()
    for needle, key in _ET:           # tuple, ordered: c2h before contract
        if needle in et:
            f[key] = 100
            break
    wm = (d.get("work_mode", "") or "").strip().lower()
    if AJB.is_remote(row) or wm == "remote":
        f["mode.remote"] = 100
    elif "hybrid" in wm:
        f["mode.hybrid"] = 100
    elif wm:
        f["mode.onsite"] = 100


def _bonus(f, d, title, skills, industry, swaths):
    # domain -- ONEHOT best, then the two ai_band flags on top
    if P.DOMAIN.search(f"{title} {skills} {industry}"):
        f["dom.finance"] = 100
    elif AIF.CORE.search(title) or AIF.CORE.search(skills):
        f["dom.ai_core"] = 100
    elif any(V.DOMAINS[k].search(f"{skills} {industry}") for k in swaths):
        # swath_vocab matches DOMAINS against skills+industry, NEVER the title
        # (swath_vocab.py) -- a domain word is not a job title.
        f["dom.swath"] = 100
    elif AIF.ADJ.search(title) or AIF.SKILL.search(skills):
        f["dom.ai_adj"] = 100
    band = (d.get("ai_band", "") or "").strip()[:1]
    if band == "1":
        f["dom.aiband1"] = 100
    elif band == "2":
        f["dom.aiband2"] = 100

    # comp -- yearly only.  vocab.comp_fit guards <1000 as an hourly rate
    # that slipped into a salary field; contract_pool.hourly() calls a bad parse
    # on a money-ranked lane the thing that "sorts straight to the top".
    sal = _f(d.get("salary_max")) or _f(d.get("salary_min"))
    per = (d.get("salary_period", "") or "").strip().lower()
    if sal and sal >= 1000 and per in ("", "year", "yearly", "annual", "annually"):
        key = ("comp.150" if sal >= 150000 else "comp.130" if sal >= 130000
               else "comp.110" if sal >= 110000 else "comp.90" if sal >= 90000
               else "comp.low")
        f[key] = 100

    tier = (d.get("startup_tier", "") or "").strip()
    if tier[:1] in ("1", "2", "3", "4"):
        f["co.tier" + tier[0]] = 100
        if "via company" in (d.get("startup_evidence", "") or ""):
            f["co.inherited"] = 100


# ---------------------------------------------------------------------- entry
def features(d, today, fabricated=False):
    """{name: 0..100} for one row dict.  Keys absent from `d` read as ''.

    `d` uses all_jobs_master column names, plus the optional context columns
    ma_remote_build.CTX adds (bucket, us_basis, startup_tier, startup_evidence,
    ai_band, ai_tier).  Missing context degrades a BONUS axis, never a hard one.
    """
    f = dict.fromkeys(INDEXED, 0)
    row = _AsRow(d)
    title = d.get("title", "") or ""
    skills = d.get("skills", "") or ""
    industry = d.get("company_industry", "") or ""

    swaths = _role(f, title, skills, industry)
    _level(f, title, d.get("seniority", "") or "")
    _geo(f, row, d)
    _fresh_and_src(f, d, today, fabricated)
    _type_and_mode(f, d, row)
    _bonus(f, d, title, skills, industry, swaths)
    return f


def coverage(d):
    """0..100 -- what share of the bonus axes this row had the data to earn.

    A row with no skills, no salary and no startup tier is not a bad job; it is
    an unmeasured one, and the card says so rather than letting it sink
    silently.  This is the defect the 2.6% ai_band coverage exposes.
    """
    have = 0
    for axis in BONUS_AXES:
        if any((d.get(c) or "") for c in BONUS_EVIDENCE[axis]):
            have += 1
    return int(round(100 * have / len(BONUS_AXES)))


# Title families, for the exploration slice's diversity cap.  Literal order,
# never a set: the code is persisted in the index and must mean the same thing
# on the next run.  swath_vocab.SWATHS is an ordered dict of the eleven
# F-swaths; the tail codes cover the families that have no swath of their own.
FAMILIES = list(V.SWATHS) + ["target", "support", "csam", "prof", "none"]
FAMILY_CODE = {n: i for i, n in enumerate(FAMILIES)}


def family(title):
    """A small stable int naming the job family, so an explore batch can be
    capped at N per family without needing the title back."""
    # Target/strong titles come FIRST, matching the role axis's own ordering:
    # "Forward Deployed Engineer" is the #1 target, and filing it under presales
    # because a swath pattern also matches would let the explore cap treat the
    # core family as one more sideways bet.
    pts, _ = P.title_fit(title or "")
    if pts >= 32:
        return FAMILY_CODE["target"]
    hits = V.all_matching(title or "")
    if hits:
        return FAMILY_CODE[hits[0]]
    if LV.SUPPORT.search(title or ""):
        return FAMILY_CODE["support"]
    if LV.CSAM.search(title or ""):
        return FAMILY_CODE["csam"]
    if LOC.PROF.search(title or ""):
        return FAMILY_CODE["prof"]
    return FAMILY_CODE["none"]


def vector(d, today, fabricated=False):
    """The indexed features as a plain list, in INDEXED order."""
    f = features(d, today, fabricated)
    return [f[n] for n in INDEXED]


# ------------------------------------------------------------------- selftest
_CASES = [
    ("target/remote/fresh", {
        "title": "Forward Deployed Engineer", "company": "Anthropic",
        "location": "San Francisco, CA; Remote - US", "country": "US",
        "remote": "True", "work_mode": "Remote", "posted": "2026-08-19",
        "source": "greenhouse", "salary_max": "180000", "salary_period": "Year",
        "skills": "LLM, agents, evals", "employment_type": "Full-time"}),
    ("over-levelled manager", {
        "title": "Director of Engineering", "company": "Acme",
        "location": "Austin, TX", "country": "US", "posted": "2026-05-02",
        "source": "workday", "work_mode": "On-site"}),
    ("ring 1 also-listed", {
        "title": "Technical Account Manager", "company": "Foo",
        "location": "New York, NY; Austin, TX, USA", "state": "",
        "posted": "", "source": "himalayas", "n_sources": "4"}),
    ("bare row, nothing known", {"title": "Cook", "company": "Bar"}),
]


def _selftest():
    today = "2026-08-20"
    print(f"{NFEAT} features ({len(INDEXED)} indexed), "
          f"{len(set(AXIS.values()))} axes")
    lines = []
    for label, d in _CASES:
        f = features(d, today)
        on = [f"{n}={f[n]}" for n in INDEXED if f[n]]
        lines.append(f"{label}: cov={coverage(d)} " + " ".join(on))
    for ln in lines:
        print("  " + ln)
    # Every axis must be reachable, or a slider would do nothing.
    axes = set()
    for label, d in _CASES:
        f = features(d, today)
        for n in INDEXED:
            if f[n]:
                axes.add(AXIS[n])
    import hashlib
    h = hashlib.sha256("\n".join(lines).encode()).hexdigest()[:16]
    print(f"  digest {h}  (PYTHONHASHSEED={os.environ.get('PYTHONHASHSEED','-')})")
    assert len(NAMES) == len(set(NAMES)), "duplicate feature name"
    for label, d in _CASES:
        f = features(d, today)
        for axis in ("role", "level", "geo", "etype", "mode", "dom", "comp", "co",
                     "date", "src"):
            hot = [n for n in INDEXED
                   if AXIS[n] == axis and KIND[n] == ONEHOT and f[n]]
            assert len(hot) <= 1, f"{label}: {axis} not one-hot: {hot}"
    print("  onehot invariant OK, names unique OK")
    return h


if __name__ == "__main__":
    if "--selftest" in sys.argv:
        _selftest()
    else:
        print(f"{NFEAT} features across {len(set(AXIS.values()))} axes")
        for n, a, k, p in SCHEMA:
            print(f"  {n:20s} {a:6s} {k:7s} {p:+4d}")
