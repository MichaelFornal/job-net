#!/usr/bin/env python3
"""
contract_pool.py — the ranked contract-gig pool that feeds the triage app's
contract lane.

    python3 contract_pool.py [--top 1000] [--out-date YYYY-MM-DD]

Reads contract_norm/*.jsonl directly rather than contract_master.csv, because
the master is a filename-filtered artifact and filenames stopped carrying the
meaning it depends on (see WHY THE FILTER IS TYPE-BASED below).  Writes:

    contract_pool.csv            the full ranked pool, all score components
    Contract_Pool_<date>.csv     the top N, in triage_load.py's column shape

Ranking is the blend the user chose: fit 60 / speed 25 / rate 15, summed to a
0-100 `score` with a plain-English `score_why` so any rank can be audited.

WHY THE FILTER IS TYPE-BASED
----------------------------
contract_build.py decides what is contingent by FILENAME: it skips any file
whose basename contains "_all".  That was right on 2026-07-21 and wrong from
2026-07-29, when ~25 new pipes began writing full-employment-type dumps into
the same directory under names carrying no "_all" suffix (workday, careerjet,
oracle, icims, jobgether, themuse, intlmisc, intlboards, adzuna, adp, ...).
The rule now fails in both directions at once: it admits ~700k Full-time rows,
and it throws away the KEEP_ALL dumps that hold the freshest contract data.

So a row qualifies here only when cc.classify(employment_type, title) returns a
contingent label.  Filenames carry no meaning, which is what stops the next new
pipe from silently re-introducing the same bug.

Generation selection is a SEPARATE concern from type: one generation per source
(*.prev.jsonl skipped; the Jul-19 contract-only twin dropped where an `_all`
dump exists).  Without it the pool resurrects Jul-19 postings long since filled.

Geography and the professional/clinical/retail split are imported from
contract_ma_fasthire, not re-typed, so "commutable" and "professional work"
have one definition across both pipelines.
"""
import argparse
import csv
import hashlib
import json
import os
import re
import sys
from collections import Counter
from datetime import date, datetime, timezone

import contract_common as cc
import contract_ma_fasthire as fh
from ai_jobs_filter import ADJ, CORE
from triage.lanes import AGGREGATORS

BASE = os.path.dirname(os.path.abspath(__file__))
SNAP = datetime.now(timezone.utc).strftime("%Y-%m-%d")
OUT_POOL = os.path.join(BASE, "contract_pool.csv")

# ---------------------------------------------------------------- geography
# contract_ma_fasthire.geo_of() treats a BLANK country as US (is_us: "" -> True)
# and calls anything remote "Remote (US)".  That held on contract_master.csv,
# which was dominated by sources that populate `country`.  It does not hold on
# this pool: 71% of rows arrive with no country at all, and the boards that
# leave it blank are the international ones -- himalayas rows reading "Spain",
# "Serbia", "Brazil"; remoteleaf rows reading "Japan, New Zealand, Singapore";
# nofluffjobs, which is a Polish board whose every listing says just "Remote".
# Absence of a country is not evidence of the United States.
#
# So US-remote has to be asserted from POSITIVE evidence.  `geo_basis` records
# which rule admitted a row, so a wrong one can be found and the rule fixed
# rather than guessed at.
# Two regexes, not one, because the evidence is not equally strong.
#
# US_NAME_RE is unambiguous: "United States", "Massachusetts".  A bare
# two-letter code is NOT, and folding it in here admitted Europe wholesale:
# case-insensitively, `in` matched "Remote in Germany", `or` matched "Or
# Yehuda, Israel", and DE/IN/IT/FR are Delaware/Indiana/... exactly as much as
# they are Germany/India/Italy/France.  So codes are tested separately, are
# case-SENSITIVE, must sit in the canonical ", ST" position, and can never
# outrank a spelled-out foreign country.
US_NAME_RE = re.compile(r"""(?ix)
    \b(?:usa|u\.s\.a?\.?|united\s+states|stateside|conus)\b
  | \b(?:alabama|alaska|arizona|arkansas|california|colorado|connecticut|
        delaware|florida|georgia|hawaii|idaho|illinois|indiana|iowa|kansas|
        kentucky|louisiana|maine|maryland|massachusetts|michigan|minnesota|
        mississippi|missouri|montana|nebraska|nevada|new\s+hampshire|
        new\s+jersey|new\s+mexico|new\s+york|north\s+carolina|north\s+dakota|
        ohio|oklahoma|oregon|pennsylvania|rhode\s+island|south\s+carolina|
        south\s+dakota|tennessee|texas|utah|vermont|virginia|washington|
        west\s+virginia|wisconsin|wyoming)\b
""")
STATE_ABBR = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC",
}
# Case-sensitive and comma-anchored: "Boston, MA" yes, "Remote in Germany" no.
US_ABBR_RE = re.compile(r",\s*(" + "|".join(sorted(STATE_ABBR)) + r")\b")
# Deliberately not exhaustive — it only has to cover what these boards actually
# emit.  A country we miss falls through to the no-signal rule below, which is
# already conservative.
NON_US_RE = re.compile(r"""(?ix)
    \b(?:canada|toronto|vancouver|montreal|ontario|mexico|brazil|brasil|
        argentina|colombia|chile|peru|uruguay|costa\s+rica|latam|
        united\s+kingdom|uk|england|scotland|wales|ireland|london|manchester|
        germany|deutschland|berlin|munich|m[uü]nchen|hamburg|france|paris|
        spain|espa[nñ]a|madrid|barcelona|portugal|lisbon|porto|italy|italia|
        rome|milan|netherlands|holland|amsterdam|utrecht|belgium|brussels|
        switzerland|zurich|austria|vienna|poland|polska|warsaw|warszawa|
        krak[oó]w|wroc[lł]aw|gda[nń]sk|pozna[nń]|katowice|[lł][oó]d[zź]|
        czech|prague|slovakia|hungary|budapest|romania|bucharest|bulgaria|
        sofia|serbia|belgrade|croatia|slovenia|greece|athens|turkey|istanbul|
        ukraine|kyiv|kiev|lviv|belarus|russia|moscow|latvia|riga|lithuania|
        vilnius|estonia|tallinn|finland|helsinki|sweden|stockholm|norway|
        oslo|denmark|copenhagen|iceland|bosnia|herzegovina|macedonia|albania|
        montenegro|moldova|georgia\s+\(country\)|armenia|azerbaijan|
        india|bengaluru|bangalore|mumbai|delhi|hyderabad|pune|chennai|noida|
        gurgaon|gurugram|pakistan|bangladesh|sri\s+lanka|nepal|
        china|beijing|shanghai|shenzhen|hong\s+kong|taiwan|japan|tokyo|osaka|
        south\s+korea|seoul|singapore|malaysia|kuala\s+lumpur|indonesia|
        jakarta|thailand|bangkok|vietnam|hanoi|philippines|manila|cebu|
        australia|sydney|melbourne|brisbane|perth|new\s+zealand|auckland|
        south\s+africa|johannesburg|cape\s+town|nigeria|lagos|kenya|nairobi|
        egypt|cairo|morocco|ghana|tanzania|uganda|ethiopia|
        uae|dubai|abu\s+dhabi|saudi|riyadh|qatar|doha|israel|tel\s+aviv|
        emea|apac|europe|european\s+union|\beu\b|sp\.\s*z\s*o\.?\s*o\.?|
        gmbh|s\.?a\.?r\.?l|\bbv\b|\bsrl\b|\bltd\b\s*\(uk\))\b
""")
WORLDWIDE_RE = re.compile(
    r"(?i)\b(?:worldwide|world\s*wide|anywhere|global(?:ly)?|"
    r"remote\s*[-,]?\s*(?:global|worldwide|anywhere))\b")

# Boards whose postings are US-first, where a blank country has always meant
# the United States in this corpus.  Everything else must say so out loud.
US_DIRECT_SRC = {
    "lever", "getro", "greenhouse", "ashby", "bullhorn", "phenom", "workday",
    "icims", "oracle", "adp", "paycom", "jobvite", "usajobs", "smartrecruiters",
    "bamboohr", "jazzhr", "breezy", "recruitee", "teamtailor", "workable",
    "comeet", "gem", "rippling", "personio", "yc", "dover",
    # 2026-08-20, admitted on MEASUREMENT not on vendor reputation:
    #   neogov  24,998 rows, 100% United States, ZERO blank-country
    #   ukg      8,251 rows, 94% US of stated, ZERO blank-country
    # Both have no blanks today, so this changes nothing now and is correct
    # the first time one appears.
    "neogov", "ukg",
}
# NOT radancy, and the numbers are why.  Its stated countries are 99% US, which
# is exactly the shape that invites a wrong entry here -- but 23,290 of its
# rows have a BLANK country, and those are not US: 10,847 carry a foreign
# region (Hesse, Santa Catarina, Nuevo Leon) and the city-only ones include
# Hyderabad, Pathum Thani, Berlin and Netherlands.  This set is about what a
# BLANK means, not about where the majority of the board is.
# NOT manatal (29% US), NOT cornerstone (19% US, NL-heavy), NOT dayforce
# (72% US of stated, and only 465 blanks, so it gains nothing).
# NOT "atstail".  That was dead config — atstail.jsonl is a shared FILE, and
# every row in it carries its own family in `source` ("zohorecruit", "keka",
# "taleo", "join", "wttj", "pinpoint"), so the feed name never matched a row
# and the entry did nothing.  Wiring it up would be wrong, not a fix: keka is
# Indian, join German, wttj French, and zohorecruit is mostly India/EU/South
# Africa staffing agencies.  A blank country on any of those is not the US.


def us_geo(r):
    """(bucket, basis) for a row, or (None, reason) if it is out of scope.

    Precedence matters: a spelled-out foreign country beats a state
    ABBREVIATION but not a spelled-out US name.  "Pune, IN" is India and
    "Hamburg, DE Germany" is Germany, while "Remote, IN" with nothing else is
    Indiana -- so only US_NAME_RE may override a non-US marker.
    """
    prim = (r.get("location") or "").split(";")[0].strip()
    blob = f"{prim} {r.get('city') or ''} {r.get('state') or ''} " \
           f"{r.get('country') or ''}"
    st = (r.get("state") or "").strip().lower()
    remote = fh.is_remote(r)

    m_name = US_NAME_RE.search(blob)
    m_abbr = US_ABBR_RE.search(blob)
    state_abbr = (r.get("state") or "").strip()
    us_name = m_name.group(0) if m_name else ""
    us_abbr = (m_abbr.group(1) if m_abbr else
               state_abbr if state_abbr.upper() in STATE_ABBR else "")
    non_us_hit = NON_US_RE.search(blob)
    if non_us_hit and not us_name:
        return None, "non-US: " + non_us_hit.group(0)
    us_hit = us_name or us_abbr

    # MA and the commutable ring keep contract_ma_fasthire's own definitions.
    if st in ("ma", "massachusetts") or fh.MA_RE.search(blob):
        return "MA", "MA"
    if (st in ("ri", "rhode island", "nh", "new hampshire", "ct", "connecticut")
            or fh.RI_RE.search(blob) or fh.NH_RE.search(blob)
            or fh.CT_RE.search(blob)):
        return "Nearby (RI/NH/CT)", "nearby state"
    if not remote:
        return None, "on-site outside MA/nearby"
    if us_hit:
        return "Remote (US)", "US named: " + us_hit
    if WORLDWIDE_RE.search(blob):
        # Genuinely US-eligible: these accept contractors anywhere.
        return "Remote (US)", "worldwide"
    if (r.get("source") or "") in US_DIRECT_SRC:
        return "Remote (US)", "US-first board"
    return None, "remote but no US evidence"


# --------------------------------------------------------------------- fit
# Contract rows carry no `skills` field and no job description, so fit is title
# (+ company) only.  That is the same constraint triage already runs under --
# the corpus has no JD either -- not a gap this script can close.
ENG_RE = re.compile(r"""(?ix)
    \b(?:software|platform|backend|back[\s-]end|frontend|front[\s-]end|
        full[\s-]?stack|devops|sre|site\s+reliability|cloud|infrastructure|
        data|analytics|database|systems?|security|network|qa|automation|
        integration|api|mobile|ios|android|web)\b
    [\s\w/,&-]{0,24}
    \b(?:engineer|engineering|developer|architect|programmer|scientist|dev)\b
  | \b(?:engineer|developer|architect|programmer)\b [\s\w/,&-]{0,20}
    \b(?:software|platform|cloud|data|systems?|web|api)\b
  | \b(?:swe|sdet|dba|etl\s+developer|salesforce\s+developer|
        java|python|golang|\.net|c\#|node|react|angular)\b
        [\s\w/,&-]{0,20}\b(?:engineer|developer|architect)\b
""")
ADJ_TECH_RE = re.compile(r"""(?ix)
    \b(?:product\s+manager|product\s+owner|program\s+manager|project\s+manager|
        technical\s+program|scrum\s+master|business\s+analyst|systems?\s+analyst|
        data\s+analyst|solutions?\s+(?:architect|consultant|engineer)|
        technical\s+writer|ux|ui|product\s+design|technical\s+lead|
        it\s+(?:analyst|specialist|support|manager)|salesforce|
        implementation\s+(?:consultant|specialist))\b
""")

FIT_TIERS = [
    (60, "Core AI building", CORE),
    (45, "AI/ML adjacent", ADJ),
    (32, "Software/platform engineering", ENG_RE),
    (18, "Adjacent tech", ADJ_TECH_RE),
]
# Floor for the triage slice.  The blend the user chose ranks correctly but
# says nothing about composition, and the pool is 85% "Other professional":
# only 1,594 of 10,891 rows reach Adjacent tech or better.  Without a floor the
# bottom 400 of a 1000-row queue fills with maximally-fast, well-paid work that
# is simply not this user's -- trial attorneys, HR interns, per-diem organ
# placement coordinators.  The floor picks WHO is eligible; the blend still
# decides the order among them.
DEFAULT_MIN_FIT = 18


def fit_score(title):
    """(points, label, matched-text) — the 60-point axis."""
    t = title or ""
    for pts, label, rx in FIT_TIERS:
        m = rx.search(t)
        if m:
            return pts, label, re.sub(r"\s+", " ", m.group(0)).strip()
    return 6, "Other professional", ""


# ------------------------------------------------------------------- speed
# Rescaled from contract_ma_fasthire's structural+urgency+recency (which runs
# 0-100 and maxes around 47 in practice) onto this axis's 25 points.
FH_SPEED_MAX = 50.0


def speed_score(r, today):
    """(points 0-25, reasons) — how structurally fast this gig tends to fill."""
    reasons = []
    raw = 0
    if r.get("source_type") in fh.AGENCY_SRC:
        raw += 25
        agency = (r.get("company") or r.get("board") or r.get("source") or "")
        reasons.append(f"staffing agency ({agency})" if agency.strip()
                       else "staffing-agency posting")
    et = (r.get("employment_type") or "").strip()
    raw += fh.TYPE_SPEED.get(et, 8)

    n_src = r.get("n_sources") or 1
    if n_src > 1:
        raw += min(n_src - 1, 3) * 2
        reasons.append(f"cross-posted x{n_src}")

    title = r.get("title") or ""
    if fh.URGENT_RE.search(title):
        raw += 12
        reasons.append("urgency wording in title")
    if fh.DURATION_RE.search(title):
        raw += 5
        reasons.append("defined duration")

    p = fh.parse_posted(r.get("posted"))
    if p:
        days = max((today - p).days, 0)
        bump = 15 if days <= 3 else 12 if days <= 7 else 8 if days <= 14 \
            else 4 if days <= 30 else 0
        raw += bump
        if bump:
            reasons.append(f"posted {days}d ago")
    return min(raw / FH_SPEED_MAX, 1.0) * 25, reasons


# -------------------------------------------------------------------- rate
# Everything lands on an hourly equivalent so a $150k/yr contract and a $75/hr
# one are comparable.  Period strings vary by source, hence the sniffing.
HOURS_PER_YEAR = 2080
_PER = {"hour": 1, "hourly": 1, "hr": 1, "h": 1,
        "day": 8, "daily": 8, "week": 40, "weekly": 40,
        "month": 173, "monthly": 173,
        "year": HOURS_PER_YEAR, "yearly": HOURS_PER_YEAR,
        "annual": HOURS_PER_YEAR, "annually": HOURS_PER_YEAR}


# A plausible US contract hourly rate.  Anything outside this band means the
# row was parsed wrong, not that someone is paying $50,000 an hour.
RATE_MIN, RATE_MAX = 10.0, 1000.0


def _num(v):
    try:
        n = float(str(v or "").replace(",", "").replace("$", "").strip())
    except ValueError:
        return None
    return n if n > 0 else None


def hourly(r):
    """Best PLAUSIBLE hourly-equivalent rate, or None.

    Returns None rather than a number it cannot vouch for.  These feeds are
    full of mislabelled pay -- salary_period='Hour' on a 50,000 annual figure,
    a 2,000,000 max sitting next to a 20,000 min, a bare 15000 with no period
    at all -- and on a money-ranked lane a bad parse does not merely sit there
    being wrong, it sorts straight to the top.  A refused rate costs one row a
    few points; an invented one costs the whole ordering.
    """
    cur = (r.get("salary_currency") or "USD").strip().upper()
    if cur not in ("", "USD", "US$", "$"):
        return None                       # don't rank EUR against USD
    lo, hi = _num(r.get("salary_min")), _num(r.get("salary_max"))
    # A max wildly out of scale with its min is a typo or a lifetime-value
    # figure, not a ceiling: 20,000-2,000,000 "per year" is not a $962/hr gig.
    if lo and hi and hi > 20 * lo:
        hi = None
    per = (r.get("salary_period") or "").strip().lower()
    div = _PER.get(per)
    for v in (hi, lo):
        if not v:
            continue
        if div is not None:
            h = v / div
        elif v >= 20000:
            h = v / HOURS_PER_YEAR        # plainly annual
        elif v < RATE_MAX:
            h = v                         # plainly already hourly
        else:
            continue    # 400-20,000 with no period: day? week? month? project?
        if RATE_MIN <= h <= RATE_MAX:
            return h
    return None


def rate_score(r):
    """(points 0-15, reason) — 0 when the source publishes no pay.

    ~70% of the pool publishes nothing.  At a 15% weight those rows still rank
    on fit+speed, and the zero is uniform so it never reorders them against
    each other.
    """
    h = hourly(r)
    if not h:
        return 0.0, ""
    for floor, pts in ((150, 15), (110, 13), (85, 11), (65, 8), (45, 5),
                       (28, 2)):
        if h >= floor:
            return float(pts), f"~${h:,.0f}/hr"
    return 1.0, f"~${h:,.0f}/hr"


# ------------------------------------------------------------- fast money
# A different question from the blend: not "is this my kind of work" but "how
# fast does this turn into cash".  Fit is deliberately absent -- a fit floor
# here would just rebuild the contract lane.  What stays is the pool's field
# filter, so this is fast professional/office/tech work, not fast anything:
# a per-diem clinical shift is not fast money for someone without the licence.
MONEY_TYPE_SPEED = {          # how rolling the hiring is for this type
    "Per Diem": 18, "Temp": 17, "Contract-to-Hire": 15, "Seasonal": 14,
    "Interim": 13, "Freelance": 12, "Fractional": 10, "Contract": 8,
}


def money_score(r, today):
    """(points 0-100, reasons) — quick-hire first, pay second.

    Split 60 speed / 40 pay.  Pay is second because an unpublished rate is
    common and a slow gig is disqualifying: a $200/hr contract you start in
    two months is not fast money.
    """
    reasons = []
    speed = 0
    if r.get("source_type") in fh.AGENCY_SRC:
        speed += 18
        agency = (r.get("company") or r.get("board") or r.get("source") or "")
        reasons.append(f"staffing agency ({agency.strip()})" if agency.strip()
                       else "staffing-agency posting")
    et = (r.get("employment_type") or "").strip()
    speed += MONEY_TYPE_SPEED.get(et, 6)
    if et:
        reasons.append(et)

    title = r.get("title") or ""
    if fh.URGENT_RE.search(title):
        speed += 12
        reasons.append("urgent/immediate wording")
    if fh.DURATION_RE.search(title):
        speed += 6
        reasons.append("defined duration")
    if (r.get("n_sources") or 1) > 1:
        speed += 3
        reasons.append("cross-posted")

    p = fh.parse_posted(r.get("posted"))
    if p:
        days = max((today - p).days, 0)
        bump = 12 if days <= 3 else 9 if days <= 7 else 5 if days <= 14 \
            else 2 if days <= 30 else 0
        speed += bump
        if bump:
            reasons.append(f"posted {days}d ago")
    speed = min(speed, 60)

    h = hourly(r)
    pay = 0.0
    if h:
        for floor, pts in ((150, 40), (110, 35), (85, 30), (65, 24), (45, 16),
                           (28, 9)):
            if h >= floor:
                pay = float(pts)
                break
        else:
            pay = 4.0
        reasons.insert(0, f"~${h:,.0f}/hr")
    return round(speed + pay, 1), round(speed, 1), pay, " · ".join(reasons)


# Eligibility for the fast-money lane.  Either the pay is known, or the hiring
# is structurally quick — a staffing desk with an urgent rolling req is fast
# money even when the rate is only quoted on the phone.
MONEY_MIN_SPEED = 22


def money_eligible(r):
    return r["money_rate"] > 0 or r["money_speed"] >= MONEY_MIN_SPEED


def score_row(r, today):
    fit, fit_label, matched = fit_score(r.get("title"))
    speed, speed_reasons = speed_score(r, today)
    rate, rate_reason = rate_score(r)
    total = round(fit + speed + rate, 1)

    why = [fit_label + (f" ('{matched}')" if matched else "")]
    if rate_reason:
        why.append(rate_reason)
    why += speed_reasons

    m_total, m_speed, m_pay, m_why = money_score(r, today)
    return {"score": total, "fit_score": round(fit, 1),
            "speed_score": round(speed, 1), "rate_score": round(rate, 1),
            "fit_tier": fit_label, "score_why": " · ".join(why),
            "money_score": m_total, "money_speed": m_speed,
            "money_rate": m_pay, "money_why": m_why}


# ------------------------------------------------------------------- gather
def merge(a, b):
    """contract_build.merge, minus its _sources/_boards set bookkeeping."""
    a["_sources"] |= b["_sources"]
    if (b.get("posted") or "") > (a.get("posted") or ""):
        a["posted"] = b["posted"]
    for k in ("apply_url", "location", "city", "state", "country",
              "salary_min", "salary_max", "salary_currency", "salary_period",
              "work_mode", "company", "board", "board_url"):
        if not a.get(k) and b.get(k):
            a[k] = b[k]


def gather(paths, stats):
    """Contingent rows from every source, deduped exactly as the master is."""
    by_id = {}
    for path in paths:
        src = os.path.basename(path)[:-len(".jsonl")]
        kept = 0
        with open(path, encoding="utf-8") as f:
            for line in f:
                stats["raw"] += 1
                try:
                    r = json.loads(line)
                except json.JSONDecodeError:
                    continue
                title = (r.get("title") or "").strip()
                if not title:
                    continue
                # The whole filter, in one line: an explicit contingent type,
                # or a title that says so.  Not the filename.
                label = cc.classify(r.get("employment_type"), title)
                if not label:
                    continue
                r["employment_type"] = label
                r["_sources"] = {r.get("source") or src}
                kept += 1
                jid = r.get("job_id")
                k = (r.get("source"), str(jid)) if jid else None
                if k and k in by_id:
                    merge(by_id[k], r)
                elif k:
                    by_id[k] = r
                else:
                    by_id[id(r)] = r
        stats["by_file"][src] = kept

    by_key = {}
    for r in by_id.values():
        comp = (r.get("company") or "").strip().lower()
        title = (r.get("title") or "").strip().lower()
        au = (r.get("apply_url") or "").strip().lower()
        k = (comp, title, au or "loc:" + (r.get("location") or "").lower())
        if k in by_key:
            merge(by_key[k], r)
        else:
            by_key[k] = r
    rows = list(by_key.values())
    for r in rows:
        r["n_sources"] = len({s for s in r["_sources"] if s})
        r["sources"] = ", ".join(sorted(s for s in r["_sources"] if s))
        r.pop("_sources", None)
    return rows


# --------------------------------------------------------------- reachable
# A row you cannot open is not a job you can take, however well it scores.
# Bullhorn postings are agency-portal only and carry no public link, so the
# pipeline synthesises one from the board name: "https://Bullhorn agency
# 1VCNF4".  Fourteen rows pool-wide, but they are staffing-desk contracts with
# high published rates, so seven of them landed in the fast-money slice and one
# was its second card.  A queue whose #2 card does nothing when you press `o`
# stops getting used.  They stay in contract_pool.csv as a record; they just do
# not go in front of you.
LINK_RE = re.compile(r"^https?://[^\s/]+\.[^\s/]+", re.I)


def usable_link(r):
    return bool(LINK_RE.match((r.get("apply_url") or "").strip()
                              or (r.get("board_url") or "").strip()))


# ------------------------------------------------------- collapse and spread
# The master's cross-source key is (company, title, apply_url).  On an
# aggregator-heavy pool that key barely binds: aggregator apply URLs are
# per-listing self-redirects, so one gig posted eleven ways stays eleven rows
# (see the aggregator-redirect-dedupe note).  Measured on the first build, 231
# of the top 1000 rows were exact company+title repeats and a single
# AI-data-labelling farm held 164 of them.
#
# So collapse on (company, title) alone, ignoring URL.  Ties go to the
# employer-direct copy: same gig, better link than an aggregator redirect.
_CO_SUFFIX = re.compile(
    r"(?i)[\s,]*(?:\.(?:ai|io|com|co|net|dev)|,?\s*(?:inc|llc|ltd|corp|"
    r"corporation|limited|gmbh|group|technologies|technology|solutions|"
    r"consulting|staffing|systems)\.?)\s*$")


def co_key(company):
    """Loose company identity, for collapsing and for the per-company cap.

    Trims one legal/TLD suffix and cuts at a colon or dash tagline, so
    'Mindrift', 'Mindrift.ai' and 'Mindrift.ai: Be the "I" in AI' count as one
    employer.  Display always uses the original string; this is only a key.
    """
    c = (company or "").strip().lower()
    c = re.split(r"\s*[:|]\s*|\s+[-–—]\s+", c)[0].strip()
    prev = None
    while c != prev:                      # 'Foo Technologies Inc.' -> 'foo'
        prev = c
        c = _CO_SUFFIX.sub("", c).strip()
    return re.sub(r"[^a-z0-9]+", " ", c).strip()


def collapse(rows):
    """One row per (company, title); keeps the best, counts the rest."""
    best = {}
    for r in rows:
        k = (co_key(r.get("company")),
             re.sub(r"\s+", " ", (r.get("title") or "").strip().lower()))
        cur = best.get(k)
        if cur is None:
            r["n_listings"] = 1
            best[k] = r
            continue
        cur["n_listings"] += 1
        better = (r["score"], not r["is_agg"]) > (cur["score"], not cur["is_agg"])
        if better:
            r["n_listings"] = cur["n_listings"]
            best[k] = r
    return list(best.values())


def spread(rows, top, per_company):
    """Top `top` rows, at most `per_company` from any one employer.

    Without this the list is technically correct and practically useless: the
    highest-scoring employer is whichever one posts the most, and a queue you
    work top-down would spend its first hour on one company.  Overflow is not
    discarded — it stays in contract_pool.csv, just not in the triage slice.
    """
    out, held, seen = [], [], Counter()
    for r in rows:
        if len(out) >= top:
            break
        k = co_key(r.get("company")) or f"__blank{len(out)}"
        if seen[k] >= per_company:
            held.append(r)
            continue
        seen[k] += 1
        out.append(r)
    # If the cap starved the list (a small pool), backfill from what it held.
    for r in held:
        if len(out) >= top:
            break
        out.append(r)
    return out


# -------------------------------------------------------------------- output
POOL_COLS = (["score", "fit_score", "speed_score", "rate_score", "fit_tier",
              "score_why", "money_score", "money_speed", "money_rate",
              "money_why", "lane_tags", "geo", "geo_basis", "hourly_rate",
              "n_listings", "is_agg"]
             + cc.COLUMNS + ["sources", "n_sources"])

# triage_load.FIELDS + job_key.  Columns triage wants that contract rows have
# no analogue for (startup_tier, ai_tier, company_stage, ...) are written empty
# rather than faked -- an invented startup tier would rank a gig in a lane it
# was never judged for.
TRIAGE_COLS = [
    "job_key", "bucket", "us_basis", "startup_tier", "startup_evidence",
    "ai_band", "ai_tier", "company", "title", "employment_type", "seniority",
    "work_mode", "remote", "location", "posted", "salary_min", "salary_max",
    "salary_currency", "salary_period", "skills", "company_stage",
    "company_size", "company_industry", "apply_url", "job_url", "source",
    "score", "score_why", "money_score", "money_why", "lane_tags",
]

AI_BAND = {"Core AI building": "1 Core AI building",
           "AI/ML adjacent": "2 AI/ML adjacent"}


def triage_row(r):
    """One pool row in the shape triage_load.py reads.

    job_key is a MD5 of the identity, NOT Python's hash(): str hashing is
    salted per process, so a hash()-derived key would differ on every run and
    every decision you had made would be re-queued as a new job.

    The identity is url + company + title, not url alone: staffing boards post
    several distinct gigs behind one apply URL, and keying on the URL silently
    dropped 20 of the first 1,000 rows as key collisions.  A URL that changes
    between runs is not a problem here -- ct_key carry-forward covers it.
    """
    ident = "|".join((
        (r.get("apply_url") or "").strip().lower(),
        (r.get("company") or "").strip().lower(),
        (r.get("title") or "").strip().lower()))
    return {
        "job_key": "contract:" + hashlib.md5(
            ident.encode("utf-8", "replace")).hexdigest()[:16],
        # bucket drives the 'MA' pill in the card UI, so the pool's geo label
        # is mapped onto the corpus's vocabulary rather than a parallel one.
        "bucket": "MA local" if r["geo"] == "MA" else r["geo"],
        "us_basis": r["geo"],
        "startup_tier": "", "startup_evidence": "",
        "ai_band": AI_BAND.get(r["fit_tier"], ""),
        "ai_tier": r["fit_tier"],
        "company": r.get("company") or "", "title": r.get("title") or "",
        "employment_type": r.get("employment_type") or "",
        "seniority": "",
        "work_mode": r.get("work_mode") or "",
        "remote": r.get("remote") or "",
        "location": r.get("location") or "",
        "posted": (r.get("posted") or "")[:10],
        "salary_min": r.get("salary_min") or "",
        "salary_max": r.get("salary_max") or "",
        "salary_currency": r.get("salary_currency") or "",
        "salary_period": r.get("salary_period") or "",
        "skills": "", "company_stage": "",
        "company_size": "", "company_industry": r.get("source_type") or "",
        "apply_url": r.get("apply_url") or "", "job_url": r.get("board_url") or "",
        "source": r.get("source") or "",
        "score": r["score"], "score_why": r["score_why"],
        "money_score": r["money_score"], "money_why": r["money_why"],
        "lane_tags": r["lane_tags"],
    }


# -------------------------------------------------------------------- tests
# Geography is where this script has already been wrong twice, and both times
# the output looked entirely plausible: rows had a place name, a score and an
# apply link, they were just not in the United States.  Nothing about the row
# counts showed it.  These cases are the record of both mistakes.
GEO_CASES = [
    # the blank-country hole: absence of a country is not evidence of the US
    (dict(location="Spain", remote="True", source="himalayas"), None),
    (dict(location="Remote", remote="True", source="himalayas"), None),
    (dict(location="Remote", remote="True", source="lever"), "Remote (US)"),
    # the two-letter-code hole: case-insensitive alternation matched English
    # words and foreign country codes alike
    (dict(location="Remote in Germany", remote="True"), None),
    (dict(location="Berlin, DE", remote="True"), None),
    (dict(location="Pune, IN", remote="True"), None),
    (dict(location="Or Yehuda, Israel", remote="True"), None),
    (dict(location="Hamburg, DE Germany", remote="True"), None),
    (dict(location="Warsaw, Poland", remote="True"), None),
    # ... while the genuine article still gets through
    (dict(location="Remote, IN", remote="True"), "Remote (US)"),
    (dict(location="Remote", state="TX", remote="True",
          source="himalayas"), "Remote (US)"),
    (dict(location="United States", remote="True",
          source="himalayas"), "Remote (US)"),
    (dict(location="Worldwide", remote="True",
          source="remoteleaf"), "Remote (US)"),
    (dict(location="California (USA); Illinois (USA)", remote="True",
          source="jobgether"), "Remote (US)"),
    (dict(location="Boston, MA", remote=""), "MA"),
    (dict(location="Providence, RI", remote=""), "Nearby (RI/NH/CT)"),
]


def selftest():
    bad = 0
    for r, expect in GEO_CASES:
        for k in ("city", "state", "country", "work_mode", "source", "title"):
            r.setdefault(k, "")
        got, why = us_geo(r)
        flag = "ok " if got == expect else "BAD"
        bad += got != expect
        print(f"  {flag} {r['location'][:34]:<36} -> {str(got):<18} ({why})")

    # A stable-key check: hash() would pass this inside one process and fail
    # across two, which is exactly how it would have reached production.
    row = {"apply_url": "https://x/1", "company": "Acme", "title": "Engineer",
           "geo": "Remote (US)", "fit_tier": "Core AI building",
           "score": 70.0, "score_why": "x", "money_score": 40.0,
           "money_why": "y", "lane_tags": ",contract,"}
    import subprocess
    k1 = triage_row(dict(row))["job_key"]
    k2 = subprocess.run(
        [sys.executable, "-c",
         "import sys;sys.path.insert(0,'.');import contract_pool as c;"
         "print(c.triage_row(" + repr(row) + ")['job_key'])"],
        capture_output=True, text=True, cwd=BASE).stdout.strip()
    same = k1 == k2
    bad += not same
    print(f"  {'ok ' if same else 'BAD'} job_key stable across processes  "
          f"{k1} == {k2}")

    # Rate parsing must refuse rather than invent.  Every one of these was a
    # real row that produced a real number the fast-money lane would have
    # sorted to the very top.
    RATE_CASES = [
        ({"salary_min": "50000", "salary_period": "Hour"}, None),
        ({"salary_min": "1920", "salary_max": "26880",
          "salary_period": "Hour"}, None),
        ({"salary_min": "20000", "salary_max": "2000000",
          "salary_period": "Year"}, None),
        ({"salary_min": "15000", "salary_max": "18000",
          "salary_period": ""}, None),
        ({"salary_min": "6000", "salary_max": "6000",
          "salary_period": ""}, None),
        ({"salary_min": "75", "salary_max": "95",
          "salary_period": "Hour"}, 95.0),
        ({"salary_min": "150000", "salary_max": "180000",
          "salary_period": "Year"}, 180000 / HOURS_PER_YEAR),
        ({"salary_min": "300", "salary_max": "600",
          "salary_period": "Hour"}, 600.0),
        ({"salary_min": "80", "salary_period": "Hour",
          "salary_currency": "EUR"}, None),
    ]
    LINK_CASES = [
        ({"apply_url": "https://Bullhorn agency 1VCNF4"}, False),
        ({"apply_url": "", "board_url": ""}, False),
        ({"apply_url": "not a url"}, False),
        ({"apply_url": "https://localhost"}, False),
        ({"apply_url": "https://boards.greenhouse.io/x/jobs/1"}, True),
        ({"apply_url": "", "board_url": "https://jobs.lever.co/acme"}, True),
        ({"apply_url": "https://host.com:8080/apply?id=2"}, True),
    ]
    for raw, expect in LINK_CASES:
        got = usable_link(raw)
        bad += got != expect
        print(f"  {'ok ' if got == expect else 'BAD'} link "
              f"{str(raw.get('apply_url') or raw.get('board_url'))[:44]:<46} "
              f"-> {'usable' if got else 'dropped'}")

    for raw, expect in RATE_CASES:
        got = hourly(raw)
        okay = (got is None and expect is None) or (
            got is not None and expect is not None and abs(got - expect) < 0.01)
        bad += not okay
        print(f"  {'ok ' if okay else 'BAD'} rate {str(raw)[:58]:<60} "
              f"-> {got if got is None else round(got, 2)}")

    print("all pass" if not bad else f"{bad} FAILED")
    return 1 if bad else 0


def main():
    if "--selftest" in sys.argv:
        sys.exit(selftest())
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    # 4000, not 1000.  contract_pool.csv held 21,003 rows and the written
    # slice held 1,899; undecided sat at 930 / 993, i.e. BOTH slices pressed
    # flat against their own ceiling, which is what a binding cap looks like
    # from the outside.  4000 clears it with headroom without writing the whole
    # pool into triage.  0 = no cap if you want the lot.
    ap.add_argument("--top", type=int, default=4000,
                    help="rows in the contract (fit-blend) slice, default 4000")
    ap.add_argument("--top-money", type=int, default=4000,
                    help="rows in the fast-money slice, default 4000")
    ap.add_argument("--min-fit", type=float, default=DEFAULT_MIN_FIT,
                    help="minimum fit points for the triage slice "
                         f"(default {DEFAULT_MIN_FIT} = Adjacent tech or "
                         "better; 0 admits any professional role)")
    ap.add_argument("--per-company", type=int, default=5,
                    help="cap on rows from one employer in the top N "
                         "(default 5; the rest stay in contract_pool.csv)")
    ap.add_argument("--out-date", default=SNAP)
    ap.add_argument("--norm-dir", default=cc.NORM_DIR)
    args = ap.parse_args()

    today = date.today()
    paths = cc.norm_files(args.norm_dir)
    print(f"reading {len(paths)} source files from "
          f"{os.path.relpath(args.norm_dir, BASE)}/", flush=True)

    stats = {"raw": 0, "by_file": {}}
    rows = gather(paths, stats)
    print(f"raw {stats['raw']:,} -> contingent+deduped {len(rows):,}",
          flush=True)

    kept = []
    n_geo = 0
    rejects = Counter()
    for r in rows:
        # us_geo/field_ok read a flat CSV row; give them the keys they index.
        for k in ("country", "location", "city", "state", "remote",
                  "work_mode", "title", "source_type", "employment_type"):
            r.setdefault(k, "")
            if r[k] is None:
                r[k] = ""
            r[k] = str(r[k])
        bucket, basis = us_geo(r)
        if not bucket:
            rejects[basis.split(":")[0]] += 1
            continue
        n_geo += 1
        if not fh.field_ok(r):
            continue
        r["geo"] = bucket
        r["geo_basis"] = basis
        r["is_agg"] = 1 if (r.get("source") or "") in AGGREGATORS else 0
        h = hourly(r)
        r["hourly_rate"] = round(h, 2) if h else ""
        r.update(score_row(r, today))
        kept.append(r)

    kept.sort(key=lambda r: (r["score"], r.get("posted") or ""), reverse=True)
    print(f"  geo-in-scope {n_geo:,} -> field-ok {len(kept):,}", flush=True)
    print("  geo rejects: " + " · ".join(
        f"{n:,} {why}" for why, n in rejects.most_common(4)), flush=True)

    n_before = len(kept)
    kept = collapse(kept)
    kept.sort(key=lambda r: (r["score"], r.get("posted") or ""), reverse=True)
    print(f"  collapsed company+title {n_before:,} -> {len(kept):,}", flush=True)

    with open(OUT_POOL, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, POOL_COLS, extrasaction="ignore")
        w.writeheader()
        w.writerows(kept)
    print(f"wrote {os.path.basename(OUT_POOL)} ({len(kept):,} rows)",
          flush=True)

    # Two slices, ONE feed.  A gig can qualify for both lanes and must still be
    # a single row: job_key is the primary key, and splitting the slices into
    # separate feeds would either collide on that key or force you to judge the
    # same job twice.  Tagging instead means deciding it in either lane clears
    # it from both, which is what you want -- you have judged that job.
    reachable = [r for r in kept if usable_link(r)]
    if len(reachable) < len(kept):
        print(f"  dropped from the slices: {len(kept) - len(reachable):,} "
              f"with no openable apply link", flush=True)

    eligible = [r for r in reachable if r["fit_score"] >= args.min_fit]
    if args.min_fit:
        print(f"  fit floor >={args.min_fit:g}: {len(eligible):,} eligible "
              f"of {len(kept):,}", flush=True)
    fit_slice = spread(eligible, args.top, args.per_company)

    money_pool = [r for r in reachable if money_eligible(r)]
    money_pool.sort(key=lambda r: (r["money_score"], r.get("posted") or ""),
                    reverse=True)
    print(f"  fast-money eligible (rate known or speed >= "
          f"{MONEY_MIN_SPEED}): {len(money_pool):,} of {len(kept):,}",
          flush=True)
    money_slice = spread(money_pool, args.top_money, args.per_company)

    for r in kept:
        r["lane_tags"] = ""
    for r in fit_slice:
        r["lane_tags"] = ",contract,"
    for r in money_slice:
        r["lane_tags"] = (r["lane_tags"] or ",") + "fastmoney,"

    top = fit_slice + [r for r in money_slice if ",contract," not in
                       r["lane_tags"]]
    overlap = sum(1 for r in money_slice if ",contract," in r["lane_tags"])
    print(f"  slices: {len(fit_slice):,} contract + {len(money_slice):,} "
          f"fast-money, {overlap:,} in both -> {len(top):,} rows to triage",
          flush=True)
    out_triage = os.path.join(BASE, f"Contract_Pool_{args.out_date}.csv")
    with open(out_triage, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, TRIAGE_COLS, extrasaction="ignore")
        w.writeheader()
        for r in top:
            w.writerow(triage_row(r))
    print(f"wrote {os.path.basename(out_triage)} (top {len(top):,})",
          flush=True)

    if fit_slice:
        print(f"\n  CONTRACT slice ({len(fit_slice):,})  score "
              f"{fit_slice[-1]['score']:.0f}–{fit_slice[0]['score']:.0f}")
        print("    fit tiers:  " + " · ".join(
            f"{n:,} {t}" for t, n in
            Counter(r["fit_tier"] for r in fit_slice).most_common()))
        print("    geo:        " + " · ".join(
            f"{n:,} {g}" for g, n in
            Counter(r["geo"] for r in fit_slice).most_common()))
        print("    published rate: "
              f"{sum(1 for r in fit_slice if r['rate_score'] > 0):,}"
              f" · employer-direct: "
              f"{sum(1 for r in fit_slice if not r['is_agg']):,}")
    if money_slice:
        print(f"\n  FAST MONEY slice ({len(money_slice):,})  score "
              f"{money_slice[-1]['money_score']:.0f}–"
              f"{money_slice[0]['money_score']:.0f}")
        rated = [r for r in money_slice if r["money_rate"] > 0]
        print(f"    published rate: {len(rated):,}/{len(money_slice):,}"
              + (f"  median ~${sorted(r['hourly_rate'] for r in rated)[len(rated) // 2]:,.0f}/hr"
                 if rated else ""))
        print("    types:      " + " · ".join(
            f"{n:,} {t}" for t, n in
            Counter(r["employment_type"] for r in money_slice).most_common(8)))
        print("    geo:        " + " · ".join(
            f"{n:,} {g}" for g, n in
            Counter(r["geo"] for r in money_slice).most_common()))
        print("    fit tiers:  " + " · ".join(
            f"{n:,} {t}" for t, n in
            Counter(r["fit_tier"] for r in money_slice).most_common()))
    # Per SLICE, not over the union: the cap is what any one lane shows you, and
    # an employer legitimately reaching it in both lanes is not a breach.  Rows
    # with no company are counted apart — spread() gives each its own key (they
    # are different employers, the name just wasn't published), so counting them
    # together reads as a breach when the cap is working.
    for name, sl in (("contract", fit_slice), ("fast-money", money_slice)):
        if not sl:
            continue
        named = Counter(k for k in (co_key(r.get("company")) for r in sl) if k)
        blank = sum(1 for r in sl if not co_key(r.get("company")))
        print(f"  {name} lane: most from one employer "
              f"{named.most_common(1)[0][1]} (cap {args.per_company})"
              + (f" · {blank} rows have no company name" if blank else ""))


if __name__ == "__main__":
    main()
