#!/usr/bin/env python3
"""
startup_build.py — the startup cut of all_jobs_master.csv: US + remote, recent.

Three kinds of evidence put a job in this sheet, kept in separate tiers and
never mixed silently:

  Tier 1  Funded              a named round (Seed..Series H) at <= ~500 staff.
                              "Pre Seed" is admitted only at <= ~500 staff AND
                              on a VC-classified board, because Getro uses
                              "Pre Seed" as a junk default — 46k of its 62k rows
                              are 1000+ employers (Walgreens, AWS, Mayo Clinic).
  Tier 2  VC-backed           on a VC-classified board at <= ~500 staff, round
                              unknown.
  Tier 3  Startup-native ATS  ashby/lever/greenhouse/rippling/dover/yc/... with
                              no enrichment either way. Absence of enterprise
                              evidence, not presence of startup evidence.

Excluded outright: Ipo / Acquisition / Private Equity, 1000+ staff, the
enterprise and aggregator sources, and every board startup_boards.py called
`junk` or `ecosystem` (People Data Labs alone carries 181k candidate rows).

Posted dates are not uniformly trustworthy, so date quality is measured per
source rather than assumed: personio and rippling stamp ~98% of rows with the
run date, and jazzhr/bamboohr/recruitee carry no date at all. A naive 30-day
window would delete 153k undated rows while keeping every stale ingest-stamped
one. Rows without a verifiable date go to their own tab instead.

  python3 startup_build.py                  last 30 days
  python3 startup_build.py --days 7
  python3 startup_build.py --since 2026-07-01
  python3 startup_build.py --all-dates      no date window
  python3 startup_build.py --worldwide      skip the US/remote geography cut
  python3 startup_build.py --keep-funded-on-junk
        keep Tier 1 rows whose only board is a junk/ecosystem board (they still
        have a real named funding round; off by default)

Outputs Startup_Jobs_<date>.xlsx and Startup_Jobs_<date>.csv.
"""

import csv
import datetime as dt
import os
import re
import sys
from urllib.parse import urlsplit

from openpyxl import Workbook
from openpyxl.cell.cell import ILLEGAL_CHARACTERS_RE, WriteOnlyCell
from openpyxl.styles import Font, PatternFill
from openpyxl.utils import get_column_letter

import all_jobs_build as ab
import startup_boards as sb
from ai_jobs_filter import ADJ, CORE, ENG_TITLE, SKILL

BASE = os.path.dirname(os.path.abspath(__file__))
SRC = os.path.join(BASE, "all_jobs_master.csv")
SNAP = dt.datetime.now().strftime("%Y-%m-%d")
OUT_XLSX = os.path.join(BASE, f"Startup_Jobs_{SNAP}.xlsx")
OUT_CSV = os.path.join(BASE, f"Startup_Jobs_{SNAP}.csv")

csv.field_size_limit(10**9)
XLSX_CAP = 1_040_000


def _argval(flag, default):
    return sys.argv[sys.argv.index(flag) + 1] if flag in sys.argv else default


DAYS = int(_argval("--days", "30"))
SINCE = _argval("--since",
                (dt.date.today() - dt.timedelta(days=DAYS - 1)).isoformat())
ALL_DATES = "--all-dates" in sys.argv
WORLDWIDE = "--worldwide" in sys.argv
KEEP_FUNDED_ON_JUNK = "--keep-funded-on-junk" in sys.argv

# ---- what counts as a startup ----------------------------------------------
FUNDED = {"Seed", "Series A", "Series B", "Series C", "Series D", "Series E",
          "Series F", "Series G", "Series H", "Series I", "Series Unknown",
          "Series C Plus", "Early", "Growth", "Undisclosed", "Ico",
          "Unknown Stage"}
LATE = {"Ipo", "Acquisition", "Private Equity"}
SMALL_HC = {"1", "2", "3", "4"}          # up to ~500 staff
FUNDED_HC = SMALL_HC | {"5"}             # Tier 1 tolerates 501-1000
BIG_HC = "6"

STAGE_RANK = {"Seed": 1, "Pre Seed": 1, "Series A": 2, "Series B": 3,
              "Series C": 4, "Series C Plus": 4, "Series D": 5, "Series E": 6,
              "Series F": 6, "Series G": 6, "Series H": 6, "Series I": 6,
              "Early": 2, "Growth": 5}
HC_MID = {"1": 5, "2": 30, "3": 125, "4": 350, "5": 750, "6": 3000}

# Enterprise ATSes and aggregators. Workday's own floor is ~1000 employees;
# the aggregators re-list rows held elsewhere behind a redirect.
EXCL_SRC = {"workday", "oracle", "icims", "taleo", "successfactors",
            "eightfold", "phenom", "adp", "paycom", "keka", "jobvite", "gem",
            "zohorecruit", "careerjet", "jobgether", "themuse", "adzuna",
            "himalayas", "remoteleaf", "powertofly", "torre", "arbeitsagentur",
            "nofluffjobs", "instahyre", "wanted", "usajobs", "freelancer",
            "braintrust", "hubstafftalent", "wttj", "bullhorn", "jobicy",
            "remoteok", "weworkremotely", "remotive", "workingnomads",
            "justremote", "dynamitejobs", "remoteio", "jobspresso",
            # 2026-08-20 families.  Same rule as the rest of this set --
            # enterprise ATSes and government -- and it denies a startup TIER
            # in ma_remote_build.startup_of as well as dropping the row here.
            #   radancy      employer career sites for Walgreens / AT&T /
            #                Citi / UnitedHealth.  Enterprise by definition.
            #   cornerstone  Cornerstone OnDemand; 19% of stated rows are US,
            #                NL-heavy, large employers.
            #   ukg          UKG Pro, mid-to-large; 94% US but not startups.
            #   dayforce     Ceridian, mid-market and up.
            #   neogov       US state and municipal agencies -- 100% US and
            #                100% not a startup.
            # manatal is DELIBERATELY ABSENT: its customers are SMBs and small
            # recruiting agencies, so excluding it would drop genuine
            # small-company roles.  It is equally absent from ATS_DENSE and
            # ATS_MIXED, so it earns a tier only on real stage/headcount
            # evidence, never on "it uses this ATS" -- which is right, because
            # a large share of Manatal boards are agencies reposting client
            # roles, and those are not startups either.
            "radancy", "cornerstone", "ukg", "dayforce", "neogov"}

# Startup density per ATS, measured by joining each source's companies against
# the enriched (Getro/YC) population and scoring funded_share * small_share:
#
#   ashby .63  yc .41  lever .37 | greenhouse .23  jazzhr .23  smartrecruiters .06
#
# smartrecruiters measures 15% funded / 39% small / 34% at 1000+ staff — it is an
# enterprise ATS and is excluded from Tier 3 entirely. It was the single largest
# source of pollution in the first build (Domino's alone landed 19,997 rows).
# Caveat: for the low-coverage families (teamtailor, recruitee, greenhouse and
# lever match only 2-7% of their companies) the sample is biased toward companies
# that also sit on a VC board, so their true density is likely lower than
# measured, not higher.
ATS_DENSE = {"ashby", "lever", "yc", "dover", "teamtailor", "recruitee",
             "comeet", "pinpoint", "join", "thehub", "berlinstartup",
             "landingjobs", "getonbrd", "web3career", "wantedly",
             "cryptojobslist", "vuejobs"}
ATS_MIXED = {"greenhouse", "rippling", "breezy", "workable", "personio",
             "bamboohr", "jazzhr"}
STARTUP_ATS = ATS_DENSE | ATS_MIXED

# No company at startup scale has 200 open requisitions. Where headcount is
# known the cap also scales to it: more than 4 open roles per employee is a
# staffing agency, a franchise or an expert network, not a company hiring.
BULK_MAX = 200
BULK_PER_HEAD = 4
BULK_FLOOR = 25

# Expert networks list hundreds of synthetic "opportunities" that are not jobs.
JUNK_TITLE = re.compile(r"(?i)expert\s+(opportunity|network)|"
                        r"talent\s+(pool|community|network)\b|"
                        r"general\s+application|speculative\s+application|"
                        r"future\s+opportunit|open\s+application\b")

# ---- derived columns -------------------------------------------------------
# First match wins, so the order is the classifier. Data-ML precedes Science so
# "Data Scientist" does not land in Science; Engineering precedes Clinical so
# "Medical Device Engineer" does not land in Clinical.
ROLE_FAMILY = [
    ("Data-ML", re.compile(r"(?i)\b(machine\s+learning|\bml\b|\bai\b|"
                           r"data\s+(scien|analy|engineer)|analytics|"
                           r"business\s+intelligence|\bbi\b|\bnlp\b|"
                           r"computer\s+vision|mlops|statistic|"
                           r"quantitative\s+research)")),
    ("Science", re.compile(r"(?i)\b(scientist|biologist|chemist|"
                           r"biochemist|physicist|research\s+associate|"
                           r"research\s+scien|\br\s*&\s*d\b|laborator|"
                           r"bioinformatic|computational\s+(biolog|chemis|"
                           r"scien)|assay|in\s?vivo|in\s?vitro|"
                           r"formulation|crystallograph|spectroscop|"
                           r"preclinical|principal\s+investigator)")),
    ("Engineering", re.compile(r"(?i)\b(engineer|developer|swe|software|"
                               r"full[\s-]?stack|back[\s-]?end|front[\s-]?end|"
                               r"devops|sre|architect|programmer|qa\b|"
                               r"security|infrastructure|platform|mobile|ios|"
                               r"android|firmware|hardware|robotics|"
                               r"technical\s+lead)")),
    ("Clinical", re.compile(r"(?i)\b(nurse|nursing|\brn\b|\blpn\b|\bcna\b|"
                            r"\bnp\b|physician|clinical|clinician|therapist|"
                            r"therapy|pharmac|dental|dentist|hygienist|"
                            r"veterinar|psychiatr|psycholog|dietitian|"
                            r"nutritionist|caregiver|care\s+giver|patient|"
                            r"health\s+aide|\bemt\b|paramedic|phlebotom|"
                            r"radiolog|sonograph|behavior\s+technician|"
                            r"medical\s+assistant|counselor|social\s+worker|"
                            r"midwife|optometr|chiropract|surgeon)")),
    ("Product", re.compile(r"(?i)\b(product\s+manager|product\s+owner|\bpm\b|"
                           r"product\s+lead|product\s+design|technical\s+"
                           r"program|head\s+of\s+product|"
                           r"vice\s+president\s+of\s+product)")),
    ("Design", re.compile(r"(?i)\b(design|ux|\bui\b|creative|graphic|"
                          r"illustrat|motion|art\s+director|copywriter)")),
    ("Sales", re.compile(r"(?i)\b(sales|account\s+(executive|manager|"
                         r"director)|\bae\b|business\s+development|\bbdr\b|"
                         r"\bsdr\b|revenue|partnership|customer\s+success|"
                         r"solutions?\s+(consultant|engineer)|"
                         r"territory|inside\s+sales|leasing\s+consultant|"
                         r"client\s+success|implementation\s+manager|"
                         r"loan\s+originator|mortgage\s+advisor)")),
    ("Marketing", re.compile(r"(?i)\b(marketing|marketer|growth|content|seo|"
                             r"demand\s+gen|communications|social\s+media|"
                             r"\bpr\b|brand|community\s+manager|"
                             r"grant\s+writer|events?\s+(manager|"
                             r"coordinator))")),
    ("Finance", re.compile(r"(?i)\b(financ|account(ing|ant)|controller|"
                           r"\bcfo\b|treasur|audit|\btax\b|payroll|bookkeep|"
                           r"fp&a|underwrit|actuar|billing|accounts\s+(payable|"
                           r"receivable)|collections\s+special|"
                           r"revenue\s+cycle|credit\s+analyst)")),
    ("People", re.compile(r"(?i)\b(recruit|talent|people\s+(ops|operations|"
                          r"partner)|human\s+resources|\bhr\b|\bhrbp\b|"
                          r"compensation|benefits\s+specialist|onboarding)")),
    ("Legal", re.compile(r"(?i)\b(legal|counsel|attorney|paralegal|"
                         r"compliance|privacy|contracts?\s+manager|"
                         r"regulatory\s+affairs)")),
    ("Education", re.compile(r"(?i)\b(teacher|instructor|tutor|educat|"
                             r"professor|lecturer|curriculum|"
                             r"instructional|preschool|classroom|"
                             r"camp\s+counselor|coach\b|admissions)")),
    ("Retail-Food", re.compile(r"(?i)\b(cashier|barista|server|bartender|"
                               r"line\s+cook|cook\b|chef|dishwasher|host(ess)?|"
                               r"store\s+manager|key\s?holder|sales\s+"
                               r"associate|retail|dining|food\s+service|"
                               r"busser|kitchen|restaurant|crew\s+member|shift\s+(lead|"
                               r"supervisor)|merchandiser|stocker|store\s+"
                               r"associate|assistant\s+manager|game\s+day)")),
    ("Trades", re.compile(r"(?i)\b(electrician|plumber|carpenter|welder|hvac|"
                          r"mechanic|installer|superintendent|foreman|"
                          r"construction|maintenance|janitor|custodian|"
                          r"landscap|roofer|painter|waste\s+removal|"
                          r"field\s+inspector|surveyor|utility\s+worker|"
                          r"housekeep|material\s+handler|groundskeep|"
                          r"assembler|machinist|forklift)")),
    ("Support", re.compile(r"(?i)\b(support|help\s?desk|service\s+desk|"
                           r"customer\s+(care|service|support)|technician|"
                           r"call\s+cent|interpreter|translator|"
                           r"receptionist|front\s+desk)")),
    ("Operations", re.compile(r"(?i)\b(operations|\bops\b|supply\s+chain|"
                              r"logistics|warehouse|driver|manufactur|"
                              r"production|facilit|project\s+manager|"
                              r"program\s+manager|administrat|office\s+"
                              r"manager|executive\s+assistant|"
                              r"chief\s+of\s+staff|general\s+manager|"
                              r"\banalyst\b|coordinator|dispatcher|"
                              r"inventory|procurement|quality\s+assurance)")),
]

# Best Bets orders roles by how likely they are to be what you are looking for:
# technical first, then science, then commercial, then service and frontline.
# Every family stays in the sheet — this only affects rank.
ROLE_BAND = {"Engineering": 0, "Data-ML": 0, "Product": 0, "Design": 0,
             "Science": 1,
             "Sales": 2, "Marketing": 2, "Finance": 2, "People": 2,
             "Legal": 2, "Operations": 2, "Support": 2,
             "Clinical": 3, "Education": 3, "Retail-Food": 3, "Trades": 3,
             "Other": 3}

LEVEL = [
    ("Founding", re.compile(r"(?i)\bfounding\b|\bfounder\b|\bco[\s-]?founder\b")),
    ("C-level", re.compile(r"(?i)\b(chief\s+\w+\s+officer|\bcto\b|\bceo\b|"
                           r"\bcfo\b|\bcoo\b|\bcmo\b|\bcpo\b|\bciso\b|"
                           r"\bcio\b)\b")),
    ("VP", re.compile(r"(?i)\b(vp\b|vice\s+president|head\s+of\b|svp\b|evp\b)")),
    ("Director", re.compile(r"(?i)\bdirector\b|\bpartner\b")),
    ("Manager", re.compile(r"(?i)\bmanager\b|\bsupervisor\b|\bteam\s+lead\b")),
    ("Staff+", re.compile(r"(?i)\b(staff|principal|distinguished|fellow|"
                          r"architect)\b")),
    ("Senior", re.compile(r"(?i)\b(senior|sr\.?|lead|iii|iv|expert)\b")),
    ("Intern", re.compile(r"(?i)\b(intern|internship|co[\s-]?op|trainee|"
                          r"apprentice|placement)\b")),
    ("Junior", re.compile(r"(?i)\b(junior|jr\.?|entry[\s-]level|graduate|"
                          r"new\s+grad|associate|\bi\b|\bii\b)\b")),
]

AI_INDUSTRY = re.compile(r"(?i)artificial\s+intelligence|machine\s+learning|"
                         r"\bai\b|computer\s+vision|\bnlp\b|deep\s+learning|"
                         r"generative")

# Recruitment agencies and training providers reposting other companies' roles.
AGENCY_INDUSTRY = re.compile(r"(?i)cabinet\s+de\s+recrutement|recruitment|"
                             r"recruiting|staffing|\binterim\b|outplacement|"
                             r"employment\s+agency|\bformation\b|\bbtp\b|"
                             r"temp\s+agency|executive\s+search")

NEARBY_RE = re.compile(r"(?i)new\s+hampshire|,\s*nh\b|\bnashua\b|\bmanchester,"
                       r"\s*nh\b|\bportsmouth,\s*nh\b|connecticut|,\s*ct\b|"
                       r"\bhartford\b|\bmaine\b|,\s*me\b|vermont|,\s*vt\b")

US_HINT = re.compile(r"(?i)\bunited\s+states\b|\bu\.?s\.?a?\b|,\s*[A-Z]{2}\b|"
                     r"\b(?:anywhere|worldwide|global)\b")
BARE_REMOTE = re.compile(r"(?i)^\s*(remote(\s*[/,-]\s*(online|remote|anywhere))?"
                         r"|work\s*from\s*home|wfh)\s*$")

ATS_HOST = [
    ("greenhouse", "greenhouse.io"), ("lever", "lever.co"),
    ("ashby", "ashbyhq.com"), ("workday", "myworkdayjobs.com"),
    ("rippling", "rippling.com"), ("dover", "dover.com"),
    ("ycwaas", "ycombinator.com"), ("smartrecruiters", "smartrecruiters.com"),
    ("recruitee", "recruitee.com"), ("personio", "personio.de"),
    ("breezy", "breezy.hr"), ("bamboohr", "bamboohr.com"),
    ("jazzhr", "applytojob.com"), ("teamtailor", "teamtailor.com"),
    ("workable", "workable.com"), ("comeet", "comeet.co"),
    ("pinpoint", "pinpointhq.com"), ("join", "join.com"),
    ("icims", "icims.com"), ("oracle", "oraclecloud.com"),
    ("taleo", "taleo.net"), ("jobvite", "jobvite.com"),
    ("paylocity", "paylocity.com"), ("adp", "adp.com"),
    # 2026-08-20.  ats_of() already returned ("other", "yes") for these, so
    # nothing was being excluded -- naming them just makes the board-density
    # evidence legible instead of lumping six platforms under "other".
    ("ukg", "ultipro.com"), ("ukg", "rec.pro.ukg.net"),
    ("manatal", "careers-page.com"), ("dayforce", "dayforcehcm.com"),
    ("neogov", "governmentjobs.com"), ("cornerstone", "csod.com"),
    ("workday", "myworkdaysite.com"),
]

FX = {"USD": 1.0, "EUR": 1.09, "GBP": 1.27, "CAD": 0.73, "AUD": 0.66,
      "CHF": 1.12, "PLN": 0.25, "SEK": 0.095, "NOK": 0.093, "DKK": 0.146,
      "INR": 0.012, "JPY": 0.0064, "MXN": 0.050, "BRL": 0.18, "ILS": 0.27,
      "SGD": 0.74, "NZD": 0.61, "ZAR": 0.055, "AED": 0.27}
PERIOD_MULT = {"year": 1, "annual": 1, "annually": 1, "yearly": 1,
               "month": 12, "monthly": 12, "week": 52, "weekly": 52,
               "day": 260, "daily": 260, "hour": 2080, "hourly": 2080}

COLS = ["company", "title", "employment_type", "work_mode", "remote",
        "location", "city", "state", "country", "posted", "salary_min",
        "salary_max", "salary_currency", "salary_period", "equity", "skills",
        "company_stage", "company_size", "company_industry", "apply_url",
        "job_url", "source", "boards", "n_sources"]

CTX = ["rank", "tier", "evidence", "flags", "role_family", "level",
       "founding_role", "ai_role", "ai_company", "geo_bucket", "commute",
       "co_openings", "co_openings_all", "hiring_intensity", "vc_boards",
       "n_vc_boards", "date_quality", "days_old", "dup_count",
       "alt_locations",
       "salary_usd_min", "salary_usd_max", "has_salary", "equity_offered",
       "ats_family", "apply_direct"]

OUT_COLS = CTX + COLS

WIDTHS = {
    "rank": 7, "tier": 20, "evidence": 30, "flags": 18, "role_family": 13,
    "level": 10, "founding_role": 8, "ai_role": 10, "ai_company": 10,
    "geo_bucket": 18, "commute": 9, "co_openings": 8, "co_openings_all": 9,
    "hiring_intensity": 9, "vc_boards": 34, "n_vc_boards": 7,
    "date_quality": 12, "days_old": 9, "dup_count": 9,
    "alt_locations": 28, "salary_usd_min": 12,
    "salary_usd_max": 12, "has_salary": 9, "equity_offered": 9,
    "ats_family": 14, "apply_direct": 9, "company": 26, "title": 46,
    "employment_type": 13, "work_mode": 10, "remote": 8, "location": 30,
    "city": 16, "state": 7, "country": 8, "posted": 11, "salary_min": 10,
    "salary_max": 10, "salary_currency": 8, "salary_period": 8, "equity": 8,
    "skills": 44, "company_stage": 13, "company_size": 11,
    "company_industry": 26, "apply_url": 44, "job_url": 40, "source": 14,
    "boards": 34, "n_sources": 8,
}

CO_COLS = ["company", "tier", "openings_here", "openings_all_master",
           "company_stage", "company_size", "hiring_intensity", "vc_boards",
           "n_vc_boards", "company_industry", "ai_company", "role_families",
           "top_level", "newest_posting", "geo_buckets", "source",
           "sample_title", "sample_apply_url"]
CO_WIDTHS = {"company": 30, "tier": 20, "openings_here": 10,
             "openings_all_master": 12, "company_stage": 13,
             "company_size": 11, "hiring_intensity": 10, "vc_boards": 40,
             "n_vc_boards": 7, "company_industry": 30, "ai_company": 10,
             "role_families": 30, "top_level": 10, "newest_posting": 12,
             "geo_buckets": 24, "source": 14, "sample_title": 44,
             "sample_apply_url": 44}


def clean(v):
    return "" if v is None else ILLEGAL_CHARACTERS_RE.sub("", str(v))


def role_family_of(title):
    for name, rx in ROLE_FAMILY:
        if rx.search(title):
            return name
    return "Other"


def level_of(title):
    for name, rx in LEVEL:
        if rx.search(title):
            return name
    return "Mid"


def ai_role_of(row, title):
    if CORE.search(title):
        return "core"
    if ADJ.search(title):
        return "adjacent"
    sk = row.get("skills") or ""
    if sk and ENG_TITLE.search(title) and SKILL.search(sk):
        return "skills"
    return ""


def salary_usd(row):
    """(min, max) annualised USD, or (None, None). Rates are approximate and
    fixed at authoring time — good for sorting and banding, not for offers."""
    cur = (row.get("salary_currency") or "USD").strip().upper()
    rate = FX.get(cur)
    if rate is None:
        return None, None
    per = (row.get("salary_period") or "year").strip().lower()
    mult = PERIOD_MULT.get(per)
    if mult is None:
        return None, None          # 'Project' and friends do not annualise
    out = []
    for k in ("salary_min", "salary_max"):
        raw = (row.get(k) or "").strip().replace(",", "")
        try:
            v = float(raw)
        except ValueError:
            out.append(None)
            continue
        v = v * mult * rate
        # a "salary" under 1k/yr or over 10M is a unit error, not a number
        out.append(round(v) if 1000 <= v <= 10_000_000 else None)
    return out[0], out[1]


def ats_of(url):
    host = (urlsplit(url if "://" in url else "https://" + url.lstrip("/"))
            .hostname or "").lower()
    if not host:
        return "", ""
    for name, h in ATS_HOST:
        if host == h or host.endswith("." + h):
            return name, "yes"
    if ab._is_aggregator_url(url):
        return "aggregator", "no"
    return "other", "yes"


def pass1():
    """(date quality by source, company -> total openings in the whole master)

    Date quality is measured, not assumed. A source that stamps most of its
    rows with one date is recording when we fetched, not when the job was
    posted; one that leaves the field blank has no date at all. Neither can be
    filtered by a posted-date window.
    """
    dq = {}
    hist = {}
    co_all = {}
    with open(SRC, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            s = r["source"]
            d = hist.setdefault(s, {})
            p = (r["posted"] or "")[:10]
            d[p] = d.get(p, 0) + 1
            co = (r["company"] or "").strip().lower()
            if co:
                co_all[co] = co_all.get(co, 0) + 1
    for s, h in hist.items():
        n = sum(h.values())
        blank = h.get("", 0)
        dated = {k: v for k, v in h.items() if k}
        if blank / n > 0.5 or not dated:
            dq[s] = "none"
            continue
        top = max(dated.values())
        dq[s] = "ingest" if (top / n > 0.4 or len(dated) < 10) else "real"
    return dq, co_all


def negdate(d):
    return tuple(-int(x) for x in d.split("-")) if d.count("-") == 2 else (0,)


def main():
    verdicts = sb.load()
    if verdicts is None:
        print("startup_boards.csv missing — run startup_boards.py first",
              file=sys.stderr)
        return 1
    vc_boards = {b for b, v in verdicts.items() if v == "vc"}
    print(f"{len(vc_boards):,} vc boards, "
          f"{sum(1 for v in verdicts.values() if v == 'ecosystem'):,} ecosystem, "
          f"{sum(1 for v in verdicts.values() if v == 'junk'):,} junk")

    print("pass 1: measuring date quality and company-wide openings ...")
    dq, co_all = pass1()
    real = sorted(s for s, v in dq.items() if v == "real")
    ing = sorted(s for s, v in dq.items() if v == "ingest")
    non = sorted(s for s, v in dq.items() if v == "none")
    print(f"  real dates: {len(real)} sources · ingest-stamped: "
          f"{', '.join(ing) or 'none'} · no date: {', '.join(non) or 'none'}")

    today = dt.date.today()
    rows = []
    stats = {k: 0 for k in ("scanned", "excl_src", "excl_late", "excl_big",
                            "excl_board", "excl_bulk", "excl_title",
                            "excl_geo", "excl_date", "no_tier", "kept",
                            "undated", "junk_funded_dropped",
                            "collapsed_dups")}
    excluded_sample = []

    print("pass 2: selecting startup rows ...")
    with open(SRC, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            stats["scanned"] += 1
            src = r["source"]
            if src in EXCL_SRC:
                stats["excl_src"] += 1
                continue
            stage = (r["company_stage"] or "").strip()
            hc = (r["company_size"] or "").strip()
            if stage in LATE:
                stats["excl_late"] += 1
                continue
            if hc == BIG_HC:
                stats["excl_big"] += 1
                continue

            # ---- board evidence ------------------------------------------
            board_toks = [b.strip() for b in (r["boards"] or "").split(";")
                          if b.strip() and ":" not in b]
            known = [b for b in board_toks if b in verdicts]
            mine = [b for b in known if verdicts[b] == "vc"]
            on_vc = bool(mine)
            only_bad = bool(known) and not mine

            # ---- tier ----------------------------------------------------
            tier = evidence = ""
            if stage in FUNDED and hc in FUNDED_HC:
                tier = "1 Funded"
                evidence = f"{stage} · {'<=500' if hc in SMALL_HC else '501-1000'} staff"
            elif stage == "Pre Seed" and hc in SMALL_HC and on_vc:
                tier = "1 Funded"
                evidence = f"Pre Seed · <=500 staff · VC board: {mine[0]}"
            elif on_vc and hc in SMALL_HC:
                tier = "2 VC-backed"
                evidence = f"VC board: {mine[0]}" + (
                    f" (+{len(mine) - 1})" if len(mine) > 1 else "")
            elif on_vc and not hc:
                tier = "2 VC-backed"
                evidence = f"VC board: {mine[0]} · size unknown"
            elif src in ATS_DENSE and not stage and not hc:
                tier = "3 Startup ATS"
                evidence = f"{src} ATS only (startup-dense)"
            elif src in ATS_MIXED and not stage and not hc:
                tier = "4 SMB ATS"
                evidence = f"{src} ATS only (mixed SMB)"
            if not tier:
                stats["no_tier"] += 1
                continue

            # Junk / ecosystem boards give no evidence. A row whose only board
            # is one of those is dropped, unless it stands on a real funding
            # round of its own and --keep-funded-on-junk is set.
            if only_bad:
                if tier.startswith("1") and KEEP_FUNDED_ON_JUNK:
                    pass
                else:
                    if tier.startswith("1"):
                        stats["junk_funded_dropped"] += 1
                    stats["excl_board"] += 1
                    if len(excluded_sample) < 4000:
                        excluded_sample.append({
                            "company": r["company"], "title": r["title"],
                            "tier": tier, "boards": r["boards"][:200],
                            "company_stage": stage, "company_size": hc,
                            "source": src, "location": r["location"],
                            "why": "all boards junk/ecosystem: "
                                   + ", ".join(f"{b}={verdicts[b]}"
                                               for b in known[:3]),
                        })
                    continue

            # ---- bulk posters and synthetic listings ----------------------
            # The strongest available size signal when enrichment is absent is
            # how many roles the company has open across the whole master.
            co = (r["company"] or "").strip().lower()
            n_all = co_all.get(co, 0)
            mid = HC_MID.get(hc)
            cap = min(BULK_MAX,
                      max(BULK_FLOOR, BULK_PER_HEAD * mid)) if mid else BULK_MAX
            if n_all > cap:
                stats["excl_bulk"] += 1
                if len(excluded_sample) < 4000:
                    excluded_sample.append({
                        "company": r["company"], "title": r["title"],
                        "tier": tier, "boards": (r["boards"] or "")[:200],
                        "company_stage": stage, "company_size": hc,
                        "source": src, "location": r["location"],
                        "why": f"bulk poster: {n_all:,} openings master-wide "
                               f"> cap {cap}",
                    })
                continue
            if JUNK_TITLE.search(r["title"] or ""):
                stats["excl_title"] += 1
                if len(excluded_sample) < 4000:
                    excluded_sample.append({
                        "company": r["company"], "title": r["title"],
                        "tier": tier, "boards": (r["boards"] or "")[:200],
                        "company_stage": stage, "company_size": hc,
                        "source": src, "location": r["location"],
                        "why": "synthetic listing (expert network / talent "
                               "pool / general application)",
                    })
                continue

            # ---- geography ------------------------------------------------
            loc = r["location"] or ""
            state = r["state"] or ""
            country = (r["country"] or "").strip().upper()
            regs = ab.regions_of(loc, state, r["country"] or "")
            is_ma = "MA" in regs
            remote = (r["remote"] == "True" or r["work_mode"] == "Remote"
                      or "remote" in loc.lower())
            us = country in ("US", "USA", "UNITED STATES") or (
                not country and bool(US_HINT.search(loc + " " + state)))

            if is_ma:
                geo = "MA remote" if remote else "MA"
            elif remote and us:
                geo = "US remote"
            elif us:
                geo = "US onsite"
            elif remote and not country and BARE_REMOTE.match(loc):
                geo = "Remote (no country)"
            else:
                geo = "Other"
            if not WORLDWIDE and geo == "Other":
                stats["excl_geo"] += 1
                continue

            commute = "MA" if is_ma else (
                "nearby" if ("RI" in regs or NEARBY_RE.search(loc + " " + state))
                else "")

            # ---- date -----------------------------------------------------
            posted = (r["posted"] or "")[:10]
            q = dq.get(src, "real")
            if not posted:
                q = "none"
            elif q == "real":
                q = "real"
            undated = q != "real"
            if not ALL_DATES and not undated and posted < SINCE:
                stats["excl_date"] += 1
                continue
            days_old = ""
            if q == "real" and posted.count("-") == 2:
                try:
                    days_old = (today - dt.date.fromisoformat(posted)).days
                except ValueError:
                    days_old = ""

            # ---- derived --------------------------------------------------
            title = r["title"] or ""
            ind = r["company_industry"] or ""
            smin, smax = salary_usd(r)
            ats, direct = ats_of(r["apply_url"] or r["job_url"] or "")
            intensity = round(n_all / mid, 2) if mid else ""

            flags = []
            if n_all > 100:
                flags.append("bulk_poster")
            if AGENCY_INDUSTRY.search(ind):
                flags.append("agency?")
            if known and not mine:
                flags.append("ecosystem_only")
            if not loc.strip():
                flags.append("no_location")
            if q == "ingest":
                flags.append("ingest_date")
            elif q == "none":
                flags.append("no_date")

            rec = {c: r.get(c, "") for c in COLS}
            rec.update(
                tier=tier, evidence=evidence, flags=" ".join(flags),
                role_family=role_family_of(title), level=level_of(title),
                founding_role="yes" if re.search(r"(?i)\bfounding\b", title)
                              else "",
                ai_role=ai_role_of(r, title),
                ai_company="yes" if AI_INDUSTRY.search(ind) else "",
                geo_bucket=geo, commute=commute,
                co_openings=0, co_openings_all=n_all,
                hiring_intensity=intensity,
                vc_boards="; ".join(mine), n_vc_boards=len(mine),
                date_quality=q, days_old=days_old,
                salary_usd_min="" if smin is None else smin,
                salary_usd_max="" if smax is None else smax,
                has_salary="yes" if (smin or smax) else "",
                equity_offered="yes" if (r["equity"] or "").strip().lower()
                               in ("yes", "true") else "",
                ats_family=ats, apply_direct=direct,
            )
            rec["dup_count"] = 1
            rec["alt_locations"] = ""
            rec["_tier"] = int(tier[0])
            rec["_geo"] = {"MA": 0, "MA remote": 0, "US remote": 1,
                           "Remote (no country)": 2, "US onsite": 3}.get(geo, 4)
            rec["_role"] = ROLE_BAND.get(rec["role_family"], 3)
            rec["_stage"] = STAGE_RANK.get(stage, 7)
            rec["_ai"] = {"core": 0, "adjacent": 1, "skills": 2}.get(
                rec["ai_role"], 3)
            rec["_found"] = 0 if rec["founding_role"] else 1
            rec["_int"] = -(intensity if isinstance(intensity, float) else 0)
            rec["_posted"] = posted
            rec["_undated"] = undated
            rec["_co"] = co
            rows.append(rec)
            stats["kept"] += 1
            if undated:
                stats["undated"] += 1

    # ---- collapse cross-source duplicates ------------------------------
    # all_jobs_build.canon_key cannot collapse a Getro-hosted redirect page
    # against the same job's real ATS URL (Teragen Energy arrives once as
    # jobs.engine.xyz/... and once as jobs.ashbyhq.com/...), so the same
    # opening can appear several times. Collapse on company + title +
    # location, keep the row with the strongest evidence and a direct apply
    # URL, and record how many copies there were in dup_count.
    # The two copies of a job often disagree about location — Teragen's Ashby
    # copy says 'Lehi, UT, USA' with city='Southborough' while its Getro copy
    # says 'Southborough, MA, USA' — so location cannot be part of the key.
    # Key on company + title, keep the best-placed copy, and list every other
    # location seen in alt_locations so nothing is thrown away silently.
    def score(r):
        return (r["_tier"], r["_geo"],
                0 if r["apply_direct"] == "yes" else 1,
                0 if r["date_quality"] == "real" else 1)

    best = {}
    seen_locs = {}
    for r in rows:
        k = (r["_co"], (r["title"] or "").strip().lower())
        loc = (r["location"] or "").strip()
        seen_locs.setdefault(k, [])
        if loc and loc not in seen_locs[k]:
            seen_locs[k].append(loc)
        cur = best.get(k)
        if cur is None:
            best[k] = r
            continue
        n = cur["dup_count"] + 1
        if score(r) < score(cur):
            best[k] = r
        best[k]["dup_count"] = n
    stats["collapsed_dups"] = len(rows) - len(best)
    for k, r in best.items():
        others = [l for l in seen_locs[k] if l != (r["location"] or "").strip()]
        r["alt_locations"] = "; ".join(others[:4])
    rows = list(best.values())

    # ---- company-level counts ------------------------------------------
    here = {}
    for r in rows:
        here[r["_co"]] = here.get(r["_co"], 0) + 1
    for r in rows:
        r["co_openings"] = here[r["_co"]]

    rows.sort(key=lambda r: (r["_tier"], r["_geo"], r["_role"], r["_stage"],
                             r["_found"], r["_ai"], r["_int"],
                             negdate(r["_posted"]), r["company"], r["title"]))
    for i, r in enumerate(rows, 1):
        r["rank"] = i

    dated = [r for r in rows if not r["_undated"]]
    und = [r for r in rows if r["_undated"]]
    t1 = [r for r in dated if r["_tier"] == 1]
    t2 = [r for r in dated if r["_tier"] == 2]
    t3 = [r for r in dated if r["_tier"] == 3]
    t4 = [r for r in dated if r["_tier"] == 4]
    ma = [r for r in dated if r["geo_bucket"].startswith("MA")]
    ai = [r for r in dated if r["ai_role"] or r["ai_company"]]

    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=OUT_COLS, extrasaction="ignore")
        w.writeheader()
        w.writerows(rows)

    companies = build_companies(rows)
    write_xlsx(rows, dated, und, t1, t2, t3, t4, ma, ai, companies,
               excluded_sample, stats, dq)

    print(f"scanned {stats['scanned']:,}")
    for k in ("excl_src", "excl_late", "excl_big", "no_tier", "excl_board",
              "excl_bulk", "excl_title", "excl_geo", "excl_date"):
        print(f"  dropped {stats[k]:>9,}  {k}")
    print(f"kept {len(rows):,} jobs "
          f"({len(dated):,} dated, {len(und):,} undated) "
          f"across {len(companies):,} companies "
          f"— {stats['collapsed_dups']:,} cross-source duplicates collapsed")
    print(f"  Tier 1 Funded      {len(t1):,}")
    print(f"  Tier 2 VC-backed   {len(t2):,}")
    print(f"  Tier 3 Startup ATS {len(t3):,}")
    print(f"  Tier 4 SMB ATS     {len(t4):,}")
    print(f"  MA                 {len(ma):,}")
    print(f"  AI                 {len(ai):,}")
    if stats["junk_funded_dropped"]:
        print(f"  note: {stats['junk_funded_dropped']:,} Tier-1 rows dropped "
              f"for sitting only on junk/ecosystem boards "
              f"(--keep-funded-on-junk keeps them)")
    print(f"wrote {OUT_XLSX}")
    print(f"wrote {OUT_CSV}")
    return 0


def build_companies(rows):
    agg = {}
    for r in rows:
        co = r["_co"]
        a = agg.get(co)
        if a is None:
            a = agg[co] = {
                "company": r["company"], "tier": r["tier"],
                "openings_here": 0,
                "openings_all_master": r["co_openings_all"],
                "company_stage": r["company_stage"],
                "company_size": r["company_size"],
                "hiring_intensity": r["hiring_intensity"],
                "vc_boards": r["vc_boards"], "n_vc_boards": r["n_vc_boards"],
                "company_industry": r["company_industry"],
                "ai_company": r["ai_company"], "source": r["source"],
                "sample_title": r["title"],
                "sample_apply_url": r["apply_url"],
                "_fams": set(), "_levels": {}, "_geos": set(), "_newest": "",
                "_tier": r["_tier"],
            }
        a["openings_here"] += 1
        a["_fams"].add(r["role_family"])
        a["_levels"][r["level"]] = a["_levels"].get(r["level"], 0) + 1
        if r["geo_bucket"]:
            a["_geos"].add(r["geo_bucket"])
        if r["date_quality"] == "real" and r["_posted"] > a["_newest"]:
            a["_newest"] = r["_posted"]
        if r["_tier"] < a["_tier"]:
            a["_tier"] = r["_tier"]
            a["tier"] = r["tier"]
    out = []
    for a in agg.values():
        a["role_families"] = "; ".join(sorted(a.pop("_fams")))
        lv = a.pop("_levels")
        a["top_level"] = max(lv, key=lv.get) if lv else ""
        a["geo_buckets"] = "; ".join(sorted(a.pop("_geos")))
        a["newest_posting"] = a.pop("_newest")
        out.append(a)
    out.sort(key=lambda a: (a["_tier"], -a["openings_here"], a["company"]))
    return out


def _hdr(ws, names, bold):
    out = []
    for nm in names:
        c = WriteOnlyCell(ws, value=nm)
        c.font = bold
        out.append(c)
    return out


def tab(wb, name, rows, cols, widths, bold):
    ws = wb.create_sheet(name[:31])
    for i, c in enumerate(cols, 1):
        ws.column_dimensions[get_column_letter(i)].width = widths.get(c, 14)
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = \
        f"A1:{get_column_letter(len(cols))}{max(len(rows) + 1, 2)}"
    ws.append(_hdr(ws, cols, bold))
    for r in rows[:XLSX_CAP]:
        ws.append([clean(r.get(c, "")) for c in cols])


def write_xlsx(rows, dated, und, t1, t2, t3, t4, ma, ai, companies, excl,
               stats, dq):
    wb = Workbook(write_only=True)
    bold = Font(bold=True)
    grey = Font(color="808080")

    ws = wb.create_sheet("Summary")
    for col, w in (("A", 26), ("B", 12), ("C", 96)):
        ws.column_dimensions[col].width = w

    def row(*vals, b=False, g=False):
        cs = []
        for v in vals:
            c = WriteOnlyCell(ws, value=v)
            if b:
                c.font = bold
            elif g:
                c.font = grey
            cs.append(c)
        ws.append(cs)

    window = ("all dates" if ALL_DATES
              else f"posted on or after {SINCE} (last {DAYS} days)")
    row(f"Startup jobs — {'worldwide' if WORLDWIDE else 'US + remote'} — {SNAP}",
        b=True)
    row(window)
    row()
    row("Tab", "Rows", "What it is", b=True)
    row("Best Bets", len(dated),
        "Every dated row, ranked: tier, then MA before US-remote before "
        "US-onsite, then role band (engineering/data/product/design, then "
        "science, then commercial, then clinical/education/retail/trades), "
        "then earlier funding round, founding roles, AI roles, hiring "
        "intensity, newest. Role band only affects rank — nothing is hidden.")
    row("Tier 1 Funded", len(t1),
        "A named round (Seed..Series H) at <=500 staff (Tier 1 tolerates "
        "501-1000). 'Pre Seed' only counts here at <=500 staff AND on a VC "
        "board, because Getro uses Pre Seed as a junk default.")
    row("Tier 2 VC-backed", len(t2),
        "On a board startup_boards.py classified 'vc', at <=500 staff or "
        "unknown size, with no named round in the data.")
    row("Tier 3 Startup ATS", len(t3),
        "Posted through a measurably startup-dense ATS (ashby .63, yc .41, "
        "lever .37, dover, teamtailor, recruitee, comeet, pinpoint ...) with no "
        "company enrichment either way. This is absence of enterprise "
        "evidence, not presence of startup evidence.")
    row("Tier 4 SMB ATS", len(t4),
        "Same as Tier 3 but through an ATS that measures mixed SMB rather than "
        "startup: greenhouse .23, jazzhr .23, plus rippling, breezy, workable, "
        "personio, bamboohr. Real startups are in here, but so is every dental "
        "group and property manager. The weakest tier — check it, don't trust "
        "it. smartrecruiters measured .06 and is excluded outright.")
    row("Companies", len(companies),
        "One row per company: openings here and across the whole master, "
        "stage, size, backers, hiring intensity, role families, newest "
        "posting. Usually the better view for deciding who to approach.")
    row("MA", len(ma),
        "Massachusetts, on-site or remote. 'commute' also marks RI/NH/CT/ME/VT "
        "as 'nearby'.")
    row("AI", len(ai),
        "Either the role is AI (ai_role = core/adjacent/skills, same tiering "
        "as ai_jobs_filter.py) or the company is an AI company (ai_company, "
        "from its industry tags). Those are different things: an AI role at a "
        "non-AI company, and a sales job at an AI startup, both land here — "
        "filter the two columns to separate them.")
    row("Undated", len(und),
        "Rows with no verifiable posted date, kept out of Best Bets rather "
        "than deleted. jazzhr/bamboohr/recruitee publish no date at all; "
        "personio and rippling stamp ~98% of rows with the fetch date. A "
        "plain 30-day filter would silently drop the first group and keep "
        "every stale row in the second.")
    row("Excluded (sample)", len(excl),
        "A sample of dropped rows and the reason: only on junk/ecosystem "
        "boards, bulk poster, or a synthetic listing. Audit the filters here "
        "before trusting them.")
    row("All", len(rows), "Every kept row, dated and undated, flat.")
    row()
    row("Tier", "Rows", "", b=True)
    for label, n in (("1 Funded", len(t1)), ("2 VC-backed", len(t2)),
                     ("3 Startup ATS", len(t3)), ("4 SMB ATS", len(t4)),
                     ("Undated (any tier)", len(und))):
        row(label, n, "")
    row()
    row("Filter", "Dropped", "", b=True)
    for k, lbl in (("excl_src", "enterprise / aggregator source"),
                   ("excl_late", "IPO / acquired / private equity"),
                   ("excl_big", "1000+ staff"),
                   ("no_tier", "no startup evidence of any kind"),
                   ("excl_board", "only on junk / ecosystem boards"),
                   ("excl_bulk", "bulk poster (>200 openings, or >4 per "
                                 "employee)"),
                   ("excl_title", "synthetic listing (expert network etc.)"),
                   ("excl_geo", "outside US and not remote"),
                   ("excl_date", "older than the window")):
        row(lbl, stats[k], "")
    row("scanned", stats["scanned"], "rows in all_jobs_master.csv")
    if stats["junk_funded_dropped"]:
        row("", stats["junk_funded_dropped"],
            "of the junk/ecosystem drops had a real named funding round. "
            "Re-run with --keep-funded-on-junk to keep them.")
    row()
    row("Date quality by source", "", "", b=True)
    for q in ("ingest", "none"):
        names = sorted(s for s, v in dq.items() if v == q)
        row(q, len(names), ", ".join(names) or "none")
    row("real", sum(1 for v in dq.values() if v == "real"),
        "posted dates spread across many days — safe to filter on")
    row()
    row("Caveats", "", "", b=True)
    row("salary_usd_*", "",
        "Annualised with fixed FX rates set when this script was written. Fine "
        "for sorting and banding, not for negotiating.")
    row("company_size", "",
        "Getro head_count bucket: 1=1-10, 2=11-50, 3=51-200, 4=201-500, "
        "5=501-1000, 6=1000+. Bucket 6 is excluded throughout.")
    row("hiring_intensity", "",
        "Openings across the whole master divided by the midpoint of the size "
        "bucket. High means a small company with a lot of open roles.")
    row("evidence", "", "Why this row qualified, in words.")
    row("dup_count / alt_locations", "",
        "Copies of this opening collapsed into the row. The master cannot "
        f"always tell a Getro redirect page from the same job's real ATS URL, "
        f"so {stats['collapsed_dups']:,} duplicate rows were merged here.")
    row("role_family", "",
        "Derived from the title, because the master's own `department` field is "
        "75% empty and polluted. 16 families; 'Other' is what none of them "
        "matched.")
    row("flags", "",
        "bulk_poster (>100 openings master-wide) · agency? (recruitment or "
        "training industry tags) · ecosystem_only · no_location · "
        "ingest_date · no_date")
    row("board classification", "",
        "startup_boards.csv holds the verdict, the three measured shares and "
        "the reason for all 836 Getro boards. Edit it and re-run this script "
        "to override any call.")

    tab(wb, "Best Bets", dated, OUT_COLS, WIDTHS, bold)
    tab(wb, "Tier 1 Funded", t1, OUT_COLS, WIDTHS, bold)
    tab(wb, "Tier 2 VC-backed", t2, OUT_COLS, WIDTHS, bold)
    tab(wb, "Tier 3 Startup ATS", t3, OUT_COLS, WIDTHS, bold)
    tab(wb, "Tier 4 SMB ATS", t4, OUT_COLS, WIDTHS, bold)
    tab(wb, "Companies", companies, CO_COLS, CO_WIDTHS, bold)
    tab(wb, "MA", ma, OUT_COLS, WIDTHS, bold)
    tab(wb, "AI", ai, OUT_COLS, WIDTHS, bold)
    tab(wb, "Undated", und, OUT_COLS, WIDTHS, bold)
    tab(wb, "Excluded (sample)", excl,
        ["company", "title", "tier", "company_stage", "company_size",
         "source", "location", "boards", "why"],
        {"company": 26, "title": 44, "tier": 16, "company_stage": 13,
         "company_size": 11, "source": 14, "location": 28, "boards": 50,
         "why": 60}, bold)
    tab(wb, "All", rows, OUT_COLS, WIDTHS, bold)
    wb.save(OUT_XLSX)


if __name__ == "__main__":
    sys.exit(main())
