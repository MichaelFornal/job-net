#!/usr/bin/env python3
"""
icims_fetch.py — multi-tenant fetcher for the iCIMS ATS family.

Seed: tenants/icims.csv (359 tenant ids mined out of apply URLs already in the
corpus, so every one is a board that really exists).  Output:
contract_norm/icims.jsonl, one cc.row(...) per posting.

--------------------------------------------------------------------------
ROUTE
--------------------------------------------------------------------------
There is no JSON API on the tenant host.  Everything else was tried and ruled
out on 2026-07-28:

  /jobs/search?ss=1               the Jibe/Angular career-site SPA shell.  On
                                  some tenants ~4KB, on others 800KB, and on
                                  BOTH it contains zero job data — the listing
                                  is fetched client-side.
  /api/jobs, /jobs/feed,          all HTTP 200.  See the TRAPS section: the
  /jobs/search/rss, /rss          portal answers 200 on unrecognised paths.
  /sitemap.xml                    HTTP 403 (and robots.txt is "Disallow: /";
                                  see the note at the bottom of this docstring).

  --> /jobs/search?ss=1&in_iframe=1&pr=<page>
      The `in_iframe=1` flag is the whole trick.  It switches the portal out
      of the SPA wrapper and into the classic SERVER-RENDERED listing, whose
      markup is emitted by iCIMS itself (class names iCIMS_JobsTable /
      iCIMS_JobCardItem / iCIMS_JobHeaderField) and is therefore identical
      across customers even though the surrounding page is customer-branded.
      Verified: 40/40 sampled tenants render the same card layout.

      Each card carries title, the per-posting apply URL on the tenant's own
      icims.com host, the requisition id, the iCIMS canonical location code
      ("US-MO-Kansas City"), and a portal-configured set of labelled fields —
      Position Type / Employment Type, a Remote or Location Type facet, and on
      roughly a third of tenants a real Posted Date.

  --> {csx_origin}/api/jobs?page=N&limit=100    (retired tenants only)
      30 of the 359 tenants have been migrated off the classic portal onto an
      iCIMS Career Site (ex-Jibe) on the customer's own domain.  Those hand
      back a real JSON API — and, critically, every record's `apply_url` is
      still the tenant's own https://{tenant}.icims.com/jobs/{id}/login, so
      the rows stay attributable to a board and stay canonically dedupable
      against the copies other sources hold.  It also carries posted_date,
      employment_type and salary, which the HTML route does not.  Those 30
      tenants include the two biggest boards in the seed (restauranthourly-
      landrysinc 1,989 corpus rows, external-novanthealth 1,632); dropping
      them would have been the single biggest hole in this pipe.

      One career site fronts a WHOLE COMPANY, not one tenant: careers.
      hrblock.com carries three hrblock tenants, and the mastec family points
      SEVEN vanity domains (careers.mastec.com, jobs.mastec.com,
      careers.wanzek.com, careers.lemartec.com, ...) at one identical feed of
      1,309 jobs.  Each row is attributed to a board by parsing the tenant out
      of its own apply_url, never by guessing from the domain, so this is a
      coverage win rather than a mis-attribution risk — and duplicate origins
      are detected by the feed's `client_code` after a single page so the other
      six are dropped instead of paginated seven times over.

      ORDER: the portal pass runs first (it is what discovers the migrated
      targets), then the CSX pass, and a global seen-set on the canonical
      (host, job id) keeps the file's rows and its distinct apply URLs 1:1.
      So where a live tenant ALSO appears on a sibling's career site the HTML
      row wins and the richer JSON row is suppressed.  Measured at ~218 rows of
      57k, which is why it is not worth a third pass to invert.

--------------------------------------------------------------------------
TRAPS  (rule 2: never trust a status code)
--------------------------------------------------------------------------
1. WILDCARD DNS + 404.  Every *.icims.com name resolves.  An UNPROVISIONED
   subdomain is the one case that answers a clean HTTP 404 with a ~3KB error
   page (verified against three invented tenants).  404 is therefore the ONLY
   signal that means "this tenant does not exist", and it is recorded dead.

2. 200 + ~150 BYTES = A RETIRED TENANT, NOT AN EMPTY BOARD.  A tenant whose
   portal has been replaced by a Career Site answers HTTP 200 with a body that
   is nothing but

       <script>window.top.location.href = 'https://careers.<customer>.com/...';</script>

   No error, no redirect status, no jobs.  Read naively that is a live board
   with zero postings, and 30 tenants / ~5.7k corpus rows vanish silently.
   Discriminator: body under CSX_SHELL_MAX bytes AND matching _REDIR_RX.
   Those tenants are recorded `migrated` (a terminal state — never retried as
   a transient) and routed to the CSX API instead.

3. 200 ON ANY UNRECOGNISED PATH.  /api/jobs, /jobs/feed and /jobs/search/rss
   all return HTTP 200 — sometimes the full 200KB search page, sometimes a 4KB
   SPA shell.  A 200 proves nothing about whether the route exists.  Nothing
   here is believed until _TABLE_RX ("iCIMS_JobsTable", the listing container
   the portal emits even when the result set is empty) is found in the body.

4. 403 IS NOT A DEAD TENANT HERE.  ats_fetch.gj() treats 403 as a parked board
   because on breezy/bamboohr per-tenant subdomains that is what it means.  On
   iCIMS it is the opposite: dead is 404, and 403 is the WAF (the same live
   host that serves jobs 200 answers 403 for /sitemap.xml).  So 403 is
   classified TRANSIENT — backed off and retried, and a tenant that never
   clears is left PENDING for the next run.  The discriminator is per-platform
   and copying the other platform's rule would have thrown away live boards.

5. rel="next" LIES PAST THE END.  pr=500 on a 29-page board answers 200, zero
   cards, and STILL emits <link rel="next">.  Pagination is driven by the
   "Search Results Page 1 of N" header captured on page 0 (falling back to the
   highest pr= in the paginator), and additionally stops on the first page
   that yields zero cards.

Resumable: icims_fetch_state.json, per tenant.  A tenant is recorded done only
after ALL of its pages parsed cleanly, so a retry cannot double-append.  A
tenant that was not reached is left ABSENT from the state file (pending), never
recorded as done-and-empty.

POSTED DATES: icims_posted_ledger.json.  Only ~1/3 of portals expose a posted
date on the card, and all_jobs_build's cutoff never ages out an undated row.
So every posting gets a ledger entry the first time it is seen: the real date
when the portal gives one (precision "portal"/"api"), otherwise the first-seen
date (precision "first_seen").  Once written an entry is never overwritten, so
dates are stable across runs and a job does eventually age out.  LIKE EVERY
OTHER POSTED-DATE LEDGER IN THIS REPO, THIS FILE MUST NEVER BE ROTATED OR
DELETED — doing so silently re-dates the entire back catalogue to today.

NOTE ON robots.txt: iCIMS serves "User-agent: * / Disallow: /" on tenant
portals (and <meta name="robots" content="noindex,follow"> on the pages), the
usual ATS anti-duplicate-content posture rather than an access control — the
same postings are syndicated by the vendor to aggregators.  This pipe is a
personal job-search aggregator over boards whose URLs are already in the
corpus, and it is deliberately gentle: one global token bucket across the whole
vendor, a bounded thread pool, a browser UA, and honest 429 backoff.  Flagged
here so the choice is visible rather than buried.

AMENDED 2026-08-26: "already in the corpus" is no longer the whole story.
careers_resolve.py resolves company careers subdomains (jobs.<company>.com)
discovered from the Common Crawl host graph, and some of those front an iCIMS
tenant this corpus has never held a URL for.  Asked directly, the operator
chose to resolve those too, so the tenant list here can now contain boards that
arrived from outside the corpus.  Everything else above is unchanged, and
careers_resolve gives iCIMS its own slower token bucket (CAREERS_ICIMS_RPS,
default 2/s) so the per-vendor load stays proportionate.  Recorded here rather
than only in careers_resolve.py so this file does not read as narrower than the
pipeline actually is.
"""
import concurrent.futures as cf
import csv
import html
import json
import os
import random
import re
import threading
import time
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
# ICIMS_PFX=smoke_ sends the output, state and ledger to a throwaway set of
# files so a shakedown run cannot contaminate the real ledger (whose whole
# value is that its first-seen dates are never reset).  ICIMS_LIMIT caps the
# tenant count for the same purpose.
PFX = os.environ.get("ICIMS_PFX") or ""
LIMIT = int(os.environ.get("ICIMS_LIMIT") or 0)
TENANTS = os.path.join(BASE, "tenants", "icims.csv")
OUT = os.path.join(cc.NORM_DIR, PFX + "icims.jsonl")
STATE = os.path.join(BASE, PFX + "icims_fetch_state.json")
LEDGER = os.path.join(BASE, PFX + "icims_posted_ledger.json")

SOURCE = "icims"
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")
_TODAY_D = datetime.now(timezone.utc).date()

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36")

# One vendor edge serves every *.icims.com tenant, so there is ONE bucket for
# the whole family rather than one per tenant.  10 rps / 16 in flight measured
# clean over ~1,500 requests on 2026-07-28 (zero 429).  CSX career sites are
# separate customer-owned hosts behind a different CDN and get their own,
# slower bucket because a single origin can hold thousands of jobs.
RPS = {"icims": float(os.environ.get("ICIMS_RPS") or 10.0),
       "csx": float(os.environ.get("ICIMS_CSX_RPS") or 5.0)}
CONCURRENCY = {"icims": int(os.environ.get("ICIMS_CONC") or 16), "csx": 6}
WORKERS = int(os.environ.get("ICIMS_WORKERS") or 20)
TIMEOUT = 45
TRIES = 5
# securitycareers-aus is 402 pages; nothing observed is close to this cap.
MAX_PAGES = int(os.environ.get("ICIMS_MAX_PAGES") or 600)
CSX_MAX_PAGES = int(os.environ.get("ICIMS_CSX_MAX_PAGES") or 400)
# 0 disables.  A tenant not reached stays pending, so successive runs finish.
BUDGET_MIN = int(os.environ.get("ICIMS_BUDGET_MIN") or 0)
# A retired-portal shell is ~150 bytes; the smallest real portal page seen is
# 28KB.  4KB is a wide moat between the two.
CSX_SHELL_MAX = 4096

_TABLE_RX = re.compile(r'iCIMS_JobsTable', re.I)
_REDIR_RX = re.compile(
    r"window\.top\.location\.href\s*=\s*'([^']+)'|"
    r'window\.top\.location\.href\s*=\s*"([^"]+)"')
_PAGES_RX = re.compile(r'Search Results\s*Page\s+\d+\s+of\s+(\d+)', re.S | re.I)
_PR_RX = re.compile(r'/jobs/search\?pr=(\d+)')
_CARD_SPLIT = '<li class="iCIMS_JobCardItem">'
_LINK_RX = re.compile(
    r'<a\b[^>]*\bhref="(https?://[^"]*?\.icims\.com/jobs/(\d+)/[^"]*?)"'
    r'([^>]*)>(.*?)</a>', re.S | re.I)
# The label group is [^<]* — NOT (.*?) — and that is load-bearing.  With (.*?)
# and re.S the engine happily swallows markup to reach a later </span> that
# happens to be followed by a <span>, which is exactly the shape of the dt some
# portals emit:
#     <dt ...><span class="glyphicons map-marker"></span>
#             <span class="sr-only field-label">Location</span></dt>
# The greedy form matched from the card's TITLE label all the way to the
# glyphicons </span>, producing a label made of the job description and a value
# of the literal string "Location" — 719 rows carried "Location"/"Job Location"
# as their location, and because that bogus entry is inserted before the real
# dt/dd fields it was also free to steal the type / remote / posted slots on
# any board whose description happened to contain those words.
_HDR_RX = re.compile(
    r'<span class="sr-only field-label">([^<]*)</span>\s*<span([^>]*)>(.*?)'
    r'</span>', re.S | re.I)
_DL_RX = re.compile(
    r'<dt[^>]*iCIMS_JobHeaderField[^>]*>(.*?)</dt>\s*'
    r'<dd[^>]*iCIMS_JobHeaderData[^>]*>(.*?)</dd>', re.S | re.I)
_TITLE_RX = re.compile(r'<title>(.*?)</title>', re.S | re.I)
_SR_LABEL_RX = re.compile(
    r'<span class="sr-only field-label">.*?</span>', re.S | re.I)
_ATTR_TITLE_RX = re.compile(r'\btitle="([^"]*)"', re.I)
_TAG_RX = re.compile(r'<[^>]+>')

# Label -> meaning.  Portals are localised (Dutch "Werklocaties"/"Functie-id",
# French "Offres d'emploi"), so these are substring patterns over several
# languages, and each one is only ever consulted for its OWN slot.
_L_LOC = re.compile(r'locat|werkloc|lieu|standort|ubicaci|citt|plaats|'
                    r'\bort\b|sted|sijainti|localiza|\bcity\b|region', re.I)
_L_TYPE = re.compile(r'position type|employment type|job type|worker type|'
                     r'employee type|^\s*type\s*$|full-?time\s*/\s*part-?time|'
                     r'schedule|\bhours\b|contrat|vertragsart|dienstverband|'
                     r'jornada|arbeitszeit|^\s*status\s*$', re.I)
_L_REMOTE = re.compile(r'^\s*remote\s*$|remote work|work ?place|work ?type|'
                       r'work arrangement|location type|on-?site|telework|'
                       r'hybrid', re.I)
_L_POSTED = re.compile(r'posted date|date posted|^\s*posted\s*$|'
                       r'ver[oö]ffentlich|publicad|publication|geplaatst', re.I)
_L_COMPANY = re.compile(r'^\s*(company|organization|organisation|entity|'
                        r'brand|employer)\s*$', re.I)
# Pay bands.  Portals that publish them use two bare columns literally labelled
# "Min" and "Max" ("USD $42.13/Hr.").  Both the LABEL and the VALUE are gated —
# an unqualified "Min" is only read as money once the value carries a currency
# or a per-period marker — because "Min"/"Max" are generic enough to mean
# anything and a wrong salary is worse than a blank one.
_L_SAL_MIN = re.compile(r'^\s*(min|minimum|salary min\w*|pay min\w*|'
                        r'min\w* (salary|pay|rate))\.?\s*$', re.I)
_L_SAL_MAX = re.compile(r'^\s*(max|maximum|salary max\w*|pay max\w*|'
                        r'max\w* (salary|pay|rate))\.?\s*$', re.I)
_L_SAL_RANGE = re.compile(r'salary( range)?|pay range|compensation|'
                          r'hiring range|pay rate|hourly rate', re.I)
# "Job Listings at <Company>" and its localisations.  Falls back to the last
# " at "/" chez " split, then to the tenant slug.
_L_COMPANY_TITLE = re.compile(
    r'(?:job listings at|careers at|offres d[\u2019\']emploi chez|'
    r'stellenangebote bei|vacatures bij|ofertas de empleo en|'
    r'oportunidades em|lavoro presso|empleos en)\s+(.+?)\s*$', re.I)

# ISO 3166-1 alpha-2.  Needed because the location column is sometimes a free
# text "Location Name" ("HQ-Main"), and a bare ^[A-Z]{2}- test would happily
# turn "HQ" into a country.
_ISO2 = set("""AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF
BG BH BI BJ BL BM BN BO BQ BR BS BT BV BW BY BZ CA CC CD CF CG CH CI CK CL CM
CN CO CR CU CV CW CX CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM
FO FR GA GB GD GE GF GG GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM HN HR HT
HU ID IE IL IM IN IO IQ IR IS IT JE JM JO JP KE KG KH KI KM KN KP KR KW KY KZ
LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK ML MM MN MO MP MQ MR
MS MT MU MV MW MX MY MZ NA NC NE NF NG NI NL NO NP NR NU NZ OM PA PE PF PG PH
PK PL PM PN PR PS PT PW PY QA RE RO RS RU RW SA SB SC SD SE SG SH SI SJ SK SL
SM SN SO SR SS ST SV SX SY SZ TC TD TF TG TH TJ TK TL TM TN TO TR TT TV TW TZ
UA UG UM US UY UZ VA VC VE VG VI VN VU WF WS YE YT ZA ZM ZW""".split())
_LOC3 = re.compile(r'^([A-Za-z]{2})-([A-Za-z0-9]{2,3})-(.+)$')
_LOC2 = re.compile(r'^([A-Za-z]{2})-(.+)$')
_US_ADDR = re.compile(r'^(.+?),\s*([A-Za-z]{2}),\s*(\d{5})(?:-\d{4})?$')

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = {k: 0.0 for k in RPS}
_interval = {k: 1.0 / v for k, v in RPS.items()}
_pause_until = {k: 0.0 for k in RPS}
_n429 = {}
_gate = {k: threading.Semaphore(v) for k, v in CONCURRENCY.items()}

_state = {}
_ledger = {}
_csx_claimed = {}       # feed client_code -> the origin that is paginating it
_seen = set()           # canonical (host, job id) -> rows ~= distinct apply_url
_writer = None
_counts = {"rows": 0, "live": 0, "dead": 0, "migrated": 0, "empty": 0,
           "transient": 0}


class Transient(Exception):
    """A retryable failure.  Raised (never returned as an empty result) so a
    tenant that was merely unreachable stays PENDING instead of being recorded
    as a completed empty board."""


# ---- http ------------------------------------------------------------------
def throttle(bucket):
    while True:
        with _rate_lock:
            now = time.time()
            if now < _pause_until[bucket]:
                wait = _pause_until[bucket] - now
            else:
                wait = _next_t[bucket] - now
                if wait <= 0:
                    _next_t[bucket] = max(
                        _next_t[bucket] + _interval[bucket], now)
                    return
        time.sleep(min(wait, 5) if wait > 0 else 0)


def slow_down(bucket):
    with _rate_lock:
        _pause_until[bucket] = time.time() + 20
        _interval[bucket] = min(_interval[bucket] * 1.5, 0.5)
        _n429[bucket] = _n429.get(bucket, 0) + 1


def get(s, bucket, url, accept="text/html"):
    """Throttled GET -> (status, text).

    Contract, borrowed from ats_fetch.gj() and inverted where iCIMS differs:
      * 404  -> returns (404, "").  The tenant does not exist.  TERMINAL.
      * 403  -> TRANSIENT on this platform (see trap 4).  Retried, and if it
                never clears the caller leaves the tenant pending.
      * 429/503/5xx/timeouts -> transient, backed off, and RAISE after TRIES.
    Nothing about a 200 is believed here; the caller validates the body.
    """
    last = None
    for attempt in range(TRIES):
        throttle(bucket)
        try:
            r = s.get(url, timeout=TIMEOUT,
                      headers={"Accept": accept}, allow_redirects=True)
            if r.status_code == 200:
                return 200, (r.text or "")
            if r.status_code in (404, 410):
                return r.status_code, ""
            if r.status_code in (429, 418, 503):
                slow_down(bucket)
                last = "HTTP %d" % r.status_code
            elif r.status_code == 403:
                # NOT a dead tenant on iCIMS — dead is 404.  Treated as WAF /
                # throttling so the board is retried rather than buried.
                slow_down(bucket)
                last = "HTTP 403 (WAF?)"
            else:
                last = "HTTP %d" % r.status_code
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(1.5 * (attempt + 1))
    raise Transient("%s failed after %d tries: %s" % (url, TRIES, last))


def session():
    s = requests.Session()
    s.headers.update({"User-Agent": UA,
                      "Accept-Language": "en-US,en;q=0.9"})
    return s


# ---- text helpers ----------------------------------------------------------
def _text(fragment):
    return html.unescape(_TAG_RX.sub(" ", fragment or ""))


def _squash(v):
    return re.sub(r"\s+", " ", v or "").strip()


def _clean(fragment):
    return _squash(_text(fragment))


# ---- dates -----------------------------------------------------------------
_D_ISO = re.compile(r'(\d{4})-(\d{1,2})-(\d{1,2})')
_D_NUM = re.compile(r'(\d{1,2})([./-])(\d{1,2})\2(\d{2,4})')
_REL = re.compile(r'(\d+)\s*(minute|hour|day|week|month|year)s?\s+ago', re.I)
_REL_TODAY = re.compile(r'\btoday\b|\bjust now\b|\bhours? ago\b', re.I)
_REL_YDAY = re.compile(r'\byesterday\b', re.I)
_REL_DAYS = {"minute": 0, "hour": 0, "day": 1, "week": 7, "month": 30,
             "year": 365}


def _mkdate(y, m, d):
    if y < 100:
        y += 2000
    try:
        dt = datetime(y, m, d).date()
    except ValueError:
        return None
    # A posted date cannot be in the future and a 10-year-old posting is a
    # parse error, not a job.  Rejecting instead of emitting garbage.
    if dt > _TODAY_D + timedelta(days=1) or dt < _TODAY_D - timedelta(days=3650):
        return None
    return dt


def _rel_days(text):
    """Days-ago implied by the visible relative label, or None."""
    if not text:
        return None
    m = _REL.search(text)
    if m:
        return int(m.group(1)) * _REL_DAYS[m.group(2).lower()]
    if _REL_YDAY.search(text):
        return 1
    if _REL_TODAY.search(text):
        return 0
    return None


def parse_posted(abs_text, rel_text=""):
    """iCIMS renders the posted date in the portal's own locale — US portals
    say '7/28/2026 5:41 PM', the Dutch one says '26.6.2026 08:00' — so a fixed
    format would mis-date whole tenants.  Both orderings are tried and, when
    they disagree, the visible relative label ('6 hours ago', '1 month ago')
    picks the winner; only if that is missing does the separator convention
    (slash = month-first, dot = day-first) decide."""
    s = _squash(abs_text)
    if not s:
        return ""
    m = _D_ISO.search(s)
    if m:
        d = _mkdate(int(m.group(1)), int(m.group(2)), int(m.group(3)))
        return d.strftime("%Y-%m-%d") if d else ""
    m = _D_NUM.search(s)
    if not m:
        return ""
    a, sep, b, y = int(m.group(1)), m.group(2), int(m.group(3)), int(m.group(4))
    mdy = _mkdate(y, a, b)          # month-first (US)
    dmy = _mkdate(y, b, a)          # day-first (EU)
    cands = [c for c in (mdy, dmy) if c]
    if not cands:
        return ""
    if len(cands) == 1 or mdy == dmy:
        return cands[0].strftime("%Y-%m-%d")
    est = _rel_days(rel_text)
    if est is not None:
        pick = min(cands, key=lambda c: abs((_TODAY_D - c).days - est))
    else:
        pick = mdy if sep == "/" else dmy
    return pick.strftime("%Y-%m-%d")


def ledger_posted(key, real, precision):
    """First writer wins.  A job whose portal shows no date is dated by the day
    this pipe first saw it, so it still ages out of the build's cutoff — and it
    keeps that date forever instead of drifting forward on every run."""
    with _lock:
        e = _ledger.get(key)
        if e:
            return e.get("posted") or ""
        _ledger[key] = {"posted": real or TODAY,
                        "precision": precision if real else "first_seen",
                        "first_seen": TODAY}
        return _ledger[key]["posted"]


# ---- pay -------------------------------------------------------------------
_CUR_SYM = {"$": "USD", "€": "EUR", "£": "GBP", "¥": "JPY",
            "₹": "INR", "₩": "KRW", "₽": "RUB"}
_CUR_CODE = re.compile(r'\b(USD|CAD|EUR|GBP|AUD|NZD|INR|MXN|BRL|ZAR|SGD|JPY|'
                       r'CHF|SEK|NOK|DKK|PLN|CZK|HUF|AED|SAR|PHP|MYR|THB|HKD)'
                       r'\b')
# The whole numeric token including its separators; _to_float decides
# which of them is a decimal point.
_NUM = re.compile(r'(\d[\d.,  ]*\d|\d)')
_PERIODS = ((re.compile(r'/\s*hr|\bhr\b|\bhour|hourly', re.I), "hour"),
            (re.compile(r'/\s*yr|\byr\b|\byear|annual|annum|\bp\.?a\.?\b',
                        re.I), "year"),
            (re.compile(r'/\s*mo\b|\bmonth', re.I), "month"),
            (re.compile(r'/\s*wk\b|\bweek', re.I), "week"),
            (re.compile(r'/\s*day\b|daily|diem', re.I), "day"))


def _to_float(tok):
    """Number token -> float, without assuming a decimal convention.

    '85,000.00', '85,000', '55.000,00' and '42.13' all appear on iCIMS pay
    columns depending on the portal's locale, and stripping separators blindly
    turns 55.000,00 into 55.  Rule: the LAST separator is the decimal point
    only when 1-2 digits follow it; otherwise every separator is a grouper."""
    tok = re.sub(r'[\s  ]', "", tok or "")
    if not tok:
        return None
    cut = max(tok.rfind("."), tok.rfind(","))
    if cut == -1:
        whole, frac = tok, ""
    elif 1 <= len(tok) - cut - 1 <= 2:
        whole, frac = tok[:cut], tok[cut + 1:]
    else:
        whole, frac = tok, ""
    whole = whole.replace(".", "").replace(",", "")
    if not whole.isdigit() or (frac and not frac.isdigit()):
        return None
    try:
        return float(whole + ("." + frac if frac else ""))
    except ValueError:
        return None


def parse_money(raw):
    """'USD $42.13/Hr.' -> (42.13, 'USD', 'hour').  Returns (None, '', '')
    unless the string is unambiguously money: a currency marker or a period
    marker must be present, so a bare '40' in a 'Standard Hours' column can
    never be mistaken for pay."""
    s = _squash(raw)
    if not s:
        return None, "", ""
    cur = ""
    m = _CUR_CODE.search(s.upper())
    if m:
        cur = m.group(1)
    else:
        for sym, code in _CUR_SYM.items():
            if sym in s:
                cur = code
                break
    period = ""
    for rx, name in _PERIODS:
        if rx.search(s):
            period = name
            break
    if not cur and not period:
        return None, "", ""
    m = _NUM.search(s)
    if not m:
        return None, "", ""
    val = _to_float(m.group(1))
    if val is None:
        return None, "", ""
    if val <= 0 or val > 1e7:
        return None, "", ""
    return val, cur, period


def card_pay(fields):
    """(min, max, currency, period) from the card's pay columns."""
    lo, cur, per = parse_money(pick(fields, _L_SAL_MIN)[0])
    hi, cur2, per2 = parse_money(pick(fields, _L_SAL_MAX)[0])
    if lo is None and hi is None:
        # Single combined column: "$60,000 - $80,000 / year".
        raw = pick(fields, _L_SAL_RANGE)[0]
        parts = re.split(r'\s*(?:-|–|to)\s*', raw, maxsplit=1)
        lo, cur, per = parse_money(parts[0] if parts else "")
        if len(parts) > 1:
            hi, cur2, per2 = parse_money(
                parts[1] if _CUR_CODE.search(parts[1].upper())
                or any(s in parts[1] for s in _CUR_SYM)
                else (cur or "$") + parts[1] + " " + (per or ""))
    return (lo if lo is not None else "", hi if hi is not None else "",
            cur or cur2 or "", per or per2 or "")


# ---- location --------------------------------------------------------------
def split_location(raw):
    """'US-MO-Kansas City' -> (city, state, country).  Only the iCIMS canonical
    code is parsed; a free-text 'Location Name' is left whole so a label like
    'HQ-Main' never becomes country 'HQ'."""
    s = _squash(raw)
    if not s:
        return "", "", ""
    first = re.split(r"\s*[|;]\s*", s)[0]
    m = _LOC3.match(first)
    if m and m.group(1).upper() in _ISO2:
        return _squash(m.group(3)), m.group(2).upper(), m.group(1).upper()
    m = _LOC2.match(first)
    if m and m.group(1).upper() in _ISO2:
        return _squash(m.group(2)), "", m.group(1).upper()
    # A minority of portals publish a free-text address instead of the code
    # ("Decatur, GA, 30033").  Only the ZIP-terminated US form is read, because
    # a 5-digit postal code next to a 2-letter state is unambiguous; "Paris,
    # TX" vs "Paris, FR" is not, so the bare two-token form is left alone.
    m = _US_ADDR.match(first)
    if m:
        return _squash(m.group(1)), m.group(2).upper(), "US"
    return "", "", ""


def work_mode_from(remote_val, loc_city, loc_raw):
    """Structured facet first, location code second, nothing else.

    A wrong remote flag is worse than none, so the title is never consulted and
    an explicit 'Remote: No' resolves to work_mode "" (the portal is saying
    "not remote", not "on-site" — hybrid boards use the same value)."""
    v = (remote_val or "").strip().lower()
    if v:
        if "hybrid" in v:
            return "Hybrid", False
        if v in ("yes", "y", "true", "1") or "remote" in v or "telework" in v \
                or "virtual" in v or "work from home" in v:
            return "Remote", True
        if v in ("no", "n", "false", "0"):
            # "not remote" is not the same claim as "on-site" — hybrid boards
            # answer No here too, so work_mode is left unknown on purpose.
            return "", False
        if "on-site" in v or "onsite" in v or "in office" in v \
                or "in-office" in v:
            return "On-site", False
    # iCIMS writes a remote posting's city as the literal token "Remote"
    # inside the location code ("US-MO-Remote"), which is structured enough to
    # trust.  A free-text location is not.
    if loc_city and loc_city.strip().lower() in ("remote", "remote/virtual",
                                                 "virtual", "telecommute"):
        return "Remote", True
    if re.search(r'(^|[-|,\s])remote($|[-|,\s])', loc_raw or "", re.I):
        return "Remote", True
    return "", False


# ---- emit ------------------------------------------------------------------
def canon(url):
    """(host, numeric job id) — the same identity all_jobs_build.canon_key
    derives from an iCIMS apply URL, used here to keep this file's rows and its
    distinct apply URLs in 1:1 correspondence."""
    sp = urlsplit(url)
    parts = [p for p in sp.path.split("/") if p]
    if len(parts) >= 2 and parts[0] == "jobs" and parts[1].isdigit():
        return sp.netloc.lower(), parts[1]
    return sp.netloc.lower(), sp.path


def emit(r):
    key = canon(r["apply_url"])
    with _lock:
        if key in _seen:
            return False
        _seen.add(key)
        _writer.write(r)
        _counts["rows"] += 1
    return True


def strip_q(url):
    sp = urlsplit(html.unescape(url or ""))
    return urlunsplit((sp.scheme or "https", sp.netloc, sp.path, "", ""))


# ---- classic portal --------------------------------------------------------
def board_company(page, tenant):
    m = _TITLE_RX.search(page)
    if m:
        t = _squash(html.unescape(m.group(1)))
        mm = _L_COMPANY_TITLE.search(t)
        if mm:
            return _squash(mm.group(1))
        for sep in (" at ", " chez ", " bei ", " bij "):
            if sep in t:
                return _squash(t.rsplit(sep, 1)[1])
        if t:
            return t
    return tenant.replace("-", " ").title()


def card_fields(chunk):
    """All labelled fields on one card, as {lowercased label: (value, attr)}.

    Two markups carry them: the sr-only header spans at the top of the card
    (Location / Posted Date / ID) and the dt/dd pairs in the additionalFields
    block (Position Type / Remote / Category).  Both are parsed because which
    fields land in which block is a per-portal configuration."""
    out = {}
    for label, attrs, val in _HDR_RX.findall(chunk):
        lab = _clean(label)
        if lab:
            out.setdefault(lab.lower(), (_clean(val), attrs or ""))
    for label, val in _DL_RX.findall(chunk):
        lab = _clean(label)
        if lab:
            out.setdefault(lab.lower(), (_clean(val), ""))
    return out


def pick(fields, rx, exclude=None):
    for lab, (val, attrs) in fields.items():
        if rx.search(lab) and not (exclude and exclude.search(lab)):
            return val, attrs
    return "", ""


def parse_cards(page, tenant, company):
    rows = []
    body = page
    chunks = body.split(_CARD_SPLIT)[1:]
    for i, chunk in enumerate(chunks):
        if i == len(chunks) - 1:
            end = chunk.find("</ul>")
            if end > 0:
                chunk = chunk[:end]
        m = _LINK_RX.search(chunk)
        if not m:
            continue
        url, jid, attrs, inner = m.group(1), m.group(2), m.group(3), m.group(4)
        # The anchor wraps a screen-reader label ("Title") before the <h3>;
        # without dropping it every title comes out as "Title <real title>".
        title = _clean(_SR_LABEL_RX.sub("", inner))
        if not title:
            # Fall back to the anchor's title attribute, "46098 - Job Title".
            at = _ATTR_TITLE_RX.search(attrs or "")
            if at:
                title = _squash(html.unescape(at.group(1)).split(" - ", 1)[-1])
        if not title:
            continue
        apply_url = strip_q(url)
        f = card_fields(chunk)

        # "Location Type" matches both patterns and its value is a work mode,
        # not a place — it must never be read as the location string.
        loc_raw, _ = pick(f, _L_LOC, exclude=_L_REMOTE)
        if not loc_raw:
            # Some portals label the location column with a bare company word;
            # fall back to any field whose VALUE is an iCIMS location code.
            for _, (val, _a) in f.items():
                if split_location(val)[2]:
                    loc_raw = val
                    break
        city, state, country = split_location(loc_raw)

        type_raw, _ = pick(f, _L_TYPE)
        remote_raw, _ = pick(f, _L_REMOTE)
        mode, remote = work_mode_from(remote_raw, city, loc_raw)

        posted_val, posted_attrs = pick(f, _L_POSTED)
        abs_txt = ""
        at = _ATTR_TITLE_RX.search(posted_attrs or "")
        if at:
            abs_txt = html.unescape(at.group(1))
        posted = parse_posted(abs_txt or posted_val, posted_val)
        posted = ledger_posted("%s:%s" % (tenant, jid), posted, "portal")

        comp_val, _ = pick(f, _L_COMPANY)
        smin, smax, scur, sper = card_pay(f)
        rows.append(cc.row(
            source=SOURCE, source_type="ats",
            company=comp_val or company, title=title,
            employment_type=cc.classify_any(type_raw, title),
            work_mode=mode, location=_squash(loc_raw),
            city=city, state=state, country=country, remote=remote,
            salary_min=smin, salary_max=smax, salary_currency=scur,
            salary_period=sper,
            posted=posted, apply_url=apply_url, job_id=jid, board=tenant,
            board_url="https://%s.icims.com/jobs/search?ss=1" % tenant))
    return rows


def total_pages(page):
    m = _PAGES_RX.search(page)
    if m:
        return max(1, int(m.group(1)))
    prs = [int(x) for x in _PR_RX.findall(page)]
    return max(prs) + 1 if prs else 1


def page_url(tenant, pr):
    u = "https://%s.icims.com/jobs/search?ss=1&in_iframe=1" % tenant
    return u if pr == 0 else u + "&pr=%d" % pr


def fetch_tenant(tenant):
    """-> ('live', rows) | ('empty', []) | ('dead', []) | ('migrated', url).
    Raises Transient when the tenant could not be reached at all."""
    s = session()
    try:
        status, page = get(s, "icims", page_url(tenant, 0))
        if status in (404, 410):
            return "dead", []
        m = _REDIR_RX.search(page)
        if m and len(page) <= CSX_SHELL_MAX:
            return "migrated", (m.group(1) or m.group(2)).replace("\\", "")
        if not _TABLE_RX.search(page):
            # 200 without the listing container: the SPA shell or an
            # unrecognised-path echo, never a real listing.  Not proof the
            # tenant is dead either, so it stays pending.
            raise Transient("%s: 200 without iCIMS_JobsTable (%d bytes)"
                            % (tenant, len(page)))
        company = board_company(page, tenant)
        rows = parse_cards(page, tenant, company)
        npages = min(total_pages(page), MAX_PAGES)
        for pr in range(1, npages):
            st, pg = get(s, "icims", page_url(tenant, pr))
            if st != 200 or not _TABLE_RX.search(pg):
                raise Transient("%s: page %d unusable (status %s)"
                                % (tenant, pr, st))
            got = parse_cards(pg, tenant, company)
            if not got:
                break       # rel="next" lies past the end; a bare page does not
            rows.extend(got)
        return ("live" if rows else "empty"), rows
    finally:
        s.close()


# ---- CSX (retired portals) -------------------------------------------------
def csx_origin(url):
    sp = urlsplit(url)
    if not sp.netloc:
        return ""
    return "%s://%s" % (sp.scheme or "https", sp.netloc)


def tenant_of(apply_url):
    host = urlsplit(apply_url or "").netloc.lower()
    if host.endswith(".icims.com"):
        return host[:-len(".icims.com")]
    return ""


def fetch_csx(origin):
    """iCIMS Career Site JSON API.  limit is capped at 100 by the server (200
    answers HTTP 422), and the row's apply_url is the tenant's own icims.com
    posting URL, which is what keeps these rows attributable and dedupable."""
    s = session()
    rows = []
    try:
        for pg in range(1, CSX_MAX_PAGES + 1):
            st, body = get(s, "csx",
                           "%s/api/jobs?page=%d&limit=100" % (origin, pg),
                           accept="application/json")
            if st != 200 or not body:
                raise Transient("%s: page %d -> %s" % (origin, pg, st))
            try:
                d = json.loads(body)
            except ValueError:
                # 200 + HTML means this origin is not a Career Site at all.
                raise Transient("%s: non-JSON body on /api/jobs" % origin)
            jobs = d.get("jobs") if isinstance(d, dict) else None
            if not jobs:
                break
            # One feed, many vanity domains: the mastec family points seven
            # hostnames at one identical 1,309-job site.  client_code names the
            # FEED, so the first origin to claim it paginates and the rest bail
            # out after this single page instead of re-walking it.
            if pg == 1:
                first = (jobs[0] or {}).get("data") or {}
                # client_code names the feed but is NOT always populated (the
                # mastec sites return it as null), so the fallback fingerprint
                # is the pair every feed does carry: total job count plus the
                # first record's apply URL.  Verified identical across all
                # seven mastec domains and distinct between real sites.
                code = first.get("client_code") or (
                    "fp", d.get("totalCount"), first.get("apply_url"))
                with _lock:
                    owner = _csx_claimed.setdefault(code, origin)
                if owner != origin:
                    return []
            for j in jobs:
                jd = (j or {}).get("data") if isinstance(j, dict) else None
                if not isinstance(jd, dict):
                    continue
                if jd.get("internal") is True or jd.get("searchable") is False:
                    continue
                apply_url = strip_q(jd.get("apply_url") or "")
                board = tenant_of(apply_url)
                if not board:
                    continue        # no icims-hosted URL -> not our family
                jid = canon(apply_url)[1]
                city = _squash(jd.get("city"))
                state = _squash(jd.get("state"))
                # country_code, not country: the API's `country` is the display
                # name ("United States") while the classic-portal rows carry
                # the ISO-2 the location code gives ("US").  Mixing the two in
                # one source's country column breaks every downstream US/remote
                # filter for exactly the boards this route exists to rescue.
                country = (_squash(jd.get("country_code")).upper()
                           or _squash(jd.get("country")))
                loc = (_squash(jd.get("full_location"))
                       or _squash(jd.get("location_name"))
                       or ", ".join(x for x in (city, state,
                                                _squash(jd.get("country")))
                                    if x))
                mode, remote = work_mode_from("", city, loc)
                posted = cc.ymd(iso=(jd.get("posted_date") or
                                     jd.get("create_date") or "")[:10])
                posted = ledger_posted("%s:%s" % (board, jid), posted, "api")
                smin = jd.get("salary_min_value") or ""
                smax = jd.get("salary_max_value") or ""
                # The API publishes an amount with no unit.  Anything at or
                # above 1,000 can only be an annual figure and anything below
                # it can only be a rate, so the period is inferred from the
                # magnitude rather than assumed — an hourly rate labelled
                # "year" would be off by three orders of magnitude.
                per = ""
                if smin or smax:
                    per = "year" if max(float(smin or 0),
                                        float(smax or 0)) >= 1000 else "hour"
                rows.append(cc.row(
                    source=SOURCE, source_type="ats",
                    company=(_squash(jd.get("hiring_organization"))
                             or _squash(jd.get("brand"))
                             or board.replace("-", " ").title()),
                    title=_squash(jd.get("title")),
                    employment_type=cc.classify_any(jd.get("employment_type"),
                                                    jd.get("title")),
                    work_mode=mode, location=loc, city=city, state=state,
                    country=country, remote=remote,
                    salary_min=smin or "", salary_max=smax or "",
                    salary_period=per,
                    posted=posted, apply_url=apply_url, job_id=jid,
                    board=board, board_url=origin + "/jobs"))
            total = d.get("totalCount")
            if isinstance(total, int) and pg * 100 >= total:
                break
        return rows
    finally:
        s.close()


# ---- driver ----------------------------------------------------------------
def load_tenants():
    with open(TENANTS, newline="", encoding="utf-8") as f:
        return [r["tenant"].strip() for r in csv.DictReader(f)
                if (r.get("tenant") or "").strip()]


def run_tenant(tenant):
    """Portal phase for one tenant.  Rows are emitted only after every page
    parsed, so a mid-board failure leaves the tenant pending with nothing
    written for it (retry cannot double-append)."""
    with _gate["icims"]:
        try:
            kind, payload = fetch_tenant(tenant)
        except Transient as e:
            with _lock:
                _counts["transient"] += 1
            return ("transient", str(e)[:160])
        except Exception as e:            # noqa: BLE001 - keep the pool alive
            with _lock:
                _counts["transient"] += 1
            return ("transient", "%s: %s" % (type(e).__name__, str(e)[:120]))
    if kind == "dead":
        with _lock:
            _state[tenant] = {"status": "dead", "why": "http404", "at": TODAY}
            _counts["dead"] += 1
        return ("dead", 0)
    if kind == "migrated":
        with _lock:
            _counts["migrated"] += 1
        return ("migrated", payload)
    n = sum(1 for r in payload if emit(r))
    with _lock:
        _state[tenant] = {"status": "done", "rows": n, "at": TODAY}
        _counts["live" if kind == "live" else "empty"] += 1
    return (kind, n)


def main():
    global _writer, _state, _ledger
    _state = cc.load_state(STATE)
    _ledger = cc.load_state(LEDGER)
    tenants = load_tenants()
    todo = [t for t in tenants
            if (_state.get(t) or {}).get("status") not in
            ("done", "dead", "migrated")]
    settled = len(tenants) - len(todo)
    if LIMIT and len(todo) > LIMIT:
        step = max(1, len(todo) // LIMIT)
        todo = todo[::step][:LIMIT]
        print("ICIMS_LIMIT: sampling %d tenants, artifacts prefixed %r"
              % (len(todo), PFX), flush=True)
    print("icims: %d tenants, %d already settled, %d to fetch"
          % (len(tenants), settled, len(todo)), flush=True)
    if not todo:
        print("nothing to do", flush=True)
        return
    _writer = cc.Writer(OUT)
    random.shuffle(todo)
    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None

    # --- pass 1: probe + classic portal ------------------------------------
    migrated = {}       # tenant -> csx origin
    transient = []
    done = 0
    stopped = False
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(run_tenant, t): t for t in todo}
        for fut in cf.as_completed(futs):
            t = futs[fut]
            kind, payload = fut.result()
            if kind == "migrated":
                migrated[t] = csx_origin(payload)
            elif kind == "transient":
                transient.append((t, payload))
            done += 1
            if done % 40 == 0:
                with _lock:
                    cc.save_state(STATE, _state)
                    cc.save_state(LEDGER, _ledger)
                print("  %d/%d tenants  %s" % (done, len(todo), _counts),
                      flush=True)
            if deadline and time.time() > deadline:
                stopped = True
                for f in futs:
                    f.cancel()
                break
    cc.save_state(STATE, _state)

    # --- pass 2: retired portals via their Career Site API ------------------
    origins = {}
    for t, o in migrated.items():
        if o:
            origins.setdefault(o, []).append(t)
    if origins:
        print("  %d retired tenants across %d career sites"
              % (len(migrated), len(origins)), flush=True)
        with cf.ThreadPoolExecutor(max_workers=6) as ex:
            futs = {ex.submit(fetch_csx, o): o for o in origins}
            for fut in cf.as_completed(futs):
                o = futs[fut]
                try:
                    rows = fut.result()
                except Exception as e:            # noqa: BLE001
                    # Leave those tenants PENDING: the portal is gone but the
                    # API failing is transient, not proof of anything.
                    transient.append((o, "%s: %s" % (type(e).__name__,
                                                     str(e)[:120])))
                    continue
                per = {}
                for r in rows:
                    if emit(r):
                        per[r["board"]] = per.get(r["board"], 0) + 1
                with _lock:
                    for t in origins[o]:
                        # Never clobber a real count with 0: this origin may be
                        # a duplicate vanity domain whose feed a sibling origin
                        # already walked, and those tenants ARE covered — their
                        # rows just arrived under the sibling's future.
                        if (_state.get(t) or {}).get("rows"):
                            continue
                        _state[t] = {"status": "migrated", "csx": o,
                                     "rows": per.get(t, 0), "at": TODAY}
                    for b, n in per.items():
                        if not (_state.get(b) or {}).get("rows"):
                            # A career site often carries sibling tenants that
                            # were never in the seed.  Free coverage, and the
                            # board id comes from the apply URL, not a guess.
                            _state[b] = {"status": "migrated", "csx": o,
                                         "rows": n, "at": TODAY,
                                         "via": "sibling"}
                dup = "" if rows else "  (duplicate feed — walked elsewhere)"
                print("    %s -> %d rows across %d boards%s"
                      % (o, sum(per.values()), len(per), dup), flush=True)

    cc.save_state(STATE, _state)
    cc.save_state(LEDGER, _ledger)
    _writer.close()

    boards = len([k for k, v in _state.items() if (v or {}).get("rows")])
    print("\nDONE icims: %d rows -> %s" % (_counts["rows"], OUT), flush=True)
    print("  live=%d empty=%d dead=%d migrated=%d transient=%d boards=%d"
          % (_counts["live"], _counts["empty"], _counts["dead"],
             _counts["migrated"], _counts["transient"], boards), flush=True)
    if _n429:
        print("  rate-limit backoffs: %s" % _n429, flush=True)
    if transient:
        for t, why in transient[:15]:
            print("  PENDING %s: %s" % (t, why), flush=True)
        raise SystemExit("%d tenants unreached and still pending — re-run"
                         % len(transient))
    if stopped:
        print("BUDGET REACHED: %d/%d tenants; the rest stay pending"
              % (done, len(todo)), flush=True)


if __name__ == "__main__":
    main()
