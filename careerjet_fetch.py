#!/usr/bin/env python3
"""
careerjet_fetch.py — harvest US jobs from the Careerjet public search API.

  GET http://public.api.careerjet.net/search
      ?affid=&user_ip=&user_agent=&url=&locale_code=en_US
      &keywords=&location=&contracttype=&contractperiod=&sort=&page=&pagesize=99
  REQUIRED HEADER: Referer (any URL).  Without it: HTTP 403
      {"error":"Undeclared referrer...","type":"ERROR"}
  REQUIRED PARAMS: user_ip + user_agent, or {"error":"missing param ..."}.
  affid is NOT validated — a made-up 20-char id returns real results.

en_US carries ~2.4M jobs but a single query can only be paged ~1,100 deep, so
this fetcher is a PARTITION SWEEP, not a crawl.  keywords="remote" is the spine
(215,654 hits measured 2026-07-29) and it is carved by location x contracttype x
contractperiod x sort into windows that each fit under the depth ceiling.

--------------------------------------------------------------------------
TRAPS MEASURED ON THIS EXACT API 2026-07-29 — read before changing anything
--------------------------------------------------------------------------
(1) THE `url` FIELD IS RANDOMIZED PER RESPONSE.  Not per job — per response.
    The identical query fired twice ~1s apart returned 99/99 jobs matching on
    (company,title,locations,date) and 0/99 matching on `url`:
        urlA=https://jobviewtrack.com/v2/jMxPStiIPL7hjfaTJg_h_nHqdmG0ldzR...
        urlB=https://jobviewtrack.com/v2/VKWDUONQD0QYM5QunASiLRuGclhGWbmJ...
        same job, same description, same date.
    Consequences, both severe:
      a. `url` CANNOT be a dedupe key.  A fetcher that dedupes on it collects
         the same job hundreds of times and reports 100% distinct apply_urls
         while doing it — the standard "distinct apply_urls ~= row count means
         it's fine" sanity check is INVERTED here and will pass on garbage.
         This module dedupes on JOB_KEY = sha1(company|title|locations|date).
      b. Every earlier url-based measurement of this API is void, including the
         spec's pagination numbers, which is why they disagree with (2).
(2) THE DEPTH CEILING IS ~1,089-1,188, and `pages` is garbage (it reports
    2179 / 119986 / 47995 depending on pagesize).  Measured with content keys:
      sort=relevance  pages 1-11 all new, page 12 adds 3, then ~3/page forever
      sort=date       pages 1-12 all new (1,177), then odd pages add ~90 and
                      even pages are a byte-repeat of the page before
    So 12 pages x 99 = the honest window.  Past that a page is either an exact
    repeat of its predecessor or ~50% churn — half efficiency, so the sweep
    spends its budget on FRESH partitions instead of deep pages.
(3) `type` IS THE BODY DISCRIMINATOR, NOT THE STATUS CODE.  An unresolvable or
    ambiguous location answers HTTP 200 with
        {"type":"LOCATIONS","solveLocations":[{"name":"Springfield, Missouri"},
         {"name":"Springfield, Illinois"},...]}   # location='Springfield'
        {"type":"LOCATIONS","solveLocations":[]}  # location='zzzzqqq'
    and no `jobs` key at all.  `d.get("jobs") or []` reads both as "this
    partition has no jobs".  Every location here is therefore written as
    "City, ST" so it resolves, and a LOCATIONS answer is logged as a config
    bug, never as an empty partition.
(4) pagesize=100 SILENTLY FALLS BACK TO 20 rows (99 -> 99, 50 -> 50,
    100 -> 20).  99 is the real maximum.
(5) `date` IS CAREERJET'S INDEX TIMESTAMP, NOT THE EMPLOYER'S POSTING DATE.
    On sort=date all 99 rows of a page share one timestamp equal to the second
    the request was served.  Across a relevance query it spreads over 11
    distinct days but skews hard to today (275 of 380 rows = today).  It is
    emitted as `posted` because it is the only date on offer and it is a valid
    freshness proxy, but it is NOT an origination date and rows will look
    newer than they are to any downstream age cutoff.
(6) contractperiod only accepts f and p.  contractperiod=s and =c are IGNORED
    and echo the unfiltered hit count, so they are not extra partitions.
(7) location='' == location='usa' == location='United States' (215,654).  An
    empty location is not a no-op filter, it is the whole country.

--------------------------------------------------------------------------
WHAT THIS SOURCE IS ACTUALLY WORTH — measured on the 154,219-row sweep
--------------------------------------------------------------------------
Row counts here badly overstate the real inventory.  On the full sweep:
    154,219 rows
    127,737 distinct (company,title,location)  -> 26,482 rows (17%) are just
            a re-index of a posting already held under a newer `date`
     62,592 distinct (company,title)           -> the same role is blasted
            across dozens of metros
     10,569 distinct companies
And the remote slice, which is the whole point of this pipe, is worse:
     65,588 rows flagged remote
      9,212 distinct (company,title)
      2,123 distinct companies
     76% of remote rows come from TEN companies, and the top three —
     Babki (14,431), Focus Group Panel (12,471), Maxion Research (11,213) —
     are "paid product tester / research panel" gig mills, 58% of the remote
     slice on their own.  QuickMD adds 4,879.
So treat the honest yield as ~9k distinct remote roles, not 65k, and expect a
company-level blocklist to be worth more here than any extra crawling.

apply_url: set to the `url` tracking redirect because there is nothing else —
jobviewtrack.com -> 302 -> careerjet.com/clk/<hash>.html -> 302 ->
careerjet.com/jobad/us<hash>.  It NEVER reaches the employer ATS, so these rows
cannot be matched against the pipeline's canonical-ATS dedupe key, AND because
of trap (1) the URL is not even stable for the same job.  Downstream must
dedupe careerjet rows on `job_id`.

Resumable: careerjet_fetch_state.json records each partition's outcome and the
seen-key set is rebuilt from contract_norm/careerjet.jsonl on start, so a
re-run continues the sweep and cannot re-emit a job already written.

env:
  CJ_DEADLINE   wall-clock seconds for the sweep (default 1200)
  CJ_RPS        global request rate (default 5.0; 40 reqs at 7.4 rps -> 0x 429)
  CJ_WORKERS    threads (default 4)
  CJ_RESET      1 to ignore the state file and re-sweep from scratch
"""
import hashlib
import heapq
import itertools
import os
import re
import sys
import threading
import time
from collections import Counter
from email.utils import parsedate_to_datetime

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(cc.NORM_DIR, "careerjet.jsonl")
STATE = os.path.join(BASE, "careerjet_fetch_state.json")
# Days before a completed partition is re-walked.  See the expiry note in
# main().  0 disables expiry and restores the old permanent-done behaviour.
TTL_DAYS = float(os.environ.get("CJ_TTL_DAYS") or 3)

API = "http://public.api.careerjet.net/search"
LOCALE = "en_US"
PAGESIZE = 99          # trap (4): 100 silently degrades to 20
MAX_PAGES = 12         # trap (2): the honest depth ceiling
KEYWORDS = "remote"    # the spine of the sweep

DEADLINE_S = float(os.environ.get("CJ_DEADLINE") or 1200)
RPS = float(os.environ.get("CJ_RPS") or 5.0)
WORKERS = int(os.environ.get("CJ_WORKERS") or 4)
TIMEOUT = 30
TRIES = 4

# affid is NOT validated at all — affid=x, affid=abcdef and a 20-char made-up
# id all return identical results (measured 2026-07-29).  Swap in a real one
# from https://www.careerjet.com/partners/api/ if that ever changes.
AFFID = os.environ.get("CJ_AFFID") or "ffffffffffffffffffff"
#
# TRAP (8) — THE REFERER **HOST** IS AN ALLOWLIST, AND IT IS THE WHOLE PIPE.
# A missing Referer gives 403 "Undeclared referrer".  A Referer whose host is
# not grandfathered gives HTTP 401 + {"type":"ERROR","error":"The legacy Job
# Search API is only accessible for authenticated legacy users."}.  Measured
# host by host with an identical made-up affid:
#     http://example.com/... , https://example.com , http://www.example.com/x,
#     http://localhost/                                            -> 200 JOBS
#     example.org, example.net, sub.example.com, 127.0.0.1,
#     test.com, google.com, careerjet.com (!)                      -> 401
# So Careerjet is sunsetting this API and only a legacy referrer allowlist
# still passes unauthenticated.  Pointing the Referer at careerjet.com — the
# obvious "polite" choice — is one of the hosts that FAILS.  If this pipe ever
# starts returning 401 everywhere, the allowlist is gone and the fix is a real
# affiliate account, not a different UA or a slower crawl.
REFERER = os.environ.get("CJ_REFERER") or "http://example.com/jobs"

# ---- partition axes --------------------------------------------------------
# contracttype x contractperiod is a genuine DISJOINT partition of the index:
# measured on MA the 8 non-empty cells sum to 7,067 against 7,085 unfiltered.
CT = {"p": "permanent", "c": "contract", "t": "temporary",
      "i": "internship", "v": "voluntary"}
CP = {"f": "full time", "p": "part time"}
CELLS = [(ct, cp) for ct in CT for cp in CP]
# metros get the 4 cells that hold ~98% of inventory (MA: 5468+1300+138+32
# of 7085) instead of all 10, so the budget is not burnt on empty cells.
METRO_CELLS = [("p", "f"), ("p", "p"), ("c", "f"), ("t", "f")]

STATES = [
    "Alabama", "Alaska", "Arizona", "Arkansas", "California", "Colorado",
    "Connecticut", "Delaware", "District of Columbia", "Florida", "Georgia",
    "Hawaii", "Idaho", "Illinois", "Indiana", "Iowa", "Kansas", "Kentucky",
    "Louisiana", "Maine", "Maryland", "Massachusetts", "Michigan", "Minnesota",
    "Mississippi", "Missouri", "Montana", "Nebraska", "Nevada",
    "New Hampshire", "New Jersey", "New Mexico", "New York", "North Carolina",
    "North Dakota", "Ohio", "Oklahoma", "Oregon", "Pennsylvania",
    "Rhode Island", "South Carolina", "South Dakota", "Tennessee", "Texas",
    "Utah", "Vermont", "Virginia", "Washington", "West Virginia", "Wisconsin",
    "Wyoming"]

# Always "City, ST" — trap (3): a bare "Springfield" answers type=LOCATIONS
# with three suggestions and zero jobs.
METROS = [
    "New York, NY", "Brooklyn, NY", "Buffalo, NY", "Rochester, NY",
    "Albany, NY", "Syracuse, NY", "White Plains, NY",
    "Los Angeles, CA", "San Diego, CA", "San Jose, CA", "San Francisco, CA",
    "Oakland, CA", "Sacramento, CA", "Fresno, CA", "Long Beach, CA",
    "Irvine, CA", "Anaheim, CA", "Riverside, CA", "Bakersfield, CA",
    "Santa Clara, CA", "Pasadena, CA", "San Bernardino, CA",
    "Chicago, IL", "Naperville, IL", "Springfield, IL", "Peoria, IL",
    "Houston, TX", "Dallas, TX", "Austin, TX", "San Antonio, TX",
    "Fort Worth, TX", "El Paso, TX", "Plano, TX", "Arlington, TX",
    "Corpus Christi, TX", "Lubbock, TX", "Irving, TX",
    "Phoenix, AZ", "Tucson, AZ", "Mesa, AZ", "Scottsdale, AZ",
    "Philadelphia, PA", "Pittsburgh, PA", "Allentown, PA", "Harrisburg, PA",
    "Jacksonville, FL", "Miami, FL", "Tampa, FL", "Orlando, FL",
    "St Petersburg, FL", "Fort Lauderdale, FL", "Tallahassee, FL",
    "Columbus, OH", "Cleveland, OH", "Cincinnati, OH", "Toledo, OH",
    "Dayton, OH", "Akron, OH",
    "Indianapolis, IN", "Fort Wayne, IN",
    "Charlotte, NC", "Raleigh, NC", "Greensboro, NC", "Durham, NC",
    "Winston Salem, NC",
    "Seattle, WA", "Spokane, WA", "Tacoma, WA", "Bellevue, WA",
    "Denver, CO", "Colorado Springs, CO", "Aurora, CO", "Boulder, CO",
    "Boston, MA", "Worcester, MA", "Springfield, MA", "Cambridge, MA",
    "Lowell, MA", "Quincy, MA",
    "Detroit, MI", "Grand Rapids, MI", "Ann Arbor, MI", "Lansing, MI",
    "Nashville, TN", "Memphis, TN", "Knoxville, TN", "Chattanooga, TN",
    "Portland, OR", "Eugene, OR", "Salem, OR",
    "Las Vegas, NV", "Reno, NV", "Henderson, NV",
    "Baltimore, MD", "Rockville, MD", "Columbia, MD", "Silver Spring, MD",
    "Milwaukee, WI", "Madison, WI", "Green Bay, WI",
    "Albuquerque, NM", "Santa Fe, NM",
    "Kansas City, MO", "St Louis, MO", "Springfield, MO",
    "Atlanta, GA", "Savannah, GA", "Augusta, GA", "Columbus, GA",
    "Omaha, NE", "Lincoln, NE",
    "Minneapolis, MN", "St Paul, MN", "Rochester, MN",
    "New Orleans, LA", "Baton Rouge, LA", "Shreveport, LA",
    "Honolulu, HI", "Anchorage, AK",
    "Louisville, KY", "Lexington, KY",
    "Oklahoma City, OK", "Tulsa, OK",
    "Salt Lake City, UT", "Provo, UT",
    "Richmond, VA", "Virginia Beach, VA", "Arlington, VA", "Norfolk, VA",
    "Alexandria, VA", "Reston, VA",
    "Newark, NJ", "Jersey City, NJ", "Trenton, NJ", "Princeton, NJ",
    "Hartford, CT", "New Haven, CT", "Stamford, CT",
    "Providence, RI", "Manchester, NH", "Portland, ME", "Burlington, VT",
    "Wilmington, DE", "Charleston, SC", "Columbia, SC", "Greenville, SC",
    "Birmingham, AL", "Huntsville, AL", "Montgomery, AL", "Mobile, AL",
    "Little Rock, AR", "Jackson, MS", "Boise, ID", "Billings, MT",
    "Sioux Falls, SD", "Fargo, ND", "Cheyenne, WY", "Charleston, WV",
    "Des Moines, IA", "Cedar Rapids, IA", "Wichita, KS", "Topeka, KS",
    "Washington, DC",
]

# ---- location parsing ------------------------------------------------------
ABBR = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "DC", "FL", "GA", "HI",
    "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN",
    "MS", "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH",
    "OK", "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA",
    "WV", "WI", "WY", "PR", "VI", "GU", "AS", "MP"}
NAME2AB = {
    "alabama": "AL", "alaska": "AK", "arizona": "AZ", "arkansas": "AR",
    "california": "CA", "colorado": "CO", "connecticut": "CT",
    "delaware": "DE", "district of columbia": "DC", "florida": "FL",
    "georgia": "GA", "hawaii": "HI", "idaho": "ID", "illinois": "IL",
    "indiana": "IN", "iowa": "IA", "kansas": "KS", "kentucky": "KY",
    "louisiana": "LA", "maine": "ME", "maryland": "MD",
    "massachusetts": "MA", "michigan": "MI", "minnesota": "MN",
    "mississippi": "MS", "missouri": "MO", "montana": "MT",
    "nebraska": "NE", "nevada": "NV", "new hampshire": "NH",
    "new jersey": "NJ", "new mexico": "NM", "new york": "NY",
    "north carolina": "NC", "north dakota": "ND", "ohio": "OH",
    "oklahoma": "OK", "oregon": "OR", "pennsylvania": "PA",
    "rhode island": "RI", "south carolina": "SC", "south dakota": "SD",
    "tennessee": "TN", "texas": "TX", "utah": "UT", "vermont": "VT",
    "virginia": "VA", "washington": "WA", "west virginia": "WV",
    "wisconsin": "WI", "wyoming": "WY", "puerto rico": "PR"}
_ZIP = re.compile(r"\s+\d{5}(?:-\d{4})?$")
_NON_CITY = re.compile(r"^(remote|usa|us|united states|nationwide|anywhere|"
                       r"work from home|multiple locations|various)$", re.I)


def _state_of(seg):
    """First state abbrev or state name inside one comma-segment, or ''.
    Segments are not always clean: 'GA - Alabama', 'MA 02108', 'NY (Remote)'
    and 'CA/NV' all occur, so a whole-segment equality test drops the state."""
    seg = (seg or "").replace(".", "").strip()
    if not seg:
        return ""
    low = seg.lower()
    best = None                      # (position, abbrev)
    for name, ab in NAME2AB.items():
        m = re.search(r"\b%s\b" % re.escape(name), low)
        if m and (best is None or m.start() < best[0]):
            best = (m.start(), ab)
    for m in re.finditer(r"\b[A-Za-z]{2}\b", seg):
        tok = m.group()
        # EARLIEST match wins, so 'GA - Alabama' is GA and not AL
        if tok.upper() in ABBR and tok.isupper():
            if best is None or m.start() < best[0]:
                best = (m.start(), tok.upper())
    return best[1] if best else ""


def parse_loc(s):
    """'Framingham, MA' -> ('Framingham','MA','US').  'Texas' -> ('','TX','US').
    'Atlanta, GA - Alabama' -> ('Atlanta','GA','US')."""
    s = (s or "").strip()
    if not s:
        return "", "", "US"
    txt = _ZIP.sub("", s).strip().rstrip(",")
    parts = [p.strip() for p in txt.split(",") if p.strip()]
    if not parts:
        return "", "", "US"
    if len(parts) == 1:
        st = _state_of(parts[0])
        # a lone segment that IS the state carries no city
        city = "" if (st and not re.sub(
            r"\b[A-Za-z]{2}\b|[^A-Za-z ]", "", parts[0]).strip()
            or parts[0].lower() in NAME2AB) else parts[0]
        return ("" if _NON_CITY.match(city or "") else city), st, "US"
    city = parts[0]
    st = ""
    for seg in parts[1:]:
        st = _state_of(seg)
        if st:
            break
    if not st:
        st = _state_of(parts[0])
        if st and parts[0].lower() in NAME2AB:
            city = ""
    if _NON_CITY.match(city or ""):
        city = ""
    return city, st, "US"


# ---- remote inference ------------------------------------------------------
# Trap: the API exposes NO structured remote/work-mode field (the `site` field
# is always "" too), so this is the one place inference is unavoidable.  The
# title/location signal is trusted first, the description only on an explicit
# phrase — "remote" alone in a description also appears in "remote desktop
# support" and "our remote offices".
_RM_STRONG = re.compile(
    r"\bremote\b|\bremotely\b|work\s*from\s*home|\bwfh\b|telecommut|telework|"
    r"home[\s-]based|virtual (?:position|role|office)|\bvirtual\b.*\brole\b",
    re.I)
# "remote site/location/camp" means FAR AWAY AND ON-SITE (rigs, mines, clinics)
_RM_FALSE = re.compile(
    r"remote\s+(?:site|sites|location|locations|area|areas|camp|village|"
    r"island|desktop|access|monitoring|sensing|control|start|support\s+tool)",
    re.I)
_RM_DESC = re.compile(
    r"fully\s+remote|100%\s*remote|work\s+from\s+home|work\s+remotely|"
    r"remote\s+(?:position|role|opportunity|job|work|first|based)|"
    r"remote[\s-]friendly|telecommut|\bwfh\b|home[\s-]based|"
    r"this\s+is\s+a\s+remote", re.I)
_HYBRID = re.compile(r"\bhybrid\b", re.I)


def infer_mode(title, locations, desc):
    head = f"{title} {locations}"
    if _RM_STRONG.search(head) and not _RM_FALSE.search(head):
        return "Remote", True
    if _RM_DESC.search(desc or "") and not _RM_FALSE.search(desc or ""):
        return "Remote", True
    if _HYBRID.search(head) or _HYBRID.search(desc or ""):
        return "Hybrid", False
    return "", False


# ---- salary parsing --------------------------------------------------------
# measured formats: '$100000 - 150000 per year', '$19.83 - 33.55 per hour'
_SAL = re.compile(
    r"(?P<cur>[$£€])?\s*(?P<a>\d[\d,]*(?:\.\d+)?)"
    r"(?:\s*(?:-|–|to)\s*(?P<b>\d[\d,]*(?:\.\d+)?))?"
    r"(?:\s*per\s+(?P<per>year|annum|hour|month|week|day))?", re.I)
_CUR = {"$": "USD", "£": "GBP", "€": "EUR"}
_PER = {"year": "Year", "annum": "Year", "hour": "Hour", "month": "Month",
        "week": "Week", "day": "Day"}


def parse_salary(s):
    if not s:
        return "", "", "", ""
    m = _SAL.search(s)
    if not m:
        return "", "", "", ""

    def num(v):
        if not v:
            return ""
        try:
            f = float(v.replace(",", ""))
        except ValueError:
            return ""
        return int(f) if f == int(f) else round(f, 2)
    lo = num(m.group("a"))
    hi = num(m.group("b"))
    if lo == "" and hi == "":
        return "", "", "", ""
    return (lo, hi, _CUR.get(m.group("cur") or "", "USD"),
            _PER.get((m.group("per") or "").lower(), ""))


def job_key(j):
    """Stable identity.  Trap (1): `url` is randomized per RESPONSE, so the
    only thing that survives across two calls is the content tuple."""
    raw = "|".join(str(j.get(k) or "") for k in
                   ("company", "title", "locations", "date"))
    return hashlib.sha1(raw.encode("utf-8", "replace")).hexdigest()[:16]


def posted_of(j):
    """`date` is RFC-1123 ('Wed, 29 Jul 2026 03:47:01 GMT').  See trap (5) —
    this is Careerjet's index time, not the employer's posting date."""
    d = j.get("date")
    if not d:
        return ""
    try:
        return cc.ymd(ts=parsedate_to_datetime(d).timestamp())
    except (TypeError, ValueError, OverflowError):
        return ""


# ---- rate limiting ---------------------------------------------------------
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]


def throttle():
    while True:
        with _rate_lock:
            now = time.time()
            if now < _pause_until[0]:
                wait = _pause_until[0] - now
            else:
                wait = _next_t[0] - now
                if wait <= 0:
                    _next_t[0] = max(_next_t[0] + _interval[0], now)
                    return
        time.sleep(min(wait, 3.0))


def backoff(sec, grow=True):
    with _rate_lock:
        _pause_until[0] = max(_pause_until[0], time.time() + sec)
        if grow:
            _interval[0] = min(_interval[0] * 1.5, 2.0)


# ---- one API call ----------------------------------------------------------
# Returns (jobs, hits, status).  status is one of:
#   ok         type=JOBS, real rows
#   empty      type=JOBS, hits==0 — a genuinely empty partition
#   locations  type=LOCATIONS — the location string did not resolve (trap 3);
#              a CONFIG bug, never "no jobs"
#   error      type=ERROR / 4xx — the API rejected the call
#   transient  timeouts, 5xx, 429 that outlived the retries — MUST NOT be
#              recorded as an empty partition; the state file keeps the
#              partition open so a re-run retries it
def fetch_page(s, location, ct, cp, sort, page):
    params = {
        "affid": AFFID, "user_ip": "8.8.8.8", "user_agent": cc.UA,
        "url": REFERER, "locale_code": LOCALE, "keywords": KEYWORDS,
        "location": location, "sort": sort, "page": page,
        "pagesize": PAGESIZE}
    if ct:
        params["contracttype"] = ct
    if cp:
        params["contractperiod"] = cp
    last = ""
    for attempt in range(TRIES):
        throttle()
        try:
            r = s.get(API, params=params, timeout=TIMEOUT)
        except requests.RequestException as e:
            last = type(e).__name__
            time.sleep(1.5 * (attempt + 1))
            continue
        if r.status_code in (429, 503, 502, 504):
            backoff(15)
            last = "HTTP %d" % r.status_code
            continue
        # Never trust the status code alone — validate the BODY.
        try:
            d = r.json()
        except ValueError:
            last = "non-json (HTTP %d) %r" % (r.status_code, r.text[:80])
            time.sleep(1.5 * (attempt + 1))
            continue
        typ = d.get("type")
        if typ == "LOCATIONS":
            return [], 0, "locations"
        if typ == "ERROR" or r.status_code >= 400:
            return [], 0, "error:%s" % str(d.get("error"))[:80]
        if typ != "JOBS":
            last = "type=%r" % typ
            time.sleep(1.5 * (attempt + 1))
            continue
        hits = int(d.get("hits") or 0)
        jobs = d.get("jobs") or []
        return jobs, hits, ("ok" if jobs else "empty")
    return [], 0, "transient:%s" % last


# ---- sweep -----------------------------------------------------------------
_lock = threading.Lock()
_seen = set()
_stats = Counter()
_heap = []
_seq = itertools.count()
_heap_lock = threading.Condition()
_inflight = [0]
_stop = [False]
_t0 = [0.0]


def push(prio, task):
    with _heap_lock:
        heapq.heappush(_heap, (prio, next(_seq), task))
        _heap_lock.notify()


def pop():
    with _heap_lock:
        while True:
            if _stop[0]:
                return None
            if _heap:
                prio, _, task = heapq.heappop(_heap)
                _inflight[0] += 1
                return (prio, task)
            if _inflight[0] == 0:
                _heap_lock.notify_all()
                return None
            _heap_lock.wait(0.5)


def done_one():
    with _heap_lock:
        _inflight[0] -= 1
        if _inflight[0] == 0 and not _heap:
            _heap_lock.notify_all()


def tkey(t):
    return "%s|%s|%s|%s" % (t["loc"], t["ct"] or "-", t["cp"] or "-", t["sort"])


def run_task(s, prio, task, out, state):
    """Walk one partition to the depth ceiling.  Returns rows written."""
    loc, ct, cp, sort = task["loc"], task["ct"], task["cp"], task["sort"]
    k = tkey(task)
    raw_type = "%s %s" % (CT.get(ct, ""), CP.get(cp, ""))
    written = 0
    hits = 0
    capped = False
    prev_page_keys = None
    stale = 0
    for page in range(1, MAX_PAGES + 1):
        if time.time() - _t0[0] > DEADLINE_S:
            with _lock:
                state[k] = {"status": "deadline", "pages": page - 1}
            return written, False
        jobs, h, st = fetch_page(s, loc, ct, cp, sort, page)
        if page == 1:
            hits = h
        if st.startswith("transient"):
            with _lock:
                _stats["transient"] += 1
                # NOT "empty" — leave the partition open for a re-run.
                state[k] = {"status": "failed", "why": st, "pages": page - 1}
            return written, False
        if st == "locations":
            with _lock:
                _stats["locations"] += 1
                state[k] = {"status": "unresolved_location"}
                print("  ! location %r did not resolve (type=LOCATIONS) — "
                      "config bug, not an empty partition" % loc, flush=True)
            return written, False
        if st.startswith("error"):
            with _lock:
                _stats["error"] += 1
                state[k] = {"status": "error", "why": st}
            return written, False
        if st == "empty":
            break
        pk = {job_key(j) for j in jobs}
        # trap (2): past the ceiling a page is a byte-repeat of its
        # predecessor.  Two stale pages in a row ends the partition.
        if pk == prev_page_keys:
            stale += 1
            if stale >= 1:
                capped = True
                break
        else:
            stale = 0
        prev_page_keys = pk
        new_rows = []
        with _lock:
            for j in jobs:
                jk = job_key(j)
                if jk in _seen:
                    continue
                _seen.add(jk)
                new_rows.append((jk, j))
        for jk, j in new_rows:
            locs = j.get("locations") or ""
            title = j.get("title") or ""
            desc = j.get("description") or ""
            city, st2, country = parse_loc(locs)
            mode, is_rem = infer_mode(title, locs, desc)
            smin, smax, scur, sper = parse_salary(j.get("salary"))
            with _lock:
                out.write(cc.row(
                    source="careerjet", source_type="aggregator",
                    company=j.get("company") or "", title=title,
                    # structured contracttype/contractperiod from the
                    # partition, not a guess off the title
                    employment_type=cc.classify_any(raw_type, title),
                    work_mode=mode, remote=is_rem,
                    location=locs, city=city, state=st2, country=country,
                    salary_min=smin, salary_max=smax,
                    salary_currency=scur, salary_period=sper,
                    posted=posted_of(j),
                    # trap (1): randomized tracking redirect, never the
                    # employer ATS.  Dedupe downstream on job_id.
                    apply_url=j.get("url") or "",
                    job_id="cj_%s" % jk, board="careerjet",
                    board_url="https://www.careerjet.com"))
                written += 1
        if len(jobs) < PAGESIZE or (hits and page * PAGESIZE >= hits):
            break
        if page == MAX_PAGES:
            capped = True
    with _lock:
        _stats["partitions"] += 1
        _stats["requests_pages"] += 1
        state[k] = {"status": "done", "ts": time.time(),
                    "hits": hits, "wrote": written,
                    "capped": capped}
    return written, capped


def worker(out, state):
    s = requests.Session()
    s.headers.update({"User-Agent": cc.UA, "Accept": "application/json",
                      "Referer": REFERER})   # trap (3): Referer or 403
    try:
        while True:
            if time.time() - _t0[0] > DEADLINE_S:
                with _heap_lock:
                    _stop[0] = True
                    _heap_lock.notify_all()
                return
            got = pop()
            if got is None:
                return
            prio, task = got
            try:
                n, capped = run_task(s, prio, task, out, state)
                # A capped partition still has inventory behind the ceiling.
                # Re-query it under a different sort: the three sorts were
                # measured 96% disjoint (3 pages each -> 853 distinct of 891).
                if capped and task["sort"] == "date":
                    for so in ("relevance", "salary"):
                        nt = dict(task, sort=so)
                        if tkey(nt) not in state:
                            push(prio + 10, nt)
            except Exception as e:                       # noqa: BLE001
                with _lock:
                    _stats["crash"] += 1
                    print("  ! %s: %s: %s" % (tkey(task), type(e).__name__, e),
                          flush=True)
            finally:
                done_one()
    finally:
        s.close()


def load_seen():
    """Rebuild the dedupe set from what is already on disk so a re-run adds
    only new jobs instead of appending the whole sweep again."""
    n = 0
    if os.path.exists(OUT):
        import json
        with open(OUT, encoding="utf-8") as f:
            for line in f:
                try:
                    jid = json.loads(line).get("job_id") or ""
                except ValueError:
                    continue
                if jid.startswith("cj_"):
                    _seen.add(jid[3:])
                    n += 1
    return n


def preflight():
    """One live call before queueing 1,100 partitions.  Without this a dead
    referrer allowlist (trap 8) silently writes 'error' into every partition
    and reports a clean run with zero rows."""
    s = requests.Session()
    s.headers.update({"User-Agent": cc.UA, "Accept": "application/json",
                      "Referer": REFERER})
    try:
        jobs, hits, st = fetch_page(s, "Massachusetts", "p", "f", "date", 1)
    finally:
        s.close()
    if st != "ok":
        print("PREFLIGHT FAILED: %s" % st, flush=True)
        print("  Referer=%r  affid=%r" % (REFERER, AFFID), flush=True)
        if "legacy" in st:
            print("  -> the referrer allowlist (trap 8) rejected this host. "
                  "Try CJ_REFERER=http://example.com/jobs or "
                  "http://localhost/, or register a real affiliate id at "
                  "https://www.careerjet.com/partners/api/", flush=True)
        return False
    print("  preflight ok: %d rows, hits=%d, sample=%r"
          % (len(jobs), hits, (jobs[0].get("title") if jobs else None)),
          flush=True)
    return True


def main():
    _t0[0] = time.time()
    if not preflight():
        return 2
    state = {} if os.environ.get("CJ_RESET") == "1" else cc.load_state(STATE)
    prior = load_seen()
    print("careerjet_fetch: %d rows already on disk, %d partitions in state"
          % (prior, len(state)), flush=True)

    # priority 0: 51 states x 10 disjoint contracttype/contractperiod cells
    # priority 1: ~160 metros x the 4 cells holding ~98% of inventory
    # priority 10/11 (pushed at runtime): sort variants of capped partitions
    n_q = 0
    for prio, locs, cells in ((0, STATES, CELLS), (1, METROS, METRO_CELLS)):
        for loc in locs:
            for ct, cp in cells:
                t = {"loc": loc, "ct": ct, "cp": cp, "sort": "date"}
                ent = state.get(tkey(t), {})
                st = ent.get("status")
                # EXPIRY.  Without it "done" is permanent and this pipe stops
                # dead: 2026-08-14 through 2026-08-19 every run printed
                # "queued 0 partitions ... new this run: 0" and exited in five
                # seconds while 310,245 rows aged in place.  CJ_RESET=1 is the
                # only other escape and it is all-or-nothing -- a full
                # re-sweep, which is why nobody ran it.  A partition with no
                # timestamp predates this and is treated as expired, which is
                # what re-opens the existing 1,268.
                if st == "done":
                    ts = ent.get("ts")
                    if ts is None or (time.time() - ts) > TTL_DAYS * 86400:
                        st = None
                # "error" is deliberately NOT skipped: the only error seen in
                # the wild is the 401 referrer gate, which is a config fault
                # affecting every partition at once, not a property of this
                # partition.  Only a completed walk or a bad location string
                # is permanent.
                if st in ("done", "unresolved_location"):
                    continue
                push(prio, t)
                n_q += 1
    print("  queued %d partitions | deadline %ds | %.1f rps x %d workers"
          % (n_q, DEADLINE_S, RPS, WORKERS), flush=True)

    out = cc.Writer(OUT)
    threads = [threading.Thread(target=worker, args=(out, state), daemon=True)
               for _ in range(WORKERS)]
    for t in threads:
        t.start()
    last = 0
    while any(t.is_alive() for t in threads):
        time.sleep(10)
        # snapshot under the lock — workers mutate `state` from four threads
        # and json.dump on a live dict raises "changed size during iteration"
        with _lock:
            cur = len(_seen)
            snap = dict(state)
            if cur != last:
                print("  %5ds | %d partitions done | %d distinct jobs"
                      % (time.time() - _t0[0], _stats["partitions"], cur),
                      flush=True)
                last = cur
        cc.save_state(STATE, snap)
    for t in threads:
        t.join()
    out.close()
    cc.save_state(STATE, state)

    ncap = sum(1 for v in state.values() if v.get("capped"))
    nopen = sum(1 for v in state.values()
                if v.get("status") in ("failed", "deadline"))
    print("\nDONE careerjet_fetch in %.0fs" % (time.time() - _t0[0]))
    print("  distinct jobs (incl. prior runs): %d   new this run: %d"
          % (len(_seen), len(_seen) - prior))
    print("  partitions done %d | capped %d | still open %d | "
          "transient %d | unresolved-loc %d | errors %d"
          % (_stats["partitions"], ncap, nopen, _stats["transient"],
             _stats["locations"], _stats["error"]))
    if nopen:
        print("  re-run to resume the %d open partitions" % nopen)
    return 0


if __name__ == "__main__":
    sys.exit(main())
