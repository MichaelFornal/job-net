#!/usr/bin/env python3
"""
neogov_fetch.py — NEOGOV / governmentjobs.com + schooljobs.com fetcher.

US state, county, city, special-district and public-education employers.  100%
US, and disjoint from the usajobs pipe (USAJOBS is federal-only; NEOGOV is
state and local).

ROUTE  (reverse-engineered 2026-08-20)
--------------------------------------
There is no /api/v1/... and no ?format=json.  The board page
/careers/{agency} is a 208KB server-rendered *shell* — grep it for a job id
and you get zero hits, because the list is injected client-side.  The route
that fills it is in /bundles/scripts/AgencyPages/search:

    var searchUrl = AgencyPages.routePrefix + "/home/loadJobsOnMaps" + params;

and AgencyPages.routePrefix is the literal string '/careers' (NOT
'/careers/{agency}' — the agency travels as a query parameter).  So:

    GET https://www.governmentjobs.com/careers/home/loadJobsOnMaps?agency={slug}
    GET https://www.schooljobs.com/careers/home/loadJobsOnMaps?agency={slug}

    -> 200 application/json
       {"success": true, "jobList": [ {...}, ... ], "customLabels": {...}, ...}

THE HEADER IS THE WHOLE ROUTE.  Without `X-Requested-With: XMLHttpRequest`
the SAME url answers 302 -> /Error/NotFound for a perfectly live agency.
Measured 2026-08-20:

    curl .../loadJobsOnMaps?agency=washington                    -> 302  132 B
    curl -H 'X-Requested-With: XMLHttpRequest' ...same url...    -> 200  1.4 MB

Anyone probing this endpoint without the header concludes "no route found".

ONE REQUEST PER BOARD, AND IT IS THE WHOLE BOARD.  loadJobsOnMaps takes no
page parameter (the param builder in search.js appends agency, keyword,
facetlocation, department, category, salary, remoteworkoptionids,
classspecificationscodes, examType, jobType, ispromotional, istransfer — and
no page), so it returns every open posting in one shot.  Verified against the
paged HTML route the site uses for its own list view,
/careers/home/index?agency=washington&page=N (10/page, same XHR header):
page 67 ends with "Showing items 661 - 661" and page 68 is empty, and the map
payload holds exactly 661 distinct IDs.  Independent confirmation, same day.

TWO HOSTS, ONE PLATFORM.  schooljobs.com is the same application on the same
routes (K-12 / community college / university customers).  Some slugs live on
one host only, and governmentjobs.com answers HTTP 301 to the schooljobs.com
twin for a slug that moved (measured: /careers/vcccd -> 301 ->
https://www.schooljobs.com/careers/vcccd).  A 301 is therefore a MOVE, not a
death, and is followed exactly once, cross-host.

JOB IDS ARE PLATFORM-GLOBAL, NOT BOARD-LOCAL.  /jobs/{id} with no agency
resolves any id on either host: schooljobs id 5322029 (bgsu) resolves on
www.governmentjobs.com/jobs/5322029-0/etl-developer.  That is what lets
all_jobs_build key a neogov URL on the id alone (see the wiring note at the
bottom) so the agency-scoped and agency-less forms of one posting collapse.

DEAD / EMPTY / ERROR  (rule 1 and rule 3)
-----------------------------------------
Status codes alone are useless here and 200 is not success:

    live agency,   XHR header      -> 200 + JSON  {"success":true,"jobList":[...]}
    dead agency,   XHR header      -> 302 Location: https://www.governmentjobs.com
    live agency,   no XHR header   -> 302 Location: /Error/NotFound
    moved agency                   -> 301 Location: https://www.schooljobs.com/...
    live agency, 0 open postings   -> 200 + JSON, jobList == []   (REAL, keep)

so the discriminator is the PARSED PAYLOAD: a dict carrying a `jobList` list.
Anything else that arrives with a 200 — an HTML shell, a WAF interstitial, a
truncated body — is deliberately classified TRANSIENT, not dead.  A dead
tenant on this platform never answers 200, so an unexpected 200 shape is
evidence of a problem on our side, and marking a live board dead is the one
error that is permanent.  403 is transient for the same reason (dead tenants
redirect; a 403 is an edge block).  429/5xx/timeouts are transient.

Transient => the board is NOT written to the state file, so it stays PENDING
and the next run retries it.  Only a live fetch or a proven death is recorded.

THROTTLE DECAYS  (rule 2)
-------------------------
slow_down()/speed_up() are the ats_fetch.py pair, per host: 429 multiplies the
interval by 1.5 (capped) and parks the host for 20s; 40 consecutive clean
responses walk it back by 0.9 toward the configured floor.  A one-way ratchet
was measured halving throughput on this repo's other pipes.

apply_url  (rule 4)
-------------------
    https://{host}/careers/{agency}/jobs/{ID}/{JobTitle-slug}

built from the payload's own ID and JobTitle fields.  Verified HTTP 200 with
no redirect, and it is the exact string the site's own list view emits (grep
the /careers/home/index partial).  Not a tracking link, not a redirector,
stable across runs because both components are immutable ids.

SALARY AND DATES — why this family is worth piping
--------------------------------------------------
PostingDate is the EMPLOYER's own posting date, present on 100% of rows in
every board sampled (719/719 washington, 478/478 colorado, 602/602 pabureau,
81/81 vcccd, 85/85 bgsu).  Given that only ~19% of live rows corpus-wide carry
a real employer date, that is the most valuable field here after the salary.
It is MM/DD/YY and is parsed as such; a parse that lands more than a day in
the future or before 2000 is dropped rather than guessed.

SalaryInfo is free text in six measured shapes over 1,965 sampled rows:
    $N - $N Annually (1053)   $N - $N Monthly (294)   $N Hourly (198)
    $N - $N Hourly (155)      $N Annually (124)       $N - $N Biweekly (7)
plus the non-numeric placeholders 'Not Displayed' (59), 'Depends on
Qualifications' (27) and 'See Position Description' (20), which yield no
salary at all rather than a zero.

ClosingDate is emitted as the extra key `closes_on`; for a continuous
recruitment the field is the literal string 'Continuous', which is not a date
and is dropped.  It is parsed with a DIFFERENT ceiling from PostingDate — a
close date is in the future by definition, and sharing the posting-date
ceiling threw away 90% of them (68 of 786 survived the first smoke run).

MULTI-ADDRESS DUPLICATES.  jobList is a MAP payload: one entry per physical
address, so a posting open at two sites appears twice with identical ID,
Classification and Location and only the lat/long differing (washington: 719
entries, 661 distinct IDs).  Rows are deduped by ID inside the board.

SYNDICATED FEDERAL ROWS.  Entries flagged IsTopUSAJob / TopUSAJobUrl are
USAJOBS listings NEOGOV resells into agency boards; they are skipped so they
cannot double the federal pipe or contribute an off-platform apply_url.  (0 of
1,965 rows sampled carried the flag, but the fields exist, so the guard is
cheap insurance.)

robots.txt: www.governmentjobs.com allows the named search-engine agents and
Disallows / for everything else.  The default rate here is deliberately low
(2 rps per host, 6 workers) and one request covers an entire board.

Env knobs
---------
    NEOGOV_KEEP_ALL=0     emit only contingent rows (default 1: every
                          employment type, the post-2026-07-29 convention that
                          workday/icims/oracle/careerjet already follow)
    NEOGOV_BUDGET_MIN     wall-clock budget in minutes (0 = no limit)
    NEOGOV_RPS            per-host requests/second floor      (default 2.0)
    NEOGOV_WORKERS        thread pool size                    (default 6)
    NEOGOV_LIMIT          fetch at most N boards (smoke tests)
    NEOGOV_REFRESH=1      re-fetch boards already recorded live
    NEOGOV_HOST           restrict to one host
    NEOGOV_TENANTS        override the tenant list path

Input:  tenants/neogov.csv        (host,agency,...) — written by
        neogov_discover_boards.py; falls back to neogov_candidates.csv
Output: contract_norm/neogov.jsonl
State:  neogov_fetch_state.json   keyed "{host}/{agency}"
"""
import concurrent.futures as cf
import csv
import json
import os
import random
import re
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
TENANTS = os.environ.get("NEOGOV_TENANTS") or os.path.join(
    BASE, "tenants", "neogov.csv")
CANDIDATES = os.path.join(BASE, "neogov_candidates.csv")
OUT = os.path.join(cc.NORM_DIR, "neogov.jsonl")
STATE = os.path.join(BASE, "neogov_fetch_state.json")

SOURCE = "neogov"
HOSTS = ("www.governmentjobs.com", "www.schooljobs.com")
MAP_URL = "https://%s/careers/home/loadJobsOnMaps?agency=%s"
BOARD_URL = "https://%s/careers/%s"
JOB_URL = "https://%s/careers/%s/jobs/%s/%s"

# The header that IS the route.  Accept must admit JSON; the app negotiates on
# X-Requested-With, not on Accept, but sending both is what a browser does.
XHR = {"X-Requested-With": "XMLHttpRequest",
       "Accept": "application/json, text/javascript, */*; q=0.01",
       "Accept-Language": "en-US,en;q=0.9"}

RPS = float(os.environ.get("NEOGOV_RPS") or 2.0)
WORKERS = int(os.environ.get("NEOGOV_WORKERS") or 6)
TIMEOUT = 60           # a big state board is 1.4 MB of JSON
TRIES = 4
BUDGET_MIN = float(os.environ.get("NEOGOV_BUDGET_MIN") or 0)
LIMIT = int(os.environ.get("NEOGOV_LIMIT") or 0)
REFRESH = os.environ.get("NEOGOV_REFRESH") == "1"
KEEP_ALL = os.environ.get("NEOGOV_KEEP_ALL", "1") != "0"
ONLY_HOST = (os.environ.get("NEOGOV_HOST") or "").strip().lower()

TODAY = datetime.now(timezone.utc).date()
TODAY_S = TODAY.strftime("%Y-%m-%d")
MAX_POSTED = TODAY + timedelta(days=1)
MAX_CLOSES = TODAY + timedelta(days=1095)   # a close date 3y out is a typo
MIN_POSTED = datetime(2000, 1, 1).date()

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = {h: 0.0 for h in HOSTS}
_interval = {h: 1.0 / RPS for h in HOSTS}
_pause_until = {h: 0.0 for h in HOSTS}
_ok_streak = {h: 0 for h in HOSTS}
_n429 = {h: 0 for h in HOSTS}

_state = cc.load_state(STATE)
_stats = {"live": 0, "empty": 0, "dead": 0, "migrated": 0, "transient": 0,
          "rows": 0, "dup_addr": 0, "usajobs_skipped": 0, "not_contingent": 0}


class Transient(Exception):
    """Retryable.  Raised so the board is left PENDING rather than recorded as
    a completed empty board — the distinction rule 3 is about."""


class Dead(Exception):
    """The board is provably gone.  Carries the discriminator that proved it,
    so the state file says WHY and a later reader can tell a real retirement
    from a misclassification."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


class Moved(Exception):
    """HTTP 301 to the sibling host.  Not a death: the same agency board lives
    at the other host and is retried there once."""

    def __init__(self, host):
        super().__init__(host)
        self.host = host


# ---- throttle: backoff that DECAYS (ats_fetch.py pair) ---------------------
def throttle(host):
    while True:
        with _rate_lock:
            now = time.time()
            if now < _pause_until[host]:
                wait = _pause_until[host] - now
            else:
                wait = _next_t[host] - now
                if wait <= 0:
                    _next_t[host] = max(_next_t[host] + _interval[host], now)
                    return
        time.sleep(min(wait, 5) if wait > 0 else 0)


def slow_down(host):
    with _rate_lock:
        _pause_until[host] = time.time() + 20
        _interval[host] = min(_interval[host] * 1.5, 2.0)
        _ok_streak[host] = 0
        _n429[host] += 1


def speed_up(host):
    """Walk the interval back toward the configured rate after a clean run.

    Recovery is slower than the backoff (0.9x per 40 clean responses vs 1.5x
    per 429) so a host that is genuinely refusing still wins the argument, but
    a handful of early 429s cannot pin the run at a crawl for hours the way a
    one-way ratchet does."""
    with _rate_lock:
        n = _ok_streak[host] + 1
        if n < 40:
            _ok_streak[host] = n
            return
        _ok_streak[host] = 0
        floor = 1.0 / RPS
        if _interval[host] > floor:
            _interval[host] = max(_interval[host] * 0.9, floor)


def fetch_board(s, host, agency):
    """-> the parsed loadJobsOnMaps payload.  Raises Dead / Moved / Transient.

    allow_redirects=False on purpose: the redirect IS the death certificate
    and following it turns every dead agency into a healthy-looking 200."""
    url = MAP_URL % (host, agency)
    last = None
    for attempt in range(TRIES):
        throttle(host)
        try:
            r = s.get(url, timeout=TIMEOUT, allow_redirects=False, headers=XHR)
        except requests.RequestException as e:
            last = type(e).__name__
            time.sleep(2 * (attempt + 1))
            continue
        code = r.status_code
        if code == 200:
            ctype = (r.headers.get("Content-Type") or "").lower()
            try:
                d = r.json()
            except ValueError:
                d = None
            # PAYLOAD SHAPE, never the status code.  A 200 that is not the
            # documented envelope is our problem, not proof of a dead board.
            if isinstance(d, dict) and isinstance(d.get("jobList"), list):
                speed_up(host)
                return d
            if isinstance(d, dict) and d.get("success") is False:
                raise Dead("success_false")
            last = "200 but not a jobList envelope (ct=%s, %d bytes)" % (
                ctype[:40], len(r.content or b""))
            time.sleep(2 * (attempt + 1))
            continue
        if code == 301:
            loc = r.headers.get("Location") or ""
            for h in HOSTS:
                if h in loc and h != host:
                    raise Moved(h)
            raise Dead("301:%s" % loc[:80])
        if code in (302, 303, 307, 308):
            # -> https://www.governmentjobs.com  (retired agency) or
            # -> /Error/NotFound.  Either way the slug is not a board here.
            raise Dead("redirect")
        if code in (404, 410):
            raise Dead("http_%d" % code)
        if code in (429, 418, 503):
            slow_down(host)
            last = "HTTP %d" % code
        else:
            # 403/401/5xx: an edge block or an outage.  Dead agencies on this
            # platform redirect, so these are OUR problem -> stay pending.
            last = "HTTP %d" % code
        time.sleep(2 * (attempt + 1))
    raise Transient("%s: %s" % (url, last))


# ---- field parsing ---------------------------------------------------------
_WS = re.compile(r"\s+")


def sq(v):
    return _WS.sub(" ", str(v if v is not None else "")).strip()


_MONEY = r"\$\s*([\d][\d,]*(?:\.\d+)?)"
_SAL_RANGE = re.compile(_MONEY + r"\s*(?:-|–|—|to)\s*" + _MONEY, re.I)
_SAL_ONE = re.compile(_MONEY)
_PERIODS = [
    (re.compile(r"\bannual(?:ly)?\b|\byearly\b|\bper year\b|/\s*yr\b", re.I), "year"),
    (re.compile(r"\bhourly\b|\bper hour\b|/\s*hr\b", re.I), "hour"),
    (re.compile(r"\bmonthly\b|\bper month\b|/\s*mo\b", re.I), "month"),
    (re.compile(r"\bbi[\s-]*weekly\b", re.I), "biweekly"),
    (re.compile(r"\bsemi[\s-]*monthly\b", re.I), "semimonthly"),
    (re.compile(r"\bweekly\b", re.I), "week"),
    (re.compile(r"\bdaily\b|\bper day\b", re.I), "day"),
]


def parse_salary(info):
    """('Not Displayed' | 'Depends on Qualifications' | '$N - $N Annually')
    -> (min, max, currency, period).  Non-numeric placeholders yield nothing
    at all rather than a zero — a 0 floor is worse than a blank."""
    s = sq(info)
    if not s or "$" not in s:
        return "", "", "", ""
    period = ""
    for rx, name in _PERIODS:
        if rx.search(s):
            period = name
            break
    m = _SAL_RANGE.search(s)
    if m:
        lo, hi = m.group(1), m.group(2)
    else:
        m = _SAL_ONE.search(s)
        if not m:
            return "", "", "", ""
        lo = hi = m.group(1)
    try:
        lo_f = float(lo.replace(",", ""))
        hi_f = float(hi.replace(",", ""))
    except ValueError:
        return "", "", "", ""
    if lo_f <= 0 and hi_f <= 0:
        return "", "", "", ""
    if hi_f < lo_f:
        lo_f, hi_f = hi_f, lo_f
    return lo_f, hi_f, "USD", period


def parse_mdy(v, future_ok=False):
    """'08/07/26' -> '2026-08-07'.

    future_ok separates the two date fields and is not cosmetic.  PostingDate
    is a past event, so anything past tomorrow is a data error and is dropped
    rather than guessed.  ClosingDate is a FUTURE event by definition — the
    first cut of this function shared the posting-date ceiling and silently
    discarded 90% of the close dates (68 of 786 survived on the first smoke
    run), which looked exactly like a sparse upstream field."""
    s = sq(v)
    if not s or not re.match(r"^\d{1,2}/\d{1,2}/\d{2,4}$", s):
        return ""          # 'Continuous' — a rolling recruitment, not a date
    fmt = "%m/%d/%Y" if len(s.split("/")[-1]) == 4 else "%m/%d/%y"
    try:
        d = datetime.strptime(s, fmt).date()
    except ValueError:
        return ""
    if d < MIN_POSTED:
        return ""
    if not future_ok and d > MAX_POSTED:
        return ""
    if future_ok and d > MAX_CLOSES:
        return ""
    return d.strftime("%Y-%m-%d")


# NEOGOV's structured remote field.  Trusted over any text guess; the text
# fallback deliberately reads the LOCATION only and never the title, or
# "Remote Sensing Analyst" becomes a remote job.
def work_mode_of(remote_text, location):
    t = sq(remote_text).lower()
    if not t:
        return cc.work_mode(text=location)
    if "hybrid" in t or "flexible" in t:
        return "Hybrid"
    if "remote" in t:          # "Remote Only" / "Remote Optional"
        return "Remote"
    if "on-site" in t or "onsite" in t or "in office" in t:
        return "On-site"
    return cc.work_mode(text=location)


def first(v):
    if isinstance(v, list):
        for x in v:
            if sq(x):
                return sq(x)
        return ""
    return sq(v)


def rows_from(host, agency, fallback_company, payload):
    """jobList -> normalized rows, deduped by ID (the payload is one entry per
    physical address, so a two-site posting arrives twice)."""
    out = []
    seen = set()
    for j in payload.get("jobList") or []:
        if not isinstance(j, dict):
            continue
        jid = j.get("ID")
        if jid is None:
            continue
        jid = str(jid)
        if jid in seen:
            with _lock:
                _stats["dup_addr"] += 1
            continue
        seen.add(jid)
        if j.get("IsTopUSAJob") or j.get("TopUSAJobUrl"):
            # a USAJOBS federal listing resold into this board; the federal
            # pipe already owns it and its apply_url points off-platform
            with _lock:
                _stats["usajobs_skipped"] += 1
            continue
        title = sq(j.get("Classification"))
        slug = sq(j.get("JobTitle"))
        if not title:
            continue
        raw_type = sq(j.get("JobType"))
        etype = (cc.classify_any(raw_type, title) if KEEP_ALL
                 else cc.classify(raw_type, title))
        if not etype:
            with _lock:
                _stats["not_contingent"] += 1
            continue
        location = sq(j.get("Location")) or sq(j.get("Location2"))
        mode = work_mode_of(j.get("RemoteWorkOptionText"), location)
        lo, hi, cur, per = parse_salary(j.get("SalaryInfo"))
        # CountyName is the employer on every board sampled ("State of
        # Washington", "Commonwealth of Pennsylvania", "Ventura County
        # Community College District").  DepartmentName is the sub-unit and is
        # NOT the employer, so it is not used as the company.
        company = sq(j.get("CountyName")) or fallback_company or agency
        r = cc.row(
            source=SOURCE, source_type="ats",
            company=company, title=title,
            employment_type=etype,
            work_mode=mode, location=location,
            city=first(j.get("JobCity")).title(),
            state=first(j.get("JobAbbrvState")).upper(),
            country="United States",
            remote=(mode == "Remote"),
            salary_min=lo, salary_max=hi, salary_currency=cur,
            salary_period=per,
            posted=parse_mdy(j.get("PostingDate")),
            apply_url=JOB_URL % (host, agency, jid, slug or "job"),
            job_id=jid, board=agency,
            board_url=BOARD_URL % (host, agency))
        # Extra key, outside cc.COLUMNS: public-sector close dates are sparse
        # elsewhere in the corpus.  Every reader selects columns by name, so an
        # extra key is inert to bh_build_combined / all_jobs_build.
        closes = parse_mdy(j.get("ClosingDate"), future_ok=True)
        if closes:
            r["closes_on"] = closes
        out.append(r)
    return out


def work(w, host, agency, company):
    key = "%s/%s" % (host, agency)
    s = cc.session()
    hops = 0
    try:
        while True:
            try:
                payload = fetch_board(s, host, agency)
            except Moved as m:
                hops += 1
                if hops > 1 or m.host == host:
                    with _lock:
                        _stats["dead"] += 1
                        _state[key] = {"status": "dead", "why": "moved_loop",
                                       "at": TODAY_S}
                    return
                with _lock:
                    _stats["migrated"] += 1
                    # "migrated", not "moved": coverage_report.DEAD_MARKERS lists
                    # "migrated" and not "moved", so this spelling makes the
                    # source-host key count as SETTLED there instead of
                    # sitting forever in the unreached column.
                    _state[key] = {"status": "migrated", "to": m.host,
                                   "at": TODAY_S}
                host, key = m.host, "%s/%s" % (m.host, agency)
                continue
            except Dead as d:
                with _lock:
                    _stats["dead"] += 1
                    _state[key] = {"status": "dead", "why": d.reason,
                                   "at": TODAY_S}
                return
            except Transient as e:
                with _lock:
                    _stats["transient"] += 1
                print("  TRANSIENT %s: %s" % (key, e), flush=True)
                return
            break
    finally:
        s.close()

    rows = rows_from(host, agency, company, payload)
    with _lock:
        for r in rows:
            w.write(r)
        _stats["rows"] += len(rows)
        if rows:
            _stats["live"] += 1
        else:
            # a real board with nothing open right now -- LIVE, not dead
            _stats["empty"] += 1
        _state[key] = {"status": "live", "jobs": len(rows),
                       "raw": len(payload.get("jobList") or []),
                       "at": TODAY_S}


# ---- tenant list -----------------------------------------------------------
def _norm_host(h):
    h = (h or "").strip().lower().replace("https://", "").replace("http://", "")
    h = h.split("/")[0]
    if not h:
        return HOSTS[0]
    if not h.startswith("www."):
        h = "www." + h
    return h if h in HOSTS else HOSTS[0]


def load_tenants():
    path = TENANTS if os.path.exists(TENANTS) else CANDIDATES
    if not os.path.exists(path):
        sys.exit("no tenant list: run neogov_discover_boards.py first "
                 "(looked for %s and %s)" % (TENANTS, CANDIDATES))
    out, seen = [], set()
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            agency = (r.get("agency") or r.get("board") or
                      r.get("tenant") or "").strip().lower()
            if not agency:
                continue
            host = _norm_host(r.get("host"))
            # neogov_candidates.csv carries unprobed, dead and moved rows too.
            # "unknown" is admitted on purpose: the discovery probe saw a 200 it
            # could not read, which is not evidence of death (rule 1), so the
            # real jobs route gets to decide.  "moved" is skipped because the
            # probe already inserted the destination host as its own row.
            status = (r.get("status") or "").strip().lower()
            if status in ("dead", "moved", "migrated"):
                continue
            if ONLY_HOST and ONLY_HOST not in host:
                continue
            k = (host, agency)
            if k in seen:
                continue
            seen.add(k)
            out.append((host, agency, (r.get("company") or "").strip()))
    print("neogov: tenant list %s -> %d boards" % (path, len(out)), flush=True)
    return out


def main():
    tenants = load_tenants()
    todo = [t for t in tenants
            if REFRESH or (_state.get("%s/%s" % (t[0], t[1])) or {}).get(
                "status") not in ("live", "dead")]
    already = len(tenants) - len(todo)
    random.shuffle(todo)
    if LIMIT:
        todo = todo[:LIMIT]
    print("neogov: %d boards, %d to fetch (%d already recorded)%s"
          % (len(tenants), len(todo), already,
             "  [KEEP_ALL off: contingent rows only]" if not KEEP_ALL else ""),
          flush=True)
    if not todo:
        return 0

    w = cc.Writer(OUT)
    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    done = 0
    stopped = False
    try:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(work, w, h, a, c) for h, a, c in todo]
            for _ in cf.as_completed(futs):
                done += 1
                if done % 25 == 0:
                    with _lock:
                        cc.save_state(STATE, _state)
                    print("  %d/%d  %s" % (done, len(todo), _stats), flush=True)
                if deadline and time.time() > deadline:
                    stopped = True
                    for f in futs:
                        f.cancel()
                    break
    finally:
        with _lock:
            cc.save_state(STATE, _state)
        w.close()
    print("DONE neogov: %s  (429 backoffs: %s)%s"
          % (_stats, _n429,
             "  [BUDGET REACHED — rest still pending]" if stopped else ""),
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
