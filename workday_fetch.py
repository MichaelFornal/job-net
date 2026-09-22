#!/usr/bin/env python3
"""
workday_fetch.py — the Workday (myworkdayjobs.com) CXS pipe.

Workday is the single biggest un-piped ATS family in the corpus: 1,014 tenants
mined out of the apply URLs of 1.17M rows, ~340k live postings.  Every tenant
runs its own subdomain on one of ~14 Workday pods (wd1, wd3, wd5, wd12, wd103,
wd50x ...), and one tenant can publish SEVERAL job-posting sites (External,
Campus, Internal-facing-but-public...), which is why tenants/workday.csv has
one row per (tenant, wd_host, site) and 1,112 rows for 1,014 tenants.

THE ENDPOINT (measured 2026-07-28, 470+ probe requests)
  POST https://{tenant}.{wd_host}.myworkdayjobs.com/wday/cxs/{tenant}/{site}/jobs
  body {"appliedFacets": {...}, "limit": 20, "offset": N, "searchText": ""}
  ->   {"total": int, "jobPostings": [...], "facets": [...],
        "userAuthenticated": false}

  limit is HARD-CAPPED AT 20.  Not "20 by default" — 21 and up answer
  HTTP 400 {"errorCode":"HTTP_400"}.  Measured: 1,5,10,15,19,20 all return
  exactly that many rows; 21,25,30,40,49,50,100,200,500 are all 400.  So a
  10,000-posting tenant is 500 requests and there is no way around it.

  `total` is only populated on the offset=0 response.  Every deeper page
  reports total=0.  Read it once, on the first page, or you will conclude the
  board went empty.

  GET on the same path is HTTP 400 — it is POST-only.

THE 2,000 CEILING (real, but NOT universal — measure, do not assume)
  The repo suspected a 2000 cap.  It exists, but it is per-tenant
  configuration, not a platform constant:
    ngc/wd1        total=3671, facet sums=3671, offsets 2000..3000 return
                   fresh distinct postings.  NOT capped.
    walmart/wd504  total=2000, but the jobFamilyGroup facet counts sum to
                   19,536.  offset>=2000 silently WRAPS BACK TO PAGE 0 —
                   same 20 externalPaths as offset=0, HTTP 200, no error.
  So the truncation signal is not the status code and not `total` alone: it is
  facet-sum > total.  Each facet's value counts sum to the REAL posting count
  even when `total` is clamped, so max(sum(facet.values.count)) > total means
  the window is truncated and the board must be partitioned by facet.
  Verified the partition works: walmart jobFamilyGroup "Maintenance" count
  1743 -> total 1743 (uncapped, fully pageable); "Supply Chain" count 262 ->
  paged out to exactly 262 distinct postings.

  And because offset>=window wraps instead of erroring, the paging loop's real
  terminator is A PAGE THAT CONTRIBUTES NO NEW externalPath.  Measured on the
  262-posting facet: offset 280 returned 20 postings, all already seen.  A
  loop that only stops on an empty page runs forever on those tenants.

HOW A DEAD WORKDAY TENANT LIES TO YOU  (rule 2 — all four measured)
  Every one of these is HTTP-level indistinguishable from a live board or from
  throttling if you look only at the status code:

  1. HTTP 200 + text/html, after a 303.  A tenant on the WRONG POD still
     resolves (wildcard DNS per pod) and the edge 303-redirects to the careers
     SPA, which answers 200 with a ~35KB HTML shell.  tenants/workday.csv
     contains exactly this: a `walmart,wd5,WalmartExternal` row whose own
     example_url is on wd504.  Discriminator: content-type is not JSON.  DEAD,
     never retry — no number of retries turns an SPA shell into JSON.
  2. HTTP 422 {"errorCode":"HTTP_422"} — tenant id does not exist on this pod.
     Confirmed with a nonsense tenant (zzznotarealtenantxyz.wd1) and with real
     mined tenants that have since moved pods.  DEAD.
  3. HTTP 404 {"errorCode":"S21","message":"not found: Job_Posting_Site_ID=X"}
     — the tenant is alive but that SITE was deleted (a Campus site retired,
     a site renamed).  DEAD for that site, and the tenant's other sites are
     unaffected.
  4. HTTP 403 {"errorCode":"S22","message":"permission denied"} — THIS IS THE
     EXPENSIVE ONE.  gainsight.wd5/Gainsight_External_Careers, a real board
     with a real example_url in the corpus, answers 403 because the site was
     switched to authenticated-only.  A bare 403 reads exactly like a WAF
     rate-limit block, and treating it as one is what has burned hours in this
     repo before: it trips a global backoff, the whole pod stalls for 20s, the
     board still ends in err:, and the next run does it all again.
     The discriminator is the BODY, not the code: Workday's application layer
     answers every deterministic refusal with a JSON envelope carrying
     `errorCode`.  A 403 with errorCode S22 is a closed board -> DEAD.  A 403
     with an HTML body / no errorCode did not come from Workday's app layer at
     all (that would be an edge/WAF block) -> transient, board stays PENDING.
     Same rule for 429: a JSON errorCode means the app refused us, anything
     else means the edge did.

  Measured liveness on 470 probe requests over 3 random samples: 269/272 of the
  (tenant,site) rows answer 200-with-JSON.  This tenant list is very clean, so
  a sudden burst of "dead" is a bug in this file, not a dying platform.

REMOTE
  Workday DOES expose a structured remote facet, but only sometimes: the
  `remoteType` facet appears on 24/119 sampled tenants and the per-posting
  `remoteType` field on 12/119.  Observed values: On-site, Onsite, In Office,
  Field, Hybrid, "Hybrid Working", "Hybrid - Onsite District Work Required",
  Remote, "Fully Remote", "Remote Eligible", Virtual, "Fully Flexible".
  When the field is absent the ONLY other signal is locationsText, which is a
  structured location value ("Remote", "US Remote", "Remote Location - All
  States", "Sofia (Hybrid)"), so a whole-word "remote"/"hybrid" match in it is
  trustworthy.  Anything else leaves work_mode blank and remote False —
  per the brief, a wrong remote flag is worse than none.

DATES
  postedOn is RELATIVE text: "Posted Today" / "Posted Yesterday" /
  "Posted N Days Ago" / "Posted 30+ Days Ago".  The first three convert to a
  real date and are stable across runs (a job posted on the 5th reads "5 Days
  Ago" today and "15 Days Ago" in ten days — same answer).  "30+" carries no
  date at all, so it goes through workday_posted_ledger.json, which pins the
  first date this pipe ever computed for a job id.  THE LEDGER IS NEVER
  ROTATED and dates only ever move EARLIER, so a stale posting cannot drift
  forward into the fresh cuts on every rerun.

COVERAGE AS RUN 2026-07-29
  1,112 board rows / 1,014 tenants, every one reached: 1,103 done, 9 dead,
  0 unreached.  402,288 rows == 402,288 distinct apply_urls across 1,006
  boards.  Of the 32 boards that were clamped at the 2,000 window, the facet
  partition recovers essentially all of them (advanceauto 16,408 of 16,454;
  target 11,900 of 11,918; citi 4,568 of 4,578).  The two genuine tails are
  accenture (4,212 of ~96,000) and walmart (9,231 of ~19,500), which need
  more than MAX_DEPTH=3 / PAGE_CAP=1500 to finish:
      WD_PAGE_CAP=6000 WD_MAX_DEPTH=4 WD_BUDGET_MIN=0 python3 workday_fetch.py
  after clearing just those two keys from the state file.

  Utility modes on this same script:
    WD_REQUEUE_TRUNCATED=1   re-probe every done board, drop the clamped ones
                             back to pending so a re-run partitions them
    WD_DEDUPE=1              collapse the append-only JSONL to one row per
                             apply_url (last wins)

Resumable: workday_fetch_state.json holds one entry per (tenant|pod|site).
A board is written and marked done only after ALL of its pages come back
cleanly, so a retry can never append a half-board twice.  A board that was
never reached is left ABSENT from the state file (still pending); a board
proven dead is recorded dead: so it is not probed again.
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
from datetime import date, timedelta

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
# WD_PFX names a throwaway generation of the output/state/ledger triple so a
# smoke run cannot contaminate the real files.  Empty = the real run.
PFX = os.environ.get("WD_PFX") or ""
TENANTS_CSV = os.path.join(BASE, "tenants", "workday.csv")
OUT_JSONL = os.path.join(cc.NORM_DIR, PFX + "workday.jsonl")
STATE = os.path.join(BASE, PFX + "workday_fetch_state.json")
LEDGER = os.path.join(BASE, PFX + "workday_posted_ledger.json")

SOURCE = "workday"
PAGE = 20                 # hard platform maximum, see the header
WINDOW = 2000             # the clamped search window on capped tenants

# One flat worker pool over ~14 pods.  Workers are held for a WHOLE board
# (a 3,671-posting board is 184 sequential pages on one keep-alive session),
# so the pool size is what buys concurrency ACROSS boards; the per-pod token
# buckets below are what protect each host.  Measured: 48 concurrent cold
# boards = 30 req/s with zero 429s; one warm keep-alive session pages at
# ~2.3 req/s (0.43s/page) after a ~0.95s first request.
WORKERS = int(os.environ.get("WD_WORKERS") or 64)
# Per-POD budget.  Pods are separate Workday data centres, so wd1 and wd5 are
# genuinely different infrastructure and get separate buckets; every tenant on
# a pod shares one.  10 rps/pod x ~6 busy pods stays under the 30 req/s that
# measured clean, while letting a big pod drain.
RPS = float(os.environ.get("WD_RPS") or 12.0)
# Burst ceiling per pod.  throttle() spaces requests in TIME but puts no cap on
# how many connections are open at once, and that is what trips an edge
# limiter.  Same lesson as ats_fetch.work().
CONC = int(os.environ.get("WD_CONC") or 20)

TIMEOUT = 35
TRIES = 5
# Pages per board before we stop and take what we have.  1,500 pages = 30,000
# postings; the largest tenant in the corpus is walmart at ~19.5k.  This bounds
# the tail, it does not truncate normal boards (the mean board is 304 postings
# = 16 pages).
PAGE_CAP = int(os.environ.get("WD_PAGE_CAP") or 1500)
# Facet-partition recursion depth for capped boards.  1 = partition once.
MAX_DEPTH = int(os.environ.get("WD_MAX_DEPTH") or 3)
BUDGET_MIN = int(os.environ.get("WD_BUDGET_MIN") or 25)
# Optional slice for a representative run: N boards, taken from a SHUFFLED
# queue so any prefix is a fair sample.
SLICE = int(os.environ.get("WD_SLICE") or 0)

TODAY = date.today()
TODAY_S = TODAY.strftime("%Y-%m-%d")

_lock = threading.Lock()
_state = cc.load_state(STATE)
_ledger = cc.load_state(LEDGER)
_writer = None
_counts = {"rows": 0, "live": 0, "dead": 0, "err": 0, "req": 0, "pages": 0}
_dead_reasons = {}
_failed_now = set()
# Wall-clock stop, shared with the in-flight boards.  A ThreadPoolExecutor only
# cancels futures that have NOT started, so without this a single 1,500-page
# board could run for ten minutes past the budget.  GRACE lets a board that is
# already most of the way through finish; past it the board raises Transient
# and is left PENDING rather than being written half-complete and marked done.
_deadline = [0.0]
GRACE_S = int(os.environ.get("WD_GRACE_S") or 180)

# ---- per-pod token bucket --------------------------------------------------
_rate_lock = threading.Lock()
_next_t = {}
_interval = {}
_pause_until = {}
_n429 = {}
_ok_streak = {}   # consecutive clean responses per pod; see speed_up()
_gates = {}
_gate_lock = threading.Lock()


def _pod_state(pod):
    with _rate_lock:
        if pod not in _next_t:
            _next_t[pod] = 0.0
            _interval[pod] = 1.0 / RPS
            _pause_until[pod] = 0.0


def gate(pod):
    with _gate_lock:
        if pod not in _gates:
            _gates[pod] = threading.Semaphore(CONC)
        return _gates[pod]


def throttle(pod):
    _pod_state(pod)
    while True:
        with _rate_lock:
            now = time.time()
            if now < _pause_until[pod]:
                wait = _pause_until[pod] - now
            else:
                wait = _next_t[pod] - now
                if wait <= 0:
                    _next_t[pod] = max(_next_t[pod] + _interval[pod], now)
                    return
        time.sleep(min(wait, 5) if wait > 0 else 0)


def slow_down(pod):
    _pod_state(pod)
    with _rate_lock:
        _pause_until[pod] = time.time() + 20
        _interval[pod] = min(_interval[pod] * 1.5, 0.5)
        _n429[pod] = _n429.get(pod, 0) + 1
        _ok_streak[pod] = 0


def speed_up(pod):
    """Walk a pod's interval back toward 1/RPS after a run of clean responses.

    Ported from ats_fetch.speed_up() on 2026-09-15.  slow_down() was one-way:
    each edge 429 multiplied the pod's interval by 1.5 up to the 0.5s cap, and
    nothing ever lowered it again, so three backoffs in the first minutes held
    a pod at 3.6 rps (and five at 2 rps) for the remaining three hours of a
    12 rps budget.  This fetcher is the refresh phase's critical path in every
    run since 09-05 -- 164-203 min, capped at WD_BUDGET_MIN in 3 of 5 -- and
    the same code on the same budget drew 867k rows one night and 1,552k the
    next depending on how early wd1 got pinned.

    Recovery is deliberately slower than the backoff (x0.9 per 40 clean
    responses vs x1.5 per 429), so a pod that is genuinely refusing still
    wins the argument.
    """
    _pod_state(pod)
    with _rate_lock:
        n = _ok_streak.get(pod, 0) + 1
        if n < 40:
            _ok_streak[pod] = n
            return
        _ok_streak[pod] = 0
        floor = 1.0 / RPS
        if _interval[pod] > floor:
            _interval[pod] = max(_interval[pod] * 0.9, floor)


class Transient(Exception):
    """Retryable.  Raised so the board is left PENDING rather than recorded as
    a completed empty board — the distinction rule 3 is about."""


class Dead(Exception):
    """The board is provably gone.  Carries the discriminator that proved it so
    the state file says WHY, and so a future reader can tell a real retirement
    from a misclassification."""
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def _err_code(r):
    """Workday's application layer wraps every deterministic refusal in
    {"errorCode": "...", "httpStatus": n}.  Its presence is what separates an
    app-level NO (deterministic, dead) from an edge-level NO (WAF, transient).
    Returns "" when the body is not a Workday error envelope."""
    ct = (r.headers.get("content-type") or "").lower()
    if "json" not in ct:
        return ""
    body = (r.text or "").lstrip()
    if not body.startswith("{"):
        return ""
    try:
        d = json.loads(body)
    except ValueError:
        return ""
    return str(d.get("errorCode") or "") if isinstance(d, dict) else ""


def post_jobs(s, pod, url, body):
    """Throttled POST -> parsed jobs payload.

    Returns the dict on success.  Raises Dead(reason) when the board is
    provably retired (see the four lies in the module header) and
    Transient(...) when we simply could not reach it.  It never returns an
    empty dict for a failure, because "empty board" and "unreachable board"
    must not be the same value: the first gets recorded done, the second must
    stay pending.
    """
    last = None
    for attempt in range(TRIES):
        throttle(pod)
        try:
            r = s.post(url, json=body, timeout=TIMEOUT)
        except requests.RequestException as e:
            last = type(e).__name__
            time.sleep(2 * (attempt + 1))
            continue
        with _lock:
            _counts["req"] += 1
        code = r.status_code
        ct = (r.headers.get("content-type") or "").lower()

        if code == 200:
            if "json" not in ct:
                # Lie #1: retired tenant / wrong pod.  Wildcard DNS resolves,
                # a 303 lands on the careers SPA, and the SPA answers 200 with
                # an HTML shell on any path.  Retrying is pure waste.
                raise Dead("200-html-shell")
            try:
                d = r.json()
            except ValueError:
                raise Dead("200-unparseable")
            if not isinstance(d, dict) or (
                    "jobPostings" not in d and "total" not in d):
                # 200 + JSON that is not a jobs payload: still not a board.
                raise Dead("200-not-jobs-payload")
            speed_up(pod)
            return d

        ec = _err_code(r)
        if code == 422 and ec:
            raise Dead("422-no-such-tenant-on-pod")     # lie #2
        if code == 404 and ec:
            raise Dead("404-no-such-site")              # lie #3
        if code == 403 and ec:
            # lie #4 — S22 "permission denied".  An APP-level refusal, i.e. the
            # site is closed to anonymous visitors.  Not throttling: do NOT
            # call slow_down(), do NOT retry.
            raise Dead("403-%s-permission-denied" % (ec or "403"))
        if code == 400 and ec:
            # Deterministic rejection of a request shape that is identical for
            # every other board on the platform.  Retrying cannot change it.
            raise Dead("400-request-rejected")
        if code in (429, 503, 502, 504, 408, 500) or code == 403:
            # Reaching here means either a real 429 or a status with NO Workday
            # error envelope — i.e. it came from the edge, not the app.  That
            # is the only thing in this function that deserves a backoff.
            slow_down(pod)
            last = "HTTP %d%s" % (code, "/edge" if not ec else "")
        else:
            last = "HTTP %d" % code
        time.sleep(2 * (attempt + 1))
    raise Transient("%s: %s" % (url, last))


# ---- normalization ---------------------------------------------------------
_REL_DAYS = re.compile(r"posted\s+(\d+)\+?\s*days?\s+ago", re.I)
_REL_TODAY = re.compile(r"posted\s+today", re.I)
_REL_YESTERDAY = re.compile(r"posted\s+yesterday", re.I)
_REL_30PLUS = re.compile(r"posted\s+30\+\s*days?\s+ago", re.I)


def parse_posted(txt):
    """('YYYY-MM-DD', precision) or (None, precision) when the text carries no
    date at all.  '30+ Days Ago' is deliberately NOT treated as 30 days: it is
    an unbounded floor, so it is handed to the ledger."""
    t = (txt or "").strip()
    if not t:
        return None, "unknown"
    if _REL_TODAY.search(t):
        return TODAY_S, "relative"
    if _REL_YESTERDAY.search(t):
        return (TODAY - timedelta(days=1)).strftime("%Y-%m-%d"), "relative"
    if _REL_30PLUS.search(t):
        return None, "30plus"
    m = _REL_DAYS.search(t)
    if m:
        try:
            n = int(m.group(1))
        except ValueError:
            return None, "unknown"
        if 0 <= n <= 3650:
            return (TODAY - timedelta(days=n)).strftime("%Y-%m-%d"), "relative"
    return None, "unknown"


def ledger_posted(key, txt):
    """Resolve a posting's date, pinning undatable ones in the ledger.

    Dates only ever move EARLIER.  Without that, a job whose postedOn is
    '30+ Days Ago' would be re-dated to the run date on every sweep and would
    sit at the top of the fresh cuts forever."""
    d, prec = parse_posted(txt)
    if d is None:
        # 30+ days ago -> at LEAST 30 days old.  today-30 is the newest date
        # consistent with what the board told us; first_seen for no signal.
        d = ((TODAY - timedelta(days=30)).strftime("%Y-%m-%d")
             if prec == "30plus" else TODAY_S)
        prec = prec if prec == "30plus" else "first_seen"
    with _lock:
        old = _ledger.get(key)
        if old:
            if old.get("posted") and old["posted"] < d:
                return old["posted"]
            old["posted"] = d
            old["precision"] = prec
            return d
        _ledger[key] = {"posted": d, "precision": prec, "first_seen": TODAY_S}
    return d


_REMOTE_RX = re.compile(r"\b(fully\s+)?remote\b|\bvirtual\b|\bwork\s+from\s+home\b|"
                        r"\btelecommut", re.I)
_HYBRID_RX = re.compile(r"\bhybrid\b|\bflex(ible)?\b", re.I)
_ONSITE_RX = re.compile(r"\bon[\s-]?site\b|\bin\s+office\b|\bin[\s-]office\b|"
                        r"\bfield\b", re.I)


def work_mode_of(remote_type, loc_text):
    """(work_mode, remote_bool).

    remoteType is the structured field and wins outright.  Order matters:
    'Hybrid - Onsite District Work Required' and 'Remote Eligible' both exist,
    so hybrid is tested BEFORE remote and remote before on-site.  With no
    structured field we fall back to locationsText only — a structured
    location value, so a whole-word match in it means something — and we
    return blank rather than guess when neither matches."""
    rt = (remote_type or "").strip()
    if rt:
        if _HYBRID_RX.search(rt):
            return "Hybrid", False
        if _REMOTE_RX.search(rt):
            return "Remote", True
        if _ONSITE_RX.search(rt):
            return "On-site", False
        return "", False
    lt = loc_text or ""
    if _HYBRID_RX.search(lt):
        return "Hybrid", False
    if _REMOTE_RX.search(lt):
        return "Remote", True
    return "", False


_SITE_NOISE = {
    "external", "internal", "site", "sites", "careers", "career", "jobs",
    "job", "search", "portal", "ext", "int", "global", "corporate", "opening",
    "openings", "opportunities", "apply", "applicant", "candidate", "talent",
    "recruiting", "recruitment", "hiring", "gateway", "cx", "www", "main",
    "public", "default", "home", "careersite", "jobsite", "employment",
    "workday", "wd", "page", "board", "new", "live", "prod", "en", "us",
    "experienced", "professional", "hourly", "rec",
}
_CAMEL = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")


def company_of(tenant, site):
    """Best-effort display name.  Workday exposes no company field anywhere in
    the CXS payload and the only cheap alternative — the site's own HTML — is
    an empty SPA shell with an empty <title>.  The SITE id is usually the
    company ('Northrop_Grumman_External_Site', 'WalmartExternal'), so it is
    mined first and the tenant id is the fallback."""
    raw = _CAMEL.sub(" ", site or "")
    toks = [t for t in re.split(r"[\s_\-.]+", raw) if t]
    keep = []
    for t in toks:
        if t.lower() in _SITE_NOISE or t.isdigit():
            continue
        # Sites glue the noise on ('alliancebernsteincareers',
        # 'targetcareers'), so strip a trailing noise word when what is left
        # is still a plausible name.
        for noise in ("careers", "career", "external", "jobs", "careersite"):
            if len(t) > len(noise) + 2 and t.lower().endswith(noise):
                t = t[:-len(noise)]
                break
        keep.append(t)
    if keep and max(len(t) for t in keep) >= 3:
        name = " ".join(keep)
    else:
        name = _CAMEL.sub(" ", tenant or "").replace("-", " ").replace("_", " ")
    name = re.sub(r"\s+", " ", name).strip()
    if not name:
        return tenant
    # Leave existing capitalisation alone for acronyms (RTX, NGC, BMC); only
    # title-case tokens that arrived all-lower.
    return " ".join(t if (t.isupper() or not t.islower()) else t.capitalize()
                    for t in name.split())


# Workday serves the same CXS API on two host shapes, and they differ in WHERE
# the tenant lives:
#   {tenant}.{pod}.myworkdayjobs.com/{site}/...          tenant = host label
#   {pod}.myworkdaysite.com/recruiting/{tenant}/{site}/  tenant = path segment
# Verified 2026-08-20: POST /wday/cxs/hawks/External/jobs on
# wd1.myworkdaysite.com returns 200 {"total":40,"jobPostings":[...]}.  Both
# forms are keyed as plat="workday" by all_jobs_build.canon_key, so a board
# reachable on both still collapses to one job.
JOBS_DOMAIN = "myworkdayjobs.com"
SITE_DOMAIN = "myworkdaysite.com"


def wd_key(tenant, pod, site, domain=None):
    """State/dedupe key for a board.  The domain is part of the identity: a
    tenant can exist on BOTH host shapes with the same pod and site, and a
    three-part key would let one of them silently mask the other."""
    dom = (domain or JOBS_DOMAIN)
    base = "%s|%s|%s" % (tenant, pod, site)
    return base if dom == JOBS_DOMAIN else "%s|%s" % (base, dom)


def wd_urls(tenant, pod, site, domain=None):
    """(cxs_url, board_url) for either Workday host shape."""
    if (domain or JOBS_DOMAIN) == SITE_DOMAIN:
        root = "https://%s.%s" % (pod, SITE_DOMAIN)
        return ("%s/wday/cxs/%s/%s/jobs" % (root, tenant, site),
                "%s/recruiting/%s/%s" % (root, tenant, site))
    root = "https://%s.%s.%s" % (tenant, pod, JOBS_DOMAIN)
    return ("%s/wday/cxs/%s/%s/jobs" % (root, tenant, site),
            "%s/%s" % (root, site))


def norm_row(p, tenant, pod, site, company, board_url, domain=None):
    path = p.get("externalPath") or ""
    if not path:
        return None
    title = (p.get("title") or "").strip()
    if not title:
        return None
    # all_jobs_build.canon_key parses workday apply URLs as
    # host-first-label + the '_'-suffixed id of the last path segment, so this
    # exact form is what makes these rows COLLAPSE onto the copies Getro and
    # the other sources already hold instead of duplicating them.
    apply_url = "%s%s" % (wd_urls(tenant, pod, site, domain)[1], path)
    tail = path.rstrip("/").rsplit("/", 1)[-1]
    # Mirrors all_jobs_build.canon_key's workday branch so job_id matches the
    # key the merge uses.  `or tail` guards the handful of externalPaths that
    # END in the separator ('..._Direkteinstieg_'), where the naive rsplit
    # yields an empty id — 37 rows in the first 86k, all of which would
    # otherwise collide with each other on one blank key.
    jid = (tail.rsplit("_", 1)[-1] or tail) if "_" in tail else tail
    loc = (p.get("locationsText") or "").strip()
    mode, remote = work_mode_of(p.get("remoteType"), loc)
    posted = ledger_posted("%s:%s" % (tenant, jid), p.get("postedOn"))
    return cc.row(
        source=SOURCE, source_type="ats", company=company, title=title,
        # classify_any, never classify: timeType is 'Full time' on ~95% of
        # Workday postings and cc.classify returns None for those, which would
        # silently discard the entire platform.
        employment_type=cc.classify_any(p.get("timeType"), title),
        work_mode=mode, location=loc, remote=remote,
        posted=posted, apply_url=apply_url, job_id=jid, board=tenant,
        board_url=board_url)


# ---- paging ----------------------------------------------------------------
def facet_sums(payload):
    """[(param, nvalues, sum_of_counts, values)] for facets that carry real
    counts.  locationMainGroup / distance are hierarchy widgets whose counts
    are all zero, so they are dropped: they cannot partition anything."""
    out = []
    for f in (payload.get("facets") or []):
        if not isinstance(f, dict):
            continue
        vals = [v for v in (f.get("values") or [])
                if isinstance(v, dict) and (v.get("count") or 0) > 0
                and v.get("id")]
        if not vals:
            continue
        out.append((f.get("facetParameter"), len(vals),
                    sum(v.get("count") or 0 for v in vals), vals))
    return out


def page_query(s, pod, url, applied, seen, out, budget):
    """Page one (possibly facetted) query to exhaustion.

    Returns (reported_total, first_payload).  Terminators, in the order they
    actually fire in the wild:
      - a page whose postings were ALL already returned BY THIS QUERY.  This is
        the real one: past the search window Workday wraps to page 0 and serves
        HTTP 200 forever.  Note "by this query", not "by this board" — the
        facet passes deliberately re-walk postings the unfacetted pass already
        collected, so testing against the board-wide `seen` set would abort
        every facet at page 0 and make the whole partition a no-op.
      - an empty page.
      - offset >= reported total (only trustworthy from the offset=0 page).
      - the per-board page budget.
    """
    total = None
    first = None
    off = 0
    qseen = set()
    while True:
        if budget[0] <= 0:
            break
        if _deadline[0] and time.time() > _deadline[0] + GRACE_S:
            raise Transient("wall-clock budget exhausted mid-board")
        body = {"appliedFacets": applied, "limit": PAGE, "offset": off,
                "searchText": ""}
        try:
            d = post_jobs(s, pod, url, body)
        except Dead:
            # A dead verdict is only meaningful on the FIRST page: that is the
            # one that proves the board/site does not exist.  If page 0 was
            # fine and page 47 suddenly says 422, the board is not retired —
            # something transient dressed itself up as one — and throwing away
            # 940 already-collected postings to record it dead would be a far
            # worse error than stopping here with what we have.
            if off == 0:
                raise
            break
        budget[0] -= 1
        with _lock:
            _counts["pages"] += 1
        if off == 0:
            first = d
            total = d.get("total") or 0
        jp = [p for p in (d.get("jobPostings") or []) if isinstance(p, dict)]
        if not jp:
            break
        fresh = 0
        for p in jp:
            path = p.get("externalPath")
            if not path or path in qseen:
                continue
            qseen.add(path)
            fresh += 1
            if path not in seen:
                seen.add(path)
                out.append(p)
        if fresh == 0:
            break
        off += PAGE
        if total and off >= total:
            break
    # len(qseen), not len(seen): a facet pass deliberately re-walks postings
    # the unfacetted pass already banked, so only the per-QUERY count can be
    # compared against this query's own `total`.
    return (total or 0), (first or {}), len(qseen)


def clamped(total, got, first):
    """Is this query's result window truncated?

    Two independent signals, either of which is conclusive:

      got < total          We were told there are N and came back with fewer.
                           Something cut us off — the wrap, most likely.

      facet-sum > total AND total >= WINDOW
                           The facet counts are computed over the whole
                           requisition set, not over the returned window, so
                           when `total` is clamped they still report the truth:
                           accenture says total=2000 and its facets sum to
                           96,083; walmart says 2000 and sums to 19,515.

    The `total >= WINDOW` qualifier is load-bearing and was added after
    measuring it: facet counts are MULTI-VALUED (one posting listed in three
    locations is counted three times), so facet-sum legitimately exceeds total
    on perfectly healthy small boards — 166 of the 198 boards the first
    version of this test flagged were of the form total=45 / sum=53, i.e. a
    handful of multi-location postings, not a clamp.  Requiring the board to
    have actually reached the window removes every one of those false
    positives and keeps all 32 genuinely clamped boards.
    """
    if total and got < total:
        return True
    if total < WINDOW:
        return False
    fs = facet_sums(first)
    return bool(fs) and max(x[2] for x in fs) > total


def harvest(s, pod, url, applied, seen, out, budget, depth):
    """page_query + facet partition when the window is clamped."""
    total, first, got = page_query(s, pod, url, applied, seen, out, budget)
    if depth >= MAX_DEPTH or budget[0] <= 0 or not first:
        return
    if not clamped(total, got, first):
        return
    fs = facet_sums(first)
    if not fs:
        return
    real = max(x[2] for x in fs)
    # Prefer a facet that covers (nearly) all the postings, and among those the
    # one with the most values: the finest partition that is not lossy.
    cands = [x for x in fs if x[0] and x[0] not in applied
             and x[2] >= 0.9 * real]
    if not cands:
        cands = [x for x in fs if x[0] and x[0] not in applied]
    if not cands:
        return
    cands.sort(key=lambda x: -x[1])
    param, _n, _sum, vals = cands[0]
    for v in vals:
        if budget[0] <= 0:
            break
        if _deadline[0] and time.time() > _deadline[0]:
            break       # stop widening; what we already paged is still valid
        sub = dict(applied)
        sub[param] = [v["id"]]
        harvest(s, pod, url, sub, seen, out, budget, depth + 1)


def fetch_board(rec):
    tenant, pod, site = rec["tenant"], rec["wd_host"], rec["site"]
    # Absent column => the original host shape, so every pre-existing
    # tenants/workday.csv keeps its exact behaviour.
    domain = (rec.get("wd_domain") or JOBS_DOMAIN).strip() or JOBS_DOMAIN
    url, board_url = wd_urls(tenant, pod, site, domain)
    company = company_of(tenant, site)
    s = requests.Session()
    s.headers.update({"User-Agent": cc.UA, "Accept": "application/json",
                      "Content-Type": "application/json"})
    try:
        seen, raw = set(), []
        harvest(s, pod, url, {}, seen, raw, [PAGE_CAP], 0)
        rows = []
        for p in raw:
            r = norm_row(p, tenant, pod, site, company, board_url, domain)
            if r:
                rows.append(r)
        return rows
    finally:
        s.close()


def work(rec):
    key = wd_key(rec["tenant"], rec["wd_host"], rec["site"],
                 rec.get("wd_domain"))
    st = _state.get(key)
    if isinstance(st, str) and (st == "done" or st.startswith("dead:")):
        return
    if _deadline[0] and time.time() > _deadline[0]:
        return          # budget blown before we started: leave it PENDING
    with gate(rec["wd_host"]):
        try:
            rows = fetch_board(rec)
        except Dead as e:
            with _lock:
                _state[key] = "dead:%s" % e.reason
                _counts["dead"] += 1
                _dead_reasons[e.reason] = _dead_reasons.get(e.reason, 0) + 1
            return
        except Transient as e:
            with _lock:
                # NOT recorded done and NOT recorded dead: absent-ish state so
                # the next run retries it.  err: entries are treated as pending
                # by main().
                _state[key] = "err:%s" % str(e)[:60]
                _counts["err"] += 1
                _failed_now.add(key)
            return
        except Exception as e:                          # noqa: BLE001
            with _lock:
                _state[key] = "err:%s" % type(e).__name__
                _counts["err"] += 1
                _failed_now.add(key)
            return
        # Whole board came back clean -> write it, then mark done.  Doing it in
        # this order means a crash between the two costs a duplicate board on
        # the next run, not a lost one; doing it the other way loses data.
        with _lock:
            for r in rows:
                _writer.write(r)
            _counts["rows"] += len(rows)
            _counts["live"] += 1
            _state[key] = "done"


def load_boards():
    if not os.path.exists(TENANTS_CSV):
        raise SystemExit("missing %s" % TENANTS_CSV)
    with open(TENANTS_CSV, newline="", encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f)
                if r.get("tenant") and r.get("wd_host") and r.get("site")]
    # (tenant, pod, site) is the unit of work; the CSV is already one row per
    # unit but guard against duplicates so no board is fetched twice.
    seen, out = set(), []
    for r in rows:
        k = (r["tenant"], r["wd_host"], r["site"],
             r.get("wd_domain") or JOBS_DOMAIN)
        if k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def dedupe_out():
    """Rewrite the JSONL keeping the LAST row per apply_url.

    The file is append-only across runs and a board re-queued after a code fix
    is re-appended in full, so this is what keeps `rows` and `distinct
    apply_url` equal.  Last-wins, because a later run is the fresher read of
    the same posting.  Rewrites through a temp file so an interrupted dedupe
    cannot truncate the corpus.
    """
    if not os.path.exists(OUT_JSONL):
        print("nothing to dedupe", flush=True)
        return 0
    best, order, bad, n = {}, [], 0, 0
    with open(OUT_JSONL, encoding="utf-8", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            n += 1
            try:
                d = json.loads(line)
            except ValueError:
                bad += 1
                continue
            k = d.get("apply_url") or ""
            if not k:
                bad += 1
                continue
            if k not in best:
                order.append(k)
            best[k] = line
    tmp = OUT_JSONL + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        for k in order:
            f.write(best[k] + "\n")
    os.replace(tmp, OUT_JSONL)
    print("dedupe: %d lines in -> %d out (%d dup, %d unusable)"
          % (n, len(order), n - len(order) - bad, bad), flush=True)
    return 0


def requeue_truncated():
    """Re-probe every done board and re-queue the ones whose search window is
    clamped (facet counts sum past the reported total).

    One page-0 request per board, so it is cheap, and it is the only way to
    find the capped minority after the fact: nothing in the emitted rows says
    "this board was truncated", the board just stops at the window."""
    boards = load_boards()
    done = [b for b in boards
            if _state.get(wd_key(b["tenant"], b["wd_host"], b["site"],
                                 b.get("wd_domain")))
            == "done"]
    print("re-probing %d done boards for a clamped window" % len(done),
          flush=True)
    hits = []
    lk = threading.Lock()

    def probe(b):
        t, pod, site = b["tenant"], b["wd_host"], b["site"]
        url = wd_urls(t, pod, site, b.get("wd_domain"))[0]
        s = requests.Session()
        s.headers.update({"User-Agent": cc.UA, "Accept": "application/json",
                          "Content-Type": "application/json"})
        try:
            with gate(pod):
                d = post_jobs(s, pod, url, {"appliedFacets": {}, "limit": PAGE,
                                            "offset": 0, "searchText": ""})
        except (Dead, Transient):
            return
        finally:
            s.close()
        total = d.get("total") or 0
        fs = facet_sums(d)
        real = max([x[2] for x in fs] or [0])
        if real > total:
            with lk:
                hits.append((t, pod, site, total, real,
                             b.get("wd_domain")))

    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        list(ex.map(probe, done))
    for t, pod, site, total, real, dom in sorted(hits, key=lambda x: -x[4]):
        print("  clamped: %s/%s/%s total=%d real>=%d" % (t, pod, site, total,
                                                         real), flush=True)
        _state.pop(wd_key(t, pod, site, dom), None)
    cc.save_state(STATE, _state)
    print("re-queued %d clamped boards — re-run to partition them" % len(hits),
          flush=True)
    return 0


def main():
    if os.environ.get("WD_DEDUPE") == "1":
        return dedupe_out()
    if os.environ.get("WD_REQUEUE_TRUNCATED") == "1":
        return requeue_truncated()
    global _writer
    boards = load_boards()
    todo = []
    for b in boards:
        k = wd_key(b["tenant"], b["wd_host"], b["site"],
                   b.get("wd_domain"))
        st = _state.get(k)
        if isinstance(st, str) and (st == "done" or st.startswith("dead:")):
            continue
        todo.append(b)
    random.seed(int(os.environ.get("WD_SEED") or 20260728))
    random.shuffle(todo)
    if SLICE and len(todo) > SLICE:
        todo = todo[:SLICE]
        print("  SLICE: %d boards of %d pending (shuffled, so a fair sample)"
              % (len(todo), len(boards)), flush=True)
    print("workday: %d boards total, %d to fetch, %d workers, %.1f rps/pod"
          % (len(boards), len(todo), WORKERS, RPS), flush=True)

    _writer = cc.Writer(OUT_JSONL)
    t0 = time.time()
    deadline = t0 + BUDGET_MIN * 60 if BUDGET_MIN else None
    _deadline[0] = deadline or 0.0
    done = 0
    stopped_early = False
    try:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(work, b) for b in todo]
            for _ in cf.as_completed(futs):
                done += 1
                if done % 100 == 0:
                    with _lock:
                        cc.save_state(STATE, _state)
                        cc.save_state(LEDGER, _ledger)
                        _writer.f.flush()
                        snap = dict(_counts)
                    el = time.time() - t0
                    print("  %d/%d boards  %.0fs  rows=%d live=%d dead=%d "
                          "err=%d  %.1f req/s"
                          % (done, len(todo), el, snap["rows"], snap["live"],
                             snap["dead"], snap["err"], snap["req"] / max(el, 1)),
                          flush=True)
                if deadline and time.time() > deadline:
                    stopped_early = True
                    for f in futs:
                        f.cancel()
                    break
    finally:
        with _lock:
            cc.save_state(STATE, _state)
            cc.save_state(LEDGER, _ledger)
        _writer.close()

    el = time.time() - t0
    print("DONE workday in %.0fs: rows=%d live=%d dead=%d err=%d "
          "requests=%d pages=%d"
          % (el, _counts["rows"], _counts["live"], _counts["dead"],
             _counts["err"], _counts["req"], _counts["pages"]), flush=True)
    if _dead_reasons:
        print("  dead by discriminator: %s" % _dead_reasons, flush=True)
    if _n429:
        print("  edge backoffs by pod: %s" % _n429, flush=True)
        # Where each backed-off pod ENDED.  1/RPS means speed_up() walked it
        # all the way back; 0.5 means it was still pinned at the finish.
        print("  final intervals by pod: %s"
              % {k: round(_interval.get(k, 0), 3) for k in _n429}, flush=True)
    if stopped_early:
        print("BUDGET REACHED after %dmin: %d/%d boards; the rest are still "
              "pending and queued for the next run (WD_BUDGET_MIN=0 to "
              "disable)." % (BUDGET_MIN, done, len(todo)), flush=True)
        return 0
    if _failed_now:
        print("%d boards unreachable — still PENDING, re-run to retry"
              % len(_failed_now), flush=True)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
