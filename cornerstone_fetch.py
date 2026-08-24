#!/usr/bin/env python3
"""
cornerstone_fetch.py — Cornerstone OnDemand (CSOD) career sites, {tenant}.csod.com.

ROUTE (reverse-engineered 2026-08-20 from the player-career-site 1.25.6 bundle)
------------------------------------------------------------------------------
The JSON search API is real and it is NOT credential-gated — it is gated on a
token the PUBLIC career-site page mints for itself.  Probing the search
endpoint cold gives

    POST https://cornerstone.csod.com/services/x/career-site/v1/search
        -> 401  "CSOD Unauthorized Exception:Check your credentials."

but the career-site shell page server-renders the token straight into the HTML:

    GET https://{tenant}.csod.com/ux/ats/careersite/{cid}/home?c={tenant}
        -> 200, ~5 KB, containing
           csod.context={"corp":"turnerconstruction","cultureID":1,
                         "cultureName":"en-US",
                         "endpoints":{"cloud":"https://us.api.csod.com/"},
                         "token":"eyJhbGciOiJIUzUxMiIs..."}

That JWT is anonymous (sub = -100, the anonymousUserId the career site itself
publishes), lives ~2h, and its `rurls` claim whitelists exactly the routes the
public page needs — `services/x/career-site`, `rec-job-search/external`,
`services/x/job-requisition/v2/requisitions`.  Send it as
`Authorization: Bearer <token>` and every one of those routes answers.

So the pipe is: one HTML fetch to mint a token, then pure JSON.

  1. TOKEN     GET  /ux/ats/careersite/{cid}/home?c={tenant}
  2. DISCOVERY GET  /services/x/career-site/v1/careersites/{cid}?cultureId=1
                    200 -> {"data":{"id":..,"active":true,"allowSEO":..}}
                    404 -> {"error":{"code":40,"message":"Resource not found"}}
  3. SEARCH    POST /services/x/career-site/v1/search        (Bearer)
                    {"careerSiteId":N,"careerSitePageId":N,"pageNumber":1,
                     "pageSize":500,"cultureId":1,"cultureName":"en-US",
                     "searchText":"","states":[],"countryCodes":[],"cities":[],
                     "placeID":"","radius":null,"postingsWithinDays":null,
                     "customFieldCheckboxKeys":[],"customFieldDropdowns":[],
                     "customFieldRadios":[]}
                 -> {"data":{"totalCount":904,"requisitions":[
                      {"requisitionId":21291,"displayJobTitle":"Estimator",
                       "postingEffectiveDate":"8/19/2026",
                       "locations":[{"city":"Seattle","state":"WA",
                                     "country":"US"}]}, ...]}}
  4. COMPANY   GET  /ux/ats/careersite/{cid}/home/requisition/{id}?c={tenant}
                    one page per CAREERSITE that has rows, for the ld+json
                    HiringOrganization.Name (see the note further down: one
                    CSOD corp is often several employers).

TWO PAYLOAD SHAPES THAT LOOK IDENTICAL AND ARE NOT
--------------------------------------------------
`radius` and `postingsWithinDays` MUST be null, not 0.  The bundle's own
filter mappers (be()/_e()) return null when no filter is selected; sending 0
is a valid request that returns

    {"status":0,"data":{"totalCount":0,"requisitions":[],...}}

— HTTP 200, well-formed JSON, correct keys, zero jobs.  That is this repo's
"200 is not success" trap in its most expensive form: it is indistinguishable
from a genuinely empty board and would have marked all 63 tenants live-and-
empty forever.  Turner Construction answers 0 with radius:0 and 904 with
radius:null.

Likewise `pageNumber` is 1-BASED.  pageNumber:0 answers HTTP 400.

REGION HOSTS — why the same-origin route is the one used
--------------------------------------------------------
The bundle ships TWO search implementations and picks between them at runtime:

    same-origin  https://{tenant}.csod.com/services/x/career-site/v1/search
    cloud        {csod.context.endpoints.cloud}rec-job-search/external/jobs

`endpoints.cloud` is per-tenant and per-REGION: turnerconstruction gets
https://us.api.csod.com/, agrial gets https://uk.api.csod.com/.  Hard-coding
the US host gives every European tenant a 401 with an empty body — a
credentials-shaped error that is really a wrong-datacentre error, and one that
would look exactly like "the token approach does not work".  Both routes return
the same totalCount and the same requisitionIds; the same-origin one is used
here because it needs no region map and its rows are ~10x smaller (it omits
externalDescription, which this pipeline does not store anyway).

CAREERSITE IDS ARE NOT ALL 1, AND ONE TENANT HAS MANY
-----------------------------------------------------
Identity is {tenant}.csod.com + a numeric careersite id + ?c={corp}.  In the
corpus the id ranges 1..50 (savemart=27, kypersonnelcabinet=50) and three
tenants appear under several (agrial 8+16, davenport 15+21, groupe-altitude
4+5+6).  Each careersite is a separate filtered view of ONE requisition pool,
so the same job is reachable under several ids:

    turnerconstruction  cid 1 -> 904 reqs   cid 3 -> 731 (21 new)
                        cid 2 -> 145 (12 new)  cid 4 -> 203 (18 new)
                        cid 5,6,8 -> 41 (1 new)   cid 7 -> active:false, 0
                        union = 956 distinct requisitionIds

So this fetcher sweeps careersite ids per tenant (mined ids ALWAYS, plus
1..CORNERSTONE_CID_MAX) and then DEDUPES BY requisitionId within the tenant,
lowest careersite id winning.  Without that dedupe turner alone would emit
2,024 rows for 956 jobs, and all_jobs_build would keep every one of them as a
distinct apply_url.  See the wiring note at the bottom: canon_key must drop
the careersite id for the same reason.

DEAD / EMPTY / ERROR
--------------------
  * Nonexistent tenant  -> DNS NXDOMAIN.  csod.com has no wildcard:
    zzznotarealtenant9x.csod.com does not resolve.  Recorded dead:dns only
    after every retry, because a resolver blip looks the same for one attempt.
  * Nonexistent careersite id -> the shell page 302s (empty body) and
    careersites/{id} answers 404 with error.code 40.  Authoritative.
  * active:false careersite -> 200 with a well-formed 0-result search
    (turner cid 7).  LIVE-BUT-EMPTY, not dead.
  * 200 HTML with no csod.context / no token -> dead:no_context.  Shape check,
    not status check.  Only 3xx / 404 / 200-without-context count toward that
    verdict; a 400/401/403 on the shell page or on careersites/{id} is
    TRANSIENT, because a per-IP WAF penalty and a retired tenant are the same
    status code.  That distinction is not theoretical: the first full run
    recorded mirvac dead:no_context and dropped its 26 live jobs, and mirvac's
    career site was serving a token normally minutes later.  The dead record
    now carries the tally that produced it, e.g.
    dead:no_context[redirect=20,no-context=1].
  * search 200 whose body is not JSON, or whose data lacks a `requisitions`
    LIST plus an integer `totalCount` -> TRANSIENT.  Never "empty".
  * 401 mid-run -> the 2h token expired: re-mint once, then TRANSIENT.
  * 429 / 5xx / timeout / conn reset -> TRANSIENT.  A transient tenant is left
    ABSENT from the state file so the next run retries it; it is never
    recorded as a completed empty board.

POSTED DATES
------------
`postingEffectiveDate` is the employer's own posting date and it is present on
essentially every row — which is rare here (only ~19% of the live corpus
carries a real employer date).  It is rendered in the CULTURE the search was
asked for: "8/19/2026" (en-US), "19/08/2026" (fr-FR), "19.08.2026" (de-DE).
Rather than parse three formats, every search is asked for cultureId 1 /
en-US even on career sites whose only allowed language is French — measured on
agrial cid 4/16 (French-only): cultureId 1 returns the identical totalCount and
the identical French job titles, with the date normalised to M/D/YYYY.  The
site's own default culture is used only as a fallback if en-US returns 0 on a
site the discovery call said was active, and parse_posted() then disambiguates
by inspecting the components.  A future postingEffectiveDate (a scheduled
posting) is clamped to today: the honest claim is "seen live today", and an
unclamped future date can never age out of all_jobs_build's posted cutoff.

NO EMPLOYMENT TYPE EXISTS
-------------------------
Neither the search rows nor /services/x/job-requisition/v2/requisitions/{id}/
jobDetails carry an employment type — jobDetails' full key set is
displayTitle, externalDescription, ref, requisitionStatusId, hiringManagerId,
allowApply, primaryLocation, additionalLocations, availableCultures,
defaultCultureId, openDate, companyApplyUrl, owners, reviewers, positionOUId.
So employment_type is cc.classify_any(None, title) — title evidence only.

Output: contract_norm/cornerstone.jsonl
State:  cornerstone_fetch_state.json  (keyed by tenant)
Tenants: tenants/cornerstone.csv  (mined from all_jobs_master.csv: 63 tenants,
         7,231 corpus rows)

Env knobs
  CORNERSTONE_KEEP_ALL   default 1.  0 = contingent roles only (cc.classify).
                         The default is inverted relative to the 2026-07
                         contract-only pipes ON PURPOSE: this source publishes
                         no employment-type field at all, so contract-only
                         would keep only the handful of rows whose TITLE says
                         "contract" and silently discard the family.
  CORNERSTONE_RPS        default 6.0   global token bucket (shared CSOD edge)
  CORNERSTONE_WORKERS    default 8
  CORNERSTONE_BUDGET_MIN default 0 (no limit)
  CORNERSTONE_LIMIT      default 0 (all tenants) — smoke tests use this
  CORNERSTONE_REFRESH    1 = re-fetch tenants already recorded
  CORNERSTONE_CID_MAX    default 20 — careersite ids swept beyond the mined
                         ones.  0 disables the sweep (mined ids only).
  CORNERSTONE_PAGE       default 500  search page size (1000 verified working)
  CORNERSTONE_ONLY       comma-separated tenant allowlist
"""
import concurrent.futures as cf
import csv
import html
import json
import os
import random
import re
import socket
import sys
import threading
import time
from datetime import datetime, timezone

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
TENANTS = os.path.join(BASE, "tenants", "cornerstone.csv")
OUT = os.path.join(cc.NORM_DIR, "cornerstone.jsonl")
STATE = os.path.join(BASE, "cornerstone_fetch_state.json")

SOURCE = "cornerstone"

RPS = float(os.environ.get("CORNERSTONE_RPS") or 6.0)
WORKERS = int(os.environ.get("CORNERSTONE_WORKERS") or 8)
BUDGET_MIN = int(os.environ.get("CORNERSTONE_BUDGET_MIN") or 0)
LIMIT = int(os.environ.get("CORNERSTONE_LIMIT") or 0)
REFRESH = os.environ.get("CORNERSTONE_REFRESH") == "1"
CID_MAX = int(os.environ.get("CORNERSTONE_CID_MAX", "20"))
PAGE = int(os.environ.get("CORNERSTONE_PAGE") or 500)
KEEP_ALL = os.environ.get("CORNERSTONE_KEEP_ALL", "1") != "0"
ONLY = {t.strip().lower() for t in
        (os.environ.get("CORNERSTONE_ONLY") or "").split(",") if t.strip()}

TIMEOUT = 45
TRIES = 4
MAX_PAGES = 200          # 100k jobs at PAGE=500; a runaway pager stops here

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

_lock = threading.Lock()
_state = cc.load_state(STATE)
_stats = {"live": 0, "dead": 0, "transient": 0, "rows": 0, "sites": 0,
          "empty": 0, "skipped_sites": 0}

_DEC = json.JSONDecoder()


class Transient(Exception):
    """Retryable failure.  RAISED, never returned, so the caller leaves the
    tenant PENDING (absent from the state file) instead of recording it as a
    completed empty board."""


class Dead(Exception):
    """The upstream ASSERTS this tenant/careersite does not exist.  Recorded."""


# ---- throttle: the speed_up()/slow_down() pair from ats_fetch.py -----------
# One global bucket: every tenant host is a subdomain of the same CSOD edge
# (Kong + CloudFront), so a 429 on one is evidence about all of them.
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]
_n429 = [0]
_ok_streak = [0]


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
        time.sleep(min(wait, 5) if wait > 0 else 0)


def slow_down():
    with _rate_lock:
        _pause_until[0] = time.time() + 20
        _interval[0] = min(_interval[0] * 1.5, 1.0)
        _n429[0] += 1
        _ok_streak[0] = 0


def speed_up():
    """Walk the interval back toward RPS after a run of clean responses.

    Without this, slow_down() is a one-way ratchet: a handful of 429s in the
    first minute pin the whole run at 1 rps for its remaining hours whatever
    RPS says.  Recovery is deliberately slower than the backoff (0.9x per 40
    clean responses vs 1.5x per 429) so a host that is genuinely refusing still
    wins the argument.
    """
    with _rate_lock:
        _ok_streak[0] += 1
        if _ok_streak[0] < 40:
            return
        _ok_streak[0] = 0
        floor = 1.0 / RPS
        if _interval[0] > floor:
            _interval[0] = max(_interval[0] * 0.9, floor)


def _req(s, method, url, **kw):
    """Throttled request with the shared retry/backoff contract.

    Returns the response for 2xx and for the statuses the caller must
    interpret (3xx/400/401/403/404).  RAISES Transient on 429/5xx/network.
    """
    last = None
    for attempt in range(TRIES):
        throttle()
        try:
            r = s.request(method, url, timeout=TIMEOUT,
                          allow_redirects=False, **kw)
            code = r.status_code
            if 200 <= code < 300:
                speed_up()
                return r
            if code in (301, 302, 303, 307, 308, 400, 401, 403, 404, 410):
                # Interpreted by the caller against the PAYLOAD, never trusted
                # on its own.  Not retried: they are all stable answers.
                return r
            if code in (429, 418):
                slow_down()
                last = "HTTP %d" % code
            else:
                last = "HTTP %d" % code
        except requests.exceptions.ConnectionError as e:
            last = "dns" if _is_dns(e) else type(e).__name__
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    raise Transient("%s %s failed after %d tries: %s" % (method, url, TRIES,
                                                         last))


def _is_dns(exc):
    t = "%s %s" % (type(exc).__name__, exc)
    return ("NameResolutionError" in t or "gaierror" in t
            or "nodename nor servname" in t or "Name or service not known" in t
            or "Temporary failure in name resolution" in t)


def resolves(host):
    """True if the host resolves.  csod.com has NO wildcard DNS, so a hard
    NXDOMAIN is the cheapest and most authoritative dead-tenant signal there
    is — but it is checked twice, seconds apart, because a single resolver
    blip must not delist a live employer."""
    for i in range(2):
        try:
            socket.getaddrinfo(host, 443)
            return True
        except socket.gaierror:
            if i == 0:
                time.sleep(1.5)
        except OSError:
            return True          # not a name error; let HTTP decide
    return False


# ---- token ----------------------------------------------------------------
def page_context(s, tenant, cid):
    """(ctx, why) for the career-site shell page.

    ctx is None with a `why` only when the upstream has ASSERTED that this
    careersite id serves nothing: a 302 (Location: /ui/error, which is what a
    nonexistent careersite id gets), a 404, or a 200 whose body carries no
    csod.context object.

    400/401/403 RAISE instead.  That distinction is the whole reason this
    function exists in this shape: on the first full run mirvac was recorded
    dead:no_context and its 26 live jobs were dropped, because every shell
    fetch in that burst came back non-200 and the old code read any non-200 as
    "no career site here".  A per-IP WAF penalty and a retired tenant look
    identical by status; only the ones above are the upstream talking about
    the RESOURCE.
    """
    url = "https://%s.csod.com/ux/ats/careersite/%d/home?c=%s" % (
        tenant, cid, tenant)
    r = _req(s, "GET", url, headers={"Accept": "text/html"})
    code = r.status_code
    if code in (301, 302, 303, 307, 308):
        return None, "redirect"
    if code in (404, 410):
        return None, "notfound"
    if code != 200:
        raise Transient("shell %s/%d: HTTP %d" % (tenant, cid, code))
    body = r.text or ""
    m = re.search(r"csod\.context\s*=\s*\{", body)
    if not m:
        # 200 HTML with no context object: parked corp / maintenance page.
        # Shape, not status.
        return None, "no-context"
    try:
        ctx, _ = _DEC.raw_decode(body, m.end() - 1)
    except ValueError:
        return None, "bad-context"
    if not isinstance(ctx, dict) or not ctx.get("token"):
        return None, "no-token"
    return ctx, ""


def mint(s, tenant, cids):
    """(ctx, cid) from the first careersite id that serves a shell page.

    Raises Dead only when EVERY candidate answered with an assertion of
    non-existence; a transient anywhere in the sweep propagates instead, so
    the tenant stays pending rather than being delisted on a bad minute.
    """
    whys = []
    for cid in cids:
        ctx, why = page_context(s, tenant, cid)
        if ctx:
            return ctx, cid
        whys.append(why)
    tally = ",".join("%s=%d" % (w, whys.count(w)) for w in sorted(set(whys)))
    raise Dead("no_context[%s]" % tally)


def auth(ctx, tenant, cid):
    return {"Authorization": "Bearer " + ctx["token"],
            "Content-Type": "application/json",
            "Accept": "application/json, text/plain, */*",
            "Origin": "https://%s.csod.com" % tenant,
            "Referer": "https://%s.csod.com/ux/ats/careersite/%d/home?c=%s"
                       % (tenant, cid, tenant)}


# ---- careersite discovery --------------------------------------------------
def site_meta(s, tenant, cid, hdr):
    """{"id":..,"active":..} for a careersite, or None when the API asserts it
    does not exist (404 + error.code 40).  Shape-validated."""
    url = ("https://%s.csod.com/services/x/career-site/v1/careersites/%d"
           "?cultureId=1" % (tenant, cid))
    r = _req(s, "GET", url, headers=hdr)
    if r.status_code in (404, 410):
        return None                      # error.code 40: no such careersite
    if r.status_code == 401:
        raise Unauthorized()
    if r.status_code != 200:
        # 400/403/3xx here are about US, not about the careersite.  Same rule
        # as page_context(): never let a WAF answer delist a board.
        raise Transient("careersites/%d: HTTP %d" % (cid, r.status_code))
    try:
        d = r.json()
    except ValueError:
        # 200 that is not JSON on a JSON route: an edge/WAF page, not an
        # answer about this careersite.  Retryable, NOT "no such site".
        raise Transient("careersites/%d: 200 non-JSON" % cid)
    data = d.get("data") if isinstance(d, dict) else None
    if not isinstance(data, dict) or "id" not in data:
        return None
    return data


class Unauthorized(Exception):
    """401 — the ~2h anonymous token expired mid-tenant."""


# ---- search ----------------------------------------------------------------
def search_page(s, tenant, cid, hdr, page, culture_id, culture_name):
    """(total, [requisition]) for one page.  Shape-validated; raises on doubt."""
    url = "https://%s.csod.com/services/x/career-site/v1/search" % tenant
    payload = {
        "careerSiteId": cid, "careerSitePageId": cid,
        "pageNumber": page,               # 1-BASED; 0 answers HTTP 400
        "pageSize": PAGE,
        "cultureId": culture_id, "cultureName": culture_name,
        "searchText": "",
        "states": [], "countryCodes": [], "cities": [], "placeID": "",
        "radius": None,                   # null, NOT 0 — see the docstring
        "postingsWithinDays": None,       # null, NOT 0
        "customFieldCheckboxKeys": [], "customFieldDropdowns": [],
        "customFieldRadios": [],
    }
    r = _req(s, "POST", url, headers=hdr, data=json.dumps(payload))
    if r.status_code == 401:
        raise Unauthorized()
    if r.status_code != 200:
        raise Transient("search %s/%d: HTTP %d" % (tenant, cid,
                                                   r.status_code))
    try:
        d = r.json()
    except ValueError:
        raise Transient("search %s/%d: 200 non-JSON (%d bytes)"
                        % (tenant, cid, len(r.text or "")))
    data = d.get("data") if isinstance(d, dict) else None
    if not isinstance(data, dict):
        raise Transient("search %s/%d: no data object" % (tenant, cid))
    reqs = data.get("requisitions")
    total = data.get("totalCount")
    # THE payload-shape gate.  "200, JSON, but not the search contract" is an
    # error, never an empty board.
    if not isinstance(reqs, list) or not isinstance(total, int):
        raise Transient("search %s/%d: bad shape %s" % (tenant, cid,
                                                        sorted(data)[:6]))
    return total, reqs


def search_all(s, tenant, cid, hdr, culture_id, culture_name, refresh):
    """Every requisition on one careersite, paged."""
    out, page = [], 1
    while page <= MAX_PAGES:
        try:
            total, reqs = search_page(s, tenant, cid, hdr, page, culture_id,
                                      culture_name)
        except Unauthorized:
            if page > 1 or not refresh:
                raise
            hdr = refresh()
            total, reqs = search_page(s, tenant, cid, hdr, page, culture_id,
                                      culture_name)
        out.extend(reqs)
        if len(reqs) < PAGE or len(out) >= total:
            break
        page += 1
    return out


# ---- normalisation ---------------------------------------------------------
_WS = re.compile(r"\s+")


def sq(v):
    return _WS.sub(" ", html.unescape(str(v or ""))).strip()


_US_CULTURES = {"en-us", "en-ph", "en-ca"}


def parse_posted(raw, culture_name):
    """'8/19/2026' -> '2026-08-19'.  Culture-aware, and self-correcting.

    Every search is asked for en-US so this is normally M/D/YYYY, but the
    fallback path can ask a site in its own culture, where the same date is
    '19/08/2026' or '19.08.2026'.  Order is decided by the culture and then
    OVERRIDDEN by the values themselves when they are unambiguous (a first
    component > 12 cannot be a month), so a mislabelled culture degrades to a
    correct date rather than a silently wrong one.
    """
    s = sq(raw)
    if not s or s == "-":
        return ""
    m = re.match(r"^(\d{4})-(\d{2})-(\d{2})", s)
    if m:
        y, mo, d = (int(x) for x in m.groups())
    else:
        parts = re.split(r"[/.\-]", s.split(" ")[0])
        if len(parts) != 3 or not all(p.isdigit() for p in parts):
            return ""
        a, b, c = (int(p) for p in parts)
        if len(parts[0]) == 4:                       # YYYY/M/D
            y, mo, d = a, b, c
        else:
            y = c
            if (culture_name or "").lower() in _US_CULTURES:
                mo, d = a, b
            else:
                d, mo = a, b
            if mo > 12 >= d:                         # unambiguously swapped
                mo, d = d, mo
        if y < 100:
            y += 2000
    if not (1 <= mo <= 12 and 1 <= d <= 31 and 1990 <= y <= 2100):
        return ""
    out = "%04d-%02d-%02d" % (y, mo, d)
    # A scheduled future posting would otherwise never age out of
    # all_jobs_build's posted cutoff.  "Seen live today" is the honest claim.
    return TODAY if out > TODAY else out


def loc_title(l):
    bits = [sq(l.get("city")), sq(l.get("state")), sq(l.get("country"))]
    return ", ".join(b for b in bits if b)


def rows_for(tenant, corp, cid, company, reqs, culture_name):
    out = []
    for q in reqs:
        if not isinstance(q, dict):
            continue
        rid = q.get("requisitionId")
        title = sq(q.get("displayJobTitle"))
        if rid is None or not title:
            continue
        locs = [l for l in (q.get("locations") or []) if isinstance(l, dict)]
        titles = [t for t in (loc_title(l) for l in locs) if t]
        locstr = " | ".join(titles[:4])
        if len(titles) > 4:
            locstr += " | +%d more" % (len(titles) - 4)
        prim = locs[0] if locs else {}
        et = (cc.classify_any(None, title) if KEEP_ALL
              else cc.classify(None, title))
        if not et:
            continue                      # KEEP_ALL=0 and not contingent
        mode = cc.work_mode(text=locstr + " " + title)
        # The employer's own posting URL, byte-identical to the one the API
        # hands back as postings[].postingUrl and to the form already in the
        # corpus.  Stable across runs: no session id, no tracking parameter.
        apply_url = ("https://%s.csod.com/ux/ats/careersite/%d/home/"
                     "requisition/%s?c=%s" % (tenant, cid, rid, corp))
        out.append(cc.row(
            source=SOURCE, source_type="ats",
            company=company, title=title,
            employment_type=et,
            work_mode=mode, location=locstr,
            city=sq(prim.get("city")), state=sq(prim.get("state")),
            country=sq(prim.get("country")),
            remote=(mode == "Remote"),
            posted=parse_posted(q.get("postingEffectiveDate"), culture_name),
            apply_url=apply_url, job_id=str(rid), board=tenant,
            board_url="https://%s.csod.com/ux/ats/careersite/%d/home?c=%s"
                      % (tenant, cid, corp)))
    return out


_LD = re.compile(r'<script[^>]*type=["\']application/ld\+json["\'][^>]*>'
                 r"(.*?)</script>", re.S | re.I)


def org_name(s, tenant, corp, cid, rid):
    """Employer name from the requisition page's ld+json HiringOrganization.

    One request per TENANT.  The mined company column is a Getro/theMuse
    by-product and is wrong on the low-volume tenants (iata -> 'To70',
    axian-group -> 'Power for All'); the career site's own structured data is
    the employer talking about itself.  Empty when the site has allowSEO off.
    """
    url = ("https://%s.csod.com/ux/ats/careersite/%d/home/requisition/%s?c=%s"
           % (tenant, cid, rid, corp))
    try:
        r = _req(s, "GET", url, headers={"Accept": "text/html"})
    except Transient:
        return ""
    if r.status_code != 200:
        return ""
    for m in _LD.finditer(r.text or ""):
        try:
            d = json.loads(m.group(1))
        except ValueError:
            continue
        if not isinstance(d, dict):
            continue
        org = d.get("HiringOrganization") or d.get("hiringOrganization")
        if isinstance(org, dict):
            n = sq(org.get("Name") or org.get("name"))
            if n:
                return n
    return ""


def pretty(tenant):
    t = re.sub(r"[-_]+", " ", tenant)
    t = re.sub(r"\b(hris|rh|career|carriere|portail|corporate|group[e]?)\b",
               " ", t, flags=re.I)
    t = _WS.sub(" ", t).strip()
    return t.title() if t else tenant


# ---- one tenant ------------------------------------------------------------
def fetch_tenant(s, tenant, mined_cids, mined_company):
    """(verdict, rows).  verdict 'live' or 'dead:<why>'; Transient raises."""
    if not resolves("%s.csod.com" % tenant):
        raise Dead("dns")

    cand = list(dict.fromkeys(mined_cids + list(range(1, CID_MAX + 1))))
    ctx, tok_cid = mint(s, tenant, cand)     # raises Dead("no_context[...]")
    corp = sq(ctx.get("corp")) or tenant

    state = {"hdr": auth(ctx, tenant, tok_cid)}

    def refresh_hdr():
        try:
            c2, c2id = mint(s, tenant, [tok_cid] + cand)
        except Dead as e:
            # The tenant served a token minutes ago; it not serving one now is
            # a failure of this run, not proof the board is gone.
            raise Transient("token refresh failed for %s: %s" % (tenant, e))
        state["hdr"] = auth(c2, tenant, c2id)
        return state["hdr"]

    def call(fn, *a):
        try:
            return fn(s, tenant, *a, state["hdr"])
        except Unauthorized:
            refresh_hdr()
            return fn(s, tenant, *a, state["hdr"])

    mined = set(mined_cids)
    sites, skipped = [], 0
    for cid in sorted(cand):
        try:
            meta = call(site_meta, cid)
        except Transient:
            # A SWEPT id is best-effort: axian-group's careersites 2 and 3
            # answer a permanent HTTP 500 ({"status":3} error envelope) while
            # 1 and 4 are healthy, and letting one broken speculative id abort
            # the tenant left axian-group pending on every run forever.
            # A MINED id is mandatory — it has rows in the corpus, so quietly
            # returning without it would under-report the board and, once the
            # ledger's delist merge is wired, could delist live jobs.
            if cid in mined:
                raise
            skipped += 1
            continue
        if meta is None:
            continue
        sites.append((cid, meta))
    if not sites:
        raise Dead("no_careersite")

    seen, rows, live_sites = set(), [], 0
    for cid, meta in sites:
        # active:false careersites were measured to answer a well-formed
        # search with totalCount 0 every time (savemart cid 1/5, turner cid 7),
        # so the sweep skips them — that is ~1,200 POSTs across the family.
        # Ids that are IN THE CORPUS are still searched whatever the flag says:
        # a mined board that has just been switched off is exactly the case
        # where an authoritative 0 matters, and guessing it is not the same
        # thing as asking.
        if meta.get("active") is False and cid not in mined:
            continue
        cn = ((meta.get("currentLanguage") or {}).get("cultureName")
              if isinstance(meta.get("currentLanguage"), dict) else "")
        reqs = search_all(s, tenant, cid, state["hdr"], 1, "en-US",
                          refresh_hdr)
        culture_used = "en-US"
        if not reqs and meta.get("active") and cn and cn != "en-US":
            # Belt and braces: en-US was measured to work on French-only
            # career sites, but if a site ever answers 0 for it, ask in the
            # site's own language before believing the board is empty.
            dc = meta.get("defaultCultureId")
            if isinstance(dc, int):
                reqs = search_all(s, tenant, cid, state["hdr"], dc, cn,
                                  refresh_hdr)
                culture_used = cn
        live_sites += 1
        fresh = [q for q in reqs
                 if isinstance(q, dict) and q.get("requisitionId") not in seen]
        seen.update(q.get("requisitionId") for q in fresh)
        if not fresh:
            continue
        rows.append((cid, fresh, culture_used))

    # Company name is resolved PER CAREERSITE, not per tenant.  One CSOD corp
    # is routinely a whole group: aswatsoneurope's 13 sites are A.S. Watson
    # Health & Beauty Europe (NL/BE) and Superdrug/Savers (UK) and Marionnaud
    # (FR), and the mined column — a Getro/theMuse by-product — labels all
    # 3,505 of them "Marionnaud".  The ld+json on the site's own requisition
    # page is the employer naming itself; it is only rendered when the site
    # has allowSEO on, so the mined value and then the prettified slug remain
    # the fallbacks.  One extra request per careersite that has rows.
    fallback = mined_company or pretty(tenant)
    seo_of = {c: m.get("allowSEO") for c, m in sites}
    out = []
    for cid, reqs, culture_used in rows:
        company = ""
        if str(seo_of.get(cid)).lower() != "false":
            company = org_name(s, tenant, corp, cid,
                               reqs[0].get("requisitionId"))
        out.extend(rows_for(tenant, corp, cid, company or fallback, reqs,
                            culture_used))
    with _lock:
        _stats["sites"] += live_sites
        _stats["skipped_sites"] += skipped
    return "live", out


def work(w, tenant, cids, company):
    s = cc.session()
    s.headers.update({"User-Agent": cc.UA,
                      "Accept-Language": "en-US,en;q=0.9"})
    try:
        try:
            verdict, rows = fetch_tenant(s, tenant, cids, company)
        except Dead as e:
            with _lock:
                _stats["dead"] += 1
                _state[tenant] = {"status": "dead", "why": str(e), "at": TODAY}
            print("  DEAD %s: %s" % (tenant, e), flush=True)
            return
        except (Transient, Unauthorized) as e:
            with _lock:
                _stats["transient"] += 1
            print("  TRANSIENT %s: %s" % (tenant, e), flush=True)
            return
        except Exception as e:                              # noqa: BLE001
            with _lock:
                _stats["transient"] += 1
            print("  TRANSIENT %s: %s: %s"
                  % (tenant, type(e).__name__, e), flush=True)
            return
        with _lock:
            for r in rows:
                w.write(r)
            _stats["rows"] += len(rows)
            _stats["live"] += 1
            if not rows:
                _stats["empty"] += 1
            _state[tenant] = {"status": "live", "jobs": len(rows),
                              "at": TODAY}
        print("  %-28s %5d rows" % (tenant, len(rows)), flush=True)
    finally:
        s.close()


def load_tenants():
    out = []
    if not os.path.exists(TENANTS):
        print("no %s — run the miner first" % TENANTS, flush=True)
        return out
    with open(TENANTS, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            t = (r.get("tenant") or "").strip().lower()
            if not t or (ONLY and t not in ONLY):
                continue
            cids = [int(x) for x in (r.get("careersite_ids") or "").split("|")
                    if x.strip().isdigit()] or [1]
            out.append((t, cids, (r.get("company") or "").strip()))
    return out


def main():
    tenants = load_tenants()
    todo = [x for x in tenants
            if REFRESH or (_state.get(x[0]) or {}).get("status")
            not in ("live", "dead")]
    already = len(tenants) - len(todo)
    random.shuffle(todo)
    if LIMIT:
        todo = todo[:LIMIT]
    print("cornerstone: %d tenants, %d to fetch (%d already recorded), "
          "cid sweep 1..%d, keep_all=%s"
          % (len(tenants), len(todo), already, CID_MAX, KEEP_ALL), flush=True)
    if not todo:
        return 0

    w = cc.Writer(OUT)
    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    done, stopped = 0, False
    try:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(work, w, t, c, co) for t, c, co in todo]
            for _ in cf.as_completed(futs):
                done += 1
                if done % 10 == 0:
                    with _lock:
                        cc.save_state(STATE, _state)
                    print("  %d/%d  %s" % (done, len(todo), _stats),
                          flush=True)
                if deadline and time.time() > deadline:
                    stopped = True
                    for f in futs:
                        f.cancel()
                    break
    finally:
        with _lock:
            cc.save_state(STATE, _state)
        w.close()
    print("DONE cornerstone: %s  (429 backoffs: %d)%s"
          % (_stats, _n429[0],
             "  [BUDGET REACHED — rest still pending]" if stopped else ""),
          flush=True)
    # Exit 1 when tenants are still pending, the house seatbelt: run_all's
    # ok_rc=(0,1) tolerates it and the next run picks them up.
    return 1 if (_stats["transient"] or stopped) else 0


if __name__ == "__main__":
    sys.exit(main())
