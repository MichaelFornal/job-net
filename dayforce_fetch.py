#!/usr/bin/env python3
"""
dayforce_fetch.py — Ceridian Dayforce (jobs.dayforcehcm.com) multi-tenant fetcher.

ROUTE  (reverse-engineered 2026-08-20 from the Next.js bundle)
--------------------------------------------------------------
Every Dayforce career site is one Next.js app at jobs.dayforcehcm.com on the
route `/[clientNamespace]/[careerSiteXRefCode]`, so a board is a PAIR, and the
locale segment in front of it is optional:

    https://jobs.dayforcehcm.com/en-US/lightwavedental/CarolinasDentist
    https://jobs.dayforcehcm.com/crisis24/CANDIDATEPORTAL          (no locale)

The page ships 439 KB of HTML but the job list is NOT in it.  __NEXT_DATA__
carries exactly one dehydrated react-query entry — "site-info" — and the list is
fetched client-side.  Scraping the rendered markup is therefore not just ugly
here, it is impossible from the SSR payload.  Two clean JSON routes do the job
(both discovered in chunks/_app-*.js, where the axios instance is created with
`baseURL = /api/geo/${clientNamespace}`):

  1. GET  /api/geo/{client}/sitecontext/{client}/{site}/{language}
     Board identity: jobBoardId, cultureCode, isoCultureCodes, isDisabled, and
     candidateCorrespondanceClientName — the employer's real display name,
     which the search payload does not carry.  Cheap, GET, no auth.
     `{language}` is forgiving: an unsupported-but-well-formed code falls back
     to the board's default and the response tells you which
     (lightwavedental + de-DE -> cultureCode "en-US"), so asking for en-US
     always returns the board's effective culture.  A MALFORMED code
     ("zz-ZZ") is a 400 — the shape is checked, the value is not.

  2. POST /api/geo/{client}/jobposting/search
     {clientNamespace, jobBoardCode, cultureCode, paginationStart}
     -> {"jobPostings":[...25...], "maxCount":N, "offset":k, "count":n}

     THE 403 THAT LOOKS LIKE A CLOSED DOOR AND IS NOT.  A plain POST returns
     HTTP 403 "Forbidden" (9 bytes) no matter what browser headers, cookies or
     Origin you send.  It is not bot protection and not a private board: _app
     installs an axios request interceptor that puts next-auth's CSRF token on
     every non-GET —

         X-CSRF-TOKEN: (await getCsrfToken())

     — and the token is public.  GET /api/auth/csrf on the same session hands
     it over and sets the matching __Host-next-auth.csrf-token cookie.  With
     the pair present the same POST is a 200.  Reading that 403 as "dead
     tenant" would have marked all 4,556 boards dead in one pass.

     Page size is FIXED at 25.  count / pageSize / paginationCount in the body
     are all ignored (measured: all three returned count=25 on a 547-job
     board).  paginationStart past the end is NOT an error — it returns
     {"jobPostings":[], "maxCount":547, "offset":99999, "count":0} — so the
     loop must stop on maxCount, not on an exception.

EMPLOYMENT TYPE FOR FREE
------------------------
The search payload has no employment-type field.  But the board's pay classes
are a public GET —

    /api/geo/{client}/postingattributes/payclasses/{client}/{jobBoardId}/{lang}
    -> {"postingAttributesAttributesList":[{"attributeId":1,"attributeValue":
        "Full-time"},{"attributeId":2,"attributeValue":"Part-time"}]}

— and `payClass` is a search filter that PARTITIONS the board exactly.
Measured on vestis/candidateportal: unfiltered maxCount 547, payClass=1 gives
492, payClass=2 gives 55, 492+55 = 547.  So one extra sweep per class labels
every row, and a board with a single pay class costs no extra pages at all
(the one class covers the whole board, so its name is simply applied).  When
the per-class counts do NOT add up to the unfiltered total the leftovers keep
the title-only classification rather than being dropped — a pay class is a
nice-to-have, never a gate.  DAYFORCE_PAYCLASS=0 turns the whole thing off.

DEAD / EMPTY / ERROR  (rule 3)
------------------------------
  404 + application/problem+json   -> DEAD.  Measured shape:
        {"type":"https://httpstatuses.io/404","title":"Not Found",
         "status":404,"traceId":"00-..."}
      Returned for a bogus client, a bogus site, and a bogus site under a REAL
      client alike — the app-level "no such board", and the only 404 we trust.
  404 without a JSON body          -> TRANSIENT.  That is the edge or a deploy
      losing the /api route, not a retired tenant.
  200 without a JSON content-type  -> DEAD ("200-html-shell"): the SPA shell
      answering on a path the API no longer serves.
  200 JSON of the wrong shape      -> DEAD ("200-not-*-payload").
  sitecontext isDisabled == True   -> DEAD ("board-disabled").
  403 on a POST                    -> refresh the CSRF token once, retry; a
      second 403 is TRANSIENT (it is our credential, not their board).
  429 / 5xx / timeouts             -> TRANSIENT, and the board stays absent
      from the state file so the next run retries it.
  200 + jobPostings == []          -> LIVE and EMPTY.  Recorded done with 0
      jobs.  This is a different outcome from every line above it.

THROTTLE (rule 2)
-----------------
slow_down() 1.5x on a 429 with a 20 s pause, speed_up() 0.9x back toward the
configured rate after 40 clean responses — the ats_fetch.py pair, not a
one-way ratchet.  One global bucket: every board is the same host.

POSTED DATES
------------
postingStartTimestampUTC is the employer's own posting date and was present on
175/175 rows measured across five boards, so `posted` is real here rather than
a first-seen stand-in.  For the rare null, dayforce_posted_ledger.json pins the
first date ever seen for that job id and never lets it move forward; the file
is created only if such a row actually appears, and must never be rotated.

apply_url  (rule 4)
-------------------
    https://jobs.dayforcehcm.com/{culture}/{client}/{site}/jobs/{jobPostingId}

built from the SERVER's own lowercase clientNamespace / jobBoardCode out of
sitecontext, never from the mixed-case spelling in the tenant file — otherwise
"crisis24/CANDIDATEPORTAL" and "crisis24/candidateportal" (both present in the
corpus) would emit two different URLs for one job across two runs.  It is the
employer's real posting page, the same URL the board's own job card links to
(`/${language}/${client}/${site}/jobs/${jobPostingId}` in the page chunk).

Input:  tenants/dayforce.csv  (or dayforce_candidates.csv), written by
        dayforce_discover_boards.py.  tenant is the pair "client/site".
Output: contract_norm/dayforce.jsonl
State:  dayforce_fetch_state.json   (keyed by "client/site", resumable)

ENV
---
  DAYFORCE_KEEP_ALL   1 = every employment type (default 1); 0 = contingent only
  DAYFORCE_BUDGET_MIN wall-clock budget in minutes, 0 = unlimited (default 0)
  DAYFORCE_RPS        requests/sec against jobs.dayforcehcm.com (default 4)
  DAYFORCE_WORKERS    thread count (default 8)
  DAYFORCE_LIMIT      stop after N boards, 0 = all (default 0)
  DAYFORCE_ONLY       comma-separated "client/site" pairs to run, ignoring state
  DAYFORCE_REFRESH    1 = re-fetch boards already recorded live/dead
  DAYFORCE_PAYCLASS   1 = label employment type from pay classes (default 1)
  DAYFORCE_TENANTS    override the tenant CSV path
"""
import concurrent.futures as cf
import csv
import os
import random
import re
import sys
import threading
import time
from datetime import datetime, timezone

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
TENANTS = os.environ.get("DAYFORCE_TENANTS") or os.path.join(
    BASE, "tenants", "dayforce.csv")
FALLBACK_TENANTS = os.path.join(BASE, "dayforce_candidates.csv")
OUT = os.path.join(cc.NORM_DIR, "dayforce.jsonl")
STATE = os.path.join(BASE, "dayforce_fetch_state.json")
LEDGER = os.path.join(BASE, "dayforce_posted_ledger.json")

SOURCE = "dayforce"
HOST = "https://jobs.dayforcehcm.com"
PAGE = 25                      # server-fixed; see the module docstring
PAGE_CAP = 400                 # 400 * 25 = 10k jobs/board; the largest board
                               # measured (vestis) is 547.

RPS = float(os.environ.get("DAYFORCE_RPS") or 4.0)
WORKERS = int(os.environ.get("DAYFORCE_WORKERS") or 8)
TIMEOUT = 40
TRIES = 4
BUDGET_MIN = int(os.environ.get("DAYFORCE_BUDGET_MIN") or 0)
LIMIT = int(os.environ.get("DAYFORCE_LIMIT") or 0)
REFRESH = os.environ.get("DAYFORCE_REFRESH") == "1"
KEEP_ALL = (os.environ.get("DAYFORCE_KEEP_ALL") or "1") == "1"
CLASSIFY = cc.classify_any if KEEP_ALL else cc.classify
USE_PAYCLASS = (os.environ.get("DAYFORCE_PAYCLASS") or "1") == "1"
ONLY = [s.strip().lower() for s in
        (os.environ.get("DAYFORCE_ONLY") or "").split(",") if s.strip()]

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

_lock = threading.Lock()
_state = cc.load_state(STATE)
_ledger = cc.load_state(LEDGER)
_ledger_dirty = [False]
_stats = {"live": 0, "empty": 0, "dead": 0, "transient": 0, "rows": 0,
          "req": 0, "undated": 0}


# ---- throttle: one bucket, one host.  slow_down/speed_up PAIR (rule 2) ------
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
        _interval[0] = min(_interval[0] * 1.5, 0.5)
        _n429[0] += 1


def speed_up():
    """Walk the interval back toward RPS after a run of clean responses.

    Without this, slow_down() is a ratchet: a handful of 429s in the first
    minutes pins the whole run at 2 rps for its remaining hours whatever RPS
    says.  Recovery is deliberately slower than the backoff (0.9x per 40 clean
    responses vs 1.5x per 429) so a host that is genuinely refusing still wins.
    """
    with _rate_lock:
        _ok_streak[0] += 1
        if _ok_streak[0] < 40:
            return
        _ok_streak[0] = 0
        floor = 1.0 / RPS
        if _interval[0] > floor:
            _interval[0] = max(_interval[0] * 0.9, floor)


class Transient(Exception):
    """Retryable.  Raised, never returned, so the board is left PENDING rather
    than recorded as a completed empty board."""


class Dead(Exception):
    """The board is provably gone.  Carries the discriminator that proved it."""
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


# ---- session + CSRF --------------------------------------------------------
_tl = threading.local()


def session():
    s = getattr(_tl, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update({
            "User-Agent": cc.UA,
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Origin": HOST,
        })
        _tl.s = s
        _tl.csrf = None
    return s


def _is_json(r):
    return "json" in (r.headers.get("content-type") or "").lower()


def _body(r):
    """Parsed JSON body, or None when the body is not JSON at all."""
    if not _is_json(r):
        return None
    try:
        return r.json()
    except ValueError:
        return None


def csrf_token(force=False):
    """next-auth's public CSRF token for this thread's session.

    Public, not a credential: /api/auth/csrf hands it to anyone and sets the
    matching cookie on the same session.  Cached per thread; refreshed once on
    a 403 before the POST is given up on.
    """
    if not force and getattr(_tl, "csrf", None):
        return _tl.csrf
    _tl.csrf = None
    s = session()
    last = None
    for attempt in range(TRIES):
        throttle()
        try:
            r = s.get(HOST + "/api/auth/csrf", timeout=TIMEOUT)
        except requests.RequestException as e:
            last = type(e).__name__
            time.sleep(2 * (attempt + 1))
            continue
        with _lock:
            _stats["req"] += 1
        if r.status_code == 200:
            d = _body(r)
            if isinstance(d, dict) and d.get("csrfToken"):
                speed_up()
                _tl.csrf = d["csrfToken"]
                return _tl.csrf
            last = "csrf body %r" % (str(d)[:80],)
        elif r.status_code == 429:
            slow_down()
            last = "HTTP 429"
        else:
            last = "HTTP %d" % r.status_code
        time.sleep(2 * (attempt + 1))
    raise Transient("csrf: %s" % last)


def _handle(r, what):
    """Common status handling -> parsed JSON, or raise Dead/Transient.

    Returns None for "not decided here, keep retrying" (the caller loops).
    """
    code = r.status_code
    d = _body(r)
    if code == 200:
        if d is None:
            # The SPA shell answering 200/text-html on an API path.  A retired
            # site does this; retrying is pure waste.
            raise Dead("200-html-shell")
        return d
    if code in (404, 410):
        # The app's own problem+json envelope is what makes a 404 trustworthy.
        # A 404 with no JSON body is the edge, or a deploy that lost the route.
        if isinstance(d, dict) and (d.get("status") or d.get("title")):
            raise Dead("%d-no-such-board" % code)
        raise Transient("%s: HTTP %d with no problem+json body" % (what, code))
    if code == 401 and isinstance(d, dict):
        raise Dead("401-not-public")
    if code == 400 and isinstance(d, dict):
        raise Dead("400-request-rejected")
    if code == 429:
        slow_down()
    return None


def api_get(path, what):
    """Throttled GET -> parsed JSON.  Raises Dead / Transient, never a
    sentinel that a caller could mistake for an empty board."""
    s = session()
    last = None
    for attempt in range(TRIES):
        throttle()
        try:
            r = s.get(HOST + path, timeout=TIMEOUT)
        except requests.RequestException as e:
            last = type(e).__name__
            time.sleep(2 * (attempt + 1))
            continue
        with _lock:
            _stats["req"] += 1
        d = _handle(r, what)
        if d is not None:
            speed_up()
            return d
        last = "HTTP %d" % r.status_code
        time.sleep(2 * (attempt + 1))
    raise Transient("%s %s: %s" % (what, path, last))


def api_post(path, payload, what):
    """Throttled POST with the CSRF header -> parsed JSON.

    A 403 here means the token, not the board: refresh it once and retry.  A
    second 403 is Transient so the board stays pending — never Dead.
    """
    s = session()
    last = None
    refreshed = False
    for attempt in range(TRIES):
        tok = csrf_token()
        throttle()
        try:
            r = s.post(HOST + path, json=payload, timeout=TIMEOUT,
                       headers={"X-CSRF-TOKEN": tok,
                                "Content-Type": "application/json",
                                "Referer": HOST + "/"})
        except requests.RequestException as e:
            last = type(e).__name__
            time.sleep(2 * (attempt + 1))
            continue
        with _lock:
            _stats["req"] += 1
        if r.status_code == 403:
            last = "HTTP 403 (csrf)"
            if not refreshed:
                refreshed = True
                csrf_token(force=True)
                continue
            time.sleep(2 * (attempt + 1))
            continue
        d = _handle(r, what)
        if d is not None:
            speed_up()
            return d
        last = "HTTP %d" % r.status_code
        time.sleep(2 * (attempt + 1))
    raise Transient("%s %s: %s" % (what, path, last))


# ---- board pieces ----------------------------------------------------------
def sitecontext(client, site):
    """Board identity, or Dead.  en-US is a safe ask: an unsupported but
    well-formed language falls back to the board's default and the response
    reports which one, so the returned cultureCode is authoritative."""
    d = api_get("/api/geo/%s/sitecontext/%s/%s/en-US" % (client, client, site),
                "sitecontext")
    if not isinstance(d, dict) or "clientNamespace" not in d \
            or d.get("jobBoardId") is None:
        raise Dead("200-not-sitecontext-payload")
    if d.get("isDisabled") is True:
        raise Dead("board-disabled")
    return d


def payclasses(client, jbid, culture):
    """[(attributeId, name)] for the board, or [] when it has none / fails.

    Best effort by design: a pay class is a label, never a gate, so anything
    unexpected here must cost rows nothing.
    """
    if not USE_PAYCLASS:
        return []
    try:
        d = api_get("/api/geo/%s/postingattributes/payclasses/%s/%s/%s"
                    % (client, client, jbid, culture), "payclasses")
    except (Dead, Transient):
        return []
    lst = d.get("postingAttributesAttributesList") if isinstance(d, dict) else None
    if not isinstance(lst, list):
        return []
    out = []
    for a in lst:
        if isinstance(a, dict) and a.get("attributeId") is not None:
            out.append((a["attributeId"], str(a.get("attributeValue") or "")))
    return out


def search_page(client, board_code, culture, start, pay_class=None):
    body = {"clientNamespace": client, "jobBoardCode": board_code,
            "cultureCode": culture, "paginationStart": start}
    if pay_class is not None:
        body["payClass"] = pay_class
    d = api_post("/api/geo/%s/jobposting/search" % client, body, "search")
    if not isinstance(d, dict) or not isinstance(d.get("jobPostings"), list) \
            or "maxCount" not in d:
        # 200 + JSON that is not a jobs payload is still not a board.
        raise Dead("200-not-jobs-payload")
    return d


def sweep(client, board_code, culture, pay_class=None):
    """Every posting on a board (optionally one pay class) -> {id: posting}.

    Stops on maxCount, because paginationStart past the end is a healthy 200
    with an empty list, not an error.  Also stops if a page adds nothing new,
    so a server that clamps the offset cannot spin the loop forever.
    """
    out = {}
    start = 0
    total = None
    for _ in range(PAGE_CAP):
        d = search_page(client, board_code, culture, start, pay_class)
        if total is None:
            total = int(d.get("maxCount") or 0)
        posts = d["jobPostings"]
        before = len(out)
        for p in posts:
            if isinstance(p, dict) and p.get("jobPostingId") is not None:
                out[p["jobPostingId"]] = p
        if not posts or len(out) == before:
            break
        start += PAGE
        if start >= (total or 0):
            break
    return out, (total or 0)


# ---- normalization ---------------------------------------------------------
_WS = re.compile(r"\s+")


def sq(v):
    return _WS.sub(" ", str(v or "")).strip()


def ledger_posted(key):
    """Date for a posting the board gave no start timestamp for.

    First date ever observed wins forever — otherwise an undatable job is
    re-dated to the run date on every sweep and sits at the top of the fresh
    cuts permanently.
    """
    with _lock:
        rec = _ledger.get(key)
        if rec:
            return rec.get("posted") or TODAY
        _ledger[key] = {"posted": TODAY, "precision": "first_seen",
                        "first_seen": TODAY}
        _ledger_dirty[0] = True
        _stats["undated"] += 1
        return TODAY


# The correspondence name is the only display name Dayforce publishes, and it
# is written for *emails to candidates*, so it carries two kinds of noise:
# a multi-brand list ("Vestis Uniforms and Workplace Supplies | Canadian Linen
# & Uniform Service | Quebec Linge...") and a recruiting-desk suffix
# ("Multi-Color Corporation Recruiting", "YMCA of Greater Cleveland Recruitment
# Team").  Both make the row fail to match the same employer from any other
# source.  The suffix strip only fires when something survives it, so a board
# whose correspondence name is nothing BUT the desk word ("Recruiting",
# "Careers") keeps that name instead of being cut to the empty string.  The
# pattern is anchored to the end, so an employer whose real name merely starts
# with one of these words ("Talent Acquisition Partners") is untouched.
_DESK_RX = re.compile(
    r"\s*[-–—,]?\s*("
    r"recruit(?:ing|ment)(?:\s+team)?|talent\s+acquisition(?:\s+team)?|"
    r"hiring\s+team|careers?(?:\s+team)?|job\s+board|candidate\s+portal|"
    r"human\s+resources|hr\s+team"
    r")\s*$", re.I)


def _from_slug(client):
    """Employer name derived from the tenant namespace, title-cased.

    This is a DERIVATION from the employer's own identifier, not a name
    borrowed from somewhere else -- the distinction that matters.  It fires on
    ~1.5% of rows (aeropostale, lantic) where the portal publishes no
    correspondence name.  Same treatment workday_fetch.company_of applies to a
    Workday tenant, including leaving existing capitalisation alone so
    acronyms survive.
    """
    return " ".join(t if (t.isupper() or not t.islower()) else t.capitalize()
                    for t in (client or "").replace("-", " ").split())


def company_of(ctx, client):
    name = (sq(ctx.get("candidateCorrespondanceClientName"))
            or sq(ctx.get("candidateCorrespondenceClientName")))
    if not name:
        return _from_slug(client)
    name = name.split("|")[0].strip() or name
    stripped = _DESK_RX.sub("", name).strip(" -–—,")
    if stripped:
        name = stripped
    return name or _from_slug(client)


def location_of(p):
    """(location, city, state, country) from postingLocations.

    formattedAddress is deliberately NOT used: it is prefixed with the site's
    own brand and full street address ("CarolinasDentist, 913 East Broad
    Street, Fuquay-Varina, North Carolina, United States of America"), which
    makes a worse `location` string than the structured parts.  country is the
    ISO code, which regions_of/us_basis both accept as US.
    """
    locs = [l for l in (p.get("postingLocations") or []) if isinstance(l, dict)]
    if not locs:
        return "", "", "", ""
    parts = []
    for l in locs:
        bits = [sq(l.get("cityName")), sq(l.get("stateCode")),
                sq(l.get("isoCountryCode"))]
        s = ", ".join(b for b in bits if b)
        if not s:
            s = sq(l.get("formattedAddress"))
        if s and s not in parts:
            parts.append(s)
    first = locs[0]
    return ("; ".join(parts), sq(first.get("cityName")),
            sq(first.get("stateCode")), sq(first.get("isoCountryCode")))


def row_of(p, client, board_code, culture, company, pay_name):
    title = sq(p.get("jobTitle"))
    if not title:
        return None
    etype = CLASSIFY(pay_name or None, title)
    if etype is None:                      # DAYFORCE_KEEP_ALL=0: contingent only
        return None
    loc, city, state, country = location_of(p)
    if p.get("hasVirtualLocation") is True:
        mode, remote = "Remote", True
    else:
        mode = cc.work_mode(text=loc + " " + title)
        remote = mode == "Remote"
    jid = str(p.get("jobPostingId"))
    posted = cc.ymd(iso=p.get("postingStartTimestampUTC")) \
        if p.get("postingStartTimestampUTC") else ""
    if not posted:
        posted = ledger_posted("%s/%s:%s" % (client, board_code, jid))
    base = "%s/%s/%s/%s" % (HOST, culture, client, board_code)
    return cc.row(
        source=SOURCE, source_type="ats",
        company=company, title=title, employment_type=etype,
        work_mode=mode, location=loc, city=city, state=state, country=country,
        remote=remote, posted=posted,
        apply_url="%s/jobs/%s" % (base, jid), job_id=jid,
        board="%s/%s" % (client, board_code), board_url=base)


def fetch_board(client, site):
    """-> (verdict, rows).  verdict in {"live", "empty"}; Dead/Transient raise."""
    ctx = sitecontext(client, site)
    # The SERVER's spelling, so the same board always emits the same URLs no
    # matter which case the tenant file happened to record.
    client = sq(ctx.get("clientNamespace")).lower() or client
    board_code = sq(ctx.get("jobBoardCode")).lower() or site
    culture = sq(ctx.get("cultureCode")) or "en-US"
    jbid = ctx.get("jobBoardId")
    company = company_of(ctx, client)

    posts, total = sweep(client, board_code, culture)
    if not posts:
        # maxCount 0 with an empty page is a real, live, vacancy-free board.
        # maxCount > 0 with an empty page is the server contradicting itself —
        # NOT an empty board, so it must stay pending rather than be banked as
        # a completed zero.
        if total:
            raise Transient("maxCount=%d but page 0 returned nothing" % total)
        return "empty", []

    # Pay-class labels.  One class covering the whole board costs nothing; the
    # per-class sweeps only run when the board really is partitioned.
    pay_of = {}
    classes = payclasses(client, jbid, culture) if USE_PAYCLASS else []
    if len(classes) == 1:
        pay_of = {i: classes[0][1] for i in posts}
    elif len(classes) > 1:
        for cid, cname in classes:
            try:
                sub, _ = sweep(client, board_code, culture, pay_class=cid)
            except (Dead, Transient):
                continue           # a label, never a gate
            for i in sub:
                pay_of.setdefault(i, cname)

    rows = []
    for i, p in posts.items():
        r = row_of(p, client, board_code, culture, company, pay_of.get(i))
        if r:
            rows.append(r)
    return "live", rows


def work(w, client, site):
    key = "%s/%s" % (client, site)
    try:
        verdict, rows = fetch_board(client, site)
    except Dead as e:
        with _lock:
            _stats["dead"] += 1
            _state[key] = {"status": "dead", "why": e.reason, "at": TODAY}
        return
    except Transient as e:
        with _lock:
            _stats["transient"] += 1
        print("  TRANSIENT %s: %s" % (key, e), flush=True)
        return
    except Exception as e:                                     # noqa: BLE001
        # Anything unforeseen is transient by policy: the board stays pending.
        with _lock:
            _stats["transient"] += 1
        print("  TRANSIENT %s: %s: %s" % (key, type(e).__name__, e), flush=True)
        return
    with _lock:
        for r in rows:
            w.write(r)
        _stats["rows"] += len(rows)
        _stats["live" if verdict == "live" else "empty"] += 1
        _state[key] = {"status": verdict, "jobs": len(rows), "at": TODAY}


def load_boards():
    path = TENANTS if os.path.exists(TENANTS) else FALLBACK_TENANTS
    if not os.path.exists(path):
        print("dayforce: no tenant list (%s) — run dayforce_discover_boards.py"
              % TENANTS, flush=True)
        return []
    out = []
    seen = set()
    with open(path, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            t = (r.get("tenant") or "").strip().lower()
            client = (r.get("client") or "").strip().lower()
            siteid = (r.get("site") or "").strip().lower()
            if not client or not siteid:
                if "/" not in t:
                    continue
                client, siteid = t.split("/", 1)
            key = "%s/%s" % (client, siteid)
            if key in seen:
                continue
            seen.add(key)
            try:
                n = int(r.get("n_rows") or 0)
            except ValueError:
                n = 0
            out.append((client, siteid, n))
    return out


def main():
    boards = load_boards()
    already = 0
    if ONLY:
        todo = [(c, s) for c, s, _ in boards if "%s/%s" % (c, s) in ONLY]
        missing = [k for k in ONLY if k not in {"%s/%s" % (c, s)
                                                for c, s, _ in boards}]
        for k in missing:                      # allow ad-hoc probes
            if "/" in k:
                todo.append(tuple(k.split("/", 1)))
    else:
        todo = [(c, s) for c, s, _ in boards
                if REFRESH or (_state.get("%s/%s" % (c, s)) or {}).get("status")
                not in ("live", "empty", "dead")]
        # Corpus-ranked boards first (n_rows), then shuffle the long CDX tail so
        # a budgeted run does not always probe the same alphabetical prefix.
        head = [(c, s) for c, s, n in boards if n > 0
                and (c, s) in set(todo)]
        tail = [x for x in todo if x not in set(head)]
        random.shuffle(tail)
        todo = head + tail
        already = len(boards) - len(todo)
    if LIMIT:
        todo = todo[:LIMIT]
    print("dayforce: %d boards, %d to fetch (%d already recorded), "
          "keep_all=%s payclass=%s rps=%.1f workers=%d"
          % (len(boards), len(todo), already, KEEP_ALL, USE_PAYCLASS,
             RPS, WORKERS), flush=True)
    if not todo:
        return 0

    w = cc.Writer(OUT)
    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    done = 0
    stopped = False
    try:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(work, w, c, s) for c, s in todo]
            for _ in cf.as_completed(futs):
                done += 1
                if done % 25 == 0:
                    with _lock:
                        cc.save_state(STATE, _state)
                        if _ledger_dirty[0]:
                            cc.save_state(LEDGER, _ledger)
                    print("  %d/%d  %s" % (done, len(todo), _stats), flush=True)
                if deadline and time.time() > deadline:
                    stopped = True
                    for f in futs:
                        f.cancel()
                    break
    finally:
        with _lock:
            cc.save_state(STATE, _state)
            if _ledger_dirty[0]:
                cc.save_state(LEDGER, _ledger)
        w.close()
    print("DONE dayforce: %s  (429 backoffs: %d)%s"
          % (_stats, _n429[0],
             "  [BUDGET REACHED — rest still pending]" if stopped else ""),
          flush=True)
    # Exit 1 when boards are still pending, matching the ats/icims convention
    # that lets a re-run revisit throttled boards.
    return 1 if (_stats["transient"] or stopped) else 0


if __name__ == "__main__":
    sys.exit(main())
