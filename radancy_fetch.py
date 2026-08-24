#!/usr/bin/env python3
"""
radancy_fetch.py — the Radancy / TalentBrew enterprise-career-site pipe.

Radancy (the platform is branded "TalentBrew") is not an ATS you can enumerate:
there is no *.radancy.com tenant host.  Every customer runs it on its own vanity
domain — jobs.walgreens.com, www.att.jobs, jobs.citi.com, search.jobs.barclays,
www.commonspirit.careers — so the board list has to be mined out of the corpus.
radancy_discover_boards.py does that; this file fetches what it found.

THE ROUTE  (reverse-engineered 2026-08-20; no public API is documented)
======================================================================
Every TalentBrew site serves its own search results over an unauthenticated
JSON endpoint.  It is the site's own AJAX call — the search page carries
`data-ajax-url="/en/search-jobs/results"` and
`data-ajax-post-url="/en/search-jobs/resultspost"`, and the criteria model is
readable in https://tbcdn.talentbrew.com/js/client/search.js.

    GET  https://{host}{prefix}/search-jobs/results?<criteria>
    POST https://{host}{prefix}/search-jobs/resultspost      <criteria as JSON>

    -> {"filters": "<html>", "results": "<html>",
        "hasJobs": bool, "hasContent": bool}

`results` is an HTML fragment, not data, but it is a STRUCTURED fragment and
the same across every theme in the two places that matter:

    <section id="search-results" ... data-total-results="23511"
             data-total-pages="1568" data-records-per-page="15" ...>
    <a href="/en/job/hood-river/shift-lead/1242/99474449040"
       data-job-id="99474449040"> <h2>Shift Lead</h2>
       <span class="job-location">Hood River, Oregon</span> </a>

Measured across all 76 live boards: the block parser used here extracts a
title and a location on 76/76.  Themes differ in what they add — a date on
28/76, a job-type on 7/76, a "Remote: No" flag, a facility/property line, a
req id — and in WHERE they put it, inside or beside the anchor.

    data-total-results  IS NOT THE JOB COUNT.  It is jobs + "content" hits
    (career-advice articles).  Measured: synopsys 443 = 413 jobs + 30 content,
    wegmans 656 = 548 + 108, bupa 377 = 270 + 107.  data-total-job-results is
    the job count, and every decision in this file — when paging stops, and
    whether a board needs a facet partition — is made on that one.

`prefix` is "" or "/en" per tenant (56 / 20 of the 76) and is stored in
tenants/radancy.csv.  It is not cosmetic: jobs.walgreens.com/search-jobs/results
answers HTTP 404.

WHY NOT THE OTHER TWO ROUTES
    JSON-LD JobPosting — real, on every job page, and it carries a genuine
    `datePosted`.  It also costs one 78KB-570KB page per posting: 105,430
    requests and ~30GB for this family.  Used here for nothing.
    Sitemaps — /sitemap.xml and /sitemap-jobs.xml exist but list CATEGORY
    pages, not postings (checked on att: 7,347 URLs, zero /job/ entries).

RecordsPerPage AND THE 10,000-RESULT WINDOW  (measured on walgreens, 23,511)
===========================================================================
RecordsPerPage is not capped at a page size — 1, 15, 100, 500, 1000 and 5000
all return exactly that many rows.  What IS capped is the WINDOW:
    RecordsPerPage=10000  CurrentPage=1  -> 10,000 postings
    RecordsPerPage=10000  CurrentPage=2  -> 0 postings, HTTP 200, no error
    RecordsPerPage=1000   CurrentPage=10 -> 1,000  (offset 9,000)
    RecordsPerPage=1000   CurrentPage=11 -> 0      (offset 10,000)
    RecordsPerPage=25000  CurrentPage=1  -> 10,000  (clamped, not an error)
So offset+size <= 10,000, and past it the endpoint answers 200 with an empty
list rather than an error.  Unlike Workday it does NOT wrap back to page 0 —
but the paging loop still terminates on "this page contributed no new job id"
as well as on the offset ceiling, because a silent 200 is exactly the shape
that makes a loop run forever.

BEYOND 10,000: FACET PARTITION
    Adding &SearchFiltersModuleName=Search+Filters to the criteria makes the
    endpoint also return the `filters` fragment — a full facet dump (5MB on
    walgreens) of checkboxes:
        <input class="filter-checkbox" data-facet-type="3"
               data-id="6252001-4829764" data-count="261"
               data-display="Alabama, United States" data-field-name="">
    Facet types: 1 category, 2 country, 3 state/region, 4 city, 5 custom
    field, 6 company division.  Measured on walgreens (total 23,511):
        type 3  88 values   sum 23,511   max 1,802   <- exact partition
        type 4  3,755       sum 23,496   max 278
        type 1  27          sum 46,576   max 21,910  <- multi-valued, useless
        type 5  5,064       sum 115,527  max 23,303  <- multi-valued
    So the axis is chosen by MEASUREMENT, not by name: a facet type qualifies
    only if its counts sum to ~the board total (single-valued) and its largest
    value fits inside the window.  Type 1 and 5 are rejected on walgreens
    precisely because a job carries several of them and the sum overshoots.

    Slices are sent back as the model-bound FacetFilters array, either as a
    JSON POST body to /search-jobs/resultspost or, when POST is not available,
    as FacetFilters[0].ID / .FacetType / .Count / .Display / .IsApplied query
    parameters.  Both verified against the 1,802-row Illinois slice.

HOW A DEAD RADANCY BOARD LIES TO YOU  (rule 1 — never trust the status code)
===========================================================================
  * www.lockheedmartinjobs.com answers HTTP 200 text/html on
    /search-jobs/results for BOTH prefixes.  The host is retired: it
    redirects into https://www.lockheedmartin.com/en-us/404.html, which is a
    63KB branded HTML page served with a 200.  Reading that as "a board with
    no jobs" would delist it forever, and reading it as a transient failure
    would retry a 404 page for ever.
  * 16 hosts in the corpus (careers.dunkindonuts.com, careers.stryker.com,
    jobs.raisingcanes.com ...) carry a Radancy-looking /{slug}/job/{32-HEX}
    URL and answer HTTP 404 on this endpoint.  They are NOT this platform and
    are excluded at discovery, not marked dead here.
  So the ONLY thing that counts as a board is: 200 + content-type json +
  parses to a dict + has a string `results` + that fragment contains
  data-total-results.  Everything else is dead or transient, per get():
    3xx off the search path / 404 / 410 / 400 / 401 / 403   -> DEAD
    200 that fails the shape test                            -> DEAD
    200 that passes with total=0                             -> LIVE, empty
    429 / 5xx / timeout / connection error                   -> TRANSIENT:
        the board is left OUT of the state file so the next run retries it
        (rule 3 — an errored board must never be recorded as a done empty one)

THROTTLE  (rule 2 — the ratchet that halved throughput)
    slow_down() multiplies a host's interval by 1.5 up to a 0.5s floor and
    pauses that host for 20s; speed_up() walks it back 0.9x after 20 clean
    responses.  Recovery is deliberately slower than the backoff so a host
    that is genuinely refusing still wins.  Rates are PER HOST because every
    board is a different domain — one slow tenant must not throttle the other
    75.

apply_url  (rule 4)
    "https://{host}" + the href the board itself printed, query string
    stripped.  That is the employer's own posting URL
    (https://jobs.walgreens.com/en/job/hood-river/shift-lead/1242/99474449040),
    it is what the JSON-LD `url` field on the job page says, and it is stable
    across runs — the trailing 11-digit id is the Radancy posting id.

POSTED DATES — READ THIS BEFORE TRUSTING THEM
=============================================
None of the three available dates is reliably the employer's posting date, and
this family should be treated as date_weak:

  1. `job-date-posted` in the list markup.  Present on 29 of 76 boards.
     Formats seen: 08/20/2026, 08-19-2026, "Aug. 19, 2026",
     "Post Date | 08/20/2026", and an EMPTY span on 2 boards (synopsys,
     empower) — all handled.
  2. /rss `<pubDate>`.  Present on every board, one request for the whole
     feed.  It is a FEED timestamp, not the posting date: att job
     96955813648 has pubDate "Sat, 11 Jul 2026" and JSON-LD datePosted
     "2026-6-26".  Feeds are complete on most boards (att 2,220 items for a
     2,220-job board; citi 3,480/3,480; UHG 5,555/5,555) but are hard-capped
     at 500 on some tenants (walgreens 500 of 23,511; walmart.ca 500 of
     4,700), so it can never be the enumeration route — only a date source.
  3. First-seen.  What is left.

  And the two real dates disagree in both directions: pwc's list date equals
  its JSON-LD datePosted, moody's list date is 14 days LATER, spectrum's is
  44 days EARLIER.  So the ledger (radancy_posted_ledger.json) pins the
  FIRST/earliest date ever recorded per job id and later runs may only move a
  date EARLIER, never forward — otherwise every re-published req would look
  freshly posted on every run.  THE LEDGER IS NEVER ROTATED.
  The ledger's `precision` field records which of the three produced each
  date, using the repo's existing convention (workday/icims/comeet/gem all
  write one), so date_basis_index.py can read it: `list_date`, `rss_feed`,
  `first_seen`.  Only `list_date` has any claim to being the employer's own
  date, and even that one is a re-stamp on some tenants.

COVERAGE AS RUN 2026-08-20  (smoke test, 25 of the 76 boards)
    RADANCY_KEEP_ALL=1 RADANCY_WORKERS=8 RADANCY_RPS=2 \
      RADANCY_ONLY=<25 hosts> python3 radancy_fetch.py

    25 live, 0 dead, 0 transient, 0 shortfalls, 0 429 backoffs, 77 seconds.
    70,100 rows == 70,100 distinct apply_urls; every board returned exactly
    its own data-total-job-results, walgreens included (23,511 recovered
    through the 88-slice state partition from a 10,000-row window).
    0 blank titles, 0 blank locations, 0 blank companies, 0 blank dates.
    Date provenance over those rows: list_date 10,708 / rss_feed 30,900 /
    first_seen 28,492 — and 23,011 of the first_seen are walgreens alone,
    whose feed is capped at 500.
    Whole-family estimate from the discovery probe: 105,413 postings.

OUTPUT / STATE
    contract_norm/radancy.jsonl        rows (board-scoped merge, see _merge)
    radancy_fetch_state.json           per-board status, resumable
    radancy_posted_ledger.json         append-only date ledger

ENV
    RADANCY_KEEP_ALL=1     keep every employment type (default: contingent only)
    RADANCY_BUDGET_MIN=N   wall-clock budget; unfinished boards stay PENDING
    RADANCY_RPS=f          per-host request rate (default 2.0)
    RADANCY_WORKERS=N      boards in flight (default 8)
    RADANCY_RPP=N          RecordsPerPage (default 1000)
    RADANCY_LIMIT=N        only the first N pending boards (smoke tests)
    RADANCY_ONLY=a,b,c     only these hosts (smoke tests)
    RADANCY_REFRESH=1      re-fetch boards already recorded live/dead
    RADANCY_RSS=0          skip the /rss date pass entirely
    RADANCY_MAX_DEPTH=N    facet recursion depth (default 2)
"""
import bisect
import concurrent.futures as cf
import csv
import email.utils
import html
import json
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
TENANTS = os.path.join(BASE, "tenants", "radancy.csv")
OUT = os.path.join(cc.NORM_DIR, "radancy.jsonl")
PART = OUT + ".part"
STATE = os.path.join(BASE, "radancy_fetch_state.json")
LEDGER = os.path.join(BASE, "radancy_posted_ledger.json")

SOURCE = "radancy"

RPS = float(os.environ.get("RADANCY_RPS") or 2.0)
WORKERS = int(os.environ.get("RADANCY_WORKERS") or 8)
RPP = max(1, min(10000, int(os.environ.get("RADANCY_RPP") or 1000)))
BUDGET_MIN = int(os.environ.get("RADANCY_BUDGET_MIN") or 0)
LIMIT = int(os.environ.get("RADANCY_LIMIT") or 0)
ONLY = [h.strip() for h in (os.environ.get("RADANCY_ONLY") or "").split(",")
        if h.strip()]
REFRESH = os.environ.get("RADANCY_REFRESH") == "1"
KEEP_ALL = os.environ.get("RADANCY_KEEP_ALL") == "1"
USE_RSS = os.environ.get("RADANCY_RSS") != "0"
MAX_DEPTH = int(os.environ.get("RADANCY_MAX_DEPTH") or 2)

TIMEOUT = 60
TRIES = 5
WINDOW = 10000                 # offset + size ceiling, measured
RSS_MAX_BYTES = int(os.environ.get("RADANCY_RSS_MAX_MB") or 200) * 1024 * 1024

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = {}
_interval = {}
_pause_until = {}
_ok_streak = {}
_n429 = [0]

_state = cc.load_state(STATE)
_ledger = cc.load_state(LEDGER)
_stats = {"live": 0, "dead": 0, "transient": 0, "rows": 0, "short": 0}


class Transient(Exception):
    """Retryable failure.  RAISED, never returned, so the caller leaves the
    board out of the state file instead of recording a completed empty board."""


class Dead(Exception):
    """The board is gone / is not a TalentBrew site.  Deterministic; recorded."""


# ---- throttle: the speed_up()/slow_down() pair from ats_fetch.py -----------
def throttle(host):
    while True:
        with _rate_lock:
            now = time.time()
            _interval.setdefault(host, 1.0 / RPS)
            _next_t.setdefault(host, 0.0)
            _pause_until.setdefault(host, 0.0)
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
        _interval[host] = min(_interval.get(host, 1.0 / RPS) * 1.5, 0.5)
        _n429[0] += 1


def speed_up(host):
    """Walk a host's interval back toward RPS after a run of clean responses.

    Without this, slow_down() is a one-way ratchet: a couple of 429s in the
    first minute pin the host at 2 req/s for the rest of the run whatever RPS
    says.  Recovery (0.9x per 20 clean) is slower than backoff (1.5x per 429)
    on purpose."""
    with _rate_lock:
        n = _ok_streak.get(host, 0) + 1
        if n < 20:
            _ok_streak[host] = n
            return
        _ok_streak[host] = 0
        floor = 1.0 / RPS
        if _interval.get(host, floor) > floor:
            _interval[host] = max(_interval[host] * 0.9, floor)


# ---- http -----------------------------------------------------------------
# data-total-results is jobs PLUS "content" hits (career-advice articles, blog
# posts) — measured: synopsys 443 = 413 jobs + 30 content, wegmans 656 = 548 +
# 108, bupa 377 = 270 + 107.  data-total-job-results is the job count and is
# the one every decision here is made on; reading the wrong one makes the
# paging loop chase rows that do not exist and, worse, pushes a 9,900-job board
# with 500 articles over the 10,000 window and into a needless facet partition.
JOB_TOTAL_RX = re.compile(r'data-total-job-results="(\d+)"')
TOTAL_RX = re.compile(r'data-total-(?:job-)?results="(\d+)"')


def _shape_ok(r):
    """The payload test.  A TalentBrew answer is JSON whose `results` fragment
    carries data-total-results.  Nothing else counts, whatever the status."""
    ct = (r.headers.get("content-type") or "").split(";")[0].strip().lower()
    if "json" not in ct:
        return None
    try:
        j = r.json()
    except ValueError:
        return None
    if not isinstance(j, dict) or not isinstance(j.get("results"), str):
        return None
    if not TOTAL_RX.search(j["results"]):
        return None
    return j


def request(s, host, url, body=None, hops=0):
    """Throttled request -> validated payload dict.

    Raises Dead for a deterministic refusal and Transient for anything that a
    retry could fix.  allow_redirects=False: a retired vanity host bounces to
    the parent corporate site, which answers 200 with branded HTML, and
    following it destroys the only evidence that the board is gone.

    ONE exception, and it is the difference between a self-healing pipe and one
    that delists a live employer: a redirect that stays on the SAME host and
    still points at /search-jobs/ is a locale-prefix change, not a retirement.
    jobs.walgreens.com/search-jobs/results answers 301 to
    jobs.walgreens.com/en/search-jobs/results — a board that moved its prefix
    after discovery would otherwise be marked permanently dead."""
    last = None
    for attempt in range(TRIES):
        throttle(host)
        try:
            if body is None:
                r = s.get(url, timeout=TIMEOUT, allow_redirects=False,
                          headers={"Accept": "application/json"})
            else:
                r = s.post(url, json=body, timeout=TIMEOUT,
                           allow_redirects=False,
                           headers={"Accept": "application/json",
                                    "Content-Type":
                                        "application/json; charset=utf-8"})
            code = r.status_code
            if code == 200:
                j = _shape_ok(r)
                if j is not None:
                    speed_up(host)
                    return j
                # 200 with an HTML shell.  Deterministic: no number of retries
                # turns lockheedmartin.com/en-us/404.html into JSON.  One retry
                # is still spent in case a 200-wrapped edge error page is
                # transient.
                if attempt >= 1:
                    raise Dead("200 but not a TalentBrew payload")
                last = "200-bad-shape"
            elif code in (301, 302, 303, 307, 308):
                loc = (r.headers.get("location") or "").strip()
                if hops < 2 and loc:
                    from urllib.parse import urljoin, urlsplit
                    nxt = urljoin(url, loc)
                    sp = urlsplit(nxt)
                    if (sp.netloc or host).lower() == host.lower() \
                            and "/search-jobs/" in sp.path:
                        return request(s, host, nxt, body, hops + 1)
                raise Dead("redirect %d -> %s" % (code, loc[:120]))
            elif code in (400, 401, 403, 404, 410):
                raise Dead("HTTP %d" % code)
            elif code in (429, 418):
                slow_down(host)
                last = "HTTP %d" % code
            else:
                last = "HTTP %d" % code
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    raise Transient("%s failed after %d tries: %s" % (url, TRIES, last))


# ---- criteria -------------------------------------------------------------
def criteria(page=1, rpp=RPP, facets=None, want_filters=False):
    c = {
        "ActiveFacetID": 0,
        "CurrentPage": page,
        "RecordsPerPage": rpp,
        "Distance": 50,
        "RadiusUnitType": 0,
        "Keywords": "",
        "Location": "",
        "ShowRadius": "False",
        "IsPagination": "False",
        "CustomFacetName": "",
        "FacetTerm": "",
        "FacetType": 0,
        "SearchResultsModuleName": "Search Results",
        "SortCriteria": 0,
        "SortDirection": 0,
        "SearchType": 5,
    }
    # SearchFiltersModuleName is omitted unless the facet dump is wanted: with
    # it, every single page also carries a 5MB filters fragment.
    if want_filters:
        c["SearchFiltersModuleName"] = "Search Filters"
    if facets:
        c["FacetFilters"] = facets
    return c


def _qs(c):
    from urllib.parse import quote
    parts = []
    for k, v in c.items():
        if k == "FacetFilters":
            for i, f in enumerate(v):
                for fk, fv in (("ID", f["ID"]), ("FacetType", f["FacetType"]),
                               ("Count", f["Count"]), ("Display", f["Display"]),
                               ("IsApplied", "True"),
                               ("FieldName", f.get("FieldName") or "")):
                    parts.append("FacetFilters%%5B%d%%5D.%s=%s"
                                 % (i, fk, quote(str(fv), safe="")))
        else:
            parts.append("%s=%s" % (k, quote(str(v), safe="")))
    return "&".join(parts)


def fetch_page(s, host, prefix, c, post_ok):
    """One results page.  POST when facets are applied (the criteria model is
    what /resultspost binds and it dodges URL-length limits); GET otherwise,
    which is the shape verified live on all 76 boards."""
    if c.get("FacetFilters") and post_ok[0]:
        try:
            return request(s, host,
                           "https://%s%s/search-jobs/resultspost"
                           % (host, prefix), body=c)
        except Dead:
            # Not every theme exposes the POST route; fall back for the rest
            # of this board rather than declaring it dead.
            post_ok[0] = False
    return request(s, host, "https://%s%s/search-jobs/results?%s"
                   % (host, prefix, _qs(c)))


# ---- parsing --------------------------------------------------------------
_WS = re.compile(r"\s+")
# Anchor that carries BOTH href and data-job-id, in either attribute order.
JOB_RX = re.compile(
    r'<a\b(?=[^>]*\bhref="(?P<href>[^"]+)")(?=[^>]*\bdata-job-id="'
    r'(?P<jid>\d+)")[^>]*>', re.S)
LI_RX = re.compile(r"<li\b", re.I)
TITLE_RX = re.compile(r"<h[1-6][^>]*>(?P<t>.*?)</h[1-6]>", re.S)
TAG_RX = re.compile(r"<[^>]+>")
# span, div AND li: search.jobs.barclays prints its location in
# <div class="job-location"> and careers.bupa.com.au in
# <li class="search-results-list__job-info job-location">, not a span.
EL_OPEN_RX = re.compile(r'<(?P<tag>span|div|li)\b(?P<attrs>[^>]*)>', re.I)
CLASS_RX = re.compile(r'\bclass="([^"]*)"', re.I)

# Class matchers.  Radancy themes are per tenant and name the same field five
# ways -- job-location, sr-job-location, location-name,
# "search-results__job-info location", job-list-info--location -- so these
# match a WORD inside the class attribute rather than a fixed token.
LOC_CLS = re.compile(r"(?:^|[\s_-])locations?(?:$|[\s_-])", re.I)
LOC_SKIP_CLS = re.compile(r"(?:^|[\s_-])(property|icon)(?:$|[\s_-])", re.I)
DATE_CLS = re.compile(r"(?:^|[\s_-])(?:date|date-posted|posted)(?:$|[\s_-])",
                      re.I)
TYPE_CLS = re.compile(r"(?:^|[\s_-])(?:job-?type|job-status|job-schedule|"
                      r"employment-?type)(?:$|[\s_-])", re.I)
REMOTE_CLS = re.compile(r"(?:^|[\s_-])job-remote(?:$|[\s_-])", re.I)


def txt(v):
    return _WS.sub(" ", html.unescape(TAG_RX.sub(" ", v or ""))).strip()


def total_of(results):
    m = JOB_TOTAL_RX.search(results or "") or TOTAL_RX.search(results or "")
    return int(m.group(1)) if m else 0


def _elements(block):
    """Yield (class_attr, inner_html) for every span/div, nesting-aware.

    A non-greedy `<span ...>(.*?)</span>` is wrong here and silently returns
    empty strings: jobs.veolia.com prints
        <span class="job-location"><span class="location-icon-red"></span>
         Fanling, Hong Kong SAR </span>
    and the lazy match stops at the INNER close tag, so the location reads as
    "".  This walks the tag depth instead.
    """
    for m in EL_OPEN_RX.finditer(block):
        cm = CLASS_RX.search(m.group("attrs"))
        if not cm:
            continue
        tag = m.group("tag").lower()
        depth = 1
        i = m.end()
        n = len(block)
        step = re.compile(r"</?%s\b" % tag, re.I)
        while i < n and depth:
            sm = step.search(block, i)
            if not sm:
                i = n
                break
            depth += -1 if sm.group(0)[1] == "/" else 1
            i = sm.end()
            if depth == 0:
                close = block.find(">", i)
                yield cm.group(1), block[m.end():sm.start()]
                i = close + 1 if close >= 0 else i
                break


def _pick(block, cls_rx, skip_cls_rx=None, skip_txt=()):
    for cls, inner in _elements(block):
        if not cls_rx.search(cls):
            continue
        if skip_cls_rx is not None and skip_cls_rx.search(cls):
            continue
        v = txt(inner)
        if not v or any(sk in v.lower() for sk in skip_txt):
            continue
        return v
    return ""


LI_TAG_RX = re.compile(r"</?li\b", re.I)


def _li_spans(results):
    """[(start, end)] for every balanced <li>...</li> in the fragment."""
    stack = []
    spans = []
    for m in LI_TAG_RX.finditer(results):
        if m.group(0)[1] == "/":
            if stack:
                spans.append((stack.pop(), m.start()))
        else:
            stack.append(m.start())
    return spans


def _blocks(results):
    """-> [(jid, href, block_html)] one entry per posting on the page.

    Block-scoped, not anchor-scoped, and that distinction is the whole reason
    this exists.  Radancy themes put the location on either side of the anchor:

      jobs.directv.com   <li><div><h2><a ...>Title</a></h2>
                           <ul><li><span class="job-location">LA, CA</span>
                         -- location is a SIBLING, after the anchor
      careers.bupa.com.au <li><div><h2><a ...>Title
                            <ul><li class="... job-location">Sydney, NSW</li>
                          </ul></a></h2>  -- inside the anchor, and the <h2>
                          only closes after it

    An anchor-scoped parser read directv as 13 jobs with no location at all on
    the first smoke run.  A "last <li> before the next anchor" heuristic then
    truncated bupa's LAST item mid-title, because that item's own info-list
    <li> was the last one on the page.  So the boundary is computed
    STRUCTURALLY: the largest balanced <li>...</li> that contains this anchor
    and no other one.  Themes with no <li> at all (search.jobs.barclays uses
    <div class="list-item">) fall back to anchor-to-anchor.
    """
    anchors = []
    for m in JOB_RX.finditer(results or ""):
        if anchors and anchors[-1][0] == m.group("jid"):
            continue          # a second anchor for the same posting ("View Job")
        anchors.append((m.group("jid"), m.group("href"), m.start()))
    if not anchors:
        return []
    starts = [a[2] for a in anchors]
    best = [None] * len(anchors)
    for s0, e0 in _li_spans(results):
        i = bisect.bisect_left(starts, s0)
        if i >= len(starts) or starts[i] >= e0:
            continue                      # contains no anchor
        if i + 1 < len(starts) and starts[i + 1] < e0:
            continue                      # contains more than one: a wrapper
        if best[i] is None or (e0 - s0) > (best[i][1] - best[i][0]):
            best[i] = (s0, e0)
    out = []
    for i, (jid, href, astart) in enumerate(anchors):
        if best[i]:
            blk = results[best[i][0]:best[i][1]]
        else:
            end = starts[i + 1] if i + 1 < len(anchors) else len(results)
            blk = results[astart:end]
        out.append((jid, href, blk))
    return out


SLUG_RX = re.compile(r"/job/([^/]+)/")


def _slug_location(href):
    """City out of the posting URL, for the boards that print none.

    careers.universityhealth.com lists company/category/salary and no place at
    all; Radancy still puts the city in the URL it generated
    (/job/san-antonio/executive-assistant/43277/87292465280).  Used only when
    the markup has nothing -- never to override a printed location.  The dash
    placeholder capitalone uses (/job/-/-/234/990...) is not a city.
    """
    m = SLUG_RX.search(href or "")
    if not m:
        return ""
    slug = m.group(1).strip("-")
    if not slug or slug in ("-", "n-a", "na"):
        return ""
    return " ".join(w.capitalize() if w.islower() else w
                    for w in slug.split("-"))


def parse_results(results):
    """-> [{jid, href, title, location, date, jtype, remote}] for one page."""
    out = []
    for jid, href, block in _blocks(results):
        tm = TITLE_RX.search(block)
        if tm:
            # citi puts the anchor inside the <h3>; irobot puts the <h2> inside
            # the anchor.  Either way the first heading in the block is it.
            title = txt(tm.group("t"))
        else:
            am = re.search(r"%s(?P<inner>.*?)</a>" % JOB_RX.pattern, block,
                           re.S)
            title = txt(am.group("inner")) if am else ""
        # greystar prints two location-name spans, the first being the property
        # name ("Haiku - The Gladstone"); catalina prints a second job-location
        # span opening "Additional Locations:".  Both are skipped.
        loc = _pick(block, LOC_CLS, LOC_SKIP_CLS, ("additional location",))
        if not loc:
            loc = _slug_location(href)
        out.append({
            "jid": jid,
            "href": href,
            "title": title,
            "location": loc,
            "date": _pick(block, DATE_CLS),
            "jtype": _pick(block, TYPE_CLS),
            "remote": _pick(block, REMOTE_CLS),
        })
    return out


FACET_RX = re.compile(r'<input\b[^>]*class="[^"]*\bfilter-checkbox\b[^"]*"'
                      r'[^>]*>')


def _attr(tag, name):
    m = re.search(name + r'="([^"]*)"', tag)
    return m.group(1) if m else ""


def parse_facets(filters):
    out = []
    for tag in FACET_RX.findall(filters or ""):
        fid = _attr(tag, "data-id")
        ft = _attr(tag, "data-facet-type")
        if not fid or not ft.isdigit():
            continue
        try:
            cnt = int(_attr(tag, "data-count") or 0)
        except ValueError:
            cnt = 0
        out.append({"ID": fid, "FacetType": int(ft), "Count": cnt,
                    "Display": html.unescape(_attr(tag, "data-display")),
                    "IsApplied": True,
                    "FieldName": _attr(tag, "data-field-name")})
    return out


# Preference order is granularity, but qualification is measured (see module
# docstring): state, city, category, custom field, country.
AXIS_ORDER = (3, 4, 1, 5, 2)


def pick_axis(facets, total, exclude=()):
    """The facet type whose values partition this board inside the window.

    A type qualifies only if its counts SUM to about the board total (which is
    what proves a job carries exactly one value of it) and its biggest value
    fits in the 10,000 window.  walgreens' category (type 1) sums to 46,576
    against a 23,511 total — multi-valued, and slicing on it would double-count
    while still leaving a 21,910 slice that cannot be paged."""
    by = {}
    for f in facets:
        by.setdefault(f["FacetType"], []).append(f)
    for ft in AXIS_ORDER:
        if ft in exclude or ft not in by:
            continue
        vals = [f for f in by[ft] if f["Count"] > 0]
        if not vals:
            continue
        s = sum(f["Count"] for f in vals)
        if s < total * 0.97 or s > total * 1.05:
            continue
        if max(f["Count"] for f in vals) >= WINDOW:
            continue
        return ft, vals
    # Nothing partitions cleanly.  Take the finest axis whose slices at least
    # fit the window, even if it double-counts — dedupe by job id absorbs that.
    for ft in AXIS_ORDER:
        if ft in exclude or ft not in by:
            continue
        vals = [f for f in by[ft] if 0 < f["Count"] < WINDOW]
        if vals and sum(f["Count"] for f in vals) >= total * 0.9:
            return ft, vals
    return None, []


# ---- dates ----------------------------------------------------------------
_MON = {m: i + 1 for i, m in enumerate(
    ["jan", "feb", "mar", "apr", "may", "jun", "jul", "aug", "sep", "oct",
     "nov", "dec"])}


def parse_list_date(s):
    """The `job-date-posted` span -> YYYY-MM-DD, or "".

    Formats measured across the boards that print one: 08/20/2026,
    08-19-2026, "Aug. 19, 2026", "Post Date | 08/20/2026",
    "Posted: 08/18/2026", "Date posted: 07/27/2026", "13 Aug" (no year),
    2026-08-20, and an empty span."""
    t = (s or "").strip()
    if not t:
        return ""
    # Labels the themes prepend: "Post Date | 08/20/2026" (scionhealth),
    # "Posted: 08/18/2026" (synopsys), "Date posted: 07/27/2026" (empower).
    if "|" in t:
        t = t.rsplit("|", 1)[1].strip()
    if ":" in t:
        head, tail = t.rsplit(":", 1)
        if re.search(r"[A-Za-z]", head) and not re.search(r"\d:\d", t):
            t = tail.strip()
    t = t.strip(" .")
    m = re.match(r"^(\d{4})-(\d{1,2})-(\d{1,2})$", t)
    if m:
        y, a, b = (int(x) for x in m.groups())
        return _mk(y, a, b)
    m = re.match(r"^(\d{1,2})[/-](\d{1,2})[/-](\d{4})$", t)
    if m:
        a, b, y = (int(x) for x in m.groups())
        # US sites print MM/DD/YYYY; a first component over 12 can only be a
        # day, so the European order is detected rather than assumed.
        return _mk(y, b, a) if a > 12 else _mk(y, a, b)
    m = re.match(r"^([A-Za-z]{3,9})\.?\s+(\d{1,2}),?\s+(\d{4})$", t)
    if m and m.group(1)[:3].lower() in _MON:
        return _mk(int(m.group(3)), _MON[m.group(1)[:3].lower()],
                   int(m.group(2)))
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]{3,9})\.?\s+(\d{4})$", t)
    if m and m.group(2)[:3].lower() in _MON:
        return _mk(int(m.group(3)), _MON[m.group(2)[:3].lower()],
                   int(m.group(1)))
    # Year-less: search.jobs.barclays prints "13 Aug".  Resolve to the most
    # recent occurrence that is not in the future -- a board never shows a
    # posting date that has not happened yet, so a month/day ahead of today
    # belongs to last year.
    m = re.match(r"^(\d{1,2})\s+([A-Za-z]{3,9})\.?$", t)
    if not (m and m.group(2)[:3].lower() in _MON):
        m2 = re.match(r"^([A-Za-z]{3,9})\.?\s+(\d{1,2})$", t)
        m = None
        if m2 and m2.group(1)[:3].lower() in _MON:
            mo, day = _MON[m2.group(1)[:3].lower()], int(m2.group(2))
        else:
            return ""
    else:
        mo, day = _MON[m.group(2)[:3].lower()], int(m.group(1))
    now = datetime.now(timezone.utc)
    cand = _mk(now.year, mo, day)
    if cand and cand <= now.strftime("%Y-%m-%d"):
        return cand
    return _mk(now.year - 1, mo, day)


def _mk(y, mo, d):
    try:
        return datetime(y, mo, d).strftime("%Y-%m-%d")
    except ValueError:
        return ""


RSS_ITEM_RX = re.compile(r"<item>(.*?)</item>", re.S)
RSS_LINK_RX = re.compile(r"<link>([^<]+)</link>")
RSS_DATE_RX = re.compile(r"<pubDate>([^<]+)</pubDate>")
JID_IN_URL_RX = re.compile(r"/(\d{6,})(?:[/?#]|$)")


def rss_dates(s, host, prefix):
    """{job_id: YYYY-MM-DD} from the board's /rss feed, or {}.

    Streamed and parsed incrementally: the feed embeds every job description,
    so it is 15MB on att and 92MB on jobs.aus.com decoded (~1/6 of that on the
    wire).  Only <link> and <pubDate> are kept.  Never authoritative — see the
    POSTED DATES section of the module docstring — and never used to decide
    whether a board is live."""
    out = {}
    try:
        throttle(host)
        r = s.get("https://%s%s/rss" % (host, prefix), timeout=TIMEOUT,
                  stream=True, allow_redirects=False,
                  headers={"Accept": "application/xml,text/xml,*/*"})
        if r.status_code != 200:
            r.close()
            return out
        ct = (r.headers.get("content-type") or "").lower()
        if "xml" not in ct and "rss" not in ct:
            r.close()
            return out
        buf = ""
        n = 0
        for chunk in r.iter_content(1 << 20):
            n += len(chunk)
            buf += chunk.decode("utf-8", "replace")
            last = 0
            for m in RSS_ITEM_RX.finditer(buf):
                _rss_item(m.group(1), out)
                last = m.end()
            if last:
                buf = buf[last:]
            if n > RSS_MAX_BYTES:
                break
        r.close()
    except (requests.RequestException, ValueError):
        return out
    return out


def _rss_item(item, out):
    lm = RSS_LINK_RX.search(item)
    dm = RSS_DATE_RX.search(item)
    if not lm or not dm:
        return
    jm = JID_IN_URL_RX.search(lm.group(1).split("?")[0])
    if not jm:
        return
    try:
        dt = email.utils.parsedate_to_datetime(dm.group(1).strip())
    except (TypeError, ValueError):
        return
    if dt is None:
        return
    out[jm.group(1)] = dt.strftime("%Y-%m-%d")


def ledger_posted(key, candidate, precision):
    """First/earliest date ever recorded for a posting wins, forever.

    Radancy re-stamps postings: moody's list date moves to today on a req that
    JSON-LD says was posted 14 days ago.  Letting a later run push a date
    forward would make a three-month-old posting land in every "new jobs" cut,
    for ever.  Dates may only move EARLIER."""
    with _lock:
        rec = _ledger.get(key)
        if rec is None:
            _ledger[key] = {"posted": candidate or TODAY,
                            "precision": precision if candidate
                            else "first_seen",
                            "first_seen": TODAY}
            return _ledger[key]["posted"], _ledger[key]["precision"]
        if candidate and (not rec.get("posted") or candidate < rec["posted"]):
            rec["posted"] = candidate
            rec["precision"] = precision
        return rec.get("posted") or TODAY, rec.get("precision") or "first_seen"


# ---- geography ------------------------------------------------------------
US_STATES = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "florida", "georgia", "hawaii", "idaho",
    "illinois", "indiana", "iowa", "kansas", "kentucky", "louisiana", "maine",
    "maryland", "massachusetts", "michigan", "minnesota", "mississippi",
    "missouri", "montana", "nebraska", "nevada", "new hampshire", "new jersey",
    "new mexico", "new york", "north carolina", "north dakota", "ohio",
    "oklahoma", "oregon", "pennsylvania", "rhode island", "south carolina",
    "south dakota", "tennessee", "texas", "utah", "vermont", "virginia",
    "washington", "west virginia", "wisconsin", "wyoming",
    "district of columbia", "puerto rico",
}
US_ABBR = {
    "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA", "HI", "ID",
    "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD", "MA", "MI", "MN", "MS",
    "MO", "MT", "NE", "NV", "NH", "NJ", "NM", "NY", "NC", "ND", "OH", "OK",
    "OR", "PA", "RI", "SC", "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV",
    "WI", "WY", "DC", "PR",
}
# Strings the boards print where a place name would go.  None of them is a
# city, and letting them through would create a "Multiple Locations, ..." city.
NON_PLACE = re.compile(
    r"^(multiple locations?|various|various locations?|nationwide|remote|"
    r"virtual|work from home|united states|usa|us|any|n/?a|-|—|field)$", re.I)


# Words that mark a FACILITY rather than a place.  A one-part location
# containing any of these is a building, not a city.
_FACILITY = re.compile(
    r"\b(hospital|medical\s+cent(er|re)|health\s+cent(er|re)|clinic|campus|"
    r"cancer\s+cent(er|re)|surgery|surgical|institute|laborator|pharmacy|"
    r"nursing|rehab|hospice|dialysis|urgent\s+care|multiple\s+locations|"
    r"corporate\s+office|headquarters|distribution\s+cent(er|re)|"
    r"warehouse|plant|store\s*#|division|region)\b", re.I)


def split_location(loc):
    """-> (city, state, country).  Blank beats wrong: the repo's geo lanes read
    these columns directly.

    Three-part locations are NOT reliably city/state/country.  Measured on the
    smoke run: "Greenville, South Carolina, United States" is, but
    "Summerville, Dorchester, SC" (spectrum) and "Rock Hill, York County, SC"
    (tenet) are city/COUNTY/state — reading the last part as the country put
    161 rows in a country called "SC", plus CA, OH, NC and PA.  So the last
    part is tested against the US state list first and only treated as a
    country when it is not one.
    """
    t = (loc or "").strip().strip(",")
    if not t or NON_PLACE.match(t):
        return "", "", ""
    parts = [p.strip() for p in t.split(",") if p.strip()]
    if len(parts) == 1:
        p = parts[0]
        if p.upper() in US_ABBR or p.lower() in US_STATES:
            return "", p, "United States"
        # A bare token is usually a CITY -- 'San Antonio', 'Pune', 'Chennai',
        # 'Mississauga', 'London'.  Returning blank here was the single
        # biggest source of geo-less rows in this family (of 15,003 rows with
        # no city/state/country, most were one-part city names), and a row
        # with no city is invisible to every geo lane in this repo.
        #
        # Facility names are the exception and must stay blank: a board like
        # commonspirit lists "Baylor St Luke's Medical Center" in the same
        # field, and calling that a city is exactly the "blank beats wrong"
        # failure this function's docstring is about.
        if _FACILITY.search(p) or len(p) > 40 or not any(c.isalpha() for c in p):
            return "", "", ""
        return p, "", ""
    city = "" if NON_PLACE.match(parts[0]) else parts[0]
    last = parts[-1]
    if last.upper() in US_ABBR or last.lower() in US_STATES:
        return city, last, "United States"
    if len(parts) >= 3:
        state = parts[-2]
        country = last
        if state.upper() in US_ABBR or state.lower() in US_STATES:
            country = "United States"
        return city, state, country
    state = parts[1]
    return city, state, ""


def work_mode_of(loc, jtype, title, remote_field=""):
    # jobs.directv.com prints an explicit "<b>Remote:</b> No" span.  Reading
    # that block as free text would match the word "remote" and flag every
    # on-site job in the company as remote, which is the exact opposite of what
    # it says, so the structured field is read first and believed.
    rf = (remote_field or "").strip().lower()
    if rf:
        val = rf.split(":", 1)[1].strip() if ":" in rf else rf
        if val in ("no", "false", "0", "not remote", "onsite", "on-site"):
            return "On-site"
        if val in ("yes", "true", "1", "remote", "fully remote"):
            return "Remote"
        if "hybrid" in val:
            return "Hybrid"
    t = (jtype or "").strip().lower()
    if "remote" in t or "virtual" in t or "work from home" in t:
        return "Remote"
    if "hybrid" in t:
        return "Hybrid"
    if t.startswith("on-site") or t.startswith("onsite") or t == "on site":
        return "On-site"
    blob = "%s %s" % (loc or "", title or "")
    if re.search(r"\bhybrid\b", blob, re.I):
        return "Hybrid"
    if re.search(r"\b(remote|virtual|work from home|telecommute)\b", blob,
                 re.I):
        return "Remote"
    return ""


# ---- one board ------------------------------------------------------------
def page_all(s, host, prefix, facets, post_ok, seen, sink, budget_at):
    """Page one slice to the end of the window.  -> reported job total for it.

    `seen` is board-wide, so a facet slice's first page is often entirely rows
    the unfaceted pass already collected.  That is why the terminator is NOT
    "this page contributed nothing new": on walgreens the 1,802-row Illinois
    slice would stop after page 1 and silently drop 802 postings.  What ends a
    slice is a short page, the 10,000 offset ceiling, or a page that repeats
    the previous one verbatim (the only shape that could loop for ever).
    """
    first = fetch_page(s, host, prefix, criteria(1, RPP, facets), post_ok)
    total = total_of(first["results"])
    page = 1
    payload = first
    prev_ids = None
    while True:
        rows = parse_results(payload["results"])
        ids = {r["jid"] for r in rows}
        for r in rows:
            if r["jid"] in seen:
                continue
            seen.add(r["jid"])
            sink.append(r)
        if not rows or len(rows) < RPP or ids == prev_ids:
            break
        if page * RPP >= min(total, WINDOW):
            break
        prev_ids = ids
        if budget_at and time.time() > budget_at:
            raise Transient("budget reached mid-board")
        page += 1
        payload = fetch_page(s, host, prefix, criteria(page, RPP, facets),
                             post_ok)
    return total


def fetch_board(s, host, prefix, budget_at):
    """-> (rows, total, note).  Raises Dead / Transient."""
    seen = set()
    sink = []
    post_ok = [True]
    total = page_all(s, host, prefix, None, post_ok, seen, sink, budget_at)
    note = ""
    if total > WINDOW:
        note = _partition(s, host, prefix, total, post_ok, seen, sink,
                          budget_at, depth=1, facets=None)
    return sink, total, note


def _partition(s, host, prefix, total, post_ok, seen, sink, budget_at, depth,
               facets, parent_axis=None):
    """Slice a board that does not fit the 10,000-result window.

    The parent's axis is excluded on the way down: inside a single-state slice
    the state facet has exactly one value equal to the slice, which sums to the
    slice total and would be re-picked for ever."""
    if depth > MAX_DEPTH:
        return "depth-capped"
    payload = fetch_page(s, host, prefix,
                         criteria(1, 1, facets, want_filters=True), post_ok)
    axis, vals = pick_axis(parse_facets(payload.get("filters") or ""), total,
                           exclude=(parent_axis,) if parent_axis else ())
    if not axis:
        return "no-partition-axis"
    note = "axis=%d slices=%d" % (axis, len(vals))
    for v in sorted(vals, key=lambda f: -f["Count"]):
        sub = (facets or []) + [v]
        try:
            st = page_all(s, host, prefix, sub, post_ok, seen, sink, budget_at)
        except Dead:
            continue
        if st > WINDOW:
            note += " " + _partition(s, host, prefix, st, post_ok, seen, sink,
                                     budget_at, depth + 1, sub, axis)
    return note


def rows_of(host, prefix, company, jobs, rssmap):
    out = []
    for j in jobs:
        title = j["title"]
        if not title:
            continue
        href = j["href"].split("?")[0]
        if href.startswith("http"):
            apply_url = href
        else:
            apply_url = "https://%s%s" % (host, href if href.startswith("/")
                                          else "/" + href)
        et = cc.classify_any(j["jtype"], title)
        if not KEEP_ALL and not cc.classify(j["jtype"], title):
            continue
        cand = parse_list_date(j["date"])
        prec = "list_date"
        if not cand:
            cand = rssmap.get(j["jid"], "")
            prec = "rss_feed" if cand else "first_seen"
        posted, _prec = ledger_posted("%s:%s" % (host, j["jid"]), cand, prec)
        city, state, country = split_location(j["location"])
        mode = work_mode_of(j["location"], j["jtype"], title,
                            j.get("remote", ""))
        r = cc.row(
            source=SOURCE, source_type="ats", company=company, title=title,
            employment_type=et, work_mode=mode, location=j["location"],
            city=city, state=state, country=country, remote=(mode == "Remote"),
            posted=posted, apply_url=apply_url, job_id=j["jid"], board=host,
            board_url="https://%s%s/search-jobs" % (host, prefix))
        out.append(r)
    return out


def work(host, prefix, company, budget_at):
    s = cc.session()
    s.headers.update({"User-Agent": cc.UA,
                      "Accept-Language": "en-US,en;q=0.9"})
    try:
        try:
            jobs, total, note = fetch_board(s, host, prefix, budget_at)
        except Dead as e:
            with _lock:
                _stats["dead"] += 1
                _state[host] = {"status": "dead", "why": str(e)[:120],
                                "at": TODAY}
            print("  DEAD %-38s %s" % (host, e), flush=True)
            return []
        except Transient as e:
            with _lock:
                _stats["transient"] += 1
            print("  TRANSIENT %-38s %s" % (host, str(e)[:160]), flush=True)
            return []
        except Exception as e:                                   # noqa: BLE001
            with _lock:
                _stats["transient"] += 1
            print("  TRANSIENT %-38s %s" % (host, type(e).__name__), flush=True)
            return []
        rssmap = {}
        if USE_RSS and jobs and any(not parse_list_date(j["date"])
                                    for j in jobs):
            rssmap = rss_dates(s, host, prefix)
        rows = rows_of(host, prefix, company, jobs, rssmap)
    finally:
        s.close()
    short = max(0, total - len(jobs))
    with _lock:
        _stats["live"] += 1
        _stats["rows"] += len(rows)
        if short:
            _stats["short"] += 1
        _state[host] = {"status": "live", "total": total, "jobs": len(jobs),
                        "rows": len(rows), "rss_dates": len(rssmap),
                        "note": note, "at": TODAY}
    print("  live %-38s total=%-6d got=%-6d rows=%-6d rss=%-5d %s"
          % (host, total, len(jobs), len(rows), len(rssmap), note), flush=True)
    return rows


# ---- output ---------------------------------------------------------------
def _merge():
    """Fold this run's rows into contract_norm/radancy.jsonl, board-scoped.

    Rows land in radancy.jsonl.part as they are produced, so a crash or a
    budget stop loses nothing and the next run picks the part file up.  On a
    clean finish the old file is rewritten WITHOUT the boards the part file
    covers, then the part is appended: a refreshed board replaces its own rows
    and no other board is touched.  Straight appending would keep every
    delisted posting alive for ever."""
    if not os.path.exists(PART):
        return 0, 0
    boards = set()
    n_new = 0
    with open(PART, encoding="utf-8") as f:
        for line in f:
            try:
                d = json.loads(line)
            except ValueError:
                continue
            boards.add(d.get("board") or "")
            n_new += 1
    tmp = OUT + ".tmp"
    kept = 0
    with open(tmp, "w", encoding="utf-8") as out:
        if os.path.exists(OUT):
            with open(OUT, encoding="utf-8") as f:
                for line in f:
                    try:
                        d = json.loads(line)
                    except ValueError:
                        continue
                    if (d.get("board") or "") in boards:
                        continue
                    out.write(line if line.endswith("\n") else line + "\n")
                    kept += 1
        with open(PART, encoding="utf-8") as f:
            for line in f:
                out.write(line if line.endswith("\n") else line + "\n")
    os.replace(tmp, OUT)
    os.remove(PART)
    return kept, n_new


def load_tenants():
    out = []
    with open(TENANTS, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            t = (r.get("tenant") or "").strip()
            if not t:
                continue
            out.append((t, (r.get("prefix") or "").strip(),
                        (r.get("company") or "").strip() or t))
    return out


def main():
    if not os.path.exists(TENANTS):
        print("no %s — run radancy_discover_boards.py first" % TENANTS)
        return 1
    tenants = load_tenants()
    todo = [t for t in tenants
            if (not ONLY or t[0] in ONLY)
            and (REFRESH or (_state.get(t[0]) or {}).get("status")
                 not in ("live", "dead"))]
    random.shuffle(todo)
    if LIMIT:
        todo = todo[:LIMIT]
    print("radancy: %d boards, %d to fetch (keep_all=%s rpp=%d rss=%s)"
          % (len(tenants), len(todo), KEEP_ALL, RPP, USE_RSS), flush=True)
    if not todo:
        print("nothing to do", flush=True)
        return 0

    w = cc.Writer(PART)
    budget_at = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    done = 0
    stopped = False
    try:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(work, h, p, c, budget_at) for h, p, c in todo]
            for fut in cf.as_completed(futs):
                done += 1
                for r in fut.result():
                    with _lock:
                        w.write(r)
                if done % 10 == 0:
                    with _lock:
                        cc.save_state(STATE, _state)
                        cc.save_state(LEDGER, _ledger)
                    print("  %d/%d %s" % (done, len(todo), _stats), flush=True)
                if budget_at and time.time() > budget_at:
                    stopped = True
                    for f in futs:
                        f.cancel()
                    break
    finally:
        cc.save_state(STATE, _state)
        cc.save_state(LEDGER, _ledger)
        w.close()
    kept, added = _merge()
    print("DONE radancy: %s  (429 backoffs: %d)  %s -> kept %d + new %d%s"
          % (_stats, _n429[0], OUT, kept, added,
             "  [BUDGET REACHED — rest still pending]" if stopped else ""),
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
