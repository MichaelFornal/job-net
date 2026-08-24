#!/usr/bin/env python3
"""
manatal_fetch.py — Manatal hosted career pages (careers-page.com) multi-tenant
fetcher.

ROUTE (reverse-engineered 2026-08-20 against tenants/manatal.csv, 595 tenants)
-----------------------------------------------------------------------------
Manatal is the vendor behind careers-page.com; every page on the domain carries
<meta property="og:site_name" content="Manatal" />.  It ships TWO career-portal
generations that are NOT interchangeable, and both are in the corpus:

  v1  "career page"   https://www.careers-page.com/{slug}
                      https://www.careers-page.com/{slug}/job/{HASH8}
  v2  "portal"        https://{slug}.careers-page.com/
                      https://{slug}.careers-page.com/jobs/{uuid}

They are largely DISJOINT, and the failure is silent in both directions:
castelion-corporation.careers-page.com is HTTP 404, and
www.careers-page.com/maxdrive is HTTP 404.  Measured on the 38 subdomain slugs
mined from the master, only 6 answer on both hosts.  So the shape is a per-
tenant fact, carried in tenants/manatal.csv as a `shape` column and re-probed
(and rewritten into the state file) whenever the hint is wrong or missing.

v1 — THERE IS A JSON API, and the board HTML is a trap
------------------------------------------------------
www.careers-page.com/{slug} is server-rendered and does contain job links, so a
regex over it "works".  It is also TRUNCATED: castelion-corporation renders 20
<a href="/castelion-corporation/job/..."> links under a heading that reads
"186 Open Positions".  The remaining 166 arrive over XHR — the page is a Vue
app whose data block spells the endpoint out in the clear:

    const baseUrl    = "https://www.careers-page.com";
    const clientSlug = "castelion-corporation";
    apiBaseURL: `${baseUrl}/api/v1.0/c/${clientSlug}/`
    ... axios.get(apiBaseURL + 'jobs/?page_size=..&page=..&ordering=..')

    GET https://www.careers-page.com/api/v1.0/c/{slug}/jobs/?page={n}

is unauthenticated, needs no token, no cookie and no Referer, and answers a
plain DRF page: {"count": N, "next": url|null, "previous": ..., "results":[..]}.
Each result carries id, hash, position_name, description, country/state/city,
address, zipcode, location_display, is_salary_visible, is_pinned_in_career_page.

`page_size` is accepted and then IGNORED — the server pins the page at the
tenant's configured size (20 for castelion-corporation, 10 for maxdrive), so
`next` is followed rather than trusted arithmetic.  Asking for page_size=100
and reading len(results) as the whole board would lose 89% of the biggest
tenants.

The board page is still fetched once per tenant, for two things the API does
not carry: the employer's display name (og:title, "Castelion Corporation |
Career Page") and proof of which generation this tenant is.

v2 — the portal is server-rendered and complete, per page
--------------------------------------------------------
{slug}.careers-page.com/?page={n} renders every posting as an <article
class="job-card"> whose title anchor carries the id, title and location as data
attributes (data-job-id / data-job-title / data-job-city / data-job-country),
and the heading carries an authoritative "N Open Positions".  Pagination is
?page=N with the same per-tenant page size.  9 of these 35 tenants do NOT
exist in the v1 API at all (mudrex, mundi, niche, proactiviti, sync, ... answer
{"detail":"No ClientPortalSettings matches the given query."}), so the API is
not a substitute for scraping them.

posted / employment_type live ONLY on the job page (both generations)
--------------------------------------------------------------------
Neither the list API nor the portal card carries a date or a commitment type.
Both job-detail pages embed a schema.org JobPosting:

    <script type="application/ld+json">{"@type":"JobPosting",
      "datePosted":"2026-08-19T19:54:40.512328+00:00",
      "validThrough":"2026-09-19T06:43:16.556568+00:00",
      "employmentType":"FULL_TIME",
      "hiringOrganization":{"name":"Castelion Corporation"}, ...}

datePosted is REAL: sampled across one board it spreads over 2026-08-07,
08-11, 08-12, 08-13, 08-18, 08-19 — it is not a rolling "today".  (validThrough
IS rolling — every job on the board shares 2026-09-19 — so it is ignored.)
Some job pages emit no ld+json at all (2 of 7 sampled on castelion), and those
have no date anywhere on the page.

That costs one request per JOB rather than per board, so it is cached: every
(slug, job_id) that has been dated once is never re-fetched.  MANATAL_DETAIL=0
skips the phase entirely and falls back to title-only classification.

DEAD / EMPTY / BLOCKED  (rules 1 and 3 — a 200 is not success)
--------------------------------------------------------------
  v1 board 404 (the 1,734-byte Manatal shell every non-tenant path serves)
                              -> no v1 portal (shape miss, NOT a verdict)
  API 404 + {"detail":"No ClientPortalSettings matches the given query."}
                              -> no v1 portal either (shape miss)
  API 200 whose body is not a dict with BOTH "count" and "results"
                              -> not a jobs payload; shape miss, never an
                                 empty board
  API 200 with count>0 and zero results served -> TRANSIENT, stays pending
  v2 portal 404               -> no v2 portal (shape miss)
  v2 portal 200 without job-card markup -> not a v2 portal (shape miss)
  v2 portal says "N Open Positions" and no anchor parses
                              -> TRANSIENT: a theme this regex misses is a
                                 parser bug, and banking it as an empty board
                                 is how a parser bug becomes a retirement
  BOTH hosts refused          -> DEAD:no-portal-at-either-host(reasons)
  live shape, count == 0      -> LIVE and empty (a real board, no openings)
  429 "Just a moment..." from Cloudflare, 5xx, timeouts
                              -> BLOCKED/TRANSIENT: the tenant stays PENDING

Only fetch_tenant() — after BOTH generations have refused — is allowed to say
dead.  The first smoke run got this wrong in the other direction and it is
worth recording: v1 detection tested for `const clientSlug`, the CURRENT
theme's Vue config, and strata-kk renders the older server-side theme.  Board
page HTTP 200, API count=3, three live postings — recorded dead.  The 404
boundary is the vendor's contract; the theme is not.

PAGINATION WITHOUT `ordering` LOSES A QUARTER OF A BIG BOARD, SILENTLY
----------------------------------------------------------------------
The v1 API's queryset has no default order, so LIMIT/OFFSET reshuffles between
pages.  sweetcarolinatravels, count=410, walked with no ordering param: 410
rows served, 314 distinct — 96 postings sent twice and 96 others never sent.
The row count looks right, which is what makes it dangerous.  Sending the
career page's own ordering, `-is_pinned_in_career_page,-last_published_at`,
returns 410/410 distinct.  `ordering=id` is byte-identical to sending nothing:
the filter backend whitelists fields and silently ignores the rest.

The Cloudflare case is real and specific to the v2 host: *.careers-page.com
sits behind Cloudflare and answers HTTP 429 with a 5.7KB
"<title>Just a moment...</title>" interstitial (server: cloudflare) when hit
from several threads at once, while www.careers-page.com (gunicorn behind
CloudFront) tolerated 60 requests over 12 threads at 50 rps with zero 429s.
Reading that interstitial as "board has no jobs" would retire 9 live tenants.
Sending a browser-shaped header set (Accept/Sec-Fetch-*/Upgrade-Insecure-
Requests) and a slower portal rate got 5 of 6 challenged tenants back at once;
whatever is still challenged is recorded "blocked", which main() retries.

THROTTLE decays (rule 2).  Two independent buckets — "www" (API + v1 pages) and
"portal" (*.careers-page.com) — because their tolerances differ by an order of
magnitude, and one shared bucket would either crawl www or get the portal
walled.  slow_down() multiplies the interval by 1.5 on a 429 and speed_up()
walks it back 0.9x per 40 clean responses, so a burst of 429s cannot pin the
run at its floor for the rest of the day.

apply_url (rule 4) is the employer's own posting URL in the generation that
tenant actually publishes — www.careers-page.com/{slug}/job/{HASH} or
{slug}.careers-page.com/jobs/{uuid} — which is byte-for-byte the form found in
all_jobs_master.csv, so the rows collapse against the aggregator copies instead
of minting permanent false uniques.  No redirect, no tracking parameter; the
/apply and /refer suffixes the aggregators sometimes carry are stripped.

Output: contract_norm/manatal.jsonl
State:  manatal_fetch_state.json          Ledger: manatal_posted_ledger.json

ENV
    MANATAL_RPS          www requests/s            (default 16)
    MANATAL_PORTAL_RPS   *.careers-page.com rps    (default 1; Cloudflare)
    MANATAL_WORKERS      thread pool               (default 16)
    MANATAL_BUDGET_MIN   wall-clock budget, 0=off  (default 0)
    MANATAL_LIMIT        stop after N tenants      (default 0 = all)
    MANATAL_ONLY         comma-separated slugs     (default: the whole CSV)
    MANATAL_DETAIL       1 = fetch job pages for datePosted/employmentType
                         (default 1; ledger-cached, so only new jobs cost)
    MANATAL_DETAIL_CAP   max NEW detail fetches per board, 0 = no cap
    MANATAL_KEEP_ALL     1 = every employment type (default 1).  0 keeps only
                         contingent rows, into the SAME manatal.jsonl.
    MANATAL_REFRESH      1 = re-fetch tenants already recorded live/dead
"""
import concurrent.futures as cf
import csv
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
TENANTS = os.path.join(BASE, "tenants", "manatal.csv")
CANDIDATES = os.path.join(BASE, "manatal_candidates.csv")
OUT = os.path.join(cc.NORM_DIR, "manatal.jsonl")
STATE = os.path.join(BASE, "manatal_fetch_state.json")
LEDGER = os.path.join(BASE, "manatal_posted_ledger.json")

SOURCE = "manatal"
WWW = "https://www.careers-page.com"
API = WWW + "/api/v1.0/c/%s/jobs/"
# The exact ordering the career page's own Vue app sends.  See api_pages():
# without it the paginator silently drops a quarter of a large board.
ORDERING = "-is_pinned_in_career_page,-last_published_at"

RPS = float(os.environ.get("MANATAL_RPS") or 16.0)
PORTAL_RPS = float(os.environ.get("MANATAL_PORTAL_RPS") or 1.0)
WORKERS = int(os.environ.get("MANATAL_WORKERS") or 16)
BUDGET_MIN = int(os.environ.get("MANATAL_BUDGET_MIN") or 0)
LIMIT = int(os.environ.get("MANATAL_LIMIT") or 0)
ONLY = [s.strip().lower() for s in
        (os.environ.get("MANATAL_ONLY") or "").split(",") if s.strip()]
DETAIL = (os.environ.get("MANATAL_DETAIL") or "1") == "1"
DETAIL_CAP = int(os.environ.get("MANATAL_DETAIL_CAP") or 0)
KEEP_ALL = (os.environ.get("MANATAL_KEEP_ALL") or "1") == "1"
CLASSIFY = cc.classify_any if KEEP_ALL else cc.classify
REFRESH = os.environ.get("MANATAL_REFRESH") == "1"

TIMEOUT = 40
TRIES = 4
PAGE_CAP = 800          # 800 pages * 20 = 16k jobs; mercor, the largest board
                        # measured, is 6,408.

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Browser-shaped headers.  Not cosmetic: with a bare UA the Cloudflare in front
# of *.careers-page.com answers 429 "Just a moment..." for a third of the
# portal tenants; with these it answers 200 for all but one.
HDRS_HTML = {
    "User-Agent": cc.UA,
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,*/*;q=0.8"),
    "Accept-Language": "en-US,en;q=0.9",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-User": "?1",
}
HDRS_JSON = {
    "User-Agent": cc.UA,
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": WWW + "/",
}

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = {"www": 0.0, "portal": 0.0}
_interval = {"www": 1.0 / RPS, "portal": 1.0 / PORTAL_RPS}
_floor = {"www": 1.0 / RPS, "portal": 1.0 / PORTAL_RPS}
_pause_until = {"www": 0.0, "portal": 0.0}
_ok_streak = {"www": 0, "portal": 0}
_n429 = {"www": 0, "portal": 0}

_state = cc.load_state(STATE)
_ledger = cc.load_state(LEDGER)
_stats = {"live": 0, "dead": 0, "blocked": 0, "err": 0, "empty": 0,
          "rows": 0, "detail": 0, "detail_cached": 0, "nodate": 0,
          "short": 0}
_dead_why = {}


class Transient(Exception):
    """Retryable.  Raised (never returned) so the tenant is left PENDING rather
    than recorded as a completed empty board — the distinction rule 3 is
    about."""


class Blocked(Transient):
    """A Cloudflare interstitial or a hard 429.  A subclass of Transient so
    every caller already leaves the tenant pending, but recorded under its own
    status so the state file can tell a walled tenant from a flaky one."""


class Dead(Exception):
    """Provably gone.  Carries the discriminator that proved it."""
    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


class ShapeMiss(Exception):
    """This host answered, but what it served is not the portal generation we
    asked it for.  NOT dead — the other generation still has to be tried."""


# ---- throttle: slow_down / speed_up, per bucket ---------------------------
def throttle(bucket):
    while True:
        with _rate_lock:
            now = time.time()
            if now < _pause_until[bucket]:
                wait = _pause_until[bucket] - now
            else:
                wait = _next_t[bucket] - now
                if wait <= 0:
                    _next_t[bucket] = max(_next_t[bucket] + _interval[bucket],
                                          now)
                    return
        time.sleep(min(wait, 5) if wait > 0 else 0)


def slow_down(bucket):
    with _rate_lock:
        _pause_until[bucket] = time.time() + 20
        _interval[bucket] = min(_interval[bucket] * 1.5, 2.0)
        _n429[bucket] += 1
        _ok_streak[bucket] = 0


def speed_up(bucket):
    """Walk the interval back toward the configured rate after a clean run.

    Without this, slow_down is a one-way ratchet: a handful of 429s in the
    first minute pins the bucket at its 2s floor for the rest of the run,
    whatever MANATAL_RPS says.  Recovery is deliberately slower than the
    backoff (0.9x per 40 clean responses vs 1.5x per 429) so a host that is
    genuinely refusing still wins the argument.
    """
    with _rate_lock:
        n = _ok_streak[bucket] + 1
        if n < 40:
            _ok_streak[bucket] = n
            return
        _ok_streak[bucket] = 0
        if _interval[bucket] > _floor[bucket]:
            _interval[bucket] = max(_interval[bucket] * 0.9, _floor[bucket])


_CF = re.compile(
    r"Just a moment\.\.\.|cf-browser-verification|__cf_chl|cf_chl_opt|"
    r"Attention Required!\s*\|\s*Cloudflare|Checking your browser before",
    re.I)


def bucket_of(url):
    return "www" if url.startswith(WWW) else "portal"


def get(s, url, want="html"):
    """Throttled GET.  Returns (status, text) where status is 'ok' or 'gone'
    ('gone' == an authoritative 404/410 for THIS url).  Everything else raises
    Transient/Blocked, so a failure never masquerades as an empty result.
    """
    b = bucket_of(url)
    hdrs = HDRS_JSON if want == "json" else HDRS_HTML
    last = None
    for attempt in range(TRIES):
        throttle(b)
        try:
            r = s.get(url, timeout=TIMEOUT, headers=hdrs, allow_redirects=True)
            code = r.status_code
            body = r.text or ""
            # Cloudflare answers its interstitial under 403/429/503 depending
            # on the ruleset, so the BODY is the discriminator, not the code.
            if _CF.search(body[:4000]):
                slow_down(b)
                last = "cloudflare-interstitial(%d)" % code
                time.sleep(3 * (attempt + 1))
                continue
            if code == 200:
                speed_up(b)
                return "ok", body
            if code in (404, 410):
                speed_up(b)
                return "gone", body
            if code in (429, 418, 503):
                slow_down(b)
                last = "HTTP %d" % code
            else:
                last = "HTTP %d" % code
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    if last and "cloudflare" in last:
        raise Blocked("%s: %s" % (url, last))
    raise Transient("%s failed after %d tries: %s" % (url, TRIES, last))


# ---- html helpers ---------------------------------------------------------
_WS = re.compile(r"\s+")


def sq(v):
    return _WS.sub(" ", html.unescape(str(v or ""))).strip()


_OG_TITLE = re.compile(
    r'<meta[^>]+property=["\']og:title["\'][^>]+content=["\']([^"\']*)',
    re.I)
_CLIENT_SLUG = re.compile(r'const\s+clientSlug\s*=\s*"([^"]+)"')
# On every careers-page.com page, both themes and both generations.
_MANATAL_SITE = re.compile(
    r'og:site_name["\'][^>]+content=["\']Manatal', re.I)
_TITLE_TAG = re.compile(r"<title[^>]*>([^<]{1,200})", re.I)
_JOBS_AT = re.compile(r"<h4[^>]*>\s*Jobs at ([^<]{1,120})</h4>", re.I)
_OPEN_POS = re.compile(r"([0-9,]+)\s+Open Positions", re.I)
_LD = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.S | re.I)
# The v2 title anchor, captured whole so its data-* attributes can be read.
_V2_ANCHOR = re.compile(
    r'<a\s+href="/jobs/([0-9a-fA-F-]{36})"([^>]*?class="[^"]*job-title-link[^"]*"[^>]*?)>',
    re.S)
_ATTR = re.compile(r'data-job-([a-z-]+)="([^"]*)"')


def company_from_board(shape, body, slug):
    if shape == "v1":
        for rx in (_OG_TITLE, _TITLE_TAG):
            m = rx.search(body)
            if not m:
                continue
            n = sq(m.group(1))
            n = re.sub(r"\s*\|\s*Career Page\s*$", "", n).strip()
            if n:
                return n
    else:
        m = _JOBS_AT.search(body)
        if m:
            n = sq(m.group(1))
            if n:
                return n
    return slug.replace("-", " ").title()


def ld_jobposting(body):
    """The schema.org JobPosting dict on a job page, or None."""
    for m in _LD.finditer(body):
        try:
            d = json.loads(m.group(1).strip())
        except ValueError:
            continue
        if isinstance(d, list):
            d = next((x for x in d if isinstance(x, dict)
                      and "JobPosting" in str(x.get("@type", ""))), None)
        if isinstance(d, dict) and "JobPosting" in str(d.get("@type", "")):
            return d
    return None


# ---- posted-date ledger ---------------------------------------------------
def ledger_get(key):
    with _lock:
        rec = _ledger.get(key)
        return dict(rec) if isinstance(rec, dict) else None


def ledger_put(key, iso_date, raw_type):
    """First date ever seen for this posting wins, forever.

    Manatal's datePosted is a genuine per-posting date, so it is preferred; a
    posting whose page carries no ld+json gets today's date labelled
    precision="first_seen" so downstream date_basis.py can demote it, exactly
    as comeet/rippling do.  The ledger is append-only and must never be
    rotated: deleting it re-dates every undated posting to the day of the
    delete.
    """
    d = cc.ymd(iso=iso_date) if iso_date else ""
    with _lock:
        rec = _ledger.get(key)
        if isinstance(rec, dict) and rec.get("posted"):
            # A date already agreed on; only fill in a type we did not have.
            if raw_type and not rec.get("etype"):
                rec["etype"] = raw_type
            return dict(rec)
        rec = {"posted": d or TODAY,
               "precision": "date_posted" if d else "first_seen",
               "etype": raw_type or "",
               "first_seen": TODAY}
        _ledger[key] = rec
        return dict(rec)


# ---- v1: the JSON API -----------------------------------------------------
def api_pages(s, slug):
    """Every result dict on a v1 board.  Follows `next` — page_size is
    accepted and ignored by the server, so arithmetic on it is a lie.

    ORDERING IS LOAD-BEARING, and its absence is silent.  The queryset behind
    this endpoint has no default order, so an unordered LIMIT/OFFSET walk
    reshuffles between pages: measured on sweetcarolinatravels (count=410,
    41 pages), an unordered walk returned 410 rows of which only 314 were
    distinct — 96 postings duplicated and 96 different ones never served, a
    23% silent loss with a row count that looks perfectly healthy.  Passing the
    exact ordering the career page's own Vue app passes,

        ordering=-is_pinned_in_career_page,-last_published_at

    returns 410/410 distinct.  `ordering=id` does NOT fix it — the filter
    backend whitelists fields and quietly ignores anything else, so `id` is
    byte-identical to sending nothing at all.  The dedupe and the `short`
    counter below stay anyway: they are what would make a future regression
    visible instead of silent.
    """
    out = []
    got = set()
    url = API % slug + "?ordering=" + ORDERING
    seen = set()
    for _ in range(PAGE_CAP):
        if url in seen:
            break
        seen.add(url)
        st, body = get(s, url, want="json")
        if st == "gone":
            # Only meaningful on the FIRST page: that is the one that proves
            # the portal does not exist.  A 404 on page 40 is not a retirement,
            # and throwing away 800 collected rows to record one would be a far
            # worse error than stopping with what we have.
            #
            # ShapeMiss, not Dead: this slug has no v1 portal, which is NOT the
            # same as having no board.  maxdrive answers this API and has no v1
            # board; nine of the corpus's v2 portals are the mirror image, live
            # on their own subdomain and 404 here.  Only fetch_tenant, after
            # BOTH hosts have refused, is entitled to say dead.
            if not out:
                if '"detail"' in body and "ClientPortalSettings" in body:
                    raise ShapeMiss("api-404-no-client-portal")
                raise ShapeMiss("api-404")
            break
        try:
            d = json.loads(body)
        except ValueError:
            raise ShapeMiss("api-200-not-json")
        if not (isinstance(d, dict) and "count" in d and "results" in d
                and isinstance(d["results"], list)):
            # A 200 that is not a jobs payload (an HTML shell, an error
            # envelope).  NEVER read as an empty board.
            raise ShapeMiss("api-200-not-jobs-payload")
        for x in d["results"]:
            if not isinstance(x, dict):
                continue
            h = str(x.get("hash") or "")
            if h and h in got:
                continue
            got.add(h)
            out.append(x)
        if not out and (d.get("count") or 0) > 0:
            # The board says it has openings and handed back none of them.
            # That is a broken response, not an empty board — bank it as empty
            # and the tenant is retired by arithmetic.  Stay pending, loudly.
            raise Transient("api-count=%s-but-0-results" % d.get("count"))
        nxt = d.get("next")
        total = d.get("count") or 0
        if not nxt or not d["results"]:
            if total and len(out) < total:
                with _lock:
                    _stats["short"] += 1
                print("  SHORT %s: %d of %d postings served"
                      % (slug, len(out), total), flush=True)
            break
        url = nxt if nxt.startswith("http") else WWW + nxt
    return out


def v1_board(s, slug):
    """-> (company, [job dicts]).  Raises ShapeMiss if this is not a v1 board.

    The 200 itself is the discriminator, and it is a safe one: every non-tenant
    path on this host answers 404 with the same 1,734-byte Manatal shell
    (/about, /pricing, /login, /signup, /contact, /terms, /privacy, /dashboard,
    /help all measured).  What is NOT safe is testing for the Vue config —
    `const clientSlug` is the CURRENT theme only.  strata-kk renders the older
    server-side theme, has no clientSlug anywhere, and its API answers
    count=3: requiring that marker retired a live board on the first smoke run.
    Themes are the vendor's to change; the 404 boundary is not.
    """
    st, body = get(s, "%s/%s" % (WWW, slug))
    if st == "gone":
        raise ShapeMiss("v1-board-404")
    if not (_MANATAL_SITE.search(body) or _CLIENT_SLUG.search(body)):
        # 200 that is not a Manatal-served page at all (a parked domain, an
        # interception page).  Not a board, and not this generation.
        raise ShapeMiss("v1-board-200-not-a-manatal-page")
    try:
        jobs = api_pages(s, slug)
    except ShapeMiss as e:
        # The board page IS a Manatal career page and the API behind it will
        # not answer.  That combination has never been observed and there is
        # no verified parser for it, so it is NOT a verdict: it stays pending
        # and shows up in the err census where somebody can look at it.
        # Guessing "dead" here is how an unexplained state becomes a
        # retirement.
        raise Transient("v1-board-200-but-%s" % e)
    return company_from_board("v1", body, slug), jobs


def api_exists(s, slug):
    """Does this slug have a live v1 client portal at all?  One request.

    Used only to decide whether the Cloudflare-fronted v2 host is worth a
    probe.  maxdrive is the shape it detects: HTTP 404 on
    www.careers-page.com/maxdrive, HTTP 200 count=13 on the API, and its
    postings live at maxdrive.careers-page.com.
    """
    st, body = get(s, API % slug + "?ordering=" + ORDERING, want="json")
    if st == "gone":
        return False
    try:
        d = json.loads(body)
    except ValueError:
        return False
    return isinstance(d, dict) and "count" in d and "results" in d


# ---- v2: the server-rendered portal ---------------------------------------
def v2_board(s, slug):
    """-> (company, [job dicts]).  Raises ShapeMiss if this is not a v2 portal."""
    host = "https://%s.careers-page.com" % slug
    company = None
    total = None
    jobs = {}
    for page in range(1, PAGE_CAP + 1):
        url = host + "/" if page == 1 else "%s/?page=%d" % (host, page)
        st, body = get(s, url)
        if st == "gone":
            if page == 1:
                raise ShapeMiss("v2-portal-404")
            break
        if page == 1:
            if not (_OPEN_POS.search(body) or "job-card" in body):
                raise ShapeMiss("v2-portal-200-no-job-markup")
            company = company_from_board("v2", body, slug)
            m = _OPEN_POS.search(body)
            if m:
                total = int(m.group(1).replace(",", ""))
        before = len(jobs)
        found = 0
        for m in _V2_ANCHOR.finditer(body):
            uuid = m.group(1).lower()
            attrs = dict(_ATTR.findall(m.group(2)))
            found += 1
            if uuid in jobs:
                continue
            jobs[uuid] = {
                "hash": uuid,
                "position_name": sq(attrs.get("title")),
                "city": sq(attrs.get("city")),
                "state": sq(attrs.get("state")),
                "country": sq(attrs.get("country")),
            }
        if page == 1 and not found and (total or 0) > 0:
            # The heading claims openings and not one title anchor parsed:
            # a theme this regex does not cover.  Recording that as an empty
            # board is exactly the silent-zero rule 1 is about, so it stays
            # PENDING and shows up in the err census instead.
            raise Transient(
                "v2-portal-%s-says-%d-open-but-no-parsable-job-anchor"
                % (slug, total))
        if not found or len(jobs) == before:
            # No cards, or a page that repeated the previous one — either way
            # the walk is over.  Without the second half of this test a portal
            # that ignores ?page would spin to PAGE_CAP.
            break
        if total is not None and len(jobs) >= total:
            break
    return company or slug, list(jobs.values())


# ---- row assembly ---------------------------------------------------------
def loc_string(j):
    if j.get("location_display"):
        return sq(j["location_display"])
    return ", ".join(x for x in (sq(j.get("city")), sq(j.get("state")),
                                 sq(j.get("country"))) if x)


def job_url(shape, slug, jid):
    if shape == "v1":
        return "%s/%s/job/%s" % (WWW, slug, jid)
    return "https://%s.careers-page.com/jobs/%s" % (slug, jid)


def board_url(shape, slug):
    if shape == "v1":
        return "%s/%s" % (WWW, slug)
    return "https://%s.careers-page.com/" % slug


def detail(s, shape, slug, jid):
    """(iso_date, raw_employment_type, company, ld_location) from the job page.

    A 404 here means that ONE posting went away between the list call and now;
    it is not evidence about the board, so it returns blanks rather than
    raising.  Transient/Blocked still propagate — a throttled detail page must
    not be banked as "this job has no date".
    """
    st, body = get(s, job_url(shape, slug, jid))
    if st == "gone":
        return "", "", "", {}
    d = ld_jobposting(body)
    if not d:
        # ld+json is genuinely absent on some postings.  og:title still names
        # the employer: "Facilities Coordinator - Castelion Corporation |
        # Career Page" (v1) / "Inventory Officer | MAX" (v2).
        m = _OG_TITLE.search(body)
        comp = ""
        if m:
            t = re.sub(r"\s*\|\s*Career Page\s*$", "", sq(m.group(1)))
            comp = t.rsplit(" - ", 1)[-1] if shape == "v1" else \
                t.rsplit(" | ", 1)[-1]
        return "", "", sq(comp), {}
    et = d.get("employmentType")
    if isinstance(et, list):
        et = " ".join(str(x) for x in et)
    org = d.get("hiringOrganization")
    comp = sq(org.get("name")) if isinstance(org, dict) else ""
    place = d.get("jobLocation")
    if isinstance(place, list):
        place = place[0] if place else None
    addr = {}
    if isinstance(place, dict) and isinstance(place.get("address"), dict):
        a = place["address"]
        addr = {"city": sq(a.get("addressLocality")),
                "state": sq(a.get("addressRegion")),
                "country": sq(a.get("addressCountry"))}
    return sq(d.get("datePosted")), sq(et), comp, addr


def build_rows(s, shape, slug, company, jobs):
    rows = []
    budget = DETAIL_CAP or None
    for j in jobs:
        jid = sq(j.get("hash"))
        title = sq(j.get("position_name"))
        if not jid or not title:
            continue
        key = "%s:%s" % (slug, jid)
        rec = ledger_get(key)
        raw_type = (rec or {}).get("etype") or ""
        posted = (rec or {}).get("posted") or ""
        comp = company
        city = sq(j.get("city"))
        state = sq(j.get("state"))
        country = sq(j.get("country"))
        if rec:
            with _lock:
                _stats["detail_cached"] += 1
        elif DETAIL and (budget is None or budget > 0):
            if budget is not None:
                budget -= 1
            iso, raw_type, dcomp, addr = detail(s, shape, slug, jid)
            with _lock:
                _stats["detail"] += 1
                if not iso:
                    _stats["nodate"] += 1
            rec = ledger_put(key, iso, raw_type)
            posted = rec["posted"]
            if dcomp:
                comp = dcomp
            city = city or addr.get("city", "")
            state = state or addr.get("state", "")
            country = country or addr.get("country", "")
        else:
            rec = ledger_put(key, "", "")
            posted = rec["posted"]
        loc = loc_string(j) or ", ".join(
            x for x in (city, state, country) if x)
        etype = CLASSIFY(raw_type or None, title)
        if etype is None:               # MANATAL_KEEP_ALL=0: contingent only
            continue
        mode = cc.work_mode(text=(loc + " " + title))
        rows.append(cc.row(
            source=SOURCE, source_type="ats",
            company=comp or company, title=title,
            employment_type=etype, work_mode=mode, location=loc,
            city=city, state=state, country=country,
            remote=(mode == "Remote"),
            posted=posted,
            apply_url=job_url(shape, slug, jid), job_id=jid,
            board=slug, board_url=board_url(shape, slug)))
    return rows


# ---- per-tenant driver ----------------------------------------------------
def fetch_tenant(s, slug, hint):
    """-> (shape, company, rows).  Raises Dead / Transient / Blocked.

    THE V2 HOST IS PROBED ONLY ON EVIDENCE, and that is a cost decision with a
    correctness consequence.  Cloudflare in front of *.careers-page.com
    challenges by CONCURRENCY, not by path: 60 requests for slugs that do not
    exist, over 10 threads, came back 48 "Just a moment..." and 12 HTTP 404.
    So a blind v2 probe of every candidate is not merely slow at 1 rps — most
    of the answers are interstitials, and an interstitial is (correctly)
    Blocked, which leaves the tenant PENDING and retried on every future run.
    Point manatal_candidates.csv (3,670 Wayback-discovered slugs, 2,688 of them
    with no live portal) at a blind probe and the state file fills with
    thousands of permanent retries.

    A slug earns a v2 probe by one of three pieces of evidence:
      * the corpus saw a {slug}.careers-page.com URL (tenants/manatal.csv
        shape=v2), or CDX did (manatal_candidates.csv),
      * the v1 board 404s but the v1 API still knows the client portal — the
        postings have to be somewhere, and the v2 host is where (maxdrive),
      * nothing else answered and the hint said v2.
    With no evidence from any of those, the verdict is dead and it cost two
    requests on the fast host and none on the slow one.
    """
    misses = []
    if hint != "v2":
        try:
            company, jobs = v1_board(s, slug)
            return "v1", company, build_rows(s, "v1", slug, company, jobs)
        except ShapeMiss as e:
            misses.append("v1:%s" % e)
            if str(e) == "v1-board-404" and not api_exists(s, slug):
                raise Dead("no-v1-portal-and-no-v2-evidence(%s)"
                           % "|".join(misses))
    try:
        company, jobs = v2_board(s, slug)
        return "v2", company, build_rows(s, "v2", slug, company, jobs)
    except ShapeMiss as e:
        misses.append("v2:%s" % e)
    if hint == "v2":
        # The hint was wrong or the tenant migrated to the www generation.
        try:
            company, jobs = v1_board(s, slug)
            return "v1", company, build_rows(s, "v1", slug, company, jobs)
        except ShapeMiss as e:
            misses.append("v1:%s" % e)
    raise Dead("no-portal-at-either-host(%s)" % "|".join(misses))


def work(w, slug, hint):
    s = requests.Session()
    try:
        try:
            shape, company, rows = fetch_tenant(s, slug, hint)
        except Dead as e:
            with _lock:
                _stats["dead"] += 1
                _dead_why[e.reason.split("(")[0]] = \
                    _dead_why.get(e.reason.split("(")[0], 0) + 1
                _state[slug] = {"status": "dead", "why": e.reason[:120],
                                "at": TODAY}
            return
        except Blocked as e:
            with _lock:
                _stats["blocked"] += 1
                _state[slug] = {"status": "blocked", "why": str(e)[:120],
                                "at": TODAY}
            return
        except Transient as e:
            with _lock:
                _stats["err"] += 1
                _state[slug] = {"status": "err", "why": str(e)[:120],
                                "at": TODAY}
            return
        except Exception as e:                            # noqa: BLE001
            with _lock:
                _stats["err"] += 1
                _state[slug] = {"status": "err", "why": type(e).__name__,
                                "at": TODAY}
            return
        # The whole board came back clean -> write, then mark live.  In this
        # order a crash between the two costs a duplicate board next run (which
        # bh_build_combined collapses) rather than a lost one.
        with _lock:
            for r in rows:
                w.write(r)
            _stats["rows"] += len(rows)
            _stats["live"] += 1
            if not rows:
                _stats["empty"] += 1
            _state[slug] = {"status": "live", "shape": shape,
                            "company": company, "jobs": len(rows),
                            "at": TODAY}
    finally:
        s.close()


def load_tenants():
    seen, out = set(), []
    for path in (TENANTS, CANDIDATES):
        if not os.path.exists(path):
            continue
        with open(path, newline="", encoding="utf-8") as f:
            for r in csv.DictReader(f):
                t = (r.get("tenant") or r.get("slug") or "").strip().lower()
                if not t or t in seen:
                    continue
                seen.add(t)
                out.append((t, (r.get("shape") or "").strip().lower()))
    return out


def main():
    tenants = load_tenants()
    if not tenants:
        raise SystemExit("missing %s" % TENANTS)
    if ONLY:
        want = set(ONLY)
        tenants = [t for t in tenants if t[0] in want]
    todo = [(t, h) for t, h in tenants
            if REFRESH or (_state.get(t) or {}).get("status")
            not in ("live", "dead")]
    already = len(tenants) - len(todo)
    random.shuffle(todo)
    if LIMIT:
        todo = todo[:LIMIT]
    print("manatal: %d tenants, %d to fetch (%d already recorded)  "
          "detail=%s keep_all=%s" % (len(tenants), len(todo), already,
                                     DETAIL, KEEP_ALL), flush=True)

    w = cc.Writer(OUT)
    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    done = 0
    stopped = False
    # NOT `with cf.ThreadPoolExecutor(...)`: its __exit__ is shutdown(wait=True),
    # and f.cancel() below is a no-op on a future that has already started.  One
    # board is hundreds of throttled requests, so "wait for the in-flight ones"
    # is unbounded -- it sat for 2h47m after the budget fired on 2026-08-21.
    ex = cf.ThreadPoolExecutor(max_workers=WORKERS)
    try:
        futs = [ex.submit(work, w, t, h) for t, h in todo]
        for _ in cf.as_completed(futs):
            done += 1
            # Checkpoint often: a board can take minutes (one request per
            # job for the dates), so a 20-board interval is tens of minutes
            # of ledger at risk, and a lost ledger re-dates every undated
            # posting to the day of the crash.
            if done % 5 == 0:
                with _lock:
                    cc.save_state(STATE, _state)
                    cc.save_state(LEDGER, _ledger)
                print("  %d/%d  %s" % (done, len(todo), _stats),
                      flush=True)
            if deadline and time.time() > deadline:
                stopped = True
                for f in futs:
                    f.cancel()
                break
    finally:
        # Drop the queued futures and stop waiting on the running ones.  Closing
        # the writer first is deliberate: an abandoned worker then fails its
        # write with ValueError inside its own thread instead of appending half
        # a line to the JSONL.
        ex.shutdown(wait=False)
        with _lock:
            cc.save_state(STATE, _state)
            cc.save_state(LEDGER, _ledger)
        w.close()
    print("DONE manatal: %s" % _stats, flush=True)
    print("  dead reasons: %s" % (_dead_why or "-"), flush=True)
    print("  429 backoffs: %s  final intervals: %s"
          % (_n429, {k: round(v, 3) for k, v in _interval.items()}),
          flush=True)
    if stopped:
        print("  [BUDGET REACHED — the rest are still pending]", flush=True)
        # The pool's worker threads are non-daemon, so a plain `return` would
        # join them at interpreter exit and reinstate the very hang the
        # shutdown(wait=False) above avoids.  Every row and both state files are
        # already on disk, and the JSONL is append-only, so a hard exit costs at
        # most the boards still in flight -- they stay pending for the next run.
        sys.stdout.flush()
        sys.stderr.flush()
        os._exit(1)
    # Exit 1 when anything is still pending, the way the other tail fetchers
    # do: run_all's ok_rc=(0, 1) tolerates it and it is what makes throttled
    # tenants get revisited on the next attempt.
    return 1 if (stopped or _stats["blocked"] or _stats["err"]) else 0


if __name__ == "__main__":
    sys.exit(main())
