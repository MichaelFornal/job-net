#!/usr/bin/env python3
"""
slug_common.py — shared Wayback-CDX board-slug discovery for every ATS whose
customer boards live at a predictable host or path.

Why this is shared, when greenhouse/lever/ashby each have their own copy:

  1. cdx_get() here VALIDATES THE RESPONSE BODY.  web.archive.org serves an
     nginx "504 Gateway Time-out" / "503 Service Unavailable" HTML page with
     HTTP status 200 (reproduced repeatedly).  The existing per-family copies
     check only r.status_code, so they write that HTML into their
     <fam>_wayback_urls.txt AND record the page as done — the real slugs on
     that page are then lost permanently, with no way to notice after the
     fact.  That bug needs one fix site, not nine.
  2. The per-apex differences are pure data (pattern, SURT prefix, extraction
     mode, junk), so there is no per-apex logic to duplicate.
  3. One global CDX politeness budget.  Nine separate processes at 6 workers
     each is 54 concurrent requests against one rate-limited host.

Precedent for sharing: contract_common.py, and ats_combined_build.py's
`import all_jobs_build as ab`.  What stays copy-pasted per family is
throttle(), because each ATS's job API gets its own RPS.

SURT urlkeys (what CDX sorts and collapses on) look like:
    de,personio,jobs,1000satellites-coworking)/
    com,rippling,ats)/4ag/jobs/0ed50ede-b350-4a64-bb38-5f62683b53e6
so two extraction modes are needed: "host" (slug is a host label) and "path"
(slug is a path segment).
"""
import json
import os
import random
import re
import threading
import time

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
CDX = "https://web.archive.org/cdx/search/cdx"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")
TRIES = 6
TIMEOUT = 150
MAX_BYTES = 64 * 1024 * 1024   # a page should be KBs; this is an OOM guard
SLUG_BUDGET = 12               # chars of slug the collapse must preserve;
                               # Metric A in slug_discover_boards.py MEASURES
                               # whether this is deep enough per apex


class Apex:
    """One CDX crawl target.

    pattern  CDX `url` param (trailing /* gives prefix matching; a leading
             *. gives the host sweep).  No matchType, no filter — that is the
             shape proven in this repo.
    prefix   SURT urlkey prefix that every row for this apex starts with.
    mode     "host" -> slug is the first host label after the apex
             "path" -> slug is a path segment
    seg      path mode: which segment (0-based)
    anchor   path mode: if set, take the segment immediately BEFORE this
             literal instead of a fixed index.  Needed for rippling, whose
             URLs are {org}/jobs/{id} but sometimes {locale}/{org}/jobs/{id}.
    """

    def __init__(self, tag, family, pattern, prefix, mode,
                 seg=0, anchor=None, budget=SLUG_BUDGET, extra_junk=(),
                 fl="urlkey", raw_only=False):
        self.tag = tag
        self.family = family
        self.pattern = pattern
        self.prefix = prefix
        self.mode = mode
        self.seg = seg
        self.anchor = anchor
        self.budget = budget
        self.junk = set(extra_junk)
        # fl="original" asks CDX for the real URL instead of the SURT urlkey.
        # It costs bytes, and it is the ONLY option when case matters: a
        # urlkey is lowercased, and Workday's CxS endpoint is
        # /wday/cxs/{tenant}/{site}/jobs with a case-SENSITIVE site, so a
        # urlkey sweep yields tenants that 404 on every request.
        self.fl = fl
        # raw_only apexes are crawled and cached like any other, but the
        # generic slug extractor skips them: their identity is not a single
        # slug (Workday needs tenant+pod+site) and a family-specific parser
        # reads the raw lines instead.
        self.raw_only = raw_only

    @property
    def collapse(self):
        return "urlkey:%d" % (len(self.prefix) + self.budget)


# Page counts in the comments are measured, not inherited from any doc.
#
# budget=36 on seven apexes (2026-08-13), MEASURED not guessed: Metric A's
# collapse_probe found the default 12 losing slugs above its own 0.5% gate on
# every one of them — jazzhr 15.38% (8 of 52), smartrecruiters-jobs 9.21%
# (14 of 152), personio-de 2.19%, breezy 1.69%, smartrecruiters-api 1.38%,
# bamboohr 1.19%, recruitee 0.88%.  crawl() drops the cached pages for an apex
# whose collapse changed, so this re-crawls ~1,549 of 2,558 pages by itself.
# The other six apexes measure 0.00-0.27% and are deliberately left at 12.
# Do NOT escalate a second time: metric_a re-probes at budget+24, so every
# deepening manufactures a fresh "failure" against a deeper reference.
APEXES = [
    Apex("personio-de", "personio", "*.jobs.personio.de/*",
         "de,personio,jobs,", "host", budget=36),                   # 47 pages
    Apex("personio-com", "personio", "*.jobs.personio.com/*",
         "com,personio,jobs,", "host"),                             # 15
    # budget=70 rather than the default 12, MEASURED not guessed: most
    # Rippling orgs appear only inside Next.js data routes shaped
    # _next/data/{21-char build hash}/en-us/{org}/jobs/... — at depth 12 every
    # one of those collapses to a single row and 45% of orgs vanish.
    # Page-0 yield by budget: 12 -> 28 slugs, 40 -> 134, 70 -> 146, 100 -> 146.
    Apex("rippling-ats", "rippling", "ats.rippling.com/*",
         "com,rippling,ats)/", "path", anchor="jobs", budget=70),   # 8
    # Rippling's public JSON host, the same way greenhouse_discover_boards.py
    # sweeps boards-api.greenhouse.io and lever_discover_boards.py sweeps
    # api.lever.co.  Tiny (1 page, ~20 rows) but it is the endpoint the
    # fetcher actually calls, so anything archived here is a real board.
    Apex("rippling-api", "rippling",
         "api.rippling.com/platform/api/ats/v1/board/*",
         "com,rippling,api)/platform/api/ats/v1/board/", "path",
         anchor="jobs", budget=36),                                 # 1
    # family is "rippling-tenant", NOT "rippling", MEASURED 2026-08-24: these
    # host labels are a DIFFERENT key space.  Probed against
    # api.rippling.com/board/{slug} they 404, so 569 of 587 were recorded
    # "dead" -- including 9 hosts the master sees real jobs on.  Of the 75 in
    # slug_wayback_urls.txt, 48 now 302 to app.rippling.com/apps_sso_direct
    # (retired) and 27 still serve boards, on applicant-tracking.com
    # infrastructure with no JSON API.  rippling_tenant_fetch.py owns them.
    Apex("rippling-tenant", "rippling-tenant", "*.rippling-ats.com/*",
         "com,rippling-ats,", "host"),                              # 5
    Apex("breezy", "breezy", "*.breezy.hr/*",
         "hr,breezy,", "host", budget=36),                          # 178
    Apex("recruitee", "recruitee", "*.recruitee.com/*",
         "com,recruitee,", "host", budget=36),                      # 92
    Apex("jazzhr", "jazzhr", "*.applytojob.com/*",
         "com,applytojob,", "host", budget=36),                     # 632
    Apex("bamboohr", "bamboohr", "*.bamboohr.com/*",
         "com,bamboohr,", "host", budget=36,
         extra_junk=("careers", "list")),                           # 299
    Apex("teamtailor", "teamtailor", "*.teamtailor.com/*",
         "com,teamtailor,", "host", extra_junk=("companies",)),     # 190
    Apex("smartrecruiters-jobs", "smartrecruiters",
         "jobs.smartrecruiters.com/*", "com,smartrecruiters,jobs)/", "path",
         budget=36,
         extra_junk=("companies", "publications", "postings")),     # 298
    Apex("smartrecruiters-api", "smartrecruiters",
         "api.smartrecruiters.com/v1/companies/*",
         "com,smartrecruiters,api)/v1/companies/", "path",
         budget=36),                                                # 3
    Apex("workable-apply", "workable", "apply.workable.com/*",
         "com,workable,apply)/", "path",
         extra_junk=("view", "j", "i", "backend")),                 # 244
    Apex("workable-host", "workable", "*.workable.com/*",
         "com,workable,", "host",
         extra_junk=("view", "j", "i", "backend", "apply")),        # 547
    # ---- workday -------------------------------------------------------
    # raw_only + fl=original, for two independent reasons:
    #  1. IDENTITY IS A TRIPLE.  The CxS endpoint is
    #     /wday/cxs/{tenant}/{site}/jobs and the board url is
    #     {tenant}.{pod}.myworkdayjobs.com/{site}, so a single slug cannot
    #     name a board.  workday_discover_boards.py parses the raw rows.
    #  2. CASE IS LOAD-BEARING.  A SURT urlkey is lowercased and {site} is
    #     case-sensitive ("SwisscomExternalCareers"), so a urlkey sweep would
    #     hand every tenant a site that 404s.
    #
    # budget=100 MEASURED 2026-08-20 on pages 461 and 922 of 923, triples kept:
    #   12 -> 134 | 40 -> 267 | 70 -> 276 | 100 -> 278   (page 922)
    #   12 ->   1 | 40 ->  12 | 70 ->  13 | 100 ->  13   (page 461)
    # 70 still loses 0.7% against 100, which is over this repo's own 0.5%
    # gate, and the default 12 collapses 13 triples to 1 on page 461 and
    # loses 52% on page 922.  Do not lower it.
    #
    # NOTE page 0 is NOT representative here: SURT sorts "impl-" before "wd",
    # so the early pages are all Workday IMPLEMENTATION sandboxes and a
    # page-0-only probe reads the platform as tiny.
    Apex("workday-jobs", "workday", "*.myworkdayjobs.com/*",
         "com,myworkdayjobs,", "path", budget=100,
         fl="original", raw_only=True),                             # 923 pages
    # The second Workday host, and a sub-gap inside a WORKING pipe:
    # workday_fetch.py and ats_mine_tenants.PATTERNS["workday"] both hard-code
    # {tenant}.wd{N}.myworkdayjobs.com, so this path-tenant form is invisible
    # to the fetcher AND to the miner.  Same CxS protocol -- verified
    # 2026-08-20: POST /wday/cxs/hawks/External/jobs on wd1.myworkdaysite.com
    # returned 200 {"total":40,"jobPostings":[...]}.
    Apex("workday-site", "workday", "*.myworkdaysite.com/*",
         "com,myworkdaysite,", "path", budget=100,
         fl="original", raw_only=True),                             # 23 pages
]
BY_TAG = {a.tag: a for a in APEXES}
FAMILIES = sorted({a.family for a in APEXES})


# ---------------------------------------------------------------- slug hygiene
# Union of the two existing repo blacklists plus what was actually observed in
# the wild (_next alone was 70% of one rippling page).
JUNK = {
    "", "*", "*\\", "...", "_next", "__next", "_app", "_document", "_error",
    "404", "500", "502", "503", ".well-known", "favicon.ico", "robots.txt",
    "sitemap.xml", "ads.txt", "app-ads.txt", "privacy-policy", "terms",
    "imprint", "impressum", "datenschutz", "search", "xml", "static",
    "assets", "img", "images", "css", "js", "fonts", "cdn", "media", "api",
    "v1", "v0", "www", "app", "admin", "login", "signup", "auth", "oauth",
    "logout", "dashboard", "settings", "index.html", "undefined", "null",
    "error", "none", "jobs", "job", "embed", "apply", "careers", "test",
    "demo", "example", "[jobboardslug]", "{slug}", "%7bslug%7d", "$company",
    "companyname", "your-company", "yourcompany", "help", "support",
    "status", "blog", "docs",
}
SLUG_OK = re.compile(r"^[a-z0-9][a-z0-9._-]{0,62}$")
UUID_RE = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-"
                     r"[0-9a-f]{4}-[0-9a-f]{12}$")
HEXY_RE = re.compile(r"^[0-9a-f]{16,}$")


def unquote2(s):
    """CDX rows are sometimes double-percent-encoded."""
    import urllib.parse
    return urllib.parse.unquote(urllib.parse.unquote(s))


def clean(seg, apex=None):
    """Normalize one candidate slug, or return None if it is junk."""
    if not seg:
        return None
    seg = unquote2(seg).strip().lower()
    if seg in JUNK or (apex is not None and seg in apex.junk):
        return None
    if UUID_RE.match(seg) or HEXY_RE.match(seg):
        return None
    if seg.isdigit() or seg.startswith("-") or seg.endswith("-"):
        return None
    if not SLUG_OK.match(seg):
        return None
    return seg


def slug_of(apex, urlkey):
    """SURT urlkey -> board slug, or None."""
    if not urlkey.startswith(apex.prefix):
        return None
    rest = urlkey[len(apex.prefix):]
    if apex.mode == "host":
        # "acme,careers)/path" -> "acme"  (label adjacent to the apex)
        seg = rest.split(")", 1)[0].split(",")[0]
    else:
        path = rest.split("?", 1)[0]
        parts = [x for x in path.split("/") if x]
        if apex.anchor:
            # Segment immediately before the anchor, so a locale prefix
            # (ats.rippling.com/en-US/{org}/jobs/...) does not shift us.
            try:
                i = parts.index(apex.anchor)
            except ValueError:
                return None
            seg = parts[i - 1] if i >= 1 else ""
        else:
            seg = parts[apex.seg] if len(parts) > apex.seg else ""
    return clean(seg, apex)


# ---------------------------------------------------------------- CDX client
def session():
    s = requests.Session()
    s.headers["User-Agent"] = UA
    return s


def _read_capped(r):
    buf = bytearray()
    for chunk in r.iter_content(65536):
        buf += chunk
        if len(buf) > MAX_BYTES:
            raise RuntimeError("CDX response exceeded %d bytes" % MAX_BYTES)
    return buf.decode("utf-8", "replace")


_BAD_MARKERS = ("<html", "<!doctype", "<title", "gateway time-out",
                "service unavailable", "internal server error",
                "too many requests")


def validate(body, expect, apex=None):
    """(ok, why).  The whole reason this module exists — see the docstring."""
    low = body[:2000].lower()
    for m in _BAD_MARKERS:
        if m in low:
            return False, "html-error-body (%s)" % m.strip("<")
    if expect == "int":
        t = body.strip()
        if not t or not t.isdigit():
            return False, "not-an-int: %r" % t[:40]
        return True, ""
    lines = [ln for ln in body.splitlines() if ln.strip()]
    if expect == "urls":
        # fl=original returns plain URLs, which contain no ')' — the urlkey
        # test below would reject every last one of them.  dover_discover_
        # boards.py asks for original URLs because Dover's board UUID sits at
        # the END of the path, where a SURT prefix collapse cannot reach it.
        if not lines:
            return True, ""
        good = sum(1 for ln in lines
                   if "://" in ln or ln.lstrip().startswith(("http", "www.")))
        if good < 0.9 * len(lines):
            return False, "only %d/%d rows look like URLs" % (good, len(lines))
        return True, ""
    if not lines:
        return True, ""            # a page CAN legitimately collapse to empty
    if lines[0].lstrip().startswith("<"):
        return False, "leading-angle-bracket"
    # Every SURT urlkey contains ')'.  Require the bulk of the rows to look
    # like urlkeys, so a truncated or substituted body is rejected.
    good = sum(1 for ln in lines if ")" in ln)
    if good < 0.9 * len(lines):
        return False, "only %d/%d rows look like urlkeys" % (good, len(lines))
    if apex is not None:
        token = apex.prefix.split(",", 1)[0] + ","
        hit = sum(1 for ln in lines if ln.startswith(token))
        if hit < 0.5 * len(lines):
            return False, ("rows are not for this apex (%d/%d start %r)"
                           % (hit, len(lines), token))
    return True, ""


# ---------------------------------------------------------------- breaker
# THE PROBLEM THIS SOLVES.  cdx_get's retry ladder is min(15*2**n, 240) over
# TRIES=6, so ONE permanently-failing request costs 705-735s -- and the sixth
# sleep is paid AFTER the last attempt, purely wasted.  Nothing shared any
# state between calls, so during the 2026-09-02 outage every request paid that
# ladder independently:
#
#   * uncached numpages -> ~12 min, then the exception escapes and the caller's
#     fallback runs.  Survivable.
#   * CACHED numpages -> straight to 3,505 page jobs at WORKERS=6, each failure
#     swallowed per page -> ~117 HOURS.  metric_a adds 84 serial requests
#     (~17 h) on top.
#
# The ceiling in run_all cannot fix this: on 2026-09-02 a 600s ceiling was set,
# which is BELOW the 705s floor of a single failing request, so the sweep was
# killed inside its FIRST numpages and produced nothing at all -- no greps, no
# candidate CSVs, no report.  A ceiling bounds the damage; it cannot buy the
# offline half.  Only giving up fast can.
#
# So: once the archive has refused CONSECUTIVE_LIMIT requests in a row, every
# later call raises IMMEDIATELY without sleeping.  Callers that already handle
# a RuntimeError (slug_discover_boards.main, and the per-page swallows) then
# reach their fallback in seconds instead of never.  Any success resets it --
# a flaky minute must not disable the channel for the rest of the run.
CDX_BREAKER_LIMIT = int(os.environ.get("CDX_BREAKER_LIMIT") or 3)
_breaker_lock = threading.Lock()
_breaker = {"consecutive": 0, "reason": ""}


def cdx_breaker_state():
    """(tripped, consecutive_failures, last_reason) — for reporting."""
    with _breaker_lock:
        return (_breaker["consecutive"] >= CDX_BREAKER_LIMIT,
                _breaker["consecutive"], _breaker["reason"])


def cdx_breaker_reset():
    with _breaker_lock:
        _breaker["consecutive"] = 0
        _breaker["reason"] = ""


def cdx_breaker_check():
    """Raise CDXUnavailable if the breaker is already open.

    Exported so the three *_discover_boards.py transports — which keep their
    own cdx_get because they validate `r.text` rather than a capped stream —
    can share ONE breaker with this module instead of each carrying a private
    failure counter that learns nothing from the others.
    """
    with _breaker_lock:
        if _breaker["consecutive"] >= CDX_BREAKER_LIMIT:
            raise CDXUnavailable(
                "CDX breaker open after %d consecutive failures (%s) — "
                "not retrying" % (_breaker["consecutive"], _breaker["reason"]))


def cdx_breaker_record(reason):
    """Record one exhausted request; announce the trip on the way past."""
    with _breaker_lock:
        _breaker["consecutive"] += 1
        _breaker["reason"] = reason or "unknown"
        n = _breaker["consecutive"]
    if n == CDX_BREAKER_LIMIT:
        print("  CDX BREAKER OPEN after %d consecutive failures (%s) — "
              "further requests fail fast this run" % (n, reason), flush=True)


class CDXUnavailable(RuntimeError):
    """The archive is refusing everything; stop asking.

    A subclass of RuntimeError on purpose -- every existing `except
    RuntimeError` and `except Exception` call site keeps working unchanged.
    """


def cdx_get(s, params, expect="rows", apex=None):
    cdx_breaker_check()
    last = None
    for attempt in range(TRIES):
        try:
            r = s.get(CDX, params=params, timeout=TIMEOUT, stream=True)
            if r.status_code == 200:
                body = _read_capped(r)
                ok, why = validate(body, expect, apex)
                if ok:
                    cdx_breaker_reset()
                    return body
                last = "bad-body: " + why
            else:
                last = "HTTP %d" % r.status_code
        except (requests.RequestException, RuntimeError) as e:
            last = type(e).__name__ + ": " + str(e)[:60]
        # Do not sleep after the final attempt -- it is 240s of pure waste on
        # the way to raising anyway.
        if attempt < TRIES - 1:
            time.sleep(min(15 * (2 ** attempt), 240) + random.uniform(0, 5))
    cdx_breaker_record(last)
    raise RuntimeError("CDX failed after %d tries: %s" % (TRIES, last))


def numpages(s, apex, since=None):
    """Page count for this apex.  `since` (YYYYMMDD) restricts the count to
    captures on or after that day -- the window that keeps a finished crawl
    from freezing forever.  See cdx_common."""
    q = {"url": apex.pattern, "showNumPages": "true"}
    if since:
        q["from"] = since
    body = cdx_get(s, q, expect="int")
    return int(body.strip() or 0)


def page_params(apex, page, collapse=None, since=None):
    q = {"url": apex.pattern, "fl": apex.fl,
         "collapse": collapse or apex.collapse, "page": str(page)}
    if since:
        q["from"] = since
    return q


def expect_for(apex):
    """What validate() should demand of this apex's rows."""
    return "urls" if apex.fl == "original" else "rows"


# ---------------------------------------------------------------- state
def load_state(path):
    st = {}
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                st = json.load(f)
        except (OSError, json.JSONDecodeError):
            st = {}
    st.setdefault("pages_done", {})
    st.setdefault("numpages", {})
    st.setdefault("empty_pages", [])
    st.setdefault("collapse_probe", {})
    # last_crawl / last_crawl_inferred are cdx_common's; seed them here so a
    # caller that never imports cdx_common still round-trips them intact.
    st.setdefault("last_crawl", {})
    return st


def save_state(path, st):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f)
    os.replace(tmp, path)
