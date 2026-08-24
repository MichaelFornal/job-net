#!/usr/bin/env python3
"""
atstail_fetch.py — the small-tail ATS families, one script, six pipes.

Every family writes into the SAME contract_norm/atstail.jsonl, each row
carrying its own `source` value, because none of these is big enough to
deserve a file (67 / 58 / 64 / 30 / 45 / 33 tenants).  `board` is always the
tenant id, which is how the pipeline attributes rows to a board.

Tenant lists come from tenants/<family>.csv — ids parsed out of apply URLs in
the pipeline's own 1.17M-row corpus, so every one is a board that really
existed.  They are a FLOOR per platform, not a census.

---------------------------------------------------------------------------
ROUTES (all unauthenticated, all verified live 2026-07-28/29)
---------------------------------------------------------------------------
pinpoint     GET  {t}.pinpointhq.com/postings.json
             One request, the whole board, and by far the richest payload in
             this group: employment_type_text, workplace_type ("remote" /
             "hybrid" / "onsite" — a real structured facet, not a guess),
             structured compensation_minimum/maximum/currency/frequency, and
             a location object.  What it does NOT carry is a posted date, so
             see PP_DETAIL below.
keka         GET  {t}.keka.com/careers/api/jobs/default/active
             GET  {t}.keka.com/careers/api/organization/default/careerportalinfo
             One request for the whole board — salaryRange, jobLocations,
             jobType and publishedOn — plus one for the company name.
             Keka ships TWO careers front-ends: a 1.7KB loader shell that
             fetches /ats/documents/{guid}/careerportal/*.html, and a fully
             server-rendered page with no guid anywhere.  Going through the
             guid (the obvious route, since the shell hands it to you) works
             on the first and left 21 of 58 tenants unreadable.  This route
             is what both front-ends ultimately call and needs no guid.
join         GET  join.com/companies/{t}?page=N  -> __NEXT_DATA__
             There is no public JSON route (api.join.com/v1 is HTTP 410,
             join.com/api/* is 401, and /_next/data/{buildId}/... 404s because
             the buildId contains an '@' the router will not round-trip).  The
             SSR payload is complete though: workplaceType, employmentType,
             salaryAmountFrom/To and createdAt.  pageSize is pinned at 5
             server-side — perPage/pageSize are ignored — so a board costs
             ceil(total/5) requests.  Boards here are tiny (median well under
             20 postings), and JOIN_MAX_PAGES bounds the tail.
zohorecruit  GET  {host}/recruit/v2/public/Job_Openings?pagename={portal}
             Unauthenticated JSON, one shot, the entire board in one response.
             NOT OAuth — /recruit/v2/public/* is a separate anonymous surface
             from the OAuth /recruit/v2/* the old note here assumed, which is
             why this family used to scrape RSS and a 1.7MB SPA page instead.
             Measured: no pagination and no cap (one tenant returns 568 jobs),
             and `pagename` must be the ONLY query param — adding page /
             per_page / limit makes it answer 200 with zero rows.
             {host} carries the data centre: all 8 resolve and behave
             identically (.com .eu .in .com.au .jp .ca .sa .uk), so the tenant
             id in tenants/zohorecruit.csv is the FULL HOST.  A bare slug with
             no dot still means .com, which is what the original 30 rows were.
             The portal segment is per-tenant but overwhelmingly "Careers"
             (29,997 of 30,000 archived URLs); it is parsed out of the tenant
             list's example_url with the three known values as fallbacks.
             Per job: Posting_Title, id, $url, Date_Opened (a REAL posting
             date), Job_Description, Job_Type, Remote_Job, City/State/Country,
             Zip_Code, Industry, Currency, Work_Experience.
             The field set is per-tenant configurable — one board returns 11
             keys, another 18, and some return no Country at all — so every
             read is a .get() and nothing is assumed present.
taleo        GET  {t}.taleo.net/careersection/{cs}/jobsearch.ftl?lang=en
             POST {t}.taleo.net/careersection/rest/jobboard/searchjobs
                    ?lang=en&portal={portalNo}
             The careersection code {cs} comes from the tenant list's
             example_url (they are per-customer: "ex", "2", "oh_int",
             "fast+pass+application+flow", ...).  The search endpoint is POST
             only (GET is 405) and it is FUSSY — see f_taleo() for the three
             things that make it answer 500 instead of 200.
wttj         GET  api.welcometothejungle.com/api/v1/organizations/{slug}
             POST {app}-dsn.algolia.net/1/indexes/wttj_jobs_production_en/query
             Reported as "gated"; it is not.  www.welcometothejungle.com is a
             client-rendered Next.js app whose SSR HTML contains no jobs (that
             is what reads as a gate), but the page ships window.env with the
             public search credentials, and the search index answers with the
             full posting record.  The key IS referer-restricted: without a
             welcometothejungle.com Referer every query is HTTP 403 "Method
             not allowed with this referer", which looks exactly like a block.

---------------------------------------------------------------------------
DEAD-TENANT DISCRIMINATORS — the expensive part
---------------------------------------------------------------------------
A status code alone is worthless here.  Per family, the ONLY thing that
actually separates a retired tenant from a live one:

  pinpoint    HTTP 404 (honest).  Wildcard DNS resolves any subdomain, but the
              app itself 404s with an 11KB branded error page.  Clean.
  keka        HTTP 302.  THE TRAP: a dead tenant 302s to
              /careers/Content/TenantNotFound.html ("Invalid Tenant"), 1.8KB
              of text/html — so to any redirect-following client it is an
              HTTP 200 that is indistinguishable from a small live board by
              status, size or content-type.  Discriminator = do NOT follow
              redirects: on the jobs endpoint, live = 200 + JSON array,
              dead = 302.
  join        HTTP 404 (honest — one shared host, real routing).
  zohorecruit the JSON error code, which is exact — this replaced a
              content-type sniff that could not tell a dead tenant from a
              live one with RSS switched off, and wrote off 27 of 30 tenants
              on the first pass.
                403 ORG_NOT_FOUND         -> genuinely dead, terminal
                404 CAREER_PAGE_NOT_FOUND -> org is ALIVE, wrong portal name;
                                             try the next one before judging
                200 + data[]              -> live
                200 + HTML (~2.6KB)       -> ALSO dead.  A second retired-
                                             tenant shape that answers 200
                                             with "<tenant> does not exist"
                                             instead of the 403 JSON, so the
                                             BODY is the discriminator, not
                                             the status.  Without this branch
                                             such a tenant burns five retries
                                             and fails the run as Transient.
              THE TRAP that makes the API the only usable probe: the career
              PAGE answers HTTP 200 for tenants that do not exist, so any
              liveness check against /jobs/{portal} scores every nonexistent
              slug as live.  Measured on 56 tenants: 39 live, 11 ORG_NOT_FOUND,
              5 CAREER_PAGE_NOT_FOUND.
  taleo       DNS NXDOMAIN.  taleo.net does NOT wildcard, so a retired tenant
              simply does not resolve; getaddrinfo() is checked first and a
              gaierror is recorded dead rather than retried five times.  A
              resolving host whose careersection is gone answers 404, or 200
              with a 1.5KB "Career Section Unavailable" shell — also dead.  A
              live host whose careersection is empty answers 200 with
              pagingData.totalCount 0: a live board with no jobs, recorded
              done.  And a live host on the CLASSIC career section answers
              200 with 130-250KB of requisitionListInterface markup and no
              portalNo — see Unsupported.
  wttj        HTTP 404 from the organizations endpoint = the company is off
              the platform (dead).  HTTP 200 + zero Algolia hits = live
              company, no open roles.  These MUST be told apart, because
              Algolia returns 0 hits for both and neither is an error.

Nothing above is ever retried as a transient failure, and nothing transient is
ever recorded as an empty board.  A tenant that could not be REACHED is left
absent from the state file so the next run picks it up (rule 3).

There is a fourth state besides pending / done / dead: `unsupported`, for a
tenant that is provably alive behind a front-end this pipe cannot read (today:
the 10 classic-JSF Taleo career sections).  It is terminal — retrying it every
night would fail the run forever — but it is deliberately NOT "dead", because
those boards are hiring and a later pass can claim them.

---------------------------------------------------------------------------
POSTED DATES
---------------------------------------------------------------------------
keka / join / zohorecruit / wttj all publish a real date.  pinpoint does not,
and taleo's date is a column that only some careersections configure.  For
those, atstail_posted_ledger.json records a first-seen date, keyed by
"{source}|{job_id}", and it is CARRY-FORWARD ONLY: a job already in the ledger
keeps its date forever.  Deleting or rotating that file re-dates the entire
back catalogue to the day it is rebuilt, so don't.

pinpoint has a real datePosted in the per-posting page's JobPosting ld+json.
PP_DETAIL=1 (the default) fetches it, capped at PP_DETAIL_CAP per tenant;
PP_DETAIL=0 skips it and falls back to the ledger.  It also lifts the real
company name (hiringOrganization) — postings.json has no company field at all.

Resumable via atstail_fetch_state.json (per family+tenant).  ATSTAIL_BUDGET_MIN
bounds the wall clock; unreached tenants stay pending.
"""
import concurrent.futures as cf
import csv
import html
import json
import os
import random
import re
import socket
import threading
import time
from urllib.parse import quote, unquote

import requests

import contract_common as cc
import remote_us_classify as ruc

BASE = os.path.dirname(os.path.abspath(__file__))
TENANT_DIR = os.path.join(BASE, "tenants")
OUT = os.path.join(cc.NORM_DIR, "atstail.jsonl")
STATE = os.path.join(BASE, "atstail_fetch_state.json")
LEDGER = os.path.join(BASE, "atstail_posted_ledger.json")

FAMILIES = ["pinpoint", "keka", "join", "zohorecruit", "taleo", "wttj"]

# Per-family request budget.  One flat worker pool serves all six, so without
# this the fan-out lands on whichever host is slowest.  join and wttj are
# SINGLE shared hosts (join.com, algolia) and get the tightest budgets; the
# other four are per-tenant subdomains where the load is spread across
# hundreds of edges and a higher rate is harmless.  taleo is deliberately slow:
# it is a stateful Java app (Oracle TEE), the search endpoint is a POST that
# does real work, and these are 45 enterprise boards worth ~8.5k rows.
RPS = {"pinpoint": 6.0, "keka": 6.0, "join": 2.0,
       "zohorecruit": 4.0, "taleo": 3.0, "wttj": 5.0}

# Max threads inside one family at once.  RPS bounds the average rate;
# this bounds the BURST, which is what a Cloudflare-fronted per-IP limiter
# actually reacts to (see the long note in ats_fetch.work()).
CONCURRENCY = {"pinpoint": 12, "keka": 12, "join": 4,
               "zohorecruit": 8, "taleo": 8, "wttj": 6}

WORKERS = int(os.environ.get("ATSTAIL_WORKERS") or 48)
BUDGET_MIN = int(os.environ.get("ATSTAIL_BUDGET_MIN") or 0)
TIMEOUT = 40
TRIES = 5

PP_DETAIL = os.environ.get("PP_DETAIL", "1") == "1"
PP_DETAIL_CAP = int(os.environ.get("PP_DETAIL_CAP") or 120)
# Whole-run ceiling on pinpoint detail requests.  The per-tenant cap alone
# bounds one pathological board; this bounds the RUN, so a bigger tenant list
# can never turn a 6-minute step into an hour.  Rows past it keep their ledger
# date instead of a fetched one.
PP_DETAIL_TOTAL = int(os.environ.get("PP_DETAIL_TOTAL") or 4000)
_pp_detail_n = [0]
JOIN_MAX_PAGES = int(os.environ.get("JOIN_MAX_PAGES") or 40)
TALEO_MAX_PAGES = int(os.environ.get("TALEO_MAX_PAGES") or 120)

TODAY = time.strftime("%Y-%m-%d", time.gmtime())

# WTTJ public search credentials, read out of window.env on
# www.welcometothejungle.com.  The key is scoped to search and is referer-
# restricted; WTTJ_REFERER is not optional.
WTTJ_APP = "CSEKHVMS53"
WTTJ_KEY = "4bd8f6215d0cc52b26430765769e65a0"
WTTJ_INDEX = "wttj_jobs_production_en"
WTTJ_REFERER = "https://www.welcometothejungle.com/"

_lock = threading.Lock()
_writer = None
_state = cc.load_state(STATE)
_ledger = cc.load_state(LEDGER)
_counts = {}
_live = {}
_dead = {}
_unsup = {}
_failed_now = set()

# ---- per-family token bucket ----------------------------------------------
_rate_lock = threading.Lock()
_next_t = {k: 0.0 for k in RPS}
_interval = {k: 1.0 / v for k, v in RPS.items()}
_pause_until = {k: 0.0 for k in RPS}
_n429 = {}
_gate = {f: threading.Semaphore(CONCURRENCY[f]) for f in FAMILIES}


class Transient(Exception):
    """A retryable failure.  Raised — never returned as an empty board — so
    the tenant stays PENDING and a re-run retries it."""


class Dead(Exception):
    """The tenant is confirmed gone.  Recorded as dead, never retried."""


class Unsupported(Exception):
    """The tenant is demonstrably ALIVE but serves a front-end this pipe
    cannot read.  A third state on purpose: calling it dead would be a lie
    about the platform's coverage, calling it done would claim an empty board,
    and leaving it pending would fail every run forever."""


def throttle(fam):
    while True:
        with _rate_lock:
            now = time.time()
            if now < _pause_until[fam]:
                wait = _pause_until[fam] - now
            else:
                wait = _next_t[fam] - now
                if wait <= 0:
                    _next_t[fam] = max(_next_t[fam] + _interval[fam], now)
                    return
        time.sleep(min(wait, 5) if wait > 0 else 0)


def slow_down(fam):
    """Only ever called on a GENUINE 429/418.  Every family's dead-tenant
    signal is handled before this point precisely so a retired board can
    never trip a platform-wide backoff."""
    with _rate_lock:
        _pause_until[fam] = time.time() + 20
        _interval[fam] = min(_interval[fam] * 1.5, 0.5)
        _n429[fam] = _n429.get(fam, 0) + 1


def req(fam, s, url, method="GET", dead_codes=(404, 410), **kw):
    """Throttled request -> requests.Response, or Dead / Transient.

    dead_codes are the statuses this family uses to mean "board is gone".
    Everything unclassified is transient and gets retried; only 429/418 backs
    the family off.  403 is NOT in the default dead set and NOT treated as a
    rate limit here — none of these six platforms uses it for either.
    """
    last = None
    for attempt in range(TRIES):
        throttle(fam)
        try:
            r = s.request(method, url, timeout=TIMEOUT, **kw)
            if r.status_code == 200:
                return r
            if r.status_code in dead_codes:
                raise Dead(f"HTTP {r.status_code} {url}")
            if r.status_code in (429, 418):
                slow_down(fam)
                last = f"HTTP {r.status_code}"
            else:
                last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(1.5 * (attempt + 1))
    raise Transient(f"{url} failed after {TRIES} tries: {last}")


def rjson(r):
    try:
        return r.json()
    except ValueError:
        raise Transient(f"non-JSON body from {r.url}")


_TAG_RX = re.compile(r"<[^>]+>")


def _text(frag):
    return re.sub(r"\s+", " ",
                  html.unescape(_TAG_RX.sub(" ", frag or ""))).strip()


def _num(v):
    try:
        f = float(v)
    except (TypeError, ValueError):
        return ""
    return f if f else ""


def posted_for(source, job_id, real):
    """Real date wins.  Otherwise fall back to the carry-forward ledger, which
    remembers the day this job id was FIRST seen — so a job keeps its age
    across runs instead of looking brand new every night.

    A date in the FUTURE is not a posting date and is thrown away.  This is
    not hypothetical: Taleo career sections publish an unlabelled date column
    and two of them (equitable, cuhk) put the requisition's CLOSE date there,
    which dated 372 rows 2036-07-29 and 2027-05-31.  Every downstream date cut
    would have ranked those first, forever.
    """
    if real and real > TODAY:
        real = ""
    if real:
        return real
    key = f"{source}|{job_id}"
    if not job_id:
        return ""
    with _lock:
        rec = _ledger.get(key)
        if rec is None:
            _ledger[key] = {"posted": TODAY, "precision": "first_seen",
                            "first_seen": TODAY}
            return TODAY
        return rec.get("posted") or ""


# ===========================================================================
# pinpoint
# ===========================================================================
_LD_RX = re.compile(
    r'<script[^>]*application/ld\+json[^>]*>(.*?)</script>', re.S | re.I)


def _ld_jobposting(body):
    for blob in _LD_RX.findall(body or ""):
        try:
            d = json.loads(blob)
        except (ValueError, TypeError):
            continue
        if isinstance(d, list):
            d = next((x for x in d if isinstance(x, dict)
                      and x.get("@type") == "JobPosting"), None)
        if isinstance(d, dict) and d.get("@type") == "JobPosting":
            return d
    return None


def f_pinpoint(s, tenant, _meta):
    # 404 here is honest: pinpointhq.com wildcards DNS but the Rails app
    # answers 404 with a branded error page for an unknown tenant.
    d = rjson(req("pinpoint", s, f"https://{tenant}.pinpointhq.com/postings.json"))
    postings = d.get("data") if isinstance(d, dict) else d
    if not isinstance(postings, list):
        raise Transient(f"pinpoint {tenant}: unexpected payload shape")
    company = ""
    out = []
    for i, p in enumerate(postings):
        if not isinstance(p, dict):
            continue
        title = p.get("title") or ""
        url = p.get("url") or ""
        if not url and p.get("path"):
            url = f"https://{tenant}.pinpointhq.com{p['path']}"
        wt = (p.get("workplace_type") or "").lower()
        loc = p.get("location") if isinstance(p.get("location"), dict) else {}
        city = loc.get("city") or ""
        state = loc.get("province") or ""
        country = loc.get("name") or ""
        locstr = ", ".join(x for x in (city, state, country) if x)
        real_posted = ""
        det = None
        want_detail = False
        if PP_DETAIL and url and i < PP_DETAIL_CAP:
            with _lock:
                if _pp_detail_n[0] < PP_DETAIL_TOTAL:
                    _pp_detail_n[0] += 1
                    want_detail = True
        if want_detail:
            try:
                det = _ld_jobposting(req("pinpoint", s, url).text)
            except Dead:
                det = None
            except Transient:
                det = None      # a missing date must not kill a whole board
        if det:
            real_posted = cc.ymd(iso=det.get("datePosted"))
            if not company:
                org = det.get("hiringOrganization")
                if isinstance(org, dict):
                    company = org.get("name") or ""
        out.append(cc.row(
            source="pinpoint", source_type="ats",
            company=company or tenant.replace("-", " ").title(),
            title=title,
            employment_type=cc.classify_any(
                p.get("employment_type_text") or p.get("employment_type"),
                title),
            # workplace_type is a real structured facet on Pinpoint, so this
            # is not inference.
            work_mode={"remote": "Remote", "hybrid": "Hybrid",
                       "onsite": "On-site"}.get(wt, ""),
            location=locstr, city=city, state=state, country=country,
            remote=(wt == "remote"),
            salary_min=_num(p.get("compensation_minimum")),
            salary_max=_num(p.get("compensation_maximum")),
            salary_currency=p.get("compensation_currency") or "",
            salary_period=p.get("compensation_frequency") or "",
            posted=posted_for("pinpoint", str(p.get("id") or ""), real_posted),
            apply_url=url, job_id=str(p.get("id") or ""), board=tenant,
            board_url=f"https://{tenant}.pinpointhq.com/"))
    # Backfill the company name onto rows emitted before the first detail hit.
    if company:
        for r in out:
            r["company"] = company
    return out


# ===========================================================================
# keka
# ===========================================================================
_KEKA_PERIOD = {0: "", 1: "hour", 2: "biweekly", 3: "month", 4: "year"}


def f_keka(s, tenant, _meta):
    # THE TRAP: an unknown keka tenant answers HTTP *200* — it 302s to
    # /careers/Content/TenantNotFound.html ("Invalid Tenant"), 1.8KB of
    # text/html, which through a redirect-following client is indistinguishable
    # from a small live board by status, size or content-type.
    #
    # So redirects are NOT followed: on this endpoint a live tenant answers 200
    # with a JSON array and a dead one answers 302.  That is the whole
    # discriminator, and it costs one request.
    #
    # (Keka ships two different careers front-ends — a 1.7KB loader shell that
    # fetches /ats/documents/{guid}/careerportal/*.html, and a fully
    # server-rendered page with no guid anywhere.  Scraping the guid works only
    # on the first and left 21 of 58 tenants unresolvable.  This route is the
    # one both front-ends ultimately call, and it needs no guid at all.)
    r = req("keka", s,
            f"https://{tenant}.keka.com/careers/api/jobs/default/active",
            dead_codes=(301, 302, 404, 410), allow_redirects=False)
    jobs = rjson(r)
    if not isinstance(jobs, list):
        raise Transient(f"keka {tenant}: unexpected /active payload")
    company = tenant.replace("-", " ").title()
    if jobs:
        try:
            info = rjson(req(
                "keka", s, f"https://{tenant}.keka.com/careers/api/"
                           f"organization/default/careerportalinfo"))
            if isinstance(info, dict) and info.get("name"):
                company = info["name"]
        except (Dead, Transient):
            pass
    out = []
    for j in jobs:
        if not isinstance(j, dict):
            continue
        title = j.get("title") or ""
        locs = j.get("jobLocations") or []
        names, cities, states, countries = [], [], [], []
        for lo in locs:
            if not isinstance(lo, dict):
                continue
            for key, bucket in (("name", names), ("city", cities),
                                ("state", states), ("countryName", countries)):
                v = lo.get(key)
                if v:
                    bucket.append(str(v))
        locstr = "; ".join(dict.fromkeys(names)) or "; ".join(
            dict.fromkeys(cities))
        sal = j.get("salaryRange") if isinstance(j.get("salaryRange"),
                                                 dict) else {}
        # jobType is Keka's own enum: 1 = Part Time, 2 = Full Time.  Nothing
        # contingent is expressible, so contract/temp roles can only be caught
        # from the title, which is what classify_any's fallback does.
        jt = {1: "Part Time", 2: "Full Time"}.get(j.get("jobType"))
        out.append(cc.row(
            source="keka", source_type="ats", company=company, title=title,
            employment_type=cc.classify_any(jt, title),
            # No workplace field on Keka.  Infer from LOCATION TEXT ONLY —
            # never from the title, where "Remote Sensing Engineer" would lie.
            work_mode=cc.work_mode(text=locstr),
            location=locstr,
            city=cities[0] if cities else "",
            state=states[0] if states else "",
            country=countries[0] if countries else "",
            remote="remote" in locstr.lower(),
            salary_min=_num(sal.get("minimum")),
            salary_max=_num(sal.get("maximum")),
            salary_currency=sal.get("currency") or "",
            salary_period=_KEKA_PERIOD.get(sal.get("salaryPeriod"), ""),
            posted=posted_for("keka", str(j.get("id") or ""),
                              cc.ymd(iso=j.get("publishedOn"))),
            # /careers/jobdetails/{id} is the form 1,000 of the 1,152 keka
            # URLs already in the master use; the bare /jobdetails/{id} form
            # is the minority.  Matching the majority is what lets these rows
            # merge instead of duplicating.
            apply_url=f"https://{tenant}.keka.com/careers/jobdetails/"
                      f"{j.get('id')}",
            job_id=str(j.get("id") or ""), board=tenant,
            board_url=f"https://{tenant}.keka.com/careers/"))
    return out


# ===========================================================================
# join
# ===========================================================================
_NEXT_RX = re.compile(
    r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', re.S)
_JOIN_WM = {"ONSITE": "On-site", "REMOTE": "Remote", "HYBRID": "Hybrid"}
_JOIN_PERIOD = {"PER_YEAR": "year", "PER_MONTH": "month", "PER_DAY": "day",
                "PER_HOUR": "hour", "PER_WEEK": "week"}


def _join_page(s, tenant, page):
    url = f"https://join.com/companies/{quote(tenant)}"
    if page > 1:
        url += f"?page={page}"
    r = req("join", s, url)
    m = _NEXT_RX.search(r.text or "")
    if not m:
        raise Transient(f"join {tenant}: no __NEXT_DATA__ on page {page}")
    try:
        d = json.loads(m.group(1))
    except ValueError:
        raise Transient(f"join {tenant}: unparseable __NEXT_DATA__")
    return ((d.get("props") or {}).get("pageProps") or {}).get(
        "initialState") or {}


def f_join(s, tenant, _meta):
    st = _join_page(s, tenant, 1)
    comp = st.get("company") if isinstance(st.get("company"), dict) else {}
    company = comp.get("name") or tenant
    out = []
    seen = set()
    page = 1
    while page <= JOIN_MAX_PAGES:
        jobs = st.get("jobs") if isinstance(st.get("jobs"), dict) else {}
        items = jobs.get("items") or []
        pg = jobs.get("pagination") or {}
        for j in items:
            if not isinstance(j, dict):
                continue
            idp = j.get("idParam") or ""
            if not idp or idp in seen:
                continue
            seen.add(idp)
            city = j.get("city") if isinstance(j.get("city"), dict) else {}
            cname = city.get("cityName") or ""
            cocountry = city.get("countryName") or ""
            locstr = ", ".join(x for x in (cname, cocountry) if x)
            wt = (j.get("workplaceType") or "").upper()
            et = j.get("employmentType") if isinstance(
                j.get("employmentType"), dict) else {}
            # salaryAmount* are integer CENTS ({'amount': 6000000} = 60,000).
            smin = j.get("salaryAmountFrom") or {}
            smax = j.get("salaryAmountTo") or {}
            cur = (smin.get("currency") or smax.get("currency") or "")
            show = (j.get("settings") or {}).get("showSalary")
            out.append(cc.row(
                source="join", source_type="ats", company=company,
                title=j.get("title") or "",
                employment_type=cc.classify_any(et.get("name"),
                                                j.get("title")),
                work_mode=_JOIN_WM.get(wt, ""),      # structured field
                location=locstr, city=cname,
                country=(j.get("country") or {}).get("iso3166") or cocountry,
                remote=(wt == "REMOTE"),
                salary_min=(_num(smin.get("amount")) / 100
                            if show and smin.get("amount") else ""),
                salary_max=(_num(smax.get("amount")) / 100
                            if show and smax.get("amount") else ""),
                salary_currency=cur if show else "",
                salary_period=_JOIN_PERIOD.get(j.get("salaryFrequency"), "")
                if show else "",
                posted=posted_for("join", str(j.get("id") or ""),
                                  cc.ymd(iso=j.get("createdAt"))),
                apply_url=f"https://join.com/companies/{tenant}/{idp}",
                job_id=str(j.get("id") or ""), board=tenant,
                board_url=f"https://join.com/companies/{tenant}"))
        total_pages = pg.get("pageCount") or 1
        if page >= total_pages or not items:
            break
        page += 1
        st = _join_page(s, tenant, page)
    return out


# ===========================================================================
# zohorecruit
# ===========================================================================
_ZOHO_PORTALS = ["Careers", "English", "Job-openings"]
_ZOHO_YES = {"yes", "true", "1", "y"}
# A page title of "Jobs at Careers" is an org that never set a display name —
# the portal label leaking through.  These are not company names.
_ZOHO_JUNK_CO = {"careers", "career", "jobs", "job", "job openings",
                 "job-openings", "english", "home", "recruit", "portal"}
_zoho_dropped = {"n": 0}          # non-US rows, reported in the run summary


def _zoho_host(tenant):
    """tenants/zohorecruit.csv carries the FULL host ('elfonze.zohorecruit.in')
    so the data centre travels with the tenant.  A bare slug with no dot is the
    original 30 rows' format and still means .com."""
    t = (tenant or "").strip().lower().rstrip("/")
    return t if "." in t else f"{t}.zohorecruit.com"


def _zoho_portals(meta):
    """Portal candidates, best first.  'Careers' covers 29,997 of the 30,000
    archived Zoho job URLs, so the example_url guess is almost always right and
    the fallbacks almost never fire."""
    out = []
    m = re.search(r"zohorecruit\.[a-z.]+/jobs/([^/?#]+)",
                  meta.get("example_url") or "", re.I)
    if m:
        out.append(unquote(m.group(1)))
    for p in _ZOHO_PORTALS:
        if p not in out:
            out.append(p)
    return out


def _zoho_slug(title):
    """Zoho's own URL slug: drop everything that is not alphanumeric, space or
    hyphen, then turn each remaining space into a hyphen.  Verified against the
    links in the feed ('Sales - Career Accelerator Programme' ->
    'Sales---Career-Accelerator-Programme').  Only needed when a record somehow
    arrives without its $url."""
    t = re.sub(r"[^A-Za-z0-9 \-]", "", title or "")
    return re.sub(r"\s", "-", t).strip()


def _zoho_get(s, host, portal):
    """The public job list.  Returns (code, payload): code is "" for a live
    200, otherwise Zoho's own error code.

    That code is the only exact liveness signal this platform has — the career
    PAGE answers HTTP 200 for tenants that do not exist, so anything probing
    /jobs/{portal} scores every nonexistent slug as live.  This mirrors req()'s
    retry and backoff but classifies on the JSON body rather than the status
    line, because a 403 here is a verdict about the tenant and not about us.

    `pagename` must be the ONLY query param: adding page / per_page / limit
    makes the endpoint answer 200 with an empty data[].
    """
    url = (f"https://{host}/recruit/v2/public/Job_Openings"
           f"?pagename={quote(portal)}")
    last = None
    for attempt in range(TRIES):
        throttle("zohorecruit")
        try:
            r = s.get(url, timeout=TIMEOUT,
                      headers={"Accept": "application/json"})
            try:
                d = r.json()
            except ValueError:
                d = None
            if r.status_code == 200 and isinstance(d, dict) and "data" in d:
                return "", d
            if r.status_code == 200 and d is None:
                # The SECOND dead shape: HTTP 200 carrying Zoho's ~2.6KB HTML
                # shell, "<tenant>.zohorecruit.com does not exist".  Some
                # retired tenants answer this instead of the 403 JSON, and
                # their career page 200s as well, so the BODY is the only
                # thing that separates them from a live board.  Retrying it
                # five times and giving up as Transient (what happened before
                # this branch existed) leaves a dead tenant permanently
                # pending and fails the whole run.
                return "ORG_NOT_FOUND_HTML", None
            code = (d or {}).get("code") or ""
            if code in ("ORG_NOT_FOUND", "CAREER_PAGE_NOT_FOUND",
                        "INVALID_URL_PATTERN"):
                return code, None
            if r.status_code in (429, 418):
                slow_down("zohorecruit")
            last = f"HTTP {r.status_code} {code}".strip()
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(1.5 * (attempt + 1))
    raise Transient(f"{url} failed after {TRIES} tries: {last}")


_ZOHO_TITLE_RX = re.compile(
    r"<title>\s*(?:Jobs at\s+)?([^<|]{2,90}?)\s*(?:\||</title>)", re.I)


def _zoho_company(s, host, portal, fallback):
    """The JSON carries no org name, so the career page's <title> ("Jobs at X")
    is the only public source.  That page is ~1.7MB of SPA and Zoho ignores
    Range (it answers 200, not 206), so this streams and aborts at </title>
    instead of pulling the whole thing for one string.  One request per
    tenant, not per job.
    """
    head = ""
    try:
        throttle("zohorecruit")
        r = s.get(f"https://{host}/jobs/{quote(portal)}", timeout=TIMEOUT,
                  stream=True)
        for chunk in r.iter_content(8192):
            head += chunk.decode("utf-8", "replace")
            if "</title>" in head or len(head) > 65536:
                break
        r.close()
    except requests.RequestException:
        return fallback
    m = _ZOHO_TITLE_RX.search(head)
    if not m:
        return fallback
    co = html.unescape(m.group(1)).strip()
    if not co or co.lower() in _ZOHO_JUNK_CO or co.lower() == portal.lower():
        return fallback
    return co


def _zoho_date(v):
    """Date_Opened is MM/DD/YYYY — verified '09/17/2025' on an .in tenant, so
    the API normalises to US order regardless of data centre.  The first>12
    swap is a guard against a tenant that does not, not an observed case."""
    v = (v or "").strip()
    m = re.match(r"^(\d{1,2})/(\d{1,2})/(\d{4})$", v)
    if not m:
        return cc.ymd(iso=v) if v else ""
    mo, day, yr = int(m.group(1)), int(m.group(2)), m.group(3)
    if mo > 12:
        mo, day = day, mo
    return f"{yr}-{mo:02d}-{day:02d}"


# Zoho ships "Outside of United States" as a COUNTRY picklist value.  The
# shared classifier's strong-US wording test matches the "United States"
# inside it, so the country is rescued by its own name and the row lands in a
# US-only cut — 16 of them did before this guard.  Any negation has to be
# stripped before a US wording test runs, and the country travels inside the
# location string too, so both are checked.
_NOT_US_RX = re.compile(
    r"outside\s+(?:of\s+)?(?:the\s+)?(?:united\s+states|u\.?s\.?a?\b)", re.I)


def _zoho_us(country, state, location, currency):
    """"" when the row is not US.

    Country is authoritative when present.  When it is BLANK — and some
    tenants publish no Country field at all — a USD requisition is positive
    evidence on its own, which is the only thing that rescues those boards.
    A row with an explicit FOREIGN country is never rescued by currency: USD
    is quoted freely on offshore staffing requisitions, and Zoho's population
    is heavily Indian and EU agencies.
    """
    if _NOT_US_RX.search(f"{country} {location}"):
        return ""
    basis = ruc.us_basis(country, state, location)
    if basis:
        return basis
    if not (country or "").strip() and (currency or "").upper() == "USD":
        return "currency"
    return ""


def f_zohorecruit(s, tenant, meta):
    host = _zoho_host(tenant)
    board = host.split(".")[0]
    portals = _zoho_portals(meta)
    payload, portal, org_alive = None, "", False
    for p in portals:
        code, d = _zoho_get(s, host, p)
        if not code:
            payload, portal = d, p
            break
        if code in ("ORG_NOT_FOUND", "ORG_NOT_FOUND_HTML"):
            raise Dead(f"zohorecruit {host}: {code}")
        org_alive = True          # the org answered; only this name was wrong
    if payload is None:
        if org_alive:
            # Live tenant we cannot read — a real miss, not a dead board, so
            # it is recorded separately instead of being buried in the dead
            # count where nobody would go looking for it.
            raise Unsupported(f"zohorecruit {host}: org is live but none of "
                              f"{portals} is its portal name")
        raise Dead(f"zohorecruit {host}: no portal answered {portals}")

    company = _zoho_company(s, host, portal,
                            board.replace("-", " ").title())
    out, dropped = [], 0
    for j in payload.get("data") or []:
        if not isinstance(j, dict) or j.get("Publish") is False:
            continue
        title = j.get("Posting_Title") or j.get("Job_Opening_Name") or ""
        jid = str(j.get("id") or "")
        if not title or not jid:
            continue
        # Every read is a .get(): the field set is per-tenant configurable.
        # One board returns 11 keys, another 18, and some return no Country
        # and no Remote_Job at all.
        city = j.get("City") or ""
        state = j.get("State") or ""
        country = j.get("Country") or ""
        locstr = ", ".join(x for x in (city, state, country) if x)
        # Remote_Job is the STRING "Yes" or absent — not a boolean.  bool() on
        # it would score a literal "No" as remote.
        rem = str(j.get("Remote_Job") or "").strip().lower() in _ZOHO_YES
        # US-only.  Remote_Job is not a US claim: a remote requisition out of
        # an Indian agency is still not US work.
        if not _zoho_us(country, state, locstr, j.get("Currency")):
            dropped += 1
            continue
        out.append(cc.row(
            source="zohorecruit", source_type="ats", company=company,
            title=title,
            employment_type=cc.classify_any(j.get("Job_Type"), title),
            work_mode="Remote" if rem else cc.work_mode(text=locstr),
            location=locstr, city=city, state=state, country=country,
            remote=rem,
            posted=posted_for("zohorecruit", jid,
                              _zoho_date(j.get("Date_Opened"))),
            # $url arrives already stamped ?source=CareerSite, which is what
            # the corpus uses; the fallback only fires on a malformed record.
            apply_url=j.get("$url") or (
                f"https://{host}/jobs/{quote(portal)}/{jid}/"
                f"{_zoho_slug(title)}?source=CareerSite"),
            job_id=jid, board=board,
            board_url=f"https://{host}/jobs/{quote(portal)}"))
    if dropped:
        with _lock:
            _zoho_dropped["n"] += dropped
        print(f"  zohorecruit {host}: kept {len(out)}, dropped {dropped} "
              f"non-US", flush=True)
    return out


# ===========================================================================
# taleo
# ===========================================================================
_T_PORTAL_RX = re.compile(r"portalNo\s*:\s*'(\d+)'")
_T_FIELD_RX = re.compile(r'class="criteriaFieldInput"\s+id="([^"]+)"')
_T_FIELD_RX2 = re.compile(r'id="([^"]+)"\s+class="criteriaFieldInput"')
_T_CODE_RX = re.compile(r'name="code"\s+value="([^"]+)"')
_T_SORT_RX = re.compile(r'id="RELEVANCY-sortfield"[^>]*sortid="(\d+)"')
_T_SORT_ANY_RX = re.compile(r'sortid="(\d+)"')
_T_SCHEDULE = {
    "full-time": "Full-time", "full time": "Full-time", "fulltime": "Full-time",
    "part-time": "Part-time", "part time": "Part-time",
    "temporary": "Temp", "temp": "Temp", "seasonal": "Seasonal",
    "casual": "Temp", "contract": "Contract", "contractor": "Contract",
    "contingent": "Contract", "intern": "Internship",
    "internship": "Internship", "per diem": "Per Diem", "prn": "Per Diem",
    "fixed term": "Temp", "fixed-term": "Temp", "regular": "Full-time",
    "permanent": "Full-time", "freelance": "Freelance",
    "apprentice": "Internship", "volunteer": "Volunteer",
}
_T_DATE_RX = re.compile(
    r"^(\d{4}-\d{2}-\d{2}|\d{1,2}/\d{1,2}/\d{4}|"
    r"[A-Z][a-z]{2}\s+\d{1,2},?\s+\d{4})")


# The company name is nowhere in the Taleo search payload; the only place it
# appears is the career section's <title>, and that is customer-branded prose
# ("Careers at Baptist Health", "Hospitality Jobs | Jobs Hotel | Hyatt Jobs",
# "Internal Career Portal | State of Ohio").  Take the LAST title (the theme's,
# not the product's "Job Search"), split it, drop the boilerplate parts, and
# shave the career-site suffixes.  Falls back to the tenant id.
_T_GENERIC = {
    "job search", "home", "careers", "career", "jobs", "search", "vacancies",
    "basic search", "advanced search", "quick search", "current openings",
    "career opportunities", "job opportunities", "employment opportunities",
    "hospitality jobs", "jobs hotel", "welcome", "job listing", "job listings",
    "openings", "apply", "career center", "careers search", "job board",
    "careers site", "career site", "join us", "work with us",
}
_T_STRIP = re.compile(
    r"^(careers?\s+at\s+|jobs?\s+at\s+|welcome\s+to\s+)"
    r"|((\s|^)+(careers?|jobs?|vacancies|openings|search|employment|"
    r"opportunities))+$", re.I)


def _t_company(titles, tenant):
    for raw in reversed(titles or []):
        cands = []
        for part in re.split(r"\s*[|–—]\s*|\s+-\s+",
                             html.unescape(raw)):
            p = part.strip()
            if not p or p.lower() in _T_GENERIC:
                continue
            p = _T_STRIP.sub("", p).strip(" -|")
            if len(p) >= 2 and p.lower() not in _T_GENERIC:
                cands.append(p)
        if cands:
            return max(cands, key=len)
    return tenant.replace("-", " ").title()


def _resolves(host):
    try:
        socket.getaddrinfo(host, 443)
        return True
    except socket.gaierror:
        return False


def _t_date(v):
    v = (v or "").strip()
    if not _T_DATE_RX.match(v):
        return ""
    for fmt in ("%Y-%m-%d", "%m/%d/%Y", "%b %d, %Y", "%b %d %Y"):
        try:
            return time.strftime("%Y-%m-%d", time.strptime(v, fmt))
        except ValueError:
            continue
    return ""


def f_taleo(s, tenant, meta):
    host = f"{tenant}.taleo.net"
    # taleo.net does NOT wildcard.  A retired tenant simply stops resolving,
    # and six retries against NXDOMAIN is pure waste — check it up front and
    # record the tenant dead.
    if not _resolves(host):
        raise Dead(f"taleo {tenant}: NXDOMAIN")
    m = re.search(r"taleo\.net/careersection/([^/]+)/",
                  meta.get("example_url") or "")
    cs = m.group(1) if m else "ex"
    base = f"https://{host}"
    # A resolving host whose careersection is gone answers 404 with a
    # "Page not Found" page — dead, not transient.
    page = req("taleo", s,
               f"{base}/careersection/{cs}/jobsearch.ftl?lang=en").text or ""
    pm = _T_PORTAL_RX.search(page)
    if not pm:
        # No portalNo means this is not the faceted career section.  Two very
        # different things look identical at the HTTP layer:
        #   * "Career Section Unavailable" / an empty shell -> genuinely gone.
        #   * a 130-250KB page full of requisitionListInterface markup -> the
        #     CLASSIC (pre-faceted) career section.  It is alive and hiring —
        #     10 of the 45 tenants here are on it, Starbucks and NATO among
        #     them — but it renders through JSF postbacks (ftlstate /
        #     jsfCmdId round-trips), has no REST endpoint at all, and its
        #     initial HTML contains only an EMPTY template row.  Replaying
        #     that flow is a separate project; recording these as dead would
        #     misreport a quarter of the family as gone.
        if page.count("requisitionListInterface") > 20:
            raise Unsupported(
                f"taleo {tenant}/{cs}: classic (non-faceted) career section — "
                f"live, but JSF-postback only")
        raise Dead(f"taleo {tenant}/{cs}: careersection retired")
    portal = pm.group(1)
    # These three are why the endpoint answers 500 instead of 200:
    #   1. fieldData.fields must be keyed by THIS career section's
    #      .criteriaFieldInput element ids (usually KEYWORD/LOCATION, but
    #      some sections add ORGANIZATION).
    #   2. filterSelectionParam.searchFilterSelections must contain one entry
    #      per configured filter group — an empty array is a 500.
    #   3. sortBySelectionParam is the `sortid` ATTRIBUTE of the option, not
    #      its value; the numbers differ per section.
    fields = _T_FIELD_RX.findall(page) or _T_FIELD_RX2.findall(page)
    codes = _T_CODE_RX.findall(page)
    sm = _T_SORT_RX.search(page) or _T_SORT_ANY_RX.search(page)
    sortid = sm.group(1) if sm else "3"
    url = (f"{base}/careersection/rest/jobboard/searchjobs"
           f"?lang=en&portal={portal}")
    hdr = {"tz": "-240", "tzname": "America/New_York",
           "Referer": f"{base}/careersection/{cs}/jobsearch.ftl?lang=en"}
    company = _t_company(re.findall(r"<title>\s*([^<]{2,90}?)\s*</title>",
                                    page, re.I), tenant)
    out = []
    seen = set()
    pageno = 1
    while pageno <= TALEO_MAX_PAGES:
        payload = {
            "multilineEnabled": True,
            "sortingSelection": {"sortBySelectionParam": sortid,
                                 "ascendingSortingOrder": "false"},
            "fieldData": {"fields": {f: "" for f in fields}, "valid": True},
            "filterSelectionParam": {
                "searchFilterSelections":
                    [{"id": c, "selectedValues": []} for c in codes]},
            "advancedSearchFiltersSelectionParam": {
                "searchFilterSelections": []},
            "pageNo": pageno,
        }
        d = rjson(req("taleo", s, url, method="POST", json=payload,
                      headers=hdr))
        if d.get("careerSectionUnAvailable"):
            raise Dead(f"taleo {tenant}/{cs}: careerSectionUnAvailable")
        reqs = d.get("requisitionList") or []
        for j in reqs:
            if not isinstance(j, dict):
                continue
            jid = str(j.get("jobId") or "")
            if not jid or jid in seen:
                continue
            seen.add(jid)
            cols = [str(c) if c is not None else ""
                    for c in (j.get("column") or [])]
            li = j.get("linkedColumn")
            title = cols[li] if isinstance(li, int) and 0 <= li < len(cols) \
                else (cols[0] if cols else "")
            locidx = [i for i in (j.get("locationsColumns") or [])
                      if isinstance(i, int) and 0 <= i < len(cols)]
            locs = []
            for i in locidx:
                v = cols[i]
                if v.startswith("["):
                    try:
                        locs.extend(str(x) for x in json.loads(v))
                        continue
                    except ValueError:
                        pass
                if v:
                    locs.append(v)
            locstr = "; ".join(dict.fromkeys(locs))
            # The remaining columns are whatever this career section chose to
            # publish (schedule, shift, pay grade, posting date, ...) with no
            # labels in the response.  Only EXACT matches against a known
            # schedule vocabulary are accepted; anything else is ignored
            # rather than guessed at.
            raw_type = ""
            real_posted = ""
            for i, v in enumerate(cols):
                if i == li or i in locidx or not v:
                    continue
                if not raw_type and v.strip().lower() in _T_SCHEDULE:
                    raw_type = _T_SCHEDULE[v.strip().lower()]
                if not real_posted:
                    real_posted = _t_date(v)
            out.append(cc.row(
                source="taleo", source_type="ats", company=company,
                title=title,
                employment_type=cc.classify_any(raw_type, title),
                # No remote facet anywhere in the Taleo search payload.
                # Location text only, and blank when it says nothing.
                work_mode=cc.work_mode(text=locstr),
                location=locstr, remote="remote" in locstr.lower(),
                posted=posted_for("taleo", jid, real_posted),
                apply_url=f"{base}/careersection/{cs}/jobdetail.ftl?job={jid}",
                job_id=jid, board=tenant,
                board_url=f"{base}/careersection/{cs}/jobsearch.ftl"))
        pd = d.get("pagingData") or {}
        total = pd.get("totalCount") or 0
        size = pd.get("pageSize") or 25
        if not reqs or pageno * size >= total:
            break
        pageno += 1
    return out


# ===========================================================================
# wttj
# ===========================================================================
_WTTJ_WM = {"fulltime": ("Remote", True), "partial": ("Hybrid", False),
            "no": ("On-site", False)}
# "punctual" (occasional remote) and "unknown" deliberately map to nothing:
# a wrong remote flag is worse than none.


def f_wttj(s, tenant, _meta):
    # Liveness and job count are DIFFERENT questions here.  Algolia answers 0
    # hits both for a company that left the platform and for one with no open
    # roles, so the organizations endpoint is what decides dead vs live.
    org = rjson(req("wttj", s, "https://api.welcometothejungle.com/api/v1/"
                    f"organizations/{quote(tenant)}"))
    o = org.get("organization") if isinstance(org, dict) else {}
    company = (o or {}).get("name") or tenant
    out = []
    page = 0
    while page < 20:
        d = rjson(req(
            "wttj", s,
            f"https://{WTTJ_APP.lower()}-dsn.algolia.net/1/indexes/"
            f"{WTTJ_INDEX}/query", method="POST",
            headers={"X-Algolia-API-Key": WTTJ_KEY,
                     "X-Algolia-Application-Id": WTTJ_APP,
                     # Without this the key answers 403 on every request —
                     # which reads as a block, not as a missing header.
                     "Referer": WTTJ_REFERER,
                     "Content-Type": "application/json"},
            json={"query": "", "hitsPerPage": 1000, "page": page,
                  "filters": f'organization.slug:"{tenant}"',
                  "attributesToHighlight": []}))
        hits = d.get("hits") or []
        for h in hits:
            if not isinstance(h, dict):
                continue
            slug = h.get("slug") or ""
            offices = h.get("offices") or []
            o0 = offices[0] if offices and isinstance(offices[0], dict) else {}
            locstr = "; ".join(dict.fromkeys(
                ", ".join(x for x in (of.get("city"), of.get("country")) if x)
                for of in offices if isinstance(of, dict)))
            wm, rem = _WTTJ_WM.get((h.get("remote") or "").lower(), ("", False))
            out.append(cc.row(
                source="wttj", source_type="ats", company=company,
                title=h.get("name") or "",
                employment_type=cc.classify_any(h.get("contract_type"),
                                                h.get("name")),
                work_mode=wm, location=locstr,
                city=o0.get("city") or "", state=o0.get("state") or "",
                country=o0.get("country") or "", remote=rem,
                salary_min=_num(h.get("salary_minimum")),
                salary_max=_num(h.get("salary_maximum")),
                salary_currency=h.get("salary_currency") or "",
                salary_period=h.get("salary_period") or "",
                posted=posted_for("wttj", h.get("reference") or "",
                                  (h.get("published_at_date") or "")[:10]
                                  or cc.ymd(iso=h.get("published_at"))),
                apply_url=f"https://www.welcometothejungle.com/en/companies/"
                          f"{tenant}/jobs/{slug}",
                job_id=h.get("reference") or h.get("objectID") or "",
                board=tenant,
                board_url=f"https://www.welcometothejungle.com/en/companies/"
                          f"{tenant}/jobs"))
        if page + 1 >= (d.get("nbPages") or 1) or not hits:
            break
        page += 1
    return out


FETCH = {"pinpoint": f_pinpoint, "keka": f_keka, "join": f_join,
         "zohorecruit": f_zohorecruit, "taleo": f_taleo, "wttj": f_wttj}


# ===========================================================================
def emit(rows):
    with _lock:
        for r in rows:
            _writer.write(r)
            _counts[r["source"]] = _counts.get(r["source"], 0) + 1


def work(fam, tenant, meta):
    key = f"{fam}:{tenant}"
    with _lock:
        if _state.get(key) in ("done", "dead", "unsupported"):
            return
    with _gate[fam]:
        s = cc.session()
        s.headers.update({"Accept": "*/*"})
        try:
            rows = FETCH[fam](s, tenant, meta)
            emit(rows)
            with _lock:
                _state[key] = "done"
                _live[fam] = _live.get(fam, 0) + 1
        except Dead as e:
            with _lock:
                _state[key] = "dead"
                _dead[fam] = _dead.get(fam, 0) + 1
                print(f"  dead {fam}:{tenant} — {e}", flush=True)
        except Unsupported as e:
            with _lock:
                _state[key] = "unsupported"
                _unsup[fam] = _unsup.get(fam, 0) + 1
                print(f"  UNSUPPORTED {fam}:{tenant} — {e}", flush=True)
        except Exception as e:            # noqa: BLE001 — keep the pool alive
            # Anything not proven dead stays PENDING (absent from state) so a
            # re-run retries it.  Never recorded as an empty board.
            with _lock:
                _failed_now.add(key)
                print(f"  ERR  {fam}:{tenant} — {type(e).__name__}: "
                      f"{str(e)[:140]}", flush=True)
        finally:
            s.close()


def load_tenants(fam):
    path = os.path.join(TENANT_DIR, f"{fam}.csv")
    if not os.path.exists(path):
        print(f"  WARNING: {path} missing — {fam} skipped", flush=True)
        return []
    with open(path, newline="", encoding="utf-8") as f:
        return [r for r in csv.DictReader(f) if (r.get("tenant") or "").strip()]


def refresh_families(fams):
    """Re-fetch a family from scratch, without disturbing the other five.

    Every family appends into the SAME atstail.jsonl (cc.Writer opens "a"), so
    re-running one without this leaves its previous rows in place and emits
    every job twice.  Rotating the whole file is not an option either — that
    would discard the five families that are not being refreshed.  So: drop
    only this family's state keys, and rewrite the JSONL keeping every row
    that is not this family's.

    atstail_posted_ledger.json is deliberately NOT touched.  It is
    carry-forward only; clearing it would re-date the entire back catalogue to
    the day of the rebuild.
    """
    fams = set(fams)
    for k in [k for k in _state if k.split(":", 1)[0] in fams]:
        del _state[k]
    cc.save_state(STATE, _state)
    if not os.path.exists(OUT):
        return
    tmp = OUT + ".tmp"
    kept = gone = 0
    with open(OUT, encoding="utf-8") as src, \
            open(tmp, "w", encoding="utf-8") as dst:
        for line in src:
            try:
                if json.loads(line).get("source") in fams:
                    gone += 1
                    continue
            except ValueError:
                pass            # keep anything unparseable rather than lose it
            dst.write(line)
            kept += 1
    os.replace(tmp, OUT)
    print(f"  refresh {sorted(fams)}: dropped {gone} old rows, kept {kept} "
          f"from the other families", flush=True)


def main():
    global _writer
    only = [x for x in (os.environ.get("ATSTAIL_ONLY") or "").split(",") if x]
    fams = [f for f in FAMILIES if not only or f in only]
    again = [x for x in (os.environ.get("ATSTAIL_REFRESH") or "").split(",")
             if x in FAMILIES]
    if again:
        refresh_families(again)
    jobs = []
    for fam in fams:
        rows = load_tenants(fam)
        pend = [r for r in rows
                if _state.get(f"{fam}:{r['tenant'].strip()}")
                not in ("done", "dead", "unsupported")]
        print(f"  {fam}: {len(rows)} tenants, {len(pend)} pending", flush=True)
        for r in pend:
            jobs.append((fam, r["tenant"].strip(), r))
    print(f"pending tenants: {len(jobs)}", flush=True)
    if not jobs:
        print("nothing to do", flush=True)
        return
    # Interleave the families so all six drain concurrently instead of the
    # whole pool queueing behind one family's token bucket.
    random.shuffle(jobs)

    _writer = cc.Writer(OUT)
    done = 0
    stopped = False
    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = [ex.submit(work, f, t, m) for f, t, m in jobs]
        for _ in cf.as_completed(futs):
            done += 1
            if done % 25 == 0:
                with _lock:
                    cc.save_state(STATE, _state)
                    cc.save_state(LEDGER, _ledger)
                print(f"  {done}/{len(jobs)} tenants  rows={_counts}",
                      flush=True)
            if deadline and time.time() > deadline:
                stopped = True
                for f in futs:
                    f.cancel()
                break
    cc.save_state(STATE, _state)
    cc.save_state(LEDGER, _ledger)
    _writer.close()
    if _n429:
        print(f"  rate-limit backoffs: {_n429}", flush=True)
    print(f"\nDONE atstail_fetch -> {OUT}", flush=True)
    print(f"  rows per family : {_counts}", flush=True)
    print(f"  live tenants    : {_live}", flush=True)
    print(f"  dead tenants    : {_dead}", flush=True)
    if _unsup:
        print(f"  live-but-unreadable front ends: {_unsup}", flush=True)
    if _zoho_dropped["n"]:
        print(f"  zohorecruit non-US rows dropped: {_zoho_dropped['n']}",
              flush=True)
    if stopped:
        print(f"BUDGET REACHED after {BUDGET_MIN}min: {done}/{len(jobs)} "
              f"tenants; the rest stay pending for the next run.", flush=True)
        return
    if _failed_now:
        raise SystemExit(f"{len(_failed_now)} tenants failed — re-run to retry")


if __name__ == "__main__":
    main()
