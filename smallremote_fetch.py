#!/usr/bin/env python3
"""
smallremote_fetch.py — five small curated remote boards, one shared JSONL.

All five write into contract_norm/smallremote.jsonl, each with its own `source`
value, the same way boards_fetch.py shares boards.jsonl.  One source blowing up
never kills the others.

  jobspresso      remote_board  WP REST /wp-json/wp/v2/job-listings   ~2,800
  hubstafftalent  remote_board  /search/jobs XHR (HTML fragment)        ~710
  dynamitejobs    remote_board  sitemap -> per-job JSON-LD              ~500
  remoteio        remote_board  sitemap -> per-job JSON-LD            ~2,200
  justremote      remote_board  justremote-api.herokuapp.com             ~83

WHY THESE ENDPOINTS AND NOT THE OBVIOUS ONES
--------------------------------------------
* jobspresso: the RSS (?feed=job_feed&posts_per_page=5000) returns the whole
  board in ONE request and is tempting, but its two taxonomy tags are SWAPPED
  (<job_listing:job_type> carries 'Marketing', <job_listing:job_category>
  carries 'Full Time') and it does NOT carry meta._application.  The REST route
  does, and _application is the DIRECT employer apply URL — ashby / greenhouse /
  lever / workable links, not a jobspresso redirect — which is the whole reason
  this source is worth having: those rows canonicalise onto the same ATS key as
  the rest of the pipeline instead of landing as un-dedupable strangers.
  (Measured on page 1: 96/100 rows had an http apply URL, 35 ashby + 16
  greenhouse + 11 lever among them.)  Note /wp-json/wp/v2/job_listing (singular)
  404s; the route is job-listings (plural, hyphen).
  AND THE BIG ONE: jobspresso's 2,796 is a ten-year ARCHIVE, not live inventory.
  Measured over the full pull 2026-07-29, the posted-year histogram is 2017:981
  2018:631 2019:251 2020:211 2025:197 2024:166 2021:106 2023:75 2016:68 2022:67
  2026:43 — only 21 rows fall inside 90 days and 132 inside a year.  All three
  surfaces agree on 2,796 (REST x-wp-total, 2,796 RSS <item>s, admin-ajax
  max_num_pages 28) and meta._filled is 0 on every single one, so the board
  exposes NO live/dead flag to filter on.  The rows are kept undropped anyway
  because the date_gmt they carry is accurate: all_jobs_build.py's own cutoff
  retires the stale ones, which is the right place for that decision.  Do not
  read "jobspresso: 2796" as 2,796 open jobs.

* hubstafftalent: WITHOUT `X-Requested-With: XMLHttpRequest` you get HTTP 200
  and a 76 KB HTML page whose results div is EMPTY — a status-code-only check
  reads that as a healthy board with no jobs.  The discriminator is the body: a
  real answer starts with `$('#results').html("`.

* dynamitejobs / remoteio: /feed, /wp-json, /wp-json/wp/v2/job_listing and
  /sitemap_index.xml all answer HTTP 200 with an SPA/marketing shell on both
  hosts, and remote.io answers the same shell for /api/anything.  Only
  /sitemap*.xml returns real XML, so the sitemap -> per-job JSON-LD walk is the
  only route.  Because a sitemap can be an archive of dead postings, every row
  is checked against validThrough and expired postings are dropped (counted
  separately so "expired" is never confused with "fetch failed").

* justremote: justremote.co answers HTTP 200 with the same ~122 KB React shell
  for LITERALLY ANY path, so the site cannot be probed by status code at all.
  The API host was found in the client bundle.  page/per_page/limit/offset are
  all silently IGNORED — every variant returns the identical 83 rows — so 83 is
  the entire public corpus, not page one of it.

APPLY URLS
----------
Only jobspresso exposes the employer's real ATS link.  hubstafftalent,
dynamitejobs, remoteio and justremote all host the application themselves
(dynamite hides the outbound link behind a POST /api/job-apply/<id> that also
records a click-application event, so it is deliberately NOT called here — a
500-row crawl would fire 500 phantom apply events on a small board), and their
apply_url is therefore the board's own job page.  That is the truth, not a
shortcut: there is no employer URL to be had from those four.

RESUMING
--------
smallremote_fetch_state.json records per-source status plus, for the two
per-job-page crawls, the set of job URLs already written.  A re-run skips
finished sources and finished URLs, so an interrupted crawl resumes instead of
duplicating rows.  SMALLREMOTE_ONLY=a,b re-runs a subset; SMALLREMOTE_FORCE=1
re-runs a source that is already `done` (which DOES append duplicate rows —
truncate the JSONL first if that matters).
"""
import concurrent.futures as cf
import html
import json
import os
import re
import sys
import threading
import time
from datetime import datetime, timezone, timedelta

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(cc.NORM_DIR, "smallremote.jsonl")
STATE_JSON = os.path.join(BASE, "smallremote_fetch_state.json")

FORCE = os.environ.get("SMALLREMOTE_FORCE") == "1"
LIMIT = int(os.environ.get("SMALLREMOTE_LIMIT") or 0)   # cap per-job-page crawls
TODAY = datetime.now(timezone.utc).date()

TIMEOUT = 30
TRIES = 4

_lock = threading.Lock()


# --------------------------------------------------------------------------
# http
# --------------------------------------------------------------------------
class Throttle:
    """Token-spacing throttle shared by the threaded per-job-page crawls.

    back_off() widens the gap on a 429/5xx, and ok() narrows it again after a
    run of clean responses.  The recovery half matters: without it a single
    rate-limit blip early in a 2,200-page crawl permanently drops the crawl to
    the floor rate, and a 7-minute walk becomes an hour.
    """

    def __init__(self, rps):
        self.base = self.gap = 1.0 / rps
        self.next = 0.0
        self.pause_until = 0.0
        self.good = 0
        self.lk = threading.Lock()

    def wait(self):
        while True:
            with self.lk:
                now = time.time()
                if now >= self.pause_until:
                    d = self.next - now
                    if d <= 0:
                        self.next = max(self.next + self.gap, now)
                        return
                else:
                    d = self.pause_until - now
            time.sleep(min(d, 5.0))

    def back_off(self, secs=20.0, slow=1.4):
        with self.lk:
            self.pause_until = max(self.pause_until, time.time() + secs)
            self.gap = min(self.gap * slow, 3.0)
            self.good = 0

    def ok(self):
        with self.lk:
            if self.gap <= self.base:
                return
            self.good += 1
            if self.good >= 40:
                self.gap = max(self.base, self.gap / 1.3)
                self.good = 0


def new_session(extra=None):
    s = requests.Session()
    s.headers.update({
        "User-Agent": cc.UA,
        "Accept-Language": "en-US,en;q=0.9",
    })
    if extra:
        s.headers.update(extra)
    return s


class Transient(Exception):
    """Fetch failed for a reason that is NOT 'this posting does not exist'."""


class Gone(Exception):
    """The resource is definitively absent (404/410)."""


def fetch_text(s, url, thr=None, tries=TRIES, want=None, resp_out=None,
               timeout=TIMEOUT):
    """Return response text, or raise Gone / Transient.

    `want` is a predicate on the body.  A body that fails it is treated as a
    TRANSIENT failure, never as an empty result — these hosts answer 200 with a
    marketing shell, and silently recording that as "no jobs" is exactly the
    failure mode this pipeline keeps hitting.

    `resp_out` is a list that receives the winning response object, for callers
    that need response HEADERS (jobspresso's x-wp-totalpages) and not just the
    body.
    """
    last = ""
    for attempt in range(tries):
        if thr:
            thr.wait()
        try:
            r = s.get(url, timeout=timeout)
        except requests.RequestException as e:
            last = type(e).__name__
            time.sleep(1.5 * (attempt + 1))
            continue
        if r.status_code in (404, 410):
            raise Gone("HTTP %d" % r.status_code)
        if r.status_code in (429, 403, 503, 502, 500, 520, 522):
            last = "HTTP %d" % r.status_code
            if thr:
                thr.back_off(20 if r.status_code == 429 else 8)
            else:
                time.sleep(10 if r.status_code == 429 else 3)
            continue
        if r.status_code != 200:
            last = "HTTP %d" % r.status_code
            time.sleep(1.5 * (attempt + 1))
            continue
        body = r.text
        if want and not want(body, r):
            last = "body-failed-validator (HTTP 200, %d bytes)" % len(body)
            time.sleep(1.5 * (attempt + 1))
            continue
        if resp_out is not None:
            resp_out.append(r)
        if thr:
            thr.ok()
        return body
    raise Transient(last or "unknown")


# --------------------------------------------------------------------------
# small helpers
# --------------------------------------------------------------------------
_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")


def clean(t):
    if not t:
        return ""
    return _WS.sub(" ", html.unescape(_TAG.sub(" ", str(t)))).strip()


def num(v):
    try:
        f = float(str(v).replace(",", ""))
        return round(f, 2) if f else ""
    except (TypeError, ValueError):
        return ""


_UTM = re.compile(r"^(utm_[a-z_]+|ref|source)$", re.I)


def strip_utm(u):
    """Drop utm_* tracking from an employer apply URL.

    Only the tracking keys go: real ATS links carry load-bearing query params
    (gh_jid, jobId, ...) and stripping those would break the canonical-ATS key
    the pipeline dedupes on.
    """
    if not u or "?" not in u:
        return u or ""
    head, _, q = u.partition("?")
    q, _, frag = q.partition("#")
    keep = [p for p in q.split("&")
            if p and not _UTM.match(p.split("=", 1)[0])]
    out = head + ("?" + "&".join(keep) if keep else "")
    return out + ("#" + frag if frag else "")


def is_http(u):
    return bool(u) and str(u).lower().startswith(("http://", "https://"))


# A location string only becomes `country` when it is unambiguously ONE country.
_COUNTRY_ALIAS = {
    "usa": "United States", "us": "United States",
    "u.s.": "United States", "u.s.a.": "United States",
    "united states": "United States",
    "united states of america": "United States",
    "uk": "United Kingdom", "u.k.": "United Kingdom",
    "great britain": "United Kingdom", "england": "United Kingdom",
    "united kingdom": "United Kingdom",
    "canada": "Canada", "australia": "Australia", "germany": "Germany",
    "france": "France", "spain": "Spain", "portugal": "Portugal",
    "india": "India", "brazil": "Brazil", "mexico": "Mexico",
    "netherlands": "Netherlands", "ireland": "Ireland", "poland": "Poland",
    "japan": "Japan", "singapore": "Singapore", "philippines": "Philippines",
    "new zealand": "New Zealand", "south africa": "South Africa",
    "argentina": "Argentina", "colombia": "Colombia", "chile": "Chile",
    "italy": "Italy", "sweden": "Sweden", "norway": "Norway",
    "denmark": "Denmark", "finland": "Finland", "switzerland": "Switzerland",
    "austria": "Austria", "belgium": "Belgium", "israel": "Israel",
    "ukraine": "Ukraine", "romania": "Romania", "nigeria": "Nigeria",
    "kenya": "Kenya", "pakistan": "Pakistan", "indonesia": "Indonesia",
    "vietnam": "Vietnam", "turkey": "Turkey",
}


_ISO2 = {
    "US": "United States", "CA": "Canada", "GB": "United Kingdom",
    "UK": "United Kingdom", "AU": "Australia", "NZ": "New Zealand",
    "DE": "Germany", "FR": "France", "ES": "Spain", "PT": "Portugal",
    "NL": "Netherlands", "IE": "Ireland", "PL": "Poland", "IN": "India",
    "BR": "Brazil", "MX": "Mexico", "AR": "Argentina", "CO": "Colombia",
    "CL": "Chile", "JP": "Japan", "SG": "Singapore", "PH": "Philippines",
    "ZA": "South Africa", "IT": "Italy", "SE": "Sweden", "NO": "Norway",
    "DK": "Denmark", "FI": "Finland", "CH": "Switzerland", "AT": "Austria",
    "BE": "Belgium", "IL": "Israel", "UA": "Ukraine", "RO": "Romania",
    "NG": "Nigeria", "KE": "Kenya", "PK": "Pakistan", "ID": "Indonesia",
    "VN": "Vietnam", "TR": "Turkey", "CZ": "Czech Republic",
}


def one_country(names):
    """`country` only when the source names exactly one — never a guess."""
    vals = [clean(n) for n in (names or []) if clean(n)]
    if len(vals) != 1:
        return ""
    return _COUNTRY_ALIAS.get(vals[0].lower(), vals[0])


def loc_text(names, cap=400):
    vals = [clean(n) for n in (names or []) if clean(n)]
    if not vals:
        return "Remote"
    s = ", ".join(vals)
    if len(s) > cap:
        s = s[:cap].rsplit(",", 1)[0] + ", …"
    return "Remote (%s)" % s


_DUR = {"P1Y": "Year", "P1M": "Month", "P1W": "Week", "P1D": "Day",
        "PT1H": "Hour", "P1H": "Hour", "YEAR": "Year", "MONTH": "Month",
        "WEEK": "Week", "DAY": "Day", "HOUR": "Hour", "HOURLY": "Hour",
        "YEARLY": "Year", "MONTHLY": "Month", "ANNUAL": "Year",
        "MO": "Month", "YR": "Year", "HR": "Hour", "WK": "Week"}


def period(v):
    return _DUR.get(str(v or "").upper(), "")


def ld_blocks(body):
    """Every application/ld+json object on a page, flattened through @graph."""
    out = []
    for raw in re.findall(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)'
            r'</script>', body, re.S | re.I):
        try:
            d = json.loads(raw.strip())
        except ValueError:
            continue
        for item in (d if isinstance(d, list) else [d]):
            if not isinstance(item, dict):
                continue
            if isinstance(item.get("@graph"), list):
                out += [g for g in item["@graph"] if isinstance(g, dict)]
            else:
                out.append(item)
    return out


def job_posting(body):
    for d in ld_blocks(body):
        t = d.get("@type")
        t = t if isinstance(t, str) else (t[0] if isinstance(t, list) and t
                                          else "")
        if str(t).lower() == "jobposting":
            return d
    return None


def alr_names(v):
    """applicantLocationRequirements -> list of names (dict or list of dicts)."""
    if isinstance(v, dict):
        v = [v]
    out = []
    for x in (v or []):
        if isinstance(x, dict) and x.get("name"):
            out.append(x["name"])
        elif isinstance(x, str):
            out.append(x)
    return out


_MON3 = {m: i for i, m in enumerate(
    ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct",
     "Nov", "Dec"], 1)}


def stamp_year(mon, day):
    """'Jul 7' -> YYYY-MM-DD.

    hubstafftalent and justremote both print a month/day with NO year.
    Stamping the current year blindly breaks every January — a '28 Dec'
    posting read on 2 Jan would be dated nine days in the FUTURE — so a date
    that lands more than a week ahead of today is rolled back a year.
    """
    if not mon:
        return ""
    try:
        d = datetime(TODAY.year, mon, day).date()
    except ValueError:
        return ""
    if d > TODAY + timedelta(days=7):
        try:
            d = d.replace(year=TODAY.year - 1)
        except ValueError:
            return ""
    return d.strftime("%Y-%m-%d")


def expired(vt):
    """True only when validThrough parses AND is in the past."""
    if not vt:
        return False
    d = cc.ymd(iso=vt)
    if not d:
        return False
    try:
        return datetime.strptime(d, "%Y-%m-%d").date() < TODAY
    except ValueError:
        return False


# --------------------------------------------------------------------------
# state
# --------------------------------------------------------------------------
def load_state():
    if os.path.exists(STATE_JSON):
        try:
            with open(STATE_JSON, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            pass
    return {}


def save_state(st):
    tmp = STATE_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(st, f)
    os.replace(tmp, STATE_JSON)


# ==========================================================================
# 1. jobspresso — WP REST, employer apply URL in meta._application
# ==========================================================================
# The REST route is SLOW: measured 2026-07-29, a page of 100 takes 48-72 s of
# server time, consistently, cached or not, with or without _fields (the meta
# join is what costs).  Serially that is 28 x ~50 s = ~24 minutes for a 2.8k
# board, which is why the pages are walked by a small thread pool (3 in flight)
# instead of one at a time, and why every finished page is recorded in the
# state file so an interrupted run resumes at the page it stopped on rather
# than re-downloading 25 MB.  Timeout is 150 s here, not the module default of
# 30 — a 30 s ceiling times out on EVERY page and the source reports zero.
JP_ROOT = "https://jobspresso.co"
JP_REST = JP_ROOT + "/wp-json/wp/v2/job-listings?per_page=100&page=%d"
JP_TIMEOUT = 150
JP_WORKERS = 3


def jp_terms(s, route):
    """id -> name for a WP taxonomy."""
    try:
        body = fetch_text(s, "%s/wp-json/wp/v2/%s?per_page=100" % (JP_ROOT,
                                                                   route),
                          want=lambda b, r: b.lstrip().startswith("["))
        return {t["id"]: clean(t.get("name"))
                for t in json.loads(body) if isinstance(t, dict)}
    except (Transient, Gone, ValueError):
        return {}


def jp_page(s, page, thr=None):
    """One REST page -> (list of records, x-wp-totalpages or None)."""
    rr = []
    body = fetch_text(s, JP_REST % page, thr=thr, tries=3, resp_out=rr,
                      timeout=JP_TIMEOUT,
                      want=lambda b, r: b.lstrip().startswith(("[", "{")))
    rows = json.loads(body)
    if isinstance(rows, dict):
        # {"code":"rest_post_invalid_page_number"} == past the last page.
        return [], None
    tp = None
    if rr:
        try:
            tp = int(rr[0].headers.get("x-wp-totalpages") or 0) or None
        except ValueError:
            tp = None
    return rows, tp


def jp_rows(rows, emp_terms, fn_terms):
    out = []
    filled = 0
    for j in rows:
        jid = str(j.get("id") or "")
        if not jid:
            continue
        meta = j.get("meta") or {}
        if str(meta.get("_filled") or "0") == "1":
            filled += 1
            continue
        title = clean((j.get("title") or {}).get("rendered"))
        emp_raw = " ".join(emp_terms.get(t, "")
                           for t in (j.get("job-categories") or []))
        fn_raw = " ".join(fn_terms.get(t, "")
                          for t in (j.get("job-types") or []))
        app = strip_utm(clean(meta.get("_application")))
        # _application is sometimes an email address (WP Job Manager allows
        # either); an email is useless as a canonical-ATS dedupe key, so those
        # rows fall back to the jobspresso posting.
        apply_url = app if is_http(app) else (j.get("link") or "")
        locs = clean(meta.get("_job_location"))
        out.append(cc.row(
            source="jobspresso", source_type="remote_board",
            company=clean(meta.get("_company_name")),
            title=title,
            employment_type=cc.classify_any(emp_raw, title),
            # jobspresso is a curated REMOTE-ONLY board: every listing is
            # remote.  meta._remote_position is a legacy per-post flag that
            # only ~9% of live rows set (measured 2026-07-28), so trusting it
            # would mark 91% of a remote board as non-remote.
            work_mode="Remote", remote=True,
            location=locs or "Remote",
            country=one_country([locs]) if locs and "," not in locs else "",
            salary_currency=clean(meta.get("_job_salary_currency")).upper(),
            salary_period=period(meta.get("_job_salary_unit")),
            posted=cc.ymd(iso=(j.get("date_gmt") or "") + "Z"
                          if j.get("date_gmt") else j.get("date")),
            apply_url=apply_url, job_id=jid,
            board="jobspresso" + (("/" + fn_raw.lower()) if fn_raw else ""),
            board_url=JP_ROOT + "/jobs/"))
    return out, filled


def do_jobspresso(out, st):
    entry = st.setdefault("jobspresso", {})
    done = set(entry.get("done") or [])          # page numbers already written
    s = new_session({"Accept": "application/json"})
    # THE SWAP: on jobspresso the taxonomy called `job-categories` holds the
    # EMPLOYMENT TYPE (Full Time / Part Time / Contract / Freelance /
    # Internship) and the one called `job-types` holds the FUNCTION (Design /
    # Engineer / Marketing / ...).  Reading them the way their names suggest
    # would classify every posting as "Full-time" and lose the contract slice.
    emp_terms = jp_terms(s, "job-categories")
    fn_terms = jp_terms(s, "job-types")
    if not emp_terms:
        print("    ! jobspresso: employment taxonomy unavailable — "
              "types fall back to title only", flush=True)

    n = filled = 0
    seen_ids = set()
    # Page 1 first, purely to learn x-wp-totalpages.
    try:
        rows, total_pages = jp_page(s, 1)
    except (Transient, Gone) as e:
        s.close()
        raise RuntimeError("jobspresso page 1: %s" % e)
    print("    jobspresso pages=%s" % total_pages, flush=True)
    if 1 not in done:
        got, f = jp_rows(rows, emp_terms, fn_terms)
        for r in got:
            seen_ids.add(r["job_id"])
            out.write(r)
        n += len(got)
        filled += f
        done.add(1)
    todo = [p for p in range(2, (total_pages or 1) + 1) if p not in done]
    if LIMIT:
        todo = todo[:LIMIT]
    thr = Throttle(1.0)          # >=1 s between page requests, 3 in flight
    tls = threading.local()

    def worker(p):
        if not getattr(tls, "s", None):
            tls.s = new_session({"Accept": "application/json"})
        try:
            rows, _ = jp_page(tls.s, p, thr=thr)
        except (Transient, Gone) as e:
            return p, None, str(e)
        return p, rows, ""

    bad = []
    with cf.ThreadPoolExecutor(max_workers=JP_WORKERS) as ex:
        for p, rows, err in ex.map(worker, todo):
            if rows is None:
                # transient — page NOT marked done, so a re-run retries it
                bad.append(p)
                print("    ! jobspresso page %d: %s" % (p, err), flush=True)
                continue
            got, f = jp_rows(rows, emp_terms, fn_terms)
            for r in got:
                if r["job_id"] in seen_ids:
                    continue                  # paginator repeat guard
                seen_ids.add(r["job_id"])
                out.write(r)
                n += 1
            filled += f
            done.add(p)
            entry["done"] = sorted(done)
            out.f.flush()
            save_state(st)
    s.close()
    if bad:
        raise RuntimeError("jobspresso: %d/%d pages failed (%s) — %d rows kept,"
                           " re-run resumes at the failed pages"
                           % (len(bad), len(todo), bad[:8], n))
    print("  jobspresso: %d rows over %d pages (%d filled skipped, %d distinct "
          "ids)" % (n, len(done), filled, len(seen_ids)), flush=True)
    return n


# ==========================================================================
# 2. hubstafftalent — XHR HTML fragment
# ==========================================================================
HT_ROOT = "https://hubstafftalent.net"
HT_URL = HT_ROOT + "/search/jobs?page=%d"
HT_HDRS = {
    # WITHOUT these two the host answers 200 with a 76 KB shell whose results
    # div is EMPTY.  They are the entire difference between 0 jobs and 710.
    "X-Requested-With": "XMLHttpRequest",
    "Accept": "text/javascript, application/javascript, */*; q=0.01",
    "Referer": HT_ROOT + "/search/jobs",
}
_HT_ENV = re.compile(r"\$\('#results'\)\.html\(\"(.*)\"\)\s*;?\s*$", re.S)


def js_unescape(s):
    """Un-escape a Rails escape_javascript string.

    json.loads would be neater but the payload contains \\$ (escaped dollar),
    which is not a legal JSON escape and makes json.loads throw on any page
    carrying a pay rate.
    """
    out = []
    i = 0
    simple = {"n": "\n", "t": "\t", "r": "\r", "b": "\b", "f": "\f"}
    while i < len(s):
        c = s[i]
        if c == "\\" and i + 1 < len(s):
            nx = s[i + 1]
            if nx == "u" and i + 5 < len(s):
                try:
                    out.append(chr(int(s[i + 2:i + 6], 16)))
                    i += 6
                    continue
                except ValueError:
                    pass
            out.append(simple.get(nx, nx))
            i += 2
            continue
        out.append(c)
        i += 1
    return "".join(out)


_HT_HREF = re.compile(r'<a class="name[^"]*"[^>]*href="(/jobs/[^"]+)"[^>]*>'
                      r'(.*?)</a>', re.S)
_HT_LABEL = re.compile(r'<span class="label label-([a-z_]+)[^"]*"[^>]*>'
                       r'([^<]*)</span>')
_HT_PAY = re.compile(r'<div class="pay-rate">([^<]*)</div>')
_HT_CO = re.compile(r'job-agency[^>]*>(?:<i[^>]*>\s*</i>)?\s*([^<]+)</a>')
_HT_HQ = re.compile(r'<strong>HQ:</strong>\s*([^<]+?)\s*</span>')
_HT_WHEN = re.compile(r'data-original-title="([^"]+)"')
_HT_CAL = re.compile(r'hi-calendar[^>]*>[^<]*</i>(.*?)</span>\s*</div>', re.S)
_HT_REL = re.compile(r'(\d+)\s*(min|minute|hr|hour|day|week|month|year)s?\s+'
                     r'ago', re.I)
_HT_MD = re.compile(r'^([A-Za-z]{3})[a-z]*\s+(\d{1,2})(?:,\s*(\d{4}))?$')
_HT_TOTAL = re.compile(r'of <b>(\d[\d,]*)</b> results')
_HT_MONEY = re.compile(r'([$£€])\s*([\d,]+(?:\.\d+)?)'
                       r'(?:\s*-\s*[$£€]?\s*([\d,]+(?:\.\d+)?))?'
                       r'\s*(/\s*(?:hr|hour|mo|month|yr|year))?', re.I)
_CUR = {"$": "USD", "£": "GBP", "€": "EUR"}
# Hubstaff Talent is a freelancer marketplace: its `hourly` listings are
# time-tracked contractor engagements, not salaried roles.  Passing the bare
# token "hourly" into classify_any would fall through to "Full-time", so the
# label is expanded to the words the classifier actually understands.
_HT_TYPE = {"full_time": "full time", "part_time": "part time",
            "hourly": "hourly contractor", "fixed": "contract",
            "fixed_price": "contract", "contract": "contract",
            "freelance": "freelance", "internship": "internship"}


def ht_posted(block):
    """Posted date out of the calendar element of a search-result card.

    THREE renderings, and only the first has a year in it:
      < ~24 h   <span class="a-tooltip" data-original-title="July 29, 2026 at
                12:38 am">3 hrs ago</span>
      older     a bare "Jul  7" — month and day, NO year, NO tooltip
      (defensive) "2 days ago" with no tooltip
    Reading only the tooltip — which is what an obvious implementation does —
    dates 10 rows out of 708 and leaves 698 undated, and undated rows never
    age out of all_jobs_build.py's cutoff.
    """
    cal = _HT_CAL.search(block)
    seg = cal.group(1) if cal else ""
    t = _HT_WHEN.search(seg)
    if t:
        txt = t.group(1).split(" at ")[0].strip()
        for fmt in ("%B %d, %Y", "%b %d, %Y"):
            try:
                return datetime.strptime(txt, fmt).strftime("%Y-%m-%d")
            except ValueError:
                continue
    txt = clean(seg)
    rel = _HT_REL.search(txt)
    if rel:
        step = {"min": 0, "minute": 0, "hr": 0, "hour": 0, "day": 1,
                "week": 7, "month": 30, "year": 365}[rel.group(2).lower()]
        return (TODAY - timedelta(days=int(rel.group(1)) * step)).strftime(
            "%Y-%m-%d")
    md = _HT_MD.match(txt)
    if md:
        if md.group(3):
            try:
                return datetime(int(md.group(3)), _MON3[md.group(1).title()],
                                int(md.group(2))).strftime("%Y-%m-%d")
            except (ValueError, KeyError):
                return ""
        return stamp_year(_MON3.get(md.group(1).title()), int(md.group(2)))
    return ""


def do_hubstaff(out, st):
    s = new_session(HT_HDRS)
    n = 0
    seen = set()
    total = None
    page = 1
    empty = 0
    while page <= 80:
        try:
            body = fetch_text(s, HT_URL % page,
                              # the discriminator: a real answer is the XHR JS
                              # wrapper, the shell is a <!DOCTYPE html> page.
                              want=lambda b, r: b.lstrip().startswith("$("))
        except Gone:
            break
        except Transient as e:
            raise RuntimeError("hubstafftalent page %d: %s" % (page, e))
        m = _HT_ENV.search(body.strip())
        if not m:
            raise RuntimeError("hubstafftalent page %d: unrecognised envelope"
                               % page)
        frag = js_unescape(m.group(1))
        if total is None:
            t = _HT_TOTAL.search(frag)
            total = int(t.group(1).replace(",", "")) if t else 0
            print("    hubstafftalent total=%s" % (total or "?"), flush=True)
        blocks = frag.split('<div class="search-result">')[1:]
        if not blocks:
            empty += 1
            if empty >= 2:
                break
            time.sleep(6)
            continue
        empty = 0
        new = 0
        for b in blocks:
            hm = _HT_HREF.search(b)
            if not hm:
                continue
            href, title = hm.group(1), clean(hm.group(2))
            if href in seen:
                continue
            seen.add(href)
            new += 1
            lab = _HT_LABEL.search(b)
            raw_type = _HT_TYPE.get(lab.group(1) if lab else "",
                                    clean(lab.group(2)) if lab else "")
            hq = clean(_HT_HQ.search(b).group(1)) if _HT_HQ.search(b) else ""
            is_remote = 'title="Remote job"' in b
            smin = smax = cur = per = ""
            pay = _HT_PAY.search(b)
            if pay and pay.group(1).strip():
                mm = _HT_MONEY.search(pay.group(1))
                if mm:
                    cur = _CUR.get(mm.group(1), "")
                    smin = num(mm.group(2))
                    smax = num(mm.group(3)) if mm.group(3) else ""
                    per = period((mm.group(4) or "").strip("/ ").strip()
                                 .replace("hr", "hour"))
            out.write(cc.row(
                source="hubstafftalent", source_type="remote_board",
                company=clean(_HT_CO.search(b).group(1))
                if _HT_CO.search(b) else "",
                title=title,
                employment_type=cc.classify_any(raw_type, title),
                work_mode="Remote" if is_remote else "",
                remote=is_remote,
                # The card carries the COMPANY HQ, not a candidate-location
                # restriction, so it is labelled as such and city/state/country
                # are left empty rather than asserting the job sits at the HQ.
                location=("Remote (HQ: %s)" % hq) if (is_remote and hq)
                else (hq or ("Remote" if is_remote else "")),
                salary_min=smin, salary_max=smax, salary_currency=cur,
                salary_period=per,
                posted=ht_posted(b),
                apply_url=HT_ROOT + href,
                job_id=href.rsplit("/", 1)[-1],
                board="hubstafftalent", board_url=HT_ROOT + "/search/jobs"))
            n += 1
        if new == 0:                       # paginator stopped advancing
            break
        if total and n >= total:
            break
        page += 1
        time.sleep(1.1)
    s.close()
    print("  hubstafftalent: %d rows over %d pages (site total %s)"
          % (n, page, total), flush=True)
    return n


# ==========================================================================
# shared: sitemap -> per-job-page JSON-LD crawl (dynamitejobs, remoteio)
# ==========================================================================
def sitemap_locs(s, url):
    body = fetch_text(
        s, url,
        # /sitemap_index.xml and friends answer 200 with an SPA shell on both
        # hosts — require real XML, not a status code.
        want=lambda b, r: "<urlset" in b[:2000] or "<sitemapindex" in b[:2000])
    return re.findall(r"<loc>\s*([^<\s]+)\s*</loc>", body)


def crawl_pages(name, urls, parse, out, st, rps=4.0, workers=4):
    """Fetch each job page, parse it, write rows; resumable via state."""
    entry = st.setdefault(name, {})
    done = set(entry.get("done") or [])
    todo = [u for u in urls if u not in done]
    if LIMIT:
        todo = todo[:LIMIT]
    print("    %s: %d job pages (%d already done)"
          % (name, len(todo), len(done)), flush=True)
    thr = Throttle(rps)
    counts = {"rows": 0, "gone": 0, "expired": 0, "nold": 0, "fail": 0}
    sess = threading.local()

    def worker(u):
        if not getattr(sess, "s", None):
            sess.s = new_session({"Accept": "text/html,application/xhtml+xml"})
        try:
            body = fetch_text(sess.s, u, thr=thr,
                              want=lambda b, r: len(b) > 500)
        except Gone:
            return u, "gone", None
        except Transient:
            return u, "fail", None
        ld = job_posting(body)
        if ld is None:
            # 200 with no JobPosting == pulled listing / shell, NOT a fetch
            # failure.  Recorded as done so a resume does not re-fetch it.
            return u, "nold", None
        if expired(ld.get("validThrough")):
            return u, "expired", None
        try:
            return u, "ok", parse(u, ld)
        except Exception as e:                       # noqa: BLE001
            print("    ! %s parse %s: %s" % (name, u, e), flush=True)
            return u, "fail", None

    with cf.ThreadPoolExecutor(max_workers=workers) as ex:
        for i, (u, kind, row) in enumerate(ex.map(worker, todo), 1):
            if kind == "fail":
                counts["fail"] += 1          # NOT recorded done -> retried
            else:
                done.add(u)
                counts[kind if kind != "ok" else "rows"] += 1
                if row:
                    with _lock:
                        out.write(row)
            if i % 250 == 0:
                entry["done"] = sorted(done)
                save_state(st)
                print("      %s %d/%d rows=%d fail=%d"
                      % (name, i, len(todo), counts["rows"], counts["fail"]),
                      flush=True)
    entry["done"] = sorted(done)
    save_state(st)
    print("  %s: %d rows (%d expired, %d no-JSON-LD, %d gone, %d transient "
          "failures left to retry)"
          % (name, counts["rows"], counts["expired"], counts["nold"],
             counts["gone"], counts["fail"]), flush=True)
    return counts["rows"], counts["fail"]


# ==========================================================================
# 3. dynamitejobs
# ==========================================================================
DJ_ROOT = "https://dynamitejobs.com"
DJ_MAPS = [DJ_ROOT + "/sitemap/jobs-of-company.xml",
           DJ_ROOT + "/sitemap/jobs-of-category.xml"]


def dj_parse(url, ld):
    title = clean(ld.get("title"))
    title = re.sub(r",?\s*Remote Job$", "", title, flags=re.I)
    org = ld.get("hiringOrganization") or {}
    names = alr_names(ld.get("applicantLocationRequirements"))
    sal = ld.get("estimatedSalary")
    sal = sal[0] if isinstance(sal, list) and sal else (
        sal if isinstance(sal, dict) else {})
    val = sal.get("value") if isinstance(sal.get("value"), dict) else sal
    tele = str(ld.get("jobLocationType") or "").upper() == "TELECOMMUTE"
    slug = url.rstrip("/").split("/remote-job/")[-1]
    comp_slug = ((ld.get("identifier") or {}).get("value")
                 or url.split("/company/")[-1].split("/")[0])
    return cc.row(
        source="dynamitejobs", source_type="remote_board",
        company=clean(org.get("name")), title=title,
        employment_type=cc.classify_any(
            str(ld.get("employmentType") or "").replace("_", " "), title),
        work_mode="Remote" if tele else "", remote=tele,
        location=loc_text(names), country=one_country(names),
        salary_min=num(val.get("minValue")), salary_max=num(val.get("maxValue")),
        salary_currency=clean(val.get("currency") or sal.get("currency")).upper(),
        salary_period=period(val.get("duration") or sal.get("duration")),
        posted=cc.ymd(iso=ld.get("datePosted")),
        # directApply:false but the outbound link is only reachable through a
        # POST that logs an apply-click, so the board page is the apply URL.
        apply_url=clean(ld.get("url")) or url,
        job_id="%s/%s" % (comp_slug, slug),
        board="dynamitejobs", board_url=DJ_ROOT + "/remote-jobs")


def do_dynamite(out, st):
    s = new_session({"Accept": "application/xml,text/xml,*/*"})
    locs = set()
    for m in DJ_MAPS:
        try:
            locs |= {u for u in sitemap_locs(s, m) if "/remote-job/" in u}
        except (Transient, Gone) as e:
            print("    ! dynamitejobs sitemap %s: %s" % (m, e), flush=True)
    s.close()
    if not locs:
        raise RuntimeError("dynamitejobs: no job URLs in either sitemap")
    n, fail = crawl_pages("dynamitejobs", sorted(locs), dj_parse, out, st,
                          rps=3.5, workers=4)
    return n


# ==========================================================================
# 4. remoteio
# ==========================================================================
RIO_ROOT = "https://remote.io"


def rio_parse(url, ld):
    org = ld.get("hiringOrganization") or {}
    names = alr_names(ld.get("applicantLocationRequirements"))
    tele = str(ld.get("jobLocationType") or "").upper() == "TELECOMMUTE"
    jl = ld.get("jobLocation")
    jl = jl[0] if isinstance(jl, list) and jl else (jl if isinstance(jl, dict)
                                                    else {})
    addr = (jl.get("address") or {}) if isinstance(jl, dict) else {}
    # addressCountry is an ISO-3166 alpha-2 code here ("CA", "US"); expand the
    # ones that matter so `country` is comparable with the rest of the corpus,
    # and leave an unknown code alone rather than inventing a name.
    ac = clean(addr.get("addressCountry"))
    country = one_country(names) or _ISO2.get(ac.upper(), ac)
    tail = url.rstrip("/").rsplit("/", 1)[-1]
    jid = tail.rsplit("-", 1)[-1]
    return cc.row(
        source="remoteio", source_type="remote_board",
        company=clean(org.get("name")), title=clean(ld.get("title")),
        employment_type=cc.classify_any(
            str(ld.get("employmentType") or "").replace("_", " "),
            clean(ld.get("title"))),
        work_mode="Remote" if tele else "", remote=tele,
        location=loc_text(names) if names else "Remote",
        city=clean(addr.get("addressLocality")),
        state=clean(addr.get("addressRegion")),
        country=country,
        posted=cc.ymd(iso=ld.get("datePosted")),
        apply_url=url,                      # directApply:true — no employer URL
        job_id=jid if jid.isdigit() else tail,
        board="remoteio", board_url=RIO_ROOT + "/remote-jobs")


def do_remoteio(out, st):
    s = new_session({"Accept": "application/xml,text/xml,*/*"})
    maps = [u for u in sitemap_locs(s, RIO_ROOT + "/sitemap.xml")
            if re.search(r"sitemap-jobs-\d+\.xml$", u)]
    locs = set()
    for m in sorted(maps):
        try:
            got = [u for u in sitemap_locs(s, m) if "/remote-jobs/" in u]
        except (Transient, Gone) as e:
            print("    ! remoteio sitemap %s: %s" % (m, e), flush=True)
            continue
        print("    %s -> %d" % (m.rsplit("/", 1)[-1], len(got)), flush=True)
        locs |= set(got)
    s.close()
    if not locs:
        raise RuntimeError("remoteio: job sitemaps carried no job URLs")
    n, fail = crawl_pages("remoteio", sorted(locs), rio_parse, out, st,
                          rps=5.0, workers=5)
    return n


# ==========================================================================
# 5. justremote
# ==========================================================================
JR_API = "https://justremote-api.herokuapp.com/api/v1/jobs"
JR_SITE = "https://justremote.co/"


def jr_date(txt):
    """'28 Jul' -> YYYY-MM-DD; the API omits the year (see stamp_year)."""
    m = re.match(r"(\d{1,2})\s+([A-Za-z]{3})", (txt or "").strip())
    if not m:
        return ""
    return stamp_year(_MON3.get(m.group(2).title()), int(m.group(1)))


def do_justremote(out, st):
    s = new_session({"Accept": "application/json",
                     "Origin": "https://justremote.co",
                     "Referer": "https://justremote.co/"})
    body = fetch_text(s, JR_API,
                      want=lambda b, r: b.lstrip().startswith("["))
    s.close()
    jobs = json.loads(body)
    if not isinstance(jobs, list):
        raise RuntimeError("justremote: expected a list, got %s" % type(jobs))
    n = 0
    for j in jobs:
        if not isinstance(j, dict) or j.get("is_active") is False:
            continue
        title = clean(j.get("title"))
        rt = clean(j.get("remote_type"))
        remote = "remote" in rt.lower()
        names = j.get("location_restrictions") or (
            [j["job_country"]] if j.get("job_country") else [])
        href = (j.get("href") or "").lstrip("/")
        out.write(cc.row(
            source="justremote", source_type="remote_board",
            company=clean(j.get("company_name")), title=title,
            employment_type=cc.classify_any(j.get("job_type"), title),
            work_mode=cc.work_mode(remote=remote or None, text=rt),
            remote=remote,
            location="%s%s" % (rt or "Remote",
                               (" (%s)" % ", ".join(clean(x) for x in names))
                               if names else ""),
            country=one_country(names),
            posted=jr_date(j.get("date")),
            apply_url=JR_SITE + href if href else "",
            job_id=j.get("id"), board="justremote",
            board_url="https://justremote.co/remote-jobs"))
        n += 1
    print("  justremote: %d rows (of %d returned; API exposes no pagination)"
          % (n, len(jobs)), flush=True)
    return n


# ==========================================================================
SOURCES = {
    "jobspresso": do_jobspresso,
    "hubstafftalent": do_hubstaff,
    "dynamitejobs": do_dynamite,
    "remoteio": do_remoteio,
    "justremote": do_justremote,
}


def main():
    only = [x.strip() for x in os.environ.get("SMALLREMOTE_ONLY", "").split(",")
            if x.strip()]
    for x in only:
        if x not in SOURCES:
            raise SystemExit("unknown source %r (choose from %s)"
                             % (x, ", ".join(SOURCES)))
    st = load_state()
    out = cc.Writer(OUT)
    bad = []
    t0 = time.time()
    for name, fn in SOURCES.items():
        if only and name not in only:
            continue
        ent = st.setdefault(name, {})
        if ent.get("status") == "done" and not FORCE:
            print("  %s: already done (%s rows on %s) — SMALLREMOTE_FORCE=1 to "
                  "re-run" % (name, ent.get("rows"), ent.get("at")), flush=True)
            continue
        t = time.time()
        try:
            n = fn(out, st)
        except Exception as e:                        # noqa: BLE001
            # A source that FAILS is left un-done so the next run retries it.
            # It is never silently recorded as "0 jobs".
            print("  %s: ERROR %s: %s" % (name, type(e).__name__, e),
                  flush=True)
            ent["status"] = "failed"
            ent["error"] = str(e)[:200]
            bad.append(name)
        else:
            ent["status"] = "done"
            ent["rows"] = n
            ent["at"] = TODAY.strftime("%Y-%m-%d")
            ent["secs"] = round(time.time() - t, 1)
            ent.pop("error", None)
            ent.pop("done", None)             # crawl finished; drop the URL set
        save_state(st)
    out.close()
    print("DONE smallremote_fetch -> %s (%.1fs)%s"
          % (OUT, time.time() - t0,
             "; FAILED: " + ", ".join(bad) if bad else ""), flush=True)
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main())
