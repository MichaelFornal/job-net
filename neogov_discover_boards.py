#!/usr/bin/env python3
"""
neogov_discover_boards.py — enumerate NEOGOV agency boards.

THE ANSWER TO "CAN AGENCIES BE ENUMERATED FROM THE SITE ITSELF?" IS NO.
Measured 2026-08-20, every first-party route was tried and none lists them:

  /careers                            302  (no directory page exists)
  /careers/home/agencies              302
  /employers                          302
  /sitemap.xml                        200, 43,182 <loc> entries — but they are
                                      42,993 /jobs/{id}-0/{slug} and 189
                                      /category/... URLs.  ZERO /careers/
                                      entries, so the sitemap enumerates
                                      postings and never agencies.
  /jobs?keyword=...                   200, but the global search paginates 10
                                      at a time and links jobs by the
                                      agency-LESS /jobs/{id} form.  The job
                                      detail page names the employer in prose
                                      ("Employer: State of Washington") and
                                      renders agency-folder-name="" — empty —
                                      so it cannot be mapped back to a slug.

So the slug list has to come from outside.  Two sources, both used:

  1. WAYBACK CDX over governmentjobs.com/careers/* and schooljobs.com/careers/*
     — the pattern this repo already uses for lever and greenhouse.  Measured:
     314,223 + 30,030 archived URLs -> 2,542 + 460 distinct cleaned slugs.
  2. all_jobs_master.csv — the syndicated floor.  Measured: 156 governmentjobs
     slugs across 2,879 rows, plus 12 schooljobs slugs across 354 rows.
     Everything the corpus knows is a strict subset of what CDX finds, which
     is exactly why CDX is worth the trouble: it is ~16x the corpus floor.

SLUG CLEANING.  CDX hands back the same agency under several encodings:
"%7b0%7dcolorado" (a literal {0} format placeholder that leaked into some
archived link), "%20cumberlandcountync" (leading space), "pabureau\\" (a
trailing backslash), plus mixed case.  Double-unquote, strip the {0} prefix,
strip braces/whitespace/backslashes, lowercase, then keep only
^[a-z0-9][a-z0-9._-]{1,60}$ and drop the app's own route words (home, jobs,
user, inbox, applications, ...) which appear as /careers/{word} in the archive
but are not agencies.

THE CHEAP LIVENESS PROBE.  Probing with the real jobs route would mean pulling
1.4 MB per live board just to learn it exists.  Instead:

    GET https://{host}/careers/home/index?agency={slug}&page=99999
        X-Requested-With: XMLHttpRequest

is the site's own paged list partial asked for a page past the end.  ~1 KB for
a live agency, and it still carries the proof and the name:

    live   -> 200, body contains 'powered-by-neogov' / 'jobs-not-found' /
              'search-results-listing', and a
              document.title = '<board title> | Sorted by ... | <agency name>'
    dead   -> 302  Location: https://www.governmentjobs.com
    moved  -> 301  Location: https://www.schooljobs.com/careers/{slug}
    no XHR -> 302  Location: /Error/NotFound   even for a live agency

That is a 200x cheaper probe than the map route and it is validated on the
PAYLOAD, not the status (rule 1): a 200 whose body carries none of the three
markers is recorded as UNKNOWN and re-probed next run, never as dead.
429/5xx/403/timeouts are likewise left unprobed (rule 3).  Throttle decays
(rule 2) via the ats_fetch.py slow_down()/speed_up() pair.

OUTPUT
  neogov_candidates.csv   host,agency,origin,status,name,jobs_hint,checked
                          the full census: live / dead / migrated / unknown /
                          pending.  "migrated" is spelled to match
                          coverage_report.DEAD_MARKERS.
                          Rewritten atomically after every batch, so the file
                          IS the resume state — re-running skips everything
                          already settled.
  tenants/neogov.csv      host,agency,company  — the live subset, the input
                          neogov_fetch.py reads.

Env knobs
    NEOGOV_CDX=0            skip the Wayback pass (use the existing csv)
    NEOGOV_MINE_MASTER=0    skip the all_jobs_master.csv pass
    NEOGOV_PROBE=0          only rebuild the candidate list, do not probe
    NEOGOV_DISCOVER_LIMIT   probe at most N candidates (smoke tests)
    NEOGOV_DISCOVER_BUDGET_MIN   wall-clock budget for the probe pass
    NEOGOV_RPS / NEOGOV_WORKERS  same meaning as in neogov_fetch.py
"""
import concurrent.futures as cf
import csv
import os
import re
import sys
import threading
import time
import urllib.parse
from datetime import datetime, timezone

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
CANDIDATES = os.path.join(BASE, "neogov_candidates.csv")
TENANTS = os.path.join(BASE, "tenants", "neogov.csv")
MASTER = os.path.join(BASE, "all_jobs_master.csv")

HOSTS = ("www.governmentjobs.com", "www.schooljobs.com")
CDX_HOSTS = {"www.governmentjobs.com": "governmentjobs.com",
             "www.schooljobs.com": "schooljobs.com"}
PROBE_URL = "https://%s/careers/home/index?agency=%s&page=99999"
XHR = {"X-Requested-With": "XMLHttpRequest",
       "Accept": "text/html, */*; q=0.01",
       "Accept-Language": "en-US,en;q=0.9"}

RPS = float(os.environ.get("NEOGOV_RPS") or 2.0)
WORKERS = int(os.environ.get("NEOGOV_WORKERS") or 6)
TIMEOUT = 45
TRIES = 3
DO_CDX = os.environ.get("NEOGOV_CDX", "1") != "0"
DO_MINE = os.environ.get("NEOGOV_MINE_MASTER", "1") != "0"
DO_PROBE = os.environ.get("NEOGOV_PROBE", "1") != "0"
LIMIT = int(os.environ.get("NEOGOV_DISCOVER_LIMIT") or 0)
BUDGET_MIN = float(os.environ.get("NEOGOV_DISCOVER_BUDGET_MIN") or 0)

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

FIELDS = ["host", "agency", "origin", "status", "name", "jobs_hint", "checked"]

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = {h: 0.0 for h in HOSTS}
_interval = {h: 1.0 / RPS for h in HOSTS}
_pause_until = {h: 0.0 for h in HOSTS}
_ok_streak = {h: 0 for h in HOSTS}
_n429 = {h: 0 for h in HOSTS}
_stats = {"live": 0, "dead": 0, "migrated": 0, "unknown": 0, "transient": 0}


class Transient(Exception):
    """Retryable — the candidate stays PENDING, never recorded as dead."""


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
    with _rate_lock:
        n = _ok_streak[host] + 1
        if n < 40:
            _ok_streak[host] = n
            return
        _ok_streak[host] = 0
        floor = 1.0 / RPS
        if _interval[host] > floor:
            _interval[host] = max(_interval[host] * 0.9, floor)


# ---- slug cleaning ---------------------------------------------------------
RESERVED = {
    "home", "jobs", "job", "user", "inbox", "applications", "application",
    "login", "logout", "account", "error", "content", "scripts", "bundles",
    "vendor", "images", "css", "careers", "termsofuse", "privacypolicy",
    "helpandsupport", "accessibilitystatement", "profilesettings",
    "coloredspinner", "agencycustomstylescheme", "signin", "register",
    "404", "500", "null", "undefined",
}
SLUG_OK = re.compile(r"^[a-z0-9][a-z0-9._-]{1,60}$")


def clean_slug(s):
    s = urllib.parse.unquote(urllib.parse.unquote(str(s or "")))
    s = s.strip().lower()
    s = re.sub(r"^\{0\}", "", s)
    s = s.strip("{}\\/ \t\r\n")
    if not s or s in RESERVED or not SLUG_OK.match(s):
        return ""
    return s


# ---- source 1: wayback cdx -------------------------------------------------
def cdx_slugs(host):
    """Every /careers/{slug} ever archived for this host.

    Paged with showResumeKey because a single query truncates: the flat
    limit=40000 form comes back sorted by urlkey and never leaves the letter
    'a'.  A 504 from the CDX front end is retried, not fatal."""
    domain = CDX_HOSTS[host]
    out = set()
    key = ""
    s = requests.Session()
    s.headers.update({"User-Agent": cc.UA})
    for batch in range(200):
        url = ("http://web.archive.org/cdx/search/cdx?url=%s/careers/*"
               "&output=text&fl=original&collapse=urlkey"
               "&filter=statuscode:200&limit=50000&showResumeKey=true" % domain)
        if key:
            url += "&resumeKey=" + urllib.parse.quote(key)
        try:
            r = s.get(url, timeout=300)
            r.raise_for_status()
            body = r.text
        except requests.RequestException as e:
            print("  cdx %s batch %d: %s (retrying)" % (domain, batch, e),
                  flush=True)
            time.sleep(10)
            continue
        lines = [ln for ln in body.split("\n") if ln.strip()]
        nk = ""
        if lines and "://" not in lines[-1]:
            nk = lines[-1].strip()
            lines = lines[:-1]
        for ln in lines:
            m = re.search(r"/careers/([^/?#\s]+)", ln)
            if m:
                c = clean_slug(m.group(1))
                if c:
                    out.add(c)
        print("  cdx %s batch %d: +%d lines, %d distinct slugs"
              % (domain, batch, len(lines), len(out)), flush=True)
        if not nk:
            break
        key = nk
        time.sleep(1)
    s.close()
    return out


# ---- source 2: the corpus --------------------------------------------------
MASTER_RX = re.compile(
    r"(governmentjobs\.com|schooljobs\.com)/careers/([A-Za-z0-9._%-]+)", re.I)


def master_slugs():
    """The syndicated floor: what other sources already leaked into the master."""
    found = {h: set() for h in HOSTS}
    if not os.path.exists(MASTER):
        print("  (no all_jobs_master.csv — skipping corpus mine)", flush=True)
        return found
    csv.field_size_limit(10 ** 9)
    n = 0
    with open(MASTER, newline="", encoding="utf-8", errors="replace") as f:
        for r in csv.DictReader(f):
            u = (r.get("apply_url") or "") + " " + (r.get("job_url") or "")
            if "governmentjobs.com" not in u and "schooljobs.com" not in u:
                continue
            for m in MASTER_RX.finditer(u):
                c = clean_slug(m.group(2))
                if c:
                    found["www." + m.group(1).lower()].add(c)
                    n += 1
    print("  corpus: %d url hits -> %s" % (
        n, {h: len(v) for h, v in found.items()}), flush=True)
    return found


# ---- probe -----------------------------------------------------------------
LIVE_MARKERS = ("powered-by-neogov", "jobs-not-found", "search-results-listing",
                "listing-title", "pagination-container")
TITLE_RX = re.compile(r"document\.title\s*=\s*'([^']*)'")


def board_name(body):
    """The agency name out of the partial's own document.title assignment.

    Format is '<board title> | Sorted by <sort> | <agency name>'.  Neither end
    is reliably the employer ('State of Washington Job Opportunities |
    Sorted by Relevance ascending |   Work that Matters' — the tail is a
    tagline; 'Career Opportunities | ... | BGSU Careers' — the head is generic)
    so BOTH halves are kept, longest first, purely as a human label.  The
    authoritative employer name comes from the jobs payload's CountyName in
    neogov_fetch.py, never from here."""
    m = TITLE_RX.search(body or "")
    if not m:
        return ""
    parts = [p.strip() for p in m.group(1).split("|") if p.strip()]
    parts = [p for p in parts if not p.lower().startswith("sorted by")]
    if not parts:
        return ""
    head = re.sub(r"\s*(job|career|employment)\s+opportunities\s*$", "",
                  parts[0], flags=re.I).strip()
    tail = parts[-1].strip() if len(parts) > 1 else ""
    # A head of exactly "Career Opportunities" collapses to nothing above, so
    # the tail carries the identity; when both survive, the longer one is the
    # more specific.
    cand = head if not tail else (head if len(head) > len(tail) else tail)
    if not head:
        cand = tail
    return (cand or parts[0])[:120]


def probe(s, host, agency):
    """-> (status, name).  status in live/dead/moved:<host>/unknown.
    Raises Transient so an unprobed candidate stays PENDING."""
    url = PROBE_URL % (host, agency)
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
            body = r.text or ""
            low = body.lower()
            if any(mk in low for mk in LIVE_MARKERS):
                speed_up(host)
                return "live", board_name(body)
            # 200 with an unrecognised body: OUR problem, not a dead board.
            return "unknown", ""
        if code == 301:
            loc = r.headers.get("Location") or ""
            for h in HOSTS:
                if h in loc and h != host:
                    return "moved:" + h, ""
            return "dead", ""
        if code in (302, 303, 307, 308, 404, 410):
            return "dead", ""
        if code in (429, 418, 503):
            slow_down(host)
            last = "HTTP %d" % code
        else:
            last = "HTTP %d" % code
        time.sleep(2 * (attempt + 1))
    raise Transient("%s: %s" % (url, last))


def probe_one(rows, host, agency):
    s = cc.session()
    try:
        try:
            status, name = probe(s, host, agency)
        except Transient as e:
            with _lock:
                _stats["transient"] += 1
            print("  TRANSIENT %s/%s: %s" % (host, agency, e), flush=True)
            return
    finally:
        s.close()
    with _lock:
        rec = rows.setdefault((host, agency), {})
        rec["host"], rec["agency"] = host, agency
        rec["checked"] = TODAY
        if status.startswith("moved:"):
            target = status.split(":", 1)[1]
            rec["status"] = "migrated"
            rec["jobs_hint"] = "moved_to:" + target
            _stats["migrated"] += 1
            other = rows.setdefault((target, agency), {
                "host": target, "agency": agency,
                "origin": "moved_from:" + host, "status": "", "name": "",
                "jobs_hint": "", "checked": ""})
            other.setdefault("origin", "moved_from:" + host)
        else:
            rec["status"] = status
            rec["name"] = name
            _stats[status if status in _stats else "unknown"] += 1


# ---- csv io ----------------------------------------------------------------
def read_rows():
    rows = {}
    if not os.path.exists(CANDIDATES):
        return rows
    with open(CANDIDATES, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            host = (r.get("host") or "").strip().lower()
            agency = (r.get("agency") or "").strip().lower()
            if not host or not agency:
                continue
            rows[(host, agency)] = {k: (r.get(k) or "") for k in FIELDS}
    return rows


def write_rows(rows):
    tmp = CANDIDATES + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=FIELDS, extrasaction="ignore")
        w.writeheader()
        for (host, agency) in sorted(rows):
            rec = dict(rows[(host, agency)])
            rec["host"], rec["agency"] = host, agency
            w.writerow({k: rec.get(k, "") for k in FIELDS})
    os.replace(tmp, CANDIDATES)


def write_tenants(rows):
    live = [(h, a, rows[(h, a)].get("name") or "")
            for (h, a) in sorted(rows)
            if (rows[(h, a)].get("status") or "").lower() == "live"]
    os.makedirs(os.path.dirname(TENANTS), exist_ok=True)
    tmp = TENANTS + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["host", "agency", "company"])
        for h, a, n in live:
            w.writerow([h, a, n])
    os.replace(tmp, TENANTS)
    return len(live)


def main():
    rows = read_rows()
    print("neogov discover: %d candidates already on file" % len(rows),
          flush=True)

    if DO_CDX:
        for host in HOSTS:
            for slug in cdx_slugs(host):
                rows.setdefault((host, slug), {
                    "host": host, "agency": slug, "origin": "cdx",
                    "status": "", "name": "", "jobs_hint": "", "checked": ""})
    if DO_MINE:
        for host, slugs in master_slugs().items():
            for slug in slugs:
                rec = rows.setdefault((host, slug), {
                    "host": host, "agency": slug, "origin": "corpus",
                    "status": "", "name": "", "jobs_hint": "", "checked": ""})
                if rec.get("origin") == "cdx":
                    rec["origin"] = "cdx+corpus"
    write_rows(rows)
    print("neogov discover: %d candidates total (%s)"
          % (len(rows),
             {h: sum(1 for k in rows if k[0] == h) for h in HOSTS}), flush=True)

    if not DO_PROBE:
        return 0

    todo = [k for k, v in sorted(rows.items())
            if (v.get("status") or "").lower() not in
            ("live", "dead", "migrated")]
    if LIMIT:
        todo = todo[:LIMIT]
    print("neogov discover: probing %d candidates" % len(todo), flush=True)

    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    done = 0
    stopped = False
    try:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(probe_one, rows, h, a) for h, a in todo]
            for _ in cf.as_completed(futs):
                done += 1
                if done % 100 == 0:
                    with _lock:
                        write_rows(rows)
                    print("  %d/%d  %s" % (done, len(todo), _stats), flush=True)
                if deadline and time.time() > deadline:
                    stopped = True
                    for f in futs:
                        f.cancel()
                    break
    finally:
        with _lock:
            write_rows(rows)
            n = write_tenants(rows)
    print("DONE neogov discover: %s  live boards written to %s: %d"
          "  (429 backoffs: %s)%s"
          % (_stats, TENANTS, n, _n429,
             "  [BUDGET REACHED — rest still pending]" if stopped else ""),
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
