#!/usr/bin/env python3
"""
jobvite_fetch.py — Jobvite CareerSite multi-tenant fetcher.

ROUTE (reverse-engineered 2026-07-28 against tenants/jobvite.csv, 80 tenants)
-----------------------------------------------------------------------------
There is NO public JSON on jobs.jobvite.com.  Every JSON-shaped guess answers
HTTP 200 with the *board HTML*, because the CareerSite servlet is a catch-all
under /{tenant}/... — measured on nutanix, all four byte-identical (99,182 B)
to the plain board page:

    /nutanix/jobs.json   /nutanix/jobs.rss   /nutanix/feed   /careers/nutanix/jobs

That catch-all is a trap in its own right: "HTTP 200 and I asked for JSON" is
true for all four and yields zero JSON.  The identical byte count is the tell.

The route actually used is the paginated search view:

    GET https://jobs.jobvite.com/{tenant}/search/?nl=1&p={page}

  * server-rendered (the Angular bundle only powers the apply flow)
  * 50 postings per page, p is 0-indexed
  * carries an authoritative total, "1-50 of 307", and a
    <a class="jv-pagination-next"> link — so completeness is verifiable rather
    than assumed
  * nl=1 ("no layout") is what makes it work for EVERY tenant — see below

The tenant ROOT page (/{tenant}) is deliberately NOT used even though it is
server-rendered and needs only one request: it is TRUNCATED on most boards.
Measured across the 71 reachable tenants, root ids vs the search total:
nutanix 101/221, jbs 345/738, resgroup 98/142, biomarin 125/136, and 11
tenants render a marketing landing page with ZERO job links (torrid 0/593,
martinmarietta 0/307, aspirepublicschools 0/156).  Root-page totals came to
2,514 against 4,650 by search — i.e. root silently loses 46% of the corpus.

Two row markups exist and both are handled: the <table>/<td class=
"jv-job-list-name"> form (nutanix) and the flex <a><div class="col
jv-job-list-name"> form (martinmarietta).  The job id is taken from the href
inside the name cell, or from the enclosing anchor just before it.

DEAD-TENANT DISCRIMINATOR  (rule 2 — never trust a status code alone)
---------------------------------------------------------------------
jobs.jobvite.com is wildcard-routed and an unknown tenant does NOT 404 — it
302s, and following that redirect lands on a 170KB HTTP 200 marketing page.
With requests' default allow_redirects=True a retired tenant is therefore
indistinguishable from a live one by status code.

But "302" alone is NOT the discriminator either, and this is the part that
would have cost the most: 9 of these 80 tenants 302 off /{tenant} to the
CUSTOMER's own site, because Jobvite is configured to embed the board in an
iframe there:

    imprivata -> https://www.imprivata.com/company/join-team?p=jobs&nl=1
    appfolio  -> https://www.appfolioinc.com/open-roles?p=jobs&nl=1
    simaai    -> https://sima.ai/careers/?jobviteiframe=jobs&nl=1

Those boards are ALIVE — the redirect target hands the nl=1 flag straight back
to Jobvite for the iframe.  Requesting ?nl=1 ourselves makes all nine serve
200 directly (imprivata 41 postings, appfolio 31, simaai 22, pih 56...).
Marking them dead on the 302 would have thrown away 11% of the platform.

So the classification, with nl=1 already on the URL:

    200                      -> LIVE, parse
    302 -> search.jobvite.com/?invalid=1   -> DEAD, the tenant does not exist
    302 -> anywhere else                   -> DEAD, recorded with the target
                                              host so a re-run of this file
                                              can revisit the assumption
    404 / 410                -> DEAD
    200, valid page, 0 rows  -> LIVE but empty (a real board with no openings)
    429 / 5xx / timeout      -> TRANSIENT: tenant left ABSENT from the state
                                file, so a re-run retries it

EMPLOYMENT TYPE / DATES
-----------------------
The public CareerSite exposes no posting date at all — the job detail page
(75KB of Angular) has no ld+json and no datePosted either, so an N+1 would buy
nothing.  Employment type exists only on a minority of boards.  Therefore:
  * employment_type is cc.classify_any(jv-job-type or None, title).  A
    <div class="…jv-job-type"> cell is present on some boards and is a real
    type field (resgroup publishes CDI / CDD / Alternance / Fixed Term Part
    Time / Regular Full-Time, all of which contract_common already maps —
    including the CDI permanent-contract trap); on the boards without it the
    title alone drives the label.  The CATEGORY heading is deliberately never
    used as raw_type: a department literally named "Consulting" would relabel
    a whole board as contract work.
  * posted comes from jobvite_posted_ledger.json — first-seen date per job id,
    precision "first_seen".  Append-only; never rotate or delete it, or every
    posting resets to "posted today".

Output: contract_norm/jobvite.jsonl   State: jobvite_fetch_state.json
"""
import concurrent.futures as cf
import csv
import html
import os
import random
import re
import sys
import threading
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
TENANTS = os.path.join(BASE, "tenants", "jobvite.csv")
OUT = os.path.join(cc.NORM_DIR, "jobvite.jsonl")
STATE = os.path.join(BASE, "jobvite_fetch_state.json")
LEDGER = os.path.join(BASE, "jobvite_posted_ledger.json")

SOURCE = "jobvite"
HOST = "https://jobs.jobvite.com"
PAGE = HOST + "/%s/search/?nl=1&p=%d"

RPS = float(os.environ.get("JOBVITE_RPS") or 4.0)
CONCURRENCY = int(os.environ.get("JOBVITE_CONCURRENCY") or 8)
WORKERS = CONCURRENCY
MAX_PAGES = int(os.environ.get("JOBVITE_MAX_PAGES") or 60)   # 60 * 50 = 3,000
TIMEOUT = 45
TRIES = 5
BUDGET_MIN = int(os.environ.get("JOBVITE_BUDGET_MIN") or 0)
LIMIT = int(os.environ.get("JOBVITE_LIMIT") or 0)
REFRESH = os.environ.get("JOBVITE_REFRESH") == "1"

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]
_n429 = [0]
_gate = threading.Semaphore(CONCURRENCY)

_state = cc.load_state(STATE)
_ledger = cc.load_state(LEDGER)
_stats = {"live": 0, "dead": 0, "transient": 0, "rows": 0, "short": 0}


class Transient(Exception):
    """Retryable failure — raised so the tenant stays PENDING."""


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


def get(s, url):
    """-> ("ok", body) | ("dead", why).  Transient failures RAISE."""
    last = None
    for attempt in range(TRIES):
        throttle()
        try:
            r = s.get(url, timeout=TIMEOUT, allow_redirects=False,
                      headers={"Accept": "text/html,application/xhtml+xml"})
            code = r.status_code
            if code == 200:
                return "ok", r.text or ""
            if code in (301, 302, 303, 307, 308):
                loc = r.headers.get("Location") or ""
                host = urlsplit(loc).netloc or "?"
                if "invalid=1" in loc or "jobvite.com" in host:
                    return "dead", "invalid_tenant"
                # With nl=1 already set, no live board was observed
                # redirecting; record the target so the call is auditable.
                return "dead", "redirect:" + host[:60]
            if code in (400, 401, 403, 404, 410):
                return "dead", "http_%d" % code
            if code in (429, 418):
                slow_down()
                last = "HTTP %d" % code
            else:
                last = "HTTP %d" % code
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    raise Transient("%s failed after %d tries: %s" % (url, TRIES, last))


# ---- markup -----------------------------------------------------------------
_TAG = re.compile(r"<[^>]+>")
_WS = re.compile(r"\s+")
# Rows are parsed by ANCHORING ON THE JOB LINK and reading the span up to the
# next job link, rather than by matching a fixed name-then-location shape.
# Three CareerSite row layouts are in the wild across these 80 tenants and a
# name-immediately-followed-by-location pattern only matches two of them:
#
#   table    <td class="jv-job-list-name"><a href=/t/job/ID>Title</a></td>
#            <td class="jv-job-list-location">City, State</td>          (nutanix)
#   flex     <a class=... href=/t/job/ID><div class="col jv-job-list-name">
#            Title</div><div class="col jv-job-list-location">…</div></a>
#                                                             (martinmarietta)
#   nested   <a href=/t/job/ID><div class="jv-job-list-name">Title</div>
#            <div class="flex-col flex-row-md">        <-- extra wrapper
#              <div class="jv-job-list-location">…</div>
#              <div class="ml2-md jv-job-type">CDD</div></div></a>   (resgroup)
#
# The wrapper in the third form is what a strict pair regex trips over: it cost
# resgroup 142 postings, neogenomics 74 and imagine-learning 35, all three
# recorded as live-with-zero-jobs — an empty board is a perfectly plausible
# result, so nothing would have flagged the loss without the claimed-total
# cross-check below.
_HREF_JOB = re.compile(r'href="([^"]*/job/([A-Za-z0-9]{4,}))[^"]*"', re.I)
_NAME_CELL = re.compile(
    r'class="[^"]*jv-job-list-name[^"]*"[^>]*>(.*?)</(?:td|div|span|h\d)>',
    re.S | re.I)
_LOC_CELL = re.compile(
    r'class="[^"]*jv-job-list-location[^"]*"[^>]*>(.*?)</(?:td|div|span)>',
    re.S | re.I)
# The one place Jobvite does publish a structured employment type.  Present on
# a minority of boards (resgroup: CDI / CDD / Alternance / Fixed Term Part
# Time / Regular Full-Time); absent on most, hence the title-only default.
_TYPE_CELL = re.compile(
    r'class="[^"]*jv-job-type[^"]*"[^>]*>(.*?)</(?:td|div|span)>',
    re.S | re.I)
_META = re.compile(r'<div[^>]*class="[^"]*jv-meta[^"]*"[^>]*>.*?</div>',
                   re.S | re.I)
_SPANCOMMA = re.compile(r"<span>\s*,\s*</span>")
_MULTI = re.compile(r"^\d+\s+locations?$", re.I)
_NEXT = re.compile(r'href="([^"]+)"[^>]*class="[^"]*jv-pagination-next',
                   re.I)
_TOTAL = re.compile(r"(\d+)\s*-\s*(\d+)\s+of\s+(\d+)")
# The workplace chip Jobvite prints ahead of the place, separated by
# <span>,</span>.  Only these exact strings are treated as a chip; anything
# else is a place name.
_CHIP = {"remote": "Remote", "hybrid remote": "Hybrid", "hybrid": "Hybrid",
         "on-site": "On-site", "onsite": "On-site", "in office": "On-site"}

# Second component of "City, Region": Jobvite prints a US state, a Canadian
# province OR a country there.  Anything unrecognized is treated as a country
# rather than guessed into `state`.
_US = {
    "alabama", "alaska", "arizona", "arkansas", "california", "colorado",
    "connecticut", "delaware", "district of columbia", "florida", "georgia",
    "hawaii", "idaho", "illinois", "indiana", "iowa", "kansas", "kentucky",
    "louisiana", "maine", "maryland", "massachusetts", "michigan", "minnesota",
    "mississippi", "missouri", "montana", "nebraska", "nevada",
    "new hampshire", "new jersey", "new mexico", "new york", "north carolina",
    "north dakota", "ohio", "oklahoma", "oregon", "pennsylvania",
    "rhode island", "south carolina", "south dakota", "tennessee", "texas",
    "utah", "vermont", "virginia", "washington", "west virginia", "wisconsin",
    "wyoming", "puerto rico",
}
_CA = {
    "alberta", "british columbia", "manitoba", "new brunswick",
    "newfoundland and labrador", "nova scotia", "ontario",
    "prince edward island", "quebec", "quebec", "saskatchewan",
    "northwest territories", "nunavut", "yukon",
}


def sq(v):
    return _WS.sub(" ", html.unescape(_TAG.sub(" ", v or ""))).strip()


def parse_loc_cell(cell):
    """-> (location, city, state, country, chip_work_mode).

    Handles 'Hybrid Remote<span>,</span> Colombia' and the
    '<div class="jv-meta">2 Locations</div>' multi-location counter, which is
    a count and not a place and is therefore dropped rather than written into
    `location` as a fake place name.
    """
    chip = ""
    segs = _SPANCOMMA.split(cell, 1)
    if len(segs) == 2:
        head = sq(segs[0]).lower()
        if head in _CHIP:
            chip = _CHIP[head]
            cell = segs[1]
    text = sq(_META.sub(" , ", cell))
    parts = [p.strip() for p in text.split(",")
             if p.strip() and not _MULTI.match(p.strip())]
    if not parts:
        return "", "", "", "", chip
    loc = ", ".join(parts)
    if len(parts) == 1:
        return loc, "", "", "", chip
    city, rest = parts[0], parts[-1]
    low = rest.lower()
    if low in _US:
        return loc, city, rest, "United States", chip
    if low in _CA:
        return loc, city, rest, "Canada", chip
    if city.lower() == low:                 # "Singapore, Singapore"
        return loc, "", "", rest, chip
    return loc, city, "", rest, chip


def ledger_posted(key):
    with _lock:
        rec = _ledger.get(key)
        if rec:
            return rec.get("posted") or ""
        _ledger[key] = {"posted": TODAY, "precision": "first_seen",
                        "first_seen": TODAY}
        return TODAY


def company_of(body, tenant):
    m = re.search(r"<title>([^<]+)</title>", body, re.I)
    if not m:
        return tenant
    t = sq(m.group(1))
    return re.sub(r"\s*(careers?|jobs)\s*$", "", t, flags=re.I).strip() or t


def parse_page(tenant, body, seen, out, company):
    """Append rows for one search page.

    Returns (raw, new): postings SEEN on the page and postings actually
    emitted.  The two differ because Jobvite's paginator repeats a posting
    when the result set shifts under it (torrid: 593 claimed, 593 raw across
    12 pages, 575 distinct).  Keeping both apart is what stops harmless
    cross-page duplication from being mistaken for a truncated fetch.
    """
    raw = new = 0
    hits = list(_HREF_JOB.finditer(body))
    for i, hm in enumerate(hits):
        jid = hm.group(2)
        end = hits[i + 1].start() if i + 1 < len(hits) else len(body)
        blk = body[hm.end():min(end, hm.end() + 2500)]
        # step past the remainder of the opening <a ...> tag
        gt = blk.find(">")
        blk = blk[gt + 1:] if 0 <= gt < 300 else blk
        nm = _NAME_CELL.search(blk)
        if nm and nm.start() < 400:
            title = sq(nm.group(1))          # flex / nested: name is a child
        else:
            title = sq(blk.split("</a>", 1)[0])   # table: name is the anchor
        if not title:
            continue
        raw += 1
        if jid in seen:
            continue
        seen.add(jid)
        new += 1
        lm = _LOC_CELL.search(blk)
        loc, city, state, country, chip = parse_loc_cell(
            lm.group(1) if lm else "")
        tm = _TYPE_CELL.search(blk)
        raw_type = sq(tm.group(1)) if tm else ""
        if "${" in raw_type or "{{" in raw_type:
            raw_type = ""                     # un-rendered Angular template
        mode = chip or cc.work_mode(text=loc + " " + title)
        out.append(cc.row(
            source=SOURCE, source_type="ats", company=company, title=title,
            # jv-job-type when the board publishes one, title-only otherwise.
            # The category heading is deliberately never used — see docstring.
            employment_type=cc.classify_any(raw_type or None, title),
            work_mode=mode, location=loc, city=city, state=state,
            country=country, remote=(mode == "Remote"),
            posted=ledger_posted("%s:%s" % (tenant, jid)),
            apply_url="%s/%s/job/%s" % (HOST, tenant, jid),
            job_id=jid, board=tenant, board_url="%s/%s" % (HOST, tenant)))
    return raw, new


def fetch_tenant(s, tenant):
    """-> (verdict, rows, total_claimed, raw_seen)."""
    seen, out = set(), []
    total = None
    raw_seen = 0
    page = 0
    company = tenant
    while page < MAX_PAGES:
        st, body = get(s, PAGE % (tenant, page))
        if st == "dead":
            if page == 0:
                return "dead:" + body, [], None, 0
            break                      # later page vanished: keep what we have
        if page == 0:
            if "jv-job-list" not in body and "jv-pagination" not in body \
                    and "jv-page" not in body:
                return "dead:not_a_board", [], None, 0
            company = company_of(body, tenant)
            m = _TOTAL.search(body)
            if m:
                total = int(m.group(3))
        raw, new = parse_page(tenant, body, seen, out, company)
        raw_seen += raw
        if not _NEXT.search(body) or raw == 0:
            break
        page += 1
    return "live", out, total, raw_seen


def work(w, tenant):
    with _gate:
        s = cc.session()
        s.headers.update({"Accept-Language": "en-US,en;q=0.9"})
        try:
            verdict, rows, total, raw_seen = fetch_tenant(s, tenant)
        except Transient as e:
            with _lock:
                _stats["transient"] += 1
            print("  TRANSIENT %s: %s" % (tenant, e), flush=True)
            return
        except Exception as e:                       # noqa: BLE001
            with _lock:
                _stats["transient"] += 1
            print("  TRANSIENT %s: %s" % (tenant, type(e).__name__), flush=True)
            return
        finally:
            s.close()
        with _lock:
            for r in rows:
                w.write(r)
            _stats["rows"] += len(rows)
            if verdict == "live":
                _stats["live"] += 1
                rec = {"status": "live", "jobs": len(rows), "at": TODAY}
                if total is not None:
                    rec["claimed_total"] = total
                    rec["raw_seen"] = raw_seen
                    # raw_seen is measured BEFORE cross-page dedupe, so this
                    # fires only when postings were never parsed at all — the
                    # markup-variant failure mode — and not when the paginator
                    # merely repeats a posting.  Without it, a parse miss looks
                    # exactly like a board with no openings.
                    if raw_seen < total:
                        _stats["short"] += 1
                        rec["short_by"] = total - raw_seen
                _state[tenant] = rec
            else:
                _stats["dead"] += 1
                _state[tenant] = {"status": "dead",
                                  "why": verdict.split(":", 1)[1], "at": TODAY}


def load_tenants():
    with open(TENANTS, newline="", encoding="utf-8") as f:
        return [r["tenant"].strip() for r in csv.DictReader(f)
                if (r.get("tenant") or "").strip()]


def main():
    tenants = load_tenants()
    todo = [t for t in tenants
            if REFRESH or (_state.get(t) or {}).get("status") not in
            ("live", "dead")]
    random.shuffle(todo)
    already = len(tenants) - len(todo)
    if LIMIT:
        todo = todo[:LIMIT]
    print("jobvite: %d tenants, %d to fetch (%d already recorded)"
          % (len(tenants), len(todo), already), flush=True)

    w = cc.Writer(OUT)
    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    done = 0
    stopped = False
    try:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(work, w, t) for t in todo]
            for _ in cf.as_completed(futs):
                done += 1
                if done % 10 == 0:
                    with _lock:
                        cc.save_state(STATE, _state)
                        cc.save_state(LEDGER, _ledger)
                    print("  %d/%d  %s" % (done, len(todo), _stats), flush=True)
                if deadline and time.time() > deadline:
                    stopped = True
                    for f in futs:
                        f.cancel()
                    break
    finally:
        cc.save_state(STATE, _state)
        cc.save_state(LEDGER, _ledger)
        w.close()
    print("DONE jobvite: %s  (429 backoffs: %d)%s"
          % (_stats, _n429[0],
             "  [BUDGET REACHED — rest still pending]" if stopped else ""),
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
