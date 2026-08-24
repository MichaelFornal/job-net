#!/usr/bin/env python3
"""
comeet_fetch.py — Comeet (now "Spark Hire Recruit") multi-tenant fetcher.

ROUTE (reverse-engineered 2026-07-28 from tenants/comeet.csv example_urls)
-------------------------------------------------------------------------
The documented API

    https://www.comeet.co/careers-api/2.0/company/{uid}/positions?token={tok}

is NOT open — without ?token it answers HTTP 400
{"status":400,"message":"Token is missing"}.  But the token is not a secret:
it is printed into the public careers page, so the board page is a complete
one-request substitute.

    GET https://www.comeet.com/jobs/{tenant}/{company_uid}        <- board page

is server-rendered and carries two JS globals:

    COMPANY_DATA           = {... "company_uid": "17.008",
                                  "token": "7182A9...", "slug": "cyera" ...}
    COMPANY_POSITIONS_DATA = [ {position}, ... ]     <- every open position

Each position carries name, department, employment_type, workplace_type, a
structured location dict (city/state/country/is_remote), uid, time_updated and
url_comeet_hosted_page.  That is everything a normalized row needs, so the
normal path is exactly ONE request per tenant.  When a board renders with
COMPANY_POSITIONS_DATA = null (Comeet does that on some themes) we fall back to
the careers-api using the token we just read out of COMPANY_DATA — still no
per-customer secret required.

The {company_uid} path segment is mandatory: https://www.comeet.com/jobs/cyera
with no uid answers HTTP 404 with a 72KB branded error page.  Every uid comes
from the mined example_url (segment 3), and all 202 rows in tenants/comeet.csv
have one.

www.comeet.co is the same product: it answers HTTP 301 to www.comeet.com for
the same path, so only one host is probed.

DEAD-TENANT DISCRIMINATOR  (rule 2 — never trust a status code alone)
---------------------------------------------------------------------
Comeet is the "307-redirects to the vendor's marketing site" trap in person.
A retired tenant does NOT 404.  Measured:

    GET /jobs/zzznotarealslug9/17.008  -> 302  Location: https://www.comeet.com/
    GET /jobs/cyera/99.999             -> 302  Location: https://www.comeet.com/
    following that redirect            -> 200, 82,620 bytes of
                                          "Spark Hire Recruit — The Applicant
                                           Tracking System that Eliminates
                                           Bottlenecks"  (marketing home page)

So with requests' default allow_redirects=True EVERY dead tenant looks like a
healthy HTTP 200 that merely fails to parse — which is precisely the shape a
naive fetcher reads as "throttled / retry me" and then burns hours on.  This
script therefore sends allow_redirects=False and treats

    3xx off /jobs/...            -> DEAD (retired tenant), never retried
    404 / 410                    -> DEAD
    200 without COMPANY_DATA     -> DEAD (parked/blank page)
    200 with COMPANY_DATA, 0 pos -> LIVE but empty (real board, no openings)
    429 / 5xx / timeout / conn   -> TRANSIENT: tenant stays PENDING (absent
                                    from the state file) so a re-run retries it

POSTED DATES
------------
Comeet exposes time_updated (last edit) and no creation date.  Writing
time_updated straight into `posted` would make every re-edited old job look
freshly posted, so dates go through comeet_posted_ledger.json: the FIRST date
ever observed for a job uid is the one that is kept forever, and later edits
cannot move it forward.  The ledger is append-only and must never be rotated
or deleted — deleting it resets every job to "posted today".

Output: contract_norm/comeet.jsonl   State: comeet_fetch_state.json
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
TENANTS = os.path.join(BASE, "tenants", "comeet.csv")
OUT = os.path.join(cc.NORM_DIR, "comeet.jsonl")
STATE = os.path.join(BASE, "comeet_fetch_state.json")
LEDGER = os.path.join(BASE, "comeet_posted_ledger.json")

SOURCE = "comeet"
HOST = "https://www.comeet.com"
API = "https://www.comeet.co/careers-api/2.0/company/%s/positions?token=%s"

# One shared host (www.comeet.com), so one global budget.  Board pages are big
# (cyera is 1.0MB uncompressed / ~150KB gzipped), so the ceiling that matters
# here is bytes in flight, not requests per second.
RPS = float(os.environ.get("COMEET_RPS") or 4.0)
CONCURRENCY = int(os.environ.get("COMEET_CONCURRENCY") or 8)
WORKERS = CONCURRENCY
TIMEOUT = 45
TRIES = 5
BUDGET_MIN = int(os.environ.get("COMEET_BUDGET_MIN") or 0)   # 0 = no limit
LIMIT = int(os.environ.get("COMEET_LIMIT") or 0)             # 0 = all tenants
REFRESH = os.environ.get("COMEET_REFRESH") == "1"

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
_stats = {"live": 0, "dead": 0, "transient": 0, "rows": 0}


class Transient(Exception):
    """Retryable failure.  Raised (never returned) so the caller leaves the
    tenant PENDING instead of recording it as an empty board."""


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


def get(s, url, accept="text/html"):
    """Throttled GET.  Returns (status, text).

    status is one of: "ok" (2xx body), "dead" (redirect / 404 / 410 — the
    tenant is gone, see the module docstring).  Anything else RAISES Transient.
    allow_redirects is False on purpose: the marketing-site redirect is the
    whole discriminator and following it destroys the evidence.
    """
    last = None
    for attempt in range(TRIES):
        throttle()
        try:
            r = s.get(url, timeout=TIMEOUT, allow_redirects=False,
                      headers={"Accept": accept})
            code = r.status_code
            if code == 200:
                return "ok", r.text or ""
            if code in (301, 302, 303, 307, 308):
                # Retired tenant bounced to https://www.comeet.com/ (the
                # Spark Hire Recruit marketing home page, HTTP 200, 82KB).
                # Dead, not transient.
                return "dead", ""
            if code in (400, 401, 403, 404, 410):
                return "dead", ""
            if code in (429, 418):
                slow_down()
                last = "HTTP %d" % code
            else:
                last = "HTTP %d" % code
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    raise Transient("%s failed after %d tries: %s" % (url, TRIES, last))


_DEC = json.JSONDecoder()


def js_global(body, name):
    """Value of a `var NAME = <json>;` global in a server-rendered page.

    raw_decode rather than a regex: the positions blob contains full job
    descriptions with escaped quotes, semicolons and newlines, and a non-greedy
    `(.*?);` match truncates it at the first semicolon inside the HTML.
    """
    for m in re.finditer(re.escape(name) + r"\s*=\s*", body):
        i = m.end()
        if body[i:i + 4] == "null":
            return None
        if body[i:i + 1] not in ("[", "{"):
            continue
        try:
            val, _ = _DEC.raw_decode(body, i)
        except ValueError:
            continue
        return val
    return None


_WS = re.compile(r"\s+")


def sq(v):
    return _WS.sub(" ", html.unescape(str(v or ""))).strip()


def wm(workplace_type, is_remote, loc_text):
    """Comeet's structured workplace_type first, its structured location
    is_remote second, and only then a text guess — a wrong remote flag is worse
    than none, so anything ambiguous stays blank."""
    t = (workplace_type or "").strip().lower()
    if "remote" in t:
        return "Remote"
    if "hybrid" in t:
        return "Hybrid"
    if t in ("on-site", "onsite", "on site", "office", "in-office"):
        return "On-site"
    if is_remote is True:
        return "Remote"
    return cc.work_mode(text=loc_text)


def ledger_posted(key, time_updated):
    """First date ever seen for this position wins, forever."""
    d = cc.ymd(iso=time_updated) if time_updated else ""
    with _lock:
        rec = _ledger.get(key)
        if rec:
            return rec.get("posted") or ""
        _ledger[key] = {"posted": d or TODAY,
                        "precision": "time_updated" if d else "first_seen",
                        "first_seen": TODAY}
        return _ledger[key]["posted"]


def rows_from_positions(tenant, uid, positions, company_fallback):
    out = []
    for p in positions or []:
        if not isinstance(p, dict):
            continue
        if p.get("is_internal") is True:
            continue
        title = sq(p.get("name"))
        if not title:
            continue
        loc = p.get("location") if isinstance(p.get("location"), dict) else {}
        # "Any" is Comeet's literal placeholder for an unset city on a
        # country-wide / remote posting — it is not a place name.
        if sq(loc.get("city")).lower() in ("any", "n/a", "none"):
            loc = dict(loc, city="")
        locstr = sq(loc.get("name")) or ", ".join(
            x for x in (sq(loc.get("city")), sq(loc.get("state")),
                        sq(loc.get("country"))) if x)
        mode = wm(p.get("workplace_type"), loc.get("is_remote"),
                  locstr + " " + title)
        pid = sq(p.get("uid"))
        apply_url = (p.get("url_comeet_hosted_page")
                     or p.get("url_active_page")
                     or p.get("url_recruit_hosted_page")
                     or ("%s/jobs/%s/%s/%s" % (HOST, tenant, uid, pid)))
        out.append(cc.row(
            source=SOURCE, source_type="ats",
            company=sq(p.get("company_name")) or company_fallback,
            title=title,
            employment_type=cc.classify_any(p.get("employment_type"), title),
            work_mode=mode, location=locstr,
            city=sq(loc.get("city")), state=sq(loc.get("state")),
            country=sq(loc.get("country")),
            remote=(mode == "Remote"),
            posted=ledger_posted("%s:%s" % (tenant, pid), p.get("time_updated")),
            apply_url=apply_url, job_id=pid, board=tenant,
            board_url="%s/jobs/%s/%s" % (HOST, tenant, uid)))
    return out


def fetch_tenant(s, tenant, uid):
    """-> (verdict, rows).  verdict in {"live", "dead:<why>"}; Transient raises."""
    st, body = get(s, "%s/jobs/%s/%s" % (HOST, tenant, uid))
    if st == "dead":
        return "dead:redirect_or_404", []
    company = js_global(body, "COMPANY_DATA")
    if not isinstance(company, dict):
        # 200 with no COMPANY_DATA at all: a parked/blank page, not a board.
        return "dead:no_company_data", []
    name = sq(company.get("name")) or tenant
    real_uid = sq(company.get("company_uid")) or uid
    positions = js_global(body, "COMPANY_POSITIONS_DATA")
    if not isinstance(positions, list):
        # Theme that does not preload the list.  The token is right there in
        # COMPANY_DATA, so use the documented careers-api with it.
        tok = sq(company.get("token"))
        if tok:
            st2, body2 = get(s, API % (real_uid, tok), accept="application/json")
            if st2 == "ok":
                try:
                    d = json.loads(body2)
                except ValueError:
                    d = None
                if isinstance(d, list):
                    positions = d
    if not isinstance(positions, list):
        positions = []
    return "live", rows_from_positions(tenant, real_uid, positions, name)


def work(w, tenant, uid):
    key = tenant
    with _gate:
        s = cc.session()
        s.headers.update({"Accept-Language": "en-US,en;q=0.9"})
        try:
            verdict, rows = fetch_tenant(s, tenant, uid)
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
                _state[key] = {"status": "live", "jobs": len(rows),
                               "at": TODAY}
            else:
                _stats["dead"] += 1
                _state[key] = {"status": "dead", "why": verdict.split(":", 1)[1],
                               "at": TODAY}


UID_RX = re.compile(r"^https?://[^/]+/jobs/([^/]+)/([0-9A-Fa-f]{2}\.[0-9A-Fa-f]{3})")


def load_tenants():
    out = []
    with open(TENANTS, newline="", encoding="utf-8") as f:
        for r in csv.DictReader(f):
            t = (r.get("tenant") or "").strip()
            if not t:
                continue
            m = UID_RX.match((r.get("example_url") or "").strip())
            if not m:
                print("  SKIP %s: no company_uid in example_url" % t,
                      flush=True)
                continue
            out.append((t, m.group(2)))
    return out


def main():
    tenants = load_tenants()
    todo = [(t, u) for t, u in tenants
            if REFRESH or (_state.get(t) or {}).get("status") not in
            ("live", "dead")]
    random.shuffle(todo)
    already = len(tenants) - len(todo)
    if LIMIT:
        todo = todo[:LIMIT]
    print("comeet: %d tenants, %d to fetch (%d already recorded)"
          % (len(tenants), len(todo), already), flush=True)

    w = cc.Writer(OUT)
    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    done = 0
    stopped = False
    try:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(work, w, t, u) for t, u in todo]
            for _ in cf.as_completed(futs):
                done += 1
                if done % 25 == 0:
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
    print("DONE comeet: %s  (429 backoffs: %d)%s"
          % (_stats, _n429[0],
             "  [BUDGET REACHED — rest still pending]" if stopped else ""),
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
