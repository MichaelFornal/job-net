#!/usr/bin/env python3
"""
adp_fetch.py — ADP WorkforceNow career-center boards (tenants/adp.csv).

THE ROUTE
---------
The tenant id is a `cid` GUID, not a subdomain.  Every board is served from one
shared host, workforcenow.adp.com, and the customer-facing page

    /mascsr/default/mdf/recruitment/recruitment.html?cid={cid}&jobId={n}

is an Angular SPA that renders nothing without JavaScript.  The REST route it
calls (verified live 2026-07-29, unauthenticated, no cookie, no referer) is

    GET /mascsr/default/careercenter/public/events/staffing/v1/job-requisitions
        ?cid={cid}&locale=en_US&$top=20&$skip={n}

    -> {"jobRequisitions": [ ... ], "meta": {"startSequence": n,
                                             "totalNumber": <board total>}}

Per-posting detail is at .../v1/job-requisitions/{itemID}?cid={cid} but adds
nothing this pipe needs except the description HTML (33KB a job) — the list
payload already carries title, post date, pay range, worker category and
locations — so it is deliberately NOT fetched.

PAGING
------
`$top` is a LIE.  The server caps a page at 20 rows no matter what you ask for
($top=200 and $top=1000 both return 20), and it pages off `$skip` alone, which
it echoes back as meta.startSequence.  So $skip is advanced by a fixed PAGE=20,
never by len(returned) — the first page of some boards comes back with 19 rows
for a 20-row window (one row filtered server-side), and advancing by 19 would
re-request a window that overlaps the previous one and shift every subsequent
page, silently duplicating and dropping rows.  Rows are additionally deduped on
itemID as a belt-and-braces check.

DEAD TENANT vs LIVE-BUT-EMPTY vs TRANSIENT  (rule 2 — the expensive one)
-----------------------------------------------------------------------
Measured against real and synthetic cids on 2026-07-29:

  live board        HTTP 200, content-type application/json, jobRequisitions
                    non-empty, meta.totalNumber present.
  live but EMPTY    HTTP 200, application/json, body is exactly
                    '{"jobRequisitions":[]}' (22 bytes) and there is NO `meta`
                    key at all.  The cid is provisioned; the customer has no
                    open postings today.  1 of 25 sampled real cids.
  DEAD tenant       HTTP 404 with a 552-byte openresty text/html error page.
                    Confirmed with four synthetic cids (all-zero GUID,
                    deadbeef GUID, a non-GUID string, and a real cid with its
                    last hex digit flipped) — every one 404s.  This is a real
                    discriminator, not a guess: the edge knows which cids
                    exist, so 404 means gone and is recorded dead, never
                    retried.

Two traps that are NOT dead-tenant signals and must not be read as one:

  * an UNKNOWN ROUTE NAME under .../staffing/v1/ answers HTTP **500**, not 404
    (verified: organizations, clients, company-name, branding, ... all 500).
    So a 500 on the one route this script calls is a server-side fault on a
    route that demonstrably exists — transient, retry it, never "dead".
  * 403 is not used for retired boards here (unlike the per-tenant-subdomain
    platforms in ats_fetch.py, where 403 means parked).  On this shared host a
    403 can only be an edge/WAF decision, so it is treated as throttling:
    back off and retry, and if it survives TRIES it stays PENDING.

Anything that is not 200-json and not 404 leaves the tenant ABSENT from the
state file, so the next run picks it up again.

COMPANY NAME
------------
The public API exposes no company/client name anywhere — not on the list, not
on the detail, and every plausible sibling route 500s.  So the display name is
resolved from adp_company_map.json, which this script builds once by reading
(never writing) all_jobs_master.csv and taking the most common `company` value
already recorded against each cid.  That also makes the emitted company string
byte-identical to the one the corpus holds, which is what lets these rows
collapse onto their existing copies: all_jobs_build.canon_key has no ADP
branch, so ADP postings dedupe on the tier-2 key
`ct|company|title|apply_url` — every one of those three has to match.

APPLY URL
---------
`?cid={cid}&jobId={ExternalJobID}` — NOT the itemID.  ExternalJobID lives in
customFieldGroup.stringFields and is verified to be the number that appears in
the real posting links (tenants/adp.csv example_url jobId=581446 is present as
an ExternalJobID on that cid's board).  Of the 10,551 ADP apply URLs already in
the master, 5,994 use exactly this two-parameter form and 3,386 add a
`ccId`; the plurality form is emitted because tier-2 dedupe compares the whole
URL string and the API gives us no way to know a board's ccId.

Resumable via adp_fetch_state.json (per cid), one row per posting into
contract_norm/adp.jsonl.
"""
import concurrent.futures as cf
import csv
import json
import os
import random
import re
import sys
import threading
import time

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
TENANTS = os.path.join(BASE, "tenants", "adp.csv")
OUT = os.path.join(cc.NORM_DIR, "adp.jsonl")
STATE = os.path.join(BASE, "adp_fetch_state.json")
COMPANY_MAP = os.path.join(BASE, "adp_company_map.json")
MASTER = os.path.join(BASE, "all_jobs_master.csv")

API = ("https://workforcenow.adp.com/mascsr/default/careercenter/public/"
       "events/staffing/v1/job-requisitions")
PAGE_URL = "https://workforcenow.adp.com/mascsr/default/mdf/recruitment/" \
           "recruitment.html"

SOURCE = "adp"
PAGE = 20                 # server-enforced page size; see the PAGING note
MAX_PAGES = 250           # 5,000 postings a board — bounds a pathological tail
WORKERS = int(os.environ.get("ADP_WORKERS") or 12)
RPS = float(os.environ.get("ADP_RPS") or 8.0)
CONCURRENCY = int(os.environ.get("ADP_CONCURRENCY") or 8)
TIMEOUT = 30
TRIES = 5
BUDGET_MIN = int(os.environ.get("ADP_BUDGET_MIN") or 0)   # 0 = no limit
LIMIT = int(os.environ.get("ADP_LIMIT") or 0)             # 0 = all tenants

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]
_n429 = [0]
_gate = threading.Semaphore(CONCURRENCY)
_state = {}
_counts = {"rows": 0, "live": 0, "dead": 0, "empty": 0, "failed": 0}


class Transient(Exception):
    """Retryable.  Raised so the tenant is left PENDING rather than recorded
    as a completed empty board — the mistake that cost this repo hours."""


class Dead(Exception):
    """The cid is not provisioned (HTTP 404 openresty).  Recorded, not retried."""


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


def gj(s, params):
    """Throttled GET -> parsed JSON, or Dead / Transient.

    Never returns a sentinel dict: an unreached board must be distinguishable
    from an empty one by the caller, and the only safe way to do that is
    control flow.
    """
    last = None
    for attempt in range(TRIES):
        throttle()
        try:
            r = s.get(API, params=params, timeout=TIMEOUT)
            ct = (r.headers.get("content-type") or "").lower()
            if r.status_code == 200:
                if "json" not in ct:
                    # The REST route always answers JSON.  A 200 carrying HTML
                    # is an edge/WAF interstitial, i.e. transient — not the
                    # SPA-shell-on-any-path trap (that is the *.html page,
                    # which this script never calls).
                    last = "200-non-json"
                else:
                    try:
                        return r.json()
                    except ValueError:
                        last = "bad-json"
            elif r.status_code == 404:
                raise Dead("404")          # see the DEAD TENANT note
            elif r.status_code in (429, 418, 503):
                slow_down()
                last = "HTTP %d" % r.status_code
            else:
                # includes 500 (server fault on a route that exists) and 403
                # (edge decision) — both retryable, neither means "dead".
                last = "HTTP %d" % r.status_code
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(1.5 * (attempt + 1))
    raise Transient(last or "unknown")


# ---- field plumbing --------------------------------------------------------
def _named(group, key, name):
    """customFieldGroup.{key}[] entry whose nameCode.codeValue == name."""
    for f in (group or {}).get(key) or []:
        if isinstance(f, dict) and (f.get("nameCode") or {}).get(
                "codeValue") == name:
            return f
    return {}


_PERIOD = {"annually": "year", "annual": "year", "yearly": "year",
           "hourly": "hour", "monthly": "month", "weekly": "week",
           "daily": "day", "bi-weekly": "biweekly", "biweekly": "biweekly",
           "semi-monthly": "semimonthly"}

# Conservative work-mode inference.  ADP exposes NO remote facet on this API,
# so the only evidence is text.  Word-boundary anchored so a job in the town of
# Remote, Oregon, or a "Hybrid Vehicle Technician", is not flagged: those match
# \bremote\b / \bhybrid\b too, which is exactly why hybrid is tested first and
# why nothing is emitted at all unless one of these fires.  A wrong remote flag
# is worse than none.
_HYBRID_RX = re.compile(r"\bhybrid\b", re.I)
_REMOTE_RX = re.compile(r"\bremote\b|\bwork from home\b|\bwfh\b|"
                        r"\btele[\s-]?commut", re.I)
_STATE_RX = re.compile(r"^[A-Za-z]{2,3}$")


def _work_mode(text):
    if _HYBRID_RX.search(text):
        return "Hybrid", False
    if _REMOTE_RX.search(text):
        return "Remote", True
    return "", False


def _locations(req):
    """-> (joined string, city, state, country)."""
    names, city, state, country = [], "", "", ""
    for loc in req.get("requisitionLocations") or []:
        if not isinstance(loc, dict):
            continue
        nm = ((loc.get("nameCode") or {}).get("shortName") or "").strip()
        ad = loc.get("address") or {}
        c = (ad.get("cityName") or "").strip()
        st = ((ad.get("countrySubdivisionLevel1") or {}).get("codeValue")
              or "").strip()
        if not nm:
            nm = ", ".join(x for x in (c, st) if x)
        if nm:
            names.append(nm)
        if not city and c:
            city = c
        if not state and st:
            state = st
        if not country and nm:
            # shortName is " City, City, ST, US" / " US - Remote, US" — the
            # trailing token is the country when it looks like a country code.
            tail = nm.rsplit(",", 1)[-1].strip()
            if _STATE_RX.match(tail):
                country = tail.upper()
    return "; ".join(dict.fromkeys(names)), city, state, country


def _amount(node):
    v = (node or {}).get("amountValue")
    return v if isinstance(v, (int, float)) else ""


def rows_for(cid, company, reqs):
    out = []
    for req in reqs:
        if not isinstance(req, dict):
            continue
        title = (req.get("requisitionTitle") or "").strip()
        if not title:
            continue
        grp = req.get("customFieldGroup") or {}
        jid = (_named(grp, "stringFields", "ExternalJobID").get("stringValue")
               or "").strip()
        item = (req.get("itemID") or "").strip()
        # Only ExternalJobID makes a working /recruitment.html link; without it
        # the posting has no addressable URL, so fall back to the board URL and
        # keep the row keyed on itemID rather than emitting a broken deep link.
        if jid:
            apply_url = "%s?cid=%s&jobId=%s" % (PAGE_URL, cid, jid)
        else:
            apply_url = "%s?cid=%s" % (PAGE_URL, cid)
        raw_type = (req.get("workLevelCode") or {}).get("shortName") or ""
        loc, city, state, country = _locations(req)
        mode, remote = _work_mode(loc + " " + title)
        pay = req.get("payGradeRange") or {}
        smin = _amount(pay.get("minimumRate"))
        smax = _amount(pay.get("maximumRate"))
        cur = ((pay.get("minimumRate") or {}).get("currencyCode")
               or (pay.get("maximumRate") or {}).get("currencyCode") or "")
        per = _PERIOD.get(
            (_named(grp, "codeFields", "SalaryType").get("shortName")
             or "").strip().lower(), "")
        posted = (req.get("postDate") or "")[:10]
        if not re.match(r"^\d{4}-\d{2}-\d{2}$", posted):
            posted = cc.ymd(iso=req.get("postDate")) or ""
        out.append(cc.row(
            source=SOURCE, source_type="ats", company=company, title=title,
            # classify_any, never classify: workLevelCode is "Full Time" on the
            # large majority of ADP postings and cc.classify returns None for
            # those, which would drop almost the whole platform.
            employment_type=cc.classify_any(raw_type, title),
            work_mode=mode, location=loc, city=city, state=state,
            country=country, remote=remote,
            salary_min=smin, salary_max=smax,
            salary_currency=cur if (smin != "" or smax != "") else "",
            salary_period=per if (smin != "" or smax != "") else "",
            posted=posted, apply_url=apply_url, job_id=jid or item,
            board=cid, board_url="%s?cid=%s" % (PAGE_URL, cid)))
    return out


def fetch_board(cid):
    """-> list of raw jobRequisition dicts.  Raises Dead / Transient."""
    s = cc.session()
    try:
        seen, reqs, skip = set(), [], 0
        for _ in range(MAX_PAGES):
            d = gj(s, {"cid": cid, "locale": "en_US",
                       "$top": PAGE, "$skip": skip})
            batch = d.get("jobRequisitions")
            if not isinstance(batch, list) or not batch:
                break
            for req in batch:
                key = (req or {}).get("itemID") or json.dumps(req,
                                                              sort_keys=True)
                if key in seen:
                    continue
                seen.add(key)
                reqs.append(req)
            total = (d.get("meta") or {}).get("totalNumber")
            skip += PAGE          # fixed stride — see the PAGING note
            if isinstance(total, int) and skip >= total:
                break
        return reqs
    finally:
        s.close()


# ---- company map -----------------------------------------------------------
def build_company_map():
    """{cid: company} mined from the corpus' own ADP apply URLs.

    Read-only over all_jobs_master.csv; the result is cached so this ~2 minute
    scan happens once.  Absent or unreadable, rows fall back to the cid, which
    is ugly in a workbook but never wrong.
    """
    if os.path.exists(COMPANY_MAP):
        try:
            with open(COMPANY_MAP, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, ValueError):
            pass
    if not os.path.exists(MASTER):
        print("  no all_jobs_master.csv — company falls back to cid",
              flush=True)
        return {}
    print("  building %s from the corpus (one-time scan)..."
          % os.path.basename(COMPANY_MAP), flush=True)
    csv.field_size_limit(sys.maxsize // 4)
    rx = re.compile(r"[?&]cid=([0-9a-fA-F-]{36})")
    tally = {}
    try:
        with open(MASTER, encoding="utf-8", errors="replace") as f:
            rd = csv.reader(f)
            hdr = next(rd)
            iu, ic = hdr.index("apply_url"), hdr.index("company")
            for row in rd:
                if len(row) <= iu:
                    continue
                u = row[iu]
                if "workforcenow.adp.com" not in u:
                    continue
                m = rx.search(u)
                comp = (row[ic] or "").strip()
                if not m or not comp:
                    continue
                tally.setdefault(m.group(1).lower(), {})
                d = tally[m.group(1).lower()]
                d[comp] = d.get(comp, 0) + 1
    except (OSError, ValueError, StopIteration) as e:
        print("  corpus scan failed (%s) — company falls back to cid" % e,
              flush=True)
        return {}
    out = {k: max(v.items(), key=lambda kv: (kv[1], kv[0]))[0]
           for k, v in tally.items()}
    tmp = COMPANY_MAP + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=0, sort_keys=True)
    os.replace(tmp, COMPANY_MAP)
    print("  company map: %d cids" % len(out), flush=True)
    return out


# ---- driver ----------------------------------------------------------------
def work(cid, company, writer):
    with _gate:
        try:
            reqs = fetch_board(cid)
        except Dead:
            with _lock:
                _state[cid] = "dead"
                _counts["dead"] += 1
            return
        except Exception as e:                       # noqa: BLE001
            # Left ABSENT from the state file on purpose: pending, not done.
            # The recurring one is ADP's own backend giving up:
            #   {"status":"FAILURE","cause":"...API-MOBILE-JOB-LISTING-V1-11111
            #    ...java.net.ConnectException: Connection refused ...",
            #    "serverTimeTaken":"20008"}
            # served as HTTP 500 after a 20-second stall.  It is a shard-level
            # fault on ADP's side, hits a few percent of cids on any given
            # pass, and clears on a later run — which is exactly why it must
            # stay pending and must never be booked as a dead or empty board.
            with _lock:
                _counts["failed"] += 1
            print("  ERR %s: %s %s" % (cid, type(e).__name__, str(e)[:80]),
                  flush=True)
            return
        # Rows are buffered and written in one locked burst only after the
        # WHOLE board succeeded, so a mid-board failure cannot leave a partial
        # board on disk that a retry would then duplicate.
        rows = rows_for(cid, company, reqs)
        with _lock:
            for r in rows:
                writer.write(r)
            _state[cid] = "done"
            _counts["rows"] += len(rows)
            if reqs:
                _counts["live"] += 1
            else:
                _counts["empty"] += 1


def main():
    global _state
    _state = cc.load_state(STATE)
    companies = build_company_map()
    with open(TENANTS, newline="", encoding="utf-8") as f:
        tenants = [r["tenant"].strip() for r in csv.DictReader(f)
                   if (r.get("tenant") or "").strip()]
    todo = [t for t in tenants if _state.get(t) not in ("done", "dead")]
    random.shuffle(todo)
    resolved = len(tenants) - len(todo)
    if LIMIT:
        todo = todo[:LIMIT]
    print("adp: %d tenants, %d already resolved, %d to fetch"
          % (len(tenants), resolved, len(todo)), flush=True)

    writer = cc.Writer(OUT)
    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    done = 0
    try:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(work, t, companies.get(t.lower(), t), writer)
                    for t in todo]
            for _ in cf.as_completed(futs):
                done += 1
                if done % 50 == 0:
                    with _lock:
                        cc.save_state(STATE, _state)
                    print("  %d/%d  %s" % (done, len(todo), _counts),
                          flush=True)
                if deadline and time.time() > deadline:
                    for fu in futs:
                        fu.cancel()
                    break
    finally:
        cc.save_state(STATE, _state)
        writer.close()
    if _n429[0]:
        print("  rate-limit backoffs: %d" % _n429[0], flush=True)
    print("DONE adp: %s -> %s" % (_counts, OUT), flush=True)
    if _counts["failed"]:
        print("  %d tenants left PENDING (transient) — re-run to retry"
              % _counts["failed"], flush=True)


if __name__ == "__main__":
    main()
