#!/usr/bin/env python3
"""
paycom_fetch.py — Paycom career portals (tenants/paycom_path.csv).

THE ROUTE
---------
The tenant id is a 32-hex "clientkey".  The customer-facing board

    https://www.paycomonline.net/v4/ats/web.php/portal/{KEY}/career-page

is a Sprawl (micro-frontend) SPA.  It is still the mandatory first hop, because
it is where the anonymous session token is minted: the page embeds

    var configsFromHost = {"sessionJWT":"<RS256 JWT>", "libConfig":"{...}"}

The JWT is board-scoped (its `clientcode` claim is the tenant), lasts ~2h, and
is the ONLY credential the JSON API accepts — there is no key-in-querystring
form.  `libConfig.atsPortalMantleServiceUrl` names the regional API host and is
read from the page rather than hardcoded, because Paycom runs more than one
(us-cent and us-west are both present in this corpus).

    POST {api}/api/ats/job-posting-previews/search      Authorization: <jwt>
         {"skip":0,"take":N,"filtersForQuery":{...}}
      -> {"jobPostingPreviews":[{jobId,jobTitle,positionType,remoteType,
                                 locations,description,postedOn,isHotJob}],
          "jobPostingPreviewsCount": <board total>}

    GET  {api}/api/ats/company-name                     -> {"companyName": ...}
    GET  {api}/api/ats/job-postings/{jobId}             -> the posting detail

The exact request body was lifted from the bundle's own builder (`skip`, `take`,
`filtersForQuery` with all eight filter arrays present); anything else — {},
{"page":1,...} — is rejected 422 "Invalid parameters provided".  `take` is NOT
capped: take=1000 returns a 201-posting board whole, so a board is one request.

DEAD TENANT vs LIVE  (rule 2 — the expensive one)
-------------------------------------------------
Paycom is the platform the brief warns about: **HTTP 200 on any unrecognised
key**.  Measured 2026-07-29 against four synthetic keys (all-zero, DEADBEEF...,
the string "abc", and a real key with its last hex digit flipped) — every one
returned HTTP **200**, so the status code carries no information whatsoever.

  DEAD  -> 200 with a 132-byte body that is one div:
           "Job board does not exist or is unavailable at this time."
           and, decisively, **no `sessionJWT` in the body**.
  LIVE  -> 200 with the ~6.3KB SPA shell that DOES contain "sessionJWT":"...".

So the discriminator is `sessionJWT` present, not the status, not the length
(a themed shell varies in size).  No JWT and the small dead-board div => dead,
recorded, never retried.  No JWT and anything else (a WAF page, a truncated
body, a 5xx, a timeout) => transient, tenant left PENDING.

Real 429s from either host trip a shared 20s pause plus a permanent interval
increase, exactly as in ats_fetch.py.

POSTED DATES + THE LEDGER
-------------------------
The list payload's `postedOn` is a per-board display setting and is empty on
~99% of postings (3 of 441 sampled were populated).  The real date lives on the
detail as `googleJobJson.datePosted`, together with a schema.org address
(addressLocality / addressRegion / addressCountry) far cleaner than the
"Site Name - City, ST 12345; ..." blob the list carries, plus `salaryRange`.

Undated rows never age out of all_jobs_build.py's cutoff, so the detail is
fetched — but only ONCE per posting.  paycom_posted_ledger.json caches
posted/city/state/country/salary per "{tenant}:{jobId}" forever, so a re-run
pays for new postings only.  THIS LEDGER MUST NEVER BE ROTATED OR DELETED:
it is the only record of when a posting was first seen, and rotating it
re-dates the entire platform to the day of the rotation.  Set
PAYCOM_NO_DETAIL=1 to skip the detail pass entirely (rows then carry
first-seen dates and unparsed locations).

Resumable via paycom_fetch_state.json (per tenant), one row per posting into
contract_norm/paycom.jsonl.
"""
import concurrent.futures as cf
import csv
import json
import os
import random
import re
import threading
import time
from datetime import datetime, timezone

import requests

import contract_common as cc

BASE = os.path.dirname(os.path.abspath(__file__))
TENANTS = os.path.join(BASE, "tenants", "paycom_path.csv")
OUT = os.path.join(cc.NORM_DIR, "paycom.jsonl")
STATE = os.path.join(BASE, "paycom_fetch_state.json")
LEDGER = os.path.join(BASE, "paycom_posted_ledger.json")

PORTAL = "https://www.paycomonline.net/v4/ats/web.php/portal/%s/career-page"
JOB_URL = "https://www.paycomonline.net/v4/ats/web.php/portal/%s/jobs/%s"
API_FALLBACK = "https://portal-applicant-tracking.us-cent.paycomonline.net/"

SOURCE = "paycom"
TAKE = 500                # server imposes no cap; 500 covers every board seen
MAX_PAGES = 40
WORKERS = int(os.environ.get("PAYCOM_WORKERS") or 8)
TIMEOUT = 40
TRIES = 5
NO_DETAIL = os.environ.get("PAYCOM_NO_DETAIL") == "1"
BUDGET_MIN = int(os.environ.get("PAYCOM_BUDGET_MIN") or 0)
LIMIT = int(os.environ.get("PAYCOM_LIMIT") or 0)
TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# Two hosts, two budgets: the shell is one hit per board off a PHP front end,
# the API is up to one hit per POSTING off a node service that measured 11.7
# req/s at 6-way concurrency without a single non-200.
RPS = {"www": 6.0, "api": 10.0}
CONCURRENCY = {"www": 8, "api": 8}

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = {k: 0.0 for k in RPS}
_interval = {k: 1.0 / v for k, v in RPS.items()}
_pause_until = {k: 0.0 for k in RPS}
_n429 = {k: 0 for k in RPS}
_gate = {k: threading.Semaphore(v) for k, v in CONCURRENCY.items()}
_state = {}
_ledger = {}
_counts = {"rows": 0, "live": 0, "dead": 0, "empty": 0, "failed": 0,
           "detail_hit": 0, "detail_cached": 0, "detail_miss": 0}

# The dead-board body, verbatim from the edge.
_DEAD_RX = re.compile(r"Job board does not exist or is unavailable", re.I)
_JWT_RX = re.compile(r'"sessionJWT"\s*:\s*"([^"]+)"')
_APIHOST_RX = re.compile(r'atsPortalMantleServiceUrl\\?"\s*:\s*\\?"(.*?)\\?"')

FILTERS = {"distanceFrom": 0, "workEnvironments": [], "positionTypes": [],
           "educationLevels": [], "categories": [], "travelTypes": [],
           "shiftTypes": [], "otherFilters": [], "keywordSearchText": "",
           "location": "", "sortOption": ""}


class Transient(Exception):
    """Retryable — the tenant stays PENDING so a re-run picks it up."""


class Dead(Exception):
    """Confirmed non-existent board (200 + no sessionJWT + the dead div)."""


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
        _interval[host] = min(_interval[host] * 1.5, 0.5)
        _n429[host] += 1


def _req(s, host, method, url, **kw):
    """Throttled request -> Response, retried on transient failures only."""
    last = None
    for attempt in range(TRIES):
        throttle(host)
        with _gate[host]:
            try:
                r = s.request(method, url, timeout=TIMEOUT, **kw)
            except requests.RequestException as e:
                last = type(e).__name__
                r = None
        if r is not None:
            if r.status_code in (429, 418, 503):
                slow_down(host)
                last = "HTTP %d" % r.status_code
            else:
                return r
        time.sleep(1.5 * (attempt + 1))
    raise Transient(last or "unknown")


def open_board(s, key):
    """-> (jwt, api_base).  Raises Dead / Transient."""
    r = _req(s, "www", "GET", PORTAL % key)
    body = r.text or ""
    m = _JWT_RX.search(body)
    if m:
        api = API_FALLBACK
        am = _APIHOST_RX.search(body)
        if am:
            # libConfig is a JSON string inside JS, so the URL arrives as
            # https:\\/\\/host\\/ — every backslash is escaping, none is data.
            cand = re.sub(r"\\+", "", am.group(1))
            if cand.startswith("https://"):
                api = cand if cand.endswith("/") else cand + "/"
        return m.group(1), api
    if r.status_code == 200 and _DEAD_RX.search(body):
        raise Dead("no-such-board")
    # 200 without a JWT and without the dead marker is NOT a dead board: it is
    # a WAF page, a truncated response or a maintenance stub.  Retry it.
    raise Transient("no-jwt (HTTP %d, %dB)" % (r.status_code, len(body)))


def _api_json(s, api, jwt, path, method="GET", payload=None):
    h = {"Authorization": jwt, "Locale": "en-US",
         "Accept": "application/json"}
    kw = {"headers": h}
    if payload is not None:
        kw["json"] = payload
    r = _req(s, "api", method, api + path, **kw)
    if r.status_code == 404:
        return None                     # posting withdrawn between list+detail
    if r.status_code != 200:
        raise Transient("%s -> HTTP %d" % (path, r.status_code))
    try:
        return r.json()
    except ValueError:
        raise Transient("%s -> non-json" % path)


# ---- field plumbing --------------------------------------------------------
# Paycom DOES expose a structured remote facet, so nothing is inferred from
# free text here: an empty remoteType is left blank rather than guessed at.
# The observed vocabulary is "", "Fully Remote", "Hybrid", "Optional Work from
# Home"; an unrecognised value also yields blank rather than a wrong flag.
def _mode(remote_type):
    t = (remote_type or "").strip().lower()
    if not t:
        return "", False
    if "hybrid" in t:
        return "Hybrid", False
    if "optional" in t or "work from home" in t:
        # WFH permitted, not remote-only — never flag these remote.
        return "Hybrid", False
    if "remote" in t:
        return "Remote", True
    return "", False


# "PRN" is the healthcare shorthand for as-needed shift work.  cc.classify_any
# has no PRN token, so it would land on the Full-time default; expanding it to
# the words the shared classifier already knows keeps the label honest without
# forking the classifier.
def _raw_type(position_type):
    t = (position_type or "").strip()
    if re.fullmatch(r"prn", t, re.I):
        return "PRN per diem"
    return t


_LOC_RX = re.compile(
    r"([A-Za-z][A-Za-z .'\-]*?),\s*([A-Z]{2})(?:\s+\d{5}(?:-\d{4})?)?\s*$")


def _parse_loc(first):
    """'Site Name - Houston, TX 77043' -> ('Houston', 'TX', 'US')."""
    m = _LOC_RX.search((first or "").strip())
    if not m:
        return "", "", ""
    return m.group(1).strip(), m.group(2), "US"


_MONEY_RX = re.compile(r"\$?\s*([\d]{1,3}(?:,\d{3})*(?:\.\d+)?|\d+(?:\.\d+)?)")
_SAL_PERIOD = [(re.compile(r"hour", re.I), "hour"),
               (re.compile(r"/\s*year|annual|yearly", re.I), "year"),
               (re.compile(r"month", re.I), "month"),
               (re.compile(r"week", re.I), "week"),
               (re.compile(r"\bdai?ly\b|\bper day\b", re.I), "day"),
               (re.compile(r"salary", re.I), "year")]


def _parse_salary(raw):
    """'$16.90 - $17.00 Hourly' / '$52,500.00 Salary/year' -> min,max,cur,per.

    Returns blanks unless BOTH a number and a period are recognisable — a
    salary with the wrong period is worse than no salary at all.
    """
    s = (raw or "").strip()
    if not s:
        return "", "", "", ""
    per = ""
    for rx, lab in _SAL_PERIOD:
        if rx.search(s):
            per = lab
            break
    if not per:
        return "", "", "", ""
    nums = []
    for m in _MONEY_RX.finditer(s):
        try:
            nums.append(float(m.group(1).replace(",", "")))
        except ValueError:
            pass
    if not nums:
        return "", "", "", ""
    lo, hi = min(nums), max(nums)
    cur = "USD" if "$" in s else ""
    return lo, (hi if hi != lo else ""), cur, per


def _detail_facts(det):
    """Pull the durable facts out of a job-postings/{id} payload."""
    jp = (det or {}).get("jobPosting") or {}
    out = {}
    g = jp.get("googleJobJson")
    if isinstance(g, str) and g.strip():
        try:
            g = json.loads(g)
        except ValueError:
            g = {}
    if not isinstance(g, dict):
        g = {}
    d = (g.get("datePosted") or "").strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", d):
        out["posted"] = d
    addr = ((g.get("jobLocation") or {}).get("address") or {})
    if isinstance(addr, dict):
        out["city"] = str(addr.get("addressLocality") or "").strip()
        out["state"] = str(addr.get("addressRegion") or "").strip()
        ctry = str(addr.get("addressCountry") or "").strip().upper()
        out["country"] = "US" if ctry in ("USA", "US") else ctry
    if not out.get("city") and jp.get("city"):
        out["city"] = str(jp["city"]).strip()
    lo, hi, cur, per = _parse_salary(jp.get("salaryRange"))
    if lo != "":
        out.update(salary_min=lo, salary_max=hi, salary_currency=cur,
                   salary_period=per)
    if jp.get("remoteType"):
        out["remote_type"] = jp["remoteType"]
    if jp.get("positionType"):
        out["position_type"] = jp["positionType"]
    return out


def enrich(s, api, jwt, key, job):
    """Ledger-backed detail lookup.  Returns the cached/looked-up fact dict."""
    jid = job.get("jobId")
    lk = "%s:%s" % (key, jid)
    with _lock:
        cached = _ledger.get(lk)
    if cached and cached.get("precision") == "datePosted":
        with _lock:
            _counts["detail_cached"] += 1
        return cached
    facts = {}
    if not NO_DETAIL:
        try:
            det = _api_json(s, api, jwt, "api/ats/job-postings/%s" % jid)
        except Transient:
            det = None
        if det:
            facts = _detail_facts(det)
    rec = dict(cached or {})
    rec.update({k: v for k, v in facts.items() if v not in ("", None)})
    if facts.get("posted"):
        rec["precision"] = "datePosted"
        with _lock:
            _counts["detail_hit"] += 1
    else:
        # No real date: keep (or set) the first-seen floor, and leave the
        # precision short of "datePosted" so the next run tries again.
        rec.setdefault("first_seen", TODAY)
        rec.setdefault("posted", rec.get("first_seen") or TODAY)
        rec["precision"] = "first_seen"
        with _lock:
            _counts["detail_miss"] += 1
    rec.setdefault("first_seen", TODAY)
    with _lock:
        _ledger[lk] = rec
    return rec


def row_for(key, company, job, facts):
    jid = job.get("jobId")
    title = (job.get("jobTitle") or "").strip()
    locs = (job.get("locations") or "").strip()
    first = locs.split(";")[0].strip() if locs else ""
    city = facts.get("city") or ""
    state = facts.get("state") or ""
    country = facts.get("country") or ""
    if not city and not state:
        city, state, country = _parse_loc(first)
    mode, remote = _mode(facts.get("remote_type") or job.get("remoteType"))
    return cc.row(
        source=SOURCE, source_type="ats", company=company, title=title,
        # classify_any, never classify: positionType is "Full Time" on most
        # Paycom postings and cc.classify returns None for those.
        employment_type=cc.classify_any(
            _raw_type(facts.get("position_type") or job.get("positionType")),
            title),
        work_mode=mode, location=locs, city=city, state=state,
        country=country, remote=remote,
        salary_min=facts.get("salary_min", ""),
        salary_max=facts.get("salary_max", ""),
        salary_currency=facts.get("salary_currency", ""),
        salary_period=facts.get("salary_period", ""),
        posted=facts.get("posted") or (job.get("postedOn") or "")[:10],
        apply_url=JOB_URL % (key, jid), job_id=str(jid), board=key,
        board_url=PORTAL % key)


def fetch_board(key):
    """-> (company, [rows]).  Raises Dead / Transient."""
    s = cc.session()
    try:
        jwt, api = open_board(s, key)
        company = key
        try:
            cn = _api_json(s, api, jwt, "api/ats/company-name")
            if cn and (cn.get("companyName") or "").strip():
                company = cn["companyName"].strip()
        except Transient:
            pass                       # a missing display name is not fatal
        jobs, skip = [], 0
        for _ in range(MAX_PAGES):
            d = _api_json(s, api, jwt, "api/ats/job-posting-previews/search",
                          method="POST",
                          payload={"skip": skip, "take": TAKE,
                                   "filtersForQuery": FILTERS})
            batch = (d or {}).get("jobPostingPreviews")
            if not isinstance(batch, list) or not batch:
                break
            jobs.extend(batch)
            total = (d or {}).get("jobPostingPreviewsCount")
            skip += len(batch)
            if isinstance(total, int) and skip >= total:
                break
        seen, rows = set(), []
        for j in jobs:
            if not isinstance(j, dict) or not j.get("jobId"):
                continue
            if j["jobId"] in seen:
                continue
            seen.add(j["jobId"])
            if not (j.get("jobTitle") or "").strip():
                continue
            rows.append(row_for(key, company, j, enrich(s, api, jwt, key, j)))
        return company, rows
    finally:
        s.close()


def work(key, writer):
    try:
        _company, rows = fetch_board(key)
    except Dead:
        with _lock:
            _state[key] = "dead"
            _counts["dead"] += 1
        return
    except Exception as e:                           # noqa: BLE001
        with _lock:
            _counts["failed"] += 1
        print("  ERR %s: %s %s" % (key, type(e).__name__, str(e)[:70]),
              flush=True)
        return
    # Buffered until the board is complete: a mid-board failure must not leave
    # half a board on disk for the next run to append a second time.
    with _lock:
        for r in rows:
            writer.write(r)
        _state[key] = "done"
        _counts["rows"] += len(rows)
        _counts["live" if rows else "empty"] += 1


def main():
    global _state, _ledger
    _state = cc.load_state(STATE)
    _ledger = cc.load_state(LEDGER)
    with open(TENANTS, newline="", encoding="utf-8") as f:
        tenants = [r["tenant"].strip() for r in csv.DictReader(f)
                   if (r.get("tenant") or "").strip()]
    # The corpus stores the key upper-cased and so does every real posting
    # link; the mined CSV lower-cases it.  The portal accepts either, but the
    # emitted apply_url has to be the form the corpus holds or the tier-2
    # dedupe key (company|title|apply_url) will not collapse onto it.
    todo = [t.upper() for t in tenants
            if _state.get(t.upper()) not in ("done", "dead")]
    random.shuffle(todo)
    resolved = len(tenants) - len(todo)
    if LIMIT:
        todo = todo[:LIMIT]
    print("paycom: %d tenants, %d already resolved, %d to fetch (detail=%s)"
          % (len(tenants), resolved, len(todo),
             "off" if NO_DETAIL else "on"), flush=True)

    writer = cc.Writer(OUT)
    deadline = time.time() + BUDGET_MIN * 60 if BUDGET_MIN else None
    done = 0
    try:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = [ex.submit(work, k, writer) for k in todo]
            for _ in cf.as_completed(futs):
                done += 1
                if done % 25 == 0:
                    with _lock:
                        cc.save_state(STATE, _state)
                        cc.save_state(LEDGER, _ledger)
                    print("  %d/%d  %s" % (done, len(todo), _counts),
                          flush=True)
                if deadline and time.time() > deadline:
                    for fu in futs:
                        fu.cancel()
                    break
    finally:
        cc.save_state(STATE, _state)
        cc.save_state(LEDGER, _ledger)
        writer.close()
    if any(_n429.values()):
        print("  rate-limit backoffs: %s" % _n429, flush=True)
    print("DONE paycom: %s -> %s" % (_counts, OUT), flush=True)
    if _counts["failed"]:
        print("  %d tenants left PENDING (transient) — re-run to retry"
              % _counts["failed"], flush=True)


if __name__ == "__main__":
    main()
