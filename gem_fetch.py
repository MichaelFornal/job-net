#!/usr/bin/env python3
"""
gem_fetch.py — Gem job-board (jobs.gem.com) multi-tenant fetcher.

The briefing recorded Gem as "302 -> login".  That is true of the ADMIN API and
false of the public board: the board app talks to an UNAUTHENTICATED public
GraphQL endpoint, and it is wide open.

ROUTE (reverse-engineered 2026-07-28 from static.gem.com/scripts/jobBoards.*.js)
-------------------------------------------------------------------------------
    POST https://jobs.gem.com/api/public/graphql
    {"operationName":"JobBoardList","variables":{"boardId":"<tenant>"},
     "query":"query JobBoardList($boardId: String!) { ... }"}

boardId is the vanity path — the same {tenant} segment as
https://jobs.gem.com/{tenant}/{extId} in the corpus.  The app splits its
Apollo links into an APP client (/api/graphql, cookie-authenticated) and a
PUBLIC one (/api/public/graphql, no auth at all); this uses the public one.

Two fields the board app's own list query does NOT request are added here and
verified to resolve: firstPublishedTsSec and startDateTs.  That is the whole
reason no N+1 is needed — real posting dates come back with the list, so this
pipe costs exactly ONE request per tenant and every row carries a real date
instead of a first-seen guess.

Row fields available from the list query: title, extId, locations
[name/city/isoCountry/isRemote], job.locationType (REMOTE|HYBRID|ONSITE),
job.employmentType (FULL_TIME|PART_TIME|CONTRACT|INTERN|...),
job.department.name, job.requisitionId, isUnlistedExternally.
compensationHtml is deliberately NOT requested: it is marketing prose, not a
structured range, and guessing numbers out of it would fabricate salary data.

DEAD-TENANT DISCRIMINATOR  (rule 2 — never trust a status code alone)
---------------------------------------------------------------------
jobs.gem.com is the "HTTP 200 with a ~4KB HTML SPA shell on ANY unrecognised
path" platform.  Measured:

    GET /fetch                                  -> 200, 4,133 B SPA shell
    GET /fetch/am9icG9zdDrqHw0juiGPue4XuchGZYOF -> 200, 4,133 B SPA shell
                                                   (byte-identical: the shell
                                                    is served for the board and
                                                    for every posting alike)

so no HTML route can tell a live posting from a removed one, and any
"200 == good" check passes on everything.  (A bogus tenant does 404 the HTML,
but that only catches tenants that never existed, not retired boards.)

The GraphQL answer is the real discriminator, and it is unambiguous because
the query asks for the BOARD as well as the postings:

    live board                -> jobBoardExternal: {...},  jobPostings: [...]
    live board, no openings   -> jobBoardExternal: {...},  jobPostings: []
    tenant gone / never was   -> jobBoardExternal: null,   jobPostings: []
                                 (still HTTP 200 — measured on
                                  boardId="zzz-not-a-real-board-xyz9")

So an empty board and a dead board are distinguished by jobBoardExternal, not
by the posting count, and neither is ever confused with a transient failure:

    HTTP 200 + parsed data       -> classify as above
    HTTP 429 / 5xx / non-JSON /
    timeout / GraphQL "errors"   -> TRANSIENT: tenant left ABSENT from the
                                    state file so a re-run retries it

Output: contract_norm/gem.jsonl   State: gem_fetch_state.json
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
TENANTS = os.path.join(BASE, "tenants", "gem.csv")
OUT = os.path.join(cc.NORM_DIR, "gem.jsonl")
STATE = os.path.join(BASE, "gem_fetch_state.json")
LEDGER = os.path.join(BASE, "gem_posted_ledger.json")

SOURCE = "gem"
HOST = "https://jobs.gem.com"
GQL = HOST + "/api/public/graphql"

QUERY = """query JobBoardList($boardId: String!) {
  oatsExternalJobPostings(boardId: $boardId) {
    jobPostings {
      id
      extId
      title
      startDateTs
      firstPublishedTsSec
      isUnlistedExternally
      locations { name city isoCountry isRemote }
      job {
        locationType
        employmentType
        requisitionId
        teamDisplayName
        department { name }
      }
    }
  }
  jobBoardExternal(vanityUrlPath: $boardId) {
    id
    teamDisplayName
    pageTitle
  }
}"""

RPS = float(os.environ.get("GEM_RPS") or 4.0)
CONCURRENCY = int(os.environ.get("GEM_CONCURRENCY") or 8)
WORKERS = CONCURRENCY
TIMEOUT = 45
TRIES = 5
BUDGET_MIN = int(os.environ.get("GEM_BUDGET_MIN") or 0)
LIMIT = int(os.environ.get("GEM_LIMIT") or 0)
REFRESH = os.environ.get("GEM_REFRESH") == "1"

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
_stats = {"live": 0, "empty": 0, "dead": 0, "transient": 0, "rows": 0}

_WS = re.compile(r"\s+")


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


def gql(s, tenant):
    """Throttled POST -> parsed `data` dict.  Anything that is not a clean
    200-with-JSON-and-no-errors RAISES Transient: the shell/edge failures on
    this host all look like HTTP 200, so "it parsed" is the only success
    signal that means anything."""
    payload = {"operationName": "JobBoardList",
               "variables": {"boardId": tenant}, "query": QUERY}
    last = None
    for attempt in range(TRIES):
        throttle()
        try:
            r = s.post(GQL, json=payload, timeout=TIMEOUT,
                       headers={"Accept": "application/json",
                                "Content-Type": "application/json",
                                "Origin": HOST,
                                "Referer": "%s/%s" % (HOST, tenant)})
            code = r.status_code
            if code == 200:
                ct = (r.headers.get("content-type") or "").lower()
                if "json" not in ct:
                    # The SPA shell (text/html, ~4KB) served in place of the
                    # API answer — an edge/route failure, not a board answer.
                    last = "non-json 200 (%s)" % ct[:40]
                else:
                    try:
                        d = r.json()
                    except ValueError:
                        d = None
                    if isinstance(d, dict):
                        if d.get("errors"):
                            last = "graphql: %s" % str(d["errors"])[:120]
                        elif isinstance(d.get("data"), dict):
                            return d["data"]
                        else:
                            last = "no data key"
                    else:
                        last = "bad json"
            elif code in (429, 418):
                slow_down()
                last = "HTTP %d" % code
            else:
                last = "HTTP %d" % code
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    raise Transient("%s failed after %d tries: %s" % (tenant, TRIES, last))


def sq(v):
    return _WS.sub(" ", html.unescape(str(v or ""))).strip()


_MODE = {"REMOTE": "Remote", "HYBRID": "Hybrid", "ONSITE": "On-site",
         "ON_SITE": "On-site", "IN_OFFICE": "On-site"}


def ledger_posted(key, real):
    """First date ever seen for this posting wins, forever.  `real` is the
    Gem-supplied publish date when it has one."""
    with _lock:
        rec = _ledger.get(key)
        if rec:
            return rec.get("posted") or ""
        _ledger[key] = {"posted": real or TODAY,
                        "precision": "firstPublished" if real else "first_seen",
                        "first_seen": TODAY}
        return _ledger[key]["posted"]


def rows_from(tenant, data):
    board = data.get("jobBoardExternal") or {}
    company = sq(board.get("teamDisplayName"))
    if not company:
        company = re.sub(r"\s*careers?\s*$", "", sq(board.get("pageTitle")),
                         flags=re.I).strip()
    postings = ((data.get("oatsExternalJobPostings") or {})
                .get("jobPostings") or [])
    out = []
    for p in postings:
        if not isinstance(p, dict):
            continue
        if p.get("isUnlistedExternally"):
            continue
        title = sq(p.get("title"))
        ext = sq(p.get("extId"))
        if not title or not ext:
            continue
        job = p.get("job") or {}
        locs = [x for x in (p.get("locations") or []) if isinstance(x, dict)]
        names = [sq(x.get("name")) for x in locs if sq(x.get("name"))]
        cities = [sq(x.get("city")) for x in locs if sq(x.get("city"))]
        countries = [sq(x.get("isoCountry")) for x in locs
                     if sq(x.get("isoCountry"))]
        locstr = "; ".join(dict.fromkeys(names)) or "; ".join(
            dict.fromkeys(cities))
        # locationType is the structured facet; only fall back to text when it
        # is absent, and never invent a remote flag from a city name alone.
        mode = _MODE.get((job.get("locationType") or "").upper(), "")
        if not mode:
            if locs and all(x.get("isRemote") for x in locs):
                mode = "Remote"
            else:
                mode = cc.work_mode(text=locstr)
        real = cc.ymd(ts=p.get("firstPublishedTsSec")) or cc.ymd(
            ts=p.get("startDateTs"))
        out.append(cc.row(
            source=SOURCE, source_type="ats",
            company=company or sq(job.get("teamDisplayName")) or tenant,
            title=title,
            employment_type=cc.classify_any(job.get("employmentType"), title),
            work_mode=mode, location=locstr,
            city=cities[0] if len(dict.fromkeys(cities)) == 1 else "",
            country=countries[0] if len(dict.fromkeys(countries)) == 1 else "",
            remote=(mode == "Remote"),
            posted=ledger_posted("%s:%s" % (tenant, ext), real),
            apply_url="%s/%s/%s" % (HOST, tenant, ext),
            job_id=ext, board=tenant,
            board_url="%s/%s" % (HOST, tenant)))
    return out


def work(w, tenant):
    with _gate:
        s = cc.session()
        try:
            data = gql(s, tenant)
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
        postings = ((data.get("oatsExternalJobPostings") or {})
                    .get("jobPostings") or [])
        alive = data.get("jobBoardExternal") is not None
        if not alive and not postings:
            with _lock:
                _stats["dead"] += 1
                _state[tenant] = {"status": "dead", "why": "no_such_board",
                                  "at": TODAY}
            return
        rows = rows_from(tenant, data)
        with _lock:
            for r in rows:
                w.write(r)
            _stats["rows"] += len(rows)
            _stats["live"] += 1
            if not rows:
                _stats["empty"] += 1
            _state[tenant] = {"status": "live", "jobs": len(rows),
                              "postings_seen": len(postings),
                              "board_record": alive, "at": TODAY}


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
    print("gem: %d tenants, %d to fetch (%d already recorded)"
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
                if done % 20 == 0:
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
    print("DONE gem: %s  (429 backoffs: %d)%s"
          % (_stats, _n429[0],
             "  [BUDGET REACHED — rest still pending]" if stopped else ""),
          flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
