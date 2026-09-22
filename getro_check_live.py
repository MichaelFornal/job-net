#!/usr/bin/env python3
"""Liveness-check every Getro board and write the final CSV with a status column.

LIVENESS IS THE API, NOT THE BOARD'S FRONT DOOR (rewritten 2026-09-07).
This used to GET `https://<board host>/jobs` and call the board live when the
HTML carried `__NEXT_DATA__`.  That gates on a host the jobs never come from:
`getro_fetch_jobs.py:64` reads
`https://api.getro.com/api/v2/collections/{id}/search/jobs`, and a board's
vanity domain can be parked, slow, Cloudflare-fronted or simply mid-DNS-blip
while its collection serves perfectly.

What that cost, measured on the 2026-09-07 run: the front-end sweep returned
138 `dead_URLError` (a connection/DNS class, not a 404) and the live census
fell 754 -> 650 in one run after eleven consecutive runs at 754-763.  The 104
boards it dropped were worth 110,529 raw getro rows, which is why the master
grew only 13,275 against 153,955 new keys.  Probing the API for all 177
non-live boards, 161 answered 200 with jobs behind them.  The casualties
included Coinbase, Alpine Investors, Atomic, Index Ventures and Founders Fund
-- none of them dead.

That was never a one-run blip either: the census sat near 755/844 for eleven
runs, so ~85-90 boards were being excluded EVERY run on the same wrong signal.

Only `getro_fetch_jobs.py` reads the `status` column (checked 2026-09-07;
startup_boards, aivc_orgs, ai_lab_orgs, getro_build_sheet and coverage_report
all read this CSV but none filters on status), so widening `live` reaches
exactly the consumer it should.
"""
import json, csv, os, sys, warnings, threading, time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import requests
warnings.filterwarnings("ignore")

# argv[1] overrides; default to the file the enumerator actually produced
SRC = sys.argv[1] if len(sys.argv) > 1 else (
    "getro_all_collections.jsonl"
    if os.path.exists("getro_all_collections.jsonl")
    else "getro_collections.jsonl")
OUT = "/Users/michaelfornal/Documents/jb/getro_job_boards.csv"
URLS = "/Users/michaelfornal/Documents/jb/getro_boards_urls.txt"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
      "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")
API = "https://api.getro.com/api/v2/collections/{cid}/search/jobs"

# THE THREE COLLECTIONS THAT MUST NEVER BE MARKED LIVE.
# getro_fetch_pdl.py owns them by hardcoded CID: 12896 is People Data Labs
# (~970k jobs, far past the 200k result window, which is the entire reason it
# has a windowed fetcher of its own), and it also stamps 8012 (Invisible) and
# 13655 (Advisors) done-with-zero.  All three answer the API, so an API-based
# check flips them live and `getro_fetch_jobs` -- which runs FIRST -- would try
# to paginate PDL through the exact path the dedicated fetcher exists to avoid.
# The old front-end probe excluded them only by accident (all three happened to
# be dead_URLError); this makes it deliberate.
PDL_FETCHER_CIDS = {"12896", "8012", "13655"}

# 6 workers, not 12.  The old sweep fanned out across 845 DIFFERENT hosts; every
# request now lands on api.getro.com, so the old concurrency would manufacture a
# fresh false-dead class out of 429s -- the [[ratchet-throttle-trap]] shape.
WORKERS = 6
TRIES = 3
BACKOFFS = (2, 5)
TIMEOUT = 25

recs = {}
for l in open(SRC):
    l = l.strip()
    if not l:
        continue
    r = json.loads(l)
    recs[str(r["id"])] = r
boards = [r for r in recs.values() if r.get("url")]


def host_type(u):
    return "getro_subdomain" if u.lower().endswith(".getro.com") else "custom_domain"


_local = threading.local()


def session():
    s = getattr(_local, "s", None)
    if s is None:
        s = requests.Session()
        s.headers.update({"User-Agent": UA, "Accept": "application/json"})
        _local.s = s
    return s


lock = threading.Lock()
done = [0]


def check(r):
    u = r["url"].strip().rstrip("/")
    r["_host"] = u
    r["_board_url"] = "https://" + u
    r["_jobs_url"] = "https://" + u + "/jobs"
    r["_host_type"] = host_type(u)
    r["_api_jobs"] = ""
    cid = str(r.get("id", ""))

    if cid in PDL_FETCHER_CIDS:
        r["_status"] = "pdl_fetcher"
    else:
        status = "dead"
        for attempt in range(TRIES):
            try:
                resp = session().post(API.format(cid=cid),
                                      json={"hits_per_page": 1, "page": 0},
                                      timeout=TIMEOUT)
                if resp.status_code == 200:
                    # count==0 is still LIVE: the collection is fetchable and
                    # may fill tomorrow.  The old check said the same thing --
                    # a rendered but empty board was live -- and an empty board
                    # costs getro_fetch_jobs exactly one request.
                    r["_api_jobs"] = (resp.json().get("results", {})
                                      .get("count", 0))
                    status = "live"
                    break
                status = f"dead_http_{resp.status_code}"
                # 4xx here is a verdict, not congestion: only retry the codes
                # that can change on their own.
                if resp.status_code not in (429, 500, 502, 503, 504):
                    break
            except Exception as e:
                status = "dead_" + type(e).__name__
            if attempt < TRIES - 1:
                time.sleep(BACKOFFS[min(attempt, len(BACKOFFS) - 1)])
        r["_status"] = status

    with lock:
        done[0] += 1
        if done[0] % 100 == 0:
            print(f"  checked {done[0]}/{len(boards)}", flush=True)
    return r


t0 = time.time()
with ThreadPoolExecutor(max_workers=WORKERS) as ex:
    list(ex.map(check, boards))

# SETTLE PASS.  Even at 6 workers the parallel sweep earns a few 429s, and a
# throttled board recorded as dead is precisely the bug this rewrite exists to
# kill -- it just moves the false-dead class from DNS to rate limiting.
# Measured 2026-09-07: the first API sweep left 6 boards on dead_http_429 and
# ALL SIX answered 200 on a serial re-probe (toledo-region alone was 3,709
# jobs).  Serial, unhurried, and only over the retryable statuses.
RETRYABLE = ("dead_http_429", "dead_http_500", "dead_http_502",
             "dead_http_503", "dead_http_504", "dead_ConnectionError",
             "dead_ReadTimeout", "dead_Timeout", "dead_ConnectTimeout")
stragglers = [r for r in boards if r["_status"] in RETRYABLE]
if stragglers:
    print(f"settle pass: re-probing {len(stragglers)} throttled/transient "
          f"board(s) serially", flush=True)
    for r in stragglers:
        for wait in (0, 6, 15):
            if wait:
                time.sleep(wait)
            try:
                resp = session().post(API.format(cid=str(r.get("id", ""))),
                                      json={"hits_per_page": 1, "page": 0},
                                      timeout=30)
                if resp.status_code == 200:
                    r["_api_jobs"] = resp.json().get("results", {}).get("count", 0)
                    r["_status"] = "live"
                    break
                r["_status"] = f"dead_http_{resp.status_code}"
            except Exception as e:
                r["_status"] = "dead_" + type(e).__name__
    print(f"  settled: {sum(1 for r in stragglers if r['_status'] == 'live')}"
          f"/{len(stragglers)} came back live", flush=True)
print(f"liveness done in {time.time()-t0:.0f}s", flush=True)

boards.sort(key=lambda r: (r.get("name", "").lower(), r["_host"]))
# api_jobs is APPENDED, never inserted: every other consumer reads this file
# with DictReader by column name, so a trailing column is invisible to them.
cols = ["name", "board_url", "jobs_url", "status", "host_type", "custom_domain",
        "kind", "has_job_board", "features", "tabs_order", "indexable",
        "connect_access", "created_at", "collection_id", "slug", "api_jobs"]
with open(OUT, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f)
    w.writerow(cols)
    for r in boards:
        w.writerow([r.get("name", ""), r["_board_url"], r["_jobs_url"],
                    r["_status"], r["_host_type"], r.get("domain", ""),
                    r.get("kind", ""), r.get("has_job_board", ""),
                    r.get("features", ""), r.get("tabs_order", ""),
                    r.get("index_job_pages", ""), r.get("connect_has_access", ""),
                    r.get("kind_setup_at", ""), r.get("id", ""),
                    r.get("slug", ""), r["_api_jobs"]])
# scraper-ready: only LIVE boards.  NOTE these are front-end URLs and liveness
# is now an API verdict, so a board here can be API-live with an unreachable
# front door.  Harmless: getro_scraper.py is a standalone tool and is wired
# into no chain (checked 2026-09-07).
with open(URLS, "w", encoding="utf-8") as f:
    f.write("# Live Getro job boards (jobs page). Feed to getro_scraper.py --boards\n")
    for r in boards:
        if r["_status"] == "live":
            f.write(r["_jobs_url"] + "\n")

c = Counter(r["_status"] for r in boards)
simple = Counter("live" if r["_status"] == "live" else "dead" for r in boards)
known = sum(int(r["_api_jobs"] or 0) for r in boards if r["_status"] == "live")
print("STATUS breakdown:", dict(c))
print("SIMPLE:", dict(simple))
print(f"jobs reported by live collections: {known:,}")
print(f"total boards={len(boards)}  live={simple['live']}  wrote {OUT}")
