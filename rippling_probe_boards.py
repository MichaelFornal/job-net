#!/usr/bin/env python3
"""
rippling_probe_boards.py — classify rippling_candidates.csv slugs against the
public Rippling ATS board API.

  GET https://api.rippling.com/platform/api/ats/v1/board/{slug}/jobs
      -> 200 + JSON array (the whole board; there is no pagination)
      -> 404 {"error_code":"RESOURCE_NOT_FOUND"} for a slug that is not a board

Deliberately NOT ats.rippling.com, which is the Cloudflare-fronted HTML host.

Four statuses, and the distinction matters:
  live   200 + a non-empty array
  empty  200 + []  — a REAL board that currently has no openings.  Roughly a
         third of live slugs land here and some refill later, so calling them
         dead would permanently drop hundreds of real boards.
  dead   404 — settled, never re-probed
  error  transient; re-probed on the next run

Resume state is rippling_probe_results.csv (the house pattern).  live/empty/
dead are kept as settled; only error rows are re-probed, so a second run is
nearly free.

Output rippling_job_boards.csv carries live AND empty boards — both are real,
and the fetcher costs one request either way.
"""
import csv
import os
import sys
import threading
import time
import concurrent.futures as cf

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
SMOKE = int(os.environ.get("SMOKE") or 0)
PFX = "smoke_" if SMOKE else ""

CAND_CSV = os.path.join(BASE, PFX + "rippling_candidates.csv")
RESULTS_CSV = os.path.join(BASE, PFX + "rippling_probe_results.csv")
BOARDS_CSV = os.path.join(BASE, PFX + "rippling_job_boards.csv")

API = "https://api.rippling.com/platform/api/ats/v1/board/%s/jobs"
BOARD_URL = "https://ats.rippling.com/%s"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

WORKERS = 8
RPS = 8.0
TIMEOUT = 30
TRIES = 6

RES_COLS = ["slug", "status", "company", "board_url", "api_url", "n_jobs",
            "sources", "detail"]

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]
_counts = {"live": 0, "empty": 0, "dead": 0, "error": 0, "r429": 0}


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
        _counts["r429"] += 1
        _pause_until[0] = time.time() + 20
        _interval[0] = min(_interval[0] * 1.5, 0.5)


def pretty(slug):
    return slug.replace("-", " ").replace("_", " ").strip().title()


def probe(cand):
    """-> (status, n_jobs, detail).  Classified from the BODY, not the status
    code: a 200 carrying [] is a real board, not a dead one."""
    slug = cand["slug"]
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json"})
    last = ""
    try:
        for attempt in range(TRIES):
            throttle()
            try:
                r = s.get(API % slug, timeout=TIMEOUT)
                if r.status_code == 404:
                    return "dead", 0, "404"
                if r.status_code in (429, 418, 403):
                    slow_down()
                    last = "HTTP %d" % r.status_code
                elif r.status_code == 200:
                    try:
                        arr = r.json()
                    except ValueError:
                        last = "bad-json"
                    else:
                        if not isinstance(arr, list):
                            return "dead", 0, "not-a-list"
                        # This board returns some uuids twice; dedupe first.
                        n = len({j.get("uuid") for j in arr
                                 if isinstance(j, dict) and j.get("uuid")})
                        return ("live" if n else "empty"), n, ""
                else:
                    last = "HTTP %d" % r.status_code
            except requests.RequestException as e:
                last = type(e).__name__
            time.sleep(2 * (attempt + 1))
    finally:
        s.close()
    return "error", 0, last


def main():
    if not os.path.exists(CAND_CSV):
        raise SystemExit("missing %s — run rippling_discover_boards.py"
                         % CAND_CSV)
    with open(CAND_CSV, newline="", encoding="utf-8") as f:
        cands = list(csv.DictReader(f))

    # Resume: live/empty/dead are settled, only error rows are re-probed.
    done, keep = set(), []
    if os.path.exists(RESULTS_CSV):
        with open(RESULTS_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("status") in ("live", "empty", "dead"):
                    done.add(row["slug"])
                    keep.append([row.get(c, "") for c in RES_COLS])
                    _counts[row["status"]] += 1
    todo = [c for c in cands if c["slug"] not in done]
    if SMOKE and len(todo) > SMOKE:
        step = max(1, len(todo) // SMOKE)
        todo = todo[::step][:SMOKE]
        print("SMOKE MODE: %d of %d candidates, artifacts prefixed '%s'"
              % (len(todo), len(cands), PFX), flush=True)
    print("%d candidates, %d settled, %d to probe"
          % (len(cands), len(done), len(todo)), flush=True)

    with open(RESULTS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(RES_COLS)
        for row in keep:
            w.writerow(row)
        f.flush()

        t0 = time.time()
        ndone = 0
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {ex.submit(probe, c): c for c in todo}
            for fut in cf.as_completed(futs):
                c = futs[fut]
                try:
                    status, n, detail = fut.result()
                except Exception as e:          # noqa: BLE001
                    status, n, detail = "error", 0, type(e).__name__
                with _lock:
                    _counts[status] += 1
                    w.writerow([c["slug"], status,
                                c.get("name_hint") or pretty(c["slug"]),
                                BOARD_URL % c["slug"], API % c["slug"], n,
                                c.get("sources", ""), detail])
                    f.flush()
                    ndone += 1
                    if ndone % 200 == 0:
                        el = time.time() - t0
                        eta = el / ndone * (len(todo) - ndone) / 60
                        print("  %d/%d  live %d empty %d dead %d err %d "
                              "| eta %.0fm"
                              % (ndone, len(todo), _counts["live"],
                                 _counts["empty"], _counts["dead"],
                                 _counts["error"], eta), flush=True)

    # live + empty are both real boards worth fetching
    boards = []
    with open(RESULTS_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["status"] in ("live", "empty"):
                boards.append(row)
    boards.sort(key=lambda r: r["slug"])
    with open(BOARDS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["slug", "company", "board_url", "api_url", "n_jobs",
                    "sources"])
        for r in boards:
            w.writerow([r["slug"], r["company"], r["board_url"], r["api_url"],
                        r["n_jobs"], r["sources"]])

    print("\nDone: %d boards (%d live, %d empty) -> %s (%d errors — re-run "
          "to retry)" % (len(boards), _counts["live"], _counts["empty"],
                         BOARDS_CSV, _counts["error"]), flush=True)
    raise SystemExit(1 if _counts["error"] else 0)


if __name__ == "__main__":
    sys.exit(main())
