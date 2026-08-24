#!/usr/bin/env python3
"""
dover_probe_boards.py — resolve each candidate careers-page UUID to a board.

  GET https://app.dover.com/api/v1/careers-page/{uuid}        -> slug, name
  GET https://app.dover.com/api/v1/careers-page/{uuid}/jobs   -> count

The probe earns a stage of its own here (unlike ashby/yc, where fetch doubles
as probe) because it does work the fetcher cannot:
  * it resolves `slug`, which the apply URLs are built from, and
  * it owns the zero_streak -> retired ledger.  Roughly a third of discovered
    UUIDs are archived boards that return count==0 forever; without retiring
    them a third of every run is spent re-probing the dead.

THE SPA GUARD IS NOT OPTIONAL.  app.dover.com answers HTTP 200 with
text/html (3,988 bytes) on any unrecognised path — /zzz/nonexistent/path
included — so a status-code-only health check reports 100% success against a
completely wrong URL.  Everything goes through json_or_none().

Statuses: live (count>0) · empty · retired (4 consecutive zeros) · dead ·
error.  Resume state is dover_probe_results.csv; error rows are re-probed,
everything else is settled.
"""
import csv
import json
import os
import sys
import threading
import time
import concurrent.futures as cf

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
SMOKE = int(os.environ.get("SMOKE") or 0)
PFX = "smoke_" if SMOKE else ""

CAND_CSV = os.path.join(BASE, PFX + "dover_candidates.csv")
RESULTS_CSV = os.path.join(BASE, PFX + "dover_probe_results.csv")
BOARDS_CSV = os.path.join(BASE, PFX + "dover_job_boards.csv")

META = "https://app.dover.com/api/v1/careers-page/%s"
JOBS = "https://app.dover.com/api/v1/careers-page/%s/jobs"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

# Dover is slow, not hostile: 6 workers at 5 rps produced 25% transient
# errors on UUIDs that succeeded on a serial retry.
WORKERS = 4
RPS = 3.0
TIMEOUT = 60
TRIES = 6
RETIRE_AFTER = 4

RES_COLS = ["uuid", "status", "company", "slug", "domain", "board_url",
            "n_jobs", "zero_streak", "sources", "detail"]

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]
_counts = {"live": 0, "empty": 0, "retired": 0, "dead": 0, "error": 0}


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


def json_or_none(r):
    if r.status_code != 200:
        return None
    if "json" not in (r.headers.get("content-type") or "").lower():
        return None
    body = r.text.lstrip()
    if not body or body[0] == "<":
        return None
    try:
        return json.loads(body)
    except ValueError:
        return None


def get(s, url):
    last = ""
    for attempt in range(TRIES):
        throttle()
        try:
            r = s.get(url, timeout=TIMEOUT)
            if r.status_code == 404:
                return None, "404"
            if r.status_code in (429, 418, 403):
                with _rate_lock:
                    _pause_until[0] = time.time() + 20
                    _interval[0] = min(_interval[0] * 1.5, 0.5)
                last = "HTTP %d" % r.status_code
            else:
                d = json_or_none(r)
                if d is not None:
                    return d, ""
                last = "spa-or-nonjson (HTTP %d)" % r.status_code
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(last or "unknown")


def probe(cand, prev_streak):
    uu = cand["uuid"]
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json"})
    try:
        meta, why = get(s, META % uu)
        if meta is None or not meta.get("slug"):
            return "dead", {}, 0, prev_streak, why or "no-slug"
        jobs, why2 = get(s, JOBS % uu)
        if jobs is None:
            return "dead", meta, 0, prev_streak, why2 or "jobs-404"
        n = int(jobs.get("count") or 0)
        if n > 0:
            return "live", meta, n, 0, ""
        streak = prev_streak + 1
        return ("retired" if streak >= RETIRE_AFTER else "empty"), \
            meta, 0, streak, ""
    finally:
        s.close()


def main():
    if not os.path.exists(CAND_CSV):
        raise SystemExit("missing %s — run dover_discover_boards.py"
                         % CAND_CSV)
    with open(CAND_CSV, newline="", encoding="utf-8") as f:
        cands = list(csv.DictReader(f))

    prev, keep = {}, []
    if os.path.exists(RESULTS_CSV):
        with open(RESULTS_CSV, newline="", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                prev[row["uuid"]] = row
                if row.get("status") in ("retired", "dead"):
                    keep.append([row.get(c, "") for c in RES_COLS])
                    _counts[row["status"]] += 1
    settled = {r[0] for r in keep}
    todo = [c for c in cands if c["uuid"] not in settled]
    if SMOKE and len(todo) > SMOKE:
        step = max(1, len(todo) // SMOKE)
        todo = todo[::step][:SMOKE]
        print("SMOKE MODE: %d of %d candidates, artifacts prefixed '%s'"
              % (len(todo), len(cands), PFX), flush=True)
    print("%d candidates, %d settled (dead/retired), %d to probe"
          % (len(cands), len(settled), len(todo)), flush=True)

    with open(RESULTS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(RES_COLS)
        for row in keep:
            w.writerow(row)
        f.flush()
        ndone = 0
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {}
            for c in todo:
                streak = int((prev.get(c["uuid"]) or {}).get("zero_streak")
                             or 0)
                futs[ex.submit(probe, c, streak)] = c
            for fut in cf.as_completed(futs):
                c = futs[fut]
                try:
                    status, meta, n, streak, detail = fut.result()
                except Exception as e:              # noqa: BLE001
                    status, meta, n, streak, detail = \
                        "error", {}, 0, 0, type(e).__name__
                with _lock:
                    _counts[status] += 1
                    slug = (meta.get("slug") or "")
                    w.writerow([c["uuid"], status,
                                meta.get("name") or c.get("name_hint") or "",
                                slug, meta.get("primary_domain") or "",
                                "https://app.dover.com/%s/careers/%s"
                                % (slug or c["uuid"], c["uuid"]),
                                n, streak, c.get("sources", ""), detail])
                    f.flush()
                    ndone += 1
                    if ndone % 50 == 0:
                        print("  %d/%d  live %d empty %d retired %d dead %d "
                              "err %d" % (ndone, len(todo), _counts["live"],
                                          _counts["empty"], _counts["retired"],
                                          _counts["dead"], _counts["error"]),
                              flush=True)

    boards = []
    with open(RESULTS_CSV, newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            if row["status"] in ("live", "empty"):
                boards.append(row)
    boards.sort(key=lambda r: r.get("slug") or r["uuid"])
    with open(BOARDS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["uuid", "company", "slug", "domain", "board_url",
                    "n_jobs", "sources"])
        for r in boards:
            w.writerow([r["uuid"], r["company"], r["slug"], r["domain"],
                        r["board_url"], r["n_jobs"], r["sources"]])
    print("\nDone: %d live boards -> %s (%d errors — re-run to retry)"
          % (_counts["live"], BOARDS_CSV, _counts["error"]), flush=True)
    raise SystemExit(1 if _counts["error"] else 0)


if __name__ == "__main__":
    sys.exit(main())
