#!/usr/bin/env python3
"""
rippling_fetch_jobs.py — pull every posting from the live Rippling boards in
rippling_job_boards.csv.

  GET https://api.rippling.com/platform/api/ats/v1/board/{slug}/jobs
  -> a flat JSON array holding the WHOLE board.  No pagination.

Payload shape, sampled from 85 postings across 10 live boards (2026-07-27):

    {"uuid": "...", "name": "Assistant Manager - Conway",
     "department":   {"id": "Club",        "label": "Club"},
     "workLocation": {"id": "Conway, AR",  "label": "Conway, AR"},
     "url": "https://ats.rippling.com/10fitness/jobs/<uuid>"}

Those five keys were present on 85/85 postings and NOTHING ELSE WAS.  In
particular there is no employmentType, no salary, and — the one that drives a
design decision — no created/posted date of any kind.

Undated rows are a problem downstream, not a cosmetic gap: all_jobs_build.py
keeps every row whose `posted` is blank, so undated jobs survive the
POSTED_SINCE cutoff forever and accumulate in vet_build.py's queue.  So this
script keeps rippling_posted_ledger.json, recording the date each uuid was
FIRST seen and reusing it verbatim thereafter.  first_seen is not the true
posting date, but it is stable across runs (unlike anything recomputed) and it
ages out, which blank never does.

  rippling_all_jobs.jsonl   append-only, {"slug","company","board_url","job"}
  rippling_fetch_state.json per-slug {"status": "done"|"failed", "fetched": n}
  rippling_posted_ledger.json  {uuid: {"first_seen": "YYYY-MM-DD"}}
"""
import csv
import json
import os
import shutil
import sys
import threading
import time
import concurrent.futures as cf
from datetime import datetime, timezone

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
SMOKE = int(os.environ.get("SMOKE") or 0)
PFX = "smoke_" if SMOKE else ""

BOARDS_CSV = os.path.join(BASE, PFX + "rippling_job_boards.csv")
OUT_JSONL = os.path.join(BASE, PFX + "rippling_all_jobs.jsonl")
STATE_JSON = os.path.join(BASE, PFX + "rippling_fetch_state.json")
LEDGER_JSON = os.path.join(BASE, PFX + "rippling_posted_ledger.json")

API = "https://api.rippling.com/platform/api/ats/v1/board/%s/jobs"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

WORKERS = 8
RPS = 8.0
TIMEOUT = 60
TRIES = 6
MIN_FREE_GB = 5

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]
_state = {}
_ledger = {}


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


def load_json(path, default):
    if os.path.exists(path):
        try:
            with open(path, encoding="utf-8") as f:
                return json.load(f)
        except (OSError, json.JSONDecodeError):
            return default
    return default


def save_json(path, obj):
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(obj, f)
    os.replace(tmp, path)


def fetch_board(slug):
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json"})
    last = ""
    try:
        for attempt in range(TRIES):
            throttle()
            try:
                r = s.get(API % slug, timeout=TIMEOUT)
                if r.status_code == 404:
                    return []          # board vanished between probe and fetch
                if r.status_code in (429, 418, 403):
                    with _rate_lock:
                        _pause_until[0] = time.time() + 20
                        _interval[0] = min(_interval[0] * 1.5, 0.5)
                    last = "HTTP %d" % r.status_code
                elif r.status_code == 200:
                    try:
                        arr = r.json()
                    except ValueError:
                        last = "bad-json"
                    else:
                        if not isinstance(arr, list):
                            return []
                        seen, out = set(), []
                        for j in arr:
                            if not isinstance(j, dict):
                                continue
                            u = j.get("uuid")
                            if not u or u in seen:
                                continue   # some boards repeat a uuid
                            seen.add(u)
                            out.append(j)
                        return out
                else:
                    last = "HTTP %d" % r.status_code
            except requests.RequestException as e:
                last = type(e).__name__
            time.sleep(2 * (attempt + 1))
    finally:
        s.close()
    raise RuntimeError("%s failed after %d tries: %s" % (slug, TRIES, last))


def main():
    if not os.path.exists(BOARDS_CSV):
        raise SystemExit("missing %s — run rippling_probe_boards.py"
                         % BOARDS_CSV)
    free_gb = shutil.disk_usage(BASE).free / 1e9
    if free_gb < MIN_FREE_GB:
        raise SystemExit("only %.1f GB free — refusing to start" % free_gb)

    global _state, _ledger
    _state = load_json(STATE_JSON, {})
    _ledger = load_json(LEDGER_JSON, {})

    with open(BOARDS_CSV, newline="", encoding="utf-8") as f:
        boards = list(csv.DictReader(f))
    todo = [b for b in boards
            if _state.get(b["slug"], {}).get("status") != "done"]
    already = len(boards) - len(todo)
    if SMOKE and len(todo) > SMOKE:
        step = max(1, len(todo) // SMOKE)
        todo = todo[::step][:SMOKE]
        print("SMOKE MODE: %d of %d boards, artifacts prefixed '%s'"
              % (len(todo), len(boards) - already, PFX), flush=True)
    print("%d boards, %d already done, %d to fetch"
          % (len(boards), already, len(todo)), flush=True)

    total = nfail = ndone = 0
    t0 = time.time()
    with open(OUT_JSONL, "a", encoding="utf-8") as out:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {ex.submit(fetch_board, b["slug"]): b for b in todo}
            for fut in cf.as_completed(futs):
                b = futs[fut]
                slug = b["slug"]
                try:
                    jobs = fut.result()
                except Exception as e:          # noqa: BLE001
                    with _lock:
                        _state[slug] = {"status": "failed", "error": str(e)[:120]}
                        nfail += 1
                        ndone += 1
                        save_json(STATE_JSON, _state)
                    continue
                with _lock:
                    for j in jobs:
                        u = j.get("uuid")
                        if u not in _ledger:
                            _ledger[u] = {"first_seen": TODAY}
                        out.write(json.dumps({
                            "slug": slug,
                            "company": b.get("company") or slug,
                            "board_url": b.get("board_url") or "",
                            "job": j,
                        }, ensure_ascii=False) + "\n")
                    out.flush()
                    total += len(jobs)
                    _state[slug] = {"status": "done", "fetched": len(jobs),
                                    "fetched_at": TODAY}
                    ndone += 1
                    save_json(STATE_JSON, _state)
                    if ndone % 200 == 0:
                        save_json(LEDGER_JSON, _ledger)
                        el = time.time() - t0
                        eta = el / ndone * (len(todo) - ndone) / 60
                        print("  %d/%d boards | %d jobs | eta %.0fm"
                              % (ndone, len(todo), total, eta), flush=True)
    save_json(LEDGER_JSON, _ledger)
    print("\nDone. %d job rows in %s; %d boards failed%s"
          % (total, OUT_JSONL, nfail,
             " (re-run to retry)" if nfail else ""), flush=True)
    raise SystemExit(1 if nfail else 0)


if __name__ == "__main__":
    sys.exit(main())
