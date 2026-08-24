#!/usr/bin/env python3
"""
lever_fetch_jobs.py
Fetch EVERY posting from every live Lever board in lever_job_boards.csv.

API (learned 2026-07-19):
    GET https://api(.eu).lever.co/v0/postings/{slug}?mode=json&limit=100&skip=N
    -> JSON array; short page = last page. No-limit returns everything, but
       paginating at 100 bounds memory on huge boards.
    Bulky fields stripped per posting: description*, additional*, lists,
    opening* (the master sheet doesn't need prose).

Resumable: per-(region,slug) state in lever_fetch_state.json; a board's rows
append to lever_all_jobs.jsonl only after the whole board fetched cleanly.
Re-run to retry failures. 8 threads, shared ~12 rps throttle.

Output line: {"slug", "company", "region", "board_url", "job": {...}}
"""

import csv
import json
import os
import threading
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
BOARDS_CSV = os.path.join(BASE, "lever_job_boards.csv")
OUT_JSONL = os.path.join(BASE, "lever_all_jobs.jsonl")
STATE_JSON = os.path.join(BASE, "lever_fetch_state.json")

API = {"us": "https://api.lever.co/v0/postings/",
       "eu": "https://api.eu.lever.co/v0/postings/"}
PAGE = 100
WORKERS = 8
RPS = 12.0
TIMEOUT = 40
STRIP = ("description", "descriptionPlain", "descriptionBody",
         "descriptionBodyPlain", "additional", "additionalPlain", "lists",
         "opening", "openingPlain")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]


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


def get_page(session, region, slug, skip):
    url = API[region] + urllib.parse.quote(slug, safe="")
    last = None
    for attempt in range(6):
        throttle()
        try:
            r = session.get(url, params={"mode": "json", "limit": PAGE,
                                         "skip": skip}, timeout=TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                if not isinstance(data, list):
                    raise RuntimeError("non-array 200")
                return data
            if r.status_code == 404:
                return None  # board vanished between probe and fetch
            last = f"HTTP {r.status_code}"
            if r.status_code in (429, 418, 403):
                with _rate_lock:
                    _pause_until[0] = time.time() + 20
                    _interval[0] = min(_interval[0] * 1.5, 0.5)
        except (requests.RequestException, ValueError) as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"{region}:{slug} skip={skip}: {last}")


def fetch_board(board):
    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Accept": "application/json"})
    region, slug = board["region"], board["slug"]
    jobs = []
    skip = 0
    while True:
        batch = get_page(session, region, slug, skip)
        if batch is None:
            return None  # gone
        for j in batch:
            for k in STRIP:
                j.pop(k, None)
        jobs.extend(batch)
        if len(batch) < PAGE:
            return jobs
        skip += PAGE


def main():
    with open(BOARDS_CSV, encoding="utf-8") as f:
        boards = list(csv.DictReader(f))

    state = {}
    if os.path.exists(STATE_JSON):
        with open(STATE_JSON, encoding="utf-8") as f:
            state = json.load(f)

    def key(b):
        return f"{b['region']}:{b['slug']}"

    todo = [b for b in boards
            if state.get(key(b), {}).get("status") != "done"]
    print(f"{len(boards)} live boards, {len(boards) - len(todo)} done, "
          f"{len(todo)} to fetch", flush=True)

    def save_state():
        tmp = STATE_JSON + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(state, f)
        os.replace(tmp, STATE_JSON)

    total = sum(v.get("fetched", 0) for v in state.values()
                if v.get("status") == "done")
    ndone = 0
    nfail = 0
    t0 = time.time()
    out = open(OUT_JSONL, "a", encoding="utf-8")

    with ThreadPoolExecutor(max_workers=WORKERS) as ex:
        futs = {ex.submit(fetch_board, b): b for b in todo}
        for fut in as_completed(futs):
            b = futs[fut]
            k = key(b)
            try:
                jobs = fut.result()
            except Exception as e:
                with _lock:
                    nfail += 1
                    state[k] = {"status": "failed", "error": str(e)}
                    save_state()
                    print(f"  FAIL {k}: {e}", flush=True)
                continue
            with _lock:
                if jobs is None:
                    state[k] = {"status": "done", "fetched": 0,
                                "note": "404 at fetch time"}
                else:
                    for j in jobs:
                        out.write(json.dumps({
                            "slug": b["slug"], "company": b["company"],
                            "region": b["region"],
                            "board_url": b["board_url"], "job": j,
                        }, ensure_ascii=False) + "\n")
                    out.flush()
                    state[k] = {"status": "done", "fetched": len(jobs),
                                "company": b["company"],
                                "fetched_at": time.strftime(
                                    "%Y-%m-%d %H:%M", time.gmtime())}
                    total += len(jobs)
                ndone += 1
                save_state()
                if ndone % 100 == 0:
                    el = time.time() - t0
                    eta = el / ndone * (len(todo) - ndone) / 60
                    print(f"  {ndone}/{len(todo)} boards | {total} jobs "
                          f"| eta {eta:.0f}m", flush=True)
    out.close()
    print(f"\nDone. {total} job rows in {OUT_JSONL}; {nfail} boards failed"
          + (" (re-run to retry)" if nfail else ""), flush=True)
    # Matches greenhouse_fetch_jobs.py — see lever_probe_boards.py.
    raise SystemExit(1 if nfail else 0)


if __name__ == "__main__":
    main()
