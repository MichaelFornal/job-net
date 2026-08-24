#!/usr/bin/env python3
"""
ashby_fetch_jobs.py
Fetch EVERY posting from every candidate Ashby board in ashby_candidates.csv,
classifying live/dead as it goes.

API (learned 2026-07-23):
    GET https://api.ashbyhq.com/posting-api/job-board/{slug}
        ?includeCompensation=true
    -> 200 {"jobs": [...], "apiVersion": "1"} — ALL jobs in one response, no
       pagination. 404 "Not Found" (bare text, not JSON) for a dead slug.
    Slugs are CASE-INSENSITIVE -> lowercase everywhere.

Why there is no separate probe stage (unlike Lever/Greenhouse): Ashby has no
cheap board-metadata endpoint — the only public board endpoint IS the jobs
endpoint, so probing and fetching are the same request. Splitting them would
download every board twice.

The response carries no organization display name (the non-user-graphql board
query returns null for it), so company falls back to the slug. That costs
nothing downstream: all_jobs_build.canon_key merges Ashby postings with other
pipelines' copies on the canonical ats|ashby|org|id URL key, not on company.

descriptionHtml / descriptionPlain are dropped immediately — they dominate the
payload and nothing downstream reads them.

Resumable: per-slug state in ashby_fetch_state.json; a board's rows append to
ashby_all_jobs.jsonl only after the whole board fetched cleanly. Re-run to
retry errors (exits non-zero while any remain).

Output line: {"slug", "board_url", "job": {...}}
Also writes ashby_job_boards.csv (live boards only, with job counts).
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
CAND_CSV = os.path.join(BASE, "ashby_candidates.csv")
OUT_JSONL = os.path.join(BASE, "ashby_all_jobs.jsonl")
STATE_JSON = os.path.join(BASE, "ashby_fetch_state.json")
BOARDS_CSV = os.path.join(BASE, "ashby_job_boards.csv")

API = "https://api.ashbyhq.com/posting-api/job-board/"
SITE = "https://jobs.ashbyhq.com/"
WORKERS = 10
RPS = 10.0
TIMEOUT = 120
STRIP = ("descriptionHtml", "descriptionPlain")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]
_state = {}
_out = None


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


def backoff(seconds):
    with _rate_lock:
        _pause_until[0] = max(_pause_until[0], time.time() + seconds)


def load_state():
    global _state
    if os.path.exists(STATE_JSON):
        with open(STATE_JSON, encoding="utf-8") as f:
            _state = json.load(f)
    return _state


def save_state():
    # Snapshot under the lock: workers mutate _state from other threads, and
    # json.dump would otherwise iterate a dict that changes size mid-write.
    with _lock:
        snap = dict(_state)
    tmp = STATE_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(snap, f)
    os.replace(tmp, STATE_JSON)


def fetch_board(slug):
    """-> (jobs list, status) ; jobs is None when the board is dead."""
    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Accept": "application/json"})
    url = API + urllib.parse.quote(slug, safe="")
    last = None
    for attempt in range(6):
        throttle()
        try:
            r = session.get(url, params={"includeCompensation": "true"},
                            timeout=TIMEOUT)
            if r.status_code == 200:
                try:
                    data = r.json()
                except ValueError:
                    last = "bad json"
                    time.sleep(2 * (attempt + 1))
                    continue
                return (data.get("jobs") or []), "live"
            if r.status_code in (404, 410):
                return None, "dead"
            if r.status_code == 429:
                backoff(30)
                last = "HTTP 429"
            else:
                last = f"HTTP {r.status_code}"
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(min(3 * (2 ** attempt), 60))
    raise RuntimeError(last or "unknown")


def do_slug(slug):
    jobs, status = fetch_board(slug)
    if status == "dead":
        with _lock:
            _state[slug] = {"status": "dead", "n": 0}
        return 0
    rows = []
    for j in jobs:
        for k in STRIP:
            j.pop(k, None)
        rows.append(json.dumps(
            {"slug": slug, "board_url": SITE + slug, "job": j},
            ensure_ascii=False))
    with _lock:
        if rows:
            _out.write("\n".join(rows) + "\n")
            _out.flush()
        _state[slug] = {"status": "live", "n": len(jobs)}
    return len(jobs)


def main():
    global _out
    if not os.path.exists(CAND_CSV):
        raise SystemExit(f"missing {CAND_CSV} — run ashby_discover_boards.py")
    load_state()
    with open(CAND_CSV, newline="", encoding="utf-8") as f:
        slugs = [r["slug"] for r in csv.DictReader(f) if r.get("slug")]
    todo = [s for s in slugs
            if _state.get(s, {}).get("status") not in ("live", "dead")]
    print(f"{len(slugs)} candidate boards, {len(todo)} to fetch "
          f"({len(slugs) - len(todo)} already done)", flush=True)

    failures = []
    total = 0
    done = 0
    t0 = time.time()
    _out = open(OUT_JSONL, "a", encoding="utf-8")
    try:
        with ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {ex.submit(do_slug, s): s for s in todo}
            for fut in as_completed(futs):
                s = futs[fut]
                done += 1
                try:
                    total += fut.result()
                except Exception as e:  # noqa: BLE001 — retried on re-run
                    failures.append((s, str(e)))
                if done % 200 == 0:
                    save_state()
                    rate = (time.time() - t0) / done
                    eta = rate * (len(todo) - done) / 60
                    print(f"  {done}/{len(todo)} boards | {total} jobs | "
                          f"eta {eta:.0f}m", flush=True)
    finally:
        save_state()
        _out.close()

    live = {s: v for s, v in _state.items() if v.get("status") == "live"}
    with open(BOARDS_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["slug", "board_url", "api_url", "n_jobs"])
        for s in sorted(live):
            w.writerow([s, SITE + s, API + s, live[s]["n"]])
    n_dead = sum(1 for v in _state.values() if v.get("status") == "dead")
    print(f"live boards: {len(live)}  dead: {n_dead}  "
          f"jobs this run: {total}", flush=True)
    print(f"wrote {BOARDS_CSV}", flush=True)
    if failures:
        for s, e in failures[:10]:
            print(f"  FAIL {s}: {e}", flush=True)
        raise SystemExit(f"{len(failures)} boards failed — re-run to retry")
    print(f"Done. jobs in {OUT_JSONL}; 0 boards failed", flush=True)


if __name__ == "__main__":
    main()
