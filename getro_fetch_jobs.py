#!/usr/bin/env python3
"""
getro_fetch_jobs.py
Fetch EVERY job from every live Getro board listed in getro_job_boards.csv.

API (learned 2026-07-19):
    POST https://api.getro.com/api/v2/collections/{collection_id}/search/jobs
    body: {"hits_per_page": 1000, "page": N}   # snake_case works; camelCase is ignored (20/page)
    resp: {"results": {"jobs": [...], "count": <total>}}
    - No deep-pagination ceiling (verified past 13k results).
    - Python-urllib UA gets 403; browser UA is fine.
    - Single keep-alive connection, modest pacing; concurrency risks 418/403.

Resumable: per-board state in getro_fetch_state.json; a board's jobs are only
written to getro_all_jobs.jsonl after the whole board fetched cleanly, so the
JSONL never contains partial boards. Re-run to retry failed/pending boards.

Output line: {"board_id", "board_name", "board_url", "job": {raw job minus location_details}}
"""

import csv
import json
import os
import sys
import time

import requests

BASE = os.path.dirname(os.path.abspath(__file__))
BOARDS_CSV = os.path.join(BASE, "getro_job_boards.csv")
OUT_JSONL = os.path.join(BASE, "getro_all_jobs.jsonl")
STATE_JSON = os.path.join(BASE, "getro_fetch_state.json")

HITS_PER_PAGE = 1000
PACE_SECONDS = 0.3
TIMEOUT = 40
TRIES = 4
BACKOFFS = [5, 15, 45]  # after try 1, 2, 3

UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")


def load_state():
    if os.path.exists(STATE_JSON):
        with open(STATE_JSON, encoding="utf-8") as f:
            return json.load(f)
    return {}


def save_state(state):
    tmp = STATE_JSON + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(state, f)
    os.replace(tmp, STATE_JSON)


def api_page(session, cid, page):
    """One page fetch with retry/backoff. Returns parsed dict or raises."""
    last = None
    for attempt in range(TRIES):
        try:
            r = session.post(
                f"https://api.getro.com/api/v2/collections/{cid}/search/jobs",
                json={"hits_per_page": HITS_PER_PAGE, "page": page},
                timeout=TIMEOUT,
            )
            if r.status_code == 200:
                return r.json()
            last = f"HTTP {r.status_code}"
            # 4xx rate-limit style responses: back off hard, then retry
        except requests.RequestException as e:
            last = f"{type(e).__name__}: {e}"
        if attempt < TRIES - 1:
            time.sleep(BACKOFFS[min(attempt, len(BACKOFFS) - 1)])
    raise RuntimeError(f"collection {cid} page {page}: {last}")


def fetch_board(session, cid):
    """Fetch all jobs for one collection. Returns (jobs, reported_count)."""
    jobs = []
    page = 0
    count = None
    while True:
        data = api_page(session, cid, page)
        res = data.get("results", {})
        batch = res.get("jobs", [])
        if count is None:
            count = res.get("count", 0)
        for j in batch:
            j.pop("location_details", None)  # geo points, bulky, unused
        jobs.extend(batch)
        page += 1
        if not batch or len(jobs) >= count:
            break
        time.sleep(PACE_SECONDS)
    return jobs, count


def main():
    with open(BOARDS_CSV, encoding="utf-8") as f:
        boards = [r for r in csv.DictReader(f) if r["status"] == "live"]
    boards.sort(key=lambda r: int(r["collection_id"]))

    state = load_state()
    done = {k for k, v in state.items() if v.get("status") == "done"}
    todo = [b for b in boards if b["collection_id"] not in done]
    print(f"{len(boards)} live boards, {len(done)} already done, {len(todo)} to fetch",
          flush=True)

    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Accept": "application/json"})

    total_jobs = sum(v.get("fetched", 0) for v in state.values()
                     if v.get("status") == "done")
    failed = []
    t0 = time.time()

    for i, b in enumerate(todo, 1):
        cid = b["collection_id"]
        name = b["name"].strip()
        try:
            jobs, count = fetch_board(session, int(cid))
        except Exception as e:
            print(f"[{i}/{len(todo)}] FAIL {name} ({cid}): {e}", flush=True)
            state[cid] = {"status": "failed", "error": str(e)}
            save_state(state)
            failed.append(name)
            time.sleep(2)
            continue

        with open(OUT_JSONL, "a", encoding="utf-8") as out:
            for j in jobs:
                out.write(json.dumps({
                    "board_id": int(cid),
                    "board_name": name,
                    "board_url": b["board_url"],
                    "job": j,
                }, ensure_ascii=False) + "\n")

        total_jobs += len(jobs)
        state[cid] = {"status": "done", "count": count, "fetched": len(jobs),
                      "name": name}
        save_state(state)
        note = "" if len(jobs) == count else f"  (reported {count})"
        rate = (time.time() - t0) / i
        eta_min = rate * (len(todo) - i) / 60
        print(f"[{i}/{len(todo)}] {name} ({cid}): {len(jobs)} jobs{note}"
              f"  | total {total_jobs}  | eta {eta_min:.0f}m", flush=True)
        time.sleep(PACE_SECONDS)

    print(f"\nDone. {total_jobs} job rows in {OUT_JSONL}", flush=True)
    if failed:
        print(f"{len(failed)} boards failed (re-run to retry): "
              + ", ".join(failed[:20]), flush=True)


if __name__ == "__main__":
    main()
