#!/usr/bin/env python3
"""
greenhouse_fetch_jobs.py
Fetch EVERY posting from every live Greenhouse board in
greenhouse_job_boards.csv (mirrors lever_fetch_jobs.py).

API (learned 2026-07-21):
    GET https://boards-api.greenhouse.io/v1/boards/{token}/jobs?content=true
    -> {"jobs": [...], "meta": {"total": N}} — ALL jobs in one response, no
       pagination (meta.total == len(jobs); logged if it ever disagrees).
    content=true adds departments[] + offices[] (wanted for the sheet) plus
    the full HTML description, which we strip immediately along with other
    bulk/noise fields. No structured salary fields exist in this API (pay
    appears only inside description prose). Big boards are a few tens of MB
    with content — hence the long timeout.

Resumable: per-token state in greenhouse_fetch_state.json; a board's rows
append to greenhouse_all_jobs.jsonl only after the whole board fetched
cleanly. Re-run to retry failures (exits non-zero while failures remain).

Output line: {"token", "company", "board_url", "job": {...}}
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
BOARDS_CSV = os.path.join(BASE, "greenhouse_job_boards.csv")
OUT_JSONL = os.path.join(BASE, "greenhouse_all_jobs.jsonl")
STATE_JSON = os.path.join(BASE, "greenhouse_fetch_state.json")

API = "https://boards-api.greenhouse.io/v1/boards/"
WORKERS = 8
RPS = 8.0
TIMEOUT = 180
STRIP = ("content", "data_compliance", "ai_disclaimer",
         "ai_opt_out_request_url", "include_ai_disclaimer")
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


def fetch_board(board):
    """-> (jobs list, note) | (None, "") when board vanished."""
    session = requests.Session()
    session.headers.update({"User-Agent": UA, "Accept": "application/json"})
    url = API + urllib.parse.quote(board["token"], safe="") + "/jobs"
    last = None
    for attempt in range(6):
        throttle()
        try:
            r = session.get(url, params={"content": "true"}, timeout=TIMEOUT)
            if r.status_code == 200:
                data = r.json()
                jobs = data.get("jobs")
                if not isinstance(jobs, list):
                    raise RuntimeError("no jobs array in 200")
                total = (data.get("meta") or {}).get("total")
                note = ""
                if isinstance(total, int) and total != len(jobs):
                    note = f"meta.total {total} != {len(jobs)} fetched"
                for j in jobs:
                    for k in STRIP:
                        j.pop(k, None)
                return jobs, note
            if r.status_code == 404:
                return None, ""  # board vanished between probe and fetch
            last = f"HTTP {r.status_code}"
            if r.status_code in (429, 418, 403):
                with _rate_lock:
                    _pause_until[0] = time.time() + 20
                    _interval[0] = min(_interval[0] * 1.5, 0.5)
        except (requests.RequestException, ValueError) as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(f"{board['token']}: {last}")


def main():
    with open(BOARDS_CSV, encoding="utf-8") as f:
        boards = list(csv.DictReader(f))

    state = {}
    if os.path.exists(STATE_JSON):
        with open(STATE_JSON, encoding="utf-8") as f:
            state = json.load(f)

    todo = [b for b in boards
            if state.get(b["token"], {}).get("status") != "done"]
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
            k = b["token"]
            try:
                jobs, note = fut.result()
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
                            "token": k, "company": b["company"],
                            "board_url": b["board_url"], "job": j,
                        }, ensure_ascii=False) + "\n")
                    out.flush()
                    st = {"status": "done", "fetched": len(jobs),
                          "company": b["company"],
                          "fetched_at": time.strftime("%Y-%m-%d %H:%M",
                                                      time.gmtime())}
                    if note:
                        st["note"] = note
                        print(f"  NOTE {k}: {note}", flush=True)
                    state[k] = st
                    total += len(jobs)
                ndone += 1
                save_state()
                if ndone % 200 == 0:
                    el = time.time() - t0
                    eta = el / ndone * (len(todo) - ndone) / 60
                    print(f"  {ndone}/{len(todo)} boards | {total} jobs "
                          f"| eta {eta:.0f}m", flush=True)
    out.close()
    print(f"\nDone. {total} job rows in {OUT_JSONL}; {nfail} boards failed"
          + (" (re-run to retry)" if nfail else ""), flush=True)
    raise SystemExit(1 if nfail else 0)


if __name__ == "__main__":
    main()
