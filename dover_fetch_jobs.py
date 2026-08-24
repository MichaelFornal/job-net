#!/usr/bin/env python3
"""
dover_fetch_jobs.py — two-level fetch of Dover postings.

  level 1  GET /api/v1/careers-page/{uuid}/jobs
           DRF page: {"count","next","previous","results":[...]}
           results carry only id / title / locations / workplace_type /
           is_published / is_sample.
  level 2  GET /api/v1/inbound/application-portal-job/{job_id}
           public, and the only place the real data lives: `created` (a real
           ISO date), structured `compensation`, client_name, client_domain,
           visa_support.

The N+1 is deliberate and is the whole reason this pipe is worth building.
Skipping level 2 would emit a few hundred rows with no date and no salary —
and undated rows never age out of all_jobs_build.py's cutoff, so they would
sit in the vet queue forever.  At ~3 rps the extra calls cost minutes.

If Dover ever closes that endpoint the pipe degrades instead of dying: after
20 consecutive 401/403 responses level 2 is abandoned, a WARNING is logged,
rows fall back to first-seen dates, and the run exits non-zero so the
degradation is visible rather than silent.

A board is recorded done only once ALL its jobs fetched cleanly, so a retry
cannot append duplicates for a partially-written board.
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

BOARDS_CSV = os.path.join(BASE, PFX + "dover_job_boards.csv")
OUT_JSONL = os.path.join(BASE, PFX + "dover_all_jobs.jsonl")
STATE_JSON = os.path.join(BASE, PFX + "dover_fetch_state.json")
LEDGER_JSON = os.path.join(BASE, PFX + "dover_posted_ledger.json")

JOBS = "https://app.dover.com/api/v1/careers-page/%s/jobs"
DETAIL = "https://app.dover.com/api/v1/inbound/application-portal-job/%s"
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

WORKERS = 4
RPS = 3.0
TIMEOUT = 60
TRIES = 6
MIN_FREE_GB = 5
AUTH_TRIP = 20
STRIP = ("user_provided_description", "application_questions",
         "public_interview_plan_preview", "client_logo", "description")

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

_lock = threading.Lock()
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]
_auth_fail = [0]          # consecutive level-2 401/403
_detail_off = [False]
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


def get(s, url, note_auth=False):
    last = ""
    for attempt in range(TRIES):
        throttle()
        try:
            r = s.get(url, timeout=TIMEOUT)
            if r.status_code == 404:
                return None, "404"
            if r.status_code in (401, 403) and note_auth:
                with _lock:
                    _auth_fail[0] += 1
                return None, "auth"
            if r.status_code in (429, 418, 403):
                with _rate_lock:
                    _pause_until[0] = time.time() + 20
                    _interval[0] = min(_interval[0] * 1.5, 0.5)
                last = "HTTP %d" % r.status_code
            else:
                d = json_or_none(r)
                if d is not None:
                    if note_auth:
                        with _lock:
                            _auth_fail[0] = 0
                    return d, ""
                last = "spa-or-nonjson (HTTP %d)" % r.status_code
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    raise RuntimeError(last or "unknown")


def fetch_board(b):
    uu = b["uuid"]
    s = requests.Session()
    s.headers.update({"User-Agent": UA, "Accept": "application/json"})
    out = []
    try:
        url = JOBS % uu
        seen = 0
        while url and seen < 50:
            page, why = get(s, url)
            if page is None:
                break
            for j in (page.get("results") or []):
                if not isinstance(j, dict):
                    continue
                if j.get("is_sample") or j.get("is_published") is False:
                    continue
                out.append(j)
            url = page.get("next")
            seen += 1

        merged = []
        for j in out:
            jid = j.get("id")
            rec = {k: v for k, v in j.items() if k not in STRIP}
            if jid and not _detail_off[0]:
                try:
                    d, why = get(s, DETAIL % jid, note_auth=True)
                except RuntimeError:
                    d = None
                if d:
                    if d.get("active") is False or d.get("is_private"):
                        continue
                    for k, v in d.items():
                        if k not in STRIP:
                            rec.setdefault(k, v)
                            rec[k] = v if k not in j else rec[k]
                else:
                    rec["detail_failed"] = True
            merged.append(rec)
        return merged
    finally:
        s.close()


def main():
    if not os.path.exists(BOARDS_CSV):
        raise SystemExit("missing %s — run dover_probe_boards.py"
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
            if _state.get(b["uuid"], {}).get("status") != "done"]
    already = len(boards) - len(todo)
    if SMOKE and len(todo) > SMOKE:
        step = max(1, len(todo) // SMOKE)
        todo = todo[::step][:SMOKE]
        print("SMOKE MODE: %d of %d boards, artifacts prefixed '%s'"
              % (len(todo), len(boards) - already, PFX), flush=True)
    print("%d boards, %d already done, %d to fetch"
          % (len(boards), already, len(todo)), flush=True)

    total = nfail = ndone = 0
    with open(OUT_JSONL, "a", encoding="utf-8") as out:
        with cf.ThreadPoolExecutor(max_workers=WORKERS) as ex:
            futs = {ex.submit(fetch_board, b): b for b in todo}
            for fut in cf.as_completed(futs):
                b = futs[fut]
                try:
                    jobs = fut.result()
                except Exception as e:              # noqa: BLE001
                    with _lock:
                        _state[b["uuid"]] = {"status": "failed",
                                             "error": str(e)[:120]}
                        nfail += 1
                        ndone += 1
                        save_json(STATE_JSON, _state)
                    continue
                with _lock:
                    if _auth_fail[0] >= AUTH_TRIP and not _detail_off[0]:
                        _detail_off[0] = True
                        print("  WARNING: detail endpoint closed (%d "
                              "consecutive auth failures) — continuing with "
                              "list-only, undated rows" % _auth_fail[0],
                              flush=True)
                    for j in jobs:
                        jid = str(j.get("id") or "")
                        if jid and jid not in _ledger:
                            created = (j.get("created") or "")[:10]
                            _ledger[jid] = {
                                "posted": created or TODAY,
                                "precision": "created" if created
                                             else "first_seen",
                                "first_seen": TODAY}
                        out.write(json.dumps({
                            "uuid": b["uuid"],
                            "company": b.get("company") or "",
                            "slug": b.get("slug") or "",
                            "board_url": b.get("board_url") or "",
                            "job": j,
                        }, ensure_ascii=False) + "\n")
                    out.flush()
                    total += len(jobs)
                    _state[b["uuid"]] = {"status": "done",
                                         "fetched": len(jobs),
                                         "fetched_at": TODAY}
                    ndone += 1
                    save_json(STATE_JSON, _state)
                    if ndone % 50 == 0:
                        save_json(LEDGER_JSON, _ledger)
                        print("  %d/%d boards | %d jobs"
                              % (ndone, len(todo), total), flush=True)
    save_json(LEDGER_JSON, _ledger)
    print("\nDone. %d job rows in %s; %d boards failed%s"
          % (total, OUT_JSONL, nfail,
             " (re-run to retry)" if nfail else ""), flush=True)
    if _detail_off[0]:
        raise SystemExit("detail endpoint unavailable — rows are undated")
    raise SystemExit(1 if nfail else 0)


if __name__ == "__main__":
    sys.exit(main())
