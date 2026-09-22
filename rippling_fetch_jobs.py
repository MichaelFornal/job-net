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

Those five keys are ALL the LIST endpoint returns — no employmentType, no
salary, and no date of any kind.

But the PER-JOB endpoint has more (2026-08-24):

  GET .../board/{slug}/jobs/{uuid}
  -> createdOn, employmentType, payRangeDetails, companyName, workLocations,
     department.department_tree, unlistedFromSearch, description

`createdOn` is the employer's real timestamp, present on 60/60 sampled
postings.  Before this, EVERY rippling row was posted_precision=first_seen and
16,634 of 22,618 ledger uuids had frozen on one day (2026-07-28), so the whole
pipe would have aged out of any POSTED_SINCE window together, on a date that
meant nothing.  So the fetch now costs one extra request per posting, cached
forever by uuid.

The ledger STAYS, as the fallback for postings whose detail fetch fails and
for the *.rippling-ats.com tenant boards, which publish no date anywhere.
Undated rows are a downstream problem, not a cosmetic gap: all_jobs_build.py
keeps every row whose `posted` is blank, so undated jobs survive the
POSTED_SINCE cutoff forever and pile up in vet_build.py's queue.

  rippling_all_jobs.jsonl   append-only,
                            {"slug","company","board_url","job","detail"}
  rippling_fetch_state.json per-slug {"status": "done"|"failed", "fetched": n}
  rippling_posted_ledger.json  {uuid: {"first_seen": "YYYY-MM-DD"}}
  rippling_detail_cache.json   {uuid: trimmed detail}.  NEVER rotated —
                            createdOn is immutable, so a cached posting is
                            never re-fetched.  Trimmed because full payloads
                            average 28 KB (512 MB here); `description` alone
                            is 18 KB of that and nothing downstream reads it.
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
# Keyed by uuid and NEVER rotated, exactly like the posted ledger: createdOn
# is immutable per posting, so a job whose detail we already hold never needs
# re-fetching.  Without this cache every refresh re-pays ~18k requests.
# Trimmed on purpose -- full payloads average 28 KB (512 MB at this scale)
# because `description` alone is 18 KB; the kept fields are 412 B (7.4 MB).
DETAIL_CACHE = os.path.join(BASE, PFX + "rippling_detail_cache.json")
DETAIL_KEEP = ("createdOn", "employmentType", "payRangeDetails",
               "companyName", "workLocations", "department",
               "unlistedFromSearch")

API = "https://api.rippling.com/platform/api/ats/v1/board/%s/jobs"
# The board endpoint returns FIVE fields and no date.  The per-job endpoint
# carries the employer's real createdOn -- which is why every rippling row
# used to be posted_precision=first_seen.  MEASURED 2026-08-24 over a 60-job
# sample: createdOn present 60/60, employmentType 60/60, payRangeDetails
# 17/60.
DETAIL_API = ("https://api.rippling.com/platform/api/ats/v1/board/%s"
              "/jobs/%s")
UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
      "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36")

WORKERS = int(os.environ.get("RIPPLING_WORKERS") or 8)
RPS = float(os.environ.get("RIPPLING_RPS") or 8.0)
TIMEOUT = 60
TRIES = 6
MIN_FREE_GB = 5

TODAY = datetime.now(timezone.utc).strftime("%Y-%m-%d")

# RLock, not Lock: save_ledger() takes this lock to snapshot _ledger, and the
# main loop calls it from inside its own `with _lock:` block.  A plain Lock
# deadlocks there -- verified by hanging a test harness for 5 minutes.
_lock = threading.RLock()
_rate_lock = threading.Lock()
_next_t = [0.0]
_interval = [1.0 / RPS]
_pause_until = [0.0]
_state = {}
_detail = {}
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


def save_ledger():
    """Read-merge-write, because rippling_tenant_fetch.py shares this file.

    Both scripts hold a start-of-run snapshot in memory; a plain overwrite
    means whoever finishes last erases the other's keys.  Ours are bare uuids,
    the tenant fetcher's are "{tenant}|{id}", so a merge is unambiguous.
    """
    merged = {}
    if os.path.exists(LEDGER_JSON):
        try:
            with open(LEDGER_JSON, encoding="utf-8") as f:
                cur = json.load(f)
            if isinstance(cur, dict):
                merged.update(cur)
        except (OSError, ValueError):
            pass
    with _lock:
        for k, v in _ledger.items():
            merged.setdefault(k, v)
    save_json(LEDGER_JSON, merged)


def fetch_details(sess, slug, jobs):
    """uuid -> trimmed detail for every job on this board, cache first."""
    out = {}
    for j in jobs:
        u = j.get("uuid")
        if not u:
            continue
        with _lock:
            hit = _detail.get(u)
        if hit is not None:
            out[u] = hit
            continue
        d = fetch_detail(sess, slug, u)
        if d is None:
            continue
        out[u] = d
        with _lock:
            _detail[u] = d
    return out


def fetch_detail(sess, slug, uuid):
    """One posting's full record, trimmed to DETAIL_KEEP.

    Returns None on a hard failure so the caller can fall back to the list
    fields plus the posted ledger -- a missing date must never abort a board.
    A 404 here means the posting went away between the list call and now,
    which is normal and not worth retrying.
    """
    last = ""
    for attempt in range(TRIES):
        throttle()
        try:
            r = sess.get(DETAIL_API % (slug, uuid), timeout=TIMEOUT)
            if r.status_code == 404:
                return None
            if r.status_code in (429, 418, 403):
                with _rate_lock:
                    _pause_until[0] = time.time() + 20
                    _interval[0] = min(_interval[0] * 1.5, 0.5)
                last = "HTTP %d" % r.status_code
            elif r.status_code == 200:
                try:
                    d = r.json()
                except ValueError:
                    last = "bad-json"
                else:
                    if not isinstance(d, dict):
                        return None
                    return {k: d.get(k) for k in DETAIL_KEEP if d.get(k)
                            is not None}
            else:
                last = "HTTP %d" % r.status_code
        except requests.RequestException as e:
            last = type(e).__name__
        time.sleep(2 * (attempt + 1))
    print("  ! detail %s/%s: %s" % (slug, uuid[:8], last), flush=True)
    return None


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
                    # board vanished between probe and fetch
                    return [], {}
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
                            return [], {}
                        seen, out = set(), []
                        for j in arr:
                            if not isinstance(j, dict):
                                continue
                            u = j.get("uuid")
                            if not u or u in seen:
                                continue   # some boards repeat a uuid
                            seen.add(u)
                            out.append(j)
                        return out, fetch_details(s, slug, out)
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
    _detail.update(load_json(DETAIL_CACHE, {}))
    print("detail cache: %d postings already dated" % len(_detail), flush=True)

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
                    jobs, details = fut.result()
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
                        rec = {
                            "slug": slug,
                            "company": b.get("company") or slug,
                            "board_url": b.get("board_url") or "",
                            "job": j,
                        }
                        det = details.get(u)
                        if det:
                            rec["detail"] = det
                        out.write(json.dumps(rec, ensure_ascii=False) + "\n")
                    out.flush()
                    total += len(jobs)
                    _state[slug] = {"status": "done", "fetched": len(jobs),
                                    "fetched_at": TODAY}
                    ndone += 1
                    save_json(STATE_JSON, _state)
                    if ndone % 200 == 0:
                        save_ledger()
                        save_json(DETAIL_CACHE, _detail)
                        el = time.time() - t0
                        eta = el / ndone * (len(todo) - ndone) / 60
                        print("  %d/%d boards | %d jobs | eta %.0fm"
                              % (ndone, len(todo), total, eta), flush=True)
    save_ledger()
    save_json(DETAIL_CACHE, _detail)
    print("\nDone. %d job rows in %s; %d boards failed%s"
          % (total, OUT_JSONL, nfail,
             " (re-run to retry)" if nfail else ""), flush=True)
    raise SystemExit(1 if nfail else 0)


if __name__ == "__main__":
    sys.exit(main())
